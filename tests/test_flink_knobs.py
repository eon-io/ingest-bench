# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from engines.flink import fleet, knobs, script
from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, model

ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def meta(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=1, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p, row_block=64, seed=1)))


REGISTRY_URL = "http://schema-registry:8080/apis/ccompat/v7"


def _site(registry: model.SchemaRegistryConfig | None = None) -> model.SiteConfig:
    props = {
        "uri": "http://iceberg-rest:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
    return model.SiteConfig(
        "s3://corpus", "s3://runs", "s3://warehouse", "kafka:9092", {}, registry, props, None, 0.0, 0.0
    )


def test_validate(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    knobs.validate(spec.engine_block, spec, meta)
    with pytest.raises(ValueError, match="distribution_mode"):
        knobs.validate({**spec.engine_block, "distribution_mode": "sorted"}, spec, meta)
    with pytest.raises(ValueError, match="source_parallelism"):
        knobs.validate({**spec.engine_block, "source_parallelism": 99}, spec, meta)
    with pytest.raises(ValueError, match="unknown"):
        knobs.validate({**spec.engine_block, "tm_gpu": 1}, spec, meta)


def test_render_sql_and_conf(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, _site(), d, meta)
    assert (
        "`id` BIGINT NOT NULL" in sql
        and "`event_time` TIMESTAMP(3) NOT NULL" in sql
        and "`payload` BYTES NOT NULL" in sql
    )
    assert (
        "'topic' = 'smoke-flink-20260908T000000Z'" in sql
        and "'format' = 'avro'" in sql
        and "'scan.startup.mode' = 'earliest-offset'" in sql
    )
    # Without this a SQL TIMESTAMP reads as Avro's zoned `timestamp-millis`
    # rather than the zoneless `local-timestamp-millis` the column is.
    assert "'avro.timestamp_mapping.legacy' = 'false'" in sql
    assert "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'" in sql and "'client.region' = 'us-east-1'" in sql
    # Two taskmanagers of four slots against four readers, so the sink has to
    # state the writer parallelism rather than inherit the readers'.
    assert "/*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '8') */" in sql
    assert sql.count("NOT NULL") == len(meta.field_names())
    conf = knobs.render_conf(spec, d)
    assert (
        conf["execution.checkpointing.interval"] == "10s"
        and conf["parallelism.default"] == "4"
        and conf["pipeline.max-parallelism"] == "32"
    )
    files = knobs.render(spec, _site(), d, meta)
    assert set(files) == {"job.sql", "flink-conf.yaml", "flink.env"}


def _confluent(spec: model.RunSpec) -> model.RunSpec:
    return replace(spec, kafka=replace(spec.kafka, value_encoding="confluent"))


def _registry(user_info: str | None = None) -> model.SchemaRegistryConfig:
    return model.SchemaRegistryConfig(url=REGISTRY_URL, basic_auth_user_info=user_info)


def test_the_shipped_confluent_spec_is_the_raw_one_plus_its_encoding() -> None:
    raw = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    framed = model.load_run_spec(ROOT / "specs/smoke/smoke-flink-confluent.yaml")
    assert raw.kafka.value_encoding == model.VALUE_ENCODING_AVRO
    assert framed.kafka.value_encoding == model.VALUE_ENCODING_CONFLUENT
    assert framed.engine_block == raw.engine_block
    assert framed.kafka.partitions == raw.kafka.partitions and framed.kafka.key == raw.kafka.key


def test_both_encodings_are_readable_and_a_third_one_is_refused(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    knobs.validate(spec.engine_block, spec, meta)
    knobs.validate(spec.engine_block, _confluent(spec), meta)
    unreadable = replace(spec, kafka=replace(spec.kafka, value_encoding="protobuf"))
    with pytest.raises(ValueError, match="value_encoding"):
        knobs.validate(spec.engine_block, unreadable, meta)


def test_every_encoding_the_spec_offers_states_its_format_and_its_options() -> None:
    """Format-specific options cannot be shared blindly: avro-confluent rejects
    avro.timestamp_mapping.legacy.
    """
    assert set(knobs._SOURCE_FORMATS) == model.VALUE_ENCODINGS
    assert set(knobs._FORMAT_OPTIONS) == model.VALUE_ENCODINGS


def test_a_confluent_run_reads_the_registry_format(meta: metadata.CorpusMetadata) -> None:
    """Flink avro-confluent uses legacy timestamp mapping without an override.
    TIMESTAMP(3) resolves against the corpus timestamp-millis long encoding.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink-confluent.yaml")
    site = _site(_registry())
    d = derive.derive(spec, site, stamp="20260909T000000Z", corpus_dir=meta.name + "-x")
    knobs.validate(spec.engine_block, spec, meta)
    sql = knobs.render_sql(spec, site, d, meta)
    assert "`event_time` TIMESTAMP(3) NOT NULL" in sql
    assert "'format' = 'avro-confluent'" in sql
    assert f"'avro-confluent.url' = '{REGISTRY_URL}'" in sql
    # The plain format's option, which `avro-confluent` refuses as an unknown
    # key of its own namespace.
    assert "avro.timestamp_mapping.legacy" not in sql
    # An open registry states no credential, rather than an empty one.
    assert "basic-auth" not in sql


def test_a_registry_behind_basic_auth_is_reached_with_the_site_credential(meta: metadata.CorpusMetadata) -> None:
    spec = _confluent(model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml"))
    site = _site(_registry("reader:secret"))
    d = derive.derive(spec, site, stamp="20260909T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, d, meta)
    assert "'avro-confluent.basic-auth.credentials-source' = 'USER_INFO'" in sql
    assert "'avro-confluent.basic-auth.user-info' = 'reader:secret'" in sql


def test_a_confluent_run_against_a_site_with_no_registry_is_refused(meta: metadata.CorpusMetadata) -> None:
    spec = _confluent(model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml"))
    d = derive.derive(spec, _site(), stamp="20260909T000000Z", corpus_dir=meta.name + "-x")
    with pytest.raises(ValueError, match="schema_registry"):
        knobs.render_sql(spec, _site(), d, meta)


def test_a_raw_avro_run_names_the_plain_format_alone(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260909T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, _site(), d, meta)
    assert "'format' = 'avro'" in sql and "avro-confluent" not in sql


def test_ddl_type_mapping(meta: metadata.CorpusMetadata) -> None:
    cols = {col.name: col for col in meta.columns}
    assert c.flink_type(cols["event_time"]) == "TIMESTAMP(3) NOT NULL"
    assert c.flink_type(cols["payload"]) == "BYTES NOT NULL"
    assert c.flink_type(cols["price"]) == "DOUBLE NOT NULL"


def test_a_column_named_after_a_keyword_is_quoted(meta: metadata.CorpusMetadata) -> None:
    """events200 has a `language` column; unquoted, Flink's parser rejects the source DDL."""
    column = replace(next(col for col in meta.columns if col.name == "price"), name="language")
    assert knobs._column_ddl(column).startswith("  `language` ")
    assert c.quoted_identifier("odd`name") == "`odd``name`"
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    insert = script.split_statements(knobs.render_sql(spec, _site(), d, meta))[2]
    assert "SELECT `id`, `partition_key`," in insert


def test_writers_spread_wider_than_readers(meta: metadata.CorpusMetadata) -> None:
    """The pinned Kafka connector has no source-parallelism option. Use the job
    default for readers and a sink hint only when writers need more parallelism.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    override = {"execution.checkpointing.min-pause": "9s"}
    wider = replace(spec, engine_block={**spec.engine_block, "taskmanagers": 4, "extra_flink_conf": override})
    knobs.validate(wider.engine_block, wider, meta)
    d = derive.derive(wider, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(wider, _site(), d, meta)
    assert "/*+ OPTIONS('distribution-mode' = 'hash', 'write-parallelism' = '16') */" in sql
    conf = knobs.render_conf(wider, d)
    assert conf["parallelism.default"] == "4" and conf["pipeline.max-parallelism"] == "64"
    # extra_flink_conf is applied last, so it overrides a setting named above.
    assert conf["execution.checkpointing.min-pause"] == "9s"

    # Equal reader and writer counts need no sink hint.
    level = replace(spec, engine_block={**spec.engine_block, "taskmanagers": 1})
    knobs.validate(level.engine_block, level, meta)
    assert "/*+ OPTIONS('distribution-mode' = 'hash') */" in knobs.render_sql(level, _site(), d, meta)


def test_render_refuses_a_catalog_flink_cannot_read(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql_catalog = replace(site, catalog_props={**site.catalog_props, "type": "sql"})
    with pytest.raises(ValueError, match="REST catalog"):
        knobs.render_sql(spec, sql_catalog, d, meta)
    no_uri = replace(site, catalog_props={"warehouse": "gs://warehouse"})
    with pytest.raises(ValueError, match="uri"):
        knobs.render_sql(spec, no_uri, d, meta)
    # The FileIO follows the site's warehouse, so a GCS site declares one.
    gcs = replace(
        site, warehouse="gs://warehouse", catalog_props={"uri": "http://c:8181", "warehouse": "gs://warehouse"}
    )
    assert "'io-impl' = 'org.apache.iceberg.gcp.gcs.GCSFileIO'" in knobs.render_sql(spec, gcs, d, meta)


def test_quotes_in_a_value_stay_inside_their_literal(meta: metadata.CorpusMetadata) -> None:
    site = replace(_site(), kafka_security={"sasl.password": "pa's's"})
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    assert "'properties.sasl.password' = 'pa''s''s'" in knobs.render_sql(spec, site, d, meta)


def test_rendered_sql_uses_the_statement_delimiter(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    statements = script.split_statements(knobs.render_sql(spec, _site(), d, meta))
    assert len(statements) == 3
    assert all(statement and not statement.endswith(";") for statement in statements)
    assert statements[0].startswith("CREATE TABLE kafka_source")
    assert statements[1].startswith("CREATE CATALOG ice")
    assert statements[2].startswith("INSERT INTO ice.")
    # A `;` inside a property value must not be read as a statement end.
    with_semicolons = replace(_site(), kafka_security={"sasl.jaas.config": "a=b;c=d;"})
    assert len(script.split_statements(knobs.render_sql(spec, with_semicolons, d, meta))) == 3


def test_the_external_example_is_what_the_renderer_produces(meta: metadata.CorpusMetadata) -> None:
    """Keep the manual walkthrough configuration aligned with the corpus schema
    and renderer, while preserving its run-name placeholders.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = model.load_site(ROOT / "deploy" / "compose" / "local" / "site.yaml")
    placeholders = derive.Derived(
        run_id="@RUN_ID@",
        topic="@TOPIC@",
        table="@NAMESPACE@.@TABLE@",
        run_root="@RUN_ROOT@",
        corpus_uri="@CORPUS_URI@",
    )
    example = ROOT / "docs" / "examples" / "external-flink"
    for name, content in knobs.render(spec, site, placeholders, meta).items():
        assert (example / name).read_text() == content, f"{example / name} is stale; re-render it"


def test_rendered_secret_references_follow_the_substitution_convention(
    meta: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    spec = replace(
        spec, engine_block={**spec.engine_block, "extra_flink_conf": {"custom.secret": "${env:IB_TEST_FLINK_SECRET}"}}
    )
    site = replace(
        _site(),
        kafka_security={"security.protocol": "SASL_SSL", "sasl.password": "${env:IB_TEST_FLINK_SECRET}"},
    )
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    rendered = knobs.render(spec, site, d, meta)

    # The renderer runs in the harness and writes what a reader may keep: the
    # variable's name, never its value.
    assert "'properties.sasl.password' = '${env:IB_TEST_FLINK_SECRET}'" in rendered[knobs.SQL_FILE]
    assert "custom.secret: ${env:IB_TEST_FLINK_SECRET}" in rendered[knobs.CONF_FILE]

    monkeypatch.setenv("IB_TEST_FLINK_SECRET", "s3cret")
    assert "'properties.sasl.password' = 's3cret'" in script.substitute_env(rendered[knobs.SQL_FILE])
    assert script.substitute_env("${env:IB_TEST_FLINK_SECRET}") == "s3cret"

    monkeypatch.delenv("IB_TEST_FLINK_SECRET")
    with pytest.raises(ValueError, match=r"\$\{env:IB_TEST_FLINK_SECRET\} is not set in the environment"):
        script.substitute_env(rendered[knobs.SQL_FILE])


# The AWS shape: an MSK broker reached by IAM, a Glue REST catalog whose
# warehouse is an account id rather than a URI, and a cluster to submit to.
_MSK_SECURITY = {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": "eu-west-1"}


def _cluster() -> model.KubernetesConfig:
    return model.KubernetesConfig(
        context="bench",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={"bench-pool": "engine"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


def _aws_site() -> model.SiteConfig:
    props = {
        "type": "rest",
        "uri": "https://glue.eu-west-1.amazonaws.com/iceberg",
        "warehouse": "123456789012",
        "rest.sigv4-enabled": "true",
        "rest.signing-name": "glue",
        "rest.signing-region": "eu-west-1",
    }
    return model.SiteConfig(
        "s3://bench-bucket/corpus",
        "s3://bench-bucket/runs",
        "s3://bench-bucket/warehouse",
        "b-1.example:9098",
        dict(_MSK_SECURITY),
        None,
        props,
        _cluster(),
        0.0,
        0.0,
    )


def test_msk_iam_replaces_the_signal_a_file_can_carry(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = replace(_aws_site(), kafka_security={**_MSK_SECURITY, "ssl.endpoint.identification.algorithm": "https"})
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, d, meta)
    assert "'properties.security.protocol' = 'SASL_SSL'" in sql
    assert "'properties.sasl.mechanism' = 'AWS_MSK_IAM'" in sql
    assert "'properties.sasl.jaas.config' = 'software.amazon.msk.auth.iam.IAMLoginModule required;'" in sql
    assert (
        "'properties.sasl.client.callback.handler.class' = 'software.amazon.msk.auth.iam.IAMClientCallbackHandler'"
        in sql
    )
    # Remove the harness-only region key; pod environment supplies the SDK region.
    assert "aws.region" not in sql
    # OAUTHBEARER is what the signal said, not what the Java client is told.
    assert "OAUTHBEARER" not in sql
    # A key that is not part of the signal still reaches the client.
    assert "'properties.ssl.endpoint.identification.algorithm' = 'https'" in sql
    # The login module's own `;` must not end the statement that carries it.
    assert len(script.split_statements(sql)) == 3
    # Do not emit duplicate WITH keys after translating properties.
    doubled = replace(_aws_site(), kafka_security={**_MSK_SECURITY, "sasl.jaas.config": "handmade;"})
    assert knobs.render_sql(spec, doubled, d, meta).count("'properties.sasl.jaas.config'") == 1


def test_a_site_that_is_not_on_msk_keeps_its_properties(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    for security in (
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"},
        {"security.protocol": "SASL_SSL", "sasl.mechanism": "SCRAM-SHA-512", "aws.region": "eu-west-1"},
    ):
        sql = knobs.render_sql(spec, replace(_site(), kafka_security=security), d, meta)
        assert "AWS_MSK_IAM" not in sql
        for key, value in security.items():
            assert f"'properties.{key}' = '{value}'" in sql


def test_a_glue_catalog_reaches_storage_through_the_sites_warehouse(meta: metadata.CorpusMetadata) -> None:
    """Glue uses an account id as its catalog warehouse. Derive FileIO from the
    site storage URI instead.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, d, meta)
    assert "'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO'" in sql
    assert "'warehouse' = '123456789012'" in sql
    for key in ("rest.sigv4-enabled", "rest.signing-name", "rest.signing-region"):
        assert f"'{key}' = '{site.catalog_props[key]}'" in sql
    # An unsupported storage scheme must not select a FileIO implementation.
    nowhere = replace(_aws_site(), warehouse="/mnt/warehouse")
    assert "io-impl" not in knobs.render_sql(spec, nowhere, d, meta)


def test_render_flinkdeployment(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="0.1.0-abc1234"))
    assert document == {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": "smoke-flink-20260908t000000z", "namespace": "ingest-bench"},
        "spec": {
            "image": "registry.example/ingest-bench/lakehouse-ingest-bench/flink:0.1.0-abc1234",
            "flinkVersion": "v2_2",
            "mode": "standalone",
            "serviceAccount": "ingest-bench-flink",
            "flinkConfiguration": {
                **knobs.render_conf(spec, d),
                "execution.checkpointing.dir": "s3://bench-bucket/runs/smoke-flink-20260908T000000Z/checkpoints",
            },
            "jobManager": {"resource": {"memory": "1024m", "cpu": 1.0}},
            "taskManager": {"resource": {"memory": "2048m", "cpu": 2.0}, "replicas": 2},
            "job": {
                "jarURI": "local:///opt/bench/sql-runner.jar",
                "entryClass": "org.ingestbench.flink.SqlRunner",
                "args": [
                    "--sql",
                    "/opt/bench/run/job.sql",
                    "--conf",
                    "/opt/bench/run/flink-conf.yaml",
                ],
                "parallelism": 4,
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": {
                "apiVersion": "v1",
                "kind": "Pod",
                "spec": {
                    "nodeSelector": {"bench-pool": "engine"},
                    "tolerations": [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
                    "volumes": [
                        {
                            "name": "job",
                            "configMap": {"name": "smoke-flink-20260908t000000z-flink-job"},
                        }
                    ],
                    "containers": [
                        {
                            "name": "flink-main-container",
                            "volumeMounts": [{"name": "job", "mountPath": "/opt/bench/run", "readOnly": True}],
                            "env": [
                                {"name": "AWS_REGION", "value": "eu-west-1"},
                                {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"},
                            ],
                        }
                    ],
                },
            },
        },
    }


def test_kubernetes_name_lowercases_a_run_id() -> None:
    assert knobs.kubernetes_name("smoke-flink-20260908T000000Z") == "smoke-flink-20260908t000000z"
    assert knobs.kubernetes_name("already-lower-1") == "already-lower-1"


def test_only_the_object_names_are_lowercased(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    conf = document["spec"]["flinkConfiguration"]
    assert conf["pipeline.name"] == d.run_id
    assert conf["execution.checkpointing.dir"].endswith(f"/{d.run_id}/checkpoints")
    assert f"'topic' = '{d.run_id}'" in knobs.render_sql(spec, site, d, meta)


def test_the_site_architecture_is_preserved_and_a_cluster_off_aws_names_no_region(
    meta: metadata.CorpusMetadata,
) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    cluster = replace(_cluster(), aws_region=None, node_selector={"kubernetes.io/arch": "arm64"}, tolerations=[])
    site = replace(_aws_site(), kubernetes=cluster)
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    pod = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))["spec"]["podTemplate"]
    assert pod["spec"]["nodeSelector"] == {"kubernetes.io/arch": "arm64"}
    assert pod["spec"]["tolerations"] == []
    assert "env" not in pod["spec"]["containers"][0]


def test_a_run_can_still_redirect_its_checkpoints(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    elsewhere = {"execution.checkpointing.dir": "s3://bench-bucket/checkpoints"}
    spec = replace(spec, engine_block={**spec.engine_block, "extra_flink_conf": elsewhere})
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    assert document["spec"]["flinkConfiguration"]["execution.checkpointing.dir"] == "s3://bench-bucket/checkpoints"


def test_the_crd_fields_follow_the_effective_conf(meta: metadata.CorpusMetadata) -> None:
    """The operator overrides flinkConfiguration with job.parallelism and resource
    memory fields. Those fields must reflect extra_flink_conf overrides.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    override = {
        "parallelism.default": "3",
        "jobmanager.memory.process.size": "3072m",
        "taskmanager.memory.process.size": "6144m",
    }
    spec = replace(spec, engine_block={**spec.engine_block, "extra_flink_conf": override})
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    deployment = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))["spec"]
    assert deployment["job"]["parallelism"] == 3
    assert deployment["jobManager"]["resource"] == {"memory": "3072m", "cpu": 1.0}
    assert deployment["taskManager"]["resource"] == {"memory": "6144m", "cpu": 2.0}
    # The operator restates neither of these, so they stay the knobs' to set.
    assert deployment["taskManager"]["replicas"] == 2
    assert deployment["flinkConfiguration"]["taskmanager.numberOfTaskSlots"] == "4"


def test_render_job_configmap(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_job_configmap(spec, site, d, meta))
    assert document["apiVersion"] == "v1" and document["kind"] == "ConfigMap"
    assert document["metadata"] == {"name": "smoke-flink-20260908t000000z-flink-job", "namespace": "ingest-bench"}
    files = knobs.render(spec, site, d, meta, image_tag="t")
    assert document["data"] == {knobs.SQL_FILE: files[knobs.SQL_FILE], knobs.CONF_FILE: files[knobs.CONF_FILE]}


def test_a_cluster_run_is_two_more_files_and_needs_an_image(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    assert set(knobs.render(spec, site, d, meta, image_tag="t")) == {
        knobs.SQL_FILE,
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.FLINKDEPLOYMENT_FILE,
        knobs.CONFIGMAP_FILE,
    }
    # Cluster rendering requires an image tag.
    with pytest.raises(ValueError, match="image_tag"):
        knobs.render(spec, site, d, meta)
    # No cluster, no Kubernetes documents — and nothing to refuse either.
    local = replace(_aws_site(), kubernetes=None)
    assert set(knobs.render(spec, local, d, meta)) == {knobs.SQL_FILE, knobs.CONF_FILE, knobs.ENV_FILE}
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_flinkdeployment(spec, local, d, meta, image_tag="t")
    with pytest.raises(ValueError, match="site.kubernetes"):
        knobs.render_job_configmap(spec, local, d, meta)


def test_the_fleet_reads_the_secret_the_site_names_and_the_script_keeps_the_reference(
    meta: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    spec = replace(spec, kafka=replace(spec.kafka, value_encoding="confluent"))
    registry = model.SchemaRegistryConfig(url=REGISTRY_URL, basic_auth_user_info="${env:IB_REGISTRY_AUTH}")
    site = replace(_aws_site(), schema_registry=registry, kubernetes=replace(_cluster(), secret_name="bench-env"))
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")

    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    container = document["spec"]["podTemplate"]["spec"]["containers"][0]
    assert container["envFrom"] == [{"secretRef": {"name": "bench-env"}}]

    sql = knobs.render_sql(spec, site, d, meta)
    assert "'avro-confluent.basic-auth.user-info' = '${env:IB_REGISTRY_AUTH}'" in sql
    assert "'avro-confluent.basic-auth.credentials-source' = 'USER_INFO'" in sql
    # Resolve references only in submitted text; keep the on-disk template unchanged.
    monkeypatch.setenv("IB_REGISTRY_AUTH", "svc:hunter2")
    assert "'avro-confluent.basic-auth.user-info' = 'svc:hunter2'" in script.substitute_env(sql)
    assert "svc:hunter2" not in sql


def test_a_cluster_that_names_no_secret_gives_its_container_no_env_from(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    document = yaml.safe_load(knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t"))
    assert "envFrom" not in document["spec"]["podTemplate"]["spec"]["containers"][0]


def test_kubernetes_refuses_an_env_placeholder_in_extra_flink_conf(meta: metadata.CorpusMetadata) -> None:
    """flinkConfiguration reaches the operator verbatim; nothing there resolves
    ${env:...}. Only the ConfigMap conf file SqlRunner reads does, so local Compose
    sites, which render no flinkConfiguration, may still keep the placeholder.
    """
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    spec = replace(
        spec, engine_block={**spec.engine_block, "extra_flink_conf": {"custom.secret": "${env:IB_TEST_FLINK_SECRET}"}}
    )
    site = _aws_site()
    d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    with pytest.raises(ValueError, match="custom.secret"):
        knobs.render_flinkdeployment(spec, site, d, meta, image_tag="t")
    local = replace(site, kubernetes=None)
    assert "custom.secret: ${env:IB_TEST_FLINK_SECRET}" in knobs.render(spec, local, d, meta)[knobs.CONF_FILE]


@pytest.mark.parametrize("warehouse", ["s3://warehouse", "gs://warehouse", "file:///warehouse"])
def test_explicit_file_io_overrides_the_storage_default(meta: metadata.CorpusMetadata, warehouse: str) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = replace(
        _site(), warehouse=warehouse, catalog_props={**_site().catalog_props, "io-impl": "example.CustomFileIO"}
    )
    derived = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, site, derived, meta)
    assert sql.count("'io-impl'") == 1
    assert "'io-impl' = 'example.CustomFileIO'" in sql


# ---------------------------------------------------------------------------
# Iceberg table maintenance inside the job
# ---------------------------------------------------------------------------


def _maint_spec() -> model.RunSpec:
    return model.load_run_spec(ROOT / "specs/smoke/smoke-flink-maint.yaml")


def test_maintenance_renders_the_post_commit_hints_and_the_v2_sink(meta: metadata.CorpusMetadata) -> None:
    spec = _maint_spec()
    knobs.validate(spec.engine_block, spec, meta)
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    sql = knobs.render_sql(spec, _site(), d, meta)
    hint = sql[sql.index("/*+ OPTIONS(") : sql.index("*/")]
    # Four writers against four readers: no write-parallelism hint, the runner's two slots are the maintenance group's.
    assert "'distribution-mode' = 'hash'" in hint and "write-parallelism" not in hint
    for key, value in (
        ("flink-maintenance.rewrite.enabled", "true"),
        ("flink-maintenance.rewrite.schedule.interval-second", "15"),
        ("flink-maintenance.rewrite.schedule.commit-count", "2147483647"),
        ("flink-maintenance.rewrite.schedule.data-file-count", "2147483647"),
        ("flink-maintenance.rewrite.schedule.data-file-size", "9223372036854775807"),
        ("flink-maintenance.parallelism", "2"),
        ("flink-maintenance.slot-sharing-group", "maintenance"),
        ("flink-maintenance.rate-limit-seconds", "5"),
        ("flink-maintenance.lock-check-delay-seconds", "2"),
        ("flink-maintenance.rewrite.min-input-files", "2"),
        ("flink-maintenance.rewrite.partial-progress.enabled", "false"),
    ):
        assert f"'{key}' = '{value}'" in hint, key
    conf = knobs.render_conf(spec, d)
    assert conf["table.exec.iceberg.use-v2-sink"] == "true"
    # The plain smoke spec keeps the v1 sink and states nothing about maintenance.
    plain = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    plain_conf = knobs.render_conf(plain, d)
    assert "table.exec.iceberg.use-v2-sink" not in plain_conf
    assert "flink-maintenance" not in knobs.render_sql(plain, _site(), d, meta)


def test_write_parallelism_sets_the_hint_instead_of_the_fleet_total(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    spec = replace(spec, engine_block={**spec.engine_block, "write_parallelism": 6})
    knobs.validate(spec.engine_block, spec, meta)
    d = derive.derive(spec, _site(), stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
    assert "'write-parallelism' = '6'" in knobs.render_sql(spec, _site(), d, meta)
    with pytest.raises(ValueError, match="write_parallelism 9 exceeds"):
        knobs.validate({**spec.engine_block, "write_parallelism": 9}, spec, meta)
    with pytest.raises(ValueError, match="write_parallelism must be at least 1"):
        knobs.read({**spec.engine_block, "write_parallelism": 0})


def test_maintenance_is_refused_when_it_cannot_run_as_declared(meta: metadata.CorpusMetadata) -> None:
    spec = _maint_spec()
    block = spec.engine_block
    maintenance = cast(dict[str, Any], block["maintenance"])
    # Inline compaction on Flink is this block, and this block is inline compaction.
    with pytest.raises(ValueError, match="spec.flink.maintenance configures"):
        knobs.validate({k: v for k, v in block.items() if k != "maintenance"}, spec, meta)
    with pytest.raises(ValueError, match="must be 'inline'"):
        knobs.validate(block, replace(spec, compaction=None), spec_meta := meta)
    del spec_meta
    # Writers plus runner must fit the fleet: 4 + 5 > 8 slots.
    with pytest.raises(ValueError, match="needs 5 slots"):
        knobs.validate({**block, "maintenance": {**maintenance, "rewrite_parallelism": 5}}, spec, meta)
    with pytest.raises(ValueError, match="requires the SinkV2 sink"):
        knobs.validate({**block, "extra_flink_conf": {"table.exec.iceberg.use-v2-sink": "false"}}, spec, meta)
    for bad, match in (
        (
            {**maintenance, "rewrite_options": {**maintenance["rewrite_options"], "target-file-size-bytes": "1"}},
            "target",
        ),
        ({**maintenance, "rewrite_options": {"schedule.commit-count": "1"}}, "rendered from the block"),
        ({**maintenance, "rate_limit_s": 30}, "exceeds schedule_interval_s"),
        ({**maintenance, "rewrite_parallelism": 0}, "at least 1"),
        ({**maintenance, "planner": "sort"}, "unknown keys"),
        ({k: v for k, v in maintenance.items() if k != "rewrite_parallelism"}, "must set rewrite_parallelism"),
        ({**maintenance, "rewrite_options": {"min-input-files": 2}}, "quote it"),
        # A misspelled option name is not a reserved key, so it must be checked against
        # the RewriteDataFiles builder's own option table instead of being dropped.
        ({**maintenance, "rewrite_options": {"min-inptu-files": "2"}}, "takes no setting for"),
        ({**maintenance, "rewrite_options": {"rewrite-all": "yes"}}, "must be true or false"),
        # The sidecar's name for the byte cap means nothing to inline maintenance.
        ({**maintenance, "rewrite_options": {"max-rewrite-bytes": "1048576"}}, "takes no setting for"),
        ({**maintenance, "rewrite_options": {"rewrite-job-order": "largest"}}, "must be one of"),
    ):
        with pytest.raises(ValueError, match=match):
            knobs.read({**block, "maintenance": bad})


def test_inline_maintenance_takes_icebergs_own_option_names() -> None:
    block = _maint_spec().engine_block
    maintenance = cast(dict[str, Any], block["maintenance"])
    options = {"max-bytes": "1048576", "rewrite-job-order": "bytes-desc"}
    read = knobs.read({**block, "maintenance": {**maintenance, "rewrite_options": options}})
    assert read.maintenance is not None
    hints = knobs._maintenance_hints(read.maintenance)
    assert ("flink-maintenance.rewrite.max-bytes", "1048576") in hints
    assert ("flink-maintenance.rewrite.rewrite-job-order", "bytes-desc") in hints


def test_maintenance_slot_check_uses_the_default_sharing_groups_demand(meta: metadata.CorpusMetadata) -> None:
    """Source, writer and committer share Flink's default slot-sharing group, whose
    demand is the greatest parallelism among them, not the writers' alone.
    """
    spec = _maint_spec()
    block = spec.engine_block
    maintenance = cast(dict[str, Any], block["maintenance"])
    # 8 taskmanager slots; a wide source (8) and a narrow writer (2) still make the
    # default group need 8, so 8 + 5 rewrite slots overflows the fleet even though
    # write_parallelism (2) + rewrite_parallelism (5) alone would fit.
    wide_source = {
        **block,
        "source_parallelism": 8,
        "write_parallelism": 2,
        "maintenance": {**maintenance, "rewrite_parallelism": 5},
    }
    with pytest.raises(ValueError, match="needs 5 slots"):
        knobs.validate(wide_source, spec, meta)


def test_maintenance_defaults_are_the_documented_ones() -> None:
    read = knobs.read(
        {
            "taskmanagers": 34,
            "slots": 4,
            "tm_cpu": 4,
            "tm_mem_mb": 16384,
            "checkpoint_interval": "60s",
            "distribution_mode": "hash",
            "write_parallelism": 80,
            "maintenance": {"rewrite_parallelism": 56},
        }
    )
    assert read.maintenance == knobs.Maintenance(56, "maintenance", 120, 60, 10, {})
    assert read.writer_parallelism() == 80 and read.v2_sink()


def _upsert(spec: model.RunSpec, format_version: int = 2) -> model.RunSpec:
    scoring = replace(spec.scoring, exactness=model.LATEST_PER_KEY)
    return replace(spec, scoring=scoring, table=replace(spec.table, format_version=format_version))


def test_an_upsert_run_writes_through_the_upsert_sink(meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(spec, _site(), stamp="20261006T000000Z", corpus_dir=meta.name + "-x")
    assert "'upsert-enabled' = 'true'" in knobs.render_sql(_upsert(spec), _site(), d, meta)
    assert "upsert-enabled" not in knobs.render_sql(spec, _site(), d, meta)


def test_a_v3_upsert_run_refuses_inline_maintenance(meta: metadata.CorpusMetadata) -> None:
    # The sink and the rewrite would both commit to the staging branch, and the converter cannot promote
    # a staging commit that removes data files.
    spec = _upsert(_maint_spec(), format_version=3)
    with pytest.raises(ValueError, match="staging"):
        knobs.validate(spec.engine_block, spec, meta)
    v2 = _upsert(_maint_spec())
    knobs.validate(v2.engine_block, v2, meta)


@pytest.mark.parametrize("mode", ["none", "range"])
def test_an_upsert_run_needs_hash_distribution(meta: metadata.CorpusMetadata, mode: str) -> None:
    spec = _upsert(model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml"))
    with pytest.raises(ValueError, match="one writer"):
        knobs.validate({**spec.engine_block, "distribution_mode": mode}, spec, meta)


def test_a_v3_upsert_run_converts_its_equality_deletes_in_the_job(meta: metadata.CorpusMetadata) -> None:
    base = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    d = derive.derive(base, _site(), stamp="20261008T000000Z", corpus_dir=meta.name + "-x")
    v3, v2 = _upsert(base, format_version=3), _upsert(base, format_version=2)
    knobs.validate(v3.engine_block, v3, meta)
    hint = "'flink-maintenance.convert-equality-deletes.enabled' = 'true'"
    sql = knobs.render_sql(v3, _site(), d, meta)
    assert hint in sql and hint not in knobs.render_sql(v2, _site(), d, meta)
    # The sink commits to a staging branch and the converter promotes data and deletion vectors to
    # main, so the converter never races the sink's own commits on the branch readers use.
    assert "'branch' = 'staging'" in sql
    assert "'flink-maintenance.convert-equality-deletes.target-branch' = 'main'" in sql
    # Iceberg runs a maintenance task at most once a minute by default and converts one staging commit
    # per run, so a sink committing every checkpoint would leave main further and further behind.
    assert "'flink-maintenance.rate-limit-seconds' = '1'" in sql
    # Iceberg runs the conversion after the SinkV2 sink's commits; its key index lives in RocksDB.
    conf = knobs.render_conf(v3, d)
    assert conf[knobs.V2_SINK_KEY] == "true" and conf["state.backend.type"] == "rocksdb"
    assert knobs.V2_SINK_KEY not in knobs.render_conf(v2, d)
    assert knobs.uses_v2_sink(knobs.read(v3.engine_block), v3)
    assert not knobs.uses_v2_sink(knobs.read(v2.engine_block), v2)
    assert knobs.REFERENCE_UPSERT_TABLE == {
        "format_version": 3,
        "properties": {"write.parquet.compression-codec": "zstd", "write.parquet.compression-level": "1"},
    }


def test_a_v3_upsert_deployment_quiets_the_key_index_and_checkpoints_incrementally(
    meta: metadata.CorpusMetadata,
) -> None:
    base = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _aws_site()
    d = derive.derive(base, site, stamp="20261008T000000Z", corpus_dir=meta.name + "-x")
    v3, v2 = _upsert(base, format_version=3), _upsert(base, format_version=2)
    # The converter's key index logs one INFO line per key it first sees: millions on a real seed.
    logs = yaml.safe_load(knobs.render_flinkdeployment(v3, site, d, meta, image_tag="t"))["spec"]["logConfiguration"]
    console = logs["log4j-console.properties"]
    assert "rootLogger.level = ${env:ROOT_LOG_LEVEL:-INFO}" in console
    assert f"logger.{knobs.PK_INDEX_LOGGER_ID}.name = {knobs.PK_INDEX_LOGGER}" in console
    assert f"logger.{knobs.PK_INDEX_LOGGER_ID}.level = WARN" in console
    v2_spec = yaml.safe_load(knobs.render_flinkdeployment(v2, site, d, meta, image_tag="t"))["spec"]
    assert "logConfiguration" not in v2_spec
    # Its RocksDB state is hundreds of MB, so checkpoints upload only what changed.
    assert knobs.render_conf(v3, d)["execution.checkpointing.incremental"] == "true"
    assert "execution.checkpointing.incremental" not in knobs.render_conf(v2, d)


@pytest.mark.parametrize("machine_type", [None, "m8g.4xlarge"])
@pytest.mark.parametrize("coordinator_type", [None, "m8g.2xlarge"])
def test_coordinator_machine_type_is_sizing_metadata(
    meta: metadata.CorpusMetadata, machine_type: str | None, coordinator_type: str | None
) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    block = dict(spec.engine_block)
    if machine_type is not None:
        block["machine_type"] = machine_type
    if coordinator_type is not None:
        block["jm_machine_type"] = coordinator_type
    pinned = replace(spec, engine_block=block)
    roles = {role.role: role for role in fleet.fleet(pinned)}
    assert roles["jobmanager"].machine_type == (coordinator_type or machine_type or model.MACHINE_TYPE_UNSPECIFIED)
    assert roles["taskmanager"].machine_type == (machine_type or model.MACHINE_TYPE_UNSPECIFIED)
    assert "jm_machine_type" in knobs.SIZING_KNOBS
    for site in (_site(), _aws_site()):
        d = derive.derive(spec, site, stamp="20260908T000000Z", corpus_dir=meta.name + "-x")
        assert knobs.render(pinned, site, d, meta, image_tag="t") == knobs.render(spec, site, d, meta, image_tag="t")


def test_coordinator_machine_type_requires_a_string() -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    with pytest.raises(ValueError, match="jm_machine_type must be a string"):
        knobs.read({**spec.engine_block, "jm_machine_type": 8})
