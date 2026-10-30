# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import cast

import pytest
import yaml

from ingest_bench import catalog as cat
from ingest_bench import kafka_admin, stage, uri
from ingest_bench.collect import leaderboard
from ingest_bench.collect.schema import ResultDocument
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import derive, engines, model

ROOT = Path(__file__).resolve().parents[1]


def test_smoke_specs_load() -> None:
    flink = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    assert flink.engine == "flink" and flink.table.managed_by == "harness" and flink.producer.speed == 1.0
    assert flink.scoring.warmup_s == 60 and flink.scoring.geometry_offsets_s == (600, 1200, 1800, 2700, 3600)
    assert flink.engine_block["taskmanagers"] == 2
    ext = model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml")
    assert ext.external is not None and ext.external.name == "your-engine" and ext.fleet[0].vcpu == 2


def test_partition_assignment_preserves_legacy_specs_and_validates_modes(tmp_path: Path) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["kafka"].pop("partition_assignment", None)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    assert model.load_run_spec(path).kafka.partition_assignment == "key"
    for mode in ("key", "round_robin"):
        raw["kafka"]["partition_assignment"] = mode
        path.write_text(yaml.safe_dump(raw))
        assert model.load_run_spec(path).kafka.partition_assignment == mode
    for invalid in ("random", 7, None):
        raw["kafka"]["partition_assignment"] = invalid
        path.write_text(yaml.safe_dump(raw))
        with pytest.raises(ValueError, match="partition_assignment"):
            model.load_run_spec(path)


@pytest.mark.parametrize("partitions", [0, -1, True, "4"])
def test_topic_partition_count_must_be_a_positive_integer(tmp_path: Path, partitions: object) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["kafka"]["partitions"] = partitions
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="spec.kafka.partitions"):
        model.load_run_spec(path)


@pytest.mark.parametrize(
    ("field", "value"),
    [("count", 0), ("count", -1), ("vcpu", 0), ("vcpu", -1.0), ("vcpu", float("nan")), ("vcpu", float("inf"))]
    + [("gib", 0), ("gib", -1.0), ("gib", float("nan")), ("gib", float("inf"))],
)
def test_fleet_roles_refuse_invalid_ranges(tmp_path: Path, field: str, value: object) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["fleet"][0][field] = value
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match=rf"spec\.fleet\[0\]\.{field}"):
        model.load_run_spec(path)


def test_spec_refusals(tmp_path: Path) -> None:
    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    mutations: tuple[tuple[Callable[[dict[str, object]], object], str], ...] = (
        (lambda d: d.pop("kafka"), "kafka"),
        (lambda d: d.__setitem__("colour", "blue"), "unknown"),
        (lambda d: d.pop("fleet"), "fleet"),
        (lambda d: d.__setitem__("name", "Bad Name"), "name"),
        (lambda d: d.__setitem__("engine", "unicorn"), "unicorn"),
    )
    for mutate, message in mutations:
        d = yaml.safe_load(yaml.safe_dump(base))
        mutate(d)
        p = tmp_path / "s.yaml"
        p.write_text(yaml.safe_dump(d))
        with pytest.raises(ValueError, match=message):
            model.load_run_spec(p)


def test_a_name_too_long_to_reach_a_job_is_refused_where_it_is_written(tmp_path: Path) -> None:
    """The name must leave room for the 17-character timestamp and drop-topic- prefix
    within a 63-character job-name label.
    """
    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    longest = "a" * 35
    base["name"] = longest
    path.write_text(yaml.safe_dump(base))
    spec = model.load_run_spec(path)
    run_id = derive.derive(spec, _bare_site(), stamp="20260908T120000Z", corpus_dir="smoke-1a2b3c4d").run_id
    assert len(f"drop-topic-{run_id}") == 63, run_id

    base["name"] = "a" * 36
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="spec.name names a topic"):
        model.load_run_spec(path)


def test_the_name_pattern_rejects_a_trailing_newline(tmp_path: Path) -> None:
    """A `$`-anchored `match` accepts a trailing newline; `fullmatch` does not."""
    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    base["name"] = "abc-run\n"
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="spec.name names a topic"):
        model.load_run_spec(path)


def test_the_value_encoding_defaults_to_avro_and_refuses_a_name_it_does_not_know(tmp_path: Path) -> None:
    assert model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml").kafka.value_encoding == "avro"
    confluent = model.load_run_spec(ROOT / "specs/smoke/smoke-external-confluent.yaml")
    assert confluent.kafka.value_encoding == "confluent"

    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    for encoding, message in (("protobuf", "value_encoding"), (7, "must be a string")):
        base["kafka"] = {**base["kafka"], "value_encoding": encoding}
        path.write_text(yaml.safe_dump(base))
        with pytest.raises(ValueError, match=message):
            model.load_run_spec(path)


def test_the_producer_compression_defaults_to_zstd_and_refuses_a_codec_it_does_not_know(tmp_path: Path) -> None:
    assert model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml").producer.compression == "zstd"

    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    for codec in sorted(model.COMPRESSIONS):
        base["producer"] = {"compression": codec}
        path.write_text(yaml.safe_dump(base))
        assert model.load_run_spec(path).producer.compression == codec

    base["producer"] = {"compression": "brotli"}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match=r"\['gzip', 'lz4', 'none', 'snappy', 'zstd'\], got 'brotli'"):
        model.load_run_spec(path)
    base["producer"] = {"compression": 7}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="must be a string"):
        model.load_run_spec(path)


def test_exactness_defaults_to_exactly_once_and_accepts_supported_modes(tmp_path: Path) -> None:
    assert model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml").scoring.exactness == model.EXACTLY_ONCE

    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    path = tmp_path / "s.yaml"
    for exactness in sorted(model.EXACTNESS_MODES):
        raw["scoring"] = {**raw["scoring"], "exactness": exactness}
        path.write_text(yaml.safe_dump(raw))
        assert model.load_run_spec(path).scoring.exactness == exactness


@pytest.mark.parametrize("exactness", ["AT_MOST_ONCE", "at_least_once", 1])
def test_exactness_refuses_unknown_or_non_string_modes(tmp_path: Path, exactness: object) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["scoring"] = {**raw["scoring"], "exactness": exactness}
    path = tmp_path / "s.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="spec.scoring.exactness"):
        model.load_run_spec(path)


def test_a_site_may_not_choose_the_wire_codec_with_a_client_property(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match=r"compression\.type.*producer\.compression"):
        model.load_site(_site_file(tmp_path, "file:///corpus", security={"compression.type": "gzip"}))
    with pytest.raises(ValueError, match=r"compression\.level.*producer\.compression"):
        model.load_site(_site_file(tmp_path, "file:///corpus", security={"compression.level": "9"}))
    loaded = model.load_site(_site_file(tmp_path, "file:///corpus", security={"security.protocol": "SASL_SSL"}))
    assert loaded.kafka_security == {"security.protocol": "SASL_SSL"}


def test_the_site_reads_a_schema_registry_and_keeps_its_reference(tmp_path: Path) -> None:
    """Resolving secrets at load time would expose them in rendered artifacts."""
    path = tmp_path / "site.yaml"

    def write(kafka: str) -> None:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            f"kafka: {kafka}\n"
            "catalog: {props: {type: sql}}\nkubernetes: {}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n"
        )

    write("{bootstrap_servers: 'localhost:9092'}")
    assert model.load_site(path).schema_registry is None

    write(
        "{bootstrap_servers: 'localhost:9092', schema_registry: "
        "{url: 'http://registry:8080/apis/ccompat/v7', basic_auth_user_info: '${env:IB_REGISTRY_AUTH}'}}"
    )
    assert model.load_site(path).schema_registry == model.SchemaRegistryConfig(
        url="http://registry:8080/apis/ccompat/v7", basic_auth_user_info="${env:IB_REGISTRY_AUTH}"
    )

    write("{bootstrap_servers: 'localhost:9092', schema_registry: {url: 'http://registry:8080'}}")
    registry = model.load_site(path).schema_registry
    assert registry is not None and registry.basic_auth_user_info is None

    for kafka, message in (
        ("{bootstrap_servers: 'x:9092', schema_registry: {basic_auth_user_info: 'a:b'}}", "must set url"),
        ("{bootstrap_servers: 'x:9092', schema_registry: {url: 'u', token: 't'}}", "unknown keys"),
        ("{bootstrap_servers: 'x:9092', schema_registry: {url: 'u', basic_auth_user_info: ''}}", "is empty"),
        ("{bootstrap_servers: 'x:9092', registry: {url: 'u'}}", "site.kafka has unknown keys"),
    ):
        write(kafka)
        with pytest.raises(ValueError, match=message):
            model.load_site(path)


def test_the_gate_keys_are_optional_and_typed(tmp_path: Path) -> None:
    base = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    shipped = model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml").scoring
    assert shipped.gate_adaptation_s is None and shipped.gate_window_s is None

    path = tmp_path / "s.yaml"
    base["scoring"] = {**base["scoring"], "gate_adaptation_s": 300, "gate_window_s": 90}
    path.write_text(yaml.safe_dump(base))
    scoring = model.load_run_spec(path).scoring
    assert scoring.gate_adaptation_s == 300 and scoring.gate_window_s == 90

    base["scoring"] = {**base["scoring"], "gate_window_s": "90"}
    path.write_text(yaml.safe_dump(base))
    with pytest.raises(ValueError, match="gate_window_s"):
        model.load_run_spec(path)


def test_site_refuses_placeholders(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="YOUR_"):
        model.load_site(ROOT / "site.example.yaml")
    good = tmp_path / "site.yaml"
    good.write_text(
        "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
        "kafka: {bootstrap_servers: 'localhost:9092', security: {}}\n"
        "catalog: {props: {type: sql, uri: 'sqlite:////tmp/x.db', warehouse: 'file:///tmp/w'}}\n"
        "kubernetes: {}\npricing: {vcpu_hour_usd: 0.03, gib_hour_usd: 0.004}\n"
    )
    site = model.load_site(good)
    assert site.kafka_bootstrap == "localhost:9092" and site.catalog_props["type"] == "sql"


CLUSTER: dict[str, object] = {
    "context": "my-cluster",
    "namespace": "ingest-bench",
    "harness_service_account": "ingest-bench-harness",
    "flink_service_account": "ingest-bench-flink",
    "registry": "registry.example/ingest-bench",
    "aws_region": "eu-west-1",
}


def _cluster_site(tmp_path: Path, cluster: dict[str, object]) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
        "kafka: {bootstrap_servers: 'localhost:9092'}\n"
        "catalog: {props: {type: sql}}\n"
        "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n" + yaml.safe_dump({"kubernetes": cluster})
    )
    return path


def test_an_empty_kubernetes_block_means_no_cluster(tmp_path: Path) -> None:
    assert model.load_site(_cluster_site(tmp_path, {})).kubernetes is None


def test_the_kubernetes_block_loads_a_cluster(tmp_path: Path) -> None:
    cluster: dict[str, object] = {
        **CLUSTER,
        "service_account_annotations": {"example.com/role": "arn"},
        "node_selector": {"kubernetes.io/arch": "amd64"},
        "tolerations": [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    }
    loaded = model.load_site(_cluster_site(tmp_path, cluster)).kubernetes
    assert loaded == model.KubernetesConfig(
        context="my-cluster",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={"example.com/role": "arn"},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={"kubernetes.io/arch": "amd64"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


@pytest.mark.parametrize("path", sorted((ROOT / "specs").glob("**/*.yaml")), ids=lambda path: path.name)
def test_every_shipped_run_spec_loads(path: Path) -> None:
    spec = model.load_run_spec(path)
    assert spec.name == path.stem
    assert spec.engine in {*engines.MANAGED, "external"}
    if spec.engine in engines.MANAGED:
        assert spec.engine_block, f"{path.name} names {spec.engine} and gives it no knobs"


@pytest.mark.parametrize("path", sorted((ROOT / "specs").glob("**/*.yaml")), ids=lambda path: path.name)
def test_every_shipped_run_spec_holds_knobs_its_engine_takes(path: Path, corpus: tuple[str, str]) -> None:
    """The spec loader passes engine blocks through; each engine must validate them."""
    spec = model.load_run_spec(path)
    if spec.engine not in engines.MANAGED:
        return
    corpus_root, corpus_dir = corpus
    engines.knobs_for(spec.engine).validate(spec.engine_block, spec, metadata.read(uri.join(corpus_root, corpus_dir)))


def test_a_full_scale_spec_ships_for_each_managed_engine() -> None:
    preset_name = "events-1mrps-skew"
    assert (ROOT / "workloads" / "presets" / f"{preset_name}.yaml").exists()
    full_scale = [
        spec
        for spec in (model.load_run_spec(path) for path in sorted((ROOT / "specs").glob("**/*.yaml")))
        if spec.corpus == preset_name and spec.producer.seconds is None
    ]
    shipped = {spec.engine for spec in full_scale}
    assert shipped >= set(engines.MANAGED), f"{preset_name} has no shipped spec for every managed engine"
    # Rows compare only within a results section, which fixes the Kafka workload. Producer
    # shard counts may vary between reference templates and measured tuned runs.
    by_partitions: dict[int, list[model.RunSpec]] = {}
    for spec in full_scale:
        by_partitions.setdefault(spec.kafka.partitions, []).append(spec)
    for partitions, group in by_partitions.items():
        offers = {(spec.producer.compression, spec.scoring.freshness_bound_s, spec.scoring.warmup_s) for spec in group}
        assert len(offers) == 1, f"{preset_name} specs at {partitions} partitions differ in offer or scoring: {offers}"


@pytest.mark.parametrize(
    ("name", "encoding"),
    [("aws-smoke-external", "avro"), ("aws-smoke-external-confluent", "confluent")],
    ids=["avro", "confluent"],
)
def test_a_cluster_external_spec_ships_for_each_wire_format(name: str, encoding: str) -> None:
    spec = model.load_run_spec(ROOT / "specs" / "cluster-smoke" / f"{name}.yaml")
    assert spec.is_external() and spec.external is not None
    assert spec.kafka.value_encoding == encoding
    # The offer crosses the cluster's network, as it does in both hour-long
    # cluster specs.
    assert spec.producer.compression == "lz4"
    assert spec.fleet and all(role.machine_type == "YOUR_MACHINE_TYPE" for role in spec.fleet)


def test_the_spark_account_is_the_one_setup_creates_unless_the_site_renames_it(tmp_path: Path) -> None:
    """The default keeps site configs written before Spark support compatible."""
    loaded = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert loaded is not None and loaded.spark_service_account == "ingest-bench-spark"
    renamed = model.load_site(_cluster_site(tmp_path, {**CLUSTER, "spark_service_account": "sparky"})).kubernetes
    assert renamed is not None and renamed.spark_service_account == "sparky"


def test_the_placement_keys_are_optional(tmp_path: Path) -> None:
    loaded = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert loaded is not None
    assert loaded.service_account_annotations == {} and loaded.node_selector == {} and loaded.tolerations == []


def test_the_aws_region_is_optional_but_never_empty(tmp_path: Path) -> None:
    elsewhere = {key: value for key, value in CLUSTER.items() if key != "aws_region"}
    loaded = model.load_site(_cluster_site(tmp_path, elsewhere)).kubernetes
    assert loaded is not None and loaded.aws_region is None
    on_aws = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert on_aws is not None and on_aws.aws_region == "eu-west-1"
    with pytest.raises(ValueError, match="aws_region is empty"):
        model.load_site(_cluster_site(tmp_path, {**CLUSTER, "aws_region": ""}))


def test_the_kubernetes_block_refuses_what_it_does_not_recognise(tmp_path: Path) -> None:
    for cluster, message in (
        ({**CLUSTER, "zone": "eu-west-1a"}, "unknown keys"),
        ({key: value for key, value in CLUSTER.items() if key != "namespace"}, "namespace"),
        ({**CLUSTER, "tolerations": {"key": "bench"}}, "list of mappings"),
        ({**CLUSTER, "node_selector": {"arch": 64}}, "node_selector.arch"),
    ):
        with pytest.raises(ValueError, match=message):
            model.load_site(_cluster_site(tmp_path, cluster))


def test_the_cluster_names_the_secret_its_pods_read(tmp_path: Path) -> None:
    absent = model.load_site(_cluster_site(tmp_path, dict(CLUSTER))).kubernetes
    assert absent is not None and absent.secret_name is None
    named = model.load_site(_cluster_site(tmp_path, {**CLUSTER, "secret_name": "ingest-bench-env"})).kubernetes
    assert named is not None and named.secret_name == "ingest-bench-env"
    with pytest.raises(ValueError, match="secret_name is empty"):
        model.load_site(_cluster_site(tmp_path, {**CLUSTER, "secret_name": ""}))


def test_a_cluster_site_refuses_a_credential_it_would_render_into_the_cluster(tmp_path: Path) -> None:
    path = tmp_path / "site.yaml"

    def written(kafka: str = "", catalog: str = "", cluster: dict[str, object] | None = None) -> Path:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            f"kafka: {{bootstrap_servers: 'localhost:9092', security: {{{kafka}}}}}\n"
            f"catalog: {{props: {{type: sql{catalog}}}}}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n"
            + yaml.safe_dump({"kubernetes": {} if cluster is None else cluster})
        )
        return path

    on_cluster = dict(CLUSTER)
    with pytest.raises(ValueError, match=r"sasl\.password.*\$\{env:NAME\}"):
        model.load_site(written(kafka="sasl.password: hunter2", cluster=on_cluster))
    with pytest.raises(ValueError, match=r"rest\.token.*secret_name"):
        model.load_site(written(catalog=", rest.token: abc123", cluster=on_cluster))
    with pytest.raises(ValueError, match=r"header\.Authorization.*secret_name"):
        model.load_site(written(catalog=", header.Authorization: 'Bearer abc123'", cluster=on_cluster))
    # A reference is what the cluster path takes, and it stays as it was written.
    referenced = model.load_site(
        written(kafka="sasl.password: '${env:IB_PASSWORD}'", cluster={**on_cluster, "secret_name": "env"})
    )
    assert referenced.kafka_security["sasl.password"] == "${env:IB_PASSWORD}"
    # The same literal, with no cluster to render it into.
    local = model.load_site(written(kafka="sasl.password: hunter2"))
    assert local.kafka_security["sasl.password"] == "hunter2"


def test_a_cluster_site_refuses_a_registry_credential_written_out(tmp_path: Path) -> None:
    path = tmp_path / "site.yaml"

    def written(user_info: str, cluster: dict[str, object]) -> Path:
        path.write_text(
            "corpus_root: /tmp/c\nruns_root: /tmp/r\nwarehouse: /tmp/w\n"
            "kafka:\n  bootstrap_servers: 'localhost:9092'\n  schema_registry:\n"
            "    url: http://registry:8080/apis/ccompat/v7\n"
            f"    basic_auth_user_info: '{user_info}'\n"
            "catalog: {props: {type: sql}}\n"
            "pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}\n" + yaml.safe_dump({"kubernetes": cluster})
        )
        return path

    with pytest.raises(ValueError, match=r"basic_auth_user_info.*\$\{env:NAME\}"):
        model.load_site(written("svc:hunter2", dict(CLUSTER)))
    loaded = model.load_site(written("${env:IB_REGISTRY_AUTH}", {**CLUSTER, "secret_name": "env"}))
    assert loaded.schema_registry is not None
    assert loaded.schema_registry.basic_auth_user_info == "${env:IB_REGISTRY_AUTH}"


def _bare_site() -> model.SiteConfig:
    """A site with nothing in it but roots, for deriving a run's names."""
    return model.SiteConfig(
        "s3://b/corpus", "s3://b/runs", "s3://b/wh", "k:9092", {}, None, {"uri": "u"}, None, 0.0, 0.0
    )


def test_derive_ids() -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    site = _bare_site()
    d = derive.derive(spec, site, stamp="20260908T120000Z", corpus_dir="smoke-1a2b3c4d")
    assert d.run_id == "smoke-flink-20260908T120000Z" and d.topic == d.run_id
    assert d.table == "ingest_bench.t_smoke_flink_20260908T120000Z"
    assert d.run_root == "s3://b/runs/smoke-flink-20260908T120000Z" and d.corpus_uri == "s3://b/corpus/smoke-1a2b3c4d"


class FakeAdmin:
    """A cluster admin that records what `stage` asked of it instead of reaching a broker."""

    def __init__(self, present: tuple[str, ...] = (), brokers: int = 1) -> None:
        self.present = set(present)
        self.brokers = brokers
        self.created: list[tuple[str, int, int, dict[str, str]]] = []
        self.deleted: list[str] = []
        # The client properties of every call, which is where a run's Kafka
        # credentials would appear.
        self.clients: list[dict[str, str]] = []

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool:
        self.clients.append(client)
        return name in self.present

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int:
        self.clients.append(client)
        return self.brokers

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None:
        self.clients.append(client)
        self.created.append((name, partitions, replication_factor, topic_config))
        self.present.add(name)

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None:
        self.clients.append(client)
        self.deleted.append(name)
        self.present.discard(name)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> tuple[str, str]:
    """A generated smoke corpus, as the root it lives under and its directory name."""
    p = preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("corpus"))
    generate.generate(p, out, seed=1, row_block=64)
    return out, preset.corpus_dir_name(p, row_block=64, seed=1)


def _site_file(
    tmp_path: Path,
    corpus_root: str,
    security: dict[str, str] | None = None,
    token: str = "shh",
    cluster: dict[str, object] | None = None,
    warehouse: str | None = None,
    registry: dict[str, str] | None = None,
) -> Path:
    path = tmp_path / "site.yaml"
    kafka: dict[str, object] = {"bootstrap_servers": "localhost:9092", "security": security or {}}
    if registry is not None:
        kafka["schema_registry"] = registry
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                # The storage warehouse, which is not the catalog's `warehouse`
                # property: a Glue catalog reads an account id there.
                "warehouse": warehouse or f"file://{tmp_path}/wh",
                "kafka": kafka,
                "catalog": {
                    "props": {
                        "type": "sql",
                        "uri": f"sqlite:///{tmp_path}/catalog.db",
                        "warehouse": f"file://{tmp_path}/wh",
                        "token": token,
                    }
                },
                "kubernetes": cluster or {},
                "pricing": {"vcpu_hour_usd": 0.03, "gib_hour_usd": 0.004},
            }
        )
    )
    return path


REGISTRY = {"url": "http://registry:8080/apis/ccompat/v7"}


@dataclass
class FakeRegistration:
    """What `stage` asked the registry for, instead of asking one."""

    calls: list[tuple[str, str | None, str, str]] = field(default_factory=list)
    schema_id: int = 7
    refuse: bool = False

    def register(self, url: str, basic_auth_user_info: str | None, subject: str, schema_text: str) -> int:
        self.calls.append((url, basic_auth_user_info, subject, schema_text))
        if self.refuse:
            raise ValueError("the registry refused the schema")
        return self.schema_id


def _confluent_spec(tmp_path: Path) -> Path:
    """The shipped external spec, offered in the Confluent wire format."""
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["kafka"] = {**raw["kafka"], "value_encoding": "confluent"}
    path = tmp_path / "confluent.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_stage_registers_the_corpus_schema_for_a_confluent_run(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, corpus_dir = corpus
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    admin = FakeAdmin()
    staged = stage.stage(
        _confluent_spec(tmp_path),
        _site_file(tmp_path, corpus_root, registry=REGISTRY),
        tmp_path / "runs",
        admin,
        stamp="20260909T100000Z",
    )
    schema_text = uri.read_text(uri.join(corpus_root, corpus_dir, "schema.avsc"))
    assert registry.calls == [(REGISTRY["url"], None, f"{staged.derived.topic}-value", schema_text)]
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["value_encoding"] == "confluent" and facts["schema_id"] == 7
    assert facts["schema_registry_url"] == REGISTRY["url"]
    assert facts["schema_subject"] == f"{staged.derived.topic}-value"
    assert admin.deleted == []


def test_stage_registers_nothing_for_a_raw_avro_run(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    staged = stage.stage(
        ROOT / "specs/smoke/smoke-external.yaml",
        _site_file(tmp_path, corpus_root, registry=REGISTRY),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260909T101000Z",
    )
    assert registry.calls == []
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["value_encoding"] == "avro"
    assert facts["schema_registry_url"] is None and facts["schema_subject"] is None and facts["schema_id"] is None


def test_the_facts_state_the_codec_a_consumer_has_to_decode(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    site = _site_file(tmp_path, corpus_root)

    staged = stage.stage(
        ROOT / "specs/smoke/smoke-external.yaml", site, tmp_path / "runs", FakeAdmin(), stamp="20260909T102000Z"
    )
    assert json.loads((staged.run_dir / "facts.json").read_text())["compression"] == "zstd"

    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["producer"] = {"compression": "lz4"}
    spec_path = tmp_path / "lz4.yaml"
    spec_path.write_text(yaml.safe_dump(raw))
    lz4 = stage.stage(spec_path, site, tmp_path / "runs", FakeAdmin(), stamp="20260909T102100Z")
    assert json.loads((lz4.run_dir / "facts.json").read_text())["compression"] == "lz4"


def test_stage_refuses_a_confluent_run_on_a_site_with_no_registry(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="kafka.schema_registry"):
        stage.stage(
            _confluent_spec(tmp_path),
            _site_file(tmp_path, corpus_root),
            tmp_path / "runs",
            admin,
            stamp="20260909T102000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_stage_drops_the_topic_when_the_registration_fails(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setattr(stage, "register_schema", FakeRegistration(refuse=True).register)
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="the registry refused"):
        stage.stage(
            _confluent_spec(tmp_path),
            _site_file(tmp_path, corpus_root, registry=REGISTRY),
            tmp_path / "runs",
            admin,
            stamp="20260909T103000Z",
        )
    assert admin.deleted == ["smoke-external-20260909T103000Z"]


def test_the_registrys_credential_is_a_reference_until_the_call(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setenv("IB_TEST_REGISTRY_AUTH", "key:s3cret")
    registry = FakeRegistration()
    monkeypatch.setattr(stage, "register_schema", registry.register)
    site_path = _site_file(
        tmp_path,
        corpus_root,
        registry={**REGISTRY, "basic_auth_user_info": "${env:IB_TEST_REGISTRY_AUTH}"},
    )
    staged = stage.stage(_confluent_spec(tmp_path), site_path, tmp_path / "runs", FakeAdmin(), stamp="20260909T104000Z")
    assert registry.calls[0][1] == "key:s3cret"
    loaded = model.load_site(site_path).schema_registry
    assert loaded is not None and loaded.basic_auth_user_info == "${env:IB_TEST_REGISTRY_AUTH}"
    assert "s3cret" not in (staged.run_dir / "facts.json").read_text()

    monkeypatch.delenv("IB_TEST_REGISTRY_AUTH")
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="IB_TEST_REGISTRY_AUTH"):
        stage.stage(_confluent_spec(tmp_path), site_path, tmp_path / "runs", admin, stamp="20260909T105000Z")
    assert admin.created == [] and admin.deleted == []


def test_resolve_corpus_dir(tmp_path: Path) -> None:
    for directory, name in (("smoke-aaa", "smoke"), ("other-bbb", "other")):
        uri.write_text(uri.join(str(tmp_path), directory, "corpus.json"), json.dumps({"name": name}))
    assert stage.resolve_corpus_dir(str(tmp_path), "smoke") == "smoke-aaa"
    with pytest.raises(ValueError, match="exactly one corpus"):
        stage.resolve_corpus_dir(str(tmp_path), "absent")
    uri.write_text(uri.join(str(tmp_path), "smoke-ccc", "corpus.json"), json.dumps({"name": "smoke"}))
    with pytest.raises(ValueError, match="smoke-aaa"):
        stage.resolve_corpus_dir(str(tmp_path), "smoke")


def test_resolve_standard_corpus_dir_accepts_legacy_name_but_rejects_ambiguity(tmp_path: Path) -> None:
    legacy = "events200-1mrps-skew-59c2b4a1"
    current = "events-1mrps-skew-59c2b4a1"
    uri.write_text(uri.join(str(tmp_path), legacy, "corpus.json"), json.dumps({"name": "events200-1mrps-skew"}))
    assert stage.resolve_corpus_dir(str(tmp_path), "events-1mrps-skew") == legacy
    uri.write_text(uri.join(str(tmp_path), current, "corpus.json"), json.dumps({"name": "events-1mrps-skew"}))
    with pytest.raises(ValueError, match="exactly one corpus"):
        stage.resolve_corpus_dir(str(tmp_path), "events-1mrps-skew")


def test_redact() -> None:
    redacted = stage.redact(
        {
            "uri": "http://catalog:8181",
            "token": "t",
            "s3.secret-access-key": "s",
            "credential": "c",
            "PASSWORD": "p",
        }
    )
    assert redacted == {
        "uri": "http://catalog:8181",
        "token": "<redacted>",
        "s3.secret-access-key": "<redacted>",
        "credential": "<redacted>",
        "PASSWORD": "<redacted>",
    }


def test_harness_table_properties() -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml")
    assert stage.harness_table_properties(spec) == {"format-version": "2"}
    codec = model.TableSpec("harness", "unpartitioned", {"write.parquet.compression-codec": "zstd"})
    assert stage.harness_table_properties(replace(spec, table=codec)) == {
        "write.parquet.compression-codec": "zstd",
        "format-version": "2",
    }


def test_stage_writes_the_run_directory(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, corpus_dir = corpus
    spec_path = ROOT / "specs/smoke/smoke-external.yaml"
    admin = FakeAdmin()
    site_path = _site_file(tmp_path, corpus_root)
    staged = stage.stage(spec_path, site_path, tmp_path / "runs", admin, stamp="20260908T120000Z")

    assert staged.derived.run_id == "smoke-external-20260908T120000Z"
    assert admin.created == [(staged.derived.topic, 4, 1, {"retention.ms": "172800000", "retention.bytes": "-1"})]
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert list(facts)[0] == "run_id" and facts["run_id"] == staged.derived.run_id
    assert facts["topic"] == staged.derived.topic and facts["bootstrap"] == "localhost:9092"
    assert facts["table"] == staged.derived.table == "ingest_bench.t_smoke_external_20260908T120000Z"
    assert facts["corpus_uri"] == uri.join(corpus_root, corpus_dir)
    assert facts["schema_avsc_uri"] == uri.join(corpus_root, corpus_dir, "schema.avsc")
    assert facts["key_column"] == "partition_key" and facts["partition"] == "identity(partition_key)"
    assert facts["partition_assignment"] == "key" and facts["kafka_partitions"] == 4
    assert facts["catalog_props"]["token"] == "<redacted>" and facts["catalog_props"]["type"] == "sql"
    assert facts["ddl"] is None and facts["epoch"] is None
    assert (staged.run_dir / "spec.yaml").read_text() == spec_path.read_text()
    assert (staged.run_dir / "timeline.log").read_text().splitlines()[0].endswith(" staged")
    assert stage.facts_lines(staged.facts)[0] == f"run_id: {staged.derived.run_id}"
    table = cat.open_catalog(model.load_site(site_path).catalog_props).load_table(str(facts["table"]))
    assert [field.name for field in table.schema().fields] == metadata.read(str(facts["corpus_uri"])).field_names()
    assert table.spec().fields[0].name == "partition_key"


def test_stage_gives_the_table_and_its_namespace_a_location_under_the_warehouse(
    tmp_path: Path, corpus: tuple[str, str]
) -> None:
    corpus_root, _ = corpus
    warehouse = f"file://{tmp_path}/explicit"
    site_path = _site_file(tmp_path, corpus_root, warehouse=warehouse)
    staged = stage.stage(
        ROOT / "specs/smoke/smoke-external.yaml", site_path, tmp_path / "runs", FakeAdmin(), stamp="20260908T190000Z"
    )
    namespace, name = cat.table_identifier(staged.derived.table)
    catalog = cat.open_catalog(model.load_site(site_path).catalog_props)
    assert catalog.load_table(staged.derived.table).location() == uri.join(warehouse, namespace, name)
    assert catalog.load_namespace_properties(namespace)["location"] == uri.join(warehouse, namespace)


def test_stage_refuses_a_cluster_run_with_no_image_tag(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="image-tag"):
        stage.stage(
            ROOT / "specs/smoke/smoke-flink.yaml",
            # Cluster sites require environment references for credentials.
            _site_file(tmp_path, corpus_root, cluster=dict(CLUSTER), token="${env:IB_TEST_CATALOG_TOKEN}"),
            tmp_path / "runs",
            admin,
            stamp="20260908T191000Z",
        )
    assert admin.created == [] and admin.deleted == []


def _rest_cluster_site(tmp_path: Path, corpus_root: str) -> Path:
    """A site on a cluster with a REST catalog, which is what a Flink run reads through."""
    path = tmp_path / "site-cluster.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                "warehouse": f"file://{tmp_path}/wh",
                "kafka": {"bootstrap_servers": "localhost:9092"},
                "catalog": {"props": {"type": "rest", "uri": "http://catalog:8181", "warehouse": "123456789012"}},
                "kubernetes": dict(CLUSTER),
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    return path


def _engine_owned_flink_spec(tmp_path: Path) -> Path:
    """The shipped Flink spec with the table left to the engine."""
    spec = yaml.safe_load((ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["table"] = {**spec["table"], "managed_by": "engine"}
    path = tmp_path / "flink-cluster.yaml"
    path.write_text(yaml.safe_dump(spec))
    return path


def test_stage_on_a_cluster_renders_its_documents_and_uploads_the_run_directory(
    tmp_path: Path, corpus: tuple[str, str]
) -> None:
    corpus_root, _ = corpus
    uploads = f"file://{tmp_path}/uploads"
    staged = stage.stage(
        _engine_owned_flink_spec(tmp_path),
        _rest_cluster_site(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T192000Z",
        image_tag="abc1234",
        upload_prefix=uploads,
    )
    written = sorted(path.name for path in staged.run_dir.iterdir())
    assert written == [
        "facts.json",
        "flink-conf.yaml",
        "flink-job-configmap.yaml",
        "flink.env",
        "flinkdeployment.yaml",
        "job.sql",
        "spec.yaml",
        "timeline.log",
    ]
    published = uri.join(uploads, staged.derived.run_id, "stage")
    assert sorted(uri.listdir(published)) == written
    assert uri.read_text(uri.join(published, "facts.json")) == (staged.run_dir / "facts.json").read_text()
    assert ":abc1234" in (staged.run_dir / "flinkdeployment.yaml").read_text()


def _named_engine_owned_spec(tmp_path: Path, engine: str, name: str) -> Path:
    """The shipped spec for ``engine``, renamed, with the table left to the engine."""
    spec = yaml.safe_load((ROOT / "specs" / "smoke" / f"smoke-{engine}.yaml").read_text())
    spec["name"] = name
    spec["table"] = {**spec["table"], "managed_by": "engine"}
    path = tmp_path / f"{name}.yaml"
    path.write_text(yaml.safe_dump(spec))
    return path


def test_stage_refuses_a_name_its_engines_operator_would_reject(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """Flink caps deployment names at 45 characters, including the run timestamp."""
    corpus_root, _ = corpus
    over = "flink-run-with-a-29-char-name"
    assert len(over) == 29
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="45"):
        stage.stage(
            _named_engine_owned_spec(tmp_path, "flink", over),
            _rest_cluster_site(tmp_path, corpus_root),
            tmp_path / "runs",
            admin,
            stamp="20260908T193000Z",
            image_tag="abc1234",
        )
    assert admin.created == [] and admin.deleted == []


def test_stage_takes_the_longest_name_its_engines_operator_accepts(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    within = "flink-run-with-a-28char-name"
    assert len(within) == 28
    staged = stage.stage(
        _named_engine_owned_spec(tmp_path, "flink", within),
        _rest_cluster_site(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T193100Z",
        image_tag="abc1234",
    )
    assert len(staged.derived.run_id) == 45


def test_stage_takes_any_legal_name_for_an_engine_that_declares_no_limit(
    tmp_path: Path, corpus: tuple[str, str]
) -> None:
    corpus_root, _ = corpus
    longest = "spark-run-with-the-longest-name-yet"
    assert len(longest) == 35, "the longest name spec.name's own pattern accepts"
    staged = stage.stage(
        _named_engine_owned_spec(tmp_path, "spark", longest),
        _rest_cluster_site(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T193200Z",
        image_tag="abc1234",
    )
    assert staged.derived.run_id == f"{longest}-20260908T193200Z"


def test_replication_factor_follows_the_broker_count(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    site_path = _site_file(tmp_path, corpus_root)
    for index, (brokers, expected) in enumerate(((1, 1), (2, 2), (5, 3))):
        admin = FakeAdmin(brokers=brokers)
        staged = stage.stage(
            ROOT / "specs/smoke/smoke-external.yaml",
            site_path,
            tmp_path / "runs",
            admin,
            stamp=f"20260908T1600{index:02d}Z",
        )
        assert admin.created == [(staged.derived.topic, 4, expected, dict(kafka_admin.DEFAULT_TOPIC_CONFIG))]


def test_stage_drops_the_topic_when_staging_fails(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    spec_path = ROOT / "specs/smoke/smoke-external.yaml"
    site_path = _site_file(tmp_path, corpus_root)
    stage.stage(spec_path, site_path, tmp_path / "runs", FakeAdmin(), stamp="20260908T130000Z")
    # A fresh admin reports no topic, so staging reaches the table it already created.
    retry = FakeAdmin()
    with pytest.raises(Exception, match="t_smoke_external_20260908T130000Z"):
        stage.stage(spec_path, site_path, tmp_path / "runs", retry, stamp="20260908T130000Z")
    assert retry.deleted == ["smoke-external-20260908T130000Z"]


def test_stage_refuses_an_existing_topic(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    admin = FakeAdmin(present=("smoke-external-20260908T140000Z",))
    with pytest.raises(ValueError, match="already exists"):
        stage.stage(
            ROOT / "specs/smoke/smoke-external.yaml",
            _site_file(tmp_path, corpus_root),
            tmp_path / "runs",
            admin,
            stamp="20260908T140000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_a_secret_is_named_in_the_facts_and_resolved_at_the_cluster(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setenv("IB_TEST_KAFKA_PASSWORD", "s3cret")
    monkeypatch.setenv("IB_TEST_CATALOG_TOKEN", "t0ken")
    site_path = _site_file(
        tmp_path,
        corpus_root,
        security={"security.protocol": "SASL_SSL", "sasl.password": "${env:IB_TEST_KAFKA_PASSWORD}"},
        token="${env:IB_TEST_CATALOG_TOKEN}",
    )
    admin = FakeAdmin()
    staged = stage.stage(
        ROOT / "specs/smoke/smoke-external.yaml", site_path, tmp_path / "runs", admin, stamp="20260908T170000Z"
    )
    # Persist references; resolve values only at credential use sites.
    assert model.load_site(site_path).kafka_security["sasl.password"] == "${env:IB_TEST_KAFKA_PASSWORD}"
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["catalog_props"]["token"] == "${env:IB_TEST_CATALOG_TOKEN}"
    # The value exists only in the calls that need it.
    assert admin.clients and all(
        client == {"security.protocol": "SASL_SSL", "sasl.password": "s3cret"} for client in admin.clients
    )


def test_a_site_spelling_the_mechanism_in_the_plural_is_refused(tmp_path: Path, corpus: tuple[str, str]) -> None:
    """librdkafka accepts both spellings, but the harness reads only sasl.mechanism
    when translating MSK IAM configuration.
    """
    corpus_root, _ = corpus
    site_path = _site_file(
        tmp_path,
        corpus_root,
        security={"security.protocol": "SASL_SSL", "sasl.mechanisms": "OAUTHBEARER", "aws.region": "eu-west-1"},
    )
    with pytest.raises(ValueError, match="'sasl.mechanism'"):
        model.load_site(site_path)


def test_a_literal_credential_is_still_redacted(tmp_path: Path, corpus: tuple[str, str]) -> None:
    corpus_root, _ = corpus
    staged = stage.stage(
        ROOT / "specs/smoke/smoke-external.yaml",
        _site_file(tmp_path, corpus_root),
        tmp_path / "runs",
        FakeAdmin(),
        stamp="20260908T171000Z",
    )
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["catalog_props"]["token"] == "<redacted>"


def test_stage_refuses_an_unset_reference_before_it_creates_anything(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_root, _ = corpus
    monkeypatch.delenv("IB_TEST_ABSENT", raising=False)
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="IB_TEST_ABSENT"):
        stage.stage(
            ROOT / "specs/smoke/smoke-external.yaml",
            _site_file(tmp_path, corpus_root, security={"sasl.password": "${env:IB_TEST_ABSENT}"}),
            tmp_path / "runs",
            admin,
            stamp="20260908T180000Z",
        )
    assert admin.created == [] and admin.deleted == []


def test_knobs_for_refuses_an_unregistered_engine() -> None:
    with pytest.raises(ValueError, match="unicorn"):
        engines.knobs_for("unicorn")
    assert engines.MANAGED["flink"] == "engines.flink.knobs"


def test_main_prints_the_run_id_first(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    corpus_root, _ = corpus
    monkeypatch.setattr(stage, "ClusterAdmin", FakeAdmin)
    code = stage.main(
        [
            "--spec",
            str(ROOT / "specs/smoke/smoke-external.yaml"),
            "--site",
            str(_site_file(tmp_path, corpus_root)),
            "--runs-dir",
            str(tmp_path / "runs"),
            "--stamp",
            "20260908T150000Z",
        ]
    )
    lines = capsys.readouterr().out.splitlines()
    assert code == 0 and lines[0] == "run_id: smoke-external-20260908T150000Z"
    assert lines[-1] == "Start your engine now; run launch when it is consuming."
    assert "catalog_props: {" in "\n".join(lines) and "epoch: null" in lines


@pytest.mark.parametrize("deployment", [None, "managed", "in-cluster", "external"])
def test_site_loads_kafka_deployment_without_changing_client_properties(tmp_path: Path, deployment: str | None) -> None:
    path = _cluster_site(tmp_path, {})
    raw = yaml.safe_load(path.read_text())
    if deployment is not None:
        raw["kafka"]["deployment"] = deployment
    path.write_text(yaml.safe_dump(raw))
    site = model.load_site(path)
    assert site.kafka_deployment == deployment
    assert site.kafka_bootstrap == "localhost:9092"
    assert site.kafka_security == {}


@pytest.mark.parametrize("deployment", ["", "automatic", True])
def test_site_rejects_unknown_kafka_deployments(tmp_path: Path, deployment: object) -> None:
    path = _cluster_site(tmp_path, {})
    raw = yaml.safe_load(path.read_text())
    raw["kafka"]["deployment"] = deployment
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="site.kafka.deployment"):
        model.load_site(path)


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [("producer", "speed", value) for value in (0, -1.0, float("nan"), float("inf"))]
    + [
        ("producer", "shards", 0),
        ("producer", "seconds", 0),
        ("producer", "behind_max_ms", -1),
        ("scoring", "geometry_offsets_s", [600, 300]),
        ("scoring", "geometry_offsets_s", [-10]),
        ("scoring", "geometry_offsets_s", [300, 300]),
        ("scoring", "freshness_bound_s", 0),
        ("scoring", "freshness_bound_s", float("nan")),
        ("scoring", "freshness_bound_s", float("inf")),
        ("scoring", "warmup_s", -5),
        ("scoring", "gate_adaptation_s", -1),
        ("scoring", "gate_window_s", 0),
    ],
)
def test_run_spec_refuses_invalid_ranges(tmp_path: Path, section: str, key: str, value: object) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw.setdefault(section, {})[key] = value
    path = tmp_path / "invalid.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match=rf"spec\.{section}\.{key}"):
        model.load_run_spec(path)


def test_run_spec_accepts_zero_delays_and_increasing_offsets(tmp_path: Path) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw.setdefault("producer", {}).update(speed=0.1, seconds=1, shards=1, behind_max_ms=0)
    raw["scoring"].update(warmup_s=0, gate_adaptation_s=0, gate_window_s=1, geometry_offsets_s=[0, 1])
    path = tmp_path / "valid.yaml"
    path.write_text(yaml.safe_dump(raw))
    spec = model.load_run_spec(path)
    assert spec.scoring.geometry_offsets_s == (0, 1)
    assert spec.producer.speed == 0.1


@pytest.mark.parametrize("engine", ["spark", "flink", "custom"])
def test_managed_oauth_is_refused_before_staging_resources(
    tmp_path: Path, engine: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    spec_path = ROOT / "specs" / "smoke" / f"smoke-{engine}.yaml"
    if engine == "custom":
        monkeypatch.setitem(engines.MANAGED, "custom", engines.MANAGED["flink"])
        raw = yaml.safe_load((ROOT / "specs/smoke/smoke-flink.yaml").read_text())
        raw["engine"] = "custom"
        raw["custom"] = raw.pop("flink")
        spec_path = tmp_path / "custom.yaml"
        spec_path.write_text(yaml.safe_dump(raw))
    site_path = _site_file(
        tmp_path,
        "file:///missing-corpus",
        security={"sasl.mechanism": "OAUTHBEARER", "aws.region": "eu-west-1", "sasl.oauthbearer.method": "oidc"},
    )
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="managed Java engines do not support"):
        stage.stage(spec_path, site_path, tmp_path / "runs", admin)
    assert not admin.clients and not admin.created
    assert not (tmp_path / "runs").exists()


def test_saturation_is_opt_in_and_requires_a_measurement_window(tmp_path: Path) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    path = tmp_path / "mode.yaml"
    path.write_text(yaml.safe_dump(raw))
    assert model.load_run_spec(path).scoring.mode == "sustainable"
    for scoring, message in (
        ({"mode": "saturation"}, "measurement_s"),
        ({"mode": "saturation", "measurement_s": 0}, "measurement_s"),
        ({"mode": "saturation", "measurement_s": True}, "measurement_s"),
        ({"measurement_s": 60}, "measurement_s"),
        ({"mode": "typo"}, "mode"),
    ):
        raw["scoring"] = scoring
        path.write_text(yaml.safe_dump(raw))
        with pytest.raises(ValueError, match=message):
            model.load_run_spec(path)
    raw["scoring"] = {"mode": "saturation", "measurement_s": 60, "warmup_s": 10}
    path.write_text(yaml.safe_dump(raw))
    configured = model.load_run_spec(path).scoring
    assert configured.mode == "saturation" and configured.measurement_s == 60


@pytest.mark.parametrize("speed, seconds, measurement", [(1, None, 1), (2, None, 1), (0.5, 1, 2)])
def test_saturation_window_must_fit_before_staging_resources(
    tmp_path: Path,
    corpus: tuple[str, str],
    speed: float,
    seconds: int | None,
    measurement: int,
) -> None:
    corpus_root, _ = corpus
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["producer"] = {"speed": speed}
    if seconds is not None:
        raw["producer"]["seconds"] = seconds
    raw["scoring"] = {"mode": "saturation", "measurement_s": measurement, "warmup_s": 0}
    path = tmp_path / "saturation.yaml"
    path.write_text(yaml.safe_dump(raw))
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="selected replay duration"):
        stage.stage(path, _site_file(tmp_path, corpus_root), tmp_path / "runs", admin)
    assert not admin.clients and not admin.created


def test_saturation_window_must_precede_the_last_scheduled_batch(
    tmp_path: Path,
    corpus: tuple[str, str],
) -> None:
    corpus_root, _ = corpus
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    # This corpus has one batch at offset zero; slowing it does not add later offers.
    raw["producer"] = {"speed": 0.01}
    raw["scoring"] = {"mode": "saturation", "warmup_s": 0, "measurement_s": 60}
    path = tmp_path / "saturation.yaml"
    path.write_text(yaml.safe_dump(raw))
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="last scheduled batch"):
        stage.stage(path, _site_file(tmp_path, corpus_root), tmp_path / "runs", admin)
    assert not admin.clients and not admin.created


# ---------------------------------------------------------------------------
# Leaderboard tracks and tiers
# ---------------------------------------------------------------------------

WORKLOADS = ROOT / "workloads"
REFERENCE_SPEC = ROOT / "specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-ref.yaml"


@pytest.mark.parametrize("path", sorted((ROOT / "specs" / "tracks").glob("*/*.yaml")), ids=lambda path: path.stem)
def test_every_track_spec_conforms_to_the_track_it_is_filed_under(path: Path) -> None:
    spec = model.load_run_spec(path)
    track = model.load_track(path.parent.name, WORKLOADS)
    assert spec.corpus == track.corpus and spec.kafka == track.kafka
    assert (spec.producer.compression, spec.producer.behind_max_ms) == (track.compression, track.behind_max_ms)
    duration_s = preset.load_preset(track.corpus, workloads_dir=WORKLOADS).duration_s
    assert (spec.producer.speed, spec.scoring.mode) == (1.0, model.SUSTAINABLE)
    assert spec.producer.seconds is None or spec.producer.seconds >= duration_s
    assert (spec.scoring.freshness_bound_s, spec.scoring.exactness) == (track.freshness_bound_s, track.exactness)
    assert spec.scoring.warmup_s <= track.max_warmup_s and spec.table.partition == track.partition


def test_shipped_tracks_name_shipped_presets_and_differ_in_what_they_fix() -> None:
    tracks = [model.load_track(name, WORKLOADS) for name in model.track_names(WORKLOADS)]
    assert tracks
    assert all((WORKLOADS / "presets" / f"{track.corpus}.yaml").is_file() for track in tracks)
    settings = [track.settings for track in tracks]
    assert all(settings.count(entry) == 1 for entry in settings)


def _changed(tmp_path: Path, change: Callable[[dict[str, object]], None], base: Path = REFERENCE_SPEC) -> Path:
    raw = yaml.safe_load(base.read_text())
    change(raw)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_a_track_spec_takes_its_workload_and_scoring_from_the_track() -> None:
    spec = model.load_run_spec(REFERENCE_SPEC)
    assert spec.track == "events-1mrps-skew" and spec.corpus == "events-1mrps-skew"
    assert spec.kafka.partitions == 192 and spec.kafka.value_encoding == "confluent"
    assert spec.producer.compression == "lz4" and spec.producer.behind_max_ms == 10000
    assert spec.scoring.freshness_bound_s == 90 and spec.scoring.warmup_s == 300


def test_a_track_spec_may_repeat_a_fixed_setting_but_not_change_it(tmp_path: Path) -> None:
    assert model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(kafka={"partitions": 192}))).kafka.partitions
    with pytest.raises(ValueError, match="fixes it at 192"):
        model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(kafka={"partitions": 32})))
    with pytest.raises(ValueError, match="fixes it at 'events-1mrps-skew'"):
        model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(corpus="smoke")))


@pytest.mark.parametrize("name", ["events-50mbs-upsert-orders", "events-1mrps-skew", "events-1mrps-uniform"])
def test_track_bounds_p95_at_90_s_and_max_at_120_s(name: str) -> None:
    track = model.load_track(name, ROOT / "workloads")
    assert (track.freshness_bound_s, track.freshness_max_bound_s) == (90, 120)


def test_a_spec_takes_its_max_bound_from_the_track_and_must_cover_its_p95_bound(tmp_path: Path) -> None:
    upsert_specs = ROOT / "specs/tracks/events-50mbs-upsert-orders"
    upsert = model.load_run_spec(upsert_specs / "aws-50mbs-upsert-orders-flink-ref.yaml")
    assert upsert.scoring.max_bound_s == 120
    loose = _changed(
        tmp_path,
        lambda raw: raw.update(scoring={"freshness_bound_s": 60, "freshness_max_bound_s": 30}),
        base=ROOT / "specs/smoke/smoke-flink.yaml",
    )
    with pytest.raises(ValueError, match="at least spec.scoring.freshness_bound_s"):
        model.load_run_spec(loose)


def test_a_track_caps_warmup_and_allows_a_longer_scored_window(tmp_path: Path) -> None:
    longer_window = _changed(tmp_path, lambda raw: raw.update(scoring={"warmup_s": 120}))
    assert model.load_run_spec(longer_window).scoring.warmup_s == 120
    with pytest.raises(ValueError, match="ceiling"):
        model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(scoring={"warmup_s": 301})))


@pytest.mark.parametrize(
    ("section", "key", "value"),
    [("producer", "speed", 2.0), ("scoring", "mode", "saturation"), ("scoring", "measurement_s", 60)],
)
def test_a_track_spec_refuses_settings_that_change_the_offer_or_mode(
    tmp_path: Path, section: str, key: str, value: object
) -> None:
    def change(raw: dict[str, object]) -> None:
        raw[section] = {**cast(dict[str, object], raw.get(section) or {}), key: value}

    with pytest.raises(ValueError, match="would change what track"):
        model.load_run_spec(_changed(tmp_path, change))


def test_an_unshipped_track_is_refused(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="is not shipped"):
        model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(track="events-9000mbs-skew")))


def test_only_an_external_engine_declares_its_tier(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="derived from the knobs"):
        model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(tier="reference")))
    external = ROOT / "specs/smoke/smoke-external.yaml"
    assert model.load_run_spec(_changed(tmp_path, lambda raw: raw.update(tier="reference"), external)).declared_tier

    def tuned_table(raw: dict[str, object]) -> None:
        raw.update(tier="reference", table={"properties": {"write.target-file-size-bytes": "1"}})

    with pytest.raises(ValueError, match="allows no table properties"):
        model.load_run_spec(_changed(tmp_path, tuned_table, external))


def _tier(engine: str, spec: dict[str, object]) -> str:
    return leaderboard.tier(cast(ResultDocument, {"run": {"engine": engine, "spec": spec}}))


def test_the_reference_tier_allows_only_sizing_changes() -> None:
    raw = yaml.safe_load(REFERENCE_SPEC.read_text())
    flink = cast(dict[str, object], raw["flink"])
    assert _tier("flink", raw) == model.TIER_REFERENCE
    assert _tier("flink", {**raw, "flink": {**flink, "taskmanagers": 9, "tm_mem_mb": 24576}}) == model.TIER_REFERENCE
    for change in (
        {"flink": {**flink, "distribution_mode": "range"}},
        {"flink": {**flink, "extra_flink_conf": {"table.exec.iceberg.use-v2-sink": "false"}}},
        {"table": {"properties": {"write.target-file-size-bytes": "536870912"}}},
        {"compaction": {"driver": "inline"}},
    ):
        assert _tier("flink", {**raw, **change}) == model.TIER_TUNED, change


def test_every_shipped_engine_has_a_reference_spec_at_the_reference_tier() -> None:
    shipped = {
        spec.engine
        for path in sorted((ROOT / "specs" / "tracks").glob("*/*.yaml"))
        if (spec := model.load_run_spec(path)).engine in engines.MANAGED
        and _tier(spec.engine, yaml.safe_load(path.read_text())) == model.TIER_REFERENCE
    }
    assert shipped == set(engines.MANAGED)


def test_every_reference_track_spec_derives_the_reference_tier() -> None:
    paths = sorted((ROOT / "specs" / "tracks").glob("*/*-ref.yaml"))
    tuned = [
        path.name
        for path in paths
        if (spec := model.load_run_spec(path)).engine in engines.MANAGED
        # A run records its spec with the track filled in, which is where an upsert track's exactness comes from.
        and _tier(spec.engine, model.expand_track(yaml.safe_load(path.read_text()), WORKLOADS)) != model.TIER_REFERENCE
    ]
    assert paths and tuned == []


def test_an_external_engine_is_tuned_unless_it_declares_the_reference_tier() -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    assert _tier("external", raw) == model.TIER_TUNED
    assert _tier("external", {**raw, "tier": "reference"}) == model.TIER_REFERENCE


def test_staging_records_the_commit_an_image_was_built_from(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(stage.HARNESS_COMMIT_ENV, "a" * 40)
    assert stage.harness_commit() == "a" * 40


def test_staging_reads_the_commit_from_a_checkout(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(stage.HARNESS_COMMIT_ENV, raising=False)
    commit = stage.harness_commit()
    assert commit is not None and re.fullmatch(r"[0-9a-f]{40}(-dirty)?", commit)


TINY_TRACK = """
corpus: smoke
kafka: {partitions: 4, key: partition_key, partition_assignment: key, value_encoding: avro}
producer: {compression: zstd, behind_max_ms: 5000}
scoring: {freshness_bound_s: 60, exactness: EXACTLY_ONCE, max_warmup_s: 60}
table: {partition: identity(partition_key)}
"""


def test_staging_a_track_spec_writes_it_self_contained(
    tmp_path: Path, corpus: tuple[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    """The staged copy carries the track's settings, so the result that embeds it outlives a track edit."""
    workloads = tmp_path / "workloads"
    (workloads / "tracks").mkdir(parents=True)
    (workloads / "tracks" / "tiny.yaml").write_text(TINY_TRACK)
    monkeypatch.setenv("INGEST_BENCH_WORKLOADS", str(workloads))
    monkeypatch.setenv(stage.HARNESS_COMMIT_ENV, "b" * 40)

    def to_track(raw: dict[str, object]) -> None:
        for key in ("corpus", "kafka", "scoring"):
            del raw[key]
        raw["track"] = "tiny"

    spec_path = _changed(tmp_path, to_track, ROOT / "specs/smoke/smoke-external.yaml")
    staged = stage.stage(
        spec_path, _site_file(tmp_path, corpus[0]), tmp_path / "runs", FakeAdmin(), stamp="20260925T000000Z"
    )
    written = yaml.safe_load((staged.run_dir / "spec.yaml").read_text())
    assert written["track"] == "tiny" and written["corpus"] == "smoke" and written["kafka"]["partitions"] == 4
    assert written["scoring"]["warmup_s"] == 60 and written["producer"]["compression"] == "zstd"
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["track"] == "tiny" and facts["harness_commit"] == "b" * 40
    assert model.expand_track(written, workloads) == written


def test_a_spec_refuses_credentials_in_table_properties(tmp_path: Path) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["table"]["properties"] = {"s3.secret-access-key": "literal"}
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    with pytest.raises(ValueError, match="credential properties"):
        model.load_run_spec(path)


def _external_spec(tmp_path: Path, **sections: dict[str, object]) -> Path:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    for section, values in sections.items():
        raw.setdefault(section, {}).update(values)
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_latest_per_key_is_a_sustainable_exactness(tmp_path: Path) -> None:
    spec = model.load_run_spec(_external_spec(tmp_path, scoring={"exactness": "LATEST_PER_KEY"}))
    assert spec.scoring.exactness == model.LATEST_PER_KEY
    saturation = {"exactness": "LATEST_PER_KEY", "mode": "saturation", "measurement_s": 60}
    with pytest.raises(ValueError, match="sustainable mode only"):
        model.load_run_spec(_external_spec(tmp_path, scoring=saturation))


def test_table_format_version_defaults_to_two(tmp_path: Path) -> None:
    assert model.load_run_spec(_external_spec(tmp_path)).table.format_version == 2
    assert model.load_run_spec(_external_spec(tmp_path, table={"format_version": 3})).table.format_version == 3
    with pytest.raises(ValueError, match="format_version"):
        model.load_run_spec(_external_spec(tmp_path, table={"format_version": 4}))


def test_a_track_may_pin_sample_offsets(tmp_path: Path) -> None:
    workloads = tmp_path / "workloads"
    (workloads / "tracks").mkdir(parents=True)
    track = yaml.safe_load((ROOT / "workloads/tracks/events-1mrps-skew.yaml").read_text())
    track["scoring"]["geometry_offsets_s"] = [613, 1427]
    (workloads / "tracks" / "pinned.yaml").write_text(yaml.safe_dump(track))
    assert model.load_track("pinned", workloads).geometry_offsets_s == (613, 1427)
    assert model.load_track("events-1mrps-skew", ROOT / "workloads").geometry_offsets_s is None
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    for section in ("kafka", "producer", "scoring", "table"):
        raw.pop(section, None)
    raw.pop("corpus", None)
    raw["track"] = "pinned"
    assert model.parse_run_spec(raw, workloads=workloads).scoring.geometry_offsets_s == (613, 1427)
    raw["scoring"] = {"geometry_offsets_s": [600]}
    with pytest.raises(ValueError, match="geometry_offsets_s"):
        model.parse_run_spec(raw, workloads=workloads)


def test_an_upsert_track_pins_its_contract(tmp_path: Path) -> None:
    workloads = tmp_path / "workloads"
    (workloads / "tracks").mkdir(parents=True)
    (workloads / "tracks" / "upsert.yaml").write_text(
        "corpus: smoke-upsert\n"
        "kafka: {partitions: 16, key: event_id, partition_assignment: corpus, value_encoding: avro}\n"
        "producer: {compression: zstd, behind_max_ms: 5000}\n"
        "scoring: {freshness_bound_s: 60, exactness: LATEST_PER_KEY, max_warmup_s: 60, geometry_offsets_s: [60, 120]}\n"
        "table: {partition: identity(partition_key)}\n"
    )
    track = model.load_track("upsert", workloads)
    assert track.exactness == model.LATEST_PER_KEY and track.kafka.partition_assignment == "corpus"
    assert track.geometry_offsets_s == (60, 120) and track.kafka.partitions == 16
    corpus = preset.load_preset(track.corpus, workloads_dir=ROOT / "workloads")
    assert corpus.upsert is not None and corpus.upsert.kafka_partitions == track.kafka.partitions
