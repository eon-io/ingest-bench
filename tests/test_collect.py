# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from ingest_bench import stage
from ingest_bench.collect import cli, run_json
from ingest_bench.collect.paths import result_relative_path
from ingest_bench.collect.redact import redact_catalog_props, redact_document, redact_props, redact_uri
from ingest_bench.collect.schema import SCHEMA_VERSION, parse_result
from ingest_bench.collect.validate import validate
from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.specs import model
from tests.collect_fixtures import (
    BUCKET,
    CORPUS,
    EPOCH_MS,
    EXTERNAL_SPEC,
    FLINK_SPEC,
    LAST_ACK_MS,
    SMOKE_CORPUS_HASH,
    build_document,
    build_exactness,
    build_geometry,
    build_run_dir,
    fixture_specs,
    fixture_workloads,
    write_site,
)
from tests.test_public_surface import tracked_files


def _strings(value: object) -> list[str]:
    """Every string anywhere in a document, keys included."""
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        found: list[str] = []
        for key, entry in value.items():
            found.append(str(key))
            found.extend(_strings(entry))
        return found
    if isinstance(value, list):
        return [text for entry in value for text in _strings(entry)]
    return []


@pytest.fixture(autouse=True)
def _fixture_tracks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Publish against the fixture tracks, since the fixture runs use a smoke corpus."""
    monkeypatch.setenv("INGEST_BENCH_WORKLOADS", str(fixture_workloads(tmp_path)))


# ---------------------------------------------------------------------------
# Redaction
# ---------------------------------------------------------------------------


def _site_config(tmp_path: Path) -> model.SiteConfig:
    return model.load_site(write_site(tmp_path))


def test_collect_import_does_not_load_staging_or_kafka() -> None:
    subprocess.run(
        [
            sys.executable,
            "-c",
            "import sys; import ingest_bench.collect.cli; "
            "assert 'ingest_bench.stage' not in sys.modules; "
            "assert 'confluent_kafka' not in sys.modules",
        ],
        check=True,
    )


def test_redact_uri_takes_the_longest_matching_root(tmp_path: Path) -> None:
    path = tmp_path / "nested.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                # The corpus root is the bucket itself, so the runs prefix sits
                # under it and only the longer root describes a run's path.
                "corpus_root": BUCKET,
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse/",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri(f"{BUCKET}/runs/r1/scores/summary.json", site) == "<runs_root>/r1/scores/summary.json"
    assert redact_uri(f"{BUCKET}/warehouse/ns/t/data/a.parquet", site) == "<warehouse>/ns/t/data/a.parquet"
    assert redact_uri(f"{BUCKET}/corpus/smoke-1", site) == "<corpus_root>/corpus/smoke-1"
    assert redact_uri(BUCKET, site) == "<corpus_root>"
    assert redact_uri("s3://another-bucket/runs/r1", site) == "s3://another-bucket/runs/r1"


def test_redact_uri_covers_the_registry_and_a_glue_warehouse(tmp_path: Path) -> None:
    path = tmp_path / "glue.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                # Glue's warehouse property is the account id itself, twelve digits
                # that sit under no object-store root.
                "catalog": {"props": {"uri": "https://glue.invalid/iceberg", "warehouse": "123456789012"}},
                "kubernetes": {
                    "context": "c",
                    "namespace": "n",
                    "harness_service_account": "h",
                    "flink_service_account": "f",
                    "registry": "123456789012.dkr.ecr.us-east-2.amazonaws.com",
                },
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    registry = "123456789012.dkr.ecr.us-east-2.amazonaws.com"
    # A bare account ID is an ID, and the catalog's own warehouse property keeps its role.
    assert redact_uri("123456789012", site) == "<account_id>"
    assert redact_uri("123456789012/ns/t", site) == "<catalog_warehouse>/ns/t"
    assert redact_uri(f"{registry}/bench/flink:abc", site) == "<registry>/bench/flink:abc"
    assert redact_uri(f"{registry}/bench/flink@sha256:0", site) == "<registry>/bench/flink@sha256:0"
    # A catalog warehouse that is the site warehouse keeps the site's own name.
    same = _site_config(tmp_path)
    assert redact_uri(f"{BUCKET}/warehouse/ns/t", same) == "<warehouse>/ns/t"


def _site_with_catalog_warehouse(tmp_path: Path, warehouse: str) -> model.SiteConfig:
    path = tmp_path / "catalog-warehouse.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid", "warehouse": warehouse}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    return model.load_site(path)


def test_a_warehouse_name_is_redacted_only_where_the_catalog_properties_hold_it(tmp_path: Path) -> None:
    """A Lakekeeper warehouse name can begin a label key, which is not the warehouse."""
    site = _site_with_catalog_warehouse(tmp_path, "lakehouse-ingest-bench")
    spec = {"node_selector": {"lakehouse-ingest-bench/role": "maintenance"}, "note": "lakehouse-ingest-bench"}
    assert redact_document(spec, site) == spec
    published = redact_catalog_props({"uri": "https://catalog.invalid", "warehouse": "lakehouse-ingest-bench"}, site)
    assert published == {"uri": "https://catalog.invalid", "warehouse": "<catalog_warehouse>"}


@pytest.mark.parametrize("name", ["role", "writer"])
def test_a_warehouse_name_leaves_keys_and_values_that_equal_it(tmp_path: Path, name: str) -> None:
    site = _site_with_catalog_warehouse(tmp_path, name)
    fleet = {"fleet": [{"role": "writer", "count": 1}]}
    assert redact_document(fleet, site) == fleet


@pytest.mark.parametrize(
    ("warehouse", "child"),
    [
        ("s3://another-bucket/lake", "s3://another-bucket/lake/ns/t"),
        ("file:/tmp/private-lake", "file:/tmp/private-lake/ns/t"),
        ("private-lake/catalog", "private-lake/catalog/ns/t"),
        ("123456789012", "123456789012/ns/t"),
    ],
    ids=["uri", "file", "relative", "account-id"],
)
def test_a_location_or_account_warehouse_is_still_redacted_as_a_prefix(
    tmp_path: Path, warehouse: str, child: str
) -> None:
    site = _site_with_catalog_warehouse(tmp_path, warehouse)
    # Publication redacts the catalog properties, then the whole document.
    published = redact_document(redact_catalog_props({"warehouse": warehouse}, site), site)
    assert published == {"warehouse": "<catalog_warehouse>"}
    assert redact_uri(child, site) == "<catalog_warehouse>/ns/t"


@pytest.mark.parametrize("catalog_type", ["REST", "Rest", ""])
def test_a_rest_catalog_type_is_read_as_pyiceberg_reads_it(tmp_path: Path, catalog_type: str) -> None:
    path = tmp_path / "typed.yaml"
    props = {"type": catalog_type, "uri": "https://catalog.invalid", "warehouse": "role"}
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": props},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_document({"fleet": [{"role": "writer"}]}, site) == {"fleet": [{"role": "writer"}]}
    assert redact_catalog_props(props, site)["warehouse"] == "<catalog_warehouse>"


def test_a_non_rest_catalog_warehouse_is_a_location_even_as_one_segment(tmp_path: Path) -> None:
    """A SQL catalog's warehouse is a directory, so a bare word there is a path, not a name."""
    path = tmp_path / "sql.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"type": "sql", "uri": "sqlite:///catalog.db", "warehouse": "private-lake"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    location = "private-lake/bench/events/metadata/v1.json"
    assert redact_uri(location, site) == "<catalog_warehouse>/bench/events/metadata/v1.json"


def _glue_site(tmp_path: Path) -> model.SiteConfig:
    path = tmp_path / "glue-notes.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {
                    "props": {
                        "type": "glue",
                        "glue.id": "123456789012",
                        "warehouse": f"{BUCKET}/warehouse",
                        "uri": "https://reader:not-a-secret@catalog.invalid/iceberg",
                    }
                },
                "kubernetes": {
                    "context": "c",
                    "namespace": "n",
                    "harness_service_account": "h",
                    "flink_service_account": "f",
                    "registry": "images.invalid/team",
                },
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    return model.load_site(path)


def test_private_site_values_are_redacted_inside_prose(tmp_path: Path) -> None:
    """Notes quote roots and IDs mid-sentence, where a whole-value match would miss them."""
    site = _glue_site(tmp_path)
    notes = f"Glue catalog 123456789012 wrote {BUCKET}/warehouse/ns/t; image images.invalid/team/writer:abc."
    assert redact_uri(notes, site) == ("Glue catalog <account_id> wrote <warehouse>/ns/t; image <registry>/writer:abc.")
    assert redact_document({"glue.id": "123456789012"}, site) == {"glue.id": "<account_id>"}


def test_credentials_in_a_uri_are_redacted(tmp_path: Path) -> None:
    site = _glue_site(tmp_path)
    assert redact_uri("https://reader:not-a-secret@catalog.invalid/iceberg", site) == (
        "https://<redacted>@catalog.invalid/iceberg"
    )


@pytest.mark.parametrize("password", ["pa'ssword", "p@ss", "p%2Fss"])
def test_a_uri_password_is_redacted_whatever_it_holds(tmp_path: Path, password: str) -> None:
    site = _glue_site(tmp_path)
    redacted = redact_uri(f"https://reader:{password}@catalog.invalid/iceberg", site)
    assert password not in redacted and "reader" not in redacted


def test_a_twelve_digit_setting_without_account_context_is_left_alone(tmp_path: Path) -> None:
    path = tmp_path / "timestamp.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid", "header.X-Bench-Timestamp": "202609251234"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri("run staged at 202609251234", site) == "run staged at 202609251234"


def test_a_short_root_never_rewrites_a_longer_roots_placeholder(tmp_path: Path) -> None:
    """A one-segment SQL warehouse is a path head, not every occurrence of the word."""
    path = tmp_path / "sql-nested.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": "/tmp/private-lake/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"type": "sql", "uri": "sqlite:///c.db", "warehouse": "warehouse"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri("/tmp/private-lake/warehouse/bench/events", site) == "<warehouse>/bench/events"
    assert redact_uri("warehouse/bench/events", site) == "<catalog_warehouse>/bench/events"
    assert redact_document({"warehouse": "x"}, site) == {"warehouse": "x"}


def test_a_bare_registry_host_and_a_catalog_bucket_id_are_redacted_in_prose(tmp_path: Path) -> None:
    path = tmp_path / "bare-registry.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://glue.invalid", "warehouse": "s3://some-bucket-123456789012/c"}},
                "kubernetes": {
                    "context": "c",
                    "namespace": "n",
                    "harness_service_account": "h",
                    "flink_service_account": "f",
                    "registry": "images.invalid",
                },
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri("Registry: images.invalid, account 123456789012.", site) == (
        "Registry: <registry>, account <account_id>."
    )
    assert redact_uri("Images came from images.invalid.", site) == "Images came from <registry>."
    assert redact_uri("images.invalid.example is another host", site) == "images.invalid.example is another host"


def test_redact_uri_does_not_match_a_sibling_prefix(tmp_path: Path) -> None:
    site = _site_config(tmp_path)
    assert redact_uri(f"{BUCKET}/corpus-archive/smoke-1", site) == f"{BUCKET}/corpus-archive/smoke-1"
    assert redact_uri(f"{BUCKET}/corpus/smoke-1", site) == "<corpus_root>/smoke-1"


def test_redact_uri_ignores_a_root_a_site_left_empty(tmp_path: Path) -> None:
    path = tmp_path / "empty-root.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": "",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {"bootstrap_servers": "broker.invalid:9092"},
                "catalog": {"props": {"uri": "https://catalog.invalid"}},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    site = model.load_site(path)
    assert redact_uri("/opt/bench/run/job.sql", site) == "/opt/bench/run/job.sql"
    assert redact_uri(f"{BUCKET}/warehouse/ns/t", site) == "<warehouse>/ns/t"


def test_redact_props_replaces_literal_credentials_and_leaves_uris_to_document_redaction(tmp_path: Path) -> None:
    props = {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": f"{BUCKET}/warehouse",
        "token": "a-literal-catalog-token",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }
    # Engine facts retain site roots; publication redacts them across the whole
    # document through redact_document.
    assert redact_props(props) == {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": f"{BUCKET}/warehouse",
        "token": "<redacted>",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }


def test_redact_document_reaches_nested_strings_and_keys(tmp_path: Path) -> None:
    site = _site_config(tmp_path)
    document = {
        "paths": [f"{BUCKET}/warehouse/a.parquet", {"inner": f"{BUCKET}/runs/r1"}],
        f"{BUCKET}/corpus/x": 3,
        "count": 7,
        "flag": True,
        "nothing": None,
    }
    assert redact_document(document, site) == {
        "paths": ["<warehouse>/a.parquet", {"inner": "<runs_root>/r1"}],
        "<corpus_root>/x": 3,
        "count": 7,
        "flag": True,
        "nothing": None,
    }


def test_stage_redact_still_answers_for_the_secret_rule() -> None:
    assert stage.redact({"token": "t", "uri": "http://catalog:8181"}) == {
        "token": "<redacted>",
        "uri": "http://catalog:8181",
    }


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------


def test_run_json_publishes_the_current_schema_version_and_no_site(tmp_path: Path) -> None:
    site_path = write_site(tmp_path)
    document = build_document(build_run_dir(tmp_path), site_path)
    assert document["schema_version"] == SCHEMA_VERSION
    assert document["collected_at"] == "2026-09-20T11:30:00Z"
    assert document["harness_version"] == "9.9.9"
    strings = _strings(document)
    assert not [text for text in strings if "a-bucket" in text]
    assert "the-broker-password" not in strings
    assert "a-literal-catalog-token" not in strings
    assert "<warehouse>/ingest_bench/t_collect/data/11-0.parquet" in strings
    assert f"<corpus_root>/smoke-{SMOKE_CORPUS_HASH}" in strings
    # The security block is never read, so neither its keys nor its values reach
    # the document under any name.
    assert "sasl.password" not in strings


def test_run_json_redacts_the_catalog_properties(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["catalog_props"] == {
        "uri": "https://catalog.invalid/iceberg",
        "warehouse": "<warehouse>",
        "token": "<redacted>",
        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
    }
    assert run["variant"] == "hash"
    assert run["topic"] == "collect-flink-20260909T052508Z"
    assert run["table"] == "ingest_bench.t_collect_flink_20260909T052508Z"
    assert run["epoch_ms"] == EPOCH_MS
    assert run["corpus_hash"] == SMOKE_CORPUS_HASH
    assert run["corpus_seed"] == 1
    assert run["corpus_generator_version"] == GENERATOR_VERSION
    assert run["corpus_row_block"] == 1024
    # Copied verbatim rather than re-serialised from the loaded dataclass.
    assert run["spec"] == yaml.safe_load(FLINK_SPEC)


def test_collect_preserves_an_older_summary_without_inventing_generator_inputs(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    summary_path = run_dir / "scores" / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["corpus_hash"] = "e13842f9"
    for field in ("corpus_seed", "corpus_generator_version", "corpus_row_block"):
        del summary[field]
    summary_path.write_text(json.dumps(summary))
    corpus_path = run_dir / "corpus.json"
    metadata = json.loads(corpus_path.read_text())
    metadata["corpus_hash"] = "e13842f9"
    for field in ("seed", "generator_version", "row_block"):
        del metadata[field]
    corpus_path.write_text(json.dumps(metadata))
    document = parse_result(build_document(run_dir, write_site(tmp_path)))
    assert document["schema_version"] == SCHEMA_VERSION
    assert document["run"]["corpus_hash"] == "e13842f9"
    assert document["run"]["corpus_seed"] is None
    assert document["run"]["corpus_generator_version"] is None
    assert document["run"]["corpus_row_block"] is None


def test_run_json_copies_the_published_corpus_fields(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["corpus"] == CORPUS
    assert run["corpus_hash"] == run["corpus"]["hash"]


def test_run_json_keeps_a_run_collectable_without_corpus_metadata(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    (run_dir / "corpus.json").unlink()
    document = build_document(run_dir, write_site(tmp_path))
    run = document["run"]
    missing = document["missing"]
    assert isinstance(run, dict) and isinstance(missing, list)
    assert run["corpus"] is None
    assert run["corpus_hash"] == SMOKE_CORPUS_HASH
    assert "corpus.json" in missing


@pytest.mark.parametrize("encoding", ["confluent", "json"])
def test_run_json_states_the_wire_format_the_offer_used(tmp_path: Path, encoding: str) -> None:
    site_path = write_site(tmp_path)
    default = build_document(build_run_dir(tmp_path / "default"), site_path)
    run = default["run"]
    assert isinstance(run, dict)
    assert not {"compression", "value_encoding", "partition_assignment"} & run.keys()
    assert run["kafka"] == {
        "partitions": 4,
        "key": "user_id",
        "partition_assignment": "key",
        "value_encoding": "avro",
        "compression": "zstd",
    }

    raw = yaml.safe_load(FLINK_SPEC)
    raw["producer"] = {**raw["producer"], "compression": "lz4"}
    framed = build_document(
        build_run_dir(
            tmp_path / "framed",
            spec=yaml.safe_dump(raw),
            value_encoding=encoding,
            compression="lz4",
        ),
        site_path,
    )
    framed_run = framed["run"]
    assert isinstance(framed_run, dict)
    kafka = framed_run["kafka"]
    assert isinstance(kafka, dict)
    assert kafka["compression"] == "lz4"
    assert kafka["value_encoding"] == encoding


def test_run_json_falls_back_to_parsed_kafka_defaults_without_changing_the_spec(tmp_path: Path) -> None:
    raw = yaml.safe_load(FLINK_SPEC)
    run_dir = build_run_dir(tmp_path, spec=yaml.safe_dump(raw))
    facts_path = run_dir / "facts.json"
    facts = json.loads(facts_path.read_text())
    for field in ("kafka_partitions", "key_column", "partition_assignment", "value_encoding", "compression"):
        del facts[field]
    facts_path.write_text(json.dumps(facts))

    document = build_document(run_dir, write_site(tmp_path))

    run = document["run"]
    assert isinstance(run, dict)
    assert run["kafka"] == {
        "partitions": 4,
        "key": "user_id",
        "partition_assignment": "key",
        "value_encoding": "avro",
        "compression": "zstd",
    }
    assert run["spec"] == raw


def test_run_json_prefers_consumed_facts_and_ignores_an_extraneous_compression_fact(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    path = run_dir / "facts.json"
    facts = json.loads(path.read_text())
    facts.update(
        kafka_partitions=8,
        key_column=None,
        partition_assignment="round_robin",
        value_encoding="confluent",
        compression="lz4",
    )
    path.write_text(json.dumps(facts))
    document = build_document(run_dir, write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["kafka"] == {
        "partitions": 8,
        "key": None,
        "partition_assignment": "round_robin",
        "value_encoding": "confluent",
        "compression": "zstd",
    }
    assert run["spec"] == yaml.safe_load(FLINK_SPEC)


def test_a_result_records_the_corpus_partition_assignment(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    path = run_dir / "facts.json"
    facts = json.loads(path.read_text())
    facts["partition_assignment"] = model.PARTITION_ASSIGNMENT_CORPUS
    path.write_text(json.dumps(facts))
    document = parse_result(build_document(run_dir, write_site(tmp_path)))
    assert document["run"]["kafka"]["partition_assignment"] == "corpus"


def test_run_json_derives_the_figures_a_result_is_read_by(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["freshness"] == {
        "window": {"max_s": 27.445, "p50_s": 7.715, "p95_s": 14.345, "p99_s": 25.025},
        "full": {"max_s": 27.445, "p50_s": 7.697, "p95_s": 11.76955, "p99_s": 24.425},
        "clock_skew_suspected": False,
        "min_lag_s": 1.887,
    }
    exactness = derived["exactness"]
    assert isinstance(exactness, dict)
    assert exactness["loss_rows"] == 0 and exactness["duplicate_rows"] == 0 and exactness["exact"] is True
    assert exactness["mode"] == "EXACTLY_ONCE" and exactness["verdict"] is True
    # Detailed violations remain in data.
    assert "violations" not in exactness
    data = document["data"]
    assert isinstance(data, dict)
    assert data["exactness"] == {**build_exactness(), "mode": "EXACTLY_ONCE", "verdict": True}
    summary = data["summary"]
    assert isinstance(summary, dict)
    assert summary["exactness_mode"] == "EXACTLY_ONCE"
    assert summary["exactness"] == {**build_exactness(), "mode": "EXACTLY_ONCE", "verdict": True}
    assert derived["keepup"] == {
        "drain_s": 5.866,
        "backlog_rows_max": 136192,
        "backlog_rows_p50": 0.0,
    }
    assert derived["producer"] == {
        "behind_ms_max": 315,
        "errors": 0,
        # 1.8 GB acked over the 1800 s between the first and the last
        # acknowledgement.
        "effective_offered_rate_bytes_per_s": pytest.approx(1_000_000.0),
        "producer_bound": False,
    }
    assert document["geometry"] == build_geometry()


def test_run_json_summarizes_each_producer_shard(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    # A second shard that stopped without its trailer: a result whose shards are
    # not all done describes a partial offer.
    (run_dir / "producer" / "publish_log-1.jsonl").write_text(
        json.dumps(
            {
                "batch": 2,
                "scheduled_ms": EPOCH_MS + 2000,
                "first_ack_ms": EPOCH_MS + 2100,
                "last_ack_ms": EPOCH_MS + 2400,
                "rows": 50,
                "bytes": 1_000,
                "errors": 3,
            }
        )
        + "\n"
    )
    artifacts = build_document(run_dir, write_site(tmp_path))["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["publish_logs"] == [
        {
            "shard": 0,
            "batches": 2,
            "first_scheduled_ms": EPOCH_MS,
            "first_ack_ms": EPOCH_MS,
            "last_ack_ms": LAST_ACK_MS,
            "bytes": 1_800_000_000,
            "rows": 200,
            "behind_ms_max": 315,
            "errors": 0,
            "done": True,
        },
        {
            "shard": 1,
            "batches": 1,
            "first_scheduled_ms": EPOCH_MS + 2000,
            "first_ack_ms": EPOCH_MS + 2100,
            "last_ack_ms": EPOCH_MS + 2400,
            "bytes": 1_000,
            "rows": 50,
            "behind_ms_max": 100,
            "errors": 3,
            "done": False,
        },
    ]


def test_run_json_points_at_the_run_s_own_artifacts(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    artifacts = build_document(run_dir, write_site(tmp_path))["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["spec"] == "spec.yaml"
    assert artifacts["facts"] == "facts.json"
    assert artifacts["timeline"] == "timeline.log"
    assert artifacts["engine_image"] == "engine-image.json"
    assert artifacts["corpus"] == "corpus.json"
    assert artifacts["summary"] == "scores/summary.json"
    assert artifacts["geometry"] == "scores/geometry.json"
    assert artifacts["keepup_samples"] == "scores/keepup_samples.jsonl"
    # The commit series is embedded whole; every freshness figure came from it.
    snapshots = artifacts["snapshots"]
    assert isinstance(snapshots, list)
    assert [row["snapshot_id"] for row in snapshots] == [11, 12]


def test_run_json_costs_the_fleet_over_the_run(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["cost"] == {
        "usd_per_hour": pytest.approx(0.25),
        "run_hours": pytest.approx(0.5),
        "usd": pytest.approx(0.125),
    }


def test_cluster_cost_uses_pods_and_never_falls_back_to_heap_knobs(tmp_path: Path) -> None:
    from tests.test_pod_fleet import ROOT, spark_pods

    run_dir = build_run_dir(tmp_path, spec=(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml").read_text())
    pods = run_dir / "engine-pods.json"
    pods.write_text(json.dumps(spark_pods()))
    derived = build_document(run_dir, write_site(tmp_path))["derived"]
    assert isinstance(derived, dict)
    assert derived["cost"]["usd_per_hour"] == pytest.approx(5 * 0.04 + (14335 / 1024) * 0.005)
    pods.unlink()
    missing = build_document(run_dir, write_site(tmp_path))
    missing_inputs, derived = missing["missing"], missing["derived"]
    assert isinstance(missing_inputs, list) and isinstance(derived, dict)
    assert "engine-pods.json" in missing_inputs
    assert derived["cost"]["usd_per_hour"] is None
    assert derived["cost"]["usd"] is None


def test_cluster_cost_stays_unknown_when_one_required_fleet_is_missing(tmp_path: Path) -> None:
    from tests.test_pod_fleet import compactor_pods

    spec = FLINK_SPEC + (
        "compaction:\n"
        "  driver: spark-rewrite\n"
        "  interval_s: 600\n"
        "  spark-rewrite: {executors: 2, executor_cores: 4, executor_mem_mb: 8192}\n"
    )
    run_dir = build_run_dir(tmp_path, spec=spec)
    (run_dir / "compactor-pods.json").write_text(json.dumps(compactor_pods()))
    (run_dir / "engine-pods.json").unlink()
    document = build_document(run_dir, write_site(tmp_path))
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["cost"]["usd_per_hour"] is None
    assert derived["cost"]["usd"] is None


def test_collect_finds_local_publish_logs_without_counting_copies(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    cluster_log = run_dir / "producer/publish_log-0.jsonl"
    local_log = run_dir / cluster_log.name
    local_log.write_bytes(cluster_log.read_bytes())
    both = build_document(run_dir, write_site(tmp_path))
    cluster_log.unlink()
    local = build_document(run_dir, write_site(tmp_path))
    local_derived, both_derived = local["derived"], both["derived"]
    assert isinstance(local_derived, dict) and isinstance(both_derived, dict)
    assert local_derived["producer"] == both_derived["producer"]
    assert local_derived["producer"]["effective_offered_rate_bytes_per_s"] is not None
    missing = local["missing"]
    assert isinstance(missing, list)
    assert "producer/publish_log-*.jsonl" not in missing


@pytest.mark.parametrize("spec", [FLINK_SPEC, EXTERNAL_SPEC], ids=["managed", "external"])
def test_local_runs_have_no_dollar_cost_even_with_site_prices(tmp_path: Path, spec: str) -> None:
    site_path = write_site(tmp_path)
    site = yaml.safe_load(site_path.read_text())
    site.pop("kubernetes")
    site_path.write_text(yaml.safe_dump(site))
    document = build_document(build_run_dir(tmp_path, spec=spec), site_path)
    derived = document["derived"]
    assert isinstance(derived, dict)
    assert derived["cost"] == {"usd_per_hour": None, "run_hours": pytest.approx(0.5), "usd": None}


def test_run_json_names_every_optional_input_it_could_not_read(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path, geometry=False, publish_logs=False, engine_image=False)
    document = build_document(run_dir, write_site(tmp_path))
    assert document["missing"] == [
        "engine-image.json",
        "scores/geometry.json",
        "scores/compaction.json",
        "producer/publish_log-*.jsonl",
    ]
    assert document["geometry"] is None
    run = document["run"]
    assert isinstance(run, dict)
    assert run["engine_versions"] is None
    derived = document["derived"]
    assert isinstance(derived, dict)
    # Without publish logs, use scorer totals and leave the rate unknown.
    assert derived["producer"] == {
        "behind_ms_max": 315,
        "errors": 0,
        "effective_offered_rate_bytes_per_s": None,
        "producer_bound": False,
    }
    cost = derived["cost"]
    assert isinstance(cost, dict)
    # The table's last commit stands in for the producer's last acknowledgement.
    assert cost["run_hours"] == pytest.approx(600_000 / 3_600_000)


def test_run_json_refuses_a_directory_that_names_no_run(tmp_path: Path) -> None:
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(ValueError, match="spec.yaml"):
        build_document(empty, write_site(tmp_path))


# ---------------------------------------------------------------------------
# Fleet
# ---------------------------------------------------------------------------


def test_fleet_of_a_managed_flink_run_comes_from_its_knobs(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["fleet"] == [
        {"role": "jobmanager", "count": 1, "vcpu": 1.0, "gib": 2.0, "machine_type": "m6i.xlarge"},
        {"role": "taskmanager", "count": 2, "vcpu": 2.0, "gib": 4.0, "machine_type": "m6i.xlarge"},
    ]
    assert run["engine_versions"] == {
        "image": "registry.invalid/lakehouse-ingest-bench/flink:abc123",
        "digest": "sha256:deadbeef",
    }


def test_fleet_without_a_machine_type_knob_says_so(tmp_path: Path) -> None:
    spec = FLINK_SPEC.replace("  machine_type: m6i.xlarge\n", "")
    document = build_document(build_run_dir(tmp_path, spec=spec), write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    fleet = run["fleet"]
    assert isinstance(fleet, list)
    assert {role["machine_type"] for role in fleet} == {model.MACHINE_TYPE_UNSPECIFIED}


def test_fleet_of_an_external_run_is_the_spec_s_own(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path, spec=EXTERNAL_SPEC), write_site(tmp_path))
    run = document["run"]
    assert isinstance(run, dict)
    assert run["engine"] == "external"
    assert run["fleet"] == [{"role": "worker", "count": 3, "vcpu": 4.0, "gib": 8.0, "machine_type": "n2-standard-4"}]
    assert run["engine_versions"] == {
        "name": "some-engine",
        "version": "1.2",
        "notes": "read the topic with its own writer",
    }
    derived = document["derived"]
    assert isinstance(derived, dict)
    cost = derived["cost"]
    assert isinstance(cost, dict)
    # 3 x (4 x 0.04 + 8 x 0.005) = 0.6 USD/h.
    assert cost["usd_per_hour"] == pytest.approx(0.6)


def test_an_inline_compactor_is_not_charged_twice(tmp_path: Path) -> None:
    """Its work is already inside the engine's fleet, which the run already counts."""
    spec = EXTERNAL_SPEC + "compaction:\n  driver: inline\n"
    document = build_document(build_run_dir(tmp_path / "with", spec=spec), write_site(tmp_path))
    plain = build_document(build_run_dir(tmp_path / "without", spec=EXTERNAL_SPEC), write_site(tmp_path))
    run, unchanged = document["run"], plain["run"]
    assert isinstance(run, dict) and isinstance(unchanged, dict)
    assert run["fleet"] == unchanged["fleet"]


def test_an_external_compactor_is_charged_on_top_of_the_engine(tmp_path: Path) -> None:
    spec = EXTERNAL_SPEC + (
        "compaction:\n"
        "  driver: external\n"
        '  external: {name: their-compactor, version: "2.1", notes: "hourly"}\n'
        "  fleet:\n"
        "    - {role: compactor, count: 1, vcpu: 4, gib: 8, machine_type: n2-standard-4}\n"
    )
    document = build_document(build_run_dir(tmp_path, spec=spec), write_site(tmp_path))
    run, derived = document["run"], document["derived"]
    assert isinstance(run, dict) and isinstance(derived, dict)
    fleet = run["fleet"]
    assert isinstance(fleet, list)
    assert [role["role"] for role in fleet] == ["worker", "compactor"]
    # The engine's 0.6 USD/h plus the compactor's 4 x 0.04 + 8 x 0.005.
    assert derived["cost"]["usd_per_hour"] == pytest.approx(0.6 + 0.2)


def test_a_rendered_sidecar_is_charged_from_its_admitted_pod_requests(tmp_path: Path) -> None:
    from tests.test_pod_fleet import compactor_pods

    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    (run_dir / "compactor-pods.json").write_text(json.dumps(compactor_pods()))
    document = build_document(run_dir, write_site(tmp_path))
    run, derived, artifacts = document["run"], document["derived"], document["artifacts"]
    assert isinstance(run, dict) and isinstance(derived, dict) and isinstance(artifacts, dict)
    fleet = run["fleet"]
    assert isinstance(fleet, list)
    assert [role["role"] for role in fleet] == ["worker", "compactor-driver", "compactor-executor"]
    assert [role["count"] for role in fleet] == [3, 1, 2]
    assert derived["cost"]["usd_per_hour"] == pytest.approx(0.6 + 10.5 * 0.04 + 23 * 0.005)
    assert artifacts["compactor_pods"] == "compactor-pods.json"


def test_the_compaction_reading_reaches_the_result(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    reading = {
        "tier": "lifecycle",
        "records_dropped": None,
        "floor_ratio": 1.5,
        "rewrite_share_of_writes": 0.69,
        "written_per_ingested_byte": 2.25,
        "converged": False,
    }
    (run_dir / "scores" / "compaction.json").write_text(json.dumps(reading))
    document = parse_result(build_document(run_dir, write_site(tmp_path)))
    assert document["compaction"] == reading
    artifacts = build_document(run_dir, write_site(tmp_path))["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["compaction"] == "scores/compaction.json"


_SPARK_COMPACTION_SPEC = EXTERNAL_SPEC + (
    "compaction:\n"
    "  driver: spark-rewrite\n"
    "  interval_s: 600\n"
    "  spark-rewrite: {executors: 2, executor_cores: 4, executor_mem_mb: 8192, machine_type: n2-standard-4}\n"
)


def _write_campaign(run_dir: Path, name: str, report: dict[str, object]) -> None:
    directory = run_dir / "scores" / name
    directory.mkdir()
    (directory / "compaction-report.json").write_text(json.dumps(report))


def _campaign_report(
    eligible: list[int],
    *,
    warmup_s: float = 60.0,
    grid_s: int = 60,
    window_s: float = 1000.0,
    phases: list[str] | None = None,
) -> dict[str, object]:
    """Build a campaign report with an eligible-backlog series.

    Omitting phases represents a report generated without an offer boundary.
    """
    grid: list[dict[str, object]] = []
    for index, value in enumerate(eligible):
        grid.append(
            {
                "at_ms": 1_000_000 + index * grid_s * 1_000,
                "offset_s": index * grid_s,
                "phase": "unknown" if phases is None else phases[index],
                "snapshot_id": index,
                "live": {"eligible": {"bytes": value, "files": 8, "partitions": 2}, "oldest_small_age_s": 30.0},
            }
        )
    return {
        "campaign_pass": None,
        "grid": grid,
        "grid_s": grid_s,
        "reasons": ["no term fired, which is not a pass: no term here reads whether the table kept up"],
        "windows": {
            "basis": "offered",
            "floor_ratio_median": 5.83,
            "rewrite_cycles": 12,
            "warmup_s": warmup_s,
            "window_s": window_s,
            "windows": 3,
            "reasons": ["no term fired, which is not a pass: no term here reads whether the table kept up"],
        },
    }


def test_the_campaign_report_reaches_the_result_whole(tmp_path: Path) -> None:
    """The grid is the only published evidence of whether the backlog grew."""
    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    report = _campaign_report([10, 20, 30, 40, 50])
    _write_campaign(run_dir, "campaign", report)

    collected = build_document(run_dir, write_site(tmp_path))
    assert collected["campaign"] == report
    artifacts = collected["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["campaign"] == "scores/campaign/compaction-report.json"


def test_campaign_survives_publication_and_table_rendering(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path, spec=EXTERNAL_SPEC + "compaction:\n  driver: inline\n")
    report = _campaign_report([10, 20, 30, 40, 50])
    _write_campaign(run_dir, "campaign", report)
    (run_dir / "scores" / "terminal-exactness.json").write_text(json.dumps({"verdict": True, "snapshot_id": 42}))
    results = tmp_path / "results"
    assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--publish", str(results)]) == 0
    published = list(results.rglob("result.json"))
    assert len(published) == 1
    document = parse_result(json.loads(published[0].read_text()))
    assert document["campaign"] == report
    assert cli.results_table_main([str(results), "--out", str(results / "RESULTS.md")]) == 0
    assert "| some-engine | Integrated compaction |" in (results / "RESULTS.md").read_text()


def test_a_report_from_the_retired_contract_is_not_the_one_published(tmp_path: Path) -> None:
    """Width alone picked the old report on 20 of 27 run directories on disk.

    A retired-contract report asserts a verdict on terms this branch removed,
    and some format raw byte counts into their reasons, which the public-surface
    scan reads as cloud account ids and refuses to publish.
    """
    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    current = _campaign_report([1, 2, 3, 4], window_s=300.0)
    retired = _campaign_report([9, 9, 9, 9], window_s=1000.0)
    retired_windows = retired["windows"]
    assert isinstance(retired_windows, dict)
    del retired_windows["basis"]
    retired_windows["backlog_growing"] = False
    retired_windows["eligible_bytes_median"] = [100_223_986_482, 315_240_384_212, 1]
    _write_campaign(run_dir, "campaign", current)
    _write_campaign(run_dir, "campaign-hour", retired)

    collected = build_document(run_dir, write_site(tmp_path))
    # The retired report is three times as wide and still loses.
    assert collected["campaign"] == current
    assert collected["campaign_source"] == "campaign"
    # It is still on disk and still listed; it is only ineligible to be read.
    assert collected["campaign_available"] == ["campaign", "campaign-hour"]


def test_a_run_holding_only_retired_reports_publishes_none_of_them(tmp_path: Path) -> None:
    """No reading beats one made under rules that no longer hold.

    Sorting a retired report last only helps a run that also holds a current
    one. Twenty of the twenty-seven run directories on this disk hold nothing
    current, and demoting would have published a retired verdict on every one.
    """
    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    retired = _campaign_report([1, 2, 3, 4])
    retired_windows = retired["windows"]
    assert isinstance(retired_windows, dict)
    del retired_windows["basis"]
    _write_campaign(run_dir, "campaign-hour", retired)

    collected = build_document(run_dir, write_site(tmp_path))
    assert collected["campaign"] is None
    assert collected["campaign_source"] is None
    # On disk and named, so a reader can see why the reading is absent.
    assert collected["campaign_available"] == ["campaign-hour"]
    missing = collected["missing"]
    assert isinstance(missing, list)
    assert any("campaign" in str(name) for name in missing)


def test_a_malformed_campaign_report_does_not_abort_the_collection(tmp_path: Path) -> None:
    """A non-object report must not prevent collection of a valid wider report."""
    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    good = _campaign_report([1, 2, 3, 4])
    _write_campaign(run_dir, "campaign-hour", good)
    broken = run_dir / "scores" / "campaign-broken"
    broken.mkdir()
    (broken / "compaction-report.json").write_text("[]")

    collected = build_document(run_dir, write_site(tmp_path))
    assert collected["campaign"] == good
    assert collected["campaign_source"] == "campaign-hour"
    assert collected["campaign_available"] == ["campaign-broken", "campaign-hour"]


def test_the_widest_campaign_report_is_the_one_published(tmp_path: Path) -> None:
    """Select by configured window span and record the selected directory name."""
    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    narrow = _campaign_report([1, 2, 3, 4], window_s=300.0)
    wide = _campaign_report([10, 20, 30, 40], window_s=1000.0)
    for name, report in (("campaign", narrow), ("campaign-hour", wide)):
        _write_campaign(run_dir, name, report)

    collected = build_document(run_dir, write_site(tmp_path))
    assert collected["campaign"] == wide
    assert collected["campaign_source"] == "campaign-hour"
    assert collected["campaign_available"] == ["campaign", "campaign-hour"]
    artifacts = collected["artifacts"]
    assert isinstance(artifacts, dict)
    assert artifacts["campaign"] == "scores/campaign-hour/compaction-report.json"


def test_a_run_without_compaction_carries_no_campaign_keys(tmp_path: Path) -> None:
    """Runs with no compaction layer neither gain the keys nor report them missing."""
    collected = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    assert "campaign" not in collected
    assert "campaign_source" not in collected
    assert "campaign_available" not in collected
    missing = collected["missing"]
    assert isinstance(missing, list)
    assert not [name for name in missing if "campaign" in str(name)]


def test_compaction_readiness_artifacts_reach_the_result_whole(tmp_path: Path) -> None:
    from tests.test_pod_fleet import compactor_pods

    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    (run_dir / "compactor-pods.json").write_text(json.dumps(compactor_pods()))
    terminal = {
        "verdict": True,
        "snapshot_id": 42,
        "expected_rows": 1_000,
        "detail": {"payload_validation": "sampled"},
    }
    status = {
        "healthy": True,
        "captured_at": "2026-09-20T11:29:00Z",
        "pods": [{"name": "compact-driver", "phase": "Running", "reason": None}],
    }
    (run_dir / "scores" / "terminal-exactness.json").write_text(json.dumps(terminal))
    (run_dir / "scores" / "compactor-status.json").write_text(json.dumps(status))

    collected = build_document(run_dir, write_site(tmp_path))
    document = parse_result(collected)
    artifacts = collected["artifacts"]

    assert document["terminal_exactness"] == terminal
    assert document["compactor_status"] == status
    assert isinstance(artifacts, dict)
    assert artifacts["terminal_exactness"] == "scores/terminal-exactness.json"
    assert artifacts["compactor_status"] == "scores/compactor-status.json"


def test_a_run_nothing_measured_for_compaction_is_still_collectable(tmp_path: Path) -> None:
    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    missing = document["missing"]
    assert isinstance(missing, list)
    assert "scores/compaction.json" in missing
    assert document["compaction"] is None
    # An older result, collected before the layer existed, still parses.
    without = {key: value for key, value in document.items() if key != "compaction"}
    assert parse_result(without)


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_result_relative_path_groups_by_staging_date_and_run(tmp_path: Path) -> None:
    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    document["collected_at"] = "2026-09-20T00:30:00+14:00"
    document["run"]["fleet"] = [
        {"role": "worker", "count": 2, "vcpu": 0.6, "gib": 2.125, "machine_type": "r6i.large"},
        {"role": "coordinator", "count": 1, "vcpu": 0.5, "gib": 1.25, "machine_type": "m6i.large"},
    ]
    assert result_relative_path(document) == Path("flink/smoke/2026-09-09/05-25-08Z/result.json")


def test_result_relative_path_keeps_metadata_out_of_the_filename(tmp_path: Path) -> None:
    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    scoring = document["run"]["spec"].get("scoring")
    summary = document["data"]["summary"]
    assert scoring is not None and summary is not None
    scoring["exactness"] = "AT_LEAST_ONCE"
    summary["exactness_mode"] = "AT_LEAST_ONCE"
    document["run"]["variant"] = "tuned/label"
    document["run"]["run_id"] = "../label-20260909T052508Z"
    assert result_relative_path(document) == Path("flink/smoke/2026-09-09/05-25-08Z/result.json")


def test_result_relative_path_requires_corpus_metadata(tmp_path: Path) -> None:
    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    document["run"]["corpus"] = None
    with pytest.raises(ValueError, match="run.corpus"):
        result_relative_path(document)


@pytest.mark.parametrize("stamp", ["", "not-a-timestamp", "20260230T120000Z", "20260909T052508"])
def test_result_relative_path_requires_a_utc_run_id_timestamp(tmp_path: Path, stamp: str) -> None:
    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    document["run"]["run_id"] = f"collect-flink-{stamp}"
    with pytest.raises(ValueError, match="run.run_id"):
        result_relative_path(document)


@pytest.mark.parametrize(
    ("field", "unsafe"),
    [
        ("engine", "../flink"),
        ("corpus", "../smoke"),
    ],
)
def test_result_relative_path_rejects_unsafe_path_components(tmp_path: Path, field: str, unsafe: str) -> None:
    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    run = document["run"]
    if field == "corpus":
        corpus = run["corpus"]
        assert corpus is not None
        corpus["name"] = unsafe
    else:
        run[field] = unsafe  # type: ignore[literal-required]
    with pytest.raises(ValueError, match=field):
        result_relative_path(document)


@pytest.mark.parametrize(
    "spec",
    [
        pytest.param(FLINK_SPEC, id="flink"),
        pytest.param(EXTERNAL_SPEC, id="external"),
        pytest.param((Path(__file__).resolve().parents[1] / "specs/smoke/smoke-spark.yaml").read_text(), id="spark"),
    ],
)
def test_collect_writes_run_json_beside_the_run_by_default(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], spec: str
) -> None:
    run_dir = build_run_dir(tmp_path, spec=spec)
    # The fixture's pod metadata is Flink-specific; collection also supports absent metadata.
    (run_dir / "engine-pods.json").unlink()
    site_path = write_site(tmp_path)
    assert cli.main(["--run-dir", str(run_dir), "--site", str(site_path)]) == 0
    document = json.loads((run_dir / "run.json").read_text())
    assert document["schema_version"] == SCHEMA_VERSION
    assert document["harness_version"]
    assert document["run"]["variant"] == "default"
    assert "run.json" in capsys.readouterr().out


@pytest.mark.parametrize(
    ("spec", "variant", "expected_variant"),
    [
        pytest.param(FLINK_SPEC, None, "default", id="flink-default"),
        pytest.param(EXTERNAL_SPEC, None, "default", id="external-default"),
        pytest.param(FLINK_SPEC, "tuned", "tuned", id="flink-explicit"),
        pytest.param(EXTERNAL_SPEC, "tuned", "tuned", id="external-explicit"),
        pytest.param(EXTERNAL_SPEC, "hash", "hash", id="external-explicit-hash"),
    ],
)
def test_collect_publishes_default_or_explicit_variant(
    tmp_path: Path, spec: str, variant: str | None, expected_variant: str
) -> None:
    run_dir = build_run_dir(tmp_path, spec=spec)
    results = tmp_path / "results"
    arguments = ["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--publish", str(results)]
    if variant is not None:
        arguments.extend(["--variant", variant])
    assert cli.main(arguments) == 0
    published = list(results.rglob("result.json"))
    assert len(published) == 1
    assert published[0].read_bytes() == (run_dir / "run.json").read_bytes()
    document = json.loads(published[0].read_text())
    assert document["run"]["variant"] == expected_variant
    table = results / "RESULTS.md"
    assert cli.results_table_main([str(results), "--out", str(table)]) == 0
    labelled = "Ingest only" if expected_variant == "default" else f"Ingest only ({expected_variant})"
    engine = "Flink" if document["run"]["engine"] == "flink" else document["run"]["engine_versions"]["name"]
    assert f"| {engine} | {labelled} |" in table.read_text()


@pytest.mark.parametrize("run_valid", [True, False])
def test_collect_publishes_the_same_document_it_keeps_locally(tmp_path: Path, run_valid: bool) -> None:
    run_dir = build_run_dir(tmp_path, run_valid=run_valid)
    results = tmp_path / "nested" / "results"
    arguments = ["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--publish", str(results)]
    if not run_valid:
        arguments.append("--publish-invalid")
    with patch.object(cli, "build_run_json", wraps=run_json.build_run_json) as build:
        assert cli.main(arguments) == 0
    build.assert_called_once()
    published = list(results.rglob("*.json"))
    assert len(published) == 1
    assert published[0].read_bytes() == (run_dir / "run.json").read_bytes()
    assert json.loads(published[0].read_text())["data"]["summary"]["run_valid"] is run_valid


def test_collect_requires_compaction_readiness_unless_publishing_the_failed_run(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from tests.test_pod_fleet import compactor_pods

    run_dir = build_run_dir(tmp_path, spec=_SPARK_COMPACTION_SPEC)
    (run_dir / "compactor-pods.json").write_text(json.dumps(compactor_pods()))
    terminal = {"verdict": False, "snapshot_id": 42, "loss_rows": 1}
    status = {"healthy": True, "captured_at": "2026-09-20T11:29:00Z", "pods": []}
    (run_dir / "scores" / "terminal-exactness.json").write_text(json.dumps(terminal))
    (run_dir / "scores" / "compactor-status.json").write_text(json.dumps(status))
    results = tmp_path / "results"
    arguments = ["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--publish", str(results)]

    assert cli.main(arguments) == 1
    assert not results.exists()
    assert "terminal_exactness.verdict is not true" in capsys.readouterr().err

    assert cli.main([*arguments, "--publish-invalid"]) == 0
    published = list(results.rglob("*.json"))
    assert len(published) == 1
    document = json.loads(published[0].read_text())
    assert document["data"]["summary"]["run_valid"] is True
    assert document["terminal_exactness"] == terminal
    assert document["compactor_status"] == status
    assert document["artifacts"]["terminal_exactness"] == "scores/terminal-exactness.json"
    assert document["artifacts"]["compactor_status"] == "scores/compactor-status.json"


@pytest.mark.parametrize("problem", ["shortened_offer", "missing_price"])
def test_publish_invalid_still_requires_document_disclosures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], problem: str
) -> None:
    run_dir = build_run_dir(tmp_path, run_valid=False)
    site_path = write_site(tmp_path)
    if problem == "shortened_offer":
        spec_path = run_dir / "spec.yaml"
        spec = yaml.safe_load(spec_path.read_text())
        spec["producer"]["seconds"] = 1
        spec_path.write_text(yaml.safe_dump(spec))
        rule = "producer.seconds"
    else:
        site = yaml.safe_load(site_path.read_text())
        site["pricing"]["vcpu_hour_usd"] = 0
        site_path.write_text(yaml.safe_dump(site))
        rule = "site_pricing.vcpu_hour_usd"
    results = tmp_path / "results"
    arguments = ["--run-dir", str(run_dir), "--site", str(site_path), "--publish", str(results), "--publish-invalid"]
    assert cli.main(arguments) == 1
    assert (run_dir / "run.json").is_file()
    assert not results.exists()
    assert rule in capsys.readouterr().err


@pytest.mark.parametrize("problem", ["invalid", "missing_summary", "machine_type", "empty_fleet"])
def test_collect_keeps_the_local_document_when_publication_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], problem: str
) -> None:
    run_dir = build_run_dir(tmp_path)
    if problem == "invalid":
        summary_path = run_dir / "scores" / "summary.json"
        summary = json.loads(summary_path.read_text())
        summary["run_valid"] = False
        summary_path.write_text(json.dumps(summary))
    elif problem == "missing_summary":
        (run_dir / "scores" / "summary.json").unlink()
    elif problem == "machine_type":
        spec_path = run_dir / "spec.yaml"
        spec_path.write_text(spec_path.read_text().replace("m6i.xlarge", "unspecified"))
    else:
        (run_dir / "engine-pods.json").unlink()
    results = tmp_path / "results"
    assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--publish", str(results)]) == 1
    assert (run_dir / "run.json").is_file()
    assert not results.exists()
    captured = capsys.readouterr()
    assert "cannot publish" in captured.err
    assert f"COLLECTED out={run_dir / 'run.json'}" in captured.out


def test_collect_derives_the_nested_path_only_for_a_trailing_slash(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    results = tmp_path / "results"
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(write_site(tmp_path)),
                "--out",
                f"{results}/",
                "--variant",
                "sorted",
            ]
        )
        == 0
    )
    written = list(results.glob("flink/smoke/*/*/result.json"))
    assert len(written) == 1
    assert written[0].relative_to(results) == Path("flink/smoke/2026-09-09/05-25-08Z/result.json")
    assert written[0].read_bytes() == (run_dir / "run.json").read_bytes()
    document = json.loads(written[0].read_text())
    assert document["run"]["variant"] == "sorted"


def test_collect_canonical_output_distinguishes_same_day_runs_and_recollects_in_place(tmp_path: Path) -> None:
    results = tmp_path / "results"
    run_ids = ("collect-flink-20260909T052508Z", "collect-flink-20260909T062508Z")
    run_dirs = [build_run_dir(tmp_path, run_id=run_id) for run_id in run_ids]

    for run_dir in (*run_dirs, run_dirs[0]):
        assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--out", f"{results}/"]) == 0

    published = results / "flink" / "smoke"
    assert sorted(path.relative_to(published).as_posix() for path in published.rglob("*.json")) == [
        "2026-09-09/05-25-08Z/result.json",
        "2026-09-09/06-25-08Z/result.json",
    ]
    assert cli.results_table_main([str(results), "--out", str(results / "RESULTS.md")]) == 0
    assert validate(results, workloads=fixture_workloads(tmp_path), specs=fixture_specs(tmp_path)) == []


@pytest.mark.parametrize("option", ["--out", "--publish"])
def test_collect_refuses_to_overwrite_a_different_run_at_the_same_timestamp(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], option: str
) -> None:
    first = build_run_dir(tmp_path, run_id="first-flink-20260909T052508Z")
    second = build_run_dir(tmp_path, run_id="second-flink-20260909T052508Z")
    results = tmp_path / "results"
    site = write_site(tmp_path)
    arguments = ["--site", str(site), option, f"{results}/"]
    assert cli.main(["--run-dir", str(first), *arguments]) == 0
    published = results / "flink/smoke/2026-09-09/05-25-08Z/result.json"
    original = published.read_bytes()

    assert cli.main(["--run-dir", str(second), *arguments]) == 1
    captured = capsys.readouterr()
    assert "different run" in captured.err
    assert published.read_bytes() == original
    local = second / "run.json"
    assert local.is_file()
    assert json.loads(local.read_text())["run"]["run_id"] == second.name
    assert f"COLLECTED out={local}" in captured.out


def test_collect_publish_uses_an_explicit_workload_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = build_run_dir(tmp_path)
    results = tmp_path / "results"
    monkeypatch.setenv("INGEST_BENCH_WORKLOADS", str(tmp_path / "unavailable"))
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(write_site(tmp_path)),
                "--publish",
                str(results),
                "--workloads",
                str(fixture_workloads(tmp_path / "explicit")),
            ]
        )
        == 0
    )
    assert len(list(results.rglob("*.json"))) == 1


def test_collect_republishes_across_utc_days_without_duplicate_resources(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    results = tmp_path / "results"
    arguments = ["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--out", f"{results}/"]
    with patch.object(cli, "datetime") as clock:
        clock.now.side_effect = [datetime(2026, 9, 20, 23, 59, tzinfo=UTC), datetime(2026, 9, 22, tzinfo=UTC)]
        assert cli.main(arguments) == 0
        first_paths = list(results.rglob("*.json"))
        first_paths[0].with_name("README.md").write_text("Operator notes.\n")
        assert cli.main([*arguments, "--variant", "renamed"]) == 0

    assert first_paths[0].with_name("README.md").read_text() == "Operator notes.\n"
    assert json.loads(first_paths[0].read_text())["run"]["variant"] == "renamed"
    assert list(results.rglob("*.json")) == first_paths
    assert len(first_paths) == 1
    assert json.loads(first_paths[0].read_text())["collected_at"] == "2026-09-22T00:00:00Z"
    assert cli.results_table_main([str(results), "--out", str(results / "RESULTS.md")]) == 0
    assert validate(results, workloads=fixture_workloads(tmp_path), specs=fixture_specs(tmp_path)) == []


def test_collect_uses_an_explicit_output_file_as_given(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    output = tmp_path / "published.json"
    assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--out", str(output)]) == 0
    assert output.is_file()
    assert json.loads(output.read_text())["run"]["corpus"] == CORPUS


def test_collect_reports_missing_corpus_for_directory_output(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    run_dir = build_run_dir(tmp_path)
    (run_dir / "corpus.json").unlink()
    output = tmp_path / "results"
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(write_site(tmp_path)),
                "--out",
                f"{output}/",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert "corpus.json" in captured.err
    assert str(run_dir) in captured.err
    assert f"COLLECTED out={run_dir / 'run.json'}" in captured.out
    assert json.loads((run_dir / "run.json").read_text())["run"]["corpus"] is None
    assert not output.exists()


@pytest.mark.parametrize("field", ["corpus_hash", "duration_s", "effective_preset", "effective_preset.alpha"])
def test_collect_reports_incomplete_corpus_metadata(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], field: str
) -> None:
    run_dir = build_run_dir(tmp_path)
    corpus = run_dir / "corpus.json"
    metadata = json.loads(corpus.read_text())
    target = metadata
    parts = field.split(".")
    for part in parts[:-1]:
        target = target[part]
    del target[parts[-1]]
    corpus.write_text(json.dumps(metadata))
    output = tmp_path / "result.json"
    assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path)), "--out", str(output)]) == 1
    error = capsys.readouterr().err
    assert "corpus.json" in error and parts[-1] in error
    assert "Traceback" not in error
    assert not output.exists()


def test_collect_reports_directory_output_without_trailing_slash(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    run_dir = build_run_dir(tmp_path)
    output = tmp_path / "results"
    output.mkdir()
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(write_site(tmp_path)),
                "--out",
                str(output),
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    assert str(output) in captured.err
    assert "append '/'" in captured.err
    assert str(run_dir) not in captured.err
    assert "COLLECTED" not in captured.out
    assert list(output.iterdir()) == []


def test_collect_reports_unwritable_output_path(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    run_dir = build_run_dir(tmp_path)
    parent = tmp_path / "file-not-directory"
    parent.write_text("preserve me")
    assert (
        cli.main(
            [
                "--run-dir",
                str(run_dir),
                "--site",
                str(write_site(tmp_path)),
                "--out",
                str(parent / "result.json"),
            ]
        )
        == 1
    )
    assert str(parent) in capsys.readouterr().err
    assert parent.read_text() == "preserve me"


@pytest.mark.parametrize("path", [path for path in tracked_files() if path.name == "run.json"])
def test_tracked_run_documents_parse(path: Path) -> None:
    parse_result(json.loads(path.read_text()))


@pytest.mark.parametrize("path", [path for path in tracked_files() if path.name == "run.json"])
def test_tracked_run_documents_use_collect_serialization(path: Path) -> None:
    text = path.read_text()
    assert text == json.dumps(json.loads(text), indent=2, sort_keys=True) + "\n"


def test_collection_omits_absorption_from_scorer_output(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    run_dir = build_run_dir(tmp_path)
    summary_path = run_dir / "scores" / "summary.json"
    summary = json.loads(summary_path.read_text())
    summary["keepup"]["absorbed_at_offer_end"] = 0.9
    summary_path.write_text(json.dumps(summary))
    read_json = run_json._read_json
    monkeypatch.setattr(run_json, "_read_json", lambda path: summary if path == summary_path else read_json(path))

    document = build_document(run_dir, write_site(tmp_path))

    assert "absorbed_at_offer_end" not in json.dumps(document)
    assert json.loads(summary_path.read_text()) == summary


def test_collection_defaults_sla_without_losing_exactness_counts_or_unknown_fields(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    exactness_path = run_dir / "scores" / "exactness.json"
    exactness = json.loads(exactness_path.read_text())
    exactness.update(exact=False, duplicate_rows=7, future_measurement={"kept": True})
    exactness_path.write_text(json.dumps(exactness))

    document = build_document(run_dir, write_site(tmp_path))

    data = document["data"]
    derived_data = document["derived"]
    assert isinstance(data, dict) and isinstance(derived_data, dict)
    published = data["exactness"]
    derived = derived_data["exactness"]
    assert isinstance(published, dict) and isinstance(derived, dict)
    assert published["duplicate_rows"] == derived["duplicate_rows"] == 7
    assert published["future_measurement"] == derived["future_measurement"] == {"kept": True}
    assert published["mode"] == derived["mode"] == "EXACTLY_ONCE"
    assert published["verdict"] is derived["verdict"] is False


@pytest.mark.parametrize("effective", [None, [], 42])
def test_collect_reports_non_object_effective_preset(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], effective: object
) -> None:
    run_dir = build_run_dir(tmp_path)
    corpus = run_dir / "corpus.json"
    metadata = json.loads(corpus.read_text())
    metadata["effective_preset"] = effective
    corpus.write_text(json.dumps(metadata))
    assert cli.main(["--run-dir", str(run_dir), "--site", str(write_site(tmp_path))]) == 1
    assert "corpus.json.effective_preset: must be an object" in capsys.readouterr().err
    assert not (run_dir / "run.json").exists()


def test_an_upsert_run_collects_its_live_state_verdict(tmp_path: Path) -> None:
    spec = yaml.safe_load(FLINK_SPEC)
    spec.setdefault("scoring", {})["exactness"] = "LATEST_PER_KEY"
    run_dir = build_run_dir(tmp_path, spec=yaml.safe_dump(spec))
    verdict = {"mode": "LATEST_PER_KEY", "verdict": True, "snapshots": []}
    (run_dir / "scores" / "terminal-upsert.json").write_text(json.dumps(verdict))
    (run_dir / "scores" / "terminal-exactness.json").write_text(json.dumps({"verdict": False}))
    health = {"snapshot_id": 7, "rows_per_live_row": 1.5}
    (run_dir / "scores" / "table-health.json").write_text(json.dumps(health))

    collected = build_document(run_dir, write_site(tmp_path))
    document = parse_result(collected)

    assert document["terminal_upsert"] == verdict
    assert document["table_health"] == health
    assert "terminal_exactness" not in collected
    artifacts = collected["artifacts"]
    assert isinstance(artifacts, dict) and artifacts["terminal_upsert"] == "scores/terminal-upsert.json"
    assert artifacts["table_health"] == "scores/table-health.json"
