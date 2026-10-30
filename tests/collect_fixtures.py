# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Shared artifact builders for result-reporting tests."""

import json
import shutil
from collections.abc import Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.collect import run_json
from ingest_bench.collect.table import render_results_table
from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.corpus.preset import corpus_hash, load_preset
from ingest_bench.specs import model
from ingest_bench.specs.derive import TABLE_NAMESPACE

BUCKET = "s3://a-bucket"
WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
HARNESS_COMMIT = "0123456789abcdef0123456789abcdef01234567"

# The tracks the fixture specs below conform to, with the variants publication
# tests switch to. Shipped tracks cover full-scale presets only, so these sit
# beside the shipped presets in a test's own workloads directory.
_TRACK = """
corpus: {corpus}
kafka: {{partitions: 4, key: user_id, partition_assignment: key, value_encoding: {encoding}}}
producer: {{compression: zstd, behind_max_ms: 5000}}
scoring: {{freshness_bound_s: 60, exactness: {exactness}, max_warmup_s: 60}}
table: {{partition: identity(partition_key)}}
"""
FIXTURE_TRACKS = {
    "smoke": _TRACK.format(corpus="smoke", encoding="avro", exactness="EXACTLY_ONCE"),
    "smoke-at-least-once": _TRACK.format(corpus="smoke", encoding="avro", exactness="AT_LEAST_ONCE"),
    "smoke-json": _TRACK.format(corpus="smoke-json", encoding="json", exactness="EXACTLY_ONCE"),
}
SMOKE_CORPUS_HASH = corpus_hash(
    load_preset("smoke", workloads_dir=Path(__file__).resolve().parents[1] / "workloads"), seed=1, row_block=1024
)

EPOCH_MS = 1_000_000_000_000
LAST_ACK_MS = EPOCH_MS + 1_800_000

# A price per vCPU-hour and per GiB-hour that make the fleet below cost
# 0.25 USD/h: 1 x (1 x 0.04 + 2 x 0.005) + 2 x (2 x 0.04 + 4 x 0.005).
VCPU_HOUR_USD = 0.04
GIB_HOUR_USD = 0.005

FLINK_SPEC = """
name: collect-flink
engine: flink
corpus: smoke
table:
  partition: identity(partition_key)
kafka:
  partitions: 4
  key: user_id
producer:
  shards: 1
scoring:
  freshness_bound_s: 60
  warmup_s: 60
flink:
  taskmanagers: 2
  slots: 4
  tm_cpu: 2
  tm_mem_mb: 4096
  jm_cpu: 1
  jm_mem_mb: 2048
  checkpoint_interval: 10s
  distribution_mode: hash
  machine_type: m6i.xlarge
"""

EXTERNAL_SPEC = """
name: collect-external
engine: external
corpus: smoke
kafka:
  partitions: 4
  key: user_id
scoring:
  freshness_bound_s: 60
  warmup_s: 60
external:
  name: some-engine
  version: "1.2"
  notes: "read the topic with its own writer"
fleet:
  - {role: worker, count: 3, vcpu: 4, gib: 8, machine_type: n2-standard-4}
"""

CORPUS_METADATA = {
    "name": "smoke",
    "corpus_hash": SMOKE_CORPUS_HASH,
    "generator_version": GENERATOR_VERSION,
    "seed": 1,
    "row_block": 1024,
    "schema_name": "events",
    "offered_bytes_per_s": 5_000_000,
    "duration_s": 300,
    "rows_per_s": 5_840_896 / 300,
    "row_count": 5_840_896,
    "batch_interval_ms": 1_000,
    "effective_preset": {
        "name": "smoke",
        "schema_name": "events",
        "offered_bytes_per_s": 5_000_000,
        "duration_s": 300,
        "partition_count": 64,
        "alpha": 1.0,
        "target_row_bytes": 256,
        "batch_interval_ms": 1_000,
        "corpus_epoch": "2026-01-01T00:00:00Z",
        "kafka_key_columns": ["user_id", "partition_key"],
        "column_overrides": {},
        "columns": [],
    },
}

CORPUS = {
    "name": "smoke",
    "hash": SMOKE_CORPUS_HASH,
    "schema": "events",
    "offered_bytes_per_s": 5_000_000,
    "duration_s": 300,
    "rows_per_s": 5_840_896 / 300,
    "row_count": 5_840_896,
    "batch_interval_ms": 1_000,
    "partition_count": 64,
    "alpha": 1.0,
    "target_row_bytes": 256,
}


def fixture_workloads(root: Path) -> Path:
    """The shipped presets and schemas, with the fixture track as the only track."""
    workloads = root / "workloads"
    if not workloads.exists():
        for part in ("presets", "schemas"):
            shutil.copytree(WORKLOADS / part, workloads / part)
        (workloads / "tracks").mkdir()
        for name, track in FIXTURE_TRACKS.items():
            (workloads / "tracks" / f"{name}.yaml").write_text(track)
    return workloads


def fixture_specs(root: Path) -> Path:
    """A specs directory holding the fixture specs, as a checkout commits them."""
    specs = root / "specs"
    if not specs.exists():
        specs.mkdir()
        (specs / "collect-flink.yaml").write_text(FLINK_SPEC)
        (specs / "collect-external.yaml").write_text(EXTERNAL_SPEC)
    return specs


def commit_spec(root: Path, document: dict[str, object]) -> None:
    """Commit the spec a mutated document embeds, as an operator commits what ran."""
    spec = cast(dict[str, object], cast(dict[str, object], document["run"])["spec"])
    specs = fixture_specs(root)
    (specs / f"committed-{len(list(specs.glob('*.yaml')))}.yaml").write_text(yaml.safe_dump(spec))


def write_site(tmp_path: Path) -> Path:
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": f"{BUCKET}/corpus",
                "runs_root": f"{BUCKET}/runs",
                "warehouse": f"{BUCKET}/warehouse",
                "kafka": {
                    "bootstrap_servers": "broker.invalid:9092",
                    "security": {"sasl.password": "${env:KAFKA_PASSWORD}"},
                },
                "catalog": {
                    "props": {
                        "uri": "https://catalog.invalid/iceberg",
                        "warehouse": f"{BUCKET}/warehouse",
                        "token": "${env:CATALOG_TOKEN}",
                        "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
                    }
                },
                "pricing": {"vcpu_hour_usd": VCPU_HOUR_USD, "gib_hour_usd": GIB_HOUR_USD},
                "kubernetes": {
                    "context": "cluster",
                    "namespace": "bench",
                    "harness_service_account": "harness",
                    "flink_service_account": "flink",
                    "registry": "images.invalid",
                },
            }
        )
    )
    return path


def build_exactness() -> dict[str, object]:
    return {
        "corrupt_batches": 0,
        "duplicate_ppm": 0.0,
        "duplicate_rows": 0,
        "exact": True,
        "expected_rows": 5840896,
        "loss_rows": 0,
        "rows": 5840896,
        "scored_batches": 300,
        "violations": [{"batch": 7, "expected_rows": 10, "rows": 9, "kind": "loss"}],
    }


def _summary(*, run_valid: bool, table: str) -> dict[str, object]:
    """The recorded AWS smoke's summary, trimmed to the keys `collect` reads."""
    return {
        "aborted": False,
        "backlog_rows": 0,
        "clock_skew_suspected": False,
        "committed_rows": 5840896,
        "corpus_hash": SMOKE_CORPUS_HASH,
        "corpus_seed": 1,
        "corpus_generator_version": GENERATOR_VERSION,
        "corpus_row_block": 1024,
        "corpus_uri": f"{BUCKET}/corpus/smoke-{SMOKE_CORPUS_HASH}",
        "epoch_ms": EPOCH_MS,
        "freshness_bound_s": 60.0,
        "exactness": build_exactness(),
        "keepup": {
            "backlog_rows_max": 136192,
            "backlog_rows_p50": 0.0,
            "drain_s": 5.866,
        },
        "producer": {"behind_ms": 315, "errors": 0},
        "producer_bound": False,
        "reason": None,
        "run_valid": run_valid,
        "speed": 1.0,
        "state": "drained",
        "table": table,
        "warmup_s": 60.0,
    }


def _freshness() -> dict[str, object]:
    return {
        "bound_s": 60.0,
        "clock": "timestamp_ms",
        "clock_skew_suspected": False,
        "drained": True,
        "full": {"max_s": 27.445, "p50_s": 7.697, "p95_s": 11.76955, "p99_s": 24.425},
        "lag_series": {"timestamp_ms": [{"at_ms": EPOCH_MS, "lag_s": 0.0, "prefix": -1}]},
        "min_lag_s": 1.887,
        "missing_emit_prefixes": [],
        "verdict": True,
        "warmup_s": 60,
        "window": {"max_s": 27.445, "p50_s": 7.715, "p95_s": 14.345, "p99_s": 25.025},
    }


def build_geometry() -> dict[str, object]:
    return {
        "epoch_ms": EPOCH_MS,
        "offsets_s": [600, 1200],
        "at": {
            "600": {
                "snapshot_id": 11,
                "timestamp_ms": EPOCH_MS + 600_000,
                "live": {
                    "files": 4,
                    "rows": 400,
                    "bytes": 4096,
                    "size_quantiles": {"p50": 1024.0, "p90": 1024.0, "p99": 1024.0, "min": 1024, "max": 1024},
                    "small_file_share_32mib": 1.0,
                    "small_file_share_8mib": 1.0,
                    "log2_histogram": {"2^10..2^11": 4},
                },
                "per_commit": {
                    "commits": 2,
                    "files_added_quantiles": {"p50": 2.0, "p90": 2.0, "p99": 2.0},
                    "file_size_quantiles": {"p50": 1024.0, "p90": 1024.0, "p99": 1024.0},
                },
            },
            "1200": "absent",
        },
        "final": None,
    }


def build_run_dir(
    tmp_path: Path,
    *,
    run_id: str = "collect-flink-20260909T052508Z",
    run_valid: bool = True,
    spec: str = FLINK_SPEC,
    geometry: bool = True,
    publish_logs: bool = True,
    engine_image: bool = True,
    value_encoding: str = "avro",
    compression: str = "zstd",
) -> Path:
    run_dir = tmp_path / "runs" / run_id
    table = f"{TABLE_NAMESPACE}.t_{run_id.replace('-', '_')}"
    scores = run_dir / "scores"
    scores.mkdir(parents=True)
    (run_dir / "spec.yaml").write_text(spec)
    (run_dir / "engine-pods.json").write_text(
        json.dumps(
            {
                "items": [
                    {
                        "metadata": {"name": name, "labels": {"component": role}},
                        "status": {"phase": "Running"},
                        "spec": {
                            "containers": [
                                {
                                    "name": "flink",
                                    "resources": {"requests": {"cpu": cpu, "memory": memory}},
                                }
                            ]
                        },
                    }
                    for name, role, cpu, memory in (
                        ("jm", "jobmanager", "1", "2Gi"),
                        ("tm-1", "taskmanager", "2", "4Gi"),
                        ("tm-2", "taskmanager", "2", "4Gi"),
                    )
                ]
            }
        )
    )
    (run_dir / "facts.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "harness_commit": HARNESS_COMMIT,
                "bootstrap": "broker.invalid:9092",
                "topic": run_id,
                "corpus_uri": f"{BUCKET}/corpus/smoke-{SMOKE_CORPUS_HASH}",
                "schema_avsc_uri": f"{BUCKET}/corpus/smoke-{SMOKE_CORPUS_HASH}/schema.avsc",
                "value_encoding": value_encoding,
                "compression": compression,
                "catalog_props": {
                    "uri": "https://catalog.invalid/iceberg",
                    "warehouse": f"{BUCKET}/warehouse",
                    "token": "<redacted>",
                    "s3.secret-access-key": "${env:AWS_SECRET_ACCESS_KEY}",
                },
                "table": table,
                "partition": "identity(partition_key)",
                "ddl": None,
                "key_column": "user_id",
                "partition_assignment": "key",
                "kafka_partitions": 4,
                "epoch": EPOCH_MS / 1000,
            }
        )
    )
    (run_dir / "timeline.log").write_text("2026-09-09T05:25:08Z staged\n")
    (run_dir / "corpus.json").write_text(json.dumps(CORPUS_METADATA))
    if engine_image:
        (run_dir / "engine-image.json").write_text(
            json.dumps({"image": "registry.invalid/lakehouse-ingest-bench/flink:abc123", "digest": "sha256:deadbeef"})
        )
    (scores / "summary.json").write_text(json.dumps(_summary(run_valid=run_valid, table=table)))
    (scores / "freshness.json").write_text(json.dumps(_freshness()))
    (scores / "exactness.json").write_text(json.dumps(build_exactness()))
    (scores / "keepup_samples.jsonl").write_text(
        json.dumps({"at_ms": EPOCH_MS, "offered_rows": 0, "committed_rows": 0, "backlog_rows": 0}) + "\n"
    )
    # Include a new path field to check that redaction is independent of field names.
    (scores / "snapshots.jsonl").write_text(
        "\n".join(
            json.dumps(
                {
                    "snapshot_id": snapshot_id,
                    "operation": "append",
                    "timestamp_ms": EPOCH_MS + 600_000 * index,
                    "added_files": 2,
                    "added_rows": 200,
                    "added_bytes": 2048,
                    "prefix_after": index,
                    "data_files": [f"{BUCKET}/warehouse/ingest_bench/t_collect/data/{snapshot_id}-0.parquet"],
                }
            )
            for index, snapshot_id in enumerate((11, 12))
        )
        + "\n"
    )
    if geometry:
        (scores / "geometry.json").write_text(json.dumps(build_geometry()))
    if publish_logs:
        producer = run_dir / "producer"
        producer.mkdir()
        # Two batches spanning the 1800 s the run is costed over, 0.9 GB each,
        # the second acked 315 ms after it was due.
        (producer / "publish_log-0.jsonl").write_text(
            "\n".join(
                json.dumps(
                    {
                        "batch": batch,
                        "scheduled_ms": EPOCH_MS + batch * 1000,
                        "first_ack_ms": EPOCH_MS if batch == 0 else EPOCH_MS + 1315,
                        "last_ack_ms": EPOCH_MS + 400 if batch == 0 else LAST_ACK_MS,
                        "rows": 100,
                        "bytes": 900_000_000,
                        "errors": 0,
                    }
                )
                for batch in (0, 1)
            )
            + "\n"
            + json.dumps({"done": True, "shard": 0, "batches": 2})
            + "\n"
        )
    return run_dir


COLLECTED_AT = datetime(2026, 9, 20, 11, 30, 0, tzinfo=UTC)


def build_document(run_dir: Path, site_path: Path, *, variant: str = "hash") -> dict[str, object]:
    return run_json.build_run_json(
        run_dir,
        model.load_site(site_path),
        harness_version="9.9.9",
        collected_at=COLLECTED_AT,
        variant=variant,
    )


def render_table(documents: Sequence[tuple[Path, object]], *, workloads: Path = WORKLOADS, **kwargs: Path) -> str:
    """Render with the shipped tracks unless a test supplies its own workloads."""
    return render_results_table(documents, workloads=workloads, **kwargs)
