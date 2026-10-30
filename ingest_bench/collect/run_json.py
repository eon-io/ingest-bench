# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Assemble a publishable run document from staged facts and recorded artifacts.

Use scorer and producer measurements, redact configured site details, and
list unavailable optional artifacts in ``missing``. Partial runs remain
collectable even when geometry or publish logs are absent.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.collect.redact import redact_catalog_props, redact_document
from ingest_bench.collect.schema import SCHEMA_VERSION
from ingest_bench.k8s.fleet import (
    COMPACTOR_PODS_FILE,
    PODS_FILE,
    compactor_fleet_from_pods,
    engine_fleet_from_pods,
)
from ingest_bench.pricing import resource_cost
from ingest_bench.producer import publish_log
from ingest_bench.producer.publish_log import PublishRecord
from ingest_bench.scorer import score
from ingest_bench.scorer.compaction import CAMPAIGN_FILE, COMPACTION_FILE
from ingest_bench.scorer.geometry import GEOMETRY_FILE
from ingest_bench.scorer.table_health import TABLE_HEALTH_FILE
from ingest_bench.specs.compactors import fleet_for as compactor_fleet_for
from ingest_bench.specs.engines import fleet_for
from ingest_bench.specs.model import (
    EXACTLY_ONCE,
    KEY_NONE,
    LATEST_PER_KEY,
    FleetRole,
    RunSpec,
    SiteConfig,
    load_archived_run_spec,
)

RUN_JSON_FILE = "run.json"

SPEC_FILE = "spec.yaml"
FACTS_FILE = "facts.json"
TIMELINE_FILE = "timeline.log"
ENGINE_IMAGE_FILE = "engine-image.json"
CORPUS_FILE = "corpus.json"

SCORES_DIR = "scores"
PRODUCER_DIR = "producer"
TERMINAL_EXACTNESS_FILE = "terminal-exactness.json"
TERMINAL_UPSERT_FILE = "terminal-upsert.json"
COMPACTOR_STATUS_FILE = "compactor-status.json"
# Report directory names are caller-selected; rank reports by window span.
CAMPAIGN_GLOB = "campaign*"

# Use a pattern because shard counts vary; also report it for missing logs.
PUBLISH_LOG_GLOB = f"{publish_log.LOG_NAME_PREFIX}*{publish_log.LOG_NAME_SUFFIX}"

# Match the run timeline format.
_COLLECTED_AT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_MS_PER_HOUR = 3_600_000

# Select published metrics explicitly so additions require a schema decision.
_KEEPUP_FIGURES = ("drain_s", "backlog_rows_max", "backlog_rows_p50")

# Keep detailed violations in the full artifact under `data`.
_VIOLATIONS = "violations"


def _score_path(name: str) -> str:
    return f"{SCORES_DIR}/{name}"


def _campaign_reports(run_dir: Path) -> tuple[list[tuple[Path, dict[str, object]]], list[str]]:
    """Return current-contract reports, widest configured span first, and every report's directory name.

    Directory names do not identify the widest report, so ties break by name.

    Exclude reports written against the retired verdict contract. Their verdicts
    use obsolete checks, and raw byte counts in their reasons can trigger the
    public-surface scanner's cloud account ID rule. If no current report remains,
    collection lists the campaign as a missing input.
    """
    paths = sorted((run_dir / SCORES_DIR).glob(f"{CAMPAIGN_GLOB}/{CAMPAIGN_FILE}"))
    names = [path.parent.name for path in paths]

    current: list[tuple[Path, dict[str, object]]] = []
    for path in paths:
        try:
            document = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError):
            continue
        windows = document.get("windows") if isinstance(document, dict) else None
        # `basis`, which every report under the current contract carries, is
        # what separates the two generations.
        if isinstance(windows, dict) and "basis" in windows:
            current.append((path, cast(dict[str, object], document)))

    def coverage(entry: tuple[Path, dict[str, object]]) -> tuple[float, str]:
        path, document = entry
        windows = cast(dict[str, object], document["windows"])
        count, width = windows.get("windows"), windows.get("window_s")
        span = (
            float(count) * float(width) if isinstance(count, (int, float)) and isinstance(width, (int, float)) else 0.0
        )
        # Break ties by name so collection is deterministic.
        return (-span, path.parent.name)

    return sorted(current, key=coverage), names


def _read_json(path: Path) -> dict[str, object]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path}: must be a JSON object")
    return cast(dict[str, object], value)


def _read_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{line_number}: must be a JSON object")
        rows.append(value)
    return rows


@dataclass
class _Inputs:
    """Run directory and accumulated missing optional inputs."""

    run_dir: Path
    missing: list[str] = field(default_factory=list)

    def required(self, relative: str) -> Path:
        path = self.run_dir / relative
        if not path.exists():
            raise ValueError(
                f"{path} does not exist; a run is collected from its {SPEC_FILE} and its {FACTS_FILE}, which "
                "staging writes together"
            )
        return path

    def optional(self, relative: str) -> Path | None:
        path = self.run_dir / relative
        if path.exists():
            return path
        self.missing.append(relative)
        return None

    def publish_logs(self) -> list[Path]:
        paths = sorted((self.run_dir / PRODUCER_DIR).glob(PUBLISH_LOG_GLOB))
        if not paths:
            paths = sorted(self.run_dir.glob(PUBLISH_LOG_GLOB))
        if not paths:
            self.missing.append(f"{PRODUCER_DIR}/{PUBLISH_LOG_GLOB}")
        return paths


# ---------------------------------------------------------------------------
# What the run was
# ---------------------------------------------------------------------------


def run_fleet(spec: RunSpec) -> list[FleetRole]:
    """Return the declared external fleet or derive managed roles from engine knobs."""
    if spec.is_external():
        return list(spec.fleet)
    return list(cast(Sequence[FleetRole], fleet_for(spec.engine).fleet(spec)))


def compaction_fleet(spec: RunSpec) -> list[FleetRole]:
    """Return compactor resources in addition to the ingestion fleet.

    Inline compaction already uses the engine fleet and adds no separate roles.
    """
    compaction = spec.compaction
    if compaction is None or compaction.is_inline():
        return []
    if compaction.is_external():
        return list(compaction.fleet)
    return list(cast(Sequence[FleetRole], compactor_fleet_for(compaction.driver).fleet(spec)))


def engine_versions(spec: RunSpec, image: dict[str, object] | None) -> dict[str, object] | None:
    """Return managed image provenance or the external engine's declared version."""
    if spec.external is not None:
        return {"name": spec.external.name, "version": spec.external.version, "notes": spec.external.notes}
    if image is None:
        return None
    return {"image": image["image"], "digest": image["digest"]}


def corpus_fields(metadata: dict[str, object]) -> dict[str, object]:
    """Copy the workload identity and scale recorded by corpus generation."""
    try:
        effective = metadata["effective_preset"]
        if not isinstance(effective, dict):
            raise ValueError("corpus.json.effective_preset: must be an object")
        return {
            "name": metadata["name"],
            "hash": metadata["corpus_hash"],
            "schema": metadata["schema_name"],
            "offered_bytes_per_s": metadata["offered_bytes_per_s"],
            "duration_s": metadata["duration_s"],
            "rows_per_s": metadata["rows_per_s"],
            "row_count": metadata["row_count"],
            "batch_interval_ms": metadata["batch_interval_ms"],
            "partition_count": effective["partition_count"],
            "alpha": effective["alpha"],
            "target_row_bytes": metadata.get("target_row_bytes", effective["target_row_bytes"]),
        }
    except KeyError as err:
        raise ValueError(f"corpus.json: missing required field {err.args[0]!r}") from err


def _identity_field(
    metadata: dict[str, object] | None,
    summary: dict[str, object] | None,
    *,
    metadata_name: str,
    summary_name: str,
) -> object | None:
    """Prefer corpus metadata, falling back to the scorer's recorded identity."""
    if metadata is not None and metadata_name in metadata:
        return metadata[metadata_name]
    return None if summary is None else summary.get(summary_name)


def resolved_kafka(spec: RunSpec, facts: dict[str, object]) -> dict[str, object]:
    """Return the Kafka workload that staging resolved for this run."""
    spec_key = None if spec.kafka.key == KEY_NONE else spec.kafka.key
    return {
        "partitions": facts.get("kafka_partitions", spec.kafka.partitions),
        # Presence matters: a staged null explicitly means unkeyed records.
        "key": facts["key_column"] if "key_column" in facts else spec_key,
        "partition_assignment": facts.get("partition_assignment", spec.kafka.partition_assignment),
        "value_encoding": facts.get("value_encoding", spec.kafka.value_encoding),
        # Launch resolves compression from the spec rather than facts.json.
        "compression": spec.producer.compression,
    }


# ---------------------------------------------------------------------------
# The derived figures
# ---------------------------------------------------------------------------


def freshness_figures(freshness: dict[str, object]) -> dict[str, object]:
    """Return window and full-run lag quantiles, plus clock-skew diagnostics."""
    return {
        "window": freshness["window"],
        "full": freshness["full"],
        "clock_skew_suspected": freshness["clock_skew_suspected"],
        "min_lag_s": freshness["min_lag_s"],
    }


def normalized_exactness(exactness: dict[str, object]) -> dict[str, object]:
    """Supply the default SLA and strict verdict when those fields are absent."""
    normalized = {**exactness}
    normalized.setdefault("mode", EXACTLY_ONCE)
    normalized.setdefault("verdict", normalized["exact"])
    return normalized


def exactness_figures(exactness: dict[str, object]) -> dict[str, object]:
    """The tally's figures, without the violation list."""
    return {key: value for key, value in exactness.items() if key != _VIOLATIONS}


def keepup_figures(summary: dict[str, object]) -> dict[str, object]:
    """Copy keep-up metrics from the scorer summary.

    Drain time cannot be reconstructed exactly from the periodic sample series.
    """
    keepup = cast(dict[str, object], summary["keepup"])
    return {name: keepup[name] for name in _KEEPUP_FIGURES}


def published_summary(summary: dict[str, object]) -> dict[str, object]:
    """Copy the summary metrics selected for publication and resolve SLA defaults."""
    keepup = cast(dict[str, object], summary["keepup"])
    published = {
        **summary,
        "keepup": {name: value for name, value in keepup.items() if name != "absorbed_at_offer_end"},
    }
    published.setdefault("exactness_mode", EXACTLY_ONCE)
    embedded = published.get("exactness")
    if isinstance(embedded, dict):
        published["exactness"] = normalized_exactness(embedded)
    return published


def publish_log_summary(path: Path, records: list[PublishRecord]) -> dict[str, object]:
    """Summarize a producer shard's acknowledgements, totals, and completion status.

    The raw per-batch logs remain under the runs prefix. A missing ``done`` trailer
    means the shard did not finish its selected batches.
    """
    return {
        "shard": publish_log.shard_index(path.name),
        "batches": len(records),
        "first_scheduled_ms": min((record.scheduled_ms for record in records), default=None),
        "first_ack_ms": min((record.first_ack_ms for record in records), default=None),
        "last_ack_ms": max((record.last_ack_ms for record in records), default=None),
        "bytes": sum(record.bytes for record in records),
        "rows": sum(record.rows for record in records),
        "behind_ms_max": publish_log.behind_ms(records),
        "errors": sum(record.errors for record in records),
        "done": publish_log.shard_done(path),
    }


def producer_figures(records: list[PublishRecord], summary: dict[str, object] | None) -> dict[str, object]:
    """Summarize offered throughput and delivery failures from publish logs.

    Measure rate over the acknowledgement interval. Without logs, use the scorer's
    lag and error totals and leave rate unknown. Always retain the scorer's
    ``producer_bound`` verdict.
    """
    bound = None if summary is None else summary["producer_bound"]
    if records:
        acked_ms = max(record.last_ack_ms for record in records) - min(record.first_ack_ms for record in records)
        offered_bytes = sum(record.bytes for record in records)
        return {
            "behind_ms_max": publish_log.behind_ms(records),
            "errors": sum(record.errors for record in records),
            "effective_offered_rate_bytes_per_s": offered_bytes / (acked_ms / 1000) if acked_ms > 0 else None,
            "producer_bound": bound,
        }
    scorer = None if summary is None else cast(dict[str, object], summary["producer"])
    return {
        "behind_ms_max": None if scorer is None else scorer["behind_ms"],
        "errors": None if scorer is None else scorer["errors"],
        "effective_offered_rate_bytes_per_s": None,
        "producer_bound": bound,
    }


def usd_per_hour(fleet: Sequence[FleetRole], site: SiteConfig) -> float:
    return sum(role.count * resource_cost(role.vcpu, role.gib, role.machine_type, site.pricing) for role in fleet)


def run_end_ms(records: list[PublishRecord], snapshots: list[dict[str, object]]) -> int | None:
    """Return the latest acknowledgement or table commit, including drain time.

    Use whichever records are available; return ``None`` if neither is present.
    """
    candidates = [record.last_ack_ms for record in records]
    candidates.extend(int(cast(int, row["timestamp_ms"])) for row in snapshots)
    return max(candidates, default=None)


def cost_figures(
    fleet: Sequence[FleetRole],
    site: SiteConfig,
    *,
    epoch_ms: int | None,
    end_ms: int | None,
    fleet_complete: bool = True,
) -> dict[str, object]:
    """Calculate Kubernetes fleet cost using site prices.

    Local runs have no resource cost. Duration is available independently when
    both start and end times are recorded.
    """
    per_hour = None if site.kubernetes is None or not fleet or not fleet_complete else usd_per_hour(fleet, site)
    hours = None if epoch_ms is None or end_ms is None else (end_ms - epoch_ms) / _MS_PER_HOUR
    return {
        "usd_per_hour": per_hour,
        "run_hours": hours,
        "usd": None if hours is None or per_hour is None else per_hour * hours,
    }


# ---------------------------------------------------------------------------
# The document
# ---------------------------------------------------------------------------


def build_run_json(
    run_dir: Path,
    site: SiteConfig,
    *,
    harness_version: str,
    collected_at: datetime,
    variant: str,
) -> dict[str, object]:
    """Build a publishable document from ``run_dir``.

    Read prices and redaction roots from ``site``; read run facts and measurements
    from the directory.
    """
    inputs = _Inputs(run_dir)
    spec_path = inputs.required(SPEC_FILE)
    spec = load_archived_run_spec(spec_path)
    # Read the staged YAML directly so omitted defaults remain omitted.
    spec_document = cast(dict[str, object], yaml.safe_load(spec_path.read_text(encoding="utf-8")))
    facts = _read_json(inputs.required(FACTS_FILE))

    timeline_path = inputs.optional(TIMELINE_FILE)
    image_path = inputs.optional(ENGINE_IMAGE_FILE)
    corpus_path = inputs.optional(CORPUS_FILE)
    summary_path = inputs.optional(_score_path(score.SUMMARY_FILE))
    freshness_path = inputs.optional(_score_path(score.FRESHNESS_FILE))
    exactness_path = inputs.optional(_score_path(score.EXACTNESS_FILE))
    geometry_path = inputs.optional(_score_path(GEOMETRY_FILE))
    compaction_path = inputs.optional(_score_path(COMPACTION_FILE))
    terminal_exactness_path = None
    terminal_upsert_path = None
    table_health_path = None
    compactor_status_path = None
    campaign_path = None
    campaign: dict[str, object] | None = None
    campaign_names: list[str] = []
    upsert = spec.scoring.exactness == LATEST_PER_KEY
    if upsert:
        # Live-state evidence replaces the append-only proof, compacted or not.
        terminal_upsert_path = inputs.optional(_score_path(TERMINAL_UPSERT_FILE))
        table_health_path = inputs.optional(_score_path(TABLE_HEALTH_FILE))
    if spec.compaction is not None:
        if not upsert:
            terminal_exactness_path = inputs.optional(_score_path(TERMINAL_EXACTNESS_FILE))
        # Preserve the offered-load series alongside final compaction geometry.
        found, campaign_names = _campaign_reports(run_dir)
        if found:
            campaign_path, campaign = found[0]
        else:
            inputs.missing.append(f"{SCORES_DIR}/{CAMPAIGN_GLOB}/{CAMPAIGN_FILE}")
        if spec.compaction.is_managed():
            compactor_status_path = inputs.optional(_score_path(COMPACTOR_STATUS_FILE))
    keepup_samples_path = inputs.optional(_score_path(score.KEEPUP_SAMPLES_FILE))
    snapshots_path = inputs.optional(_score_path(score.SNAPSHOTS_FILE))
    log_paths = inputs.publish_logs()

    image = None if image_path is None else _read_json(image_path)
    corpus_metadata = None if corpus_path is None else _read_json(corpus_path)
    corpus = None if corpus_metadata is None else corpus_fields(corpus_metadata)
    summary = None if summary_path is None else _read_json(summary_path)
    freshness = None if freshness_path is None else _read_json(freshness_path)
    exactness = None if exactness_path is None else normalized_exactness(_read_json(exactness_path))
    geometry = None if geometry_path is None else _read_json(geometry_path)
    compaction = None if compaction_path is None else _read_json(compaction_path)
    terminal_exactness = None if terminal_exactness_path is None else _read_json(terminal_exactness_path)
    terminal_upsert = None if terminal_upsert_path is None else _read_json(terminal_upsert_path)
    table_health = None if table_health_path is None else _read_json(table_health_path)
    compactor_status = None if compactor_status_path is None else _read_json(compactor_status_path)
    snapshots = [] if snapshots_path is None else _read_jsonl(snapshots_path)
    publish_logs: list[dict[str, object]] = []
    records: list[PublishRecord] = []
    for log_path in log_paths:
        shard_records = publish_log.read(log_path)
        publish_logs.append(publish_log_summary(log_path, shard_records))
        records.extend(shard_records)

    epoch = facts["epoch"]
    # A staged but unlaunched run has no epoch and can still be collected.
    epoch_ms = None if epoch is None else round(float(cast(float, epoch)) * 1000)
    pods_path = None
    compactor_pods_path = None
    fleet_complete = True
    if site.kubernetes is not None and not spec.is_external():
        pods_path = inputs.optional(PODS_FILE)
        if pods_path is None:
            fleet = []
            fleet_complete = False
        else:
            fleet = engine_fleet_from_pods(spec, _read_json(pods_path))
    else:
        fleet = run_fleet(spec)
    if site.kubernetes is not None and spec.compaction is not None and spec.compaction.is_managed():
        compactor_pods_path = inputs.optional(COMPACTOR_PODS_FILE)
        if compactor_pods_path is None:
            compactor_fleet = []
            fleet_complete = False
        else:
            compactor_fleet = compactor_fleet_from_pods(spec, _read_json(compactor_pods_path))
    else:
        compactor_fleet = compaction_fleet(spec)
    fleet = list(fleet) + list(compactor_fleet)

    artifacts: dict[str, object] = {"spec": SPEC_FILE, "facts": FACTS_FILE}
    for name, path in (
        ("timeline", timeline_path),
        ("engine_image", image_path),
        ("corpus", corpus_path),
        ("engine_pods", pods_path),
        ("compactor_pods", compactor_pods_path),
        ("summary", summary_path),
        ("freshness", freshness_path),
        ("exactness", exactness_path),
        ("geometry", geometry_path),
        ("compaction", compaction_path),
        ("campaign", campaign_path),
        ("terminal_exactness", terminal_exactness_path),
        ("terminal_upsert", terminal_upsert_path),
        ("table_health", table_health_path),
        ("compactor_status", compactor_status_path),
        ("keepup_samples", keepup_samples_path),
    ):
        if path is not None:
            artifacts[name] = str(path.relative_to(run_dir))
    # Scorer snapshots stop at the last poll and may omit later commits. Use
    # compaction.file_counts for added bytes across the saved metadata history.
    artifacts["snapshots"] = snapshots
    # Summarize the larger per-batch logs; retain raw logs under the runs prefix.
    artifacts["publish_logs"] = publish_logs

    kafka = resolved_kafka(spec, facts)
    corpus_hash = _identity_field(
        corpus_metadata,
        summary,
        metadata_name="corpus_hash",
        summary_name="corpus_hash",
    )

    document: dict[str, object] = {
        "schema_version": SCHEMA_VERSION,
        "collected_at": collected_at.astimezone(UTC).strftime(_COLLECTED_AT_FORMAT),
        "harness_version": harness_version,
        "run": {
            "run_id": facts["run_id"],
            # Runs staged before the harness recorded its commit have none.
            "harness_commit": facts.get("harness_commit"),
            "variant": variant,
            "spec": spec_document,
            "engine": spec.engine,
            "engine_versions": engine_versions(spec, image),
            "fleet": [asdict(role) for role in fleet],
            "site_pricing": site.pricing,
            "catalog_props": redact_catalog_props(cast(dict[str, str], facts["catalog_props"]), site),
            "epoch_ms": epoch_ms,
            "corpus": corpus,
            "corpus_hash": corpus_hash,
            # A corpus hash cannot be verified without its generator inputs.
            "corpus_seed": _identity_field(
                corpus_metadata,
                summary,
                metadata_name="seed",
                summary_name="corpus_seed",
            ),
            "corpus_generator_version": _identity_field(
                corpus_metadata,
                summary,
                metadata_name="generator_version",
                summary_name="corpus_generator_version",
            ),
            "corpus_row_block": _identity_field(
                corpus_metadata,
                summary,
                metadata_name="row_block",
                summary_name="corpus_row_block",
            ),
            "table": facts["table"],
            "topic": facts["topic"],
            "kafka": kafka,
        },
        "artifacts": artifacts,
        "data": {
            "summary": None if summary is None else published_summary(summary),
            "freshness": freshness,
            "exactness": exactness,
        },
        "derived": {
            "freshness": None if freshness is None else freshness_figures(freshness),
            "exactness": None if exactness is None else exactness_figures(exactness),
            "keepup": None if summary is None else keepup_figures(summary),
            "producer": producer_figures(records, summary),
            "cost": cost_figures(
                fleet,
                site,
                epoch_ms=epoch_ms,
                end_ms=run_end_ms(records, snapshots),
                fleet_complete=fleet_complete,
            ),
        },
        "geometry": geometry,
        "compaction": compaction,
        "missing": inputs.missing,
    }
    if spec.compaction is not None:
        document["campaign"] = campaign
        # Record the selected report and alternatives so its window scope is explicit.
        document["campaign_source"] = None if campaign_path is None else campaign_path.parent.name
        document["campaign_available"] = campaign_names
        if not upsert:
            document["terminal_exactness"] = terminal_exactness
        if spec.compaction.is_managed():
            document["compactor_status"] = compactor_status
    if upsert:
        document["terminal_upsert"] = terminal_upsert
        document["table_health"] = table_health
    return cast(dict[str, object], redact_document(document, site))
