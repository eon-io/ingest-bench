# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Checked fields shared by result validation and table rendering.

Additional artifact fields are preserved. Nullable measurements represent partial
runs; publication requirements are checked separately by ``validate``.
"""

import math
from datetime import datetime
from types import UnionType
from typing import Literal, NotRequired, TypedDict, cast, get_args, get_origin, get_type_hints, is_typeddict

from ingest_bench.pricing import Pricing
from ingest_bench.specs.model import DEFAULT_WARMUP_S, LATEST_PER_KEY, SUSTAINABLE

SCHEMA_VERSION = 4
# Version 4 records the harness commit. Earlier results predate that rule and
# still parse without one.
LEGACY_SCHEMA_VERSIONS = frozenset({3})


class ProducerSpec(TypedDict):
    speed: NotRequired[float]
    seconds: NotRequired[float | None]
    behind_max_ms: NotRequired[int]


ExactnessMode = Literal["EXACTLY_ONCE", "AT_LEAST_ONCE", "LATEST_PER_KEY"]
ScoringMode = Literal["sustainable", "saturation"]


class ResultScoringSpec(TypedDict):
    exactness: NotRequired[ExactnessMode]
    mode: NotRequired[ScoringMode]
    warmup_s: NotRequired[int]
    measurement_s: NotRequired[int]


class ResultTableSpec(TypedDict):
    partition: NotRequired[str]
    properties: NotRequired[dict[str, str]]


class ResultCompactionSpec(TypedDict):
    driver: str


# The spec fields results are compared and placed by. The rest pass through.
class ResultSpec(TypedDict):
    corpus: str
    track: NotRequired[str]
    tier: NotRequired[Literal["reference", "tuned"]]
    table: NotRequired[ResultTableSpec]
    producer: NotRequired[ProducerSpec]
    scoring: NotRequired[ResultScoringSpec]
    compaction: NotRequired[ResultCompactionSpec]


class FleetRole(TypedDict):
    role: str
    count: int
    machine_type: str
    vcpu: float
    gib: float


class Corpus(TypedDict):
    name: str
    hash: str
    schema: str
    offered_bytes_per_s: float
    duration_s: float
    rows_per_s: float
    row_count: int
    batch_interval_ms: float
    partition_count: int
    alpha: float
    target_row_bytes: float


class EngineVersions(TypedDict):
    image: NotRequired[str]
    name: NotRequired[str]
    version: NotRequired[str]


class Kafka(TypedDict):
    partitions: int
    key: str | None
    partition_assignment: Literal["corpus", "key", "round_robin"]
    value_encoding: Literal["avro", "confluent", "json"]
    compression: Literal["gzip", "lz4", "none", "snappy", "zstd"]


class Run(TypedDict):
    run_id: str
    harness_commit: NotRequired[str | None]
    engine: str
    variant: str
    spec: ResultSpec
    fleet: list[FleetRole]
    site_pricing: Pricing
    corpus_hash: str | None
    corpus_seed: NotRequired[int | None]
    corpus_generator_version: NotRequired[str | None]
    corpus_row_block: NotRequired[int | None]
    corpus: Corpus | None
    engine_versions: EngineVersions | None
    kafka: Kafka
    table: NotRequired[str | None]
    topic: NotRequired[str | None]


class Quantiles(TypedDict):
    p50_s: float | None
    p95_s: float | None
    max_s: float | None


class Freshness(TypedDict):
    window: Quantiles


class Exactness(TypedDict):
    exact: bool
    mode: ExactnessMode
    verdict: bool
    loss_rows: int
    duplicate_rows: int
    corrupt_batches: int
    # Absent from records scored before the scorer counted rows outside the offer.
    invalid_rows: NotRequired[int]
    unoffered_rows: NotRequired[int]


class MeasurementQuantiles(TypedDict):
    p50: float | None
    p95: float | None
    max: float | None


class CommitSpan(TypedDict):
    start_ms: int
    end_ms: int
    duration_s: float
    committed_rows: int
    rows_per_s: float
    append_commits: int


class Saturation(TypedDict):
    verdict: bool
    reason: str | None
    start_ms: int
    end_ms: int
    duration_s: float
    committed_rows: int
    rows_per_s: float
    source_mb_per_s_estimate: float | None
    first_seen_rows_per_s: float
    first_seen_rate_difference_pct: NotRequired[float | None]
    commit_to_commit: NotRequired[CommitSpan | None]
    first_half_commit_to_commit: NotRequired[CommitSpan | None]
    second_half_commit_to_commit: NotRequired[CommitSpan | None]
    start_edge_gap_s: NotRequired[float | None]
    end_edge_gap_s: NotRequired[float | None]
    first_half_rows_per_s: float
    second_half_rows_per_s: float
    append_commits: int
    commit_gap_s: MeasurementQuantiles
    rows_per_commit: MeasurementQuantiles
    max_silence_s: float
    backlog_min_rows: int
    backlog_growth_rows_per_s: float


class Summary(TypedDict):
    run_valid: bool
    state: str
    reason: str | None
    speed: float
    freshness_bound_s: float
    exactness_mode: ExactnessMode
    exactness: NotRequired[Exactness | None]
    # Missing on historical results, which use sustainable scoring.
    scoring_mode: NotRequired[ScoringMode]
    measurement_s: NotRequired[int | None]
    warmup_s: NotRequired[float]
    epoch_ms: NotRequired[int]
    saturation: NotRequired[Saturation | None]


class Data(TypedDict):
    summary: Summary | None
    exactness: NotRequired[Exactness | None]


class Keepup(TypedDict):
    drain_s: float | None


class Producer(TypedDict):
    producer_bound: bool | None


class Cost(TypedDict):
    usd_per_hour: float | None
    run_hours: NotRequired[float | None]
    usd: NotRequired[float | None]


class Derived(TypedDict):
    freshness: Freshness | None
    exactness: Exactness | None
    keepup: Keepup | None
    producer: Producer
    cost: Cost


class SizeQuantiles(TypedDict):
    p50: float | None


class LiveFiles(TypedDict):
    size_quantiles: SizeQuantiles
    small_file_share_32mib: float | None


class FinalGeometry(TypedDict):
    live: LiveFiles


class Geometry(TypedDict):
    final: FinalGeometry | None


class Compaction(TypedDict):
    """Compaction metrics and the lower bound on proven row loss.

    `records_dropped` is null when the final row total is missing; zero means
    no deficit was found. It does not certify row preservation. Byte figures
    count all removed data files.
    """

    records_dropped: int | None
    floor_ratio: float | None
    # Removed bytes / written bytes, followed by written bytes / appended bytes.
    rewrite_share_of_writes: float | None
    written_per_ingested_byte: float | None
    converged: bool | None


class TerminalExactness(TypedDict):
    verdict: bool
    snapshot_id: NotRequired[int | None]


class CompactorStatus(TypedDict):
    healthy: bool


class ResultDocument(TypedDict):
    schema_version: int
    collected_at: str
    harness_version: str
    run: Run
    data: Data
    derived: Derived
    geometry: Geometry | None
    # Absent from results collected before the compaction layer existed.
    compaction: NotRequired[Compaction | None]
    # Preserve the whole backlog grid; summary statistics do not establish keep-up.
    campaign: NotRequired[dict[str, object] | None]
    # The selected report and all available alternatives identify its window scope.
    campaign_source: NotRequired[str | None]
    campaign_available: NotRequired[list[str]]
    terminal_exactness: NotRequired[TerminalExactness | None]
    # Upsert runs: the live-state check at sample snapshots, in place of terminal_exactness.
    terminal_upsert: NotRequired[dict[str, object] | None]
    # Upsert runs: the merge-on-read work the final table leaves readers, from its metadata.
    table_health: NotRequired[dict[str, object] | None]
    compactor_status: NotRequired[CompactorStatus | None]


def _saturation_diagnostic_failures(metrics: Saturation) -> list[str]:
    """Check additive diagnostics when present; older results remain readable."""
    failures = []
    start, end = metrics["start_ms"], metrics["end_ms"]
    middle = (start + end) // 2
    for field, span, lower, upper in (
        ("commit_to_commit", metrics.get("commit_to_commit"), start, end),
        ("first_half_commit_to_commit", metrics.get("first_half_commit_to_commit"), start, middle),
        ("second_half_commit_to_commit", metrics.get("second_half_commit_to_commit"), middle, end),
    ):
        if span is None:
            continue
        if not (
            lower < span["start_ms"] < span["end_ms"] <= upper
            and 2 <= span["append_commits"] <= metrics["append_commits"]
            and 0 <= span["committed_rows"] <= metrics["committed_rows"]
            and span["rows_per_s"] >= 0
            and math.isclose(span["duration_s"], (span["end_ms"] - span["start_ms"]) / 1000, rel_tol=1e-9)
            and math.isclose(span["rows_per_s"] * span["duration_s"], span["committed_rows"], rel_tol=1e-9)
        ):
            failures.append(f"saturation.{field} has inconsistent boundaries, counts or rate")
    for field, gap in (
        ("start_edge_gap_s", metrics.get("start_edge_gap_s")),
        ("end_edge_gap_s", metrics.get("end_edge_gap_s")),
    ):
        if gap is not None and not 0 <= gap <= metrics["max_silence_s"]:
            failures.append(f"saturation.{field} must be nonnegative and no greater than max_silence_s")
    span = metrics.get("commit_to_commit")
    if span is not None:
        for field, gap, expected in (
            ("start_edge_gap_s", metrics.get("start_edge_gap_s"), (span["start_ms"] - start) / 1000),
            ("end_edge_gap_s", metrics.get("end_edge_gap_s"), (end - span["end_ms"]) / 1000),
        ):
            if gap is not None and not math.isclose(gap, expected, rel_tol=1e-9):
                failures.append(f"saturation.{field} does not match commit_to_commit boundaries")
    difference = metrics.get("first_seen_rate_difference_pct")
    if difference is not None:
        rate = metrics["rows_per_s"]
        if rate <= 0 or not math.isclose(
            difference, 100 * (metrics["first_seen_rows_per_s"] - rate) / rate, rel_tol=1e-9, abs_tol=1e-9
        ):
            failures.append("saturation.first_seen_rate_difference_pct does not match the two rates")
    return failures


def scoring_failures(document: ResultDocument) -> list[str]:
    """Check scoring against the run spec and verify saturation evidence."""
    spec = document["run"]["spec"]
    configured = spec.get("scoring", {})
    mode = configured.get("mode", SUSTAINABLE)
    summary = document["data"]["summary"]
    failures: list[str] = []
    if summary is None:
        return failures
    if summary.get("scoring_mode", SUSTAINABLE) != mode:
        failures.append("data.summary.scoring_mode does not match run.spec.scoring.mode")
    if mode == SUSTAINABLE:
        if configured.get("measurement_s") is not None or summary.get("measurement_s") is not None:
            failures.append("measurement_s is only used in saturation mode")
        if summary.get("saturation") is not None:
            failures.append("sustainable mode must not contain saturation measurements")
        # Apply the spec model's defaults so an omitted field compares against the value the run used.
        if summary["speed"] != spec.get("producer", {}).get("speed", 1.0):
            failures.append("data.summary.speed does not match run.spec.producer.speed")
        if summary.get("warmup_s", DEFAULT_WARMUP_S) != configured.get("warmup_s", DEFAULT_WARMUP_S):
            failures.append("data.summary.warmup_s does not match run.spec.scoring.warmup_s")
        if summary["freshness_bound_s"] != configured.get("freshness_bound_s", 180.0):
            failures.append("data.summary.freshness_bound_s does not match run.spec.scoring.freshness_bound_s")
        return failures
    if summary["speed"] != spec.get("producer", {}).get("speed", 1.0):
        failures.append("data.summary.speed does not match run.spec.producer.speed")
    duration = configured.get("measurement_s")
    warmup = configured.get("warmup_s", DEFAULT_WARMUP_S)
    if duration is None or duration <= 0 or warmup < 0:
        failures.append("saturation requires positive measurement_s and nonnegative warmup_s")
        return failures
    if summary.get("measurement_s") != duration or summary.get("warmup_s") != warmup:
        failures.append("data.summary measurement_s/warmup_s does not match run.spec.scoring")
    corpus = document["run"]["corpus"]
    if corpus is not None:
        seconds = spec.get("producer", {}).get("seconds")
        replay_s = (
            min(corpus["duration_s"], seconds if seconds is not None else corpus["duration_s"]) / summary["speed"]
        )
        if warmup + duration >= replay_s:
            failures.append("saturation measurement window must end before the selected replay duration")
    metrics = summary.get("saturation")
    if metrics is None:
        if summary["run_valid"]:
            failures.append("data.summary.saturation is missing")
        return failures
    epoch = summary.get("epoch_ms")
    if (
        epoch is None
        or metrics["start_ms"] != epoch + warmup * 1000
        or metrics["end_ms"] != metrics["start_ms"] + duration * 1000
        or metrics["duration_s"] != duration
    ):
        failures.append("data.summary.saturation window does not match the configured epoch, warmup and duration")
    if summary["run_valid"] and not metrics["verdict"]:
        failures.append("run_valid is true despite failed saturation verdict")
    if not metrics["verdict"]:
        return failures
    failures.extend(_saturation_diagnostic_failures(metrics))
    if metrics["reason"] is not None:
        failures.append("passing saturation verdict has a failure reason")
    if metrics["append_commits"] < 3 or metrics["committed_rows"] <= 0 or metrics["backlog_min_rows"] <= 0:
        failures.append("saturation needs three nonempty appends, positive rows and positive backlog")
    largest = metrics["rows_per_commit"]["max"]
    if largest is None or metrics["backlog_growth_rows_per_s"] * duration <= largest:
        failures.append("saturation backlog growth must exceed one commit batching swing")
    if not math.isclose(metrics["rows_per_s"] * duration, metrics["committed_rows"], rel_tol=1e-9):
        failures.append("saturation rows_per_s does not match committed_rows / duration_s")
    for field in (
        "rows_per_s",
        "first_seen_rows_per_s",
        "first_half_rows_per_s",
        "second_half_rows_per_s",
        "source_mb_per_s_estimate",
        "max_silence_s",
    ):
        value = metrics[field]
        if value is not None and value < 0:
            failures.append(f"saturation.{field} must be nonnegative")
    for field, quantiles in (
        ("commit_gap_s", metrics["commit_gap_s"]),
        ("rows_per_commit", metrics["rows_per_commit"]),
    ):
        p50, p95, maximum = quantiles["p50"], quantiles["p95"], quantiles["max"]
        if p50 is None or p95 is None or maximum is None or not 0 <= p50 <= p95 <= maximum:
            failures.append(f"saturation.{field} needs ordered nonnegative quantiles")
    if summary["run_valid"]:
        exactness = document["derived"]["exactness"]
        if summary["state"] != "drained" or exactness is None or not exactness["verdict"]:
            failures.append("valid saturation result requires drain and exactness")
        if document["derived"]["producer"]["producer_bound"] is not False:
            failures.append("valid saturation result requires an on-schedule producer")
    return failures


def ingest_only_failures(document: ResultDocument) -> list[str]:
    """Reject undeclared rewrites using collected metadata, without scanning table rows."""
    spec = document["run"]["spec"]
    if spec.get("compaction") is not None or spec.get("scoring", {}).get("exactness") == LATEST_PER_KEY:
        return []
    raw = cast(dict[str, object], document)
    artifacts = raw.get("artifacts")
    snapshots = artifacts.get("snapshots") if isinstance(artifacts, dict) else None
    report = raw.get("compaction")
    # The saved metadata history includes commits after the scorer's last poll.
    final_history = report.get("file_counts") if isinstance(report, dict) else None
    for history in (snapshots, final_history):
        if isinstance(history, list) and any(
            isinstance(snapshot, dict) and snapshot.get("operation") not in (None, "append") for snapshot in history
        ):
            return [
                "ingest-only result contains non-append snapshots; declare compaction and verify the final live rows"
            ]
    if isinstance(report, dict):
        rates = report.get("rates")
        replaced = rates.get("files_replaced") if isinstance(rates, dict) else None
        if any(isinstance(value, (int, float)) and value > 0 for value in (report.get("records_dropped"), replaced)):
            return [
                "ingest-only result removed rows or replaced files; declare compaction and verify the final live rows"
            ]
    return []


def readiness_failures(document: ResultDocument) -> list[str]:
    """Return missing or failed evidence required by the selected run mode and compactor."""
    spec = cast(dict[str, object], document["run"]["spec"])
    compaction = spec.get("compaction")
    failures = scoring_failures(document) + ingest_only_failures(document)
    raw_document = cast(dict[str, object], document)
    scoring = spec.get("scoring")
    upsert = isinstance(scoring, dict) and scoring.get("exactness") == LATEST_PER_KEY
    if upsert:
        live_state = raw_document.get("terminal_upsert")
        if not isinstance(live_state, dict):
            failures.append("terminal_upsert is missing")
        elif live_state.get("verdict") is not True:
            failures.append("terminal_upsert.verdict is not true")
    if not isinstance(compaction, dict):
        return failures

    if not upsert:
        terminal = raw_document.get("terminal_exactness")
        if not isinstance(terminal, dict):
            failures.append("terminal_exactness is missing")
        elif terminal.get("verdict") is not True:
            failures.append("terminal_exactness.verdict is not true")

    if compaction.get("driver") not in ("inline", "external"):
        status = raw_document.get("compactor_status")
        if not isinstance(status, dict):
            failures.append("compactor_status is missing")
        elif status.get("healthy") is not True:
            failures.append("compactor_status.healthy is not true")
    return failures


def _check(value: object, expected: object, field: str) -> None:
    """Check the limited JSON types used above, retaining unknown fields."""
    origin = get_origin(expected)
    if expected is object:
        return
    if origin is NotRequired:
        _check(value, get_args(expected)[0], field)
    elif origin is UnionType:
        options = get_args(expected)
        if value is None and type(None) in options:
            return
        _check(value, next(option for option in options if option is not type(None)), field)
    elif origin is Literal:
        if value not in get_args(expected):
            raise ValueError(f"{field}: must be one of {list(get_args(expected))}, got {value!r}")
    elif is_typeddict(expected):
        if not isinstance(value, dict):
            raise ValueError(f"{field}: must be an object")
        for name, member_type in get_type_hints(expected, include_extras=True).items():
            if name not in value:
                if get_origin(member_type) is NotRequired:
                    continue
                raise ValueError(f"{field}.{name}: is missing")
            _check(value[name], member_type, f"{field}.{name}")
    elif origin is dict:
        if not isinstance(value, dict):
            raise ValueError(f"{field}: must be an object")
        key_type, value_type = get_args(expected)
        for key, entry in value.items():
            _check(key, key_type, f"{field} key")
            _check(entry, value_type, f"{field}.{key}")
    elif origin is list:
        if not isinstance(value, list):
            raise ValueError(f"{field}: must be an array")
        for index, item in enumerate(value):
            _check(item, get_args(expected)[0], f"{field}[{index}]")
    elif expected is float:
        if type(value) not in (int, float) or (isinstance(value, float) and not math.isfinite(value)):
            raise ValueError(f"{field}: must be a finite number")
    elif type(value) is not expected:
        raise ValueError(f"{field}: must be {getattr(expected, '__name__', expected)}")


def parse_result(value: object) -> ResultDocument:
    """Validate consumer fields and return the original document, including extras."""
    if not isinstance(value, dict):
        raise ValueError("document: must be an object")
    version = value.get("schema_version")
    if type(version) is not int or (version != SCHEMA_VERSION and version not in LEGACY_SCHEMA_VERSIONS):
        raise ValueError(f"schema_version: must be {SCHEMA_VERSION}, got {version!r}")
    _check(value, ResultDocument, "document")
    document = cast(ResultDocument, value)
    try:
        if datetime.fromisoformat(document["collected_at"]).tzinfo is None:
            raise ValueError("missing UTC offset")
    except ValueError as err:
        raise ValueError(f"collected_at: {err}") from err
    run = document["run"]
    pricing = run["site_pricing"]
    if "machine_types" in pricing:
        if not pricing["machine_types"]:
            raise ValueError("run.site_pricing.machine_types: must not be empty")
        if "vcpu_hour_usd" in pricing or "gib_hour_usd" in pricing:
            raise ValueError("run.site_pricing: choose machine_types or flat rates, not both")
        for machine, node in pricing["machine_types"].items():
            for key, value in (("vcpu", node["vcpu"]), ("gib", node["gib"]), ("usd_per_hour", node["usd_per_hour"])):
                if value <= 0:
                    raise ValueError(f"run.site_pricing.machine_types.{machine}.{key}: must be positive")
    elif "vcpu_hour_usd" not in pricing or "gib_hour_usd" not in pricing:
        raise ValueError("run.site_pricing: requires machine_types or both flat rates")
    engine_block = cast(dict[str, object], run["spec"]).get(run["engine"])
    if engine_block is not None and not isinstance(engine_block, dict):
        raise ValueError(f"document.run.spec.{run['engine']}: must be an object")
    if run["kafka"]["partitions"] < 1:
        raise ValueError("document.run.kafka.partitions: must be positive")
    corpus, summary = run["corpus"], document["data"]["summary"]
    if corpus is not None and corpus["offered_bytes_per_s"] <= 0:
        raise ValueError("run.corpus.offered_bytes_per_s: must be positive")
    if summary is not None and summary["speed"] <= 0:
        raise ValueError("data.summary.speed: must be positive")
    versions = run["engine_versions"]
    if run["engine"] == "external":
        if versions is None:
            raise ValueError("run.engine_versions: an external engine must disclose its name and version")
        for name, value in (("name", versions.get("name")), ("version", versions.get("version"))):
            if value is None:
                raise ValueError(f"run.engine_versions.{name}: is missing")
            if not value.strip():
                raise ValueError(f"run.engine_versions.{name}: must not be empty")
    elif versions is not None and "image" not in versions:
        raise ValueError("run.engine_versions.image: is missing")
    return document
