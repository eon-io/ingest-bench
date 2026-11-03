# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Score a live run by processing each table commit once.

Accumulate the tally, observations, and keep-up samples for both live and
final metrics. Reuse them at drain instead of rescanning the table. Write
artifacts incrementally for the gate and to preserve partial-run evidence.
"""

from __future__ import annotations

import json
import multiprocessing
import os
from collections.abc import Callable, Iterable
from concurrent.futures import FIRST_COMPLETED, Future, ProcessPoolExecutor, ThreadPoolExecutor, wait
from concurrent.futures.process import BrokenProcessPool
from dataclasses import asdict, dataclass, field, replace
from pathlib import Path
from statistics import median
from typing import TextIO, cast
from uuid import UUID

import numpy as np
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.io import FileIO
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata

from ingest_bench import uri
from ingest_bench.clock import Clock
from ingest_bench.corpus import metadata
from ingest_bench.producer import pacing, publish_log
from ingest_bench.scorer import freshness, live_rows, saturation
from ingest_bench.scorer.exactness import exactness_result
from ingest_bench.scorer.keepup import KeepupSample, keepup_summary, make_sample
from ingest_bench.scorer.keycoverage import KeyCoverage, upsert_exactness
from ingest_bench.scorer.manifest_cache import ManifestCache
from ingest_bench.scorer.snapshots import (
    AddedFile,
    TableHistoryError,
    added_files,
    check_table_schema,
    current_snapshot_ids,
    load_table,
    read_metadata,
    snapshots_in_order,
    visible_snapshot_ids,
)
from ingest_bench.scorer.tally import BatchTally, read_id_column, read_id_columns
from ingest_bench.specs.model import (
    AT_LEAST_ONCE,
    ENGINE_OWNED,
    EXACTLY_ONCE,
    EXACTNESS_MODES,
    HARNESS,
    LATEST_PER_KEY,
    SATURATION,
    SUSTAINABLE,
    validate_scoring_mode,
)

APPEND = "append"
# Upsert commits that add new versions; `replace` commits only rewrite, so they never move a stamp.
UPSERT_TALLIED = frozenset({APPEND, "overwrite"})

RUNNING = "running"
DRAINED = "drained"
IDLE_STOP = "idle_stop"
PRODUCER_BOUND = "producer_bound"
VOID = "void"
FAILED = "failed"

IDLE_STOP_REASON = "idle_stop_before_drain"
SCHEMA_MISMATCH_REASON = "table schema mismatch"

SNAPSHOTS_FILE = "snapshots.jsonl"
KEEPUP_SAMPLES_FILE = "keepup_samples.jsonl"
SUMMARY_FILE = "summary.json"
FRESHNESS_FILE = "freshness.json"
EXACTNESS_FILE = "exactness.json"
KEEPUP_FILE = "keepup.json"
CHECKPOINTS_DIR = "checkpoints"
# Not mirrored: the key map is rebuilt from the corpus wherever it is needed.
KEY_MAP_DIR = "keymap"
CHECKPOINTS_FILE = "checkpoints.json"
FINAL_CHECKPOINT = "final"

GRID_MS = 1000

# Retry transient input and upload failures with a bound. Persistent failures
# must abort scoring instead of resembling an idle engine.
MAX_CONSECUTIVE_IO_FAILURES = 5


@dataclass(frozen=True)
class ScoreArgs:
    """One run's scoring inputs, all of them facts the run was started with."""

    corpus_uri: str
    table: str
    catalog_props: dict[str, str]
    publish_logs_uri: str
    epoch_ms: int
    out_dir: Path
    poll_interval_s: float = 5.0
    idle_stop_s: float = 300.0
    warmup_s: int = 120
    freshness_bound_s: float = 180.0
    freshness_max_bound_s: float | None = None
    exactness_mode: str = EXACTLY_ONCE
    speed: float = 1.0
    behind_max_ms: int = 5000
    expected_publish_shards: int = 1
    gate_window_s: int = 60
    upload_prefix: str | None = None
    # Overlap ID-column reads to hide request latency. Limit concurrent arrays
    # to this width and release each after tallying.
    read_workers: int = 32
    # Above 1, use worker processes, capped at read_workers. Each adds an
    # interpreter; size the scorer pod's CPU and memory accordingly. Delete-aware
    # overwrite reads use threads to share masks rather than copying them per process.
    read_processes: int = 1
    # An engine-owned table may not exist until the first record arrives.
    table_managed_by: str = HARNESS
    scoring_mode: str = SUSTAINABLE
    measurement_s: int | None = None
    # Upsert runs: seconds after the epoch whose current snapshot is verified against a stamp checkpoint.
    sample_offsets_s: tuple[int, ...] = ()


@dataclass
class ScoreState:
    """Accumulated tally, observations, and status shared between polls.

    Track seen snapshots and applied files to make retries safe. Keep the observed
    ancestry to detect rollback or expiry; persist artifacts for aborted runs too.
    """

    args: ScoreArgs
    corpus: metadata.CorpusMetadata
    tally: BatchTally | KeyCoverage
    # When committed rows last advanced, and the count they reached.
    last_progress_ms: int
    progress_rows: int = 0
    observations: list[freshness.Observation] = field(default_factory=list)
    samples: list[KeepupSample] = field(default_factory=list)
    commits: list[saturation.Commit] = field(default_factory=list)
    saturation_result: dict[str, object] | None = None
    seen: set[int] = field(default_factory=set)
    table_uuid: UUID | None = None
    observed_ancestry: set[int] = field(default_factory=set)
    # Track applied files so retrying a partial commit cannot count them twice.
    applied_files: set[str] = field(default_factory=set)
    records: list[publish_log.PublishRecord] = field(default_factory=list)
    producer_windows: list[publish_log.ProducerWindow] = field(default_factory=list)
    sampled_producer_windows: list[publish_log.ProducerWindow] = field(default_factory=list)
    # Defer schema validation until the first successful table load.
    schema_mismatches: list[str] | None = None
    # Track table absence to avoid repeating the same log message every poll.
    table_absent: bool = False
    offer_ended: bool = False
    read_failures: int = 0
    mirror_failures: int = 0
    state: str = RUNNING
    reason: str | None = None
    aborted: bool = False
    result: freshness.FreshnessResult | None = None
    exactness: dict[str, object] | None = None
    read_pool: ProcessPoolExecutor | None = None
    manifest_cache: ManifestCache | None = None
    # The last snapshot processed, as (snapshot_id, timestamp_ms), and the sample offsets still ahead of it.
    last_snapshot: tuple[int, int] | None = None
    pending_offsets: list[int] = field(default_factory=list)
    checkpoints: list[dict[str, object]] = field(default_factory=list)
    absent_offsets: list[dict[str, object]] = field(default_factory=list)
    # Snapshots some metadata version made current, and the metadata-log files already read for them.
    visible: set[int] = field(default_factory=set)
    seen_metadata_logs: set[str] = field(default_factory=set)
    # The metadata document the previous poll read; the next document's log must still reach it.
    last_metadata_location: str | None = None

    def offered_rows(self) -> int:
        return sum(record.rows for record in self.records)

    def emit_ms(self) -> dict[int, int]:
        return publish_log.emit_times(self.records)

    def last_batch(self) -> int:
        """Return the final corpus batch, or final published batch after offer completion."""
        if self.offer_ended:
            return max((record.batch for record in self.records), default=-1)
        return self.corpus.batch_count - 1

    def producer_bound(self) -> bool:
        """Return whether producer lag or delivery failures invalidate the offer."""
        return any(window.behind_ms_max > self.args.behind_max_ms or window.errors for window in self.producer_windows)

    def run_valid(self) -> bool:
        """Require exactness, a valid offer, and the selected mode's measurements."""
        if self.args.scoring_mode == SATURATION:
            measurement_valid = (
                self.state == DRAINED
                and self.offer_ended
                and self.saturation_result is not None
                and bool(self.saturation_result["verdict"])
            )
        else:
            measurement_valid = self.result is not None and self.result.verdict
        return (
            not self.aborted
            and not self.schema_mismatches
            and measurement_valid
            and self.exactness is not None
            and bool(self.exactness["verdict"])
            and not self.producer_bound()
        )


def _append_line(path: Path, row: dict[str, object]) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n")
        handle.flush()


def _write_json(path: Path, document: dict[str, object]) -> None:
    """Replace an artifact atomically so live readers cannot see partial JSON."""
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _mirror(args: ScoreArgs, names: Iterable[str]) -> None:
    """Upload named local artifacts when an upload prefix is configured."""
    if args.upload_prefix is None:
        return
    for name in names:
        uri.write_bytes(uri.join(args.upload_prefix, name), (args.out_dir / name).read_bytes())


def _mirror_everything(args: ScoreArgs) -> None:
    """Upload all available artifacts, including after scorer failure."""
    _mirror(args, sorted(path.name for path in args.out_dir.iterdir() if path.is_file()))


def _as_int(value: object) -> int:
    return int(cast(int, value))


def _as_optional_float(value: object) -> float | None:
    return None if value is None else float(cast(float, value))


def read_keepup_samples(path: Path) -> list[KeepupSample]:
    """The keep-up samples the loop wrote, for a reader judging a run in flight."""
    samples: list[KeepupSample] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        raw = cast(dict[str, object], json.loads(line))
        try:
            samples.append(
                KeepupSample(
                    at_ms=_as_int(raw["at_ms"]),
                    offered_rows=_as_int(raw["offered_rows"]),
                    committed_rows=_as_int(raw["committed_rows"]),
                    backlog_rows=_as_int(raw["backlog_rows"]),
                    offered_rate=_as_optional_float(raw["offered_rate"]),
                    committed_rate=_as_optional_float(raw["committed_rate"]),
                    producer_windows=[
                        publish_log.ProducerWindow(
                            start_ms=_as_int(window["start_ms"]),
                            end_ms=_as_int(window["end_ms"]),
                            shard=_as_int(window["shard"]),
                            batches=_as_int(window["batches"]),
                            behind_ms_median=float(cast(float, window["behind_ms_median"])),
                            behind_ms_max=_as_int(window["behind_ms_max"]),
                            errors=_as_int(window["errors"]),
                        )
                        for window in cast(list[dict[str, object]], raw.get("producer_windows", []))
                    ],
                )
            )
        except KeyError as err:
            raise ValueError(f"{path} holds a sample missing key {err.args[0]!r}: {line}") from err
    return samples


def _offer_ended(args: ScoreArgs, done: set[int]) -> bool:
    """Whether every producer shard has published everything it selected."""
    expected = set(range(args.expected_publish_shards))
    surplus = sorted(done - expected)
    if surplus:
        raise ValueError(
            f"publish logs under {args.publish_logs_uri} carry a done marker for shard(s) {surplus}, past the "
            f"{args.expected_publish_shards} this run expects; pass --publish-shards to match the producer"
        )
    return done == expected


def _offer_end_ms(state: ScoreState) -> int | None:
    """When the last row of the offer was acknowledged, or None while it runs."""
    if not state.offer_ended or not state.records:
        return None
    return max(record.last_ack_ms for record in state.records)


def _drained_ms(state: ScoreState) -> int | None:
    """Scorer time when the prefix was first observed covering the last batch."""
    last = state.last_batch()
    if last < 0:
        return None
    return next((obs.first_seen_ms for obs in state.observations if obs.prefix >= last), None)


def _keepup(state: ScoreState) -> dict[str, object]:
    # Samples before the epoch precede the offer, so their zero backlog is not the run's.
    samples = [sample for sample in state.samples if sample.at_ms >= state.args.epoch_ms]
    return keepup_summary(samples, _offer_end_ms(state), _drained_ms(state))


def _live_lag_s(state: ScoreState) -> float | None:
    """Measure live lag from the latest sample and covered batch emit time.

    Use the run epoch until the first batch is covered, matching ``lag_series``.
    """
    if not state.samples:
        return None
    prefix = state.tally.prefix()
    # An unuploaded publish record makes lag unknown until a later poll.
    reference = state.args.epoch_ms if prefix < 0 else state.emit_ms().get(prefix)
    return None if reference is None else (state.samples[-1].at_ms - reference) / 1000


def _live_freshness(state: ScoreState) -> dict[str, int] | None:
    """Count the window's over-bound lag seconds so far, and how many fail the planned window's p95.

    The planned window runs from warm-up to the end of the corpus offer. A drain past that end
    lengthens the final window, raising the failing count by about 5% of the extra seconds.
    A commit made but not yet observed overstates the newest poll interval until the next poll.
    """
    if not state.samples:
        return None
    window_start_ms = state.args.epoch_ms + state.args.warmup_s * 1000
    planned_end_ms = pacing.scheduled_ms(state.args.epoch_ms, state.corpus.duration_s * 1000, state.args.speed)
    series = freshness.lag_series(
        state.observations,
        state.emit_ms(),
        state.args.epoch_ms,
        state.samples[-1].at_ms,
        GRID_MS,
        freshness.TIMESTAMP_CLOCK,
    )
    return {
        "window_over_bound_s": freshness.over_bound_samples(
            series, window_start_ms=window_start_ms, bound_s=state.args.freshness_bound_s
        ),
        "window_failing_s": freshness.p95_failing_count(max(1, (planned_end_ms - window_start_ms) // GRID_MS + 1)),
    }


def _producer_reason(state: ScoreState) -> str:
    """Which half of `producer_bound` was true, with the figure behind it."""
    faults: list[str] = []
    behind_ms = max((window.behind_ms_max for window in state.producer_windows), default=0)
    if behind_ms > state.args.behind_max_ms:
        faults.append(
            f"a batch was acknowledged {behind_ms} ms after it was due, over behind_max_ms {state.args.behind_max_ms}"
        )
    errors = sum(window.errors for window in state.producer_windows)
    if errors:
        faults.append(f"{errors} deliveries errored")
    return f"{PRODUCER_BOUND}: " + "; ".join(faults)


def _exactness_reason(exactness: dict[str, object]) -> str:
    """Summarize nonzero exactness fault counts."""
    faults = ("loss_rows", "duplicate_rows", "corrupt_batches", "invalid_rows", "unoffered_rows")
    return "exactness: " + ", ".join(f"{name}={exactness[name]}" for name in faults if exactness[name])


def _invalid_reason(state: ScoreState, result: freshness.FreshnessResult, exactness: dict[str, object]) -> str:
    """Choose the primary metric failure: producer, exactness, then freshness.

    Abort and schema failures are handled before this function.
    """
    if state.producer_bound():
        return _producer_reason(state)
    if not exactness["verdict"]:
        return _exactness_reason(exactness)
    if state.args.scoring_mode == SATURATION:
        reason = "measurement is unavailable" if state.saturation_result is None else state.saturation_result["reason"]
        return f"saturation: {reason}"
    return freshness.failure_reason(result, state.observations)


def _latest_producer_window(state: ScoreState) -> dict[str, object] | None:
    if not state.producer_windows or not state.samples:
        return None
    sampled_ms = max(sample.at_ms for sample in state.samples)
    closed = [window for window in state.producer_windows if window.end_ms <= sampled_ms]
    if not closed:
        return None
    latest_start = max(window.start_ms for window in closed)
    latest = [window for window in closed if window.start_ms == latest_start]
    return {
        "start_ms": latest_start,
        "end_ms": latest[0].end_ms,
        "shards": len(latest),
        "behind_ms_median": median(window.behind_ms_median for window in latest),
        "behind_ms_max": max(window.behind_ms_max for window in latest),
    }


def _write_summary(state: ScoreState) -> None:
    """Write the shared live and final summary format."""
    result = state.result
    document: dict[str, object] = {
        "run_valid": state.run_valid(),
        "scoring_mode": state.args.scoring_mode,
        "measurement_s": state.args.measurement_s,
        "saturation": state.saturation_result,
        "state": state.state,
        "reason": state.reason,
        "aborted": state.aborted,
        "prefix": state.tally.prefix(),
        "last_batch": state.last_batch(),
        "committed_rows": state.tally.committed_rows(),
        "offered_rows": state.offered_rows(),
        "backlog_rows": state.samples[-1].backlog_rows if state.samples else None,
        "lag_s": _live_lag_s(state),
        "freshness_live": _live_freshness(state),
        "snapshots": len(state.seen),
        "offer_ended": state.offer_ended,
        "producer": {
            "behind_ms": max((window.behind_ms_max for window in state.producer_windows), default=0),
            "errors": sum(window.errors for window in state.producer_windows),
            "window": _latest_producer_window(state),
        },
        "producer_bound": state.producer_bound(),
        "freshness": None if result is None else asdict(result),
        "exactness": state.exactness,
        "keepup": _keepup(state),
        "clock_skew_suspected": freshness.clock_skew_suspected(state.observations, state.emit_ms()),
        "epoch_ms": state.args.epoch_ms,
        "freshness_bound_s": state.args.freshness_bound_s,
        "freshness_max_bound_s": state.args.freshness_max_bound_s,
        "exactness_mode": state.args.exactness_mode,
        "warmup_s": state.args.warmup_s,
        "speed": state.args.speed,
        "table": state.args.table,
        "corpus_uri": state.corpus.uri,
        "corpus_hash": state.corpus.corpus_hash,
        "corpus_seed": state.corpus.seed,
        "corpus_generator_version": state.corpus.generator_version,
        "corpus_row_block": state.corpus.row_block,
    }
    _write_json(state.args.out_dir / SUMMARY_FILE, document)


def _load_inputs(args: ScoreArgs, clock: Clock, log: TextIO) -> ScoreState:
    validate_scoring_mode(args.scoring_mode, args.measurement_s)
    if args.scoring_mode == SATURATION and args.warmup_s < 0:
        raise ValueError("saturation warmup_s must be nonnegative")
    corpus = metadata.read(args.corpus_uri)
    if args.exactness_mode not in EXACTNESS_MODES:
        raise ValueError(f"unknown exactness mode: {args.exactness_mode!r}")
    manifest = metadata.read_manifest(args.corpus_uri)
    tally: BatchTally | KeyCoverage
    if args.exactness_mode == LATEST_PER_KEY:
        if corpus.schedule is None:
            raise ValueError(f"{LATEST_PER_KEY} scoring needs an upsert corpus, and {corpus.name} is append-only")
        (args.out_dir / CHECKPOINTS_DIR).mkdir(parents=True, exist_ok=True)
        print(f"KEY_MAP building {corpus.schedule.batch_count} batches", file=log, flush=True)
        corpus.schedule.build_key_map(args.out_dir / KEY_MAP_DIR / "keys.npy")
        tally = KeyCoverage(corpus.schedule, manifest)
    else:
        tally = BatchTally(manifest, corpus.p, track_unique=args.exactness_mode == AT_LEAST_ONCE)
    args.out_dir.mkdir(parents=True, exist_ok=True)
    # Start fresh artifacts so another scorer cannot append unrelated history.
    for name in (SNAPSHOTS_FILE, KEEPUP_SAMPLES_FILE):
        (args.out_dir / name).write_text("", encoding="utf-8")
    print(
        f"SCORER_START table={args.table} batches={corpus.batch_count} rows={corpus.row_count} "
        f"epoch_ms={args.epoch_ms}",
        file=log,
        flush=True,
    )
    return ScoreState(
        args=args,
        corpus=corpus,
        tally=tally,
        last_progress_ms=clock.now_ms(),
        pending_offsets=sorted(args.sample_offsets_s) if isinstance(tally, KeyCoverage) else [],
    )


def _write_checkpoint(state: ScoreState, name: str, offset_s: int | None) -> None:
    """Save the stamps as of the last processed snapshot: the live table that snapshot must hold."""
    assert isinstance(state.tally, KeyCoverage) and state.last_snapshot is not None
    np.save(state.args.out_dir / CHECKPOINTS_DIR / f"stamp-{name}.npy", state.tally.stamp)
    state.checkpoints.append(
        {
            "name": name,
            "offset_s": offset_s,
            "snapshot_id": state.last_snapshot[0],
            "timestamp_ms": state.last_snapshot[1],
            "prefix": state.tally.prefix(),
        }
    )


def _checkpoint_passed_offsets(state: ScoreState, next_timestamp_ms: int) -> None:
    """Checkpoint every sample offset the next snapshot passes, before its files move any stamp."""
    while state.pending_offsets and next_timestamp_ms > state.args.epoch_ms + state.pending_offsets[0] * 1000:
        offset_s = state.pending_offsets[0]
        if state.last_snapshot is None:
            state.absent_offsets.append({"offset_s": offset_s, "reason": "before the first commit"})
        else:
            _write_checkpoint(state, str(offset_s), offset_s)
        # Consume the offset only once recorded, so a failed write is retried with the snapshot's poll.
        state.pending_offsets.pop(0)


def _load_table(state: ScoreState, log: TextIO) -> Table | None:
    """Load the table, tolerating absence only for engine-owned tables.

    Such engines may create the namespace and table after the first record.
    Treat absence as an empty baseline so launch can start the producer. Missing
    harness-owned tables remain errors.
    """
    try:
        table = load_table(state.args.catalog_props, state.args.table)
    except (NoSuchTableError, NoSuchNamespaceError):
        if state.table_uuid is not None:
            raise TableHistoryError("table identity disappeared while scoring") from None
        if state.args.table_managed_by != ENGINE_OWNED:
            raise
        if not state.table_absent:
            state.table_absent = True
            print(
                f"TABLE_ABSENT table={state.args.table} managed_by={ENGINE_OWNED}: reading it as empty until "
                "the engine creates it",
                file=log,
                flush=True,
            )
        return None
    if state.table_absent:
        state.table_absent = False
        print(f"TABLE_PRESENT table={state.args.table}", file=log, flush=True)
    return table


def _apply_added_files(
    state: ScoreState, files: list[AddedFile], read_ids: Callable[[AddedFile], np.ndarray] | None = None
) -> int:
    """Read new ID columns concurrently and apply them to the tally serially.

    Counts and modular sums are order-independent. Track files only after their
    IDs are tallied so failed polls retry only unapplied files.
    """
    pending = [file for file in files if file.path not in state.applied_files]
    if not pending:
        return 0
    if state.args.read_processes > 1 and read_ids is None:
        return _apply_added_files_in_processes(state, pending)

    def read_file(file: AddedFile) -> np.ndarray:
        return read_id_column(file.path, file.file_format) if read_ids is None else read_ids(file)

    pool = ThreadPoolExecutor(max_workers=min(state.args.read_workers, len(pending)))
    try:
        width = state.args.read_workers
        reads = {pool.submit(read_file, file): file.path for file in pending[:width]}
        next_file = width
        while reads:
            completed, _ = wait(reads, return_when=FIRST_COMPLETED)
            read = completed.pop()
            path = reads.pop(read)
            state.tally.add_ids(read.result())
            state.applied_files.add(path)
            # Release the future and its array before scheduling a replacement read.
            del read
            if next_file < len(pending):
                file = pending[next_file]
                next_file += 1
                reads[pool.submit(read_file, file)] = file.path
    finally:
        # Cancel queued work on failure and finish running reads before retrying.
        pool.shutdown(wait=True, cancel_futures=True)
    return len(pending)


def _apply_added_files_in_processes(state: ScoreState, pending: list[AddedFile]) -> int:
    """Read bounded chunks in worker processes and tally each completed chunk.

    Keep at most ``read_workers`` files in flight across all processes. Each
    worker also holds its per-file arrays while concatenating the chunk.
    Track files after tallying so retries skip chunks already applied.
    """
    processes = min(state.args.read_processes, state.args.read_workers)
    if state.read_pool is None:
        # Spawn rather than fork: the parent holds S3 client threads.
        state.read_pool = ProcessPoolExecutor(max_workers=processes, mp_context=multiprocessing.get_context("spawn"))
    pool = state.read_pool
    chunk = state.args.read_workers // processes
    chunks = [pending[i : i + chunk] for i in range(0, len(pending), chunk)]
    futures: dict[Future[np.ndarray], list[AddedFile]] = {}
    next_chunk = 0
    try:
        for part in chunks[:processes]:
            futures[pool.submit(read_id_columns, [(file.path, file.file_format) for file in part], chunk)] = part
            next_chunk += 1
        while futures:
            completed, _ = wait(futures, return_when=FIRST_COMPLETED)
            future = completed.pop()
            del completed
            part = futures.pop(future)
            state.tally.add_ids(future.result())
            state.applied_files.update(file.path for file in part)
            # The future owns its result array; release both before the next read.
            del future
            if next_chunk < len(chunks):
                part = chunks[next_chunk]
                futures[pool.submit(read_id_columns, [(file.path, file.file_format) for file in part], chunk)] = part
                next_chunk += 1
    except Exception as error:
        # Finish or cancel outstanding reads before retrying. Reuse workers after
        # transient read errors; only a broken pool needs new interpreters.
        for future in futures:
            future.cancel()
        wait(futures)
        if isinstance(error, BrokenProcessPool):
            _close_read_pool(state)
        raise
    return len(pending)


def _close_read_pool(state: ScoreState) -> None:
    if state.read_pool is not None:
        state.read_pool.shutdown(wait=True, cancel_futures=True)
        state.read_pool = None


def _apply_ingestion_overwrite(
    state: ScoreState, files: list[AddedFile], document: TableMetadata, snapshot_id: int, io: FileIO
) -> int | None:
    assert isinstance(state.tally, BatchTally)
    incoming = live_rows.incoming_files(
        document, snapshot_id, io, state.tally, state.args.read_workers, manifest_cache=state.manifest_cache
    )
    if incoming is None:
        return None
    reader = live_rows.LiveIdReader(io, incoming.values(), state.args.read_workers)

    def read(file: AddedFile) -> np.ndarray:
        return reader.read(incoming[file.path])

    return _apply_added_files(state, files, read)


def _read_inputs(state: ScoreState, clock: Clock, log: TextIO) -> int:
    """Read producer logs and unseen table commits, returning the data files read.

    Tally appends and ingestion overwrites, applying their delete masks. Refuse
    ambiguous rewrites and record every supported operation in ``snapshots.jsonl``.
    """
    # Completion and records must come from the same fetched version of each shard log.
    logs = publish_log.read_logs(state.args.publish_logs_uri, read_workers=state.args.read_workers)
    offer_ended = _offer_ended(state.args, logs.done_shards)
    state.records = logs.batches
    state.producer_windows = publish_log.windows(
        logs.slices or logs.batches,
        epoch_ms=state.args.epoch_ms,
        window_s=state.args.gate_window_s,
        shards=state.args.expected_publish_shards,
    )
    # Update completion only after the matching records have been read.
    state.offer_ended = offer_ended
    table = _load_table(state, log)
    if table is None:
        # The empty-table sample provides the baseline launch waits for.
        return 0
    if state.schema_mismatches is None:
        state.schema_mismatches = check_table_schema(table.schema(), state.corpus)
    if state.schema_mismatches:
        # Do not publish measurements for a table that violates the corpus schema.
        return 0
    document = read_metadata(table)
    if state.table_uuid is not None and state.table_uuid != document.table_uuid:
        raise TableHistoryError("table identity changed while scoring")
    ancestry = current_snapshot_ids(document)
    if not state.observed_ancestry <= ancestry:
        raise TableHistoryError("snapshot history changed: a previously observed snapshot is no longer an ancestor")
    # Pin before scanning files: even a partial read must not survive a rollback.
    state.table_uuid = document.table_uuid
    state.observed_ancestry = ancestry
    # Every snapshot in this document was visible before any ID-column scans.
    first_seen_ms = clock.now_ms()
    if state.manifest_cache is None:
        state.manifest_cache = ManifestCache(table.io)
    read_files = 0
    upsert = isinstance(state.tally, KeyCoverage)
    pending = [
        info
        for info in snapshots_in_order(document)
        if info.snapshot_id in ancestry and info.snapshot_id not in state.seen
    ]
    # When each pending snapshot's commit became visible: the timestamp of the visible snapshot that ends it.
    commit_visible_ms: dict[int, int] = {}
    if upsert:
        previous = state.last_metadata_location
        # A first poll can tell which states were current only from the metadata log. When the log (or, with
        # none, the one document) starts after the epoch and anything committed since, the versions current at
        # the samples may be gone, including the one current at the epoch, so the samples cannot be placed.
        history_ms = document.metadata_log[0].timestamp_ms if document.metadata_log else document.last_updated_ms
        if (
            previous is None
            and history_ms > state.args.epoch_ms
            and any(info.timestamp_ms > state.args.epoch_ms for info in pending)
        ):
            raise TableHistoryError(
                "the first poll found the metadata log starting after the epoch, so commits a reader may have "
                "seen are missing from it; start the scorer before the epoch or raise "
                "write.metadata.previous-versions-max"
            )
        if previous not in (None, table.metadata_location) and previous not in {
            entry.metadata_file for entry in document.metadata_log
        }:
            # Commits whose metadata files fell out of the log may have been current; grouping them
            # as never-current could sample a state no reader saw, so refuse rather than guess.
            raise TableHistoryError(
                "the metadata log no longer reaches the previous poll's document: more commits landed between "
                "polls than write.metadata.previous-versions-max retains; raise it or poll more often"
            )
    state.visible |= visible_snapshot_ids(document, table.io, state.seen_metadata_logs)
    state.last_metadata_location = table.metadata_location
    group: list[int] = []
    for info in pending:
        group.append(info.snapshot_id)
        if info.snapshot_id in state.visible or info is pending[-1]:
            commit_visible_ms.update(dict.fromkeys(group, info.timestamp_ms))
            group = []
    starts_commit = True
    for info in pending:
        # Readers saw a commit's snapshots only once the commit became visible.
        commit_ms = commit_visible_ms.get(info.snapshot_id, info.timestamp_ms)
        if upsert and starts_commit:
            # Offsets that pass before a commit became visible sample the state before it.
            _checkpoint_passed_offsets(state, commit_ms)
        files = added_files(document, info.snapshot_id, state.manifest_cache)
        if not upsert and info.operation == "delete":
            raise TableHistoryError(f"unsupported append-track delete snapshot {info.snapshot_id}")
        observe_delivery = info.operation in UPSERT_TALLIED
        if not upsert and info.operation == "overwrite":
            applied = _apply_ingestion_overwrite(state, files, document, info.snapshot_id, table.io)
            observe_delivery = applied is not None
            if applied is not None:
                read_files += applied
        elif info.operation in (UPSERT_TALLIED if upsert else {APPEND}):
            read_files += _apply_added_files(state, files)
        if observe_delivery:
            # Record the observation first: retrying a failed artifact write may repeat
            # an idempotent observation, but must not duplicate an artifact line.
            state.observations.append(freshness.Observation(commit_ms, first_seen_ms, state.tally.prefix()))
            if state.args.scoring_mode == SATURATION:
                state.commits.append(saturation.Commit(commit_ms, first_seen_ms, state.tally.committed_rows()))
        _append_line(
            state.args.out_dir / SNAPSHOTS_FILE,
            {
                "snapshot_id": info.snapshot_id,
                "parent_id": info.parent_id,
                "operation": info.operation,
                "timestamp_ms": info.timestamp_ms,
                "observation_timestamp_ms": commit_ms if observe_delivery else None,
                "first_seen_ms": first_seen_ms,
                "added_files": len(files),
                "added_rows": sum(data_file.record_count for data_file in files),
                "added_bytes": sum(data_file.size_bytes for data_file in files),
                "prefix_after": state.tally.prefix(),
                "committed_rows": state.tally.committed_rows(),
            },
        )
        state.seen.add(info.snapshot_id)
        state.applied_files.clear()
        starts_commit = not upsert or info.snapshot_id in state.visible
        if starts_commit:
            state.last_snapshot = (info.snapshot_id, info.timestamp_ms)
    return read_files


def _poll_once(state: ScoreState, clock: Clock, log: TextIO) -> bool:
    """Poll inputs and return whether the read succeeded.

    On read failure, leave samples and summary unchanged so the gate can detect
    staleness. Log read duration and file count to diagnose slow polls.
    """
    started_ms = clock.now_ms()
    try:
        files = _read_inputs(state, clock, log)
    except TableHistoryError:
        # Retrying with the old tally could turn a rollback into a drained run.
        raise
    except Exception as error:
        state.read_failures += 1
        print(
            f"POLL_FAILED consecutive={state.read_failures} error={type(error).__name__}: {error}",
            file=log,
            flush=True,
        )
        if state.read_failures >= MAX_CONSECUTIVE_IO_FAILURES:
            raise
        return False
    state.read_failures = 0
    at_ms = clock.now_ms()
    poll_ms = at_ms - started_ms
    interval_ms = round(state.args.poll_interval_s * 1000)
    sample = make_sample(
        at_ms,
        state.offered_rows(),
        state.tally.committed_rows(),
        state.samples[-1] if state.samples else None,
    )
    previously_sampled = set(state.sampled_producer_windows)
    sample = replace(
        sample, producer_windows=[window for window in state.producer_windows if window not in previously_sampled]
    )
    state.samples.append(sample)
    state.sampled_producer_windows = state.producer_windows
    _append_line(state.args.out_dir / KEEPUP_SAMPLES_FILE, cast(dict[str, object], asdict(sample)))
    _write_summary(state)
    # Refresh the live gate inputs on every successful poll. A failed upload is
    # retried with the next poll's files; the gate sees stale samples meanwhile.
    try:
        _mirror(state.args, (KEEPUP_SAMPLES_FILE, SUMMARY_FILE))
    except Exception as error:
        state.mirror_failures += 1
        print(
            f"MIRROR_FAILED consecutive={state.mirror_failures} error={type(error).__name__}: {error}",
            file=log,
            flush=True,
        )
        if state.mirror_failures >= MAX_CONSECUTIVE_IO_FAILURES:
            raise
    else:
        state.mirror_failures = 0
    if poll_ms > interval_ms:
        print(
            f"SLOW_POLL poll_ms={poll_ms} files={files} interval_ms={interval_ms}",
            file=log,
            flush=True,
        )
    print(
        f"POLL t={(at_ms - state.args.epoch_ms) / 1000:.1f} prefix={state.tally.prefix()}/{state.last_batch()} "
        f"committed={sample.committed_rows} offered={sample.offered_rows} backlog={sample.backlog_rows} "
        f"snapshots={len(state.seen)} poll_ms={poll_ms} files={files}",
        file=log,
        flush=True,
    )
    return True


def _finalize(state: ScoreState, ending: str, clock: Clock, log: TextIO) -> int:
    """Write final metrics from accumulated state, including for incomplete runs."""
    emit_ms = state.emit_ms()
    end_ms = clock.now_ms()
    result = freshness.freshness_result(
        state.observations,
        emit_ms,
        epoch_ms=state.args.epoch_ms,
        end_ms=end_ms,
        last_batch=state.last_batch(),
        warmup_s=state.args.warmup_s,
        bound_s=state.args.freshness_bound_s,
        max_bound_s=state.args.freshness_max_bound_s,
        grid_ms=GRID_MS,
    )
    state.result = result
    offered_batches = {record.batch for record in state.records}
    if state.args.scoring_mode == SATURATION:
        assert state.args.measurement_s is not None
        offered_manifest = [record for record in state.tally.records if record.batch in offered_batches]
        offered_rows = sum(record.rows for record in offered_manifest)
        source_bytes_per_row = (
            sum(record.encoded_bytes for record in offered_manifest) / offered_rows if offered_rows else None
        )
        state.saturation_result = saturation.measure(
            state.commits,
            state.records,
            epoch_ms=state.args.epoch_ms,
            warmup_s=state.args.warmup_s,
            measurement_s=state.args.measurement_s,
            end_ms=end_ms,
            source_bytes_per_row=source_bytes_per_row,
        )
    # Exclude unoffered batches from shortened replays.
    if isinstance(state.tally, KeyCoverage):
        exactness = upsert_exactness(state.tally, offered_batches=offered_batches)
        _finish_checkpoints(state)
    else:
        exactness = exactness_result(state.tally, offered_batches=offered_batches, mode=state.args.exactness_mode)
    state.exactness = exactness
    if ending == VOID:
        # Schema failure takes precedence over a producer bottleneck.
        state.state = VOID
        state.reason = f"{SCHEMA_MISMATCH_REASON}: {'; '.join(state.schema_mismatches or [])}"
    else:
        # Report producer bottlenecks regardless of how the run ended.
        state.state = PRODUCER_BOUND if state.producer_bound() else ending
        if ending == IDLE_STOP:
            state.reason = IDLE_STOP_REASON
            # No verdict was reached, so the gate has nothing to judge the fleet on.
            state.aborted = True
    # Surface metric failures in the summary as well as detailed artifacts.
    if state.reason is None and not state.run_valid():
        state.reason = _invalid_reason(state, result, exactness)
    document: dict[str, object] = dict(asdict(result))
    # Publish both commit and observation clocks for offline re-evaluation.
    document["lag_series"] = {
        name: freshness.lag_series(
            state.observations,
            emit_ms,
            state.args.epoch_ms,
            freshness.series_end_ms(state.observations, end_ms, state.last_batch(), name),
            GRID_MS,
            name,
        )
        for name in (freshness.TIMESTAMP_CLOCK, freshness.FIRST_SEEN_CLOCK)
    }
    _write_json(state.args.out_dir / FRESHNESS_FILE, document)
    _write_json(state.args.out_dir / EXACTNESS_FILE, state.exactness)
    _write_json(state.args.out_dir / KEEPUP_FILE, _keepup(state))
    _write_summary(state)
    print(f"SCORER_DONE run_valid={state.run_valid()} state={state.state}", file=log, flush=True)
    return 0 if ending == DRAINED else 2


def _finish_checkpoints(state: ScoreState) -> None:
    """Checkpoint the final snapshot, record offsets no commit preceded or followed, and publish the set.

    An offset after the final commit samples the final snapshot, which the final check already covers.
    """
    if state.last_snapshot is not None:
        _write_checkpoint(state, FINAL_CHECKPOINT, None)
    absent = state.absent_offsets + [
        {"offset_s": offset_s, "reason": "after the final commit"} for offset_s in state.pending_offsets
    ]
    state.pending_offsets = []
    directory = state.args.out_dir / CHECKPOINTS_DIR
    document: dict[str, object] = {
        "offsets": sorted(state.args.sample_offsets_s),
        # A shortened offer sends a prefix of the corpus, which sets the final table the verifier expects.
        "offered_batches": state.last_batch() + 1,
        "checkpoints": cast(list[object], state.checkpoints),
        "absent": absent,
    }
    _write_json(directory / CHECKPOINTS_FILE, document)
    if state.args.upload_prefix is None:
        return
    for name in [CHECKPOINTS_FILE, *(f"stamp-{checkpoint['name']}.npy" for checkpoint in state.checkpoints)]:
        filesystem, path = uri.filesystem_for(uri.join(state.args.upload_prefix, CHECKPOINTS_DIR, name))
        filesystem.put_file(str(directory / name), path)


def run(args: ScoreArgs, clock: Clock, log: TextIO) -> int:
    """Score until drain, idle stop, or schema failure.

    Return 0 for drain and 2 for idle or schema failure. A drained run can still
    fail metric checks; read ``run_valid`` in the summary for that verdict.
    """
    state = _load_inputs(args, clock, log)
    try:
        while True:
            polled = _poll_once(state, clock, log)
            if state.schema_mismatches:
                return _finalize(state, VOID, clock, log)
            # Empty and rewrite commits are not progress, so they cannot hold off an idle stop.
            if state.tally.committed_rows() > state.progress_rows:
                state.progress_rows = state.tally.committed_rows()
                state.last_progress_ms = clock.now_ms()
            # A failed poll may have tallied part of a commit, so neither ending can be judged.
            if polled:
                if state.offer_ended and state.tally.prefix() >= state.last_batch():
                    return _finalize(state, DRAINED, clock, log)
                if clock.now_ms() - state.last_progress_ms >= args.idle_stop_s * 1000:
                    return _finalize(state, IDLE_STOP, clock, log)
            clock.sleep(args.poll_interval_s)
    except Exception as error:
        # Persist reader failure so the gate cannot accept the previous running state.
        state.state = FAILED
        state.aborted = True
        state.reason = f"scorer_failed: {type(error).__name__}"
        _write_summary(state)
        print(f"SCORER_FAILED error={type(error).__name__}: {error}", file=log, flush=True)
        raise
    finally:
        # Release the worker interpreters before the final artifact upload.
        _close_read_pool(state)
        # Upload artifacts on every exit path. Propagate upload failures.
        _mirror_everything(args)
