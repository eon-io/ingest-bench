# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Measure final compaction geometry and offered-load backlog from table metadata.

Saved metadata and manifests support analysis after engine shutdown. File
lifecycles identify removals independently of commit labels; the campaign
grid preserves the time series needed to assess whether compaction kept up.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass
from math import ceil
from typing import cast

import numpy as np
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.table.metadata import TableMetadata

from ingest_bench.scorer.geometry import ABSENT, DataFileInfo, data_files_at, live_geometry, prefix_end
from ingest_bench.scorer.snapshots import snapshot_by_id, snapshots_in_order

COMPACTION_FILE = "compaction.json"


def partition_floor(files: Sequence[DataFileInfo], target_bytes: int) -> int:
    """The fewest files this data could occupy at ``target_bytes``.

    A data file cannot span partitions, so the floor is the sum of per-partition
    floors. Dividing the table's total bytes by the target instead understates
    it, and on a table with many partitions it understates it by a large factor.
    """
    if target_bytes <= 0:
        raise ValueError(f"target_bytes must be positive, got {target_bytes}")
    per_partition: Counter[str] = Counter()
    for info in files:
        per_partition[info.partition] += info.size_bytes
    return sum(max(1, ceil(total / target_bytes)) for total in per_partition.values())


# Snapshot summary properties holding the table's live totals.
TOTAL_DATA_FILES = "total-data-files"
TOTAL_RECORDS = "total-records"
ADDED_DATA_FILES = "added-data-files"
ADDED_FILES_SIZE = "added-files-size"
DELETED_DATA_FILES = "deleted-data-files"

# The commit that puts new data in the table, as opposed to one that moves data
# the table already holds.
APPEND_OPERATION = "append"


@dataclass(frozen=True)
class FileCount:
    """The table's live data-file count at one commit.

    ``files`` is ``None`` where the summary omits the total: a commit that did
    not report its count is not a commit of zero files.
    """

    snapshot_id: int
    timestamp_ms: int
    operation: str
    files: int | None
    added_files: int
    # Includes commits after the scorer stopped polling, such as final rewrites.
    added_bytes: int


def _summary_int(metadata: TableMetadata, snapshot_id: int, key: str) -> int | None:
    """One summary property as an integer, or ``None`` where it is absent."""
    summary = snapshot_by_id(metadata, snapshot_id).summary
    if summary is None:
        return None
    value = summary[key]
    return None if value is None else int(value)


def file_count_series(metadata: TableMetadata) -> list[FileCount]:
    """Read live file counts and added bytes from each snapshot summary.

    The series follows commit order and requires no manifest walks.
    """
    return [
        FileCount(
            snapshot_id=info.snapshot_id,
            timestamp_ms=info.timestamp_ms,
            operation=info.operation,
            files=_summary_int(metadata, info.snapshot_id, TOTAL_DATA_FILES),
            added_files=_summary_int(metadata, info.snapshot_id, ADDED_DATA_FILES) or 0,
            # `or 0` reads an absent `added-files-size` as zero, which is right
            # for a commit that only removed files and wrong for a writer that
            # omits the key while adding some. Over 1,010 commits of this
            # campaign: 81 pure-delete commits where it is doing real work, and
            # none of the other kind. A writer that omitted it would understate
            # this series and the numerator of the amplification; catch that in
            # validation, by flagging a commit that added files and reported no
            # size, rather than by making this field nullable for a writer that
            # does not exist.
            added_bytes=_summary_int(metadata, info.snapshot_id, ADDED_FILES_SIZE) or 0,
        )
        for info in snapshots_in_order(metadata)
    ]


@dataclass(frozen=True)
class FileLife:
    """One data file's span in the table.

    ``removed_ms`` is ``None`` for a file still live at the last commit, and
    ``removed_by`` names the commit that took it away, which is what groups
    removals into the cycles a compactor ran.
    """

    path: str
    size_bytes: int
    record_count: int
    partition: str
    added_ms: int
    removed_ms: int | None
    removed_by: int | None = None


def _events(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> tuple[list[DataFileInfo], list[str]]:
    """Return data files added and paths removed by one commit.

    Manifests carried over unchanged from an earlier commit are skipped, which
    keeps the walk linear in the history rather than quadratic; a commit's own
    additions and removals live in the manifests it wrote. Deleted entries are
    only visible with ``discard_deleted=False``, and they are the whole point
    here: a removal is what distinguishes a rewritten file from a live one.
    """
    added: list[DataFileInfo] = []
    removed: list[str] = []
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        if manifest.added_snapshot_id is not None and manifest.added_snapshot_id != snapshot_id:
            continue
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=False):
            data_file = entry.data_file
            if entry.snapshot_id != snapshot_id or data_file.content != DataFileContent.DATA:
                continue
            if entry.status == ManifestEntryStatus.ADDED:
                added.append(
                    DataFileInfo(
                        path=data_file.file_path,
                        size_bytes=data_file.file_size_in_bytes,
                        record_count=data_file.record_count,
                        added_snapshot_id=snapshot_id,
                        partition=str(data_file.partition),
                    )
                )
            elif entry.status == ManifestEntryStatus.DELETED:
                removed.append(data_file.file_path)
    return added, removed


def _live_paths(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> frozenset[str]:
    return frozenset(file.path for file in data_files_at(metadata, snapshot_id, io))


def file_lifecycles(metadata: TableMetadata, io: FileIO) -> list[FileLife]:
    """Every data file the table ever held, with when it arrived and left.

    Rewrites are read from this rather than from commit operations because an
    operation label does not identify one: a rewrite may land as a removal
    commit followed by an addition commit, where neither half is labelled as a
    rewrite and the second half is labelled the same as ingest.
    """
    added_at: dict[str, DataFileInfo] = {}
    added_ms: dict[str, int] = {}
    removed_ms: dict[str, int] = {}
    removed_by: dict[str, int] = {}
    previous: tuple[int, frozenset[str]] | None = None
    ordered = snapshots_in_order(metadata)
    for index, info in enumerate(ordered):
        added, removed = _events(metadata, info.snapshot_id, io)
        if not removed and index > 0 and (_summary_int(metadata, info.snapshot_id, DELETED_DATA_FILES) or 0) > 0:
            # A rewrite that re-materializes its manifest as kept EXISTING plus new
            # ADDED entries, with no DELETED tombstone for the inputs it dropped,
            # declares those removals only in its summary. Then the inputs are
            # what was live before and is not live now, at the cost of a full
            # walk of both snapshots instead of this commit's own manifests.
            before_id = ordered[index - 1].snapshot_id
            if previous is None or previous[0] != before_id:
                previous = (before_id, _live_paths(metadata, before_id, io))
            current = (info.snapshot_id, _live_paths(metadata, info.snapshot_id, io))
            removed = sorted(previous[1] - current[1])
            previous = current
        for file_info in added:
            added_at[file_info.path] = file_info
            added_ms[file_info.path] = info.timestamp_ms
        for path in removed:
            if path not in removed_ms:
                removed_ms[path] = info.timestamp_ms
                removed_by[path] = info.snapshot_id
    return [
        FileLife(
            path=path,
            size_bytes=file_info.size_bytes,
            record_count=file_info.record_count,
            partition=file_info.partition,
            added_ms=added_ms[path],
            removed_ms=removed_ms.get(path),
            removed_by=removed_by.get(path),
        )
        for path, file_info in added_at.items()
    ]


def rewritten_bytes(lives: Sequence[FileLife]) -> int:
    """Bytes of files that were written and later replaced."""
    return sum(life.size_bytes for life in lives if life.removed_ms is not None)


def written_bytes(lives: Sequence[FileLife]) -> int:
    """Bytes of every data file the table ever held, appended or rewritten."""
    return sum(life.size_bytes for life in lives)


def rewrite_share_of_writes(lives: Sequence[FileLife]) -> float | None:
    """The share of everything written that was later rewritten away.

    Bounded by 1 by construction, so it is not an amplification and was named
    one until 2026-09-21: a reader seeing 0.62 took compaction to have cost
    less than the data, where the same run rewrote 810 GB against 550 GB
    appended. ``written_per_ingested_byte`` is the figure that word means.

    This one is still worth having: no fleet, no cloud and no price list enter
    it, so it compares across engines and across sites.
    """
    written = written_bytes(lives)
    if written == 0:
        return None
    return rewritten_bytes(lives) / written


def written_per_ingested_byte(metadata: TableMetadata) -> float | None:
    """Return total added bytes divided by bytes added in append commits.

    Commit labels distinguish ingestion from rewrites; file lifecycles alone
    do not identify which operation wrote a file.
    """
    return written_over_appended(
        (info.operation, _summary_int(metadata, info.snapshot_id, ADDED_FILES_SIZE) or 0)
        for info in snapshots_in_order(metadata)
    )


def written_over_appended(commits: Iterable[tuple[str, int]]) -> float | None:
    """The ratio over ``(operation, added bytes)`` pairs, oldest first.

    Reads 1.0 where an engine's rewrite outputs land under the append label,
    which is what pyiceberg's ``overwrite`` does: it commits a delete and then
    an append, and no label then separates the rewrite's output from arriving
    data. Both compared arms commit their rewrites as ``replace``.
    """
    appended = 0
    written = 0
    for operation, size in commits:
        written += size
        if operation == APPEND_OPERATION:
            appended += size
    return None if appended == 0 else written / appended


# Where the file-size target came from. A target read from the table's write
# property is the engine's write target, which an engine may compact away from.
TARGET_DECLARED = "declared"
TARGET_PROPERTY = "table-property"
TARGET_DEFAULT = "format-default"
TARGET_TABLE_PROPERTY = "write.target-file-size-bytes"
# Iceberg's own default, used where neither the caller nor the table names one.
TARGET_DEFAULT_BYTES = 536870912


def resolve_target(metadata: TableMetadata, declared: int | None) -> tuple[int, str]:
    """The file-size target to measure against, and where it came from.

    A declared target wins: an engine may compact to a size other than the one
    its writer aims at, and only the operator knows which. The table property
    is the next best evidence, and the format default is a last resort that the
    record names as such rather than presenting as the compactor's intent.
    """
    if declared is not None:
        if declared <= 0:
            raise ValueError(f"the declared target must be positive, got {declared}")
        return declared, TARGET_DECLARED
    stated = metadata.properties.get(TARGET_TABLE_PROPERTY)
    if stated is not None:
        return int(stated), TARGET_PROPERTY
    return TARGET_DEFAULT_BYTES, TARGET_DEFAULT


# How long the commit log must show no removal before compaction is called
# settled, and how close to the floor the live set must sit. Both are policy:
# a caller measuring a different cadence passes its own.
QUIET_HOLD_S = 300.0
MAX_FLOOR_RATIO = 1.1

# How far the reading got. A run states this rather than emitting nulls that
# would read as failures on an engine the deeper tiers never applied to.
TIER_EMPTY = "empty"
TIER_LIFECYCLE = "lifecycle"


def _ratio_at(
    metadata: TableMetadata,
    io: FileIO,
    *,
    target_file_bytes: int,
    epoch_ms: int,
    offsets_s: Sequence[int],
) -> dict[str, object]:
    """The floor ratio at each offset, so the sawtooth is read rather than a phase of it.

    A single terminal reading lands wherever the run happened to stop, which
    for a periodic compactor is anywhere between the trough just after a pass
    and the peak just before one. Sampling costs a manifest walk per offset,
    which is why it runs at the spec's handful of offsets and not per commit.
    """
    ordered = snapshots_in_order(metadata)
    sampled: dict[str, object] = {str(offset): ABSENT for offset in offsets_s}
    if not ordered:
        return sampled
    last_ms = ordered[-1].timestamp_ms
    for offset in offsets_s:
        at_ms = epoch_ms + offset * 1000
        # Past the last commit the run has nothing more to say, and repeating
        # the terminal reading at every later offset would look like several
        # measurements of a settled table rather than one.
        if at_ms > last_ms:
            continue
        reached = prefix_end(ordered, at_ms)
        if reached is None:
            continue
        snapshot_id = ordered[reached].snapshot_id
        live = data_files_at(metadata, snapshot_id, io)
        floor = partition_floor(live, target_file_bytes)
        sampled[str(offset)] = {
            "snapshot_id": snapshot_id,
            "files": len(live),
            "floor": floor,
            "floor_ratio": None if floor == 0 else len(live) / floor,
        }
    return sampled


def compaction_report(
    metadata: TableMetadata,
    io: FileIO,
    *,
    target_file_bytes: int,
    target_source: str,
    epoch_ms: int | None = None,
    offsets_s: Sequence[int] = (),
    observed_at_ms: int | None = None,
    quiet_hold_s: float = QUIET_HOLD_S,
    max_floor_ratio: float = MAX_FLOOR_RATIO,
) -> dict[str, object]:
    """Report final geometry, rewrite activity, and convergence.

    Geometry uses the final live set; rewrite bytes and rates use file
    lifecycles. Convergence requires both a near-floor file count and a quiet
    period measured against ``observed_at_ms``.
    """
    ordered = snapshots_in_order(metadata)
    if not ordered:
        return {
            "tier": TIER_EMPTY,
            "records_dropped": 0,
            "observed_at_ms": observed_at_ms,
            "target_file_bytes": target_file_bytes,
            "target_source": target_source,
            "commits": 0,
            "files": 0,
            "floor": 0,
            "floor_ratio": None,
            "rewritten_bytes": 0,
            "written_bytes": 0,
            "rewrite_share_of_writes": None,
            "written_per_ingested_byte": None,
            "live": live_geometry([]),
            "file_counts": [],
            "rates": rates([]),
            "quiet_for_s": None,
            "converged": None,
            "quiet_hold_s": quiet_hold_s,
            "max_floor_ratio": max_floor_ratio,
            "ratio_at": {},
        }
    latest_commit_ms = max(info.timestamp_ms for info in ordered)
    if observed_at_ms is not None and observed_at_ms < latest_commit_ms:
        raise ValueError(f"observation time {observed_at_ms}ms precedes the latest commit at {latest_commit_ms}ms")
    live = data_files_at(metadata, ordered[-1].snapshot_id, io)
    floor = partition_floor(live, target_file_bytes)
    lives = file_lifecycles(metadata, io)
    dropped = records_dropped(metadata)
    counts = file_count_series(metadata)
    ratio = None if floor == 0 else len(live) / floor
    quiet = None if observed_at_ms is None else quiet_for_s(lives, observed_at_ms)
    return {
        "tier": TIER_LIFECYCLE,
        "records_dropped": dropped,
        "observed_at_ms": observed_at_ms,
        "target_file_bytes": target_file_bytes,
        "target_source": target_source,
        "commits": len(ordered),
        "files": len(live),
        "floor": floor,
        "floor_ratio": ratio,
        "rewritten_bytes": rewritten_bytes(lives),
        # Count each observed file once, whether still live or later removed.
        # This equals `rewritten_bytes` plus what is still stored only while the
        # history holds the commits that added the live files, since the
        # lifecycle walk reads manifests written by snapshots in this metadata.
        # The benchmark requires that (`docs/adding-an-engine.md` rule 6, no
        # snapshot expiry before scoring), so the identity is a run contract
        # rather than a property of this code.
        "written_bytes": written_bytes(lives),
        "rewrite_share_of_writes": rewrite_share_of_writes(lives),
        "written_per_ingested_byte": written_per_ingested_byte(metadata),
        "live": live_geometry(live),
        "file_counts": [asdict(point) for point in counts],
        "rates": rates(lives),
        "quiet_for_s": quiet,
        "converged": converged(
            quiet_for_s=quiet, floor_ratio=ratio, quiet_hold_s=quiet_hold_s, max_ratio=max_floor_ratio
        ),
        "quiet_hold_s": quiet_hold_s,
        "max_floor_ratio": max_floor_ratio,
        "ratio_at": {}
        if epoch_ms is None
        else _ratio_at(metadata, io, target_file_bytes=target_file_bytes, epoch_ms=epoch_ms, offsets_s=offsets_s),
    }


def records_dropped(metadata: TableMetadata) -> int | None:
    """Return the proven row deficit, or ``None`` if the final total is missing.

    A lower final total proves that at least the deficit left the table. Equal
    totals do not prove preservation because a deletion can be hidden by later
    appends. Zero means no deficit was found, not that rows were preserved.
    Missing intermediate totals still permit a deficit between known points.
    """
    ordered = snapshots_in_order(metadata)
    counts = [_summary_int(metadata, info.snapshot_id, TOTAL_RECORDS) for info in ordered]
    if not counts:
        return 0
    if counts[-1] is None:
        return None
    known = [total for total in counts if total is not None]
    return max(known) - known[-1]


def rates(lives: Sequence[FileLife]) -> dict[str, object]:
    """Report file additions and removals per second over observed lifecycles.

    Additions include rewrite output. Rates are ``None`` for an empty or
    zero-length history; these totals alone do not establish keep-up.
    """
    written = len(lives)
    replaced = sum(1 for life in lives if life.removed_ms is not None)
    if not lives:
        return {
            "files_written": 0,
            "files_replaced": 0,
            "span_s": None,
            "files_written_per_s": None,
            "files_replaced_per_s": None,
        }
    first = min(life.added_ms for life in lives)
    last = max(life.removed_ms if life.removed_ms is not None else life.added_ms for life in lives)
    span_s = (last - first) / 1000
    return {
        "files_written": written,
        "files_replaced": replaced,
        "span_s": span_s,
        "files_written_per_s": None if span_s <= 0 else written / span_s,
        "files_replaced_per_s": None if span_s <= 0 else replaced / span_s,
    }


def converged(
    *, quiet_for_s: float | None, floor_ratio: float | None, quiet_hold_s: float, max_ratio: float
) -> bool | None:
    """Require a quiet period and a live file count within the configured ratio.

    Quietness alone also describes a stopped compactor. Return ``None`` when
    either input is unknown, including histories with no observed removals.
    """
    if quiet_for_s is None or floor_ratio is None:
        return None
    return quiet_for_s >= quiet_hold_s and floor_ratio <= max_ratio


def quiet_for_s(lives: Sequence[FileLife], as_of_ms: int) -> float | None:
    """Seconds since the last file was replaced, or ``None`` if none ever was."""
    removals = [life.removed_ms for life in lives if life.removed_ms is not None]
    if not removals:
        return None
    return (as_of_ms - max(removals)) / 1000


# Campaign measurements use a time grid to give commit gaps their full weight.
GRID_S = 60

# A file below this share of the target is small enough to be worth merging.
SMALL_FRACTION = 0.5

# Minimum small-file count per partition in the reported eligible backlog.
# Match the engine planner; other planner rules can also trigger rewrites.
MIN_INPUT_FILES = 5

# The measured window: skip a warmup, then read consecutive equal windows.
WARMUP_S = 600.0
WINDOW_S = 600.0
CAMPAIGN_WINDOWS = 3

# The N/F bound is echoed into the report and never compared: it is structural,
# so a healthy table under either planner sits at 4 to 6 and fails it.
MAX_FLOOR_RATIO_MEDIAN = 2.0
# The cycle floor is the opposite: the only term that can still fail a run, and
# the only thing standing between a dead compactor and a clean report.
MIN_REWRITE_CYCLES = 5

# Missing boundaries leave the corresponding phase unknown.
PHASE_OFFERED = "offered"
PHASE_DRAIN = "drain"
PHASE_SHUTDOWN = "shutdown"
PHASE_UNKNOWN = "unknown"


def classify_instant(at_ms: int, *, offer_end_ms: int | None, writers_stopped_ms: int | None) -> str:
    """Assign a sampled state to offered load, drain, shutdown, or unknown.

    Each boundary belongs to the phase ending there: the offer-end instant
    records the state produced by offered load. Missing boundaries leave the
    corresponding phase unknown.
    """
    if writers_stopped_ms is not None and offer_end_ms is not None and writers_stopped_ms < offer_end_ms:
        raise ValueError(f"the writers stopped at {writers_stopped_ms}ms, before the offer ended at {offer_end_ms}ms")
    if offer_end_ms is None:
        return PHASE_UNKNOWN
    if at_ms <= offer_end_ms:
        return PHASE_OFFERED
    if writers_stopped_ms is None:
        return PHASE_UNKNOWN
    return PHASE_DRAIN if at_ms <= writers_stopped_ms else PHASE_SHUTDOWN


@dataclass(frozen=True)
class RewriteCycle:
    """One commit that took data files out of the table.

    ``bytes_removed`` is the rewrite's input, which is only readable where the
    commit records its removals; see ``rewrite_input_bytes``.
    """

    snapshot_id: int
    timestamp_ms: int
    files_removed: int
    bytes_removed: int


def rewrite_cycles(lives: Sequence[FileLife]) -> list[RewriteCycle]:
    """Every commit that removed files, oldest first.

    Read from lifecycles rather than from a ``deleted-data-files`` summary for
    the reason the module reads rewrites from lifecycles at all: summary
    properties are optional, and a commit that omits its removal count is not
    a commit that removed nothing.
    """
    removed: Counter[int] = Counter()
    removed_bytes: Counter[int] = Counter()
    at_ms: dict[int, int] = {}
    for life in lives:
        if life.removed_by is None or life.removed_ms is None:
            continue
        removed[life.removed_by] += 1
        removed_bytes[life.removed_by] += life.size_bytes
        at_ms[life.removed_by] = life.removed_ms
    cycles = [
        RewriteCycle(
            snapshot_id=snapshot_id,
            timestamp_ms=at_ms[snapshot_id],
            files_removed=count,
            bytes_removed=removed_bytes[snapshot_id],
        )
        for snapshot_id, count in removed.items()
    ]
    return sorted(cycles, key=lambda cycle: (cycle.timestamp_ms, cycle.snapshot_id))


def _quantile(values: Sequence[float], quantile: float) -> float | None:
    """One linearly interpolated quantile, or ``None`` over no values."""
    if not values:
        return None
    return float(np.quantile(np.array(values, dtype=np.float64), quantile))


def small_file_bytes(target_file_bytes: int, small_fraction: float) -> int:
    """The size below which a file counts as small.

    At least one byte: a zero threshold would make every file large and the
    backlog permanently empty, which reads as a compactor that kept up.
    """
    if not 0 < small_fraction <= 1:
        raise ValueError(f"small_fraction must be within (0, 1], got {small_fraction}")
    return max(1, round(target_file_bytes * small_fraction))


def _ages_s(files: Sequence[DataFileInfo], added_ms: dict[str, int], at_ms: int) -> list[float]:
    """Each file's age at ``at_ms``, skipping files whose arrival is unknown."""
    return [(at_ms - added_ms[info.path]) / 1000 for info in files if info.path in added_ms]


def live_backlog(
    files: Sequence[DataFileInfo],
    added_ms: dict[str, int],
    *,
    at_ms: int,
    target_file_bytes: int,
    small_bytes: int,
    min_input_files: int,
) -> dict[str, object]:
    """Report live geometry and small-file backlog at one instant.

    ``floor`` is the per-partition file-count floor and ``excess`` is the
    live count minus that floor. Hot partitions hold multiple small files;
    cold partitions hold one. ``eligible`` applies only the supplied small-file
    threshold and minimum count, not every rule in an engine planner.
    """
    small = [info for info in files if info.size_bytes < small_bytes]
    per_partition: Counter[str] = Counter(info.partition for info in small)
    cold = {name for name, count in per_partition.items() if count == 1}
    hot = {name for name, count in per_partition.items() if count > 1}
    open_to_planner = {name for name, count in per_partition.items() if count >= min_input_files}
    eligible = [info for info in small if info.partition in open_to_planner]
    sizes = [float(info.size_bytes) for info in files]
    floor = partition_floor(files, target_file_bytes)
    return {
        "files": len(files),
        "floor": floor,
        "floor_ratio": None if floor == 0 else len(files) / floor,
        "excess": len(files) - floor,
        "bytes": sum(info.size_bytes for info in files),
        "partitions": len({info.partition for info in files}),
        "small_files": len(small),
        "small_bytes": sum(info.size_bytes for info in small),
        "oldest_small_age_s": max(_ages_s(small, added_ms, at_ms), default=None),
        "size_quantiles": {
            "p50": _quantile(sizes, 0.5),
            "p90": _quantile(sizes, 0.9),
            "max": max(sizes, default=None),
        },
        "hot": {
            "partitions": len(hot),
            "small_files": sum(count for name, count in per_partition.items() if name in hot),
            "small_bytes": sum(info.size_bytes for info in small if info.partition in hot),
        },
        "cold": {
            "partitions": len(cold),
            "small_files": len(cold),
            "small_bytes": sum(info.size_bytes for info in small if info.partition in cold),
        },
        "eligible": {
            "files": len(eligible),
            "bytes": sum(info.size_bytes for info in eligible),
            "partitions": len({info.partition for info in eligible}),
            "p95_age_s": _quantile(_ages_s(eligible, added_ms, at_ms), 0.95),
        },
    }


def grid_offsets_s(*, epoch_ms: int, last_ms: int, grid_s: int) -> list[int]:
    """Offsets every ``grid_s`` from the epoch through the last commit.

    Stop at the last commit: past it the table says nothing further, and
    repeating the terminal reading at every later offset would look like a
    series of measurements of a settled table rather than one measurement of
    where the run stopped.
    """
    if grid_s <= 0:
        raise ValueError(f"grid_s must be positive, got {grid_s}")
    if last_ms < epoch_ms:
        return []
    return list(range(0, (last_ms - epoch_ms) // 1000 + 1, grid_s))


def snapshot_grid(
    metadata: TableMetadata,
    io: FileIO,
    lives: Sequence[FileLife],
    *,
    target_file_bytes: int,
    epoch_ms: int,
    grid_s: int = GRID_S,
    small_fraction: float = SMALL_FRACTION,
    min_input_files: int = MIN_INPUT_FILES,
    offer_end_ms: int | None = None,
    writers_stopped_ms: int | None = None,
) -> list[dict[str, object]]:
    """The table's geometry on a wall-clock grid, one entry per instant.

    The snapshot at an instant ends the history prefix visible by then (see
    ``prefix_end``). A commit gap longer than a grid step carries the previous
    snapshot forward because its geometry still applies. An instant
    before the first commit has no reading and is ``null``, which is not a
    geometry of zero files and must never satisfy a bound.

    Sizes come from the snapshot's live files and ages from lifecycles, so
    both describe one live set. The live set is re-read only when the grid
    moves to a new snapshot; ages are recomputed at every instant, because a
    carried-forward snapshot still ages between commits.
    """
    ordered = snapshots_in_order(metadata)
    if not ordered:
        return []
    small_bytes = small_file_bytes(target_file_bytes, small_fraction)
    added_ms = {life.path: life.added_ms for life in lives}
    last_ms = max(info.timestamp_ms for info in ordered)
    held_id: int | None = None
    held: list[DataFileInfo] = []
    samples: list[dict[str, object]] = []
    for offset_s in grid_offsets_s(epoch_ms=epoch_ms, last_ms=last_ms, grid_s=grid_s):
        at_ms = epoch_ms + offset_s * 1000
        reached = prefix_end(ordered, at_ms)
        sample: dict[str, object] = {
            "offset_s": offset_s,
            "at_ms": at_ms,
            "phase": classify_instant(at_ms, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms),
            "snapshot_id": None,
            "live": None,
        }
        if reached is not None:
            snapshot_id = ordered[reached].snapshot_id
            if snapshot_id != held_id:
                held_id, held = snapshot_id, data_files_at(metadata, snapshot_id, io)
            sample["snapshot_id"] = snapshot_id
            sample["live"] = live_backlog(
                held,
                added_ms,
                at_ms=at_ms,
                target_file_bytes=target_file_bytes,
                small_bytes=small_bytes,
                min_input_files=min_input_files,
            )
        samples.append(sample)
    return samples


def _reading(sample: Mapping[str, object], *path: str) -> float | None:
    """One number out of a nested reading, or ``None`` where it is absent.

    An instant the history never reached carries no reading at all, and that
    has to stay distinct from a reading of zero all the way to the verdict.
    """
    current: object = sample
    for key in path:
        if not isinstance(current, dict):
            return None
        current = cast(dict[str, object], current).get(key)
    if isinstance(current, bool) or not isinstance(current, (int, float)):
        return None
    return float(current)


def _in_window(samples: Sequence[Mapping[str, object]], from_ms: int, to_ms: int) -> list[Mapping[str, object]]:
    """Select samples in the measured span, both ends included.

    There is one span and no adjacent windows to double count, so both ends
    close. The right end has to: the instant the offer ended is offered load,
    and on one published row it was the lowest reading of its window.
    """
    return [sample for sample in samples if from_ms <= cast(int, sample["at_ms"]) <= to_ms]


def _median_of(samples: Sequence[Mapping[str, object]], *path: str) -> float | None:
    values = [value for sample in samples if (value := _reading(sample, "live", *path)) is not None]
    return _quantile(values, 0.5)


def _beyond_span_s(offer_end_ms: int | None, last_offered_ms: int | None, to_ms: int) -> float:
    end = offer_end_ms if offer_end_ms is not None else last_offered_ms
    return 0.0 if end is None else max(0.0, (end - to_ms) / 1000)


def window_verdicts(
    samples: Sequence[Mapping[str, object]],
    cycles: Sequence[RewriteCycle],
    *,
    epoch_ms: int,
    offer_end_ms: int | None = None,
    warmup_s: float = WARMUP_S,
    window_s: float = WINDOW_S,
    windows: int = CAMPAIGN_WINDOWS,
    max_floor_ratio_median: float = MAX_FLOOR_RATIO_MEDIAN,
    min_rewrite_cycles: int = MIN_REWRITE_CYCLES,
) -> dict[str, object]:
    """Summarize the measured span and check the minimum rewrite-cycle count.

    Use offered samples when their phase is known, otherwise all samples.
    Too few rewrite cycles yields ``False``; otherwise the verdict is ``None``.
    Neither the cycle count nor the reported floor-ratio median proves keep-up.
    """
    if windows < 1:
        raise ValueError(f"windows must be at least one, got {windows}")
    if window_s <= 0:
        raise ValueError(f"window_s must be positive, got {window_s}")
    from_ms = epoch_ms + round(warmup_s * 1000)
    step_ms = round(window_s * 1000)
    to_ms = from_ms + step_ms * windows
    # Drain can reduce backlog without demonstrating keep-up under offered load.
    offered_known = any(sample.get("phase") == PHASE_OFFERED for sample in samples)
    if offered_known:
        samples = [sample for sample in samples if sample.get("phase") == PHASE_OFFERED]
    ratio_median = _median_of(_in_window(samples, from_ms, to_ms), "floor_ratio")
    # Exclude drain cycles. The grid boundary is conservative: it can omit
    # rewrites between the last offered sample and the actual offer end.
    last_offered_ms = max(int(cast(int, sample["at_ms"])) for sample in samples) if offered_known and samples else None
    measured = [
        cycle
        for cycle in cycles
        if from_ms <= cycle.timestamp_ms <= to_ms
        and cycle.files_removed > 0
        and (last_offered_ms is None or cycle.timestamp_ms <= last_offered_ms)
    ]
    reasons: list[str] = []
    if len(measured) < min_rewrite_cycles:
        reasons.append(
            f"{len(measured)} commits removed files in the measured window, fewer than the {min_rewrite_cycles} "
            "that show a compactor running"
        )
    # A minimum cycle count proves activity, not sufficient compaction capacity.
    # Keep geometry out of the verdict: planner policy affects N/F, while
    # backlog summaries depend on where windows fall in the compaction cycle.
    # A grid with no instant at all measured nothing, and nothing measured is
    # not a compactor that did not run. The same principle the removed terms
    # followed: absence of evidence is null, not false.
    passed = None if not samples else (False if len(measured) < min_rewrite_cycles else None)
    if passed is None:
        reasons.append("no term fired, which is not a pass: no term here reads whether the table kept up")
    return {
        "warmup_s": warmup_s,
        "window_s": window_s,
        "windows": windows,
        "max_floor_ratio_median": max_floor_ratio_median,
        "min_rewrite_cycles": min_rewrite_cycles,
        "measured_from_ms": from_ms,
        "measured_to_ms": to_ms,
        # Keep fixed window widths and disclose any remaining offered load.
        "offered_beyond_span_s": _beyond_span_s(offer_end_ms, last_offered_ms, to_ms),
        "basis": PHASE_OFFERED if offered_known else "all_instants",
        "floor_ratio_median": ratio_median,
        "rewrite_cycles": len(measured),
        # No measured removals leave input bytes unknown rather than zero.
        "rewrite_input_bytes": None if not measured else sum(cycle.bytes_removed for cycle in measured),
        "campaign_pass": passed,
        "reasons": reasons,
    }


def place_on_grid(
    events: Sequence[tuple[int, dict[str, object]]], *, epoch_ms: int, grid_s: int, offsets_s: Sequence[int]
) -> dict[str, list[dict[str, object]]]:
    """Group diagnostic events into grid cells starting at each sampled offset.

    Each cell spans from its offset through, but excluding, the next one.
    Events outside the supplied cells are omitted.
    """
    if grid_s <= 0:
        raise ValueError(f"grid_s must be positive, got {grid_s}")
    placed: dict[str, list[dict[str, object]]] = {str(offset): [] for offset in offsets_s}
    for at_ms, payload in events:
        cell = ((at_ms - epoch_ms) // 1000 // grid_s) * grid_s
        if (key := str(cell)) in placed:
            placed[key].append(payload)
    return placed


CAMPAIGN_FILE = "compaction-report.json"


def campaign_report(
    metadata: TableMetadata,
    io: FileIO,
    *,
    target_file_bytes: int,
    target_source: str,
    epoch_ms: int,
    offer_end_ms: int | None = None,
    writers_stopped_ms: int | None = None,
    grid_s: int = GRID_S,
    small_fraction: float = SMALL_FRACTION,
    min_input_files: int = MIN_INPUT_FILES,
    warmup_s: float = WARMUP_S,
    window_s: float = WINDOW_S,
    windows: int = CAMPAIGN_WINDOWS,
    max_floor_ratio_median: float = MAX_FLOOR_RATIO_MEDIAN,
    min_rewrite_cycles: int = MIN_REWRITE_CYCLES,
) -> dict[str, object]:
    """The streaming compaction reading for one arm of a campaign.

    Everything here comes from table metadata, so both arms are measured the
    same way whatever compacted them. Engine diagnostics are joined onto this
    grid by the caller; they are evidence about how the work was done, not
    about whether the table ended up in the shape the contract requires.
    """
    lives = file_lifecycles(metadata, io)
    samples = snapshot_grid(
        metadata,
        io,
        lives,
        target_file_bytes=target_file_bytes,
        epoch_ms=epoch_ms,
        grid_s=grid_s,
        small_fraction=small_fraction,
        min_input_files=min_input_files,
        offer_end_ms=offer_end_ms,
        writers_stopped_ms=writers_stopped_ms,
    )
    cycles = rewrite_cycles(lives)
    verdicts = window_verdicts(
        samples,
        cycles,
        epoch_ms=epoch_ms,
        offer_end_ms=offer_end_ms,
        warmup_s=warmup_s,
        window_s=window_s,
        windows=windows,
        max_floor_ratio_median=max_floor_ratio_median,
        min_rewrite_cycles=min_rewrite_cycles,
    )
    return {
        "epoch_ms": epoch_ms,
        "offer_end_ms": offer_end_ms,
        "writers_stopped_ms": writers_stopped_ms,
        "target_file_bytes": target_file_bytes,
        "target_source": target_source,
        "grid_s": grid_s,
        "small_fraction": small_fraction,
        "small_file_bytes": small_file_bytes(target_file_bytes, small_fraction),
        "min_input_files": min_input_files,
        "grid": samples,
        "cycles": [asdict(cycle) for cycle in cycles],
        "windows": verdicts,
        "campaign_pass": verdicts["campaign_pass"],
        "reasons": verdicts["reasons"],
    }
