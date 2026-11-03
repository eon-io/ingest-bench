# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Measure file geometry across selected table snapshots.

A series of measurements exposes changes such as accumulating small files.
Read only the metadata document and its manifests, allowing measurement after
the engine and catalog have been removed.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass

import numpy as np
from pyiceberg.io import FileIO, load_file_io
from pyiceberg.manifest import DataFileContent
from pyiceberg.serializers import FromInputFile
from pyiceberg.table.metadata import TableMetadata

from ingest_bench.scorer.snapshots import SnapshotInfo, added_files, snapshot_by_id, snapshots_in_order

GEOMETRY_FILE = "geometry.json"

# What an offset the run never reached is reported as, rather than a point.
ABSENT = "absent"

MIB = 1024 * 1024

_QUANTILES = (("p50", 0.5), ("p90", 0.9), ("p99", 0.99))


@dataclass(frozen=True)
class DataFileInfo:
    """Manifest metadata for a data file.

    Keep ``added_snapshot_id`` unknown when the entry does not identify its origin.
    ``partition`` is the partition tuple's text, which is what groups files for
    the smallest-file-count calculation; a file cannot span partitions.
    """

    path: str
    size_bytes: int
    record_count: int
    added_snapshot_id: int | None
    partition: str


def open_metadata_document(location: str, props: dict[str, str]) -> tuple[TableMetadata, FileIO]:
    """Read a metadata document and configure IO without contacting a catalog.

    Accept copied filenames without the ``.metadata.json`` suffix required by
    StaticTable's path detection. Merge supplied and recorded table properties
    for subsequent file reads.
    """
    metadata = FromInputFile.table_metadata(load_file_io(properties=props, location=location).new_input(location))
    return metadata, load_file_io({**props, **metadata.properties}, location=location)


def data_files_at(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> list[DataFileInfo]:
    """Return every live data file at a snapshot, excluding delete files.

    Unlike ``added_files``, include inherited data files to measure the whole
    visible table.
    """
    files: list[DataFileInfo] = []
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            files.append(
                DataFileInfo(
                    path=data_file.file_path,
                    size_bytes=data_file.file_size_in_bytes,
                    record_count=data_file.record_count,
                    added_snapshot_id=entry.snapshot_id,
                    partition=str(data_file.partition),
                )
            )
    return files


def prefix_end(ordered: Sequence[SnapshotInfo], at_ms: int) -> int | None:
    """Find the last commit in the history prefix visible by ``at_ms``.

    Stop at the first later timestamp so clock disagreement cannot select a
    commit beyond the visible prefix.
    """
    end: int | None = None
    for index, info in enumerate(ordered):
        if info.timestamp_ms > at_ms:
            break
        end = index
    return end


def _quantiles(values: Sequence[int]) -> dict[str, float | None]:
    """Return linearly interpolated p50, p90, and p99, or nulls for empty input."""
    if not values:
        return {name: None for name, _ in _QUANTILES}
    array = np.array(values, dtype=np.float64)
    return {name: float(np.quantile(array, q)) for name, q in _QUANTILES}


def _log2_histogram(sizes: Sequence[int]) -> dict[str, int]:
    """Count files in power-of-two size bands, omitting empty bands."""
    bands = Counter(size.bit_length() - 1 for size in sizes)
    return {f"2^{band}..2^{band + 1}": bands[band] for band in sorted(bands)}


def _small_file_share(sizes: Sequence[int], limit: int) -> float | None:
    """Return the fraction of files below ``limit``.

    Weight by file count to reflect per-file open and footer costs.
    """
    if not sizes:
        return None
    return sum(1 for size in sizes if size < limit) / len(sizes)


def live_geometry(files: Sequence[DataFileInfo]) -> dict[str, object]:
    """Summarize live data-file sizes, counts, and row totals."""
    sizes = [info.size_bytes for info in files]
    return {
        "files": len(files),
        "rows": sum(info.record_count for info in files),
        "bytes": sum(sizes),
        "size_quantiles": {
            **_quantiles(sizes),
            "min": min(sizes, default=None),
            "max": max(sizes, default=None),
        },
        # Report two small-file thresholds to expose per-file overhead.
        "small_file_share_32mib": _small_file_share(sizes, 32 * MIB),
        "small_file_share_8mib": _small_file_share(sizes, 8 * MIB),
        "log2_histogram": _log2_histogram(sizes),
    }


def _per_commit(metadata: TableMetadata, io: FileIO, commits: Sequence[SnapshotInfo]) -> dict[str, object]:
    """Summarize files added by commits since the previous measurement point.

    This exposes write patterns that later compaction can hide in the live set.
    """
    added_counts: list[int] = []
    sizes: list[int] = []
    for info in commits:
        files = added_files(metadata, info.snapshot_id, io)
        added_counts.append(len(files))
        sizes.extend(added.size_bytes for added in files)
    return {
        "commits": len(commits),
        "files_added_quantiles": _quantiles(added_counts),
        "file_size_quantiles": _quantiles(sizes),
    }


def _point(
    metadata: TableMetadata, io: FileIO, info: SnapshotInfo, commits: Sequence[SnapshotInfo]
) -> dict[str, object]:
    return {
        "snapshot_id": info.snapshot_id,
        "timestamp_ms": info.timestamp_ms,
        "live": live_geometry(data_files_at(metadata, info.snapshot_id, io)),
        "per_commit": _per_commit(metadata, io, commits),
    }


def geometry_report(metadata: TableMetadata, io: FileIO, epoch_ms: int, offsets_s: Sequence[int]) -> dict[str, object]:
    """Measure geometry at strictly increasing offsets and at the end.

    Mark offsets beyond the final commit or before the first visible commit as
    ``absent``. Keep absent points from advancing the per-commit boundary so each
    commit belongs to at most one reported interval.
    """
    if any(later <= earlier for earlier, later in zip(offsets_s, offsets_s[1:], strict=False)):
        raise ValueError(f"the geometry offsets must ascend, got {list(offsets_s)}")
    ordered = snapshots_in_order(metadata)
    at: dict[str, object] = {str(offset_s): ABSENT for offset_s in offsets_s}
    report: dict[str, object] = {
        "epoch_ms": epoch_ms,
        "offsets_s": list(offsets_s),
        "at": at,
        "final": None,
    }
    if not ordered:
        return report
    last_ms = ordered[-1].timestamp_ms
    boundary = 0
    for offset_s in offsets_s:
        point_ms = epoch_ms + offset_s * 1000
        if point_ms > last_ms:
            continue
        reached = prefix_end(ordered, point_ms)
        if reached is None:
            continue
        at[str(offset_s)] = _point(metadata, io, ordered[reached], ordered[boundary : reached + 1])
        boundary = reached + 1
    report["final"] = _point(metadata, io, ordered[-1], ordered[boundary:])
    return report
