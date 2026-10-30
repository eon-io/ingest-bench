# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""How much merge-on-read work an upsert table leaves its readers, from metadata alone.

Upsert engines defer work to readers: equality deletes that every older data file in their
partition must be checked against, and deleted rows a reader still decodes before a deletion
vector drops them. The figures read only manifests and the corpus's expected live rows, so they
exist for every engine even when a full scan of the table cannot finish.
"""

from __future__ import annotations

import bisect
from collections import defaultdict
from itertools import accumulate
from typing import cast

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, FileFormat
from pyiceberg.table.metadata import TableMetadata

from ingest_bench.scorer.snapshots import snapshot_by_id

TABLE_HEALTH_FILE = "table-health.json"

# The partition an unpartitioned delete applies across: all of them.
_GLOBAL = (-1, "")


class _EqualityDeletes:
    """Equality deletes of one partition, by data sequence number, to count those newer than a data file."""

    def __init__(self, deletes: list[tuple[int, int]]) -> None:
        deletes.sort()
        self.sequences = [sequence for sequence, _ in deletes]
        self.rows_after = list(accumulate((rows for _, rows in reversed(deletes)), initial=0))[::-1]

    def newer_than(self, sequence: int) -> tuple[int, int]:
        """Files and rows of the deletes that apply to a data file: those with a higher sequence number."""
        first = bisect.bisect_right(self.sequences, sequence)
        return len(self.sequences) - first, self.rows_after[first]


def snapshot_files(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> dict[str, object]:
    """Data and delete files at a snapshot, and how many equality deletes apply to each data file.

    Deletion vectors share Puffin files, one manifest entry per vector, so files and bytes count each
    Puffin file once. Format v3 allows one vector per data file, so the entries count the data files
    carrying one. An equality delete applies to data files of its partition with a lower data sequence
    number, or to every partition when its spec is unpartitioned.
    """
    specs = metadata.specs()
    data_rows = 0
    data: list[tuple[tuple[int, str], int]] = []
    files: dict[str, dict[str, int]] = {"position": {}, "equality": {}, "deletion_vector": {}}
    rows = dict.fromkeys(files, 0)
    vectors = 0
    equality: dict[tuple[int, str], list[tuple[int, int]]] = defaultdict(list)
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            data_file = entry.data_file
            partition = (data_file.spec_id, str(data_file.partition))
            sequence = entry.sequence_number or 0
            if data_file.content == DataFileContent.DATA:
                data_rows += data_file.record_count
                data.append((partition, sequence))
                continue
            kind = "equality" if data_file.content == DataFileContent.EQUALITY_DELETES else "position"
            if data_file.file_format == FileFormat.PUFFIN:
                kind = "deletion_vector"
                vectors += 1
            files[kind][data_file.file_path] = data_file.file_size_in_bytes
            rows[kind] += data_file.record_count
            if kind == "equality":
                scope = _GLOBAL if specs[data_file.spec_id].is_unpartitioned() else partition
                equality[scope].append((sequence, data_file.record_count))
    delete_files: dict[str, dict[str, int]] = {
        kind: {"files": len(paths), "bytes": sum(paths.values()), "rows": rows[kind]} for kind, paths in files.items()
    }
    delete_files["deletion_vector"]["vectors"] = vectors
    return {
        "data_files": len(data),
        "data_file_rows": data_rows,
        "delete_files": delete_files,
        "data_files_with_deletion_vectors": vectors,
        "equality_fanout": _equality_fanout(data, equality),
    }


def _equality_fanout(
    data: list[tuple[tuple[int, str], int]], equality: dict[tuple[int, str], list[tuple[int, int]]]
) -> dict[str, float | int | None]:
    """Equality delete files a reader checks per data file, and the most delete rows one data file loads."""
    indexes = {scope: _EqualityDeletes(deletes) for scope, deletes in equality.items()}
    empty = _EqualityDeletes([])
    total = max_files = max_rows = 0
    for partition, sequence in data:
        local_files, local_rows = indexes.get(partition, empty).newer_than(sequence)
        global_files, global_rows = indexes.get(_GLOBAL, empty).newer_than(sequence)
        total += local_files + global_files
        max_files = max(max_files, local_files + global_files)
        max_rows = max(max_rows, local_rows + global_rows)
    return {"mean_files": total / len(data) if data else None, "max_files": max_files, "max_rows": max_rows}


def health_report(metadata: TableMetadata, io: FileIO, live_rows: int) -> dict[str, object]:
    """Read-cost figures for the table readers see: its current snapshot.

    ``live_rows`` is the corpus's expected live row count, so dead rows are known whatever kind of
    delete hides them.
    """
    snapshot_id = metadata.current_snapshot_id
    if snapshot_id is None:
        raise ValueError("the table has no current snapshot")
    files = snapshot_files(metadata, snapshot_id, io)
    data_rows = cast(int, files["data_file_rows"])
    return {
        "snapshot_id": snapshot_id,
        **files,
        "live_rows": live_rows,
        "dead_rows": data_rows - live_rows,
        "rows_per_live_row": data_rows / live_rows if live_rows else None,
    }
