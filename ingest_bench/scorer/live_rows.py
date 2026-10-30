# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Read snapshot-pinned live IDs, including position deletes and deletion vectors."""

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from itertools import islice

import numpy as np
import pyarrow as pa
from pyiceberg.io import FileIO
from pyiceberg.io.pyarrow import _read_deletes
from pyiceberg.table import DataScan, FileScanTask
from pyiceberg.table.metadata import TableMetadata

from ingest_bench.scorer.manifest_cache import ManifestCache
from ingest_bench.scorer.snapshots import TableHistoryError, snapshot_by_id
from ingest_bench.scorer.tally import BatchTally, read_id_column


def plan_files(document: TableMetadata, snapshot_id: int, io: FileIO) -> dict[str, FileScanTask]:
    scan = DataScan(document, io, snapshot_id=snapshot_id, selected_fields=("id",))
    tasks: dict[str, FileScanTask] = {}
    for task in scan.plan_files():
        path = task.file.file_path
        if path in tasks:
            raise TableHistoryError(f"snapshot {snapshot_id} lists a data file more than once")
        tasks[path] = task
    return tasks


class LiveIdReader:
    """Share delete masks while callers bound the data files being read concurrently."""

    def __init__(self, io: FileIO, tasks: Iterable[FileScanTask], read_workers: int = 32) -> None:
        files = {delete.file_path: delete for task in tasks for delete in task.delete_files}
        self._deletes: dict[str, dict[str, pa.ChunkedArray]] = {}
        if files:
            with ThreadPoolExecutor(max_workers=min(read_workers, len(files))) as pool:
                self._deletes = dict(
                    zip(files, pool.map(lambda file: _read_deletes(io, file), files.values()), strict=True)
                )

    def read(self, task: FileScanTask) -> np.ndarray:
        """Apply the task's deletes to unfiltered physical row positions."""
        ids = read_id_column(task.file.file_path, task.file.file_format.name.lower())
        if ids.size != task.file.record_count:
            raise ValueError("data row count disagrees with the data-file manifest")
        keep: np.ndarray | None = None
        for delete in task.delete_files:
            positions = self._deletes[delete.file_path].get(task.file.file_path)
            if positions is None:
                continue
            values = positions.to_numpy(zero_copy_only=False)
            if positions.null_count or (values.size and values.dtype.kind not in ("i", "u")):
                raise ValueError("delete positions must be non-null integers")
            if not values.size:
                continue
            if np.any(values < 0) or np.any(values >= ids.size):
                raise ValueError("delete position is outside the data-file row range")
            if keep is None:
                keep = np.ones(ids.size, dtype=bool)
            keep[values] = False
        return ids if keep is None else ids[keep]


def _tally_files(io: FileIO, tasks: dict[str, FileScanTask], tally: BatchTally, read_workers: int) -> None:
    reader = LiveIdReader(io, tasks.values(), read_workers)
    pending = iter(tasks.values())
    pool = ThreadPoolExecutor(max_workers=read_workers)
    try:
        reads = {pool.submit(reader.read, task) for task in islice(pending, read_workers)}
        while reads:
            completed = wait(reads, return_when=FIRST_COMPLETED)[0]
            future = completed.pop()
            del completed
            reads.remove(future)
            tally.add_ids(future.result())
            # A completed future owns its array until it is released.
            del future
            task = next(pending, None)
            if task is not None:
                reads.add(pool.submit(reader.read, task))
    finally:
        pool.shutdown(wait=True, cancel_futures=True)


def incoming_files(
    document: TableMetadata,
    snapshot_id: int,
    io: FileIO,
    tally: BatchTally,
    read_workers: int,
    *,
    manifest_cache: ManifestCache | None = None,
) -> dict[str, FileScanTask] | None:
    """Return incoming files, or None for a count/checksum-preserving rewrite.

    Validate retained rows before classifying a rewrite, and compare changed-file tallies
    without retaining all their IDs. Final distinct-ID verification must still check compaction.
    """
    planning_io = io if manifest_cache is None else manifest_cache
    current = plan_files(document, snapshot_id, planning_io)
    parent_id = snapshot_by_id(document, snapshot_id).parent_snapshot_id
    parent = {} if parent_id is None else plan_files(document, parent_id, planning_io)
    changed = [
        path
        for path in parent
        if path in current
        and {str(delete) for delete in parent[path].delete_files}
        != {str(delete) for delete in current[path].delete_files}
    ]
    # V2 position-delete planning can attach one delete file to many data files.
    # Inspect its actual targets before rereading any retained data.
    deletes = {
        delete.file_path: delete
        for path in changed
        for task in (parent[path], current[path])
        for delete in task.delete_files
    }
    targets: set[str] = set()
    for delete in deletes.values():
        targets.update(_read_deletes(io, delete))
    for path in changed:
        if path not in targets:
            continue
        previous = LiveIdReader(io, [parent[path]], read_workers).read(parent[path])
        present = LiveIdReader(io, [current[path]], read_workers).read(current[path])
        if not np.array_equal(previous, present):
            raise TableHistoryError(
                f"unsupported append-track overwrite {snapshot_id}: changes previously tallied live rows"
            )
    incoming = {path: task for path, task in current.items() if path not in parent}
    removed = {path: task for path, task in parent.items() if path not in current}
    if removed:
        before = BatchTally(tally.records, tally.p)
        after = BatchTally(tally.records, tally.p)
        for accumulated, tasks in ((before, removed), (after, incoming)):
            _tally_files(io, tasks, accumulated, read_workers)
        assert before.sum_mod is not None and after.sum_mod is not None
        if (
            np.array_equal(before.counts, after.counts)
            and np.array_equal(before.sum_mod, after.sum_mod)
            and before.invalid_rows == after.invalid_rows
        ):
            return None
        raise TableHistoryError(
            f"unsupported append-track overwrite {snapshot_id}: removes existing data files "
            "and changes batch counts or checksums; cannot separate a mixed rewrite from delivery"
        )
    return incoming
