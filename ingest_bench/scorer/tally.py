# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Track per-batch coverage and exactness from committed row IDs.

By default, accumulate counts and row-ID checksums modulo a prime as snapshots
arrive. Coverage uses counts; completeness also requires the checksum. These
checks detect loss, duplication, and many substitutions without rescanning the
table, but do not prove row-set identity.

When duplicates are permitted, track distinct expected IDs so duplicates cannot
hide loss or advance freshness. Late duplicates never move the coverage
watermark backward.
"""

from __future__ import annotations

import functools
import os
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import urlparse

import numpy as np
import pyarrow.fs as pa_fs
import pyarrow.orc as pa_orc
import pyarrow.parquet as pa_parquet

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus.generate import BatchRecord

ID_COLUMN = "id"

LOSS = "loss"
DUPLICATION = "duplication"
CORRUPTION = "corruption"

PARQUET = "parquet"
ORC = "orc"

# Float64 weighted counts are exact only below 2**53. Chunk well below
# roughly nine million residues per sum to preserve checksum equality.
_CHUNK_ROWS = 4_000_000


class BatchTally:
    """Track row counts and exactness for the manifest's contiguous batch IDs.

    Decode batch ownership from the high bits of each row ID.
    """

    def __init__(self, records: list[BatchRecord], p: int, *, track_unique: bool = False) -> None:
        held = [record.batch for record in records]
        if held != list(range(len(records))):
            raise ValueError(f"a tally needs batches 0..{len(records) - 1} in order, and was given {held[:8]}")
        self.p = p
        self.records = records
        count = len(records)
        self.expected_rows = np.array([record.rows for record in records], dtype=np.int64)
        self.expected_checksum = np.array([record.checksum for record in records], dtype=np.int64)
        self.counts = np.zeros(count, dtype=np.int64)
        self.sum_mod = None if track_unique else np.zeros(count, dtype=np.int64)
        self.unique_counts = np.zeros(count, dtype=np.int64) if track_unique else None
        self.unexpected_rows = np.zeros(count, dtype=np.int64)
        # Rows whose ID encodes no manifest batch, so no per-batch array can hold them.
        self.invalid_rows = 0
        self._id_min = np.array([record.id_min for record in records], dtype=np.int64)
        self._id_max = np.array([record.id_max for record in records], dtype=np.int64)
        self._offsets = np.cumsum(self.expected_rows) - self.expected_rows
        # One bit per expected ID lets duplicates coexist with reliable loss checks.
        self._seen = np.zeros((int(self.expected_rows.sum()) + 7) // 8, dtype=np.uint8) if track_unique else None
        self._prefix = -1

    def add_ids(self, ids: np.ndarray) -> None:
        """Accumulate counts and checksum or distinct-ID state by encoded batch ID."""
        flat = np.asarray(ids).reshape(-1)
        if flat.size == 0:
            return
        if flat.dtype.kind not in ("i", "u"):
            raise ValueError(f"row ids must be integers, and arrived as {flat.dtype}")
        flat = flat.astype(np.int64, copy=False)
        batches = flat // c.ID_BLOCK
        valid = (batches >= 0) & (batches < self.counts.size)
        if not valid.all():
            self.invalid_rows += int((~valid).sum())
            flat, batches = flat[valid], batches[valid]
        for start in range(0, flat.size, _CHUNK_ROWS):
            chunk = batches[start : start + _CHUNK_ROWS]
            self.counts += np.bincount(chunk, minlength=self.counts.size)
            if self._seen is not None:
                self._add_unique(flat[start : start + _CHUNK_ROWS], chunk)
            else:
                assert self.sum_mod is not None
                residues = (flat[start : start + _CHUNK_ROWS] % self.p).astype(np.float64)
                self.sum_mod += np.bincount(chunk, weights=residues, minlength=self.counts.size).astype(np.int64)
                # Reduce each chunk to prevent int64 overflow across large runs.
                self.sum_mod %= self.p
        while self._prefix + 1 < self.counts.size and self.covered(self._prefix + 1):
            self._prefix += 1

    def _add_unique(self, ids: np.ndarray, batches: np.ndarray) -> None:
        assert self._seen is not None and self.unique_counts is not None
        valid = (ids >= self._id_min[batches]) & (ids <= self._id_max[batches])
        self.unexpected_rows += np.bincount(batches[~valid], minlength=self.counts.size)
        unique = np.unique(ids[valid])
        owners = unique // c.ID_BLOCK
        positions = self._offsets[owners] + unique - self._id_min[owners]
        byte_indices = positions // 8
        bits = (1 << (positions % 8)).astype(np.uint8)
        unseen = (self._seen[byte_indices] & bits) == 0
        # Different IDs can share a byte, so indexed assignment would lose updates.
        np.bitwise_or.at(self._seen, byte_indices[unseen], bits[unseen])
        self.unique_counts += np.bincount(owners[unseen], minlength=self.counts.size)

    def covered(self, batch: int) -> bool:
        """Check expected coverage, using distinct IDs when tracking duplicates."""
        counts = self.counts if self.unique_counts is None else self.unique_counts
        return bool(counts[batch] >= self.expected_rows[batch])

    def complete(self, batch: int) -> bool:
        """Return whether the batch satisfies the configured row-ID checks."""
        if self.unique_counts is not None:
            return bool(self.unique_counts[batch] == self.expected_rows[batch] and self.unexpected_rows[batch] == 0)
        assert self.sum_mod is not None
        return bool(
            self.counts[batch] == self.expected_rows[batch] and self.sum_mod[batch] == self.expected_checksum[batch]
        )

    def _mismatched(self) -> np.ndarray:
        assert self.sum_mod is not None
        mismatched: np.ndarray = (self.counts != self.expected_rows) | (self.sum_mod != self.expected_checksum)
        return mismatched

    def prefix(self) -> int:
        """Return the largest fully covered prefix ``0..k``, or ``-1``.

        Coverage is monotonic. Late duplicates affect exactness without moving the
        arrival watermark backward.
        """
        return self._prefix

    def committed_rows(self) -> int:
        counts = self.counts if self.unique_counts is None else self.unique_counts
        return int(counts.sum())

    def violations(self, *, include_missing: bool = False) -> list[dict[str, object]]:
        """Report loss, duplication, and corruption under the configured ID checks.

        Exclude wholly absent batches unless ``include_missing`` is set, since they
        may still be in flight. Partially received batches report their current loss.
        """
        if self.unique_counts is not None:
            return self._unique_violations(include_missing=include_missing)
        assert self.sum_mod is not None
        found: list[dict[str, object]] = []
        for index in np.flatnonzero(self._mismatched()):
            batch = int(index)
            rows = int(self.counts[batch])
            expected = int(self.expected_rows[batch])
            if rows == 0 and not include_missing:
                continue
            if rows < expected:
                kind = LOSS
            elif rows > expected:
                kind = DUPLICATION
            else:
                kind = CORRUPTION
            found.append(
                {
                    "batch": batch,
                    "expected_rows": expected,
                    "rows": rows,
                    "expected_checksum": int(self.expected_checksum[batch]),
                    "checksum": int(self.sum_mod[batch]),
                    "kind": kind,
                }
            )
        return found

    def _unique_violations(self, *, include_missing: bool) -> list[dict[str, object]]:
        assert self.unique_counts is not None
        duplicates = self.counts - self.unique_counts - self.unexpected_rows
        corrupt = self.unexpected_rows > 0
        found: list[dict[str, object]] = []
        for kind, mask in (
            (LOSS, self.unique_counts < self.expected_rows),
            (DUPLICATION, duplicates > 0),
            (CORRUPTION, corrupt),
        ):
            for index in np.flatnonzero(mask):
                batch = int(index)
                if self.counts[batch] == 0 and not include_missing:
                    continue
                found.append(
                    {
                        "batch": batch,
                        "expected_rows": int(self.expected_rows[batch]),
                        "rows": int(self.counts[batch]),
                        "distinct_rows": int(self.unique_counts[batch]),
                        "kind": kind,
                    }
                )
        return found


@functools.lru_cache(maxsize=1)
def _s3_filesystem() -> pa_fs.S3FileSystem:
    """Build one native S3 filesystem from the same environment fsspec reads.

    Native PyArrow IO releases the GIL and avoids serialising reads through
    s3fs's event loop when a commit adds many small files at once.
    """
    options: dict[str, object] = {}
    endpoint = os.environ.get("AWS_ENDPOINT_URL")
    if endpoint:
        parsed = urlparse(endpoint)
        options["endpoint_override"] = parsed.netloc or endpoint
        options["scheme"] = parsed.scheme or "https"
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if region:
        options["region"] = region
    return pa_fs.S3FileSystem(**options)


def _pyarrow_filesystem(location: str) -> tuple[pa_fs.FileSystem, str]:
    """Return a filesystem and path for reading ID columns.

    Use native PyArrow IO for S3 and local files; keep the fsspec bridge for
    other remote schemes so endpoint and credential handling stay in one place.
    """
    if location.startswith("s3://"):
        return _s3_filesystem(), location[len("s3://") :]
    fs, path = uri.filesystem_for(location)
    if uri.is_remote(location):
        return pa_fs.PyFileSystem(pa_fs.FSSpecHandler(fs)), path
    return pa_fs.LocalFileSystem(), path


def read_id_column(path: str, file_format: str) -> np.ndarray:
    """Read only the scoring ID column and reject null IDs.

    Projection avoids loading the corpus payload; null IDs cannot be attributed
    to a batch.
    """
    filesystem, inner = _pyarrow_filesystem(path)
    if file_format == PARQUET:
        # Open directly: read_table first stats the path, a second round trip per file.
        with filesystem.open_input_file(inner) as handle:
            table = pa_parquet.ParquetFile(handle).read(columns=[ID_COLUMN])
    elif file_format == ORC:
        with filesystem.open_input_file(inner) as handle:
            table = pa_orc.ORCFile(handle).read(columns=[ID_COLUMN])
    else:
        raise ValueError(f"cannot read row ids out of a {file_format!r} data file at {path}")
    column = table.column(ID_COLUMN)
    if column.null_count:
        raise ValueError(f"{path} holds {column.null_count} null row ids, which belong to no batch")
    return np.asarray(column.to_numpy(zero_copy_only=False), dtype=np.int64)


def read_id_columns(files: list[tuple[str, str]], threads: int) -> np.ndarray:
    """Read several ID columns inside one worker process and return them joined.

    Separate processes avoid sharing the GIL for Python work per file, while
    each process uses threads to overlap IO within its chunk.
    """
    if not files:
        return np.empty(0, dtype=np.int64)
    with ThreadPoolExecutor(max_workers=min(threads, len(files))) as pool:
        arrays = list(pool.map(lambda file: read_id_column(file[0], file[1]), files))
    return np.concatenate(arrays)
