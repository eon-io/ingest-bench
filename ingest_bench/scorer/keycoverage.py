# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Track upsert coverage from committed row IDs, one stamp per key.

A key's stamp is the newest stream ordinal any committed data file has held for it. A batch
is covered once every key in it has a stamp at least as new as the batch's last version of
that key, so an engine may skip versions a newer one replaces. Stamps only grow, so coverage
is monotone. A batch checked once keeps only its still-uncovered keys, and every check after
new rows re-tests just those, so a stalled partition costs its missing keys, not whole batches.

Coverage says the newest versions landed. Whether superseded versions were deleted is
checked separately, on the live table at sample snapshots.
"""

from __future__ import annotations

from typing import cast

import numpy as np

from ingest_bench.corpus import columns as c
from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.corpus.upsert import KeySchedule
from ingest_bench.scorer.tally import LOSS
from ingest_bench.specs.model import LATEST_PER_KEY

_VIOLATION_CAP = 200


class KeyCoverage:
    """Upsert counterpart of ``BatchTally``: the same reads, a key-level notion of coverage."""

    def __init__(self, schedule: KeySchedule, records: list[BatchRecord]) -> None:
        held = [record.batch for record in records]
        if held != list(range(len(records))):
            raise ValueError(f"key coverage needs batches 0..{len(records) - 1} in order, and was given {held[:8]}")
        if len(records) != schedule.batch_count:
            raise ValueError(f"the manifest holds {len(records)} batches and the key schedule {schedule.batch_count}")
        self.schedule = schedule
        self.records = records
        self.expected_rows = np.array([record.rows for record in records], dtype=np.int64)
        # Stream rows read per batch, re-added copies included: exactness needs rows of unoffered batches.
        self.counts = np.zeros(len(records), dtype=np.int64)
        self.stamp = np.zeros(schedule.total_keys, dtype=np.uint32)
        self.covered = np.zeros(len(records), dtype=bool)
        self.invalid_rows = 0
        self._prefix = -1
        # Batches that received rows since the last check, and the uncovered keys (with the versions they
        # need) of every batch checked so far. A later version can cover a batch that receives nothing new.
        self._touched: set[int] = set()
        self._open: dict[int, tuple[np.ndarray, np.ndarray]] = {}

    def add_ids(self, ids: np.ndarray) -> None:
        """Raise each committed version's key stamp. Seed ids are valid and stamp nothing."""
        flat = np.asarray(ids).reshape(-1)
        if flat.size == 0:
            return
        if flat.dtype.kind not in ("i", "u"):
            raise ValueError(f"row ids must be integers, and arrived as {flat.dtype}")
        flat = flat.astype(np.int64, copy=False)
        seed = flat < 0
        stream = flat[~seed]
        batches, positions = stream // c.ID_BLOCK, stream % c.ID_BLOCK
        valid = (batches < self.counts.size) & (positions < self.schedule.rows_per_batch)
        self.invalid_rows += int((~valid).sum()) + int((-flat[seed] - 1 >= self.schedule.config.seed_keys).sum())
        stream, batches, positions = stream[valid], batches[valid], positions[valid]
        if stream.size == 0:
            return
        self.counts += np.bincount(batches, minlength=self.counts.size)
        keys = self.schedule.keys_for_ids(stream)
        ordinals = (batches * self.schedule.rows_per_batch + positions + 1).astype(np.uint32)
        np.maximum.at(self.stamp, keys, ordinals)
        self._touched.update(np.unique(batches).tolist())

    def _check(self, batch: int) -> bool:
        if self.covered[batch]:
            return True
        found = self._open.get(batch)
        if found is None:
            keys, first = np.unique(self.schedule.batch_keys(batch)[::-1], return_index=True)
            found = (keys, self.schedule.ordinals(batch, self.schedule.rows_per_batch - 1 - first))
        keys, last = found
        missing = self.stamp[keys] < last
        if missing.any():
            self._open[batch] = (keys[missing], last[missing])
            return False
        self.covered[batch] = True
        self._open.pop(batch, None)
        return True

    def _advance(self) -> None:
        if not self._touched:
            return
        for batch in sorted(self._touched | set(self._open)):
            if batch > self._prefix:
                self._check(batch)
        self._touched.clear()
        while self._prefix + 1 < self.covered.size and self._check(self._prefix + 1):
            self._prefix += 1

    def prefix(self) -> int:
        """The largest covered prefix ``0..k``, or ``-1``."""
        self._advance()
        return self._prefix

    def committed_rows(self) -> int:
        """Rows of covered batches: the keep-up measure, at batch granularity."""
        self._advance()
        return int(self.expected_rows[self.covered].sum())


def upsert_exactness(coverage: KeyCoverage, *, offered_batches: set[int]) -> dict[str, object]:
    """Summarize upsert delivery for offered batches in the shape ``exactness_result`` uses.

    Loss is the rows of offered batches never covered. Duplicate inserts are not faults here,
    since rewrites legitimately re-add rows; duplicate live keys are judged on the live table.
    """
    coverage.prefix()
    scored = np.zeros(coverage.covered.size, dtype=bool)
    indices = sorted(offered_batches)
    if indices and (indices[0] < 0 or indices[-1] >= scored.size):
        offender = indices[0] if indices[0] < 0 else indices[-1]
        raise ValueError(f"offered batch {offender} is outside the manifest's {scored.size} batches")
    scored[indices] = True
    missing = scored & ~coverage.covered
    violations = [
        {"batch": int(batch), "kind": LOSS, "expected_rows": int(coverage.expected_rows[batch])}
        for batch in np.flatnonzero(missing)[:_VIOLATION_CAP].tolist()
    ]
    expected_rows = int(coverage.expected_rows[scored].sum())
    loss_rows = int(coverage.expected_rows[missing].sum())
    unoffered_rows = int(coverage.counts[~scored].sum())
    verdict = loss_rows == 0 and coverage.invalid_rows == 0 and unoffered_rows == 0
    return {
        "expected_rows": expected_rows,
        "rows": int(coverage.expected_rows[scored & coverage.covered].sum()),
        "scored_batches": int(scored.sum()),
        "loss_rows": loss_rows,
        "duplicate_rows": 0,
        "duplicate_ppm": 0.0 if expected_rows else None,
        "corrupt_batches": 0,
        "invalid_rows": coverage.invalid_rows,
        "unoffered_rows": unoffered_rows,
        "violations": cast(list[object], violations),
        "exact": verdict,
        "mode": LATEST_PER_KEY,
        "verdict": verdict,
    }
