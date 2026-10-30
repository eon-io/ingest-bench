# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Summarize missing rows, duplicate rows, and tally violations.

Score only offered batches, including for shortened replays. Keep loss and
duplication separate across batches; distinct-ID tracking also separates them
within a batch when duplicates are permitted.
"""

from __future__ import annotations

from typing import cast

import numpy as np

from ingest_bench.scorer.tally import CORRUPTION, DUPLICATION, BatchTally
from ingest_bench.specs.model import AT_LEAST_ONCE, EXACTLY_ONCE, EXACTNESS_MODES

_VIOLATION_CAP = 200


def _scored(tally: BatchTally, offered_batches: set[int]) -> np.ndarray:
    """Select manifest batches recorded as offered in the publish logs."""
    mask = np.zeros(tally.counts.size, dtype=bool)
    indices = sorted(offered_batches)
    if indices and (indices[0] < 0 or indices[-1] >= mask.size):
        offender = indices[0] if indices[0] < 0 else indices[-1]
        raise ValueError(f"offered batch {offender} is outside the manifest's {mask.size} batches")
    mask[indices] = True
    return mask


def exactness_result(tally: BatchTally, *, offered_batches: set[int], mode: str = EXACTLY_ONCE) -> dict[str, object]:
    """Summarize exactness for offered batches, counting missing batches as loss.

    Rows that belong to no manifest batch, or to a batch that was never offered,
    fail both verdicts as terminal verification does. Cap the detailed violation
    list while retaining complete aggregate counts.
    """
    if mode not in EXACTNESS_MODES:
        raise ValueError(f"unknown exactness mode: {mode!r}")
    if mode == AT_LEAST_ONCE and tally.unique_counts is None:
        raise ValueError("AT_LEAST_ONCE requires distinct row-ID tracking")
    scored = _scored(tally, offered_batches)
    violations = [
        violation for violation in tally.violations(include_missing=True) if scored[int(cast(int, violation["batch"]))]
    ]
    expected = tally.expected_rows[scored]
    counts = tally.counts[scored]
    expected_rows = int(expected.sum())
    logical_counts = counts if tally.unique_counts is None else tally.unique_counts[scored]
    duplicate_rows = int(
        np.maximum(counts - expected, 0).sum()
        if tally.unique_counts is None
        else (counts - logical_counts - tally.unexpected_rows[scored]).sum()
    )
    failures = violations if mode == EXACTLY_ONCE else [v for v in violations if v["kind"] != DUPLICATION]
    unoffered_rows = int(tally.counts[~scored].sum())
    stray = tally.invalid_rows > 0 or unoffered_rows > 0
    return {
        "expected_rows": expected_rows,
        "rows": int(counts.sum()),
        "scored_batches": int(scored.sum()),
        "loss_rows": int(np.maximum(expected - logical_counts, 0).sum()),
        "duplicate_rows": duplicate_rows,
        "duplicate_ppm": duplicate_rows / expected_rows * 1e6 if expected_rows else None,
        "corrupt_batches": sum(1 for violation in violations if violation["kind"] == CORRUPTION),
        "invalid_rows": tally.invalid_rows,
        "unoffered_rows": unoffered_rows,
        "violations": violations[:_VIOLATION_CAP],
        "exact": not violations and not stray,
        "mode": mode,
        "verdict": not failures and not stray,
    }
