# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Measure table lag on a fixed time grid.

Time-based sampling weights stalls fairly; per-commit quantiles would
overweight bursts of frequent commits. Lag uses the emit time of the newest
fully covered batch in the tally prefix.

Support both table commit timestamps and first-observed wall times. The
latter avoids relying on the writer clock; report suspected clock skew.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from itertools import pairwise

import numpy as np

TIMESTAMP_CLOCK = "timestamp_ms"
FIRST_SEEN_CLOCK = "first_seen_ms"


@dataclass(frozen=True)
class Observation:
    """Coverage watermark at a commit, with commit and observation timestamps.

    ``prefix`` is the largest batch for which every batch through it is covered.
    """

    timestamp_ms: int
    first_seen_ms: int
    prefix: int


def _clock_steps(observations: list[Observation], clock: str) -> list[tuple[int, Observation]]:
    if clock not in (TIMESTAMP_CLOCK, FIRST_SEEN_CLOCK):
        raise ValueError(f"unknown observation clock: {clock}")
    steps: list[tuple[int, Observation]] = []
    for observation in observations:
        at_ms = observation.timestamp_ms if clock == TIMESTAMP_CLOCK else observation.first_seen_ms
        # Coverage advances in snapshot order, even when the writer clock goes backwards.
        if steps:
            at_ms = max(at_ms, steps[-1][0])
        steps.append((at_ms, observation))
    return steps


def has_timestamp_reversal(observations: list[Observation]) -> bool:
    """Return whether the commit clock moves backwards in observed snapshot order."""
    return any(later.timestamp_ms < earlier.timestamp_ms for earlier, later in pairwise(observations))


def series_end_ms(observations: list[Observation], end_ms: int, last_batch: int, clock: str) -> int:
    """End drained runs at first complete coverage on the selected clock.

    Incomplete runs retain the scorer's end time. Each clock has its own drain
    endpoint, so delayed observation cannot inflate commit-clock freshness.
    """
    if not observations or observations[-1].prefix != last_batch:
        return end_ms
    for at_ms, observation in _clock_steps(observations, clock):
        if observation.prefix >= last_batch:
            return min(at_ms, end_ms)
    return end_ms


def lag_series(
    observations: list[Observation],
    emit_ms: dict[int, int],
    t0_ms: int,
    end_ms: int,
    grid_ms: int,
    clock: str,
) -> list[dict[str, int | float | None]]:
    """Sample lag on a fixed grid using the latest visible coverage watermark.

    Before any batch is covered, measure lag from ``t0_ms``. A missing publish-log
    emit time produces ``lag_s=None`` so callers can report the coverage gap.
    """
    steps = _clock_steps(observations, clock)
    series: list[dict[str, int | float | None]] = []
    index = 0
    current: Observation | None = None
    grid_points = list(range(t0_ms, end_ms + 1, grid_ms))
    if not grid_points or grid_points[-1] != end_ms:
        grid_points.append(end_ms)
    for at_ms in grid_points:
        while index < len(steps) and steps[index][0] <= at_ms:
            current = steps[index][1]
            index += 1
        prefix = current.prefix if current is not None else -1
        reference_ms = emit_ms.get(prefix) if prefix >= 0 else t0_ms
        series.append(
            {
                "at_ms": at_ms,
                "prefix": prefix,
                "lag_s": (at_ms - reference_ms) / 1000 if reference_ms is not None else None,
            }
        )
    return series


def over_bound_samples(series: list[dict[str, int | float | None]], *, window_start_ms: int, bound_s: float) -> int:
    """Count the window's grid samples whose lag exceeds the bound."""
    return sum(
        1
        for row in series
        if _at_ms(row) >= window_start_ms and row["lag_s"] is not None and float(row["lag_s"]) > bound_s
    )


def p95_failing_count(samples: int) -> int:
    """Return how many over-bound samples put the p95 of a window of ``samples`` over the bound.

    ``np.percentile`` interpolates between the samples on either side of rank
    0.95 x (n - 1). Once the top n - floor(rank) samples exceed the bound, both do.
    """
    return samples - math.floor(0.95 * (samples - 1))


def missing_emit_prefixes(series: list[dict[str, int | float | None]]) -> list[int]:
    """Return sampled batch prefixes whose emit times are missing."""
    gaps = {int(row["prefix"]) for row in series if row["lag_s"] is None and row["prefix"] is not None}
    return sorted(gaps)


def _no_quantiles() -> dict[str, float | None]:
    return {"p50_s": None, "p95_s": None, "p99_s": None, "max_s": None}


def lag_quantiles(series: list[dict[str, int | float | None]]) -> dict[str, float | None]:
    lags: list[float] = []
    for row in series:
        lag = row["lag_s"]
        # A missing emit time invalidates all quantiles; do not omit the missing sample.
        if lag is None:
            return _no_quantiles()
        lags.append(float(lag))
    if not lags:
        return _no_quantiles()
    arr = np.array(lags, dtype=np.float64)
    return {
        "p50_s": float(np.percentile(arr, 50)),
        "p95_s": float(np.percentile(arr, 95)),
        "p99_s": float(np.percentile(arr, 99)),
        "max_s": float(arr.max()),
    }


def _observation_lags_s(observations: list[Observation], emit_ms: dict[int, int]) -> list[float]:
    """Return per-observation lag on the table clock, skipping missing emit times.

    Use these values for clock diagnostics, not time-weighted quantiles.
    """
    return [
        (obs.timestamp_ms - emit_ms[obs.prefix]) / 1000
        for obs in observations
        if obs.prefix >= 0 and obs.prefix in emit_ms
    ]


def min_observation_lag_s(observations: list[Observation], emit_ms: dict[int, int]) -> float | None:
    """Return minimum per-observation lag, or ``None`` without a usable prefix.

    A negative value indicates suspected disagreement between commit and producer
    clocks.
    """
    lags = _observation_lags_s(observations, emit_ms)
    return min(lags, default=None)


def clock_skew_suspected(observations: list[Observation], emit_ms: dict[int, int]) -> bool:
    """Flag negative lag or a backwards commit clock, regardless of scoring clock."""
    minimum = min_observation_lag_s(observations, emit_ms)
    return has_timestamp_reversal(observations) or (minimum is not None and minimum < 0)


@dataclass
class FreshnessResult:
    """Freshness verdict, window and full-run metrics, and clock diagnostics.

    The bound applies to the post-warmup window. Full-run metrics retain startup
    lag. ``min_lag_s`` is the per-observation minimum used to flag clock skew.
    """

    window: dict[str, float | None]
    full: dict[str, float | None]
    verdict: bool
    drained: bool
    bound_s: float
    max_bound_s: float
    warmup_s: int
    clock: str
    clock_skew_suspected: bool
    min_lag_s: float | None
    missing_emit_prefixes: list[int]


def _at_ms(row: dict[str, int | float | None]) -> int:
    at_ms = row["at_ms"]
    if at_ms is None:
        raise ValueError("a lag sample must carry the grid instant it was taken at")
    return int(at_ms)


def freshness_result(
    observations: list[Observation],
    emit_ms: dict[int, int],
    *,
    epoch_ms: int,
    end_ms: int,
    last_batch: int,
    warmup_s: int,
    bound_s: float,
    max_bound_s: float | None = None,
    grid_ms: int = 1000,
    clock: str = TIMESTAMP_CLOCK,
) -> FreshnessResult:
    """Evaluate post-warmup lag bounds and require the table to drain.

    Publish full-run quantiles alongside the measurement window.
    """
    end_ms = series_end_ms(observations, end_ms, last_batch, clock)
    series = lag_series(observations, emit_ms, epoch_ms, end_ms, grid_ms, clock)
    window_start = epoch_ms + warmup_s * 1000
    # If warmup exceeds the run, evaluate the final grid point.
    window = [row for row in series if _at_ms(row) >= window_start] or series[-1:]
    missing = missing_emit_prefixes(series)
    window_quantiles, full_quantiles = lag_quantiles(window), lag_quantiles(series)
    drained = bool(observations) and observations[-1].prefix == last_batch
    p95_s, max_s = window_quantiles["p95_s"], window_quantiles["max_s"]
    # Without an explicit max bound, the max bound is twice the p95 bound.
    max_bound_s = 2 * bound_s if max_bound_s is None else max_bound_s
    verdict = (
        drained
        and not (clock == TIMESTAMP_CLOCK and has_timestamp_reversal(observations))
        and not missing
        and p95_s is not None
        and p95_s <= bound_s
        and max_s is not None
        and max_s <= max_bound_s
    )
    return FreshnessResult(
        window=window_quantiles,
        full=full_quantiles,
        verdict=verdict,
        drained=drained,
        bound_s=bound_s,
        max_bound_s=max_bound_s,
        warmup_s=warmup_s,
        clock=clock,
        clock_skew_suspected=clock_skew_suspected(observations, emit_ms),
        min_lag_s=min_observation_lag_s(observations, emit_ms),
        missing_emit_prefixes=missing,
    )


def failure_reason(result: FreshnessResult, observations: list[Observation]) -> str:
    """Describe the first failed freshness condition for live and offline scoring."""
    if result.clock == TIMESTAMP_CLOCK and has_timestamp_reversal(observations):
        return "freshness: commit timestamps move backwards in snapshot order"
    if not result.drained:
        return "freshness: the table never drained"
    if result.missing_emit_prefixes:
        return (
            f"freshness: missing_emit_prefixes={len(result.missing_emit_prefixes)}, "
            f"first={result.missing_emit_prefixes[0]}"
        )
    p95_s, max_s = result.window["p95_s"], result.window["max_s"]
    if p95_s is not None and p95_s > result.bound_s:
        return f"freshness: window p95 {p95_s:.2f} s exceeds bound {result.bound_s:.2f} s"
    if max_s is not None and max_s > result.max_bound_s:
        return f"freshness: window max {max_s:.2f} s exceeds max bound {result.max_bound_s:.2f} s"
    return "freshness: the measurement window holds no lag sample to judge the bound on"
