# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Measure committed throughput over a declared window under sustained backlog."""

from __future__ import annotations

from bisect import bisect_left, bisect_right
from dataclasses import dataclass
from itertools import accumulate, pairwise

import numpy as np

from ingest_bench.producer.publish_log import PublishRecord


@dataclass(frozen=True)
class Commit:
    """Cumulative useful rows after an append, on both visibility clocks."""

    timestamp_ms: int
    first_seen_ms: int
    committed_rows: int


def _quantiles(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "max": None}
    return {"p50": float(np.percentile(values, 50)), "p95": float(np.percentile(values, 95)), "max": max(values)}


def measure(
    commits: list[Commit],
    records: list[PublishRecord],
    *,
    epoch_ms: int,
    warmup_s: int,
    measurement_s: int,
    end_ms: int,
    source_bytes_per_row: float | None = None,
) -> dict[str, object]:
    """Measure (start, end], including pauses and excluding startup and drain.

    A positive backlog uses only fully acknowledged batches. It establishes
    pending output, not CPU saturation or continuous availability at each reader.
    Commit timestamps preserve cadence when a slow scorer observes many commits
    together; first-seen throughput exposes the alternative observation boundary.
    """
    if measurement_s <= 0 or warmup_s < 0:
        raise ValueError("measurement_s must be positive and warmup_s nonnegative")
    start = epoch_ms + warmup_s * 1000
    end = start + measurement_s * 1000
    middle = (start + end) // 2
    # Idle managed engines may append empty snapshots before the offer starts.
    clock_bad = any(
        (c.committed_rows > 0 and c.timestamp_ms < epoch_ms) or c.timestamp_ms > c.first_seen_ms for c in commits
    ) or any(
        b.timestamp_ms < a.timestamp_ms or b.first_seen_ms < a.first_seen_ms or b.committed_rows < a.committed_rows
        for a, b in pairwise(commits)
    )
    # Invalid clocks still produce diagnostic figures, but never a valid result.
    ordered = sorted(commits, key=lambda c: c.timestamp_ms)
    times = [c.timestamp_ms for c in ordered]
    counts = [0, *(c.committed_rows for c in ordered)]

    def committed(at_ms: int) -> int:
        return counts[bisect_right(times, at_ms)]

    observed = sorted(commits, key=lambda c: c.first_seen_ms)
    seen_times = [c.first_seen_ms for c in observed]
    seen_counts = [0, *(c.committed_rows for c in observed)]
    seen_rows = seen_counts[bisect_right(seen_times, end)] - seen_counts[bisect_right(seen_times, start)]
    arrivals = sorted(records, key=lambda r: r.last_ack_ms)
    ack_times = [r.last_ack_ms for r in arrivals]
    offered_counts = [0, *accumulate(r.rows for r in arrivals)]

    def backlog(at_ms: int) -> int:
        return offered_counts[bisect_right(ack_times, at_ms)] - committed(at_ms)

    # Acknowledgements only increase backlog between commits. At each commit
    # timestamp, subtract all tied commits but exclude tied acknowledgements:
    # millisecond precision cannot establish that those acknowledgements came first.
    floors = [
        backlog(start),
        *(offered_counts[bisect_left(ack_times, t)] - committed(t) for t in times if start <= t <= end),
        backlog(end),
    ]
    nonempty = [
        (c.timestamp_ms, c.committed_rows - counts[i])
        for i, c in enumerate(ordered)
        if start < c.timestamp_ms <= end and c.committed_rows > counts[i]
    ]
    commit_times = [t for t, _ in nonempty]

    def commit_to_commit(lower: int, upper: int) -> dict[str, int | float] | None:
        selected = commit_times[bisect_right(commit_times, lower) : bisect_right(commit_times, upper)]
        if len(selected) < 2 or selected[0] == selected[-1]:
            return None
        first, last = selected[0], selected[-1]
        duration = (last - first) / 1000
        # Exclude every append tied to the baseline timestamp, not just the first.
        delta = committed(last) - committed(first)
        return {
            "start_ms": first,
            "end_ms": last,
            "duration_s": duration,
            "committed_rows": delta,
            "rows_per_s": delta / duration,
            "append_commits": len(selected),
        }

    gaps = [(b - a) / 1000 for a, b in pairwise(commit_times)]
    edges = [start, *commit_times, end]
    silence = max((b - a) / 1000 for a, b in pairwise(edges))
    rows = committed(end) - committed(start)
    growth = backlog(end) - backlog(start)
    reason = None
    if clock_bad:
        reason = "commit clock is nonmonotonic or outside its observed run lifetime"
    elif end_ms < end:
        reason = "measurement window did not finish"
    elif max(ack_times, default=epoch_ms) < end:
        reason = "offer ended before the measurement window"
    elif len(nonempty) < 3:
        reason = "measurement requires at least three nonempty append commits"
    elif min(floors) <= 0:
        reason = "committed output backlog was not positive throughout the measurement window"
    elif growth <= max((n for _, n in nonempty), default=0):
        reason = "offer did not exceed output beyond one commit batching swing"
    return {
        "verdict": reason is None,
        "reason": reason,
        "start_ms": start,
        "end_ms": end,
        "duration_s": float(measurement_s),
        "committed_rows": rows,
        "rows_per_s": rows / measurement_s,
        "source_mb_per_s_estimate": None
        if source_bytes_per_row is None
        else rows * source_bytes_per_row / measurement_s / 1e6,
        "first_seen_rows_per_s": seen_rows / measurement_s,
        "first_seen_rate_difference_pct": None if rows == 0 else 100 * (seen_rows - rows) / rows,
        "commit_to_commit": commit_to_commit(start, end),
        "first_half_commit_to_commit": commit_to_commit(start, middle),
        "second_half_commit_to_commit": commit_to_commit(middle, end),
        "start_edge_gap_s": None if not commit_times else (commit_times[0] - start) / 1000,
        "end_edge_gap_s": None if not commit_times else (end - commit_times[-1]) / 1000,
        "first_half_rows_per_s": (committed(middle) - committed(start)) / (measurement_s / 2),
        "second_half_rows_per_s": (committed(end) - committed(middle)) / (measurement_s / 2),
        "append_commits": len(nonempty),
        "commit_gap_s": _quantiles(gaps),
        "rows_per_commit": _quantiles([float(n) for _, n in nonempty]),
        "max_silence_s": silence,
        "backlog_min_rows": min(floors),
        "backlog_growth_rows_per_s": growth / measurement_s,
    }
