# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Decide whether a live run should continue.

Check scorer liveness before judging lag or a rising backlog floor. A stale
sample indicates missing measurements even if the last summary looked healthy.
Window minima distinguish persistent backlog growth from normal commit cycles.
Keep UNDERSIZED (a capacity result) separate from VOID (no valid measurement).
"""

from __future__ import annotations

from collections.abc import Mapping
from statistics import median

from ingest_bench.scorer.keepup import KeepupSample

PASS = "PASS"
UNDERSIZED = "UNDERSIZED"
VOID = "VOID"

_FLOOR_WINDOWS = 3


def _backlog_floors(samples: list[KeepupSample], *, now_ms: int, window_s: int, windows: int) -> list[int | None]:
    """Return minimum backlog in each recent window, newest first.

    An empty window yields ``None`` because missing samples do not prove an
    empty backlog.
    """
    step_ms = window_s * 1000
    floors: list[int | None] = []
    for index in range(windows):
        upper_ms = now_ms - index * step_ms
        lower_ms = upper_ms - step_ms
        backlogs = [sample.backlog_rows for sample in samples if lower_ms <= sample.at_ms < upper_ms]
        floors.append(min(backlogs, default=None))
    return floors


def _commit_cadence(
    samples: list[KeepupSample], *, epoch_ms: int, adaptation_s: int, window_s: int, now_ms: int
) -> tuple[int, int]:
    """Freeze commit spacing after adaptation; track typical commit size.

    Window minima still move with polling and burst size even when every window
    contains a commit. Require growth beyond the typical committed-row increase
    at a backlog decrease. Net backlog drops subtract the interval's offered rows
    and understate commit size when polls are slow. Skip the first decrease,
    which may drain startup backlog, and take the median of later commit sizes
    to limit the influence of unusually large commits. With no decreases,
    retain the original window and zero allowance so stalled fleets fail promptly.
    """
    end_ms = min(now_ms, epoch_ms + adaptation_s * 1000)
    decreases = [
        (sample.at_ms, sample.committed_rows - previous.committed_rows)
        for previous, sample in zip(samples, samples[1:], strict=False)
        if epoch_ms <= previous.at_ms < sample.at_ms <= now_ms and sample.backlog_rows < previous.backlog_rows
    ]
    adaptation_drops = [at_ms for at_ms, _ in decreases if at_ms <= end_ms]
    gap_ms = max(
        (later - earlier for earlier, later in zip(adaptation_drops, adaptation_drops[1:], strict=False)), default=0
    )
    # Bound the learned window by the original three-window decision horizon.
    learned_s = min((gap_ms + 999) // 1000, _FLOOR_WINDOWS * window_s)
    commit_sizes = [rows for _, rows in decreases[1:]]
    return max(window_s, learned_s), int(median(commit_sizes)) if commit_sizes else 0


def gate_verdict(
    freshness_partial: Mapping[str, object],
    keepup_samples: list[KeepupSample],
    *,
    bound_s: float,
    max_bound_s: float | None = None,
    adaptation_s: int,
    window_s: int,
    now_ms: int,
    epoch_ms: int,
    stale_after_s: int,
    scoring_mode: str = "sustainable",
) -> tuple[str, str]:
    """Return the live verdict and its reason.

    Require recent scorer samples. Apply the lag limit after adaptation, and
    check for a rising positive backlog floor across at least three populated windows.
    """
    if freshness_partial["aborted"]:
        return VOID, "the freshness reader aborted, so the run has no measurement to judge"

    newest_ms = max((sample.at_ms for sample in keepup_samples), default=None)
    if newest_ms is None:
        return VOID, "the scorer has published no keep-up sample, so nothing is measuring the run"
    age_s = (now_ms - newest_ms) / 1000
    if age_s > stale_after_s:
        return VOID, (
            f"the scorer's newest keep-up sample is {age_s:.0f}s old, past the {stale_after_s}s staleness bound, "
            "so nothing is measuring the run"
        )

    if scoring_mode == "saturation":
        return PASS, "saturation mode: measurement is live; lag and backlog are diagnostic"
    if scoring_mode != "sustainable":
        return VOID, f"unknown scoring mode: {scoring_mode!r}"
    lag = freshness_partial["lag_s"]
    if lag is not None and not isinstance(lag, (int, float)):
        raise TypeError(f"freshness lag must be a number or None, and arrived as {type(lag).__name__}")
    # Without an explicit max bound, the max bound is twice the p95 bound.
    max_bound_s = 2 * bound_s if max_bound_s is None else max_bound_s
    adapted = now_ms >= epoch_ms + adaptation_s * 1000
    if adapted and lag is not None and lag > max_bound_s:
        return UNDERSIZED, f"lag {lag:.1f}s is past the {max_bound_s:.0f}s max bound {adaptation_s}s after the epoch"
    # Seconds already scored never leave the window, so once the over-bound count reaches the
    # failing count the run cannot pass. Only the newest poll interval can still be revised down.
    live = freshness_partial.get("freshness_live")
    if isinstance(live, Mapping):
        over_s, failing_s = int(live["window_over_bound_s"]), int(live["window_failing_s"])
        if over_s >= failing_s:
            return UNDERSIZED, (
                f"lag has exceeded the {bound_s:.0f}s bound for {over_s}s of the measurement window; "
                f"{failing_s}s puts the planned window's p95 over the bound"
            )

    effective_window_s, commit_swing = _commit_cadence(
        keepup_samples, epoch_ms=epoch_ms, adaptation_s=adaptation_s, window_s=window_s, now_ms=now_ms
    )
    recent, middle, oldest = _backlog_floors(
        keepup_samples, now_ms=now_ms, window_s=effective_window_s, windows=_FLOOR_WINDOWS
    )
    if recent is None:
        # Keep this guard if the staleness limit is increased beyond the window.
        return VOID, f"no keep-up sample in the last {effective_window_s}s, so the backlog floor cannot be read"
    if middle is not None and oldest is not None and recent > middle > oldest > 0:
        windows = _FLOOR_WINDOWS
        # Small capacity deficits need more than three cycles to exceed commit
        # jitter. Accumulate evidence only while every older floor is lower.
        while recent - oldest <= commit_swing:
            older = _backlog_floors(
                keepup_samples,
                now_ms=now_ms - windows * effective_window_s * 1000,
                window_s=effective_window_s,
                windows=1,
            )[0]
            if older is None or not 0 < older < oldest:
                break
            oldest = older
            windows += 1
        if recent - oldest > commit_swing:
            return UNDERSIZED, (
                f"backlog floor rose from {oldest} to {recent} rows across {windows} consecutive increasing "
                f"{effective_window_s}s windows, beyond the {commit_swing}-row commit swing"
            )

    if now_ms < epoch_ms:
        return PASS, f"waiting for epoch in {(epoch_ms - now_ms) / 1000:.1f}s, backlog floor {recent} rows not rising"
    lag_figure = f"lag {lag:.1f}s" if lag is not None else "no lag sample yet"
    return PASS, (
        f"{lag_figure} within the {max_bound_s:.0f}s max bound, backlog floor {recent} rows; "
        f"no sustained growth beyond the {commit_swing}-row commit swing"
    )
