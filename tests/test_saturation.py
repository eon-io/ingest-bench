# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from dataclasses import replace
from typing import cast

import pytest

from ingest_bench.producer.publish_log import PublishRecord
from ingest_bench.scorer.saturation import Commit, measure


def offers() -> list[PublishRecord]:
    return [PublishRecord(i, i * 1000, i * 1000, i * 1000, 100, 100_000, 0) for i in range(12)]


def commits() -> list[Commit]:
    # 20 rows/s, versus 100 offered rows/s; the final drain is outside the window.
    return [Commit(t * 1000, t * 1000 + 100, t * 20) for t in (1, 3, 5, 7, 9, 60)]


def test_fixed_window_excludes_startup_and_drain() -> None:
    result = measure(
        commits(), offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000, source_bytes_per_row=1000
    )
    assert result["verdict"] is True
    assert result["committed_rows"] == 120
    assert result["rows_per_s"] == 20
    assert result["source_mb_per_s_estimate"] == pytest.approx(0.02)
    assert result["append_commits"] == 3
    assert result["commit_gap_s"] == {"p50": 2.0, "p95": 2.0, "max": 2.0}
    assert result["max_silence_s"] == 2
    assert result["backlog_min_rows"] == 240
    assert result["backlog_growth_rows_per_s"] == 80
    assert result["first_half_rows_per_s"] == pytest.approx(80 / 3)
    assert result["second_half_rows_per_s"] == pytest.approx(40 / 3)


@pytest.mark.parametrize(
    "change, reason",
    [
        ("clears", "backlog"),
        ("few", "three"),
        ("early", "offer"),
        ("unfinished", "window"),
        ("reversed", "clock"),
        ("future", "clock"),
    ],
)
def test_measurement_refuses_unsupported_saturation(change: str, reason: str) -> None:
    rows, emitted, end = commits(), offers(), 61_000
    if change == "clears":
        rows = [replace(c, committed_rows=(c.timestamp_ms // 1000 + 1) * 100) for c in rows]
    elif change == "few":
        rows = rows[:2]
    elif change == "early":
        emitted = emitted[:6]
    elif change == "unfinished":
        end = 7_000
    elif change == "reversed":
        rows[2] = replace(rows[2], timestamp_ms=2_000)
    else:
        rows[2] = replace(rows[2], timestamp_ms=rows[2].first_seen_ms + 1)
    result = measure(rows, emitted, epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=end)
    assert not result["verdict"]
    assert reason in cast(str, result["reason"])


def test_exact_boundaries_and_long_tail_silence() -> None:
    rows = [Commit(t, t, n) for t, n in ((2000, 10), (3000, 20), (4000, 30), (8000, 40), (8001, 90))]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000)
    assert result["committed_rows"] == 30  # start excluded, end included
    assert result["append_commits"] == 3
    assert result["max_silence_s"] == 4
    assert cast(dict[str, float], result["rows_per_commit"])["p50"] == 10


def test_first_seen_clock_is_reported_separately() -> None:
    rows = [replace(c, first_seen_ms=10_000 if c.timestamp_ms < 10_000 else c.first_seen_ms) for c in commits()]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000)
    assert result["rows_per_s"] == 20
    assert result["first_seen_rows_per_s"] == 0


def test_empty_commits_do_not_establish_cadence_or_saturation() -> None:
    result = measure(
        [Commit(t, t, 0) for t in (3000, 5000, 7000)], offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000
    )
    assert result["append_commits"] == 0
    assert result["rows_per_s"] == 0
    assert not result["verdict"]


def test_acknowledgements_after_window_cannot_supply_backlog() -> None:
    emitted = [replace(r, last_ack_ms=20_000) for r in offers()]
    result = measure(commits(), emitted, epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000)
    assert not result["verdict"]
    assert cast(int, result["backlog_min_rows"]) < 0


def test_backlog_clearing_between_commits_is_not_hidden_by_endpoints() -> None:
    rows = [Commit(1000, 1000, 20), Commit(3000, 3000, 300), Commit(5000, 5000, 450), Commit(7000, 7000, 500)]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000)
    assert not result["verdict"]
    assert result["backlog_min_rows"] == 0


def test_constant_pipeline_backlog_does_not_establish_overload() -> None:
    rows = [Commit(t * 1000 + 2500, t * 1000 + 2500, (t + 1) * 100) for t in range(9)]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000)
    assert cast(int, result["backlog_min_rows"]) > 0
    assert result["backlog_growth_rows_per_s"] == 0
    assert not result["verdict"]
    assert "batching swing" in cast(str, result["reason"])


def test_tied_acknowledgement_cannot_hide_a_momentarily_empty_backlog() -> None:
    emitted = [
        PublishRecord(i, t, t, t, n, n * 1000, 0)
        for i, (t, n) in enumerate(((0, 150), (2000, 300), (3000, 300), (4000, 300), (6000, 300)))
    ]
    rows = [Commit(t, t, n) for t, n in ((2000, 150), (3000, 300), (4000, 450))]
    result = measure(rows, emitted, epoch_ms=0, warmup_s=1, measurement_s=4, end_ms=7000)
    assert not result["verdict"]
    assert result["backlog_min_rows"] == 0


def test_source_byte_estimate_excludes_message_framing() -> None:
    framed = [replace(r, bytes=r.bytes + 5 * r.rows) for r in offers()]
    result = measure(
        commits(), framed, epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000, source_bytes_per_row=1000
    )
    assert result["source_mb_per_s_estimate"] == pytest.approx(0.02)


@pytest.mark.parametrize("startup_rows", [0, 1])
def test_pre_epoch_empty_commits_are_allowed_but_data_is_rejected(startup_rows: int) -> None:
    epoch = 1_000_000
    emitted = [
        replace(
            r,
            scheduled_ms=r.scheduled_ms + epoch,
            first_ack_ms=r.first_ack_ms + epoch,
            last_ack_ms=r.last_ack_ms + epoch,
        )
        for r in offers()
    ]
    rows = [Commit(epoch - 35_000, epoch - 34_000, startup_rows)] + [
        replace(c, timestamp_ms=c.timestamp_ms + epoch, first_seen_ms=c.first_seen_ms + epoch) for c in commits()
    ]
    result = measure(rows, emitted, epoch_ms=epoch, warmup_s=2, measurement_s=6, end_ms=epoch + 61_000)
    assert result["verdict"] is (startup_rows == 0)
    assert result["rows_per_s"] == 20
    assert result["append_commits"] == 3


def test_start_boundary_commit_excludes_tied_acknowledgements_from_backlog_floor() -> None:
    emitted = [
        PublishRecord(i, t, t, t, n, n * 1000, 0)
        for i, (t, n) in enumerate(((0, 100), (1000, 300), (2000, 300), (3000, 300), (4000, 300), (6000, 300)))
    ]
    rows = [Commit(t, t, n) for t, n in ((1000, 100), (2000, 150), (3000, 200), (4000, 250))]
    result = measure(rows, emitted, epoch_ms=0, warmup_s=1, measurement_s=4, end_ms=7000)
    assert not result["verdict"]
    assert result["backlog_min_rows"] == 0


def test_commit_aligned_rates_exclude_baseline_rows_and_report_their_spans() -> None:
    result = measure(commits(), offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000)
    assert result["commit_to_commit"] == {
        "start_ms": 3000,
        "end_ms": 7000,
        "duration_s": 4.0,
        "committed_rows": 80,
        "rows_per_s": 20.0,
        "append_commits": 3,
    }
    assert result["start_edge_gap_s"] == 1
    assert result["end_edge_gap_s"] == 1
    # A single commit in the second half cannot establish an aligned rate.
    first = cast(dict[str, float], result["first_half_commit_to_commit"])
    assert first["rows_per_s"] == 20
    assert first["duration_s"] == 2
    assert result["second_half_commit_to_commit"] is None
    assert result["first_seen_rate_difference_pct"] == 0


def test_commit_alignment_keeps_internal_pauses_but_discloses_excluded_tail() -> None:
    rows = [Commit(t, t, n) for t, n in ((2000, 10), (3000, 30), (6000, 60), (6001, 60), (11000, 100))]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=1, measurement_s=9, end_ms=12_000)
    aligned = cast(dict[str, float], result["commit_to_commit"])
    assert aligned["rows_per_s"] == 12.5
    assert aligned["duration_s"] == 4
    assert aligned["append_commits"] == 3
    assert result["end_edge_gap_s"] == 4
    assert result["rows_per_s"] == pytest.approx(60 / 9)


@pytest.mark.parametrize("times", [[], [3000], [3000, 3000], [3000, 3000, 5000, 5000]])
def test_commit_alignment_handles_missing_and_tied_timestamps(times: list[int]) -> None:
    rows = [Commit(t, t, (i + 1) * 10) for i, t in enumerate(times)]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000)
    if len(set(times)) < 2:
        assert result["commit_to_commit"] is None
    else:
        aligned = cast(dict[str, float], result["commit_to_commit"])
        assert aligned["committed_rows"] == 20  # All commits tied to the baseline are excluded.
        assert aligned["rows_per_s"] == 10
    if not times:
        assert result["start_edge_gap_s"] is None
        assert result["end_edge_gap_s"] is None
        assert result["first_seen_rate_difference_pct"] is None


def test_clock_difference_is_signed_relative_to_commit_rate() -> None:
    rows = [replace(c, first_seen_ms=max(c.first_seen_ms, 10_000)) for c in commits()]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000)
    assert result["first_seen_rate_difference_pct"] == -100
    assert result["verdict"] is True


def test_both_half_spans_respect_the_midpoint_and_exclude_window_start() -> None:
    rows = [Commit(t * 1000, t * 1000, t * 20) for t in range(1, 10)]
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=12_000)
    first = cast(dict[str, float], result["first_half_commit_to_commit"])
    second = cast(dict[str, float], result["second_half_commit_to_commit"])
    assert (first["start_ms"], first["end_ms"], first["rows_per_s"]) == (3000, 5000, 20)
    assert (second["start_ms"], second["end_ms"], second["rows_per_s"]) == (6000, 8000, 20)
    assert result["end_edge_gap_s"] == 0


def test_clock_difference_can_be_positive_from_a_delayed_baseline_commit() -> None:
    rows = commits()
    rows[0] = replace(rows[0], first_seen_ms=3000)
    result = measure(rows, offers(), epoch_ms=0, warmup_s=2, measurement_s=6, end_ms=61_000)
    assert result["first_seen_rate_difference_pct"] == pytest.approx(100 / 6)
