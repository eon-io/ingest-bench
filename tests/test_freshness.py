# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import numpy as np
import pytest

from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.scorer import exactness, freshness, gate, keepup, tally

E = 1_000_000  # epoch ms


def _emit(n: int) -> dict[int, int]:
    return {k: E + k * 1000 + 100 for k in range(n)}


def test_lag_series_is_time_weighted_step_function() -> None:
    obs = [freshness.Observation(E + 5_000, E + 5_050, 3), freshness.Observation(E + 65_000, E + 65_020, 60)]
    series = freshness.lag_series(obs, _emit(61), E, E + 70_000, 1000, "timestamp_ms")
    assert series[0]["lag_s"] == 0.0 and series[4]["prefix"] == -1
    assert series[5]["prefix"] == 3 and series[5]["lag_s"] == (E + 5_000 - (E + 3_100)) / 1000
    # Between commits the lag grows one second per grid step.
    assert series[30]["lag_s"] == series[5]["lag_s"] + 25
    assert series[70]["prefix"] == 60
    q = freshness.lag_quantiles(series)
    p50_s, p95_s, max_s = q["p50_s"], q["p95_s"], q["max_s"]
    assert p50_s is not None and p95_s is not None and max_s is not None
    assert max_s >= p95_s >= p50_s


def test_lagging_lane_is_not_complete() -> None:
    # Half of batch 1 landed early, the rest much later: the prefix stays at 0 until then.
    records = [
        BatchRecord(
            k,
            k * 1000,
            100,
            k << 32,
            (k << 32) + 99,
            ((k << 32) * 2 + 99) * 100 // 2 % 1_000_000_007,
            0,
            0,
            "",
            "",
            {},
        )
        for k in range(3)
    ]
    t = tally.BatchTally(records, 1_000_000_007)
    t.add_ids(np.arange(0, 100))
    t.add_ids(np.arange(1 << 32, (1 << 32) + 50))
    assert t.prefix() == 0
    t.add_ids(np.arange((1 << 32) + 50, (1 << 32) + 100))
    assert t.prefix() == 1


def test_warmup_window_and_verdict() -> None:
    emit = _emit(300)
    # Cold start: nothing committed for 100 s, then a commit every 10 s that catches up to the previous second.
    obs = [freshness.Observation(E + t * 1000, E + t * 1000, t - 1) for t in range(100, 301, 10)]
    result = freshness.freshness_result(
        obs, emit, epoch_ms=E, end_ms=E + 300_000, last_batch=299, warmup_s=120, bound_s=60
    )
    assert result.drained
    window_max_s, window_p95_s = result.window["max_s"], result.window["p95_s"]
    assert window_max_s is not None and window_p95_s is not None
    assert result.full["max_s"] == 99.0 and window_max_s < 11
    assert window_p95_s <= 60 and result.verdict
    # Every commit here lands 0.9 s after its batch's last ack, and the
    # per-commit minimum says so rather than reporting the epoch's own zero.
    assert result.min_lag_s == 0.9
    strict = freshness.freshness_result(
        obs, emit, epoch_ms=E, end_ms=E + 300_000, last_batch=299, warmup_s=0, bound_s=60
    )
    strict_p95_s = strict.full["p95_s"]
    assert strict_p95_s is not None
    assert strict_p95_s > 60 and strict.verdict is False


def test_an_explicit_max_bound_replaces_twice_the_p95_bound() -> None:
    emit = _emit(300)
    # A 99 s cold start with no warmup: p95 84 s and max 99 s, within twice a 90 s bound but past a 95 s max.
    obs = [freshness.Observation(E + t * 1000, E + t * 1000, t - 1) for t in range(100, 301, 10)]
    default, explicit = (
        freshness.freshness_result(
            obs, emit, epoch_ms=E, end_ms=E + 300_000, last_batch=299, warmup_s=0, bound_s=90, max_bound_s=max_bound
        )
        for max_bound in (None, 95)
    )
    assert (default.max_bound_s, explicit.max_bound_s) == (180, 95)
    assert default.window["max_s"] == explicit.window["max_s"] == 99.0
    assert default.verdict and explicit.verdict is False


def test_clock_skew_flag() -> None:
    emit = _emit(5)
    ok = [freshness.Observation(E + 4_500, E + 4_500, 3)]
    bad = [freshness.Observation(E + 2_000, E + 2_000, 3)]  # visible 1.1 s before its last ack
    assert not freshness.clock_skew_suspected(ok, emit)
    assert freshness.clock_skew_suspected(bad, emit)
    assert freshness.min_observation_lag_s(ok, emit) == 1.4
    r = freshness.freshness_result(bad, emit, epoch_ms=E, end_ms=E + 6_000, last_batch=4, warmup_s=0, bound_s=180)
    assert r.clock_skew_suspected and not r.drained
    # The skew flag is this figure being negative, so it cannot disagree with it.
    assert r.min_lag_s == -1.1
    assert freshness.min_observation_lag_s([], emit) is None


def test_keepup_summary() -> None:
    samples: list[keepup.KeepupSample] = []
    prev = None
    for i in range(10):
        prev = keepup.make_sample(
            E + i * 1000, offered_rows=1000 * (i + 1), committed_rows=800 * (i + 1), previous=prev
        )
        samples.append(prev)
    assert samples[1].offered_rate == 1000.0 and samples[1].committed_rate == 800.0
    s = keepup.keepup_summary(samples, offer_end_ms=E + 9000, drained_ms=E + 12000)
    assert s == {"drain_s": 3.0, "backlog_rows_max": 2000, "backlog_rows_p50": 1100.0}


def test_exactness_result() -> None:
    records = [
        BatchRecord(k, 0, 10, k << 32, (k << 32) + 9, ((k << 32) * 2 + 9) * 10 // 2 % 1_000_000_007, 0, 0, "", "", {})
        for k in range(2)
    ]
    t = tally.BatchTally(records, 1_000_000_007)
    t.add_ids(np.arange(0, 10))
    t.add_ids(np.arange(1 << 32, (1 << 32) + 12))  # two extra rows
    r = exactness.exactness_result(t, offered_batches={0, 1})
    assert r["exact"] is False and r["duplicate_rows"] == 2 and r["loss_rows"] == 0 and r["duplicate_ppm"] == 100_000.0


# What the gate is given for the staleness bound in these cases: the default,
# which is the width of its own newest floor window.
STALE_AFTER_S = 60


def test_gate() -> None:
    samples = [
        keepup.KeepupSample(E + i * 1000, 1000 * i, 1000 * i - min(i, 5) * 100, min(i, 5) * 100, None, None)
        for i in range(1, 400)
    ]
    flat = gate.gate_verdict(
        {"lag_s": 30.0, "aborted": False},
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert flat[0] == "PASS"
    late = gate.gate_verdict(
        {"lag_s": 400.0, "aborted": False},
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert late[0] == "UNDERSIZED"
    tight = gate.gate_verdict(
        {"lag_s": 150.0, "aborted": False},
        samples,
        bound_s=90,
        max_bound_s=120,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert tight[0] == "UNDERSIZED" and "120s max bound" in tight[1]
    rising = [keepup.KeepupSample(E + i * 1000, 1000 * i, 700 * i, 300 * i, None, None) for i in range(1, 400)]
    assert (
        gate.gate_verdict(
            {"lag_s": 30.0, "aborted": False},
            rising,
            bound_s=180,
            adaptation_s=120,
            window_s=60,
            now_ms=E + 400_000,
            epoch_ms=E,
            stale_after_s=STALE_AFTER_S,
        )[0]
        == "UNDERSIZED"
    )
    assert (
        gate.gate_verdict(
            {"lag_s": None, "aborted": True},
            samples,
            bound_s=180,
            adaptation_s=120,
            window_s=60,
            now_ms=E + 400_000,
            epoch_ms=E,
            stale_after_s=STALE_AFTER_S,
        )[0]
        == "VOID"
    )


@pytest.mark.parametrize("samples", [1, 2, 20, 21, 100, 3301])
def test_the_failing_count_is_the_fewest_over_bound_samples_that_breach_the_p95(samples: int) -> None:
    failing = freshness.p95_failing_count(samples)
    breached = [0.0] * (samples - failing) + [10.0] * failing
    p95_s = freshness.lag_quantiles([{"lag_s": lag} for lag in breached])["p95_s"]
    assert p95_s is not None and p95_s > 5.0
    # One fewer leaves the rank's lower neighbour inside the bound, so the p95 is not yet decided.
    fewer = [0.0] * (samples - failing + 1) + [10.0] * (failing - 1)
    assert fewer[int(np.floor(0.95 * (samples - 1)))] == 0.0


def test_over_bound_samples_count_only_the_window() -> None:
    series: list[dict[str, int | float | None]] = [
        {"at_ms": E + i * 1000, "prefix": 0, "lag_s": float(i)} for i in range(10)
    ]
    assert freshness.over_bound_samples(series, window_start_ms=E + 5000, bound_s=6.0) == 3  # lags 7, 8, 9
    assert freshness.over_bound_samples(series, window_start_ms=E + 8000, bound_s=6.0) == 2


def test_gate_stops_a_run_whose_window_can_no_longer_meet_the_p95() -> None:
    samples = [
        keepup.KeepupSample(E + i * 1000, 1000 * i, 1000 * i - min(i, 5) * 100, min(i, 5) * 100, None, None)
        for i in range(1, 400)
    ]

    def verdict(over_s: int) -> tuple[str, str]:
        live = {"window_over_bound_s": over_s, "window_failing_s": 166}
        return gate.gate_verdict(
            {"lag_s": 200.0, "aborted": False, "freshness_live": live},
            samples,
            bound_s=180,
            adaptation_s=120,
            window_s=60,
            now_ms=E + 400_000,
            epoch_ms=E,
            stale_after_s=STALE_AFTER_S,
        )

    assert verdict(165)[0] == "PASS"
    assert verdict(166) == (
        "UNDERSIZED",
        "lag has exceeded the 180s bound for 166s of the measurement window; "
        "166s puts the planned window's p95 over the bound",
    )


def test_a_reading_nothing_is_still_taking_is_void_and_never_a_pass() -> None:
    """OOM kills and node loss may leave a healthy-looking summary. Sample age must
    detect a dead scorer even when it could not publish an error.
    """
    samples = [
        keepup.KeepupSample(E + i * 1000, 1000 * i, 1000 * i - min(i, 5) * 100, min(i, 5) * 100, None, None)
        for i in range(1, 400)
    ]
    within_bound = {"lag_s": 30.0, "aborted": False}
    fresh = gate.gate_verdict(
        within_bound,
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert fresh[0] == "PASS"

    stale = gate.gate_verdict(
        within_bound,
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        # Two minutes after the last sample, with a sixty-second bound.
        now_ms=E + 520_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert stale[0] == "VOID" and "121s old" in stale[1], stale

    # And a run whose scorer published no sample at all is the same answer:
    # the absence of a measurement, not a measurement of a healthy fleet.
    none_at_all = gate.gate_verdict(
        within_bound,
        [],
        bound_s=180,
        adaptation_s=120,
        window_s=60,
        now_ms=E + 400_000,
        epoch_ms=E,
        stale_after_s=STALE_AFTER_S,
    )
    assert none_at_all[0] == "VOID" and "no keep-up sample" in none_at_all[1], none_at_all


def test_an_empty_newest_window_is_void_however_the_bound_was_raised() -> None:
    """A raised staleness bound can permit an empty newest backlog window.
    Missing samples must not count as zero backlog.
    """
    samples = [keepup.KeepupSample(E + i * 1000, 1000 * i, 900 * i, 100 * i, None, None) for i in range(1, 100)]
    verdict = gate.gate_verdict(
        {"lag_s": 30.0, "aborted": False},
        samples,
        bound_s=180,
        adaptation_s=120,
        window_s=10,
        now_ms=E + 160_000,
        epoch_ms=E,
        stale_after_s=600,
    )
    assert verdict[0] == "VOID" and "last 10s" in verdict[1], verdict


@pytest.mark.parametrize("age_s, verdict", [(1, "PASS"), (61, "VOID")])
def test_gate_describes_waiting_for_epoch_without_hiding_stale_samples(age_s: int, verdict: str) -> None:
    now = E - 100_000
    result, reason = gate.gate_verdict(
        {"lag_s": -100.0, "aborted": False},
        [keepup.KeepupSample(now - age_s * 1000, 0, 0, 0, None, None)],
        bound_s=60,
        adaptation_s=120,
        window_s=60,
        now_ms=now,
        epoch_ms=E,
        stale_after_s=60,
    )
    assert result == verdict
    if verdict == "PASS":
        assert "waiting for epoch" in reason and "100.0s" in reason
        assert "lag -" not in reason
    else:
        assert "staleness" in reason


def test_gate_keeps_up_with_seventy_second_commits() -> None:
    samples = []
    for second in range(1, 1201):
        committed = max(0, (second // 70) * 70_000 - 5000)
        if second % 9 == 0:
            samples.append(
                keepup.KeepupSample(E + second * 1000, second * 1000, committed, second * 1000 - committed, None, None)
            )
        if second % 60 == 0:
            verdict, reason = gate.gate_verdict(
                {"lag_s": float(second % 70), "aborted": False},
                samples,
                bound_s=180,
                adaptation_s=300,
                window_s=60,
                now_ms=E + second * 1000,
                epoch_ms=E,
                stale_after_s=60,
            )
            assert verdict == "PASS", reason


@pytest.mark.parametrize("startup_rows", [0, 300_000_000])
def test_gate_keeps_up_with_steady_commits_and_slow_polls(startup_rows: int) -> None:
    # A 23s poll sees a 30,000-row commit as only a 7,000-row net drop.
    # Poll phase alone raises three window minima by more than that drop.
    samples: list[keepup.KeepupSample] = []
    for second in range(1, 1801):
        if second % 23 == 0:
            committed = max(0, second // 30 * 30_000 - 20_000)
            if second >= 30:
                committed += startup_rows
            samples.append(
                keepup.make_sample(
                    E + second * 1000,
                    startup_rows + second * 1000,
                    committed,
                    samples[-1] if samples else None,
                )
            )
        if second % 60 == 0:
            verdict, reason = gate.gate_verdict(
                {"aborted": False, "lag_s": None},
                samples,
                bound_s=180,
                adaptation_s=300,
                window_s=60,
                now_ms=E + second * 1000,
                epoch_ms=E,
                stale_after_s=60,
            )
            assert verdict == "PASS", reason


def test_gate_capacity_growth_keeps_original_detection_time() -> None:
    samples = []
    verdicts = []
    for second in range(1, 421):
        samples.append(keepup.KeepupSample(E + second * 1000, second * 1000, second * 700, second * 300, None, None))
        if second % 60 == 0:
            verdicts.append(
                gate.gate_verdict(
                    {"lag_s": 30.0, "aborted": False},
                    samples,
                    bound_s=180,
                    adaptation_s=300,
                    window_s=60,
                    now_ms=E + second * 1000,
                    epoch_ms=E,
                    stale_after_s=60,
                )[0]
            )
    assert verdicts == ["PASS", "PASS"] + ["UNDERSIZED"] * 5


@pytest.mark.parametrize(
    "startup_rows, commit_rows, first_tick, stop_tick",
    [
        (0, 63_000, 720, 840),
        (300_000_000, 63_000, 780, 900),
        (0, 56_000, 360, 480),
        (300_000_000, 56_000, 480, 600),
        (0, 0, 180, 300),
        (300_000_000, 0, 180, 300),
    ],
)
def test_gate_deficit_detection_latency(startup_rows: int, commit_rows: int, first_tick: int, stop_tick: int) -> None:
    # Offer 1,000 rows/s, commit every 70s, and sample every 5s. Startup work
    # drains at the first commit and must not inflate the learned commit size.
    samples = []
    first_undersized = None
    stop = None
    breaches = 0
    reason = ""
    for second in range(5, 901, 5):
        offered = startup_rows + second * 1000
        committed = (second // 70) * commit_rows
        if second >= 70 and commit_rows:
            committed += startup_rows
        samples.append(keepup.KeepupSample(E + second * 1000, offered, committed, offered - committed, None, None))
        if second % 60 == 0:
            verdict, reason = gate.gate_verdict(
                {"aborted": False, "lag_s": None},
                samples,
                bound_s=180,
                adaptation_s=300,
                window_s=60,
                now_ms=E + second * 1000,
                epoch_ms=E,
                stale_after_s=60,
            )
            breaches = breaches + 1 if verdict == "UNDERSIZED" else 0
            if verdict == "UNDERSIZED" and first_undersized is None:
                first_undersized = second
            if breaches == 3 and stop is None:
                stop = second
    assert (first_undersized, stop) == (first_tick, stop_tick)
    assert "consecutive increasing" in reason and "->" not in reason


def test_commit_swing_skips_first_drop_and_updates_after_adaptation() -> None:
    backlogs = [400_000, 100_000, 150_000, 110_000, 160_000, 120_000, 170_000, 130_000]
    offered = 400_000
    samples: list[keepup.KeepupSample] = []
    for i, backlog in enumerate(backlogs):
        samples.append(keepup.make_sample(E + i * 30_000, offered, offered - backlog, samples[-1] if samples else None))
        offered += 50_000
    _, swing = gate._commit_cadence(samples, epoch_ms=E, adaptation_s=300, window_s=60, now_ms=samples[3].at_ms)
    assert swing == 90_000  # 40,000-row net drop plus 50,000 offered rows, excluding the startup drain.
    # A later commit policy replaces the startup estimate, even after adaptation.
    offered = samples[-1].offered_rows + 100_000
    for i in range(5):
        for at_ms, backlog in [(E + 330_000 + i * 60_000, 200_000), (E + 360_000 + i * 60_000, 190_000)]:
            samples.append(keepup.make_sample(at_ms, offered, offered - backlog, samples[-1]))
            offered += 20_000
    _, swing = gate._commit_cadence(samples, epoch_ms=E, adaptation_s=300, window_s=60, now_ms=samples[-1].at_ms)
    assert swing == 30_000  # 10,000-row net drop plus 20,000 offered rows.


def test_drained_freshness_ends_at_first_coverage_on_each_clock() -> None:
    observations = [
        freshness.Observation(E + 4_000, E + 7_000, 0),
        freshness.Observation(E + 6_500, E + 10_500, 1),
        freshness.Observation(E + 8_000, E + 12_000, 1),
    ]
    for clock, expected_end in ((freshness.TIMESTAMP_CLOCK, 6_500), (freshness.FIRST_SEEN_CLOCK, 10_500)):
        result = freshness.freshness_result(
            observations,
            _emit(2),
            epoch_ms=E,
            end_ms=E + 50_000,
            last_batch=1,
            warmup_s=0,
            bound_s=20,
            clock=clock,
        )
        expected = freshness.lag_quantiles(
            freshness.lag_series(observations, _emit(2), E, E + expected_end, 1000, clock)
        )
        assert result.full == expected
        assert result.verdict
        assert freshness.series_end_ms(observations, E + 50_000, 1, clock) == E + expected_end


def test_incomplete_freshness_keeps_the_scorer_end() -> None:
    observations = [freshness.Observation(E + 4_000, E + 7_000, 0)]
    result = freshness.freshness_result(
        observations,
        _emit(2),
        epoch_ms=E,
        end_ms=E + 50_000,
        last_batch=1,
        warmup_s=0,
        bound_s=20,
    )
    assert result.full["max_s"] == 49.9
    assert not result.drained
    assert freshness.series_end_ms(observations, E + 50_000, 1, freshness.TIMESTAMP_CLOCK) == E + 50_000


def test_backwards_commit_clock_never_regresses_coverage_and_fails_timestamp_verdict() -> None:
    observations = [
        freshness.Observation(E + 10_000, E + 11_000, 0),
        freshness.Observation(E + 9_000, E + 12_000, 1),
    ]
    series = freshness.lag_series(observations, _emit(2), E, E + 15_000, 1000, freshness.TIMESTAMP_CLOCK)
    assert [row["prefix"] for row in series[9:]] == [-1, 1, 1, 1, 1, 1, 1]
    assert freshness.clock_skew_suspected(observations, _emit(2))
    for clock in (freshness.TIMESTAMP_CLOCK, freshness.FIRST_SEEN_CLOCK):
        result = freshness.freshness_result(
            observations,
            _emit(2),
            epoch_ms=E,
            end_ms=E + 50_000,
            last_batch=1,
            warmup_s=0,
            bound_s=20,
            clock=clock,
        )
        assert result.verdict == (clock == freshness.FIRST_SEEN_CLOCK)
