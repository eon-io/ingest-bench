# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Replay campaign backlog observations with timestamps relative to each epoch.

Fixtures preserve the published external hour's 2026-09-15 16:49:10 run and
Flink probe 3's 01:59:22 run. No cluster or object-store access is needed.
"""

from pathlib import Path

from ingest_bench.scorer import gate, score

FIXTURES = Path(__file__).parent / "fixtures"


def test_external_campaign_has_no_false_capacity_ticks() -> None:
    samples = score.read_keepup_samples(FIXTURES / "external_commit_cadence.jsonl")
    old_breaches = 0
    for at_ms in range(60_000, 3_600_001, 60_000):
        visible = [sample for sample in samples if sample.at_ms <= at_ms]
        floors = gate._backlog_floors(visible, now_ms=at_ms, window_s=60, windows=3)
        newest, middle, oldest = floors
        if newest is not None and middle is not None and oldest is not None and newest > middle > oldest > 0:
            old_breaches += 1
        verdict, reason = gate.gate_verdict(
            {"aborted": False, "lag_s": None},
            visible,
            bound_s=180,
            adaptation_s=300,
            window_s=60,
            now_ms=at_ms,
            epoch_ms=0,
            stale_after_s=60,
        )
        assert verdict == gate.PASS, reason
    assert old_breaches == 8


def test_flink_campaign_still_stops_at_the_third_breach() -> None:
    samples = score.read_keepup_samples(FIXTURES / "flink_growing_backlog.jsonl")
    verdicts = []
    for at_ms in range(60_000, 360_001, 60_000):
        visible = [sample for sample in samples if sample.at_ms <= at_ms]
        verdicts.append(
            gate.gate_verdict(
                {"aborted": False, "lag_s": None},
                visible,
                bound_s=180,
                adaptation_s=300,
                window_s=60,
                now_ms=at_ms,
                epoch_ms=0,
                stale_after_s=60,
            )[0]
        )
    assert verdicts == [gate.PASS] * 3 + [gate.UNDERSIZED] * 3
