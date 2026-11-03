# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Offline freshness corrections preserve the other saved-run evidence."""

import json
from dataclasses import asdict
from pathlib import Path

import pytest

from ingest_bench.producer import publish_log
from ingest_bench.scorer import freshness
from ingest_bench.scorer.rescore import rescore_run

E = 1_000_000


def _run(tmp_path: Path, *, drained: bool = True, mode: str = "EXACTLY_ONCE") -> Path:
    run = tmp_path / "run"
    scores = run / "scores"
    scores.mkdir(parents=True)
    producer = run / "producer"
    producer.mkdir()
    for batch in range(2):
        publish_log.append(
            producer / "publish_log-0.jsonl",
            publish_log.PublishRecord(
                batch=batch,
                scheduled_ms=E + batch * 1000,
                first_ack_ms=E + batch * 1000,
                last_ack_ms=E + batch * 1000,
                rows=1,
                bytes=8,
                errors=0,
            ),
        )
    observations = [freshness.Observation(E + 4_000, E + 5_000, 0)]
    if drained:
        observations.append(freshness.Observation(E + 6_000, E + 9_000, 1))
    rows = [
        {
            "timestamp_ms": obs.timestamp_ms,
            "first_seen_ms": obs.first_seen_ms,
            "prefix_after": obs.prefix,
            "operation": "append",
        }
        for obs in observations
    ]
    (scores / "snapshots.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = freshness.freshness_result(
        observations, {0: E, 1: E + 1000}, epoch_ms=E, end_ms=E + 50_000, last_batch=1, warmup_s=0, bound_s=20
    )
    old_series = {
        clock: freshness.lag_series(observations, {0: E, 1: E + 1000}, E, E + 50_000, 1000, clock)
        for clock in (freshness.TIMESTAMP_CLOCK, freshness.FIRST_SEEN_CLOCK)
    }
    saved_result = {
        **asdict(result),
        "full": freshness.lag_quantiles(old_series[freshness.TIMESTAMP_CLOCK]),
        "window": freshness.lag_quantiles(old_series[freshness.TIMESTAMP_CLOCK]),
        "verdict": False,
    }
    (scores / "freshness.json").write_text(json.dumps({**saved_result, "lag_series": old_series}))
    summary = {
        "freshness": saved_result,
        "epoch_ms": E,
        "last_batch": 1,
        "prefix": observations[-1].prefix,
        "warmup_s": 0,
        "freshness_bound_s": 20,
        "freshness_max_bound_s": 40,
        "scoring_mode": "sustainable",
        "state": "drained" if drained else "idle_stop",
        "aborted": False,
        "exactness": {"verdict": True},
        "exactness_mode": mode,
        "producer_bound": False,
        "clock_skew_suspected": False,
        "reason": "freshness: window max 49.00 s exceeds max bound 40.00 s",
        "run_valid": False,
        "keepup": {"drain_s": 4.2},
    }
    (scores / "summary.json").write_text(json.dumps(summary))
    (run / "spec.yaml").write_text("scoring:\n  freshness_bound_s: 999\n")
    (scores / "keepup_samples.jsonl").write_text('{"at_ms":1050000}\n')
    (run / "run.json").write_text('{"preserve":true}\n')
    return run


def test_rescore_corrects_the_tail_and_only_rewrites_freshness_and_summary(tmp_path: Path) -> None:
    run = _run(tmp_path)
    before = {path: path.read_bytes() for path in run.rglob("*") if path.is_file()}
    result = rescore_run(run)
    assert result.window["max_s"] == 5.0
    assert result.verdict
    summary = json.loads((run / "scores/summary.json").read_text())
    assert summary["run_valid"] and summary["reason"] is None
    assert summary["keepup"] == {"drain_s": 4.2}
    saved = json.loads((run / "scores/freshness.json").read_text())
    assert saved["lag_series"]["timestamp_ms"][-1]["at_ms"] == E + 6_000
    assert saved["lag_series"]["first_seen_ms"][-1]["at_ms"] == E + 9_000
    for path, content in before.items():
        if path.name not in ("freshness.json", "summary.json"):
            assert path.read_bytes() == content
    assert rescore_run(run) == result


def test_rescore_retains_the_end_for_an_incomplete_run(tmp_path: Path) -> None:
    run = _run(tmp_path, drained=False)
    result = rescore_run(run)
    assert result.window["max_s"] == 50
    saved = json.loads((run / "scores/freshness.json").read_text())
    assert saved["lag_series"]["timestamp_ms"][-1]["at_ms"] == E + 50_000
    assert not result.drained


def test_rescore_does_not_clear_other_failures(tmp_path: Path) -> None:
    run = _run(tmp_path)
    summary_path = run / "scores/summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(producer_bound=True, reason="producer_bound: invalid offer")
    summary_path.write_text(json.dumps(summary))
    assert rescore_run(run).verdict
    after = json.loads(summary_path.read_text())
    assert not after["run_valid"]
    assert after["reason"] == summary["reason"]


def test_legacy_upsert_observations_cannot_reconstruct_visibility(tmp_path: Path) -> None:
    run = _run(tmp_path, mode="LATEST_PER_KEY")
    before = (run / "scores/freshness.json").read_bytes()
    with pytest.raises(ValueError, match="effective observation timestamps"):
        rescore_run(run)
    assert (run / "scores/freshness.json").read_bytes() == before


def test_rescore_uses_effective_timestamps_and_skips_rewrites(tmp_path: Path) -> None:
    run = _run(tmp_path, mode="LATEST_PER_KEY")
    path = run / "scores/snapshots.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows[0]["observation_timestamp_ms"] = E + 5_000
    rows[1]["observation_timestamp_ms"] = E + 8_000
    rows.append(
        {
            **rows[1],
            "observation_timestamp_ms": None,
            "operation": "overwrite",
            "timestamp_ms": E + 100_000,
            "first_seen_ms": E + 100_000,
        }
    )
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    result = rescore_run(run)
    assert result.full["max_s"] == 7.0
    assert result.drained


def test_legacy_advancing_overwrite_is_rejected(tmp_path: Path) -> None:
    run = _run(tmp_path)
    path = run / "scores/snapshots.jsonl"
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    rows[1]["operation"] = "overwrite"
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    with pytest.raises(ValueError, match="effective observation timestamps"):
        rescore_run(run)
