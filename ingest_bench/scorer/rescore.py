# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Recompute a saved run's freshness using local scoring and producer artifacts."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from dataclasses import asdict
from pathlib import Path
from typing import cast

from ingest_bench.producer import publish_log
from ingest_bench.scorer import freshness
from ingest_bench.specs.model import LATEST_PER_KEY, SATURATION


def _document(path: Path) -> dict[str, object]:
    return cast(dict[str, object], json.loads(path.read_text(encoding="utf-8")))


def _integer(document: dict[str, object], key: str) -> int:
    return int(cast(int, document[key]))


def _observations(path: Path, exactness_mode: object) -> list[freshness.Observation]:
    observations: list[freshness.Observation] = []
    prefix = -1
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        row = cast(dict[str, object], json.loads(line))
        after = _integer(row, "prefix_after")
        if "observation_timestamp_ms" in row:
            if row["observation_timestamp_ms"] is None:
                continue
            timestamp_ms = _integer(row, "observation_timestamp_ms")
        else:
            # Legacy upserts recorded raw snapshot time, not transaction visibility.
            if exactness_mode == LATEST_PER_KEY or (row.get("operation") != "append" and after > prefix):
                raise ValueError("cannot rescore these snapshots without effective observation timestamps")
            if row.get("operation") != "append":
                continue
            timestamp_ms = _integer(row, "timestamp_ms")
        observations.append(freshness.Observation(timestamp_ms, _integer(row, "first_seen_ms"), after))
        prefix = after
    return observations


def _saved_end_ms(document: dict[str, object]) -> int:
    series = cast(dict[str, list[dict[str, object]]], document["lag_series"])
    # A corrected export ends each clock at its own drain. Taking the latest
    # endpoint keeps repeated rescoring from truncating the observation clock.
    ends = [_integer(rows[-1], "at_ms") for rows in series.values() if rows]
    if not ends:
        raise ValueError("saved freshness has no series endpoint")
    return max(ends)


def _write(path: Path, document: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def rescore_run(run_dir: Path) -> freshness.FreshnessResult:
    """Replace freshness and its derived summary verdict without reading the table.

    Saved scorer parameters take precedence over the spec, which may since have
    changed. Legacy append artifacts retain their recorded commit timestamps;
    upserts and ingestion overwrites need effective visibility timestamps.
    """
    scores = run_dir / "scores"
    summary = _document(scores / "summary.json")
    if summary.get("state") == "running" or not isinstance(summary.get("freshness"), dict):
        raise ValueError("freshness rescoring requires a finished scorer")
    saved = _document(scores / "freshness.json")
    observations = _observations(scores / "snapshots.jsonl", summary.get("exactness_mode"))
    producer_dir = run_dir / "producer"
    if not list(producer_dir.glob("publish_log-*.jsonl")):
        raise ValueError("freshness rescoring requires local producer publish logs")
    records = publish_log.read_all(str(producer_dir))
    emit_ms = publish_log.emit_times(records)
    epoch_ms, end_ms = _integer(summary, "epoch_ms"), _saved_end_ms(saved)
    last_batch = _integer(summary, "last_batch")
    if end_ms < epoch_ms:
        raise ValueError("saved freshness ends before the run epoch")
    clock = str(saved.get("clock", freshness.TIMESTAMP_CLOCK))
    result = freshness.freshness_result(
        observations,
        emit_ms,
        epoch_ms=epoch_ms,
        end_ms=end_ms,
        last_batch=last_batch,
        warmup_s=_integer(summary, "warmup_s"),
        bound_s=float(cast(float, summary["freshness_bound_s"])),
        max_bound_s=cast(float | None, summary.get("freshness_max_bound_s")),
        clock=clock,
    )
    document: dict[str, object] = dict(asdict(result))
    document["lag_series"] = {
        name: freshness.lag_series(
            observations,
            emit_ms,
            epoch_ms,
            freshness.series_end_ms(observations, end_ms, last_batch, name),
            1000,
            name,
        )
        for name in (freshness.TIMESTAMP_CLOCK, freshness.FIRST_SEEN_CLOCK)
    }
    summary["freshness"] = asdict(result)
    summary["clock_skew_suspected"] = result.clock_skew_suspected
    reason = summary.get("reason")
    if summary.get("scoring_mode") != SATURATION:
        if summary.get("run_valid") or (isinstance(reason, str) and reason.startswith("freshness:")):
            exactness = cast(dict[str, object], summary["exactness"])
            valid = result.verdict and bool(exactness["verdict"]) and not summary.get("aborted")
            valid = valid and not summary.get("producer_bound")
            summary["run_valid"] = valid
            summary["reason"] = None if valid else freshness.failure_reason(result, observations)
    _write(scores / "freshness.json", document)
    _write(scores / "summary.json", summary)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Recompute freshness from a finished run's local artifacts.")
    parser.add_argument("run_dir", type=Path, help="run directory containing scores/ and producer/")
    args = parser.parse_args(argv)
    try:
        result = rescore_run(args.run_dir)
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(2, f"rescore-freshness: {error}\n")
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
