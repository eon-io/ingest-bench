# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import copy
import json
from pathlib import Path
from typing import Any

import pytest
import yaml

from ingest_bench.collect.schema import parse_result, readiness_failures
from ingest_bench.collect.validate import document_failures
from ingest_bench.producer.publish_log import PublishRecord
from ingest_bench.scorer.saturation import Commit, measure
from tests.collect_fixtures import build_document, build_run_dir, render_table, write_site
from tests.test_results_table import _document

ROOT = Path(__file__).resolve().parents[1]


def saturation_fields(epoch: int = 0, warmup: int = 120, duration: int = 600) -> dict[str, Any]:
    return {
        "scoring_mode": "saturation",
        "measurement_s": duration,
        "warmup_s": warmup,
        "epoch_ms": epoch,
        "saturation": {
            "verdict": True,
            "reason": None,
            "start_ms": epoch + warmup * 1000,
            "end_ms": epoch + (warmup + duration) * 1000,
            "duration_s": float(duration),
            "committed_rows": 100 * duration,
            "rows_per_s": 100.0,
            "source_mb_per_s_estimate": 0.1,
            "first_seen_rows_per_s": 99.0,
            "first_seen_rate_difference_pct": -1.0,
            "start_edge_gap_s": 10.0,
            "end_edge_gap_s": 0.0,
            "commit_to_commit": {
                "start_ms": epoch + (warmup + 10) * 1000,
                "end_ms": epoch + (warmup + duration) * 1000,
                "duration_s": float(duration - 10),
                "committed_rows": 100 * (duration - 10),
                "rows_per_s": 100.0,
                "append_commits": 60,
            },
            "first_half_rows_per_s": 100.0,
            "second_half_rows_per_s": 100.0,
            "append_commits": 60,
            "commit_gap_s": {"p50": 10.0, "p95": 10.0, "max": 10.0},
            "rows_per_commit": {"p50": 1000.0, "p95": 1000.0, "max": 1000.0},
            "max_silence_s": 10.0,
            "backlog_min_rows": 5000,
            "backlog_growth_rows_per_s": 200.0,
        },
    }


def saturation_document() -> dict[str, Any]:
    doc = _document()
    doc["run"]["spec"]["scoring"].update(mode="saturation", warmup_s=120, measurement_s=600)
    doc["data"]["summary"].update(saturation_fields())
    return doc


def test_modes_are_separate_and_saturation_has_no_offered_rate_cost_metric() -> None:
    sat = saturation_document()
    old = _document()
    report = render_table([(Path("old/result.json"), old), (Path("sat/result.json"), sat)])
    assert "## 100 MB/s · skewed keys" in report and report.count("## events-") == 1
    assert "Rows/s" in report and "MB/s (est)" in report
    assert "saturation" in report and "Commit gap p95" in report
    only_sat = render_table([(Path("sat/result.json"), sat)])
    assert "$/TB" not in only_sat
    assert "freshness p95 ≤" not in only_sat


@pytest.mark.parametrize("field", ["measurement_s", "warmup_s", "vcpu", "gib", "machine_type", "producer.seconds"])
def test_saturation_comparisons_fix_the_window_and_compute(field: str) -> None:
    a, b = saturation_document(), saturation_document()
    if field in ("measurement_s", "warmup_s"):
        b["run"]["spec"]["scoring"][field] += 1
        config = b["run"]["spec"]["scoring"]
        b["data"]["summary"].update(saturation_fields(warmup=config["warmup_s"], duration=config["measurement_s"]))
    elif field == "producer.seconds":
        a["run"]["spec"]["producer"] = {"seconds": 1000}
        b["run"]["spec"]["producer"] = {"seconds": 2000}
    elif field == "machine_type":
        b["run"]["fleet"][0][field] = "different-machine"
    else:
        b["run"]["fleet"][0][field] += 1
    report = render_table([(Path("a/result.json"), a), (Path("b/result.json"), b)])
    assert report.count("## events-") == 2


def test_freshness_bound_is_diagnostic_in_saturation_grouping() -> None:
    a, b = saturation_document(), saturation_document()
    b["data"]["summary"]["freshness_bound_s"] = 1
    report = render_table([(Path("a/result.json"), a), (Path("b/result.json"), b)])
    assert report.count("## events-") == 1


@pytest.mark.parametrize(
    "fault", ["mode", "duration", "missing", "false_verdict", "backlog", "growth", "rate", "speed"]
)
def test_bad_saturation_evidence_cannot_be_rendered_as_valid(fault: str) -> None:
    doc = saturation_document()
    summary = doc["data"]["summary"]
    if fault == "mode":
        summary["scoring_mode"] = "sustainable"
    elif fault == "speed":
        doc["run"]["spec"]["producer"] = {"speed": 2}
    elif fault == "duration":
        summary["measurement_s"] = 601
    elif fault == "missing":
        summary["saturation"] = None
    elif fault == "false_verdict":
        summary["saturation"]["verdict"] = False
    elif fault == "backlog":
        summary["saturation"]["backlog_min_rows"] = 0
    elif fault == "growth":
        summary["saturation"]["backlog_growth_rows_per_s"] = 0
    else:
        summary["saturation"]["rows_per_s"] = 9000
    assert readiness_failures(parse_result(doc))
    report = render_table([(Path("sat/result.json"), doc)])
    assert "did-not-validate" in report


def test_collection_preserves_saturation_and_allows_a_bounded_shortened_offer(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    spec_path = run_dir / "spec.yaml"
    spec = yaml.safe_load(spec_path.read_text())
    spec["scoring"].update(mode="saturation", warmup_s=10, measurement_s=60)
    spec["producer"]["seconds"] = 100  # smoke corpus is longer; saturation uses a declared finite offer
    spec_path.write_text(yaml.safe_dump(spec))
    summary_path = run_dir / "scores/summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(saturation_fields(epoch=summary["epoch_ms"], warmup=10, duration=60))
    epoch = summary["epoch_ms"]
    emitted = [PublishRecord(i, epoch + i * 1000, epoch + i * 1000, epoch + i * 1000, 100, 1000, 0) for i in range(100)]
    commits = [Commit(epoch + t * 1000, epoch + t * 1000 + 100, t * 20) for t in range(5, 100, 10)]
    summary["saturation"] = measure(
        commits, emitted, epoch_ms=epoch, warmup_s=10, measurement_s=60, end_ms=epoch + 100_000
    )
    summary_path.write_text(json.dumps(summary))
    parsed = parse_result(build_document(run_dir, write_site(tmp_path)))
    collected_summary = parsed["data"]["summary"]
    assert collected_summary is not None
    assert collected_summary["saturation"] == summary["saturation"]
    assert not document_failures(parsed, workloads=ROOT / "workloads")
    report = render_table([(Path("collected/result.json"), parsed)])
    assert "Rows/s" in report and "did-not-validate" not in report
    bad = copy.deepcopy(parsed)
    bad["run"]["spec"]["producer"]["seconds"] = 50
    assert any("replay" in reason for reason in document_failures(bad, workloads=ROOT / "workloads"))


def test_failed_saturation_without_final_measurement_can_be_recorded(tmp_path: Path) -> None:
    run_dir = build_run_dir(tmp_path)
    spec_path = run_dir / "spec.yaml"
    spec = yaml.safe_load(spec_path.read_text())
    spec["scoring"].update(mode="saturation", warmup_s=10, measurement_s=60)
    spec_path.write_text(yaml.safe_dump(spec))
    summary_path = run_dir / "scores/summary.json"
    summary = json.loads(summary_path.read_text())
    summary.update(saturation_fields(epoch=summary["epoch_ms"], warmup=10, duration=60))
    summary.update(run_valid=False, state="abandoned", reason="scorer failed", saturation=None)
    summary_path.write_text(json.dumps(summary))
    parsed = parse_result(build_document(run_dir, write_site(tmp_path)))
    assert not document_failures(parsed, workloads=ROOT / "workloads")
    report = render_table([(Path("failed/result.json"), parsed)])
    assert "### Did not validate" in report
    assert "| not drained | scorer failed |" in report


def test_historical_saturation_without_diagnostics_still_renders() -> None:
    doc = saturation_document()
    metrics = doc["data"]["summary"]["saturation"]
    for field in ("commit_to_commit", "start_edge_gap_s", "end_edge_gap_s", "first_seen_rate_difference_pct"):
        metrics.pop(field)
    assert not readiness_failures(parse_result(doc))
    report = render_table([(Path("old/result.json"), doc)])
    assert "did-not-validate" not in report
    assert "| n/a | n/a | n/a / n/a | n/a |" in report


def test_saturation_table_discloses_span_edges_and_clock_difference() -> None:
    report = render_table([(Path("sat/result.json"), saturation_document())])
    assert "Commit-to-commit rows/s" in report
    assert "Commit span (s)" in report
    assert "Start / end gap (s)" in report
    assert "First-seen difference (%)" in report
    assert "| 100.0 | 590.0 | 10.0 / 0.0 | -1.0 |" in report


@pytest.mark.parametrize("fault", ["rate", "span", "half", "count", "edge", "clock"])
def test_inconsistent_diagnostics_cannot_be_rendered_as_valid(fault: str) -> None:
    doc = saturation_document()
    metrics = doc["data"]["summary"]["saturation"]
    if fault == "rate":
        metrics["commit_to_commit"]["rows_per_s"] *= 2
    elif fault == "span":
        metrics["commit_to_commit"]["duration_s"] += 1
    elif fault == "half":
        metrics["first_half_commit_to_commit"] = metrics["commit_to_commit"].copy()
    elif fault == "count":
        metrics["commit_to_commit"]["append_commits"] = 100
    elif fault == "edge":
        metrics["start_edge_gap_s"] = 5
    else:
        metrics["first_seen_rate_difference_pct"] = 1
    assert readiness_failures(parse_result(doc))
    assert "did-not-validate" in render_table([(Path("sat/result.json"), doc)])
