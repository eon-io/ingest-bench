# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any, cast

import pytest

from ingest_bench.collect.privacy import PLACEHOLDER_ACCOUNT_ID
from ingest_bench.collect.validate import _account_id_failures, _exactness_failures
from ingest_bench.corpus.preset import corpus_hash, load_preset
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, PLACEHOLDER
from tests.collect_fixtures import (
    EXTERNAL_SPEC,
    HARNESS_COMMIT,
    build_document,
    build_run_dir,
    commit_spec,
    fixture_specs,
    fixture_workloads,
    render_table,
    write_site,
)
from tests.test_public_surface import leaks_in

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "validate-results.py"
PRIVATE_ACCOUNT_ID = str(int(PLACEHOLDER_ACCOUNT_ID) + 1)


@pytest.mark.parametrize(
    ("value", "private"),
    [
        (PLACEHOLDER_ACCOUNT_ID, False),
        (f"arn:aws:iam::{PLACEHOLDER_ACCOUNT_ID}:root", False),
        (PRIVATE_ACCOUNT_ID, False),
        (f"{PRIVATE_ACCOUNT_ID} bytes", False),
        (f"arn:aws:iam::{PRIVATE_ACCOUNT_ID}:root", True),
        (f"{PRIVATE_ACCOUNT_ID}.dkr.ecr.us-east-1.amazonaws.com/bench/flink:abc", True),
        (f"account {PRIVATE_ACCOUNT_ID}", True),
        (f"acct_{PRIVATE_ACCOUNT_ID}", True),
        (f"a{PRIVATE_ACCOUNT_ID}b", False),
        ("19469.653333333332", False),
    ],
)
def test_account_identifier_rules_agree_with_repository_scan(value: str, private: bool) -> None:
    assert bool(_account_id_failures(value)) is private
    assert any(leak.rule.name == "cloud account id" for leak in leaks_in(value)) is private


def test_valid_run_conflict_is_reported_once_with_all_failed_surfaces(tmp_path: Path) -> None:
    from ingest_bench.collect.schema import parse_result

    document = parse_result(build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    summary = document["data"]["summary"]
    assert summary is not None
    for exactness in (summary.get("exactness"), document["data"].get("exactness"), document["derived"]["exactness"]):
        assert exactness is not None
        exactness.update({"exact": False, "verdict": False, "loss_rows": 1})
    errors = [error for error in _exactness_failures(document) if "data.summary.run_valid:" in error]
    assert len(errors) == 1
    for field in ("data.summary.exactness", "data.exactness", "derived.exactness"):
        assert field in errors[0]


@pytest.mark.parametrize("mode", ["EXACTLY_ONCE", "AT_LEAST_ONCE"])
@pytest.mark.parametrize("counter", ["invalid_rows", "unoffered_rows"])
def test_rows_outside_the_offer_fail_the_recorded_verdicts_consistently(
    tmp_path: Path, mode: str, counter: str
) -> None:
    from ingest_bench.collect.schema import parse_result

    raw = cast(dict[str, Any], build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    raw["run"]["spec"]["scoring"] = {**raw["run"]["spec"].get("scoring", {}), "exactness": mode}
    summary = raw["data"]["summary"]
    summary.update(exactness_mode=mode, run_valid=False)
    for exactness in (summary["exactness"], raw["data"]["exactness"], raw["derived"]["exactness"]):
        exactness.update({"mode": mode, "exact": False, "verdict": False, counter: 3})

    assert _exactness_failures(parse_result(raw)) == []


@pytest.mark.parametrize("evidence", ["overwrite", "replace", "delete", "records_dropped", "files_replaced"])
def test_ingest_only_result_rejects_mutations_without_a_live_scan(tmp_path: Path, evidence: str) -> None:
    from ingest_bench.collect.schema import parse_result, readiness_failures
    from ingest_bench.collect.validate import document_failures

    raw = cast(dict[str, Any], build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    if evidence in {"overwrite", "replace", "delete"}:
        raw["artifacts"]["snapshots"][-1]["operation"] = evidence
    else:
        raw["compaction"] = {
            "records_dropped": 0,
            "rates": {"files_replaced": 0},
            "floor_ratio": None,
            "rewrite_share_of_writes": None,
            "written_per_ingested_byte": None,
            "converged": None,
        }
        if evidence == "records_dropped":
            raw["compaction"]["records_dropped"] = 1
        else:
            raw["compaction"]["rates"]["files_replaced"] = 1
    parsed = parse_result(raw)
    assert any("ingest-only" in error for error in readiness_failures(parsed))
    assert any("ingest-only" in error for error in document_failures(parsed, workloads=fixture_workloads(tmp_path)))

    raw["data"]["summary"]["run_valid"] = False
    assert not any(
        "ingest-only" in error for error in document_failures(parse_result(raw), workloads=fixture_workloads(tmp_path))
    ), "failed probes remain publishable without claiming row preservation"


@pytest.mark.parametrize("operation", ["delete", "overwrite", "replace"])
def test_ingest_only_result_rejects_mutations_after_the_last_scorer_poll(tmp_path: Path, operation: str) -> None:
    from ingest_bench.collect.schema import parse_result, readiness_failures
    from ingest_bench.collect.validate import document_failures

    raw = cast(dict[str, Any], build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    assert all(snapshot["operation"] == "append" for snapshot in raw["artifacts"]["snapshots"])
    # Row-level deletes can leave both data-file counts and physical row totals unchanged.
    raw["compaction"] = {
        "file_counts": [{"operation": "append"}, {"operation": operation}],
        "records_dropped": 0,
        "rates": {"files_replaced": 0},
        "floor_ratio": None,
        "rewrite_share_of_writes": None,
        "written_per_ingested_byte": None,
        "converged": None,
    }
    parsed = parse_result(raw)
    assert any("ingest-only" in error for error in readiness_failures(parsed))
    assert any("ingest-only" in error for error in document_failures(parsed, workloads=fixture_workloads(tmp_path)))


def _run(results_dir: Path) -> subprocess.CompletedProcess[str]:
    """Validate against the fixture track and specs, which sit beside the results directory."""
    root = results_dir.parent
    workloads, specs = fixture_workloads(root), fixture_specs(root)
    command = [sys.executable, str(SCRIPT), str(results_dir), "--workloads", str(workloads), "--specs", str(specs)]
    return subprocess.run(command, capture_output=True, text=True)


def _rerender(results: Path) -> None:
    """Regenerate `RESULTS.md` from whatever `.json` files `results` holds now."""
    documents = [(path, json.loads(path.read_text())) for path in sorted(results.glob("**/*.json"))]
    table = render_table(documents, results_dir=results, workloads=fixture_workloads(results.parent))
    (results / "RESULTS.md").write_text(table)


def _valid_results_dir(tmp_path: Path) -> tuple[Path, Path]:
    """One publishable flink result, with a `RESULTS.md` that matches it."""
    results = tmp_path / "results"
    flink_dir = results / "flink" / "smoke" / "2026-09-09" / "05-25-08Z"
    flink_dir.mkdir(parents=True)
    document = build_document(build_run_dir(tmp_path / "run"), write_site(tmp_path / "run"))
    doc_path = flink_dir / "result.json"
    doc_path.write_text(json.dumps(document))
    _rerender(results)
    return results, doc_path


def _mutated(tmp_path: Path, mutate: Callable[[dict[str, object]], None], *, rerender: bool = True) -> Path:
    results, doc_path = _valid_results_dir(tmp_path)
    document = json.loads(doc_path.read_text())
    mutate(document)
    commit_spec(tmp_path, document)
    doc_path.write_text(json.dumps(document))
    if rerender:
        _rerender(results)
    return results


def test_validate_results_passes_on_a_valid_document_and_a_fresh_table(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_passes_on_an_empty_directory(tmp_path: Path) -> None:
    results = tmp_path / "results"
    results.mkdir()
    (results / "RESULTS.md").write_text(render_table([]))
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_on_a_missing_results_directory(tmp_path: Path) -> None:
    missing = tmp_path / "results"
    result = _run(missing)
    assert result.returncode == 1
    assert str(missing) in result.stdout


@pytest.mark.parametrize("count", [0, -1])
def test_validate_rejects_nonpositive_fleet_counts(tmp_path: Path, count: int) -> None:
    results, path = _valid_results_dir(tmp_path)
    document = json.loads(path.read_text())
    document["run"]["fleet"][0]["count"] = count
    path.write_text(json.dumps(document))
    _rerender(results)

    result = _run(results)

    assert result.returncode == 1
    assert "non-positive count" in result.stdout


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema", "other"),
        ("offered_bytes_per_s", 10_000_000),
        ("duration_s", 600),
        ("partition_count", 128),
        ("alpha", 0.0),
        ("target_row_bytes", 512),
        ("batch_interval_ms", 2_000),
    ],
)
def test_validate_rejects_corpus_scalars_that_disagree_with_the_preset(
    tmp_path: Path, field: str, value: object
) -> None:
    results, doc_path = _valid_results_dir(tmp_path)
    document = json.loads(doc_path.read_text())
    document["run"]["corpus"][field] = value
    doc_path.write_text(json.dumps(document))
    _rerender(results)

    result = _run(results)

    assert result.returncode == 1
    assert f"run.corpus.{field}:" in result.stdout
    assert "shipped preset" in result.stdout


def test_validate_results_fails_on_an_account_id_in_a_string(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"arn:aws:glue:us-east-1:{PRIVATE_ACCOUNT_ID}:table/ns/t"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "account_id" in result.stdout


def test_validate_results_passes_a_twelve_digit_number_joined_to_a_name(tmp_path: Path) -> None:
    """A name that ends in twelve digits, such as a table or a file, does not identify an account."""

    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"{run['table']}-{PRIVATE_ACCOUNT_ID}"

    result = _run(_mutated(tmp_path, mutate))
    assert "account_id" not in result.stdout


def test_validate_results_passes_on_a_twelve_digit_numeric_byte_total(tmp_path: Path) -> None:
    """Large numeric byte totals can have twelve digits. Account-id checks apply
    to strings, not JSON numbers.
    """

    def mutate(document: dict[str, object]) -> None:
        document["geometry"] = {
            "final": {
                "live": {
                    "bytes": 360_000_000_000,
                    "size_quantiles": {"p50": 33_554_432.0},
                    "small_file_share_32mib": 0.0,
                }
            }
        }

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_on_a_twelve_digit_number_inside_a_uri(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"s3://bucket/AWSLogs/{PRIVATE_ACCOUNT_ID}/"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "account_id" in result.stdout
    assert ": uri:" in result.stdout


def test_validate_results_fails_on_a_non_placeholder_uri(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["table"] = f"{run['table']} s3://leaked-bucket/x"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert ": uri:" in result.stdout


def test_validate_results_fails_when_the_producer_was_shortened(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        spec = run["spec"]
        assert isinstance(spec, dict)
        producer = spec["producer"]
        assert isinstance(producer, dict)
        producer["seconds"] = 299

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "producer.seconds" in result.stdout


@pytest.mark.parametrize(
    ("field", "value", "rule"),
    [
        ("speed", 2.0, "data.summary.speed"),
        ("warmup_s", 30, "data.summary.warmup_s"),
        ("freshness_bound_s", 90.0, "data.summary.freshness_bound_s"),
    ],
)
def test_validate_results_fails_when_a_sustainable_summary_disagrees_with_the_spec(
    tmp_path: Path, field: str, value: object, rule: str
) -> None:
    def mutate(document: dict[str, object]) -> None:
        data = document["data"]
        assert isinstance(data, dict)
        data["summary"][field] = value

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert rule in result.stdout


def test_validate_results_fails_when_the_corpus_hash_does_not_match_the_shipped_preset(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["corpus_hash"] = "deadbeef"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "corpus_hash" in result.stdout


@pytest.mark.parametrize("seed,row_block", [(7, 1024), (1, 64)])
def test_validate_results_accepts_nondefault_generator_inputs(tmp_path: Path, seed: int, row_block: int) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["corpus_seed"] = seed
        run["corpus_row_block"] = row_block
        preset = load_preset("smoke", workloads_dir=SCRIPT.parents[1] / "workloads")
        run["corpus_hash"] = corpus_hash(preset, seed=seed, row_block=row_block)
        corpus = run["corpus"]
        data = document["data"]
        assert isinstance(corpus, dict) and isinstance(data, dict)
        corpus["hash"] = run["corpus_hash"]
        summary = data["summary"]
        assert isinstance(summary, dict)
        summary["corpus_hash"] = run["corpus_hash"]
        summary["corpus_seed"] = seed
        summary["corpus_row_block"] = row_block

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize(
    "field,value,rule",
    [
        ("corpus_seed", None, "corpus_seed"),
        ("corpus_seed", 7, "corpus_hash"),
        ("corpus_generator_version", None, "corpus_generator_version"),
        ("corpus_generator_version", "0", "corpus_generator_version"),
        ("corpus_row_block", None, "corpus_row_block"),
        ("corpus_row_block", 0, "corpus_row_block"),
        ("corpus_row_block", 64, "corpus_hash"),
        ("corpus_hash", "e13842f9", "corpus_hash"),
    ],
)
def test_validate_results_rejects_missing_or_mismatched_corpus_identity(
    tmp_path: Path, field: str, value: object, rule: str
) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run[field] = value

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert rule in result.stdout


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("corpus_hash", "deadbeef"),
        ("corpus_seed", 7),
        ("corpus_generator_version", "0"),
        ("corpus_row_block", 64),
    ],
)
def test_validate_results_rejects_scorer_identity_that_disagrees_with_the_run(
    tmp_path: Path, field: str, value: object
) -> None:
    def mutate(document: dict[str, object]) -> None:
        data = document["data"]
        assert isinstance(data, dict)
        summary = data["summary"]
        assert isinstance(summary, dict)
        summary[field] = value

    result = _run(_mutated(tmp_path, mutate))

    assert result.returncode == 1
    assert f"data.summary.{field}" in result.stdout


def _write_readme(results: Path, block: str | None = None) -> Path:
    """A README beside ``results`` whose marked block is fresh, or holds ``block``."""
    from ingest_bench.collect.table import README_END, README_START, render_readme_block, splice_readme

    documents = [(path, json.loads(path.read_text())) for path in sorted(results.glob("**/*.json"))]
    if block is None:
        block = render_readme_block(
            documents, workloads=fixture_workloads(results.parent), results_dir=results, prefix="results/"
        )
    readme = results.parent / "README.md"
    template = f"# Project\n\n{README_START}\n{README_END}\n"
    readme.write_text(cast(str, splice_readme(template, block)))
    return readme


def test_validate_results_passes_with_a_fresh_readme_block(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    _write_readme(results)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_on_a_stale_readme_block(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    _write_readme(results, block="stale\n")
    result = _run(results)
    assert result.returncode != 0
    assert "README.md: results_block: does not match" in result.stdout


def test_validate_results_ignores_a_readme_without_markers(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    (results.parent / "README.md").write_text("# Project\n")
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_committed_track_form_spec_matches_the_full_spec_a_result_embeds(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    specs = fixture_specs(tmp_path)
    (specs / "collect-flink.yaml").write_text(
        "name: collect-flink\nengine: flink\ntrack: smoke\nproducer:\n  shards: 1\nflink:\n"
        "  taskmanagers: 2\n  slots: 4\n  tm_cpu: 2\n  tm_mem_mb: 4096\n  jm_cpu: 1\n  jm_mem_mb: 2048\n"
        "  checkpoint_interval: 10s\n  distribution_mode: hash\n  machine_type: m6i.xlarge\n"
    )
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def _tighten_fixture_track(root: Path, bound_s: int, max_bound_s: int) -> None:
    """Hold the fixture smoke track to tighter bounds than the fixture result was scored under (60 s)."""
    track = fixture_workloads(root) / "tracks" / "smoke.yaml"
    track.write_text(
        track.read_text().replace(
            "freshness_bound_s: 60,", f"freshness_bound_s: {bound_s}, freshness_max_bound_s: {max_bound_s},"
        )
    )


def test_a_result_scored_under_looser_bounds_conforms_when_it_meets_the_tracks(tmp_path: Path) -> None:
    # The fixture result measured p95 14.3 s and max 27.4 s.
    _tighten_fixture_track(tmp_path, 20, 30)
    results, _ = _valid_results_dir(tmp_path)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "Freshness max: ≤ 30 s" in (results / "RESULTS.md").read_text()


def test_a_valid_result_past_the_tracks_freshness_bounds_publishes_as_not_passing(tmp_path: Path) -> None:
    _tighten_fixture_track(tmp_path, 12, 20)
    results, _ = _valid_results_dir(tmp_path)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr
    table = (results / "RESULTS.md").read_text()
    failed = table[table.index("### Did not validate") :]
    assert "over-bound" in failed
    assert "freshness p95 is 14.345 s where track 'smoke' bounds it at 12 s" in failed
    assert "freshness max is 27.445 s where track 'smoke' bounds it at 20 s" in failed


def test_a_committed_spec_with_another_setting_does_not_match(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    specs = fixture_specs(tmp_path)
    flink = (specs / "collect-flink.yaml").read_text().replace("taskmanagers: 2", "taskmanagers: 3")
    (specs / "collect-flink.yaml").write_text(flink)
    result = _run(results)
    assert result.returncode == 1
    assert "no spec under" in result.stdout


def test_a_committed_spec_matches_a_run_placed_on_other_nodes(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    specs = fixture_specs(tmp_path)
    flink = (specs / "collect-flink.yaml").read_text()
    (specs / "collect-flink.yaml").write_text(
        flink + "  extra_flink_conf:\n"
        '    kubernetes.taskmanager.node-selector: "eks.amazonaws.com/nodegroup:engine"\n'
        '    kubernetes.jobmanager.node-selector: "eks.amazonaws.com/nodegroup:shared"\n'
    )
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_committed_spec_with_other_engine_conf_does_not_match(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    specs = fixture_specs(tmp_path)
    flink = (specs / "collect-flink.yaml").read_text()
    (specs / "collect-flink.yaml").write_text(
        flink + '  extra_flink_conf:\n    taskmanager.load-balance.mode: "TASKS"\n'
    )
    result = _run(results)
    assert result.returncode == 1
    assert "no spec under" in result.stdout


def test_validate_results_fails_on_a_stale_results_md(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        run["variant"] = "sorted"

    result = _run(_mutated(tmp_path, mutate, rerender=False))
    assert result.returncode == 1
    assert "results_md" in result.stdout


def test_validate_results_fails_on_a_null_derived_keepup(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        derived["keepup"] = None

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "derived.keepup" in result.stdout


def test_validate_results_fails_on_a_fleet_that_discloses_no_machine_type(tmp_path: Path) -> None:
    for index, absent in enumerate(("", MACHINE_TYPE_UNSPECIFIED, f"{PLACEHOLDER}MACHINE_TYPE")):

        def mutate(document: dict[str, object], absent: str = absent) -> None:
            run = document["run"]
            assert isinstance(run, dict)
            fleet = run["fleet"]
            assert isinstance(fleet, list)
            for role in fleet:
                assert isinstance(role, dict)
                role["machine_type"] = absent

        result = _run(_mutated(tmp_path / f"case{index}", mutate))
        assert result.returncode != 0, result.stdout
        assert "no machine_type" in result.stdout, result.stdout


@pytest.mark.parametrize("seconds", [None, 300, 301])
def test_validate_results_accepts_a_full_corpus_offer(tmp_path: Path, seconds: int | None) -> None:
    def mutate(document: dict[str, object]) -> None:
        run = document["run"]
        assert isinstance(run, dict)
        spec = run["spec"]
        assert isinstance(spec, dict)
        producer = spec["producer"]
        assert isinstance(producer, dict)
        # The whole corpus is the default, and the spec loader refuses an explicit null.
        if seconds is None:
            producer.pop("seconds", None)
        else:
            producer["seconds"] = seconds

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_when_two_results_measured_the_same_table(tmp_path: Path) -> None:
    results, first = _valid_results_dir(tmp_path)
    document = json.loads(first.read_text())
    document["run"]["run_id"] = "collect-flink-20260909T062508Z"
    second = first.parent.parent / "06-25-08Z" / "result.json"
    second.parent.mkdir()
    second.write_text(json.dumps(document))
    _rerender(results)
    result = _run(results)
    assert result.returncode != 0, result.stdout
    assert "is also the table of" in result.stdout, result.stdout
    assert "is also the topic of" in result.stdout, result.stdout


def test_validate_results_fails_when_resource_cost_is_unavailable(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        derived["cost"] = {"usd_per_hour": None}

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode != 0, result.stdout
    assert "derived.cost.usd_per_hour" in result.stdout


def test_validate_results_fails_when_the_hourly_cost_disagrees_with_the_fleet(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        cost = derived["cost"]
        assert isinstance(cost, dict)
        cost["usd_per_hour"] += 1.0

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "derived.cost.usd_per_hour: does not match the fleet cost" in result.stdout


@pytest.mark.parametrize(
    ("usd", "run_hours", "rule"),
    [("5.0", 1.0, "derived.cost.usd: must be a finite number"), (5.0, None, "must both be null or both be numbers")],
)
def test_validate_results_fails_on_a_malformed_total_cost(
    tmp_path: Path, usd: object, run_hours: object, rule: str
) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        derived["cost"].update(usd=usd, run_hours=run_hours)

    result = _run(_mutated(tmp_path, mutate, rerender=False))
    assert result.returncode == 1
    assert rule in result.stdout


def test_validate_results_fails_when_the_total_cost_disagrees_with_the_hourly_rate(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        derived = document["derived"]
        assert isinstance(derived, dict)
        cost = derived["cost"]
        assert isinstance(cost, dict)
        cost["usd"] += 1.0

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "derived.cost.usd: does not match" in result.stdout


@pytest.mark.parametrize(
    "field",
    [
        ("data", "summary", "exactness_mode"),
        ("data", "summary", "exactness", "mode"),
        ("data", "exactness", "mode"),
        ("derived", "exactness", "mode"),
    ],
)
def test_validate_rejects_exactness_modes_that_disagree_with_the_spec(tmp_path: Path, field: tuple[str, ...]) -> None:
    def mutate(document: dict[str, object]) -> None:
        target = document
        for name in field[:-1]:
            child = target[name]
            assert isinstance(child, dict)
            target = child
        target[field[-1]] = "AT_LEAST_ONCE"

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert ".".join(field) in result.stdout
    assert "run.spec.scoring.exactness" in result.stdout


def test_validate_rejects_a_valid_run_with_a_failed_exactness_verdict(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        document["derived"]["exactness"]["verdict"] = False  # type: ignore[index]

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "run_valid" in result.stdout
    assert "derived.exactness.verdict" in result.stdout


def test_validate_rejects_an_exactness_verdict_that_disagrees_with_its_counts(tmp_path: Path) -> None:
    def mutate(document: dict[str, object]) -> None:
        document["derived"]["exactness"]["duplicate_rows"] = 2  # type: ignore[index]

    result = _run(_mutated(tmp_path, mutate))
    assert result.returncode == 1
    assert "derived.exactness.verdict" in result.stdout
    assert "counts" in result.stdout


def test_validate_accepts_at_least_once_with_duplicates(tmp_path: Path) -> None:
    results, path = _valid_results_dir(tmp_path)
    document = json.loads(path.read_text())
    document["run"]["spec"]["scoring"] = {**document["run"]["spec"]["scoring"], "exactness": "AT_LEAST_ONCE"}
    summary = document["data"]["summary"]
    summary["exactness_mode"] = "AT_LEAST_ONCE"
    for exactness in (summary["exactness"], document["data"]["exactness"], document["derived"]["exactness"]):
        exactness.update(mode="AT_LEAST_ONCE", exact=False, verdict=True, duplicate_rows=2)
    commit_spec(tmp_path, document)
    path.write_text(json.dumps(document))
    _rerender(results)

    result = _run(results)

    assert result.returncode == 0, result.stdout + result.stderr


def test_validate_results_fails_on_a_cost_column_with_no_price_behind_it(tmp_path: Path) -> None:
    for field in ("vcpu_hour_usd", "gib_hour_usd"):

        def mutate(document: dict[str, object], field: str = field) -> None:
            run = document["run"]
            assert isinstance(run, dict)
            pricing = run["site_pricing"]
            assert isinstance(pricing, dict)
            pricing[field] = 0.0

        result = _run(_mutated(tmp_path / field, mutate))
        assert result.returncode != 0, result.stdout
        assert f"site_pricing.{field}" in result.stdout, result.stdout


@pytest.mark.parametrize("value", [None, [], "not a document", 3, {"schema_version": 3}])
def test_validate_reports_invalid_document_shapes(tmp_path: Path, value: object) -> None:
    path = tmp_path / "broken.json"
    path.write_text(json.dumps(value))
    result = _run(tmp_path)
    assert result.returncode == 1
    assert str(path) in result.stdout
    assert "Traceback" not in result.stderr
    assert "results_md" not in result.stdout


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (("run", "spec"), []),
        (("run", "fleet"), [None]),
        (("run", "site_pricing"), {}),
        (("run", "site_pricing", "vcpu_hour_usd"), True),
        (("derived", "cost", "usd_per_hour"), "1.2"),
        (("derived", "freshness", "window", "p50_s"), float("nan")),
        (("data", "summary", "run_valid"), "false"),
        (("geometry",), {"final": {"live": {}}}),
    ],
)
def test_validate_reports_nested_shape_errors(tmp_path: Path, field: tuple[str, ...], value: object) -> None:
    results, path = _valid_results_dir(tmp_path)
    document = json.loads(path.read_text())
    target = document
    for name in field[:-1]:
        target = target[name]
    target[field[-1]] = value
    path.write_text(json.dumps(document))
    result = _run(results)
    assert result.returncode == 1
    assert ".".join(field) in result.stdout
    assert "Traceback" not in result.stderr
    assert "results_md" not in result.stdout


def test_validate_reports_missing_summary_without_rendering_it(tmp_path: Path) -> None:
    results, path = _valid_results_dir(tmp_path)
    document = json.loads(path.read_text())
    document["data"]["summary"] = None
    path.write_text(json.dumps(document))
    result = _run(results)
    assert result.returncode == 1
    assert "data.summary: is null" in result.stdout
    assert "Traceback" not in result.stderr
    assert "results_md" not in result.stdout


@pytest.mark.parametrize(
    "relative",
    [
        "spark/smoke/2026-09-09/05-25-08Z/result.json",
        "flink/other/2026-09-09/05-25-08Z/result.json",
        "flink/smoke/2026-09-10/05-25-08Z/result.json",
        "flink/smoke/2026-09-09/06-25-08Z/result.json",
        "flink/smoke/2026-09-09/05-25-08Z/other.json",
        "flink/smoke/05-25-08Z/result.json",
        "flink/smoke/2026-09-09-hash-5c10g-m6i.xlarge-collect-flink-20260909T052508Z.json",
    ],
)
def test_validate_requires_the_canonical_document_path(tmp_path: Path, relative: str) -> None:
    results, original = _valid_results_dir(tmp_path)
    moved = results / relative
    moved.parent.mkdir(parents=True, exist_ok=True)
    original.rename(moved)
    _rerender(results)
    result = _run(results)
    assert result.returncode == 1
    assert "path:" in result.stdout
    assert "flink/smoke/2026-09-09/05-25-08Z/result.json" in result.stdout


def test_validate_requires_external_notes_beside_the_result(tmp_path: Path) -> None:

    results = tmp_path / "results"
    path = results / "external/smoke/2026-09-09/05-25-08Z/result.json"
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps(build_document(build_run_dir(tmp_path / "run", spec=EXTERNAL_SPEC), write_site(tmp_path / "run")))
    )
    _rerender(results)
    absent = _run(results)
    assert absent.returncode == 1
    assert "notes:" in absent.stdout
    assert str(path.with_name("README.md")) in absent.stdout
    path.with_name("README.md").write_text("Read the topic with the operator's writer.\n")
    _rerender(results)
    present = _run(results)
    assert present.returncode == 0, present.stdout + present.stderr


@pytest.mark.parametrize("seconds", [None, 300])
def test_validate_rejects_null_corpus_without_a_traceback(tmp_path: Path, seconds: int | None) -> None:
    results, path = _valid_results_dir(tmp_path)
    doc = json.loads(path.read_text())
    doc["run"]["corpus"] = None
    doc["run"]["spec"]["producer"]["seconds"] = seconds
    path.write_text(json.dumps(doc))
    result = _run(results)
    assert result.returncode == 1
    assert "run.corpus: is null" in result.stdout
    assert "Traceback" not in result.stderr
    assert "results_md" not in result.stdout


def test_validate_requires_consistent_corpus_hash_and_name(tmp_path: Path) -> None:
    for field, value in (("hash", "different"), ("name", "different")):
        results, path = _valid_results_dir(tmp_path / field)
        doc = json.loads(path.read_text())
        doc["run"]["corpus"][field] = value
        path.write_text(json.dumps(doc))
        _rerender(results)
        result = _run(results)
        assert result.returncode == 1
        assert f"run.corpus.{field}" in result.stdout
        assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("notes", "rule"),
    [
        ("Storage is s3://leaked-bucket/warehouse.\n", "uri"),
        ("Storage is gs://leaked-bucket/warehouse.\n", "uri"),
        (f"Account: {PRIVATE_ACCOUNT_ID}\n", "account_id"),
    ],
)
def test_validate_scans_adjacent_notes_for_private_data(tmp_path: Path, notes: str, rule: str) -> None:
    results, path = _valid_results_dir(tmp_path)
    notes_path = path.with_name("README.md")
    notes_path.write_text(notes)
    _rerender(results)
    result = _run(results)
    assert result.returncode == 1
    assert f"{notes_path}: {rule}:" in result.stdout


def test_validate_accepts_notes_with_placeholder_roots(tmp_path: Path) -> None:
    results, path = _valid_results_dir(tmp_path)
    path.with_name("README.md").write_text("Artifacts live under <runs_root>; data under <warehouse>.\n")
    _rerender(results)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("relative", ["orphan/notes.md", "README.md", "RESULTS.md"])
@pytest.mark.parametrize(
    ("text", "rule"),
    [
        ("s3://leaked-bucket/data", "uri"),
        ("gs://leaked-bucket/data", "uri"),
        (f"arn:aws:iam::{PRIVATE_ACCOUNT_ID}:root", "account_id"),
    ],
)
def test_validate_scans_all_markdown(tmp_path: Path, relative: str, text: str, rule: str) -> None:
    results, _ = _valid_results_dir(tmp_path)
    path = results / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)

    result = _run(results)

    assert result.returncode == 1
    assert f"{path}: {rule}:" in result.stdout


@pytest.mark.parametrize(
    ("leak", "rule"),
    [
        ("s3://leaked-bucket/warehouse", "uri"),
        (f"arn:aws:iam::{PRIVATE_ACCOUNT_ID}:root", "account_id"),
        (PRIVATE_ACCOUNT_ID, None),
        (int(PRIVATE_ACCOUNT_ID), None),
    ],
)
def test_validate_scans_documents_that_fail_to_parse(tmp_path: Path, leak: str | int, rule: str | None) -> None:
    """Report privacy failures alongside schema errors, without flagging numeric measurements."""
    path = tmp_path / "broken.json"
    path.write_text(json.dumps({"schema_version": 5, "value": leak}))

    result = _run(tmp_path)

    assert result.returncode == 1
    assert f"{path}: schema_version:" in result.stdout
    if rule is None:
        assert "account_id:" not in result.stdout
    else:
        assert result.stdout.count(f"{path}: {rule}:") == 1


def test_validate_scans_documents_that_are_not_json(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text('{"warehouse": "s3://leaked-bucket/warehouse",')

    result = _run(tmp_path)

    assert result.returncode == 1
    assert f"{path}: json:" in result.stdout
    assert result.stdout.count(f"{path}: uri:") == 1


def test_validate_allows_documentation_to_name_uri_schemes(tmp_path: Path) -> None:
    results, _ = _valid_results_dir(tmp_path)
    (results / "README.md").write_text("Redact `s3://` and `gs://` URIs before publishing.")
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("root", ["<bucket>", "$BUCKET", "${BUCKET}", "YOUR_BUCKET"])
@pytest.mark.parametrize("scheme", ["s3", "s3a", "gs"])
def test_uri_placeholders_are_allowed_by_both_scanners(root: str, scheme: str) -> None:
    from ingest_bench.collect.validate import _uri_failures
    from tests.test_public_surface import leaks_in

    text = f"Example: `{scheme}://{root}/path`."
    assert _uri_failures(text) == []
    assert leaks_in(text) == []


@pytest.mark.parametrize("root", ["<bucket>-private", "$BUCKET-private", "YOUR_BUCKET-private"])
def test_partial_placeholders_do_not_hide_bucket_names(root: str) -> None:
    from ingest_bench.collect.validate import _uri_failures
    from tests.test_public_surface import leaks_in

    text = "s3:" + f"//{root}/path"
    assert _uri_failures(text)
    assert leaks_in(text)


def test_publication_rejects_json_wire_encoding_for_an_avro_corpus(tmp_path: Path) -> None:
    results, doc_path = _valid_results_dir(tmp_path)
    document = json.loads(doc_path.read_text())
    document["run"]["kafka"]["value_encoding"] = "json"
    doc_path.write_text(json.dumps(document))
    _rerender(results)
    result = _run(results)
    assert result.returncode != 0
    assert "encoding" in result.stdout + result.stderr


def test_publication_accepts_the_shipped_json_corpus(tmp_path: Path) -> None:
    from ingest_bench.corpus.values import corpus_sizing

    results, old_path = _valid_results_dir(tmp_path)
    document = json.loads(old_path.read_text())
    run = document["run"]
    name = "smoke-json"
    p = load_preset(name, workloads_dir=SCRIPT.parents[1] / "workloads")
    sizing = corpus_sizing(p, run["corpus_seed"])
    expected_hash = corpus_hash(p, run["corpus_seed"], row_block=run["corpus_row_block"])
    run["spec"]["corpus"] = name
    run["spec"]["kafka"]["value_encoding"] = "json"
    run["kafka"]["value_encoding"] = "json"
    run["corpus_hash"] = expected_hash
    document["data"]["summary"]["corpus_hash"] = expected_hash
    run["corpus"].update(
        name=name,
        hash=expected_hash,
        target_row_bytes=sizing.target_row_bytes,
        offered_bytes_per_s=sizing.offered_bytes_per_s,
        duration_s=p.duration_s,
        partition_count=p.partition_count,
        alpha=p.alpha,
    )
    new_path = results / "flink" / name / "2026-09-09" / "05-25-08Z" / "result.json"
    new_path.parent.mkdir(parents=True)
    new_path.write_text(json.dumps(document))
    commit_spec(tmp_path, document)
    old_path.unlink()
    _rerender(results)
    result = _run(results)
    assert result.returncode == 0, result.stdout + result.stderr


# ---------------------------------------------------------------------------
# Harness commit, tracks and committed specs
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("commit", [None, "abc1234", HARNESS_COMMIT + "-dirty"])
def test_publication_requires_the_clean_full_harness_commit(tmp_path: Path, commit: str | None) -> None:
    results = _mutated(tmp_path, lambda document: cast(dict[str, Any], document["run"]).update(harness_commit=commit))
    result = _run(results)
    assert result.returncode == 1
    assert "run.harness_commit:" in result.stdout


def test_a_result_that_predates_the_harness_commit_rule_still_validates(tmp_path: Path) -> None:
    def legacy(document: dict[str, object]) -> None:
        document["schema_version"] = 3
        del cast(dict[str, Any], document["run"])["harness_commit"]

    result = _run(_mutated(tmp_path, legacy))
    assert result.returncode == 0, result.stdout + result.stderr


def test_a_sustainable_result_must_conform_to_a_track(tmp_path: Path) -> None:
    def other_topic(document: dict[str, object]) -> None:
        run = cast(dict[str, Any], document["run"])
        run["kafka"]["partitions"] = 8
        run["spec"]["kafka"]["partitions"] = 8

    result = _run(_mutated(tmp_path, other_topic))
    assert result.returncode == 1
    assert "track: conforms to no shipped track" in result.stdout
    assert "kafka.partitions is 8 where track 'smoke' fixes 4" in result.stdout


def test_a_spec_that_names_a_track_must_conform_to_that_track(tmp_path: Path) -> None:
    def claims_json(document: dict[str, object]) -> None:
        cast(dict[str, Any], document["run"])["spec"]["track"] = "smoke-json"

    result = _run(_mutated(tmp_path, claims_json))
    assert result.returncode == 1
    assert "track: corpus is 'smoke' where track 'smoke-json' fixes 'smoke-json'" in result.stdout
    assert "spec: spec.corpus is 'smoke', but track 'smoke-json' fixes it at 'smoke-json'" in result.stdout


def test_the_spec_behind_a_result_must_be_committed(tmp_path: Path) -> None:
    results, doc_path = _valid_results_dir(tmp_path)
    document = json.loads(doc_path.read_text())
    document["run"]["spec"]["producer"] = {"shards": 2}
    doc_path.write_text(json.dumps(document))
    _rerender(results)
    result = _run(results)
    assert result.returncode == 1
    assert "spec: no spec under" in result.stdout
    commit_spec(tmp_path, document)
    assert _run(results).returncode == 0


@pytest.mark.parametrize("key", ["account_id", "AWS_ACCOUNT_ID", "glue.id"])
def test_a_key_that_labels_an_account_marks_its_bare_value(key: str) -> None:
    assert _account_id_failures({key: PRIVATE_ACCOUNT_ID})
    assert _account_id_failures({key: int(PRIVATE_ACCOUNT_ID)})
    assert not _account_id_failures({key: PLACEHOLDER_ACCOUNT_ID})
    assert not _account_id_failures({"bytes": PRIVATE_ACCOUNT_ID})
