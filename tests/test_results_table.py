# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import copy
import json
import re
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest
import yaml

from ingest_bench.collect import leaderboard
from ingest_bench.collect.schema import ResultDocument
from ingest_bench.specs import model
from tests.collect_fixtures import EXTERNAL_SPEC, WORKLOADS, build_document, build_run_dir, render_table, write_site


def _document(**run_fields: Any) -> dict[str, Any]:
    return {
        "schema_version": 3,
        "collected_at": "2026-09-20T12:00:00Z",
        "harness_version": "1.2.3",
        "run": {
            "run_id": "report-flink-20260920T120000Z",
            "engine": "flink",
            "variant": "hash",
            "engine_versions": {"image": "<registry>/project/flink:6d24375", "digest": "sha256:abc"},
            "spec": {"corpus": "ignored-spec-name", "scoring": {"exactness": "EXACTLY_ONCE"}},
            "corpus_hash": "e13842f9",
            "kafka": {
                "partitions": 4,
                "key": "user_id",
                "partition_assignment": "key",
                "value_encoding": "avro",
                "compression": "zstd",
            },
            "corpus": {
                "name": "events-100mbs-skew",
                "hash": "e13842f9",
                "schema": "events",
                "offered_bytes_per_s": 100_000_000,
                "duration_s": 3600,
                "rows_per_s": 100_000,
                "row_count": 360_000_000,
                "batch_interval_ms": 1000,
                "partition_count": 512,
                "alpha": 1.0,
                "target_row_bytes": 1000,
            },
            "fleet": [
                {"role": "jobmanager", "count": 1, "vcpu": 1, "gib": 2, "machine_type": "m6i.xlarge"},
                {"role": "taskmanager", "count": 4, "vcpu": 4, "gib": 8, "machine_type": "m6i.xlarge"},
            ],
            "site_pricing": {"vcpu_hour_usd": 0.04, "gib_hour_usd": 0.005},
            **run_fields,
        },
        "data": {
            "summary": {
                "run_valid": True,
                "state": "drained",
                "reason": None,
                "speed": 1.0,
                "freshness_bound_s": 180,
                "exactness_mode": "EXACTLY_ONCE",
            }
        },
        "derived": {
            "freshness": {"window": {"p50_s": 7.71, "p95_s": 14.3, "max_s": 27.44}},
            "exactness": {
                "exact": True,
                "mode": "EXACTLY_ONCE",
                "verdict": True,
                "loss_rows": 0,
                "duplicate_rows": 0,
                "corrupt_batches": 0,
            },
            "keepup": {"drain_s": 5.87},
            "cost": {"usd_per_hour": 0.85},
            "producer": {"producer_bound": False},
        },
        "geometry": {"final": {"live": {"size_quantiles": {"p50": 64 * 1024**2}, "small_file_share_32mib": 0.124}}},
    }


@pytest.mark.parametrize("version", [1, 2, 5])
def test_result_reader_rejects_incompatible_schema_versions(version: int) -> None:
    from ingest_bench.collect.schema import parse_result

    document = _document()
    document["schema_version"] = version
    with pytest.raises(ValueError, match="schema_version: must be 4"):
        parse_result(document)


@pytest.mark.parametrize("field", ["name", "version"])
def test_external_versions_require_both_name_and_version(field: str) -> None:
    document = _document(engine="external", engine_versions={"name": "Example Engine", "version": "1.0"})
    del document["run"]["engine_versions"][field]
    with pytest.raises(ValueError, match=rf"external.json.*run.engine_versions.{field}"):
        render_table([(Path("external.json"), document)])


@pytest.mark.parametrize(
    "versions",
    [None, {"name": "", "version": "1.0"}, {"name": "Engine", "version": "  "}],
)
def test_external_identity_is_required_before_rendering(versions: dict[str, str] | None) -> None:
    from ingest_bench.collect.schema import parse_result

    document = _document(engine="external", engine_versions=versions)
    with pytest.raises(ValueError, match="run.engine_versions"):
        parse_result(document)


@pytest.mark.parametrize("collected_at", ["not-a-date", "2026-09-14T12:00:00"])
def test_result_parser_requires_a_timestamp_with_an_offset(collected_at: str) -> None:
    from ingest_bench.collect.schema import parse_result

    document = _document()
    document["collected_at"] = collected_at
    with pytest.raises(ValueError, match="collected_at"):
        parse_result(document)


def _rows(text: str) -> list[list[str]]:
    return [
        line[2:-2].split(" | ")
        for line in text.splitlines()
        if line.startswith("| ") and not line.startswith("| Engine |")
    ]


def _table_after(text: str, marker: str) -> list[list[str]]:
    """Rows of the first table after ``marker``, without its header and rule."""
    rows: list[list[str]] = []
    for line in text[text.index(marker) :].splitlines()[1:]:
        if line.startswith("|"):
            if not line.startswith(("| Engine |", "|---")):
                rows.append(line[2:-2].split(" | "))
        elif rows:
            break
    return rows


def _failed_rows(text: str) -> list[list[str]]:
    return _table_after(text, "### Did not validate")


def _superseded_rows(text: str) -> list[list[str]]:
    return _table_after(text, "<details><summary>Superseded")


def _render(document: object) -> str:
    return render_table([(Path("flink/events-1mrps-skew/result.json"), document)])


def test_columns_and_workload_use_recorded_metadata() -> None:
    text = _render(_document())
    assert "## 100 MB/s · skewed keys" in text
    for line in ("  - Partitions: 4", "  - Message key: `user_id`", "  - Partition assignment: key"):
        assert line in text, line
    assert "  - Encoding: Avro" in text and "  - Compression: zstd" in text
    assert "- Track:" not in text and "Warm-up" not in text
    assert "Duplicates" not in text
    assert _rows(text) == [
        [
            "Flink",
            "Ingest only (hash)",
            "1&nbsp;×&nbsp;JM<br>4&nbsp;×&nbsp;TM",
            "17&nbsp;vCPU · 34&nbsp;GiB",
            "8&nbsp;s / 14&nbsp;s",
            "64.0&nbsp;MiB",
            "12%",
            "[json](flink/events-1mrps-skew/result.json)",
        ]
    ]
    assert "Did not validate" not in text
    assert "docs/methodology.md" in text


def test_ingest_only_mutation_cannot_rank_as_valid() -> None:
    document = _document()
    document["artifacts"] = {"snapshots": [{"operation": "overwrite"}]}
    row = _failed_rows(_render(document))[0]
    assert row[4] == "did-not-validate"
    assert "ingest-only" in row[5]


@pytest.mark.parametrize("terminal", [None, {"verdict": False, "snapshot_id": 42}])
def test_compaction_without_terminal_validation_is_classified_as_did_not_validate(terminal: object) -> None:
    document = _document()
    document["run"]["spec"]["compaction"] = {"driver": "inline"}
    if terminal is not None:
        document["terminal_exactness"] = terminal

    row = _failed_rows(_render(document))[0]

    assert row[4] == "did-not-validate"
    assert "terminal_exactness" in row[5]


@pytest.mark.parametrize("status", [None, {"healthy": False, "captured_at": "2026-09-20T11:00:00Z", "pods": []}])
def test_managed_compaction_without_a_healthy_sidecar_is_classified_as_did_not_validate(status: object) -> None:
    document = _document()
    document["run"]["spec"]["compaction"] = {"driver": "spark-rewrite"}
    document["terminal_exactness"] = {"verdict": True, "snapshot_id": 42}
    if status is not None:
        document["compactor_status"] = status

    row = _failed_rows(_render(document))[0]

    assert row[4] == "did-not-validate"
    assert "compactor_status" in row[5]


def test_healthy_managed_compaction_remains_a_valid_result() -> None:
    document = _document()
    document["run"]["spec"]["compaction"] = {"driver": "spark-rewrite"}
    document["terminal_exactness"] = {"verdict": True, "snapshot_id": 42}
    document["compactor_status"] = {
        "healthy": True,
        "captured_at": "2026-09-20T11:00:00Z",
        "pods": [],
    }

    text = _render(document)

    assert "### Did not validate" not in text
    assert _rows(text)[0][4] == "8&nbsp;s / 14&nbsp;s"


def test_readiness_failure_does_not_hide_an_ingest_failure() -> None:
    document = _document()
    document["run"]["spec"]["compaction"] = {"driver": "inline"}
    document["terminal_exactness"] = {"verdict": False, "snapshot_id": 42}
    document["data"]["summary"].update(run_valid=False, state="idle_stop", reason="ingest stalled")

    row = _failed_rows(_render(document))[0]

    assert row[4] == "undersized"
    assert row[5] == "ingest stalled; terminal_exactness.verdict is not true"


def test_render_collected_managed_and_external_results(tmp_path: Path) -> None:
    managed = build_document(build_run_dir(tmp_path / "flink"), write_site(tmp_path / "flink"), variant="hash")
    external = build_document(
        build_run_dir(tmp_path / "external", spec=EXTERNAL_SPEC), write_site(tmp_path / "external"), variant="hash"
    )
    rows = _rows(render_table([(Path("flink.json"), managed), (Path("external.json"), external)]))
    # Cheapest first: the Flink fleet costs 0.25 USD/h and the external one 0.60.
    assert [r[0] for r in rows] == ["Flink", "some-engine"]
    assert rows[0][4] == "8&nbsp;s / 14&nbsp;s"
    assert rows[0][5:7] == ["n/a", "n/a"]


def test_workloads_group_by_workload_freshness_and_exactness_sla() -> None:
    documents = []
    for i, (preset, hash_value, speed, bound, rate) in enumerate(
        [
            ("zeta", "hash1", 1.0, 180, 100_000_000),
            ("alpha", "hash1", 2.0, 180, 100_000_000),
            ("alpha", "hash1", 1.0, 180, 100_000_000),
            ("alpha", "hash1", 1.0, 60, 100_000_000),
            ("alpha", "hash2", 1.0, 180, 100_000_000),
            ("slow", "hash1", 1.0, 180, 500_000),
        ]
    ):
        doc = _document()
        doc["run"]["corpus"].update(name=preset, hash=hash_value, offered_bytes_per_s=rate, duration_s=90)
        doc["data"]["summary"].update(speed=speed, freshness_bound_s=bound)
        documents.append((Path(str(i)), doc))
    text = render_table(documents)
    headings = [line for line in text.splitlines() if line.startswith("## ")]
    assert len(headings) == len(set(headings)) == 6
    assert headings[0] == "## 0.5 MB/s · skewed keys"
    assert headings[-1] == "## 200 MB/s · skewed keys"
    assert "  - Duration: 1.5 minutes" in text
    assert "  - Freshness p95: ≤ 60 s" in text
    assert "  - Rate: 200 MB/s (2.0× alpha)" in text
    assert text == render_table(list(reversed(documents)))

    alternate = _document()
    alternate["run"]["spec"]["scoring"]["exactness"] = "AT_LEAST_ONCE"
    alternate["data"]["summary"]["exactness_mode"] = "AT_LEAST_ONCE"
    alternate["derived"]["exactness"].update(mode="AT_LEAST_ONCE", verdict=True, exact=False, duplicate_rows=100)
    rendered = render_table([(Path("exactly"), _document()), (Path("at-least"), alternate)])
    headings = [line for line in rendered.splitlines() if line.startswith("## ")]
    assert len(headings) == len(set(headings)) == 2
    assert "  - Exactly-once" in rendered and "  - At-least-once" in rendered


def test_workloads_group_by_resolved_kafka_settings_without_superseding() -> None:
    keyed = _document()
    round_robin = _document()
    round_robin["run"]["run_id"] = "report-flink-20260920T130000Z"
    round_robin["run"]["kafka"].update(key=None, partition_assignment="round_robin")

    text = render_table([(Path("keyed"), keyed), (Path("round-robin"), round_robin)])

    headings = [line for line in text.splitlines() if line.startswith("## ")]
    assert len(headings) == len(set(headings)) == 2
    assert "  - Message key: `user_id`" in text and "  - Partition assignment: key" in text
    assert "  - Message key: none" in text and "  - Partition assignment: round-robin" in text
    assert len(_rows(text)) == 2
    assert "Superseded" not in text


def test_valid_rows_sort_by_disclosed_cost_then_compute_then_path() -> None:
    documents = []
    for engine, variant, cost, p95 in [
        ("z", "hash", 0.9, 2),
        ("b", "hash", 0.8, 20),
        ("a", "z", 0.8, 20),
        ("a", "a", 0.8, 20),
        ("fast", "hash", 0.8, 10),
        ("null", "hash", None, 1),
        ("free", "hash", 0, 30),
        ("null-p95", "hash", 0.8, None),
    ]:
        doc = _document(engine=engine, variant=variant)
        doc["derived"]["cost"]["usd_per_hour"] = cost
        doc["derived"]["freshness"]["window"]["p95_s"] = p95
        if engine == "free":
            doc["run"]["site_pricing"] = {"vcpu_hour_usd": 0, "gib_hour_usd": 0}
        documents.append((Path(engine + variant), doc))
    rows = _rows(render_table(documents))
    # Equal costs and fleets fall back to the result path. Unpriced results come last.
    assert [(r[0], r[1]) for r in rows] == [
        (engine, f"Ingest only ({variant})")
        for engine, variant in [
            ("a", "a"),
            ("a", "z"),
            ("b", "hash"),
            ("fast", "hash"),
            ("null-p95", "hash"),
            ("z", "hash"),
            ("free", "hash"),
            ("null", "hash"),
        ]
    ]


def test_latest_selection_crosses_validity_and_ignores_fleet_role_order() -> None:
    old = _document()
    old["run"]["run_id"] = "report-flink-20260918T120000Z"
    old["collected_at"] = "2026-09-22T12:00:00Z"
    middle = copy.deepcopy(old)
    middle["run"]["run_id"] = "report-flink-20260919T120000Z"
    middle["collected_at"] = "2026-09-21T12:00:00Z"
    latest = _document()
    latest["collected_at"] = "2026-09-20T12:00:00Z"
    latest["run"]["fleet"].reverse()
    latest["data"]["summary"].update(run_valid=False, state="idle_stop", reason="stalled")
    other_fleet = _document()
    other_fleet["run"]["fleet"][0]["gib"] = 3
    documents = [
        (Path("old.json"), old),
        (Path("middle.json"), middle),
        (Path("latest.json"), latest),
        (Path("other.json"), other_fleet),
    ]
    text = render_table(documents)
    assert [row[3] for row in _track_rows(text, "100 MB/s · skewed keys")] == ["17&nbsp;vCPU · 35&nbsp;GiB"]
    assert _failed_rows(text)[0][4:6] == ["undersized", "stalled"]
    history = _superseded_rows(text)
    assert history[0] == [
        "Flink",
        "Ingest only (hash)",
        "1&nbsp;×&nbsp;JM<br>4&nbsp;×&nbsp;TM",
        "17&nbsp;vCPU · 34&nbsp;GiB",
        "2026-09-19",
        "[json](middle.json)",
    ]
    assert [row[-1] for row in history] == ["[json](middle.json)", "[json](old.json)"]
    assert text == render_table(list(reversed(documents)))


def test_external_engine_names_have_distinct_latest_results() -> None:
    acme_v1 = _document(
        run_id="report-acme-20260918T120000Z",
        engine="external",
        engine_versions={"name": "Acme", "version": "1.0"},
    )
    acme_v2 = _document(
        run_id="report-acme-20260920T120000Z",
        engine="external",
        engine_versions={"name": "Acme", "version": "2.0"},
    )
    other = _document(
        run_id="report-other-20260919T120000Z",
        engine="external",
        engine_versions={"name": "Other", "version": "1.0"},
    )

    text = render_table([(Path("acme-v1.json"), acme_v1), (Path("acme-v2.json"), acme_v2), (Path("other.json"), other)])

    assert sorted(row[0] for row in _track_rows(text, "100 MB/s · skewed keys")) == ["Acme", "Other"]
    assert [row[-1] for row in _superseded_rows(text)] == ["[json](acme-v1.json)"]


def test_equal_run_times_have_deterministic_latest_selection() -> None:
    docs = [(Path("a.json"), _document()), (Path("b.json"), _document())]
    assert render_table(docs) == render_table(list(reversed(docs)))
    assert len(_track_rows(render_table(docs), "100 MB/s · skewed keys")) == 1


@pytest.mark.parametrize(
    ("state", "label"),
    [
        ("producer_bound", "producer_bound"),
        ("void", "void"),
        ("idle_stop", "undersized"),
        ("drained", "drained"),
        ("running", "not drained"),
    ],
)
def test_invalid_rows_show_state_and_escape_reason(state: str, label: str) -> None:
    doc = _document(engine="external", engine_versions={"name": "My Engine", "version": "1.2"})
    doc["data"]["summary"].update(run_valid=False, state=state, reason="a | b")
    assert _failed_rows(_render(doc))[0] == [
        "My Engine",
        "Ingest only (hash)",
        "1&nbsp;×&nbsp;JM<br>4&nbsp;×&nbsp;TM",
        "17&nbsp;vCPU · 34&nbsp;GiB",
        label,
        "a \\| b",
        "2026-09-20",
        "[json](flink/events-1mrps-skew/result.json)",
    ]


def test_invalid_rows_sort_by_cost_then_compute() -> None:
    docs = []
    for engine, vcpu, date in [("large", 20, "2026-09-01"), ("late", 1, "2026-09-03"), ("early", 1, "2026-09-02")]:
        doc = _document(engine=engine)
        doc["run"]["fleet"][0]["vcpu"] = vcpu
        doc["run"]["run_id"] = f"report-{engine}-{date.replace('-', '')}T000000Z"
        doc["collected_at"] = "2026-09-20T12:00:00Z"
        doc["data"]["summary"].update(run_valid=False, reason=None)
        docs.append((Path(engine), doc))
    rows = _failed_rows(render_table(docs))
    assert [r[0] for r in rows] == ["early", "late", "large"]
    assert [r[6] for r in rows] == ["2026-09-02", "2026-09-03", "2026-09-01"]
    assert all(r[5] == "n/a" for r in rows)


def test_mixed_fleet_and_fractional_resources_round_half_up() -> None:
    doc = _document()
    doc["run"]["fleet"][0].update(machine_type="c7g.large", vcpu=0.5, gib=1.25)
    assert _rows(_render(doc))[0][2:4] == ["1&nbsp;×&nbsp;JM<br>4&nbsp;×&nbsp;TM", "17&nbsp;vCPU · 33&nbsp;GiB"]


@pytest.mark.parametrize(
    "geometry",
    [
        None,
        {"final": None},
        {"final": {"live": {"size_quantiles": {"p50": None}, "small_file_share_32mib": None}}},
    ],
)
def test_null_measurements_render_na(geometry: object) -> None:
    doc = _document(engine_versions=None)
    doc["geometry"] = geometry
    doc["derived"].update(freshness=None, exactness=None, keepup=None)
    doc["derived"]["cost"]["usd_per_hour"] = None
    row = _rows(_render(doc))[0]
    assert row[4:7] == ["n/a"] * 3


def test_null_freshness_quantiles() -> None:
    doc = _document()
    doc["derived"]["freshness"]["window"].update(p50_s=None, p95_s=None, max_s=None)
    assert _rows(_render(doc))[0][4] == "n/a"


@pytest.mark.parametrize("valid", [True, False])
def test_sla_sections_are_ordered_and_identified_without_repeated_columns(valid: bool) -> None:
    documents = []
    for mode in ("AT_LEAST_ONCE", "EXACTLY_ONCE"):
        document = _document()
        document["run"]["spec"]["scoring"]["exactness"] = mode
        document["data"]["summary"].update(exactness_mode=mode, run_valid=valid)
        document["derived"]["exactness"]["mode"] = mode
        documents.append((Path(mode + ".json"), document))
    text = render_table(documents)
    headings = [line for line in text.splitlines() if line.startswith("## ")]
    assert headings == ["## 100 MB/s · skewed keys · exactly once", "## 100 MB/s · skewed keys · at least once"]
    sections = text.split("\n## ")[1:]
    assert "  - Exactly-once" in sections[0] and "  - At-least-once" in sections[1]
    assert "| Exactness |" not in text
    assert text == render_table(list(reversed(documents)))


@pytest.mark.parametrize(
    ("lost", "duplicates", "corrupt", "reason"),
    [
        (0, 3, 0, "exactness: duplicate_rows=3"),
        (2, 3, 1, "exactness: loss_rows=2, duplicate_rows=3, corrupt_batches=1"),
        (0, 0, 2, "exactness: corrupt_batches=2"),
    ],
)
def test_exactness_failures_appear_in_invalid_row_reasons(
    lost: int, duplicates: int, corrupt: int, reason: str
) -> None:
    doc = _document()
    doc["derived"]["exactness"].update(exact=False, loss_rows=lost, duplicate_rows=duplicates, corrupt_batches=corrupt)
    doc["data"]["summary"].update(run_valid=False, reason=reason)

    assert _failed_rows(_render(doc))[0][5] == reason


def test_links_are_relative_to_results_root_and_include_existing_notes(tmp_path: Path) -> None:
    root = tmp_path / "published"
    path = root / "flink" / "smoke" / "2026-09-09" / "05-25-08Z" / "result.json"
    path.parent.mkdir(parents=True)
    path.with_name("README.md").write_text("Setup notes.")
    text = render_table([(path, _document())], results_dir=root)
    assert _rows(text)[0][-1] == "[notes](flink/smoke/2026-09-09/05-25-08Z/README.md)"


def test_empty_table_has_explanation_and_footer_without_table() -> None:
    text = render_table([])
    assert text.startswith("# Results\n\nNo results published yet.\n")
    assert "No results published yet." in text
    assert "| Engine |" not in text
    assert "results-table" in text.splitlines()[-1]


def test_header_reports_sorted_distinct_harness_versions() -> None:
    a, b = _document(), _document(engine="spark")
    b["harness_version"] = "1.0.0"
    text = render_table([(Path("a"), a), (Path("b"), b)])
    assert text.splitlines()[-1] == (
        "_Generated by `results-table` from the results in this directory with harness 1.0.0, 1.2.3. "
        "Do not edit by hand._"
    )
    assert "Harness version" not in text


@pytest.mark.parametrize(
    "field",
    [
        "run.run_id",
        "run.kafka",
        "run.kafka.partitions",
        "run.kafka.key",
        "run.kafka.partition_assignment",
        "run.kafka.value_encoding",
        "run.kafka.compression",
        "run.corpus",
        "run.corpus.name",
        "run.corpus.hash",
        "run.corpus.duration_s",
        "run.engine_versions",
        "run.engine_versions.image",
        "data.summary.speed",
        "data.summary.freshness_bound_s",
        "data.summary.exactness_mode",
        "data.summary.reason",
        "geometry.final.live.small_file_share_32mib",
        "derived.freshness.window.max_s",
        "derived.exactness.mode",
        "derived.exactness.verdict",
    ],
)
def test_missing_fields_fail_with_file_and_field(field: str) -> None:
    doc = _document()
    target = doc
    parts = field.split(".")
    for part in parts[:-1]:
        target = target[part]
    del target[parts[-1]]
    with pytest.raises(ValueError, match=rf"broken.json.*{field}"):
        render_table([(Path("broken.json"), doc)])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("partitions", 0),
        ("partition_assignment", "random"),
        ("value_encoding", "protobuf"),
        ("compression", "brotli"),
    ],
)
def test_invalid_kafka_settings_fail_with_file_and_field(field: str, value: object) -> None:
    doc = _document()
    doc["run"]["kafka"][field] = value
    with pytest.raises(ValueError, match=rf"broken.json.*run.kafka.{field}"):
        render_table([(Path("broken.json"), doc)])


@pytest.mark.parametrize("field", ["corpus", "summary"])
def test_null_workload_identity_is_not_renderable(field: str) -> None:
    doc = _document()
    doc["run" if field == "corpus" else "data"][field] = None
    with pytest.raises(ValueError, match=rf"broken.json.*{field}"):
        render_table([(Path("broken.json"), doc)])


@pytest.mark.parametrize("value", [None, [], {"schema_version": 3}])
def test_results_table_cli_reports_bad_shapes(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], value: object
) -> None:
    from ingest_bench.collect.cli import results_table_main

    source = tmp_path / "broken.json"
    source.write_text(json.dumps(value))
    output = tmp_path / "RESULTS.md"
    assert results_table_main([str(tmp_path), "--out", str(output)]) == 1
    assert str(source) in capsys.readouterr().err
    assert not output.exists()


def test_results_table_cli_refuses_a_missing_results_directory(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from ingest_bench.collect.cli import results_table_main

    missing = tmp_path / "not-there"
    output = tmp_path / "RESULTS.md"
    assert results_table_main([str(missing), "--out", str(output)]) == 1
    assert str(missing) in capsys.readouterr().err
    assert not output.exists()


def test_result_parser_preserves_unconsumed_artifacts() -> None:
    from ingest_bench.collect.schema import parse_result

    doc = _document()
    doc["artifacts"] = {"future_measurement": [1, 2, 3]}
    assert parse_result(doc) is doc


def test_section_title_preserves_fractional_replay_speed() -> None:
    doc = _document()
    doc["data"]["summary"]["speed"] = 1.25
    assert "  - Rate: 125 MB/s (1.25× events-100mbs-skew)" in _render(doc)


@pytest.mark.parametrize(
    ("speed", "rate", "duration"), [(0.5, 50, "2 hours"), (1, 100, "1 hour"), (6, 600, "10 minutes")]
)
def test_section_scales_rate_and_duration_together(speed: float, rate: int, duration: str) -> None:
    doc = _document()
    doc["data"]["summary"]["speed"] = speed
    text = _render(doc)
    heading = next(line for line in text.splitlines() if line.startswith("## "))
    assert heading == f"## {rate} MB/s · skewed keys"
    assert f"  - Duration: {duration}" in text


def test_empty_reason_is_preserved() -> None:
    doc = _document()
    doc["data"]["summary"].update(run_valid=False, reason="")
    assert _failed_rows(_render(doc))[0][5] == ""


def test_results_separate_json_from_avro() -> None:
    avro = _document()
    json_run = _document(run_id="json-flink-20260920T120000Z")
    json_run["run"]["kafka"]["value_encoding"] = "json"
    rendered = render_table([(Path("avro.json"), avro), (Path("json.json"), json_run)])
    assert "  - Encoding: JSON" in rendered and "  - Encoding: Avro" in rendered


def test_a_tracked_result_leads_the_summary_with_its_compute(tmp_path: Path) -> None:
    from tests.collect_fixtures import fixture_workloads

    document = build_document(build_run_dir(tmp_path), write_site(tmp_path))
    text = render_table([(Path("flink/smoke/result.json"), document)], workloads=fixture_workloads(tmp_path))
    summary = text[text.index("## Ingest only") : text.index("\n## ", text.index("## Ingest only") + 1)]
    assert re.search(r"\| Engine \| \[[^]]+\]\(#[^)]+\) \|", summary)
    assert "| Flink | [" in summary and "](flink/smoke/result.json) |" in summary
    assert "- Track: `smoke`" in text and "  - Warm-up: ≤ 60 s" in text


def test_untracked_results_render_without_a_summary() -> None:
    assert "## Ingest only" not in _render(_document())


def test_a_track_with_two_corpus_seeds_gets_one_summary_column(tmp_path: Path) -> None:
    from tests.collect_fixtures import fixture_workloads

    first: dict[str, Any] = build_document(build_run_dir(tmp_path / "a"), write_site(tmp_path / "a"))
    second = copy.deepcopy(first)
    second["run"]["run_id"] = "collect-flink-20260910T052508Z"
    second["run"]["corpus_hash"] = second["run"]["corpus"]["hash"] = "0123abcd"
    documents = [(Path("a/result.json"), first), (Path("b/result.json"), second)]
    text = render_table(documents, workloads=fixture_workloads(tmp_path))
    headings = [line for line in text.splitlines() if line.startswith("## ") and "smoke (" in line]
    assert len(headings) == 2
    header = next(line for line in text.splitlines() if line.startswith("| Engine | ["))
    assert header.count("](#") == 1


def test_summary_cells_escape_an_external_engine_name(tmp_path: Path) -> None:
    from tests.collect_fixtures import fixture_workloads

    document: dict[str, Any] = build_document(build_run_dir(tmp_path, spec=EXTERNAL_SPEC), write_site(tmp_path))
    document["run"]["engine_versions"]["name"] = "Example | Cloud"
    text = render_table([(Path("external/result.json"), document)], workloads=fixture_workloads(tmp_path))
    summary = text[text.index("## Ingest only") :]
    assert "| Example \\| Cloud | [" in summary


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("table", ["oops"], "run.spec.table"),
        ("flink", ["oops"], "run.spec.flink"),
        ("tier", "best", "run.spec.tier"),
    ],
)
def test_result_parser_rejects_malformed_spec_fields(field: str, value: object, message: str) -> None:
    from ingest_bench.collect.schema import parse_result

    document = _document()
    document["run"]["spec"][field] = value
    with pytest.raises(ValueError, match=message):
        parse_result(document)


def test_an_unknown_compaction_driver_is_reported_not_raised_as_a_key_error() -> None:
    document = _document()
    document["run"]["spec"]["compaction"] = {"driver": "ghost"}
    with pytest.raises(ValueError, match="compaction.driver: 'ghost'"):
        _render(document)


@pytest.mark.parametrize(
    ("compaction", "roles"),
    [
        (None, frozenset()),
        ({"driver": "inline"}, frozenset()),
        ({"driver": "spark-rewrite"}, frozenset({"compactor-driver", "compactor-executor"})),
        ({"driver": "flink-rewrite"}, frozenset({"compactor-jobmanager", "compactor-taskmanager"})),
    ],
)
def test_sidecar_roles_come_from_the_compactor_registry(compaction: object, roles: frozenset[str]) -> None:
    from ingest_bench.collect.leaderboard import SIDECAR_CLASSES, sidecar_roles
    from ingest_bench.collect.schema import parse_result

    document = _document()
    if compaction is not None:
        document["run"]["spec"]["compaction"] = compaction
    assert sidecar_roles(parse_result(document)) == roles
    assert SIDECAR_CLASSES == {"spark-compaction", "flink-maintenance"}


def _tracked(**run_fields: Any) -> dict[str, Any]:
    """A document that conforms to the shipped events-1mrps-skew track."""
    from ingest_bench.collect.leaderboard import load_tracks, track_for
    from ingest_bench.collect.schema import parse_result
    from tests.collect_fixtures import WORKLOADS

    document = _document(**run_fields)
    document["run"]["corpus"].update(
        name="events-1mrps-skew",
        hash="59c2b4a1",
        schema="events200",
        offered_bytes_per_s=1_376_000_000,
        rows_per_s=1_000_448,
        row_count=3_601_612_800,
        target_row_bytes=1375,
    )
    document["run"]["corpus_hash"] = run_fields.get("corpus_hash", "59c2b4a1")
    document["run"]["kafka"] = {
        "partitions": 192,
        "key": "partition_key",
        "partition_assignment": "round_robin",
        "value_encoding": "confluent",
        "compression": "lz4",
    }
    document["run"]["spec"]["producer"] = {"behind_max_ms": 10000, "compression": "lz4"}
    assert track_for(parse_result(document), load_tracks(WORKLOADS)) is not None
    return document


def _track_rows(text: str, title: str = "1376 MB/s · skewed keys") -> list[list[str]]:
    return _table_after(text, f"## {title}")


def test_historical_standard_corpus_name_uses_the_current_track() -> None:
    from ingest_bench.collect.leaderboard import load_tracks, track_for
    from ingest_bench.collect.schema import parse_result

    document = _tracked()
    document["run"]["corpus"].update(name="events200-1mrps-skew", hash="59c2b4a1")
    document["run"]["corpus_hash"] = "59c2b4a1"
    track = track_for(parse_result(document), load_tracks(WORKLOADS))
    assert track is not None and track.name == "events-1mrps-skew"
    assert "- Track: `events-1mrps-skew`" in _render(document)


def test_track_section_uses_compute_layout() -> None:
    text = _render(_tracked())
    assert "## 1376 MB/s · skewed keys" in text
    assert "<details><summary>Workload details</summary>" in text
    for line in (
        "- Track: `events-1mrps-skew`",
        "  - Rate: 1376 MB/s",
        "  - Duration: 1 hour",
        "  - Keys: 512",
        "  - Zipf alpha: 1.0",
        "  - Corpus hash: `59c2b4a1`",
        "  - Partitions: 192",
        "  - Message key: `partition_key`",
        "  - Partition assignment: round-robin",
        "  - Encoding: Confluent Avro",
        "  - Compression: lz4",
        "  - Exactly-once",
        "  - Freshness p95: ≤ 90 s",
        "  - Freshness max: ≤ 120 s",
        "  - Warm-up: ≤ 300 s",
        "- Iceberg partitioning: `partition_key`",
    ):
        assert line in text, line
    assert "| Engine | Mode | Fleet | Compute | Freshness p50 / p95 | Median file | Files < 32 MiB | Details |" in text
    assert _track_rows(text) == [
        [
            "Flink",
            "Ingest only (hash)",
            "1&nbsp;×&nbsp;JM<br>4&nbsp;×&nbsp;TM",
            "17&nbsp;vCPU · 34&nbsp;GiB",
            "8&nbsp;s / 14&nbsp;s",
            "64.0&nbsp;MiB",
            "12%",
            "[json](flink/events-1mrps-skew/result.json)",
        ]
    ]
    section = text[text.index("## 1376 MB/s") :]
    assert "$" not in section and "m6i" not in section


def test_hyphenated_roles_do_not_break_and_fractions_of_one_keep_decimals() -> None:
    document = _tracked(
        fleet=[{"role": "schema-registry", "count": 1, "vcpu": 0.5, "gib": 2.5, "machine_type": "m6i.xlarge"}]
    )
    row = _track_rows(_render(document))[0]
    assert row[2] == "1&nbsp;×&nbsp;schema\u2011registry"
    assert row[3] == "0.5&nbsp;vCPU · 3&nbsp;GiB"


def _sidecar(document: dict[str, Any], driver: str = "spark-rewrite") -> dict[str, Any]:
    document["run"]["spec"]["compaction"] = {"driver": driver}
    document["terminal_exactness"] = {"verdict": True, "snapshot_id": 42}
    document["compactor_status"] = {"healthy": True, "captured_at": "2026-09-22T11:00:00Z", "pods": []}
    document["run"]["fleet"] = [
        *document["run"]["fleet"],
        {"role": "compactor-driver", "count": 1, "vcpu": 2, "gib": 8, "machine_type": "m6i.xlarge"},
        {"role": "compactor-executor", "count": 4, "vcpu": 4, "gib": 16, "machine_type": "m6i.xlarge"},
    ]
    return document


def test_rows_order_by_cost_and_extra_valid_fleets_fold_into_other_runs() -> None:
    small = _tracked(run_id="small-flink-20260920T120000Z")
    large = _tracked(run_id="large-flink-20260921T120000Z")
    large["run"]["fleet"][1]["count"] = 8
    large["derived"]["cost"]["usd_per_hour"] = 1.65
    sidecar = _sidecar(_tracked(run_id="side-flink-20260922T120000Z"))
    sidecar["derived"]["cost"]["usd_per_hour"] = 0.5
    text = render_table([(Path(f"{name}.json"), doc) for name, doc in (("l", large), ("s", small), ("c", sidecar))])
    assert [row[1] for row in _track_rows(text)] == ["Spark rewrite sidecar (hash)", "Ingest only (hash)"]
    assert "<details><summary>Other qualifying runs (1)</summary>" in text
    assert "| Engine | Mode | Version | Fleet | Compute | Freshness p50 / p95 | Date | Details |" in text


def test_superseded_runs_are_not_listed_as_qualifying() -> None:
    older = _tracked(run_id="old-flink-20260920T120000Z")
    newer = _tracked(run_id="new-flink-20260921T120000Z")
    newer["data"]["summary"].update(run_valid=False, state="idle_stop", reason="ingest stalled")
    text = render_table([(Path("old.json"), older), (Path("new.json"), newer)])
    assert "Other qualifying runs" not in text
    assert "<details><summary>Superseded runs (1)</summary>" in text
    assert "### Did not validate" in text


def test_zero_pricing_orders_by_compute() -> None:
    free = {"vcpu_hour_usd": 0, "gib_hour_usd": 0}
    small = _tracked(run_id="small-flink-20260920T120000Z", site_pricing=free)
    large = _tracked(run_id="large-flink-20260921T120000Z", site_pricing=free, variant="range")
    large["run"]["fleet"][1]["count"] = 8
    text = render_table([(Path("l.json"), large), (Path("s.json"), small)])
    assert [row[1] for row in _track_rows(text)] == ["Ingest only (hash)", "Ingest only (range)"]


def test_tier_column_appears_only_with_a_reference_row() -> None:
    def section(text: str) -> str:
        return text[text.index("## 1376 MB/s") :]

    assert "| Tier |" not in section(_render(_tracked()))
    external = _tracked(engine="external", engine_versions={"name": "Example", "version": "1"})
    external["run"]["spec"] = {**external["run"]["spec"], "tier": "reference"}
    assert "| Engine | Mode | Tier | Fleet |" in section(_render(external))


def test_duplicate_titles_are_disambiguated() -> None:
    first = _tracked()
    second = _tracked(run_id="seed-flink-20260921T120000Z", corpus_hash="0123abcd")
    second["run"]["corpus"]["hash"] = "0123abcd"
    text = render_table([(Path("a.json"), first), (Path("b.json"), second)])
    assert "## 1376 MB/s · skewed keys · events-1mrps-skew (59c2b4a1)" in text
    assert "## 1376 MB/s · skewed keys · events-1mrps-skew (0123abcd)" in text


def test_slug_matches_github() -> None:
    from ingest_bench.collect.table import _slug

    assert _slug("600 MB/s · skewed keys") == "600-mbs--skewed-keys"
    assert _slug("1376 MB/s · skewed keys · events-1mrps-skew (59c2b4a1)") == (
        "1376-mbs--skewed-keys--events-1mrps-skew-59c2b4a1"
    )


def test_summary_shows_compute_and_orders_rows_by_cost() -> None:
    flink = _tracked()
    external = _tracked(
        run_id="ext-external-20260920T120000Z",
        engine="external",
        engine_versions={"name": "Example", "version": "1"},
        fleet=[{"role": "writer", "count": 2, "vcpu": 2, "gib": 8, "machine_type": "m8g.xlarge"}],
    )
    external["derived"]["cost"]["usd_per_hour"] = 0.2
    text = render_table([(Path("f.json"), flink), (Path("e.json"), external)])
    summary = text[text.index("## Ingest only") : text.index("\n## 1376 MB/s")]
    assert "Requested compute for ingest without file compaction." in summary
    assert "| Engine | [1376 MB/s skewed](#1376-mbs--skewed-keys) |" in summary
    assert [row[0] for row in _rows(summary)] == ["Example", "Flink"]
    assert "4&nbsp;vCPU · 16&nbsp;GiB" in summary
    assert "$" not in text and "Leaderboard" not in text


def test_sidecar_run_fills_a_missing_ingest_only_cell_with_its_ingest_roles() -> None:
    text = _render(_sidecar(_tracked()))
    ingest = text[text.index("## Ingest only") : text.index("## With compaction")]
    assert "17&nbsp;vCPU · 34&nbsp;GiB](flink/events-1mrps-skew/result.json)&nbsp;\\*" in ingest
    assert "\\* No valid ingest-only run exists for this track." in ingest
    compaction = text[text.index("## With compaction") : text.index("\n## 1376 MB/s")]
    assert "| Flink | Spark rewrite sidecar |" in compaction
    assert "35&nbsp;vCPU · 106&nbsp;GiB" in compaction


def test_sidecar_ingest_fallback_uses_machine_specific_prices() -> None:
    from datetime import UTC, datetime

    from ingest_bench.collect.schema import parse_result
    from ingest_bench.collect.table import _Entry, _ingest_cell

    raw = _sidecar(_tracked())
    raw["run"]["site_pricing"] = {"machine_types": {"m6i.xlarge": {"vcpu": 4, "gib": 16, "usd_per_hour": 0.2}}}
    document = parse_result(raw)
    corpus, summary = document["run"]["corpus"], document["data"]["summary"]
    assert corpus is not None and summary is not None
    entry = _Entry(Path("result.json"), document, corpus, summary, datetime.now(UTC), None, "tuned", "spark-compaction")
    cell = _ingest_cell(entry)
    assert (cell.vcpu, cell.gib) == (17, 34)
    assert cell.cost == pytest.approx(0.2 / 2 * (17 / 4 + 34 / 16))
    assert "## Ingest only" in _render(raw)


def test_integrated_compaction_never_falls_back() -> None:
    document = _tracked()
    document["run"]["spec"]["compaction"] = {"driver": "inline"}
    document["terminal_exactness"] = {"verdict": True, "snapshot_id": 42}
    text = _render(document)
    assert "## Ingest only" not in text and "ingest-only run exists" not in text
    assert "| Flink | Integrated compaction |" in text


def test_engine_with_only_sidecar_runs_gets_fallback_row() -> None:
    sidecar = _sidecar(_tracked(run_id="side-flink-20260922T120000Z"))
    external = _tracked(
        run_id="ext-external-20260920T120000Z",
        engine="external",
        engine_versions={"name": "Example", "version": "1"},
    )
    text = render_table([(Path("s.json"), sidecar), (Path("e.json"), external)])
    ingest = text[text.index("## Ingest only") : text.index("## With compaction")]
    assert [row[0] for row in _rows(ingest)] == ["Example", "Flink"]


def test_summary_links_resolve_to_section_anchors() -> None:
    from ingest_bench.collect.table import _slug

    first = _tracked()
    second = _tracked(run_id="seed-flink-20260921T120000Z", corpus_hash="0123abcd")
    second["run"]["corpus"]["hash"] = "0123abcd"
    text = render_table([(Path("a.json"), first), (Path("b.json"), second)])
    headings = {_slug(line[3:]) for line in text.splitlines() if line.startswith("## ")}
    anchors = re.findall(r"\]\(#([^)]+)\)", text)
    assert anchors
    for anchor in anchors:
        assert anchor in headings, anchor


def test_intro_states_the_shared_keep_up_bounds() -> None:
    text = _render(_tracked())
    assert "**Freshness:** lag p95 ≤ 90 s and max ≤ 120 s, after up to 5 minutes of warm-up." in text
    assert "**Exactly-once:** every offered row is committed once, verified by row ID." in text


def test_readme_block_rebases_links_and_replaces_only_the_marked_region() -> None:
    from ingest_bench.collect.table import README_END, README_START, render_readme_block, splice_readme
    from tests.collect_fixtures import WORKLOADS

    block = render_readme_block(
        [(Path("flink/events-1mrps-skew/result.json"), _tracked())],
        workloads=WORKLOADS,
        results_dir=None,
        prefix="results/",
    )
    assert "](results/flink/events-1mrps-skew/result.json)" in block
    assert "(results/RESULTS.md#1376-mbs--skewed-keys)" in block
    assert "[methodology](docs/methodology.md)" in block
    assert "### Ingest only" in block and "\n## " not in block
    assert block.rstrip().endswith("[full results](results/RESULTS.md).")
    text = f"# Title\n\nbefore\n\n{README_START}\nold\n{README_END}\n\nafter\n"
    spliced = splice_readme(text, block)
    assert spliced is not None
    assert spliced.startswith(f"# Title\n\nbefore\n\n{README_START}\n")
    assert spliced.endswith(f"{README_END}\n\nafter\n")
    assert "old" not in spliced
    assert splice_readme(spliced, block) == spliced


@pytest.mark.parametrize(
    ("p95", "maximum", "shown"),
    [(90, 120, True), (90.1, 120, False), (90, 120.1, False), (None, 100, False), (80, None, False)],
)
def test_readme_enforces_its_own_freshness_bounds(p95: float | None, maximum: float | None, shown: bool) -> None:
    from ingest_bench.collect.table import render_readme_block

    document = _tracked()
    document["derived"]["freshness"]["window"].update(p95_s=p95, max_s=maximum)
    block = render_readme_block(
        [(Path("run.json"), document)], workloads=WORKLOADS, results_dir=None, prefix="results/"
    )
    assert "lag p95 ≤ 90 s and max ≤ 120 s" in block
    assert ("### Ingest only" in block) is shown
    assert "Freshness p95: ≤ 90 s" in _render(document)


def test_freshness_failure_reason_uses_compact_decimal_values() -> None:
    document = _tracked()
    document["derived"]["freshness"]["window"].update(p95_s=105.83324999999999, max_s=158.33999999999997)

    text = _render(document)

    assert "freshness p95 is 105.833 s where track 'events-1mrps-skew' bounds it at 90 s" in text
    assert "freshness max is 158.34 s where track 'events-1mrps-skew' bounds it at 120 s" in text
    assert "105.83324999999999" not in text
    assert "158.33999999999997" not in text


def test_readme_does_not_revive_a_superseded_fleet_that_met_the_tighter_bounds() -> None:
    from ingest_bench.collect.table import render_readme_block

    older = _tracked(run_id="old-flink-20260920T120000Z")
    newer = _tracked(run_id="new-flink-20260921T120000Z")
    newer["derived"]["freshness"]["window"].update(p95_s=121, max_s=180)
    block = render_readme_block(
        [(Path("old.json"), older), (Path("new.json"), newer)], workloads=WORKLOADS, results_dir=None, prefix="results/"
    )
    assert "### Ingest only" not in block


def test_readme_keeps_full_results_anchors_when_a_sibling_section_is_filtered() -> None:
    from ingest_bench.collect.table import _slug, render_readme_block

    passing = _tracked()
    hidden = _tracked(run_id="second-flink-20260921T120000Z", corpus_hash="0123abcd")
    hidden["run"]["corpus"]["hash"] = "0123abcd"
    hidden["derived"]["freshness"]["window"]["p95_s"] = 121
    documents = [(Path("pass.json"), passing), (Path("hidden.json"), hidden)]
    full = render_table(documents)
    anchors = {_slug(line[3:]) for line in full.splitlines() if line.startswith("## ")}
    block = render_readme_block(documents, workloads=WORKLOADS, results_dir=None, prefix="results/")
    links = re.findall(r"RESULTS.md#([^)]*)", block)
    assert links and all(anchor in anchors for anchor in links)


def test_readme_without_markers_is_left_alone() -> None:
    from ingest_bench.collect.table import README_END, README_START, splice_readme

    assert splice_readme("# Title\n", "block") is None
    assert splice_readme(f"# Title\n{README_START}\n", "block") is None
    assert splice_readme(f"# Title\n{README_END}\n{README_START}\n", "block") is None


def test_summary_links_notes_and_numbers_align_right(tmp_path: Path) -> None:
    root = tmp_path / "results"
    path = root / "flink" / "events-1mrps-skew" / "2026-09-20" / "12-00-00Z" / "result.json"
    path.parent.mkdir(parents=True)
    path.with_name("README.md").write_text("Notes.")
    larger = root / "flink" / "events-1mrps-skew" / "2026-09-21" / "12-00-00Z" / "result.json"
    big = _tracked(run_id="big-flink-20260921T120000Z")
    big["run"]["fleet"][1]["count"] = 8
    big["derived"]["cost"]["usd_per_hour"] = 1.65
    text = render_table([(path, _tracked()), (larger, big)], results_dir=root)
    summary = text[text.index("## Ingest only") : text.index("\n## 1376 MB/s")]
    assert "](flink/events-1mrps-skew/2026-09-20/12-00-00Z/README.md) |" in summary
    assert "|---|---|---|---:|---:|---:|---:|---|" in text
    assert "|---|---|---|---|---:|---:|---|---|" in text
    assert (
        "<details><summary>Other qualifying runs (1)</summary>\n\nLarger fleets and earlier engine versions.\n" in text
    )


def test_readme_markers_count_only_on_their_own_lines() -> None:
    from ingest_bench.collect.table import README_END, README_START, splice_readme

    quoted = f"The block between `{README_START}` and `{README_END}` is generated.\n"
    assert splice_readme(quoted, "block") is None
    text = f"{quoted}\n{README_START}\nold\n{README_END}\n"
    assert splice_readme(text, "new\n") == f"{quoted}\n{README_START}\nnew\n{README_END}\n"


def test_readme_is_found_beside_a_relative_results_directory(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from ingest_bench.collect.table import readme_for

    (tmp_path / "results").mkdir()
    monkeypatch.chdir(tmp_path / "results")
    assert readme_for(Path(".")) == tmp_path / "README.md"


def test_a_reference_ingest_run_suppresses_the_sidecar_fallback() -> None:
    reference = _tracked(engine="external", engine_versions={"name": "Example", "version": "1"})
    reference["run"]["spec"] = {**reference["run"]["spec"], "tier": "reference"}
    sidecar = _sidecar(
        _tracked(
            run_id="side-external-20260922T120000Z",
            engine="external",
            engine_versions={"name": "Example", "version": "1"},
        )
    )
    text = render_table([(Path("r.json"), reference), (Path("s.json"), sidecar)])
    ingest = text[text.index("## Ingest only") : text.index("## With compaction")]
    assert [row[:2] for row in _rows(ingest)] == [["Example", "reference"]]
    assert "\\*" not in ingest


def test_footnote_says_no_valid_ingest_only_run() -> None:
    failed = _tracked(run_id="fail-flink-20260920T120000Z")
    failed["data"]["summary"].update(run_valid=False, state="idle_stop", reason="stalled")
    text = render_table([(Path("f.json"), failed), (Path("s.json"), _sidecar(_tracked()))])
    assert "\\* No valid ingest-only run exists for this track." in text


def test_tracks_sharing_rate_and_keys_get_their_names_in_headings_and_columns(tmp_path: Path) -> None:
    from tests.collect_fixtures import fixture_workloads

    workloads = fixture_workloads(tmp_path)
    twin = (workloads / "tracks" / "smoke.yaml").read_text().replace("partitions: 4", "partitions: 8")
    (workloads / "tracks" / "smoke-wide.yaml").write_text(twin)
    first: dict[str, Any] = build_document(build_run_dir(tmp_path / "a"), write_site(tmp_path / "a"))
    second = copy.deepcopy(first)
    second["run"]["run_id"] = "collect-flink-20260910T052508Z"
    second["run"]["kafka"]["partitions"] = 8
    second["run"]["spec"]["kafka"] = {**second["run"]["spec"]["kafka"], "partitions": 8}
    text = render_table([(Path("a/result.json"), first), (Path("b/result.json"), second)], workloads=workloads)
    headings = [line for line in text.splitlines() if line.startswith("## ") and "MB/s" in line]
    assert headings == [
        f"{headings[0].split(' · smoke')[0]} · smoke",
        f"{headings[0].split(' · smoke')[0]} · smoke-wide",
    ]
    header = next(line for line in text.splitlines() if line.startswith("| Engine | ["))
    labels = re.findall(r"\[([^]]+)\]\(#", header)
    assert len(labels) == len(set(labels)) == 2
    assert labels[1].endswith("(smoke-wide)")


def _upsert_document(**extra: Any) -> dict[str, Any]:
    document = _document()
    document["run"]["spec"]["scoring"] = {"exactness": "LATEST_PER_KEY"}
    document["data"]["summary"]["exactness_mode"] = "LATEST_PER_KEY"
    document.update(extra)
    return document


@pytest.mark.parametrize("live_state", [None, {"verdict": False}])
def test_an_upsert_run_without_passing_live_state_did_not_validate(live_state: object) -> None:
    document = _upsert_document()
    if live_state is not None:
        document["terminal_upsert"] = live_state
    row = _failed_rows(_render(document))[0]
    assert row[4] == "did-not-validate" and "terminal_upsert" in row[5]


def _health(equality: int, position: int, vectors: int, rows_per_live_row: float) -> dict[str, object]:
    return {
        "delete_files": {
            "equality": {"files": equality},
            "position": {"files": position},
            "deletion_vector": {"files": vectors},
        },
        "rows_per_live_row": rows_per_live_row,
    }


@pytest.mark.parametrize(
    ("health", "deletes", "per_live_row"),
    [
        (_health(1984, 1974, 0, 3.649), "1,984 eq + 1,974 pos", "3.65"),
        (_health(0, 0, 9, 1.0), "9 DV", "1.00"),
        (_health(0, 0, 0, 1.0), "0", "1.00"),
        (None, "n/a", "n/a"),
    ],
)
def test_an_upsert_track_shows_the_read_work_its_table_leaves(
    health: dict[str, object] | None, deletes: str, per_live_row: str
) -> None:
    document = _upsert_document(terminal_upsert={"verdict": True})
    if health is not None:
        document["table_health"] = health
    text = _render(document)
    assert "| Median file | Files < 32 MiB | Delete files | Rows per live row | Details |" in text
    assert _track_rows(text, "100 MB/s · upsert")[0][-3:-1] == [deletes, per_live_row]


def test_an_upsert_track_is_titled_by_its_writes_not_its_key_skew() -> None:
    text = _render(_upsert_document(terminal_upsert={"verdict": True}))
    assert "## 100 MB/s · upsert" in text and "skewed keys" not in text
    # The upsert key is the message key; the partition count is the number of table partition values.
    assert "  - `partition_key` values: 512" in text and "  - Keys: 512" not in text
    assert "  - Zipf alpha: 1.0" in text


def test_the_intro_states_the_upsert_guarantee_beside_exactly_once() -> None:
    upsert_only = _render(_upsert_document(terminal_upsert={"verdict": True}))
    assert "- **Latest per key:** each key's newest version is live, and no other," in upsert_only
    assert "**Exactly-once:**" not in upsert_only
    both = render_table(
        [
            (Path("flink/events-1mrps-skew/a/result.json"), _upsert_document(terminal_upsert={"verdict": True})),
            (Path("flink/events-1mrps-skew/b/result.json"), _document()),
        ]
    )
    assert "- **Exactly-once** (append tracks):" in both and "- **Latest per key** (upsert tracks):" in both
    assert "- **Exactly-once:** every offered row is committed once, verified by row ID." in _render(_document())


def test_a_compacted_upsert_run_publishes_on_live_state_alone() -> None:
    document = _upsert_document(terminal_upsert={"verdict": True})
    document["run"]["spec"]["compaction"] = {"driver": "inline"}
    text = _render(document)
    assert "### Did not validate" not in text


def test_a_track_that_pins_offsets_checks_them() -> None:
    workloads = Path(__file__).resolve().parents[1] / "workloads"
    track = replace(model.load_track("events-1mrps-skew", workloads), geometry_offsets_s=(613, 1427))
    document = _document()

    def offset_mismatches(offsets: list[int]) -> list[str]:
        document["run"]["spec"]["scoring"]["geometry_offsets_s"] = offsets
        found = leaderboard.track_mismatches(cast(ResultDocument, document), track)
        return [mismatch for mismatch in found if "geometry_offsets_s" in mismatch]

    assert offset_mismatches([600])
    assert not offset_mismatches([613, 1427])


SPARK_REFERENCE = {"trigger_interval": "60 seconds", "distribution_mode": "hash", "executors": 4, "executor_cores": 4}
MOR_PROPERTIES = {
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
}
ZSTD_1 = {"write.parquet.compression-codec": "zstd", "write.parquet.compression-level": "1"}
SPARK_MOR_V3: dict[str, object] = {"format_version": 3, "properties": {**MOR_PROPERTIES, **ZSTD_1}}


def _tiered(engine: str, knob_block: dict[str, object], table: dict[str, object] | None, *, upsert: bool) -> str:
    document = _document(engine=engine)
    spec = document["run"]["spec"]
    spec[engine] = knob_block
    spec["scoring"] = {"exactness": "LATEST_PER_KEY" if upsert else "EXACTLY_ONCE"}
    if table is not None:
        spec["table"] = {"partition": "identity(partition_key)", **table}
    return leaderboard.tier(cast(ResultDocument, document))


@pytest.mark.parametrize("engine", ["flink", "spark"])
def test_the_shipped_reference_specs_derive_the_reference_tier(engine: str) -> None:
    path = Path(__file__).resolve().parents[1] / "specs/tracks/events-1mrps-skew"
    raw = yaml.safe_load((path / f"aws-1mrps-skew-{engine}-ref.yaml").read_text())
    document = _document(engine=engine)
    document["run"]["spec"] = model.expand_track(raw, WORKLOADS)
    assert leaderboard.tier(cast(ResultDocument, document)) == "reference"


def test_the_spark_upsert_reference_holds_the_v3_merge_on_read_table() -> None:
    assert _tiered("spark", SPARK_REFERENCE, SPARK_MOR_V3, upsert=True) == "reference"
    copy_on_write = {"format_version": 3, "properties": {"write.merge.mode": "copy-on-write"}}
    assert _tiered("spark", SPARK_REFERENCE, copy_on_write, upsert=True) == "tuned"
    extra = {"format_version": 3, "properties": {**MOR_PROPERTIES, **ZSTD_1, "write.target-file-size-bytes": "1"}}
    assert _tiered("spark", SPARK_REFERENCE, extra, upsert=True) == "tuned"
    # Without a codec Iceberg's Java writers fall back to gzip.
    gzip = {"format_version": 3, "properties": MOR_PROPERTIES}
    assert _tiered("spark", SPARK_REFERENCE, gzip, upsert=True) == "tuned"
    # The same table on an append run is tuning, as before.
    assert _tiered("spark", SPARK_REFERENCE, SPARK_MOR_V3, upsert=False) == "tuned"


def test_a_flink_upsert_reference_holds_a_v3_table_with_only_the_codec() -> None:
    flink = {"checkpoint_interval": "60s", "min_pause": "2s", "distribution_mode": "hash", "taskmanagers": 2}
    assert _tiered("flink", flink, {"format_version": 3, "properties": ZSTD_1}, upsert=True) == "reference"
    assert _tiered("flink", flink, {"format_version": 3}, upsert=True) == "tuned"
    # On v2 the job keeps its equality deletes, which is not the shipped configuration.
    assert _tiered("flink", flink, None, upsert=True) == "tuned"
    assert _tiered("flink", flink, {"format_version": 2}, upsert=True) == "tuned"
    v3_with_properties = {"format_version": 3, "properties": {**ZSTD_1, "write.merge.mode": "merge-on-read"}}
    assert _tiered("flink", flink, v3_with_properties, upsert=True) == "tuned"


def test_compaction_still_makes_an_upsert_run_tuned() -> None:
    document = _document(engine="spark")
    spec = document["run"]["spec"]
    spec["spark"] = SPARK_REFERENCE
    spec["scoring"] = {"exactness": "LATEST_PER_KEY"}
    spec["table"] = SPARK_MOR_V3
    spec["compaction"] = {"driver": "inline"}
    assert leaderboard.tier(cast(ResultDocument, document)) == "tuned"
