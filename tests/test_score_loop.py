# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import io
import json
import re
from collections.abc import Callable
from concurrent.futures import Future
from contextlib import suppress
from dataclasses import replace
from pathlib import Path
from typing import TextIO
from weakref import finalize

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io import FileIO
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.types import LongType, NestedField

from ingest_bench import uri
from ingest_bench.catalog import open_catalog
from ingest_bench.clock import Clock, now_ms
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.producer import publish_log
from ingest_bench.scorer import cli, freshness, keepup, score, snapshots, tally
from ingest_bench.table import create
from tests.test_tally import _rows_of

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


class StepClock:
    """Real ordering, fake time: every sleep advances the clock instantly."""

    def __init__(self, start_ms: int) -> None:
        self.t = start_ms

    def now_ms(self) -> int:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += int(seconds * 1000)


@pytest.fixture
def corpus(tmp_path: Path) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=4", "partition_count=8"],
    )
    generate.generate(p, str(tmp_path / "c"), seed=6, row_block=64)
    return metadata.read(uri.join(str(tmp_path / "c"), preset.corpus_dir_name(p, seed=6, row_block=64)))


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _finished_producer(
    logs: Path, records: list[generate.BatchRecord], epoch: int, late_batch: int = -1, done: bool = True
) -> None:
    """One shard's publish log: every batch acked, then the done trailer."""
    for record in records:
        late = 9000 if record.batch == late_batch else 50
        publish_log.append(
            logs / "publish_log-0.jsonl",
            publish_log.PublishRecord(
                record.batch,
                epoch + record.offset_ms,
                epoch + record.offset_ms + late,
                epoch + record.offset_ms + late + 250,
                record.rows,
                record.encoded_bytes,
                0,
            ),
        )
    if done:
        publish_log.append_done(logs / "publish_log-0.jsonl", 0, len(records))


def test_scorer_drains_and_validates(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run1", corpus, create.parse_partition("identity(partition_key)"), {})
    # The offer happened just before the engine commits, so commit timestamps follow emit times.
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    # The producer already finished: every batch acked within 300 ms of schedule.
    _finished_producer(logs, records, epoch)

    def engine() -> None:
        # Commit two batches per snapshot, out of order within the pair, like a keyed shuffle would.
        for a, b in ((1, 0), (3, 2)):
            table.append(_rows_of(records[a], corpus))
            table.append(_rows_of(records[b], corpus))

    engine()  # deterministic: all commits exist before the scorer starts; the loop still walks them in order
    clock = StepClock(now_ms())
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run1",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, clock, open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is True and summary["state"] == "drained" and summary["prefix"] == 3
    assert summary["reason"] is None
    assert summary["corpus_seed"] == 6
    assert summary["corpus_generator_version"] == corpus.generator_version
    assert summary["corpus_row_block"] == 64

    assert "absorbed_at_offer_end" not in json.dumps(summary)
    keepup = json.loads((args.out_dir / score.KEEPUP_FILE).read_text())
    assert set(keepup) == {"drain_s", "backlog_rows_max", "backlog_rows_p50"}
    fresh = json.loads((tmp_path / "out" / "freshness.json").read_text())
    assert fresh["drained"] and fresh["verdict"] and fresh["window"]["p95_s"] is not None
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["exact"] and exact["rows"] == corpus.row_count
    lines = (tmp_path / "out" / "snapshots.jsonl").read_text().splitlines()
    assert len(lines) == 4 and [json.loads(line)["prefix_after"] for line in lines] == [-1, 1, 1, 3]
    assert "SCORER_DONE run_valid=True" in (tmp_path / "score.log").read_text()


def test_idle_stop_before_drain_is_invalid(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run2", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    table.append(_rows_of(records[0], corpus))  # batch 1 never lands
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run2",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is False and summary["state"] == "idle_stop" and summary["prefix"] == 0
    assert summary["reason"] == score.IDLE_STOP_REASON
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["loss_rows"] == sum(r.rows for r in records[1:])


def test_retained_snapshots_after_rollback_do_not_count_as_delivered(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.rollback", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_rows_of(records[0], corpus))
    first = table.current_snapshot()
    assert first is not None
    for record in records[1:]:
        table.append(_rows_of(record, corpus))
    table.manage_snapshots().set_current_snapshot(snapshot_id=first.snapshot_id).commit()
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    _finished_producer(logs, records, epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.rollback",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1,
        idle_stop_s=2,
        warmup_s=0,
    )
    assert score.run(args, StepClock(now_ms()), io.StringIO()) == 2
    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert not summary["run_valid"]
    assert summary["committed_rows"] == table.scan().count() == records[0].rows


@pytest.mark.parametrize("change", ["rollback", "fork", "recreate", "expire", "expire-cleared-parent"])
def test_table_history_changes_abort_scoring(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, change: str
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.changing", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_rows_of(records[0], corpus))
    first = table.current_snapshot()
    assert first is not None
    table.append(_rows_of(records[1], corpus))
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    _finished_producer(logs, records[:2], epoch, done=False)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.changing",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1,
        idle_stop_s=2,
        warmup_s=0,
    )
    clock = StepClock(now_ms())
    original_sleep = clock.sleep

    def change_table(seconds: float) -> None:
        monkeypatch.setattr(clock, "sleep", original_sleep)
        if change == "recreate":
            open_catalog(props).drop_table("bench.changing")
            create.create_table(props, "bench.changing", corpus, create.parse_partition("unpartitioned"), {})
        elif change.startswith("expire"):
            document = snapshots.read_metadata(table)
            retained = [snapshot for snapshot in document.snapshots if snapshot.snapshot_id != first.snapshot_id]
            if change == "expire-cleared-parent":
                retained = [snapshot.model_copy(update={"parent_snapshot_id": None}) for snapshot in retained]
            expired = document.model_copy(update={"snapshots": retained})

            def expired_metadata(_: Table) -> TableMetadata:
                return expired

            monkeypatch.setattr(score, "read_metadata", expired_metadata)
        else:
            table.manage_snapshots().set_current_snapshot(snapshot_id=first.snapshot_id).commit()
            if change == "fork":
                table.append(_rows_of(records[2], corpus))
        publish_log.append_done(logs / "publish_log-0.jsonl", 0, 2)
        clock.t += int(seconds * 1000)

    monkeypatch.setattr(clock, "sleep", change_table)
    with pytest.raises(ValueError, match="table identity|snapshot history"):
        score.run(args, clock, io.StringIO())
    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["aborted"] and not summary["run_valid"]


@pytest.mark.parametrize(
    ("mode", "fault", "valid", "loss", "duplicates", "corrupt"),
    [
        ("EXACTLY_ONCE", "duplicate", False, 0, 1, 0),
        ("AT_LEAST_ONCE", "duplicate", True, 0, 1, 0),
        ("AT_LEAST_ONCE", "masked_loss", False, 1, 2, 0),
        ("AT_LEAST_ONCE", "unexpected_id", False, 0, 0, 1),
    ],
)
def test_configured_exactness_controls_the_final_verdict(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    mode: str,
    fault: str,
    valid: bool,
    loss: int,
    duplicates: int,
    corrupt: int,
) -> None:
    records = metadata.read_manifest(corpus.uri)
    props = _props(tmp_path)
    table = create.create_table(props, "bench.sla", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    for record in records:
        rows = _rows_of(record, corpus)
        if record.batch == 0:
            if fault == "masked_loss":
                rows = pa.concat_tables([rows.slice(1), rows.slice(1, 2)])
            else:
                extra = rows.slice(0, 1)
                if fault == "unexpected_id":
                    extra = extra.set_column(
                        0, extra.schema.field("id"), pa.array([record.id_max + 1], type=pa.int64())
                    )
                rows = pa.concat_tables([rows, extra])
        table.append(rows)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.sla",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        exactness_mode=mode,
    )
    score.run(args, StepClock(now_ms()), io.StringIO())
    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    exactness = json.loads((args.out_dir / score.EXACTNESS_FILE).read_text())
    assert summary["run_valid"] is valid
    assert summary["exactness_mode"] == exactness["mode"] == mode
    assert exactness["verdict"] is valid
    assert exactness["exact"] is False
    assert exactness["loss_rows"] == loss
    assert exactness["duplicate_rows"] == duplicates
    assert exactness["corrupt_batches"] == corrupt
    assert (summary["state"] == "drained") is (fault != "masked_loss")


def test_producer_bound_voids(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run3", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, late_batch=2)
    for r in records:
        table.append(_rows_of(r, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run3",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["producer_bound"] is True and summary["run_valid"] is False and summary["state"] == "producer_bound"
    assert summary["reason"] == (
        "producer_bound: a batch was acknowledged 9000 ms after it was due, over behind_max_ms 5000"
    )


def _drained_run(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    name: str,
    late_batch: int = -1,
    duplicate_batch: int = -1,
    freshness_bound_s: float = 180.0,
) -> Path:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, f"bench.{name}", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, late_batch=late_batch)
    for record in records:
        table.append(_rows_of(record, corpus))
        if record.batch == duplicate_batch:
            table.append(_rows_of(record, corpus))
    out_dir = tmp_path / "out"
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=f"bench.{name}",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=freshness_bound_s,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    return out_dir


def test_gate_judges_a_run_from_the_artifacts(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    assert cli.gate(["--out", str(tmp_path / "nothing")]) == 5  # no summary is no measurement
    out_dir = _drained_run(tmp_path, corpus, "gate1")
    assert cli.gate(["--out", str(out_dir)]) == 0
    assert score.read_keepup_samples(out_dir / score.KEEPUP_SAMPLES_FILE)[0].backlog_rows == 0


def test_gate_voids_a_producer_bound_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    out_dir = _drained_run(tmp_path, corpus, "gate2", late_batch=2)
    assert cli.gate(["--out", str(out_dir)]) == 5


def test_a_breached_bound_names_the_freshness_clause(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    out_dir = _drained_run(tmp_path, corpus, "reason1", freshness_bound_s=1.0)
    summary = json.loads((out_dir / "summary.json").read_text())
    assert summary["run_valid"] is False and summary["state"] == "drained"
    # The offer began ten seconds before the first commit, so the window's p95
    # is measured in seconds and cannot be inside a one-second bound.
    assert re.fullmatch(r"freshness: window p95 \d+\.\d\d s exceeds bound 1\.00 s", summary["reason"])
    # The 4 s corpus plans a 5-sample window, which 2 over-bound samples already breach.
    assert summary["freshness_live"]["window_failing_s"] == 2
    assert summary["freshness_live"]["window_over_bound_s"] >= 2


def test_duplicated_rows_outrank_a_breached_bound(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    out_dir = _drained_run(tmp_path, corpus, "reason2", duplicate_batch=1, freshness_bound_s=1.0)
    summary = json.loads((out_dir / "summary.json").read_text())
    fresh = json.loads((out_dir / "freshness.json").read_text())
    exact = json.loads((out_dir / "exactness.json").read_text())
    assert fresh["verdict"] is False and exact["duplicate_rows"] > 0
    assert summary["reason"] == f"exactness: duplicate_rows={exact['duplicate_rows']}"


def test_the_exactness_reason_lists_only_the_figures_that_are_not_zero() -> None:
    exactness: dict[str, object] = {
        "loss_rows": 12,
        "duplicate_rows": 0,
        "corrupt_batches": 3,
        "invalid_rows": 0,
        "unoffered_rows": 4,
    }
    assert score._exactness_reason(exactness) == "exactness: loss_rows=12, corrupt_batches=3, unoffered_rows=4"


def test_the_freshness_reason_names_the_clause_that_failed() -> None:
    breached = freshness.FreshnessResult(
        window={"p50_s": 3.0, "p95_s": 79.4, "p99_s": 90.0, "max_s": 100.0},
        full={"p50_s": 3.0, "p95_s": 79.4, "p99_s": 90.0, "max_s": 100.0},
        verdict=False,
        drained=True,
        bound_s=60.0,
        max_bound_s=120.0,
        warmup_s=0,
        clock=freshness.TIMESTAMP_CLOCK,
        clock_skew_suspected=False,
        min_lag_s=1.0,
        missing_emit_prefixes=[],
    )
    assert freshness.failure_reason(breached, []) == "freshness: window p95 79.40 s exceeds bound 60.00 s"
    assert freshness.failure_reason(replace(breached, drained=False), []) == "freshness: the table never drained"
    assert freshness.failure_reason(replace(breached, missing_emit_prefixes=[7, 9]), []) == (
        "freshness: missing_emit_prefixes=2, first=7"
    )
    inside_p95 = replace(breached, window={"p50_s": 3.0, "p95_s": 10.0, "p99_s": 100.0, "max_s": 130.0})
    assert freshness.failure_reason(inside_p95, []) == "freshness: window max 130.00 s exceeds max bound 120.00 s"


def _table_of(props: dict[str, str], name: str, fields: list[NestedField]) -> Table:
    """A table holding exactly these columns, which `create_table` will not build."""
    catalog = open_catalog(props)
    with suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("bench", properties={"location": f"{props['warehouse']}/bench"})
    return catalog.create_table(("bench", name), schema=Schema(*fields))


def test_check_table_schema_reads_the_columns_the_table_holds(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    created = create.create_table(props, "bench.shape1", corpus, create.parse_partition("unpartitioned"), {})
    assert snapshots.check_table_schema(created.schema(), corpus) == []

    fields = list(create.iceberg_schema(corpus).fields)
    dropped = _table_of(props, "shape2", [f for f in fields if f.name != "event_type"])
    assert snapshots.check_table_schema(dropped.schema(), corpus) == [
        "the table has no column 'event_type', which the corpus publishes as string"
    ]

    retyped = _table_of(
        props,
        "shape3",
        [f if f.name != "event_time" else NestedField(f.field_id, f.name, LongType(), required=True) for f in fields],
    )
    assert snapshots.check_table_schema(retyped.schema(), corpus) == [
        "column 'event_time' is long in the table and timestamp in the corpus"
    ]

    # Extra engine columns may be nullable.
    widened = _table_of(props, "shape4", [*fields, NestedField(900, "ingest_ms", LongType(), required=False)])
    assert snapshots.check_table_schema(widened.schema(), corpus) == []

    optional = _table_of(
        props,
        "shape5",
        [
            f if f.name != "event_type" else NestedField(f.field_id, f.name, f.field_type, required=False)
            for f in fields
        ],
    )
    assert snapshots.check_table_schema(optional.schema(), corpus) == [
        "column 'event_type' is optional in the table and required in the corpus"
    ]


def test_a_table_missing_a_corpus_column_voids_the_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    fields = [f for f in create.iceberg_schema(corpus).fields if f.name != "event_type"]
    table = _table_of(props, "run7", fields)
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    # All rows are present; only the schema mismatch should invalidate this run.
    for record in records:
        table.append(_rows_of(record, corpus).drop_columns(["event_type"]))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run7",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "void" and summary["run_valid"] is False
    assert summary["reason"] == (
        "table schema mismatch: the table has no column 'event_type', which the corpus publishes as string"
    )
    # Nothing was tallied: the rows are there, and they describe another table.
    assert summary["committed_rows"] == 0
    assert cli.gate(["--out", str(tmp_path / "out")]) == 5


def test_an_optional_corpus_column_voids_the_run(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    fields = [
        f if f.name != "event_type" else NestedField(f.field_id, f.name, f.field_type, required=False)
        for f in create.iceberg_schema(corpus).fields
    ]
    table = _table_of(props, "run8", fields)
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    # All values are present; only declared nullability should invalidate this run.
    for record in records:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run8",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "void" and summary["run_valid"] is False
    assert summary["reason"] == (
        "table schema mismatch: column 'event_type' is optional in the table and required in the corpus"
    )
    assert summary["committed_rows"] == 0


def test_score_cli_maps_its_flags(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[score.ScoreArgs] = []
    assert cli.build_score_parser().get_default("exactness") == "EXACTLY_ONCE"

    def capture(args: score.ScoreArgs, clock: Clock, log: TextIO) -> int:
        seen.append(args)
        return 0

    # Patch the module attribute used by the CLI.
    monkeypatch.setattr(score, "run", capture)
    assert (
        cli.score(
            [
                "--corpus",
                "s3://bench/corpus/events",
                "--table",
                "bench.events",
                "--catalog-prop",
                "type=sql",
                "--catalog-prop",
                "uri=sqlite:///cat.db",
                "--publish-logs",
                "s3://bench/runs/events/producer",
                "--epoch",
                "1700000000.25",
                "--out",
                str(tmp_path / "scores"),
                "--publish-shards",
                "4",
                "--warmup-s",
                "60",
                "--exactness",
                "AT_LEAST_ONCE",
                "--upload-prefix",
                "s3://bench/runs/events/scores",
                "--read-workers",
                "8",
            ]
        )
        == 0
    )
    args = seen[0]
    # --epoch is unix seconds on every command line of the harness, and every
    # figure inside one is milliseconds.
    assert args.epoch_ms == 1_700_000_000_250
    assert args.catalog_props == {"type": "sql", "uri": "sqlite:///cat.db"}
    assert args.expected_publish_shards == 4 and args.warmup_s == 60
    assert args.out_dir == tmp_path / "scores" and args.freshness_bound_s == 180.0
    assert args.exactness_mode == "AT_LEAST_ONCE"
    assert args.upload_prefix == "s3://bench/runs/events/scores"
    assert args.read_workers == 8


def test_the_score_cli_refuses_a_reader_count_below_one(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:

    def unreachable(args: score.ScoreArgs, clock: Clock, log: TextIO) -> int:
        raise AssertionError("the loop must not start on a width it cannot read with")

    monkeypatch.setattr(score, "run", unreachable)
    for width in ("0", "-4"):
        with pytest.raises(SystemExit):
            cli.score(
                [
                    "--corpus",
                    "s3://bench/corpus/events",
                    "--table",
                    "bench.events",
                    "--publish-logs",
                    "s3://bench/runs/events/producer",
                    "--epoch",
                    "1700000000",
                    "--out",
                    str(tmp_path / "scores"),
                    "--read-workers",
                    width,
                ]
            )
        assert "--read-workers" in capsys.readouterr().err


@pytest.mark.parametrize(
    ("option", "value"),
    [("--publish-shards", "0"), ("--publish-shards", "-1"), ("--gate-window-s", "0"), ("--gate-window-s", "-1")],
)
def test_the_score_cli_refuses_nonpositive_producer_window_dimensions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    option: str,
    value: str,
) -> None:

    def unreachable(args: score.ScoreArgs, clock: Clock, log: TextIO) -> int:
        raise AssertionError("the loop must not start with an invalid producer window dimension")

    monkeypatch.setattr(score, "run", unreachable)
    with pytest.raises(SystemExit):
        cli.score(
            [
                "--corpus",
                "s3://bench/corpus/events",
                "--table",
                "bench.events",
                "--publish-logs",
                "s3://bench/runs/events/producer",
                "--epoch",
                "1700000000",
                "--out",
                str(tmp_path / "scores"),
                option,
                value,
            ]
        )
    assert option in capsys.readouterr().err


def test_shortened_replay_is_scored_on_what_was_offered(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run4", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    # The producer was given --seconds, so only the first two of the manifest's
    # four batches were ever sent.
    _finished_producer(logs, records[:2], epoch)
    for record in records[:2]:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run4",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is True and summary["state"] == "drained"
    assert summary["prefix"] == 1 and summary["last_batch"] == 1
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["expected_rows"] == sum(r.rows for r in records[:2]) and exact["expected_rows"] < corpus.row_count
    assert exact["exact"] and exact["loss_rows"] == 0 and exact["scored_batches"] == 2


def test_a_snapshot_arriving_between_polls_is_tallied_once(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run5", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    table.append(_rows_of(records[0], corpus))
    clock = StepClock(now_ms())
    log = io.StringIO()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run5",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=5.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    assert len(state.seen) == 1 and state.tally.prefix() == 0
    samples = score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)
    assert len(samples) == 1 and samples[0].committed_rate is None

    table.append(_rows_of(records[1], corpus))
    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is True
    assert len(state.seen) == 2 and state.tally.prefix() == 1
    assert state.tally.committed_rows() == records[0].rows + records[1].rows
    samples = score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)
    assert len(samples) == 2
    # The whole offer was already published, so only the committed side moved.
    assert samples[1].offered_rate == 0.0 and samples[1].committed_rate == records[1].rows / 5.0

    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is True
    assert len(state.seen) == 2 and state.tally.committed_rows() == records[0].rows + records[1].rows
    assert len(score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE)) == 3
    assert len(state.observations) == 2 and [obs.prefix for obs in state.observations] == [0, 1]


def _many_file_commit(tmp_path: Path, corpus: metadata.CorpusMetadata, name: str, files: int) -> Table:
    """A table whose one commit added ``files`` data files, all of one batch."""
    props = _props(tmp_path)
    table = create.create_table(props, f"bench.{name}", corpus, create.parse_partition("unpartitioned"), {})
    rows = _rows_of(metadata.read_manifest(corpus.uri)[0], corpus)
    per_file = rows.num_rows // files
    directory = tmp_path / f"{name}-files"
    directory.mkdir()
    written: list[str] = []
    for index in range(files):
        start = index * per_file
        stop = rows.num_rows if index == files - 1 else start + per_file
        path = directory / f"part-{index:05d}.parquet"
        pq.write_table(rows.slice(start, stop - start), path)
        written.append(f"file://{path}")
    table.add_files(written)
    return table


def _many_file_args(
    tmp_path: Path, corpus: metadata.CorpusMetadata, name: str, read_workers: int, poll_interval_s: float = 5.0
) -> score.ScoreArgs:
    logs = tmp_path / f"{name}-logs"
    logs.mkdir()
    _finished_producer(logs, metadata.read_manifest(corpus.uri), now_ms() - 10_000)
    return score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=f"bench.{name}",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms() - 10_000,
        out_dir=tmp_path / f"{name}-out",
        poll_interval_s=poll_interval_s,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        read_workers=read_workers,
    )


@pytest.mark.parametrize("processes", [1, 2])
def test_a_commit_of_many_files_is_tallied_once_at_any_worker_count(
    tmp_path: Path, corpus: metadata.CorpusMetadata, processes: int
) -> None:
    records = metadata.read_manifest(corpus.uri)
    file_count = 64
    figures: list[tuple[int, int, int, int]] = []
    for workers in (1, 16):
        name = f"fanout{workers}"
        _many_file_commit(tmp_path, corpus, name, files=file_count)
        args = _many_file_args(tmp_path, corpus, name, read_workers=workers)
        args = replace(args, read_processes=processes)
        clock = StepClock(now_ms())
        log = io.StringIO()
        state = score._load_inputs(args, clock, log)
        try:
            assert score._poll_once(state, clock, log) is True
        finally:
            score._close_read_pool(state)
        assert isinstance(state.tally, tally.BatchTally)
        assert state.tally.complete(0), "a file read twice, or not at all"
        # Every file in the commit must be read at either worker count.
        assert f"files={file_count}" in log.getvalue().rsplit("POLL t=", 1)[1], log.getvalue()
        line = json.loads((args.out_dir / score.SNAPSHOTS_FILE).read_text().splitlines()[-1])
        figures.append(
            (state.tally.prefix(), state.tally.committed_rows(), int(line["added_files"]), len(state.observations))
        )
    assert figures[0] == figures[1] == (0, records[0].rows, file_count, 1)


@pytest.mark.parametrize("workers, processes", [(6, 1), (6, 3), (2, 8)])
def test_added_file_reads_retain_at_most_the_worker_width(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, workers: int, processes: int
) -> None:
    chunk = max(1, workers // processes)
    # Enough files that a chunk sized off the commit rather than off the worker
    # width would retain several times the bound.
    files = [snapshots.AddedFile(f"file-{index}", "parquet", 1, 8) for index in range(64)]
    args = _many_file_args(tmp_path, corpus, "bounded", read_workers=workers)
    args = replace(args, read_processes=processes)
    state = score._load_inputs(args, StepClock(now_ms()), io.StringIO())
    # Count rows, not arrays, so a task that joins its chunk is charged for the join.
    live_rows = 0
    max_live_rows = 0

    def track(ids: np.ndarray) -> np.ndarray:
        nonlocal live_rows, max_live_rows
        rows = int(ids.size)
        live_rows += rows
        max_live_rows = max(max_live_rows, live_rows)

        def released() -> None:
            nonlocal live_rows
            live_rows -= rows

        finalize(ids, released)
        return ids

    def read(path: str, file_format: str) -> np.ndarray:
        return track(np.array([0], dtype=np.int64))

    widths: set[int] = set()

    def read_chunk(targets: list[tuple[str, str]], threads: int) -> np.ndarray:
        widths.add(threads)
        return track(np.concatenate([read(path, file_format) for path, file_format in targets]))

    class ImmediateExecutor:
        """Complete reads inline so retained results are measured deterministically."""

        def __init__(self, max_workers: int, mp_context: object = None) -> None:
            assert max_workers == (workers if processes == 1 else min(workers, processes))

        def submit(self, fn: Callable[..., np.ndarray], *args: object) -> Future[np.ndarray]:
            future: Future[np.ndarray] = Future()
            future.set_result(fn(*args))
            return future

        def shutdown(self, wait: bool, cancel_futures: bool) -> None:
            assert wait and cancel_futures

    monkeypatch.setattr(score, "ThreadPoolExecutor", ImmediateExecutor)
    monkeypatch.setattr(score, "ProcessPoolExecutor", ImmediateExecutor)
    monkeypatch.setattr(score, "read_id_column", read)
    monkeypatch.setattr(score, "read_id_columns", read_chunk)

    assert score._apply_added_files(state, files) == len(files)
    # Applied results stay within the worker width; a worker reading and joining a
    # chunk adds that chunk twice on top while its task runs.
    assert max_live_rows <= workers + (0 if processes == 1 else 2 * chunk)
    assert live_rows == 0
    # Each worker reads its whole chunk at once, so its thread width is the chunk.
    assert widths == (set() if processes == 1 else {chunk})
    assert state.applied_files == {file.path for file in files}


def _batch_zero_args(tmp_path: Path, corpus: metadata.CorpusMetadata, name: str) -> score.ScoreArgs:
    """Arguments for a run whose finished offer holds batch 0 alone."""
    logs = tmp_path / f"{name}-logs"
    logs.mkdir()
    epoch = now_ms() - 10_000
    _finished_producer(logs, metadata.read_manifest(corpus.uri)[:1], epoch)
    return score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=f"bench.{name}",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / f"{name}-out",
        poll_interval_s=5.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        read_workers=1,
    )


def test_a_poll_that_fails_partway_through_the_last_commit_does_not_drain(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The files read before the failure cover the offer; the unread one holds duplicates."""
    records = metadata.read_manifest(corpus.uri)
    args = _batch_zero_args(tmp_path, corpus, "torn_last")
    table = create.create_table(args.catalog_props, args.table, corpus, create.parse_partition("unpartitioned"), {})
    rows = _rows_of(records[0], corpus)
    directory = tmp_path / "torn_last-files"
    directory.mkdir()
    parts = [("part-0", rows), ("part-1-duplicates", rows.slice(0, 3))]
    for name, part in parts:
        pq.write_table(part, directory / f"{name}.parquet")
    table.add_files([f"file://{directory / name}.parquet" for name, _ in parts])
    failed: list[str] = []
    read = tally.read_id_column

    def flaky(path: str, file_format: str) -> np.ndarray:
        if "duplicates" in path and not failed:
            failed.append(path)
            raise OSError("the object store dropped the connection")
        return read(path, file_format)

    monkeypatch.setattr(score, "read_id_column", flaky)
    log = io.StringIO()

    assert score.run(args, StepClock(now_ms()), log) == 0

    assert failed and "POLL_FAILED" in log.getvalue()
    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["state"] == score.DRAINED and summary["snapshots"] == 1
    assert summary["exactness"]["duplicate_rows"] == 3
    assert summary["reason"] == "exactness: duplicate_rows=3"


def test_rows_of_a_batch_that_was_never_offered_drain_and_fail_exactness(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    records = metadata.read_manifest(corpus.uri)
    args = _batch_zero_args(tmp_path, corpus, "stray")
    table = create.create_table(args.catalog_props, args.table, corpus, create.parse_partition("unpartitioned"), {})
    table.append(_rows_of(records[0], corpus))
    table.append(_rows_of(records[1], corpus))

    assert score.run(args, StepClock(now_ms()), io.StringIO()) == 0

    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["state"] == score.DRAINED and summary["prefix"] > summary["last_batch"]
    assert summary["exactness"]["unoffered_rows"] == records[1].rows
    assert summary["run_valid"] is False
    assert summary["reason"] == f"exactness: unoffered_rows={records[1].rows}"


def test_commits_that_add_no_rows_do_not_hold_off_an_idle_stop(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Empty appends and compactor rewrites keep committing while ingest has stalled."""
    args = _batch_zero_args(tmp_path, corpus, "empty_commits")
    polls: list[int] = []

    def empty_commit(state: score.ScoreState, clock: Clock, log: TextIO) -> int:
        polls.append(clock.now_ms())
        if len(polls) > 100:
            raise AssertionError("the scorer never idle-stopped")
        state.seen.add(len(polls))
        return 0

    monkeypatch.setattr(score, "_read_inputs", empty_commit)
    clock = StepClock(now_ms())

    assert score.run(args, clock, io.StringIO()) == 2

    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["state"] == score.IDLE_STOP and summary["snapshots"] == len(polls)
    assert (polls[-1] - polls[0]) / 1000 <= args.idle_stop_s


def test_a_failed_live_upload_is_retried_on_the_next_poll(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    args = replace(_batch_zero_args(tmp_path, corpus, "mirror"), upload_prefix=f"file://{tmp_path / 'mirror'}")
    create.create_table(args.catalog_props, args.table, corpus, create.parse_partition("unpartitioned"), {})

    def unreliable(uri_: str, data: bytes) -> None:
        raise OSError("the upload was refused")

    monkeypatch.setattr(uri, "write_bytes", unreliable)
    clock = StepClock(now_ms())
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)

    for attempt in range(1, score.MAX_CONSECUTIVE_IO_FAILURES):
        assert score._poll_once(state, clock, log) is True
        assert state.mirror_failures == attempt
    assert log.getvalue().count("MIRROR_FAILED") == score.MAX_CONSECUTIVE_IO_FAILURES - 1
    with pytest.raises(OSError, match="refused"):
        score._poll_once(state, clock, log)

    monkeypatch.undo()
    state.mirror_failures = 1
    assert score._poll_once(state, clock, log) is True
    assert state.mirror_failures == 0


def test_backlog_percentiles_leave_out_samples_from_before_the_epoch(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    args = _batch_zero_args(tmp_path, corpus, "lead")
    state = score._load_inputs(args, StepClock(args.epoch_ms), io.StringIO())
    state.samples = [
        keepup.KeepupSample(args.epoch_ms + offset_ms, 0, 0, backlog, None, None)
        for offset_ms, backlog in ((-10_000, 0), (-5_000, 0), (0, 100), (5_000, 300), (10_000, 500))
    ]

    figures = score._keepup(state)

    assert figures["backlog_rows_p50"] == 300.0 and figures["backlog_rows_max"] == 500


def test_a_read_that_fails_inside_the_pool_fails_the_poll_once(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A partial poll must not advance snapshot state; retry the whole commit."""
    _many_file_commit(tmp_path, corpus, "torn", files=64)
    args = _many_file_args(tmp_path, corpus, "torn", read_workers=16)

    def unreadable(path: str, file_format: str) -> np.ndarray:
        raise RuntimeError("the object store stopped answering")

    monkeypatch.setattr(score, "read_id_column", unreadable)
    clock = StepClock(now_ms())
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is False
    printed = log.getvalue()
    assert printed.count("POLL_FAILED") == 1, printed
    assert "POLL t=" not in printed, printed
    assert state.read_failures == 1 and state.tally.committed_rows() == 0
    # The commit stays unseen, so the retry reads it whole.
    assert state.seen == set()
    assert score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE) == []


def test_process_reads_retry_without_recounting_applied_files(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    args = replace(_many_file_args(tmp_path, corpus, "retry", read_workers=2), read_processes=2)
    state = score._load_inputs(args, StepClock(now_ms()), io.StringIO())
    paths = [tmp_path / f"retry-{index}.parquet" for index in range(3)]
    files = [snapshots.AddedFile(str(path), "parquet", 1, 8) for path in paths]
    for index, path in enumerate(paths[:2]):
        pq.write_table(pa.table({"id": pa.array([index], type=pa.int64())}), path)
    try:
        with pytest.raises(FileNotFoundError):
            score._apply_added_files(state, files)
        applied = len(state.applied_files)
        assert 1 <= applied <= 2
        assert state.tally.committed_rows() == applied
        # A read error does not break the pool, so the retry reuses its workers.
        assert state.read_pool is not None
        pq.write_table(pa.table({"id": pa.array([2], type=pa.int64())}), paths[2])
        assert score._apply_added_files(state, files) == len(files) - applied
        assert state.tally.committed_rows() == 3
        assert state.applied_files == {file.path for file in files}
        assert score._apply_added_files(state, files) == 0
        assert state.tally.committed_rows() == 3
    finally:
        score._close_read_pool(state)


def test_the_read_pool_is_released_when_the_loop_ends(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Worker interpreters must not outlive the loop that writes the artifacts."""
    _many_file_commit(tmp_path, corpus, "released", files=4)
    args = replace(_many_file_args(tmp_path, corpus, "released", read_workers=2), read_processes=2)
    held: list[bool] = []
    close = score._close_read_pool

    def record(state: score.ScoreState) -> None:
        held.append(state.read_pool is not None)
        close(state)

    monkeypatch.setattr(score, "_close_read_pool", record)

    assert score.run(args, StepClock(now_ms()), io.StringIO()) == 2
    assert held == [True], "the loop ended without a pool to release, so it read nothing in processes"


def test_a_read_phase_past_the_poll_interval_says_so(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = _many_file_commit(tmp_path, corpus, "slow", files=64)
    args = _many_file_args(tmp_path, corpus, "slow", read_workers=1, poll_interval_s=5.0)
    clock = StepClock(now_ms())

    def slowly(document: TableMetadata, snapshot_id: int, io_for_table: FileIO) -> list[snapshots.AddedFile]:
        added = snapshots.added_files(document, snapshot_id, io_for_table)
        # Charge fake time outside the reader pool because StepClock is not thread-safe.
        clock.sleep(0.1 * len(added))
        return added

    monkeypatch.setattr(score, "added_files", slowly)
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    printed = log.getvalue()
    assert "SLOW_POLL poll_ms=6400 files=64 interval_ms=5000" in printed, printed
    assert "poll_ms=6400 files=64" in printed.rsplit("POLL t=", 1)[1], printed

    # The same loop, reading a commit of one file at the speed of the disk.
    monkeypatch.setattr(score, "added_files", snapshots.added_files)
    table.append(_rows_of(metadata.read_manifest(corpus.uri)[1], corpus))
    clock.sleep(5.0)
    assert score._poll_once(state, clock, log) is True
    printed = log.getvalue()
    assert printed.count("SLOW_POLL") == 1, printed
    assert "poll_ms=0 files=1" in printed.rsplit("POLL t=", 1)[1], printed


def test_a_shard_finishing_mid_poll_is_not_scored_over_a_partial_offer(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.run6", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    log_path = logs / "publish_log-0.jsonl"
    _finished_producer(logs, records[:3], epoch, done=False)
    for record in records[:3]:
        table.append(_rows_of(record, corpus))
    real_read_text = uri.read_text

    def finishing(path: str) -> str:
        text = real_read_text(path)
        # Completion lands after this poll fetched an unfinished version. Its
        # records must not be mistaken for the complete offer.
        if path == str(log_path) and not publish_log.shard_done(log_path):
            _finished_producer(logs, records[3:], epoch, done=False)
            publish_log.append_done(log_path, 0, len(records))
        return text

    monkeypatch.setattr(uri, "read_text", finishing)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.run6",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    # The next poll must include the final batch before it can observe completion.
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["run_valid"] is False and summary["last_batch"] == 3 and summary["prefix"] == 2
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["scored_batches"] == 4 and exact["loss_rows"] == records[3].rows


def test_the_upload_prefix_mirrors_the_artifacts_the_gate_reads(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.mirror", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch)
    for record in records:
        table.append(_rows_of(record, corpus))
    out_dir = tmp_path / "out"
    mirror = tmp_path / "mirror"
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.mirror",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        upload_prefix=f"file://{mirror}",
    )
    clock = StepClock(now_ms())
    log = io.StringIO()

    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log) is True
    assert {path.name for path in mirror.iterdir()} == {score.SUMMARY_FILE, score.KEEPUP_SAMPLES_FILE}

    assert score.run(args, StepClock(now_ms()), log) == 0
    assert {path.name for path in mirror.iterdir()} == {path.name for path in out_dir.iterdir()}
    assert json.loads((mirror / score.SUMMARY_FILE).read_text())["run_valid"] is True


def test_a_failed_scorer_still_publishes_what_it_had(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir()
    out_dir = tmp_path / "out"
    mirror = tmp_path / "mirror"

    def unreachable(catalog_props: dict[str, str], table: str) -> Table:
        raise RuntimeError("the catalog stopped answering")

    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.gone",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=out_dir,
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        upload_prefix=f"file://{mirror}",
    )
    with pytest.MonkeyPatch.context() as patch:
        patch.setattr(score, "load_table", unreachable)
        with pytest.raises(RuntimeError, match="stopped answering"):
            score.run(args, StepClock(now_ms()), io.StringIO())

    assert {path.name for path in mirror.iterdir()} == {path.name for path in out_dir.iterdir()}
    summary = json.loads((mirror / score.SUMMARY_FILE).read_text())
    assert summary["aborted"] is True and summary["reason"] == "scorer_failed: RuntimeError"
    assert summary["state"] == score.FAILED


def test_a_table_its_engine_has_not_created_yet_is_an_empty_baseline(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """The scorer starts before production. Treat an engine-owned missing table as
    empty so the producer can start and trigger its creation.
    """
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.never_created",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        table_managed_by="engine",
    )
    log = tmp_path / "score.log"
    # Publish empty baseline readings until the idle timeout.
    assert score.run(args, StepClock(now_ms()), open(log, "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["committed_rows"] == 0 and summary["snapshots"] == 0 and summary["prefix"] == -1
    assert summary["aborted"] is True and summary["run_valid"] is False
    printed = log.read_text()
    assert "POLL t=" in printed, printed
    assert "POLL_FAILED" not in printed, printed
    assert "TABLE_ABSENT table=bench.never_created" in printed, printed
    # Announced once and not per poll: it is one state, not one event a second.
    assert printed.count("TABLE_ABSENT") == 1, printed
    samples = score.read_keepup_samples(tmp_path / "out" / score.KEEPUP_SAMPLES_FILE)
    assert samples and all(sample.committed_rows == 0 for sample in samples)


def test_a_harness_managed_table_that_is_absent_is_still_a_failure(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """Staging already created this table. Retry transient catalog faults, then fail."""
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.never_created",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    log = tmp_path / "score.log"
    with pytest.raises(Exception, match="never_created"):
        score.run(args, StepClock(now_ms()), open(log, "w"))
    printed = log.read_text()
    assert f"POLL_FAILED consecutive={score.MAX_CONSECUTIVE_IO_FAILURES}" in printed, printed


def test_the_schema_is_checked_on_the_first_load_that_succeeds(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    logs = tmp_path / "logs"
    logs.mkdir()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.late_shape",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=now_ms(),
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
        table_managed_by="engine",
    )
    state = score._load_inputs(args, StepClock(now_ms()), io.StringIO())
    clock = StepClock(now_ms())
    assert score._poll_once(state, clock, io.StringIO()) is True
    assert state.schema_mismatches is None, "there was no table to read a shape off"

    # Now the engine creates one, with a column of its own and one of the
    # corpus's missing.
    _table_of(props, "late_shape", [NestedField(1, "id", LongType(), required=True)])
    assert score._poll_once(state, clock, io.StringIO()) is True
    assert state.schema_mismatches, "the first load that succeeded did not check the shape"


def test_a_run_the_loop_abandoned_is_not_publishable(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.no_trailer", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 10_000
    logs = tmp_path / "logs"
    logs.mkdir()
    _finished_producer(logs, records, epoch, done=False)
    for record in records:
        table.append(_rows_of(record, corpus))
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.no_trailer",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=5.0,
        warmup_s=0,
        freshness_bound_s=180.0,
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 2
    summary = json.loads((tmp_path / "out" / "summary.json").read_text())
    assert summary["state"] == "idle_stop" and summary["aborted"] is True
    # Clean row metrics do not make an abandoned run valid.
    exact = json.loads((tmp_path / "out" / "exactness.json").read_text())
    assert exact["exact"] is True
    assert summary["run_valid"] is False


def test_snapshots_share_metadata_observation_time_despite_slow_scans_and_writer_skew(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), "bench.clock", corpus, create.parse_partition("unpartitioned"), {})
    for record in records:
        table.append(_rows_of(record, corpus))
    args = _many_file_args(tmp_path, corpus, "clock", read_workers=1)
    clock = StepClock(now_ms())
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)

    def slow_read(path: str, file_format: str) -> np.ndarray:
        clock.sleep(2)
        return tally.read_id_column(path, file_format)

    def skewed_snapshots(document: TableMetadata) -> list[snapshots.SnapshotInfo]:
        return [
            replace(info, timestamp_ms=info.timestamp_ms - 60_000) for info in snapshots.snapshots_in_order(document)
        ]

    monkeypatch.setattr(score, "read_id_column", slow_read)
    monkeypatch.setattr(score, "snapshots_in_order", skewed_snapshots)
    observed_ms = clock.now_ms()
    assert score._poll_once(state, clock, log)
    assert len(state.observations) == len(records)
    assert {obs.first_seen_ms for obs in state.observations} == {observed_ms}
    saved = [json.loads(line) for line in (args.out_dir / score.SNAPSHOTS_FILE).read_text().splitlines()]
    assert {row["first_seen_ms"] for row in saved} == {observed_ms}
    offer_end_ms = max(record.last_ack_ms for record in state.records)
    assert score._keepup(state)["drain_s"] == (observed_ms - offer_end_ms) / 1000
    assert "SLOW_POLL" in log.getvalue()


def test_late_final_publish_logs_update_totals_without_rewriting_samples(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    records = metadata.read_manifest(corpus.uri)
    _many_file_commit(tmp_path, corpus, "late_logs", files=1)
    args = _many_file_args(tmp_path, corpus, "late_logs", read_workers=1)
    logs = Path(args.publish_logs_uri)
    (logs / "publish_log-0.jsonl").unlink()
    _finished_producer(logs, records[:1], args.epoch_ms, done=False)
    clock = StepClock(args.epoch_ms + 1000)
    log = io.StringIO()
    state = score._load_inputs(args, clock, log)
    assert score._poll_once(state, clock, log)
    historical = state.samples[-1]
    assert historical.offered_rows == historical.committed_rows == records[0].rows
    assert score._offer_end_ms(state) is None

    _finished_producer(logs, records[1:], args.epoch_ms)
    clock.sleep(5)
    assert score._poll_once(state, clock, log)
    assert state.offer_ended
    assert state.offered_rows() == corpus.row_count
    assert state.samples[0] == historical


def test_producer_windows_survive_late_uploads(
    tmp_path: Path, corpus: metadata.CorpusMetadata, capsys: pytest.CaptureFixture[str]
) -> None:
    records = metadata.read_manifest(corpus.uri)
    _many_file_commit(tmp_path, corpus, "windowed", files=1)
    args = replace(_many_file_args(tmp_path, corpus, "windowed", read_workers=1), gate_window_s=2)
    path = Path(args.publish_logs_uri) / "publish_log-0.jsonl"
    path.unlink()
    _finished_producer(path.parent, records[:2], args.epoch_ms, done=False)
    clock = StepClock(args.epoch_ms + 30_000)
    state = score._load_inputs(args, clock, io.StringIO())
    score._poll_once(state, clock, io.StringIO())
    first = state.samples[-1].producer_windows
    assert len(first) == 1 and first[0].batches == 2
    assert first[0].behind_ms_median == first[0].behind_ms_max == 50

    _finished_producer(path.parent, records[2:], args.epoch_ms, late_batch=3)
    clock.sleep(30)
    score._poll_once(state, clock, io.StringIO())
    assert state.producer_bound()
    assert len(state.samples[-1].producer_windows) == 1
    assert state.samples[-1].producer_windows[0].start_ms == args.epoch_ms + 2000
    assert state.samples[-1].producer_windows[0].behind_ms_max == 9000

    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["producer"]["errors"] == 0
    assert summary["producer"]["behind_ms"] == 9000
    assert score.read_keepup_samples(args.out_dir / score.KEEPUP_SAMPLES_FILE) == state.samples
    assert cli.gate(["--out", str(args.out_dir)]) == 5
    assert "producers closed_window n=1 behind median=4.5s max=9.0s" in capsys.readouterr().out


def test_summary_reports_the_newest_closed_producer_window(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    epoch_ms = now_ms()
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.window_summary",
        catalog_props=_props(tmp_path),
        publish_logs_uri=str(tmp_path / "logs"),
        epoch_ms=epoch_ms,
        out_dir=tmp_path / "out",
        expected_publish_shards=2,
        gate_window_s=30,
    )
    state = score._load_inputs(args, StepClock(epoch_ms), io.StringIO())
    state.producer_windows = publish_log.windows(
        [
            publish_log.PublishRecord(0, epoch_ms, epoch_ms + 9000, epoch_ms + 9010, 1, 1, 0),
            publish_log.PublishRecord(1, epoch_ms, epoch_ms + 30, epoch_ms + 40, 1, 1, 0),
            publish_log.PublishRecord(2, epoch_ms + 30_000, epoch_ms + 30_100, epoch_ms + 30_200, 1, 1, 0),
            publish_log.PublishRecord(3, epoch_ms + 30_000, epoch_ms + 30_300, epoch_ms + 30_400, 1, 1, 0),
            publish_log.PublishRecord(4, epoch_ms + 60_000, epoch_ms + 60_500, epoch_ms + 60_600, 1, 1, 0),
        ],
        epoch_ms=epoch_ms,
        window_s=30,
        shards=2,
    )
    # Shard 1's next batch is scheduled at +60 s and arrives 29 s late. At
    # +89 s it is still absent, so the open window has only shard 0's record.
    state.samples.append(keepup.make_sample(epoch_ms + 89_000, 0, 0, None))

    assert state.producer_bound()
    assert score._latest_producer_window(state) == {
        "start_ms": epoch_ms + 30_000,
        "end_ms": epoch_ms + 60_000,
        "shards": 2,
        "behind_ms_median": 200.0,
        "behind_ms_max": 300,
    }
    score._write_summary(state)
    summary = json.loads((args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["producer_bound"] is True
    assert summary["producer"]["behind_ms"] == 9000
    assert summary["producer"]["window"]["behind_ms_max"] == 300


@pytest.mark.parametrize(
    "mode, duplicates, valid",
    [
        ("EXACTLY_ONCE", False, True),
        ("EXACTLY_ONCE", True, False),
        ("AT_LEAST_ONCE", True, True),
    ],
)
def test_saturation_drains_checks_exactness_and_keeps_freshness_diagnostic(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
    duplicates: bool,
    valid: bool,
) -> None:
    props = _props(tmp_path)
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(props, "bench.saturation", corpus, create.parse_partition("unpartitioned"), {})
    epoch = now_ms() - 20_000
    logs = tmp_path / "logs"
    _finished_producer(logs, records, epoch)
    first = _rows_of(records[0], corpus)
    step = first.num_rows // 3
    for offset, length in ((0, step), (step, step), (step * 2, first.num_rows - step * 2)):
        table.append(first.slice(offset, length))
    for record in records[1:]:
        table.append(_rows_of(record, corpus))
    if duplicates:
        table.append(first)
    original = snapshots.snapshots_in_order

    def timed(document: TableMetadata) -> list[snapshots.SnapshotInfo]:
        return [
            replace(s, timestamp_ms=epoch + t)
            for s, t in zip(original(document), (1500, 2000, 2500, 5000, 5100, 5200, 5300), strict=False)
        ]

    monkeypatch.setattr(score, "snapshots_in_order", timed)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.saturation",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        warmup_s=1,
        freshness_bound_s=0.01,
        exactness_mode=mode,
        scoring_mode="saturation",
        measurement_s=2,
    )
    assert score.run(args, StepClock(epoch + 10_000), io.StringIO()) == 0
    summary = json.loads((args.out_dir / "summary.json").read_text())
    assert summary["run_valid"] is valid
    assert summary["freshness"]["verdict"] is False
    assert summary["saturation"]["rows_per_s"] == first.num_rows / 2
    assert summary["saturation"]["append_commits"] == 3
    assert summary["scoring_mode"] == "saturation"
    assert summary["exactness"]["verdict"] is valid
    raw = [json.loads(line) for line in (args.out_dir / "snapshots.jsonl").read_text().splitlines()]
    assert raw[2]["committed_rows"] == first.num_rows
    if not valid:
        assert summary["reason"].startswith("exactness:")


def test_saturation_gate_ignores_overload_but_requires_live_measurements() -> None:
    from ingest_bench.scorer.gate import gate_verdict

    samples = [keepup.KeepupSample(t, t, 0, t, None, None) for t in (1000, 2000, 3000)]

    def verdict(aborted: bool = False, now: int = 3000, missing: bool = False) -> str:
        return gate_verdict(
            {"aborted": aborted, "lag_s": 999},
            [] if missing else samples,
            bound_s=1,
            adaptation_s=0,
            window_s=1,
            now_ms=now,
            epoch_ms=0,
            stale_after_s=1,
            scoring_mode="saturation",
        )[0]

    assert verdict() == "PASS"
    assert verdict(aborted=True) == "VOID"
    assert verdict(missing=True) == "VOID"
    assert verdict(now=5000) == "VOID"


def test_gate_cli_reads_mode_and_still_rejects_producer_failures(tmp_path: Path) -> None:
    epoch = now_ms() - 1000
    summary = {
        "state": "running",
        "reason": None,
        "aborted": False,
        "lag_s": 999,
        "producer": {"behind_ms": 0, "errors": 0},
        "producer_bound": False,
        "scoring_mode": "saturation",
        "freshness_bound_s": 1,
        "epoch_ms": epoch,
    }
    (tmp_path / score.SUMMARY_FILE).write_text(json.dumps(summary))
    sample = {
        "at_ms": now_ms(),
        "offered_rows": 100,
        "committed_rows": 0,
        "backlog_rows": 100,
        "offered_rate": None,
        "committed_rate": None,
    }
    (tmp_path / score.KEEPUP_SAMPLES_FILE).write_text(json.dumps(sample) + "\n")
    assert cli.gate(["--out", str(tmp_path), "--adaptation-s", "0"]) == 0
    summary["producer_bound"] = True
    (tmp_path / score.SUMMARY_FILE).write_text(json.dumps(summary))
    assert cli.gate(["--out", str(tmp_path)]) == 5


def test_saturation_cli_refuses_a_missing_window_before_reading_inputs(tmp_path: Path) -> None:
    with pytest.raises(SystemExit) as error:
        cli.score(
            [
                "--mode",
                "saturation",
                "--corpus",
                "absent",
                "--table",
                "bench.absent",
                "--publish-logs",
                "absent",
                "--epoch",
                "0",
                "--out",
                str(tmp_path),
            ]
        )
    assert error.value.code == 2


def test_append_transaction_uses_visible_commit_time(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    import time

    props = _props(tmp_path)
    table = create.create_table(props, "bench.transaction", corpus, create.parse_partition("unpartitioned"), {})
    records = metadata.read_manifest(corpus.uri)
    with table.transaction() as transaction:
        transaction.append(_rows_of(records[0], corpus))
        time.sleep(0.02)
        transaction.append(_rows_of(records[1], corpus))
    visible = table.current_snapshot()
    assert visible is not None
    logs = tmp_path / "logs"
    _finished_producer(logs, records[:2], visible.timestamp_ms - 10_000)
    args = score.ScoreArgs(
        corpus.uri, "bench.transaction", props, str(logs), visible.timestamp_ms - 10_000, tmp_path / "scores"
    )
    state = score._load_inputs(args, StepClock(visible.timestamp_ms + 100), io.StringIO())
    score._read_inputs(state, StepClock(visible.timestamp_ms + 100), io.StringIO())
    assert [observation.timestamp_ms for observation in state.observations] == [visible.timestamp_ms] * 2
    rows = [json.loads(line) for line in (args.out_dir / "snapshots.jsonl").read_text().splitlines()]
    assert [row["observation_timestamp_ms"] for row in rows] == [visible.timestamp_ms] * 2
    assert rows[0]["timestamp_ms"] < visible.timestamp_ms
