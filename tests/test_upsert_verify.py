# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import time
from pathlib import Path
from typing import Any, cast

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pytest
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import DataScan

from ingest_bench import catalog as cat
from ingest_bench import uri
from ingest_bench.clock import now_ms
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.scorer import cli, freshness, score, table_health, upsert_verify
from ingest_bench.scorer.snapshots import TableHistoryError, load_table, read_metadata, visible_snapshot_ids
from ingest_bench.scorer.upsert_verify import check_snapshot
from tests.test_score_loop import StepClock
from tests.test_upsert_score import _offer
from tests.upsert_fixtures import (
    KAFKA_SEED_SETS,
    ToyEngine,
    build_upsert_corpus,
    create_empty,
    register,
    version_rows,
)


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> corpus_metadata.CorpusMetadata:
    return build_upsert_corpus(tmp_path_factory.mktemp("corpora"))


def final_rows(corpus: corpus_metadata.CorpusMetadata) -> tuple[pa.Table, np.ndarray]:
    """The correct final live table and the stamps it answers to."""
    schedule = corpus.schedule
    assert schedule is not None
    stamp = schedule.last_ordinals()
    keys = np.arange(stamp.size, dtype=np.int64)
    return version_rows(corpus, schedule.ids_for_ordinals(stamp, keys)), stamp


def check(corpus: corpus_metadata.CorpusMetadata, rows: pa.Table, stamp: np.ndarray) -> dict[str, int]:
    assert corpus.schedule is not None
    return check_snapshot(corpus.schedule, iter(rows.to_batches(max_chunksize=500)), stamp).faults()


def test_the_correct_final_table_passes(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    assert check(corpus, rows, stamp) == dict.fromkeys(upsert_verify._FAULTS, 0)


def test_a_duplicate_key_fails(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    faults = check(corpus, pa.concat_tables([rows, rows.slice(10, 1)]), stamp)
    assert faults["duplicate_keys"] == 1


def test_a_missing_key_fails(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    assert check(corpus, rows.slice(1), stamp)["missing_keys"] == 1


def test_a_stale_version_fails(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    updated = int(np.flatnonzero(stamp[: corpus.schedule.config.seed_keys] > 0)[0])  # type: ignore[union-attr]
    stale = version_rows(corpus, np.array([-(updated + 1)], dtype=np.int64))
    current = pc.invert(pc.equal(rows.column("id"), pa.scalar(int(rows.column("id")[updated].as_py()))))
    faults = check(corpus, pa.concat_tables([rows.filter(current), stale]), stamp)
    assert faults["stale_rows"] == 1 and faults["missing_keys"] == 1


def test_a_persisted_key_that_disagrees_with_its_id_fails(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    tokens = rows.column("event_id").to_pylist()
    tokens[3] = "0" * 32
    index = rows.schema.get_field_index("event_id")
    damaged = rows.set_column(index, rows.schema.field(index), pa.array(tokens))
    assert check(corpus, damaged, stamp)["key_mismatch_rows"] == 1


def test_ids_outside_the_corpus_are_invalid(corpus: corpus_metadata.CorpusMetadata) -> None:
    rows, stamp = final_rows(corpus)
    stray = rows.slice(0, 1).set_column(0, rows.schema.field(0), pa.array([-(10**6)], pa.int64()))
    assert check(corpus, pa.concat_tables([rows, stray]), stamp)["invalid_rows"] == 1


def _scored_run(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> tuple[dict[str, str], Path, str]:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    engine = ToyEngine(table, corpus)
    time.sleep(0.01)
    epoch = now_ms()
    engine.upsert(engine.batch_versions(0, collapse=False))
    engine.upsert(engine.batch_versions(1, collapse=True))
    time.sleep(1.2)
    middle_s = (now_ms() - epoch) // 1000
    engine.upsert(engine.batch_versions(2, collapse=True))
    engine.upsert(engine.batch_versions(3, collapse=False))
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0, middle_s),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    return props, args.out_dir, table.metadata_location


def test_verify_upsert_passes_a_correct_run_and_reports_diagnostics(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, scores, metadata_location = _scored_run(tmp_path, corpus)
    argv = ["--metadata", metadata_location, "--corpus-uri", corpus.uri, "--checkpoints", str(scores / "checkpoints")]
    argv += ["--live", str(tmp_path / "live"), "--out", str(tmp_path / "verdict")]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    assert cli.verify_upsert(argv) == 0
    report = json.loads((tmp_path / "verdict" / upsert_verify.TERMINAL_UPSERT_FILE).read_text())
    assert report["verdict"] is True and report["reader"] == "pyiceberg"
    assert [snapshot["name"] for snapshot in report["snapshots"]][-1] == "final"
    assert len(report["snapshots"]) == 3 and all(snapshot["verdict"] for snapshot in report["snapshots"])
    assert report["live_checksum"]["expected"] == report["live_checksum"]["found"]
    final = report["snapshots"][-1]
    assert final["files"]["data_files"] >= 1 and final["scan_s"] is not None
    assert cast(float, report["write_amplification"]) > 0


def test_a_shortened_offer_is_verified_against_the_batches_it_offered(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    engine = ToyEngine(table, corpus)
    time.sleep(0.01)
    epoch = now_ms()
    engine.upsert(engine.batch_versions(0, collapse=False))
    engine.upsert(engine.batch_versions(1, collapse=True))
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri)[:2], epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0,),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    report = _verify(tmp_path, props, args.out_dir, table.metadata_location, corpus.uri)
    assert corpus.schedule is not None
    truth = corpus.schedule.live_truth(2)
    assert report["verdict"] is True
    assert report["live_checksum"] == {"expected": truth.checksum, "found": truth.checksum}


def test_an_engine_written_kafka_seed_verifies(tmp_path: Path) -> None:
    meta = build_upsert_corpus(tmp_path / "corpora", *KAFKA_SEED_SETS)
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = create_empty(props, "bench.up", meta, tmp_path / "wh")
    engine = ToyEngine(table, meta)
    engine.write_seed()
    seed_bytes = sum(record.encoded_bytes for record in corpus_metadata.read_seed_manifest(meta.uri))
    time.sleep(0.01)
    epoch = now_ms()
    for batch in range(meta.batch_count):
        engine.upsert(engine.batch_versions(batch, collapse=batch % 2 == 1))
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(meta.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=meta.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0,),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    report = _verify(tmp_path, props, args.out_dir, load_table(props, "bench.up").metadata_location, meta.uri)
    assert report["verdict"] is True
    assert report["live_checksum"]["expected"] == report["live_checksum"]["found"]
    # The engine wrote the seed too, so amplification counts every committed byte against seed and stream.
    written = sum(
        int(snapshot.summary["added-files-size"])
        for snapshot in load_table(props, "bench.up").metadata.snapshots
        if snapshot.summary is not None and snapshot.summary["added-files-size"] is not None
    )
    offered = seed_bytes + sum(record.encoded_bytes for record in corpus_metadata.read_manifest(meta.uri))
    assert report["write_amplification"] == pytest.approx(written / offered)


def test_verify_upsert_reads_a_copied_metadata_file_of_any_name(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    """finish.sh fetches the final metadata as table-metadata.final.json, which is not a table root."""
    props, scores, metadata_location = _scored_run(tmp_path, corpus)
    copied = tmp_path / "run" / "table-metadata.final.json"
    copied.parent.mkdir()
    copied.write_text(Path(metadata_location.removeprefix("file://")).read_text())
    report = _verify(tmp_path, props, scores, str(copied), corpus.uri)
    assert report["verdict"] is True


def test_the_pyiceberg_reader_refuses_equality_deletes(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    props, _, metadata_location = _scored_run(tmp_path, corpus)
    current = load_table(props, "bench.up").current_snapshot()
    assert current is not None

    def equality(self: DataScan) -> list[object]:
        raise ValueError(
            "PyIceberg does not yet support equality deletes: https://github.com/apache/iceberg/issues/6568"
        )

    monkeypatch.setattr(DataScan, "plan_files", equality)
    with pytest.raises(ValueError, match="--verify-in-cluster"):
        upsert_verify.scan_live_rows(
            metadata_location, [current.snapshot_id], "event_id", str(tmp_path / "live"), props
        )


def _verify(
    tmp_path: Path, props: dict[str, str], scores: Path, metadata_location: str, corpus_uri: str
) -> dict[str, Any]:
    argv = ["--metadata", metadata_location, "--corpus-uri", corpus_uri, "--checkpoints", str(scores / "checkpoints")]
    argv += ["--live", str(tmp_path / "live"), "--out", str(tmp_path / "verdict")]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    cli.verify_upsert(argv)
    return cast(dict[str, Any], json.loads((tmp_path / "verdict" / upsert_verify.TERMINAL_UPSERT_FILE).read_text()))


def test_a_change_after_drain_is_verified_on_the_frozen_snapshot(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, scores, _ = _scored_run(tmp_path, corpus)
    table = load_table(props, "bench.up")
    row_id = int(table.scan(selected_fields=("id",)).to_arrow().column("id")[0].as_py())
    table.delete(EqualTo(term=Reference("id"), value=literal(row_id)))
    table.refresh()
    report = _verify(tmp_path, props, scores, table.metadata_location, corpus.uri)
    frozen = next(snapshot for snapshot in report["snapshots"] if snapshot["name"] == "frozen")
    assert frozen["faults"]["missing_keys"] == 1 and report["verdict"] is False


def test_a_sample_missing_from_the_checkpoints_fails(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, scores, metadata_location = _scored_run(tmp_path, corpus)
    manifest = scores / "checkpoints" / "checkpoints.json"
    document = json.loads(manifest.read_text())
    document["checkpoints"] = [entry for entry in document["checkpoints"] if entry["name"] != "0"]
    manifest.write_text(json.dumps(document))
    report = _verify(tmp_path, props, scores, metadata_location, corpus.uri)
    assert report["unaccounted_offsets"] == [0] and report["verdict"] is False


def test_a_failed_checkpoint_write_is_retried(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_save = np.save
    failures = [OSError("disk full")]

    def flaky_save(path: Any, array: np.ndarray) -> None:
        if failures:
            raise failures.pop()
        real_save(path, array)

    monkeypatch.setattr(np, "save", flaky_save)
    _, scores, _ = _scored_run(tmp_path, corpus)
    names = {
        entry["name"] for entry in json.loads((scores / "checkpoints" / "checkpoints.json").read_text())["checkpoints"]
    }
    assert {"0", "final"} <= names and not failures


def test_a_schema_change_after_drain_fails(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, scores, _ = _scored_run(tmp_path, corpus)
    table = load_table(props, "bench.up")
    with table.update_schema() as update:
        update.make_column_optional("amount_micros")
    table.refresh()
    report = _verify(tmp_path, props, scores, table.metadata_location, corpus.uri)
    assert report["schema_mismatches"] and report["verdict"] is False


def _intermediate_run(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> tuple[dict[str, str], Path, str, int, int]:
    """Score a run whose sample offset lands inside batch 1's two-snapshot commit (delete, then append)."""
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    engine = ToyEngine(table, corpus)
    for batch in range(4):
        engine.upsert(engine.batch_versions(batch, collapse=False))
    snapshots = table.snapshots()
    # Seed, then a delete and an append per batch: batch 1's pair are snapshots 3 and 4.
    before, intermediate, after = snapshots[2], snapshots[3], snapshots[4]
    if after.timestamp_ms <= intermediate.timestamp_ms:
        pytest.skip("the commit's two snapshots share a millisecond")
    epoch = intermediate.timestamp_ms - 5000
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(5,),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    return props, args.out_dir, table.metadata_location, before.snapshot_id, intermediate.snapshot_id


def test_a_sample_inside_a_commit_checkpoints_the_state_before_it(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, scores, metadata_location, before, _ = _intermediate_run(tmp_path, corpus)
    checkpoints = json.loads((scores / "checkpoints" / "checkpoints.json").read_text())["checkpoints"]
    assert next(entry for entry in checkpoints if entry["name"] == "5")["snapshot_id"] == before
    assert _verify(tmp_path, props, scores, metadata_location, corpus.uri)["verdict"] is True


def test_a_checkpoint_on_a_snapshot_never_current_is_refused(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, scores, metadata_location, _, intermediate = _intermediate_run(tmp_path, corpus)
    manifest = scores / "checkpoints" / "checkpoints.json"
    document = json.loads(manifest.read_text())
    for entry in document["checkpoints"]:
        if entry["name"] == "5":
            entry["snapshot_id"] = intermediate
    manifest.write_text(json.dumps(document))
    report = _verify(tmp_path, props, scores, metadata_location, corpus.uri)
    assert report["never_current"] == [intermediate] and report["verdict"] is False


def test_the_scan_plan_lists_every_checkpoint_and_the_frozen_snapshot(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, capsys: pytest.CaptureFixture[str]
) -> None:
    props, scores, _ = _scored_run(tmp_path, corpus)
    table = load_table(props, "bench.up")
    row_id = int(table.scan(selected_fields=("id",)).to_arrow().column("id")[0].as_py())
    table.delete(EqualTo(term=Reference("id"), value=literal(row_id)))
    table.refresh()
    checkpoints = json.loads((scores / "checkpoints" / "checkpoints.json").read_text())["checkpoints"]
    current = table.current_snapshot()
    assert current is not None
    expected = sorted({entry["snapshot_id"] for entry in checkpoints} | {current.snapshot_id})
    argv = ["--metadata", table.metadata_location, "--checkpoints", str(scores / "checkpoints")]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    assert cli.upsert_scan_plan(argv) == 0
    assert capsys.readouterr().out.strip() == ",".join(str(snapshot) for snapshot in expected)


def test_table_health_reads_metadata_and_the_corpus_truth_only(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, scores, metadata_location = _scored_run(tmp_path, corpus)
    assert corpus.upsert_truth is not None
    argv = ["--metadata", metadata_location, "--corpus-uri", corpus.uri, "--checkpoints", str(scores / "checkpoints")]
    argv += ["--out", str(tmp_path / "health")]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    assert cli.table_health(argv) == 0
    report = json.loads((tmp_path / "health" / table_health.TABLE_HEALTH_FILE).read_text())
    assert report["live_rows"] == corpus.upsert_truth["live_rows"]
    assert report["data_file_rows"] == report["live_rows"] + report["dead_rows"]
    assert set(report["delete_files"]) == {"position", "equality", "deletion_vector"}


def _broken_run(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, *, repair: bool) -> dict[str, Any]:
    """An engine that only appends, optionally deduplicating the whole table just before drain."""
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    engine = ToyEngine(table, corpus)
    time.sleep(0.01)
    epoch = now_ms()
    engine.append(engine.batch_versions(0, collapse=False))
    engine.append(engine.batch_versions(1, collapse=False))
    time.sleep(1.2)
    middle_s = (now_ms() - epoch) // 1000
    engine.append(engine.batch_versions(2, collapse=False))
    engine.append(engine.batch_versions(3, collapse=False))
    if repair:
        engine.dedupe()
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(middle_s,),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    return _verify(tmp_path, props, args.out_dir, load_table(props, "bench.up").metadata_location, corpus.uri)


def test_an_engine_that_only_appends_fails_on_duplicate_keys(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    report = _broken_run(tmp_path, corpus, repair=False)
    assert report["verdict"] is False
    assert all(snapshot["faults"]["duplicate_keys"] > 0 for snapshot in report["snapshots"])


def test_an_engine_that_repairs_before_drain_fails_at_the_sample(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    report = _broken_run(tmp_path, corpus, repair=True)
    by_name = {snapshot["name"]: snapshot for snapshot in report["snapshots"]}
    assert by_name["final"]["verdict"] is True
    sample = next(snapshot for name, snapshot in by_name.items() if name != "final")
    assert sample["faults"]["duplicate_keys"] > 0 and report["verdict"] is False


def test_freshness_credits_a_commit_when_it_became_visible(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed: list[int] = []
    real = freshness.Observation

    def record(timestamp_ms: int, first_seen_ms: int, prefix: int) -> Any:
        observed.append(timestamp_ms)
        return real(timestamp_ms, first_seen_ms, prefix)

    monkeypatch.setattr(freshness, "Observation", record)
    props, _, _, _, intermediate = _intermediate_run(tmp_path, corpus)
    snapshots = {snapshot.snapshot_id: snapshot for snapshot in load_table(props, "bench.up").snapshots()}
    commit_end = next(s for s in snapshots.values() if s.parent_snapshot_id == intermediate)
    # The delete snapshot inside batch 1's commit is credited at the append that made the commit visible.
    assert snapshots[intermediate].timestamp_ms not in observed
    assert commit_end.timestamp_ms in observed


class _CommitsBetweenPolls(StepClock):
    """Lands the remaining batches during the scorer's first sleep."""

    def __init__(self, start_ms: int, engine: ToyEngine) -> None:
        super().__init__(start_ms)
        self.engine: ToyEngine | None = engine

    def sleep(self, seconds: float) -> None:
        if self.engine is not None:
            for batch in range(1, 4):
                self.engine.upsert(self.engine.batch_versions(batch, collapse=False))
            self.engine = None
        super().sleep(seconds)


def test_commits_beyond_the_retained_metadata_log_fail_the_scorer(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    with table.transaction() as transaction:
        transaction.set_properties({"write.metadata.previous-versions-max": "1"})
    engine = ToyEngine(load_table(props, "bench.up"), corpus)
    epoch = now_ms()
    engine.upsert(engine.batch_versions(0, collapse=False))
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0,),
    )
    clock = _CommitsBetweenPolls(now_ms(), engine)
    with pytest.raises(TableHistoryError, match="previous-versions-max"):
        score.run(args, clock, open(tmp_path / "score.log", "w"))


@pytest.mark.parametrize(("log", "commits"), [("trimmed", 4), ("absent", 4), ("absent", 1)])
def test_a_first_poll_after_the_metadata_log_dropped_scored_commits_fails_the_scorer(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, log: str, commits: int
) -> None:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    with table.transaction() as transaction:
        transaction.set_properties({"write.metadata.previous-versions-max": "1"})
    engine = ToyEngine(load_table(props, "bench.up"), corpus)
    epoch = now_ms() - 1
    # Every commit lands before the scorer's first poll, so the log keeps none of the early ones. A single
    # commit is current, but the state it replaced, which readers saw at the epoch, is still unknown.
    if commits == 1:
        # One append is one snapshot, and that snapshot is current.
        record = corpus_metadata.read_manifest(corpus.uri)[0]
        engine.table.append(version_rows(corpus, np.arange(record.id_min, record.id_max + 1, dtype=np.int64)))
    else:
        for batch in range(commits):
            engine.upsert(engine.batch_versions(batch, collapse=False))
    name = "bench.up"
    if log == "absent":
        # A document with no metadata log at all, as the format allows, registered as the run's table.
        location = load_table(props, name).metadata_location
        document = json.loads(uri.read_text(location))
        del document["metadata-log"]
        bare = location.replace(".metadata.json", "-bare.metadata.json")
        uri.write_text(bare, json.dumps(document))
        name = "bench.bare"
        cat.open_catalog(props).register_table(cat.table_identifier(name), bare)
        assert not read_metadata(load_table(props, name)).metadata_log
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, corpus_metadata.read_manifest(corpus.uri), epoch)
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table=name,
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "scores",
        poll_interval_s=1.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0,),
    )
    with pytest.raises(TableHistoryError, match="first poll"):
        score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w"))


def test_a_failed_metadata_read_loses_no_visible_snapshot(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, _, metadata_location, _, _ = _intermediate_run(tmp_path, corpus)
    table = load_table(props, "bench.up")
    expected = visible_snapshot_ids(table.metadata, table.io, set())
    failing = table.metadata.metadata_log[len(table.metadata.metadata_log) // 2].metadata_file

    class FailsOnce:
        def __init__(self) -> None:
            self.failed = False

        def new_input(self, location: str) -> Any:
            if location == failing and not self.failed:
                self.failed = True
                raise OSError("transient read failure")
            return table.io.new_input(location)

    seen: set[str] = set()
    with pytest.raises(OSError):
        visible_snapshot_ids(table.metadata, cast(Any, FailsOnce()), seen)
    assert visible_snapshot_ids(table.metadata, table.io, seen) == expected
