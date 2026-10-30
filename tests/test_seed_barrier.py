# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import time
from collections.abc import Sequence
from pathlib import Path

import numpy as np
import pyarrow as pa
import pytest

from ingest_bench import uri
from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.clock import now_ms
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.producer import publish_log
from ingest_bench.scorer import cli, seed_barrier
from ingest_bench.scorer.snapshots import TableHistoryError
from tests.upsert_fixtures import KAFKA_SEED_SETS, ToyEngine, build_upsert_corpus, create_empty, version_rows

TABLE = "bench.up"


class Clock:
    """Advances by each sleep; ``on_sleep`` lets a test commit while the barrier waits."""

    def __init__(self, on_sleep: object = None) -> None:
        self.now, self.on_sleep = 0.0, on_sleep

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds
        if self.on_sleep is not None:
            self.on_sleep()  # type: ignore[operator]


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> corpus_metadata.CorpusMetadata:
    return build_upsert_corpus(tmp_path_factory.mktemp("corpora"), *KAFKA_SEED_SETS)


def seed_keys(corpus: corpus_metadata.CorpusMetadata) -> int:
    assert corpus.schedule is not None
    return corpus.schedule.config.seed_keys


def commit(engine: ToyEngine, corpus: corpus_metadata.CorpusMetadata, records: Sequence[BatchRecord]) -> None:
    for record in records:
        engine.table.append(version_rows(corpus, np.arange(record.id_min, record.id_max + 1, dtype=np.int64)))


def seeded_run(
    tmp_path: Path,
    corpus: corpus_metadata.CorpusMetadata,
    *,
    emitted_s_ago: float,
    batches: int | None = None,
    late_ms: int = 0,
    unlogged: frozenset[int] = frozenset(),
) -> tuple[dict[str, str], Path, ToyEngine]:
    """An empty table, the seed's publish log, and an engine that commits seed batches one by one."""
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    engine = ToyEngine(create_empty(props, TABLE, corpus, tmp_path / "wh"), corpus)
    records = corpus_metadata.read_seed_manifest(corpus.uri)
    logs = tmp_path / "seed"
    logs.mkdir()
    last = now_ms() - round(emitted_s_ago * 1000)
    for record in records:
        if record.batch in unlogged:
            continue
        sent = last - (len(records) - 1 - record.batch) * 1000
        due = sent - late_ms
        publish_log.append(
            logs / "publish_log-0.jsonl",
            publish_log.PublishRecord(record.batch, due, sent + 5, sent + 10, record.rows, record.encoded_bytes, 0),
        )
    publish_log.append_done(logs / "publish_log-0.jsonl", 0, len(records))
    commit(engine, corpus, records[: len(records) if batches is None else batches])
    return props, logs, engine


def wait(
    props: dict[str, str],
    corpus: corpus_metadata.CorpusMetadata,
    logs: Path,
    *,
    timeout_s: float = 10,
    clock: Clock | None = None,
) -> seed_barrier.SeedLoaded:
    return seed_barrier.wait_for_seed(
        props,
        TABLE,
        corpus.uri,
        str(logs),
        warmup_s=0,
        bound_s=60,
        timeout_s=timeout_s,
        poll_s=1,
        behind_max_ms=10_000,
        clock=clock or Clock(),
    )


def test_a_seed_committed_as_it_arrives_passes(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=2)
    loaded = wait(props, corpus, logs)
    assert loaded.rows == seed_keys(corpus)
    assert loaded.freshness.verdict and loaded.freshness.drained


def test_a_seed_held_back_and_committed_late_fails(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    # Every batch was published at least ten minutes before the engine committed any of it.
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=600)
    freshness = wait(props, corpus, logs).freshness
    assert freshness.drained and not freshness.verdict
    max_s = freshness.window["max_s"]
    assert max_s is not None and max_s > 120


def test_the_barrier_waits_for_every_seed_batch(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, logs, engine = seeded_run(tmp_path, corpus, emitted_s_ago=2, batches=1)
    rest = list(corpus_metadata.read_seed_manifest(corpus.uri)[1:])

    def commit_the_rest() -> None:
        if rest:
            commit(engine, corpus, rest)
            rest.clear()

    loaded = wait(props, corpus, logs, clock=Clock(commit_the_rest))
    assert loaded.waited_s == 1 and loaded.freshness.verdict


def test_the_barrier_gives_up_at_the_timeout(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=2, batches=1)
    with pytest.raises(TimeoutError, match="seed batches"):
        wait(props, corpus, logs, timeout_s=3)


def test_the_barrier_waits_for_publish_records_of_every_seed_batch(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    # Only the last batch's record and the trailer were logged: the earlier batches' delays are unknown,
    # so the seed's freshness and producer pace cannot be judged.
    batches = len(corpus_metadata.read_seed_manifest(corpus.uri))
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=2, unlogged=frozenset(range(batches - 1)))
    with pytest.raises(TimeoutError, match=f"publish logs record 1 of {batches} seed batches"):
        wait(props, corpus, logs, timeout_s=3)


@pytest.mark.parametrize(("emitted_s_ago", "status"), [(2, 0), (600, 3)])
def test_the_cli_writes_the_seeds_freshness_and_fails_a_late_seed(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata, emitted_s_ago: float, status: int
) -> None:
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=emitted_s_ago)
    out = tmp_path / "scores" / "seed_freshness.json"
    argv = ["--table", TABLE, "--corpus", corpus.uri, "--publish-logs", str(logs), "--out", str(out)]
    argv += ["--freshness-bound-s", "60", "--warmup-s", "0", "--timeout-s", "5", "--poll-s", "1"]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    assert cli.wait_seed(argv) == status
    report = json.loads(uri.read_text(str(out)))
    assert report["verdict"] is (status == 0) and report["rows"] == seed_keys(corpus)


def seed_rows(corpus: corpus_metadata.CorpusMetadata, record: BatchRecord) -> pa.Table:
    return version_rows(corpus, np.arange(record.id_min, record.id_max + 1, dtype=np.int64))


def test_snapshots_inside_one_commit_take_the_time_it_became_visible(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, _, engine = seeded_run(tmp_path, corpus, emitted_s_ago=2, batches=0)
    first, second = corpus_metadata.read_seed_manifest(corpus.uri)[:2]
    with engine.table.transaction() as transaction:
        transaction.append(seed_rows(corpus, first))
        time.sleep(1.1)
        transaction.append(seed_rows(corpus, second))
    tally = seed_barrier.SeedTally(corpus.uri)
    tally.poll(props, TABLE)
    visible = engine.table.refresh().current_snapshot()
    assert visible is not None
    assert [observation.timestamp_ms for observation in tally.observations] == [visible.timestamp_ms] * 2


def test_a_counted_snapshot_that_leaves_the_history_is_refused(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props, _, engine = seeded_run(tmp_path, corpus, emitted_s_ago=2, batches=2)
    tally = seed_barrier.SeedTally(corpus.uri)
    tally.poll(props, TABLE)
    oldest = min(engine.table.metadata.snapshots, key=lambda snapshot: snapshot.timestamp_ms)
    engine.table.manage_snapshots().set_current_snapshot(oldest.snapshot_id).commit()
    with pytest.raises(TableHistoryError, match="left the table's history"):
        tally.poll(props, TABLE)


def test_a_replaced_table_is_refused(tmp_path: Path, corpus: corpus_metadata.CorpusMetadata) -> None:
    props, _, _ = seeded_run(tmp_path, corpus, emitted_s_ago=2, batches=1)
    tally = seed_barrier.SeedTally(corpus.uri)
    tally.poll(props, TABLE)
    open_catalog(props).drop_table(table_identifier(TABLE))
    create_empty(props, TABLE, corpus, tmp_path / "wh2")
    with pytest.raises(TableHistoryError, match="replaced"):
        tally.poll(props, TABLE)


def test_a_late_seed_producer_voids_the_run_instead_of_failing_the_engine(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    # The producer sent every batch 20 s behind its schedule, past the 10 s limit.
    props, logs, _ = seeded_run(tmp_path, corpus, emitted_s_ago=2, late_ms=20_000)
    out = tmp_path / "scores" / "seed_freshness.json"
    argv = ["--table", TABLE, "--corpus", corpus.uri, "--publish-logs", str(logs), "--out", str(out)]
    argv += ["--behind-max-ms", "10000", "--timeout-s", "5", "--poll-s", "1"]
    argv += [f"--catalog-prop={key}={value}" for key, value in props.items()]
    assert cli.wait_seed(argv) == 5
    report = json.loads(uri.read_text(str(out)))
    assert report["producer_bound"] is True and report["producer_behind_ms"] >= 20_000
