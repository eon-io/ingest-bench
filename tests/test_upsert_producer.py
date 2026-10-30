# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import io
from collections import defaultdict
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import cast

import fastavro
import numpy as np
import pytest

from ingest_bench import kafka_admin, uri
from ingest_bench.corpus import generate, metadata, preset, seed
from ingest_bench.producer import pacing, produce, publish_log
from tests.test_producer import FakeClock, FakeProducer

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
SETS = ["offered_bytes_per_s=200KB", "duration_s=4", "partition_count=8", "upsert.seed_keys=5000"]


@pytest.fixture(scope="module")
def upsert_corpus(tmp_path_factory: pytest.TempPathFactory) -> str:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=SETS)
    out = str(tmp_path_factory.mktemp("upsert"))
    generate.generate(p, out, seed=3, row_block=64)
    return uri.join(out, preset.corpus_dir_name(p, seed=3, row_block=64))


@pytest.fixture(scope="module")
def append_corpus(tmp_path_factory: pytest.TempPathFactory) -> str:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=SETS[:3])
    out = str(tmp_path_factory.mktemp("append"))
    generate.generate(p, out, seed=3, row_block=64)
    return uri.join(out, preset.corpus_dir_name(p, seed=3, row_block=64))


@pytest.fixture(autouse=True)
def sixteen_partitions(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(kafka_admin, "topic_partition_count", lambda *args: 16)


def arguments(corpus_uri: str, log: Path, shard: int = 0, shards: int = 1) -> produce.ProduceArgs:
    return produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1000,
        shard=shard,
        shards=shards,
        seconds=None,
        key_column="event_id",
        value_prefix=b"",
        publish_log_path=log,
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
        partition_assignment="corpus",
        partitions=16,
    )


def offer(corpus_uri: str, logs: Path, shards: int) -> list[FakeProducer]:
    producers = []
    for shard in range(shards):
        clock = FakeClock(0)
        producer = FakeProducer(clock)
        args = arguments(corpus_uri, logs / f"publish_log-{shard}.jsonl", shard, shards)
        assert produce.run(args, partial(lambda fp, cfg: fp, producer), clock, io.StringIO()) == 0
        producers.append(producer)
    return producers


def decoded_ids(corpus_uri: str, values: list[bytes]) -> np.ndarray:
    meta = metadata.read(corpus_uri)
    schema = fastavro.parse_schema(meta.schema)
    return np.array([cast(dict[str, int], fastavro.schemaless_reader(io.BytesIO(v), schema))["id"] for v in values])


def test_shards_send_every_row_once_in_corpus_order_per_partition(upsert_corpus: str, tmp_path: Path) -> None:
    producers = offer(upsert_corpus, tmp_path, 3)
    schedule = metadata.read(upsert_corpus).schedule
    assert schedule is not None
    by_partition: dict[int, list[int]] = defaultdict(list)
    all_ids = []
    for producer in producers:
        ids = decoded_ids(upsert_corpus, [value for _, value, _ in producer.sent])
        keys = schedule.keys_for_ids(ids)
        assert np.array_equal(schedule.kafka_partition_of(keys), np.array(producer.partitions))
        assert [key for _, _, key in producer.sent] == [token.encode() for token in schedule.tokens(keys)]
        for partition, row_id in zip(producer.partitions, ids.tolist(), strict=True):
            by_partition[partition].append(row_id)
        all_ids.extend(ids.tolist())
    total = sum(record.rows for record in metadata.read_manifest(upsert_corpus))
    assert len(all_ids) == len(set(all_ids)) == total
    for sent in by_partition.values():
        assert sent == sorted(sent)


def test_slices_merge_into_complete_batches(upsert_corpus: str, tmp_path: Path) -> None:
    offer(upsert_corpus, tmp_path, 3)
    slices = publish_log.read(tmp_path / "publish_log-1.jsonl")
    assert [(record.shard, record.slices) for record in slices] == [(1, 3)] * 4
    merged = publish_log.read_all(str(tmp_path))
    assert [record.rows for record in merged] == [record.rows for record in metadata.read_manifest(upsert_corpus)]
    assert publish_log.shards_done(str(tmp_path)) == {0, 1, 2}


def test_shards_owning_nothing_still_complete_the_batches(upsert_corpus: str, tmp_path: Path) -> None:
    idle = [shard for shard in range(20) if not pacing.owned_partitions(shard, 20, 16)]
    assert len(idle) == 4
    offer(upsert_corpus, tmp_path, 20)
    for shard in idle:
        assert [record.rows for record in publish_log.read(tmp_path / f"publish_log-{shard}.jsonl")] == [0] * 4
    assert len(publish_log.read_all(str(tmp_path))) == 4
    assert publish_log.shards_done(str(tmp_path)) == set(range(20))


def test_ownership_is_contiguous_and_covers_every_partition() -> None:
    blocks = [pacing.owned_partitions(shard, 5, 16) for shard in range(5)]
    assert [p for block in blocks for p in block] == list(range(16))


def test_a_corrupt_owned_segment_stops_the_shard_before_sending(upsert_corpus: str, tmp_path: Path) -> None:
    record = metadata.read_manifest(upsert_corpus)[0]
    segment = cast(list[dict[str, object]], record.segments)[0]
    path = Path(record.uri)
    original = path.read_bytes()
    damaged = bytearray(original)
    damaged[cast(int, segment["offset"]) + 2] ^= 0xFF
    path.write_bytes(bytes(damaged))
    try:
        clock = FakeClock(0)
        producer = FakeProducer(clock)
        with pytest.raises(AssertionError, match="sha256"):
            produce.run(arguments(upsert_corpus, tmp_path / "p.jsonl"), lambda cfg: producer, clock, io.StringIO())
        assert producer.sent == []
    finally:
        path.write_bytes(original)


def test_assignment_and_corpus_must_agree(upsert_corpus: str, append_corpus: str, tmp_path: Path) -> None:
    clock = FakeClock(0)
    with pytest.raises(ValueError, match="append-only corpus"):
        produce.run(arguments(append_corpus, tmp_path / "a.jsonl"), lambda cfg: pytest.fail("no"), clock, io.StringIO())
    keyed = replace(arguments(upsert_corpus, tmp_path / "k.jsonl"), partition_assignment="key", partitions=None)
    with pytest.raises(ValueError, match="upsert corpus"):
        produce.run(keyed, lambda cfg: pytest.fail("no"), clock, io.StringIO())


def test_topic_must_match_the_corpus_layout(
    upsert_corpus: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(kafka_admin, "topic_partition_count", lambda *args: 12)
    with pytest.raises(ValueError, match="laid out for 16"):
        produce.run(
            arguments(upsert_corpus, tmp_path / "p.jsonl"), lambda cfg: pytest.fail("no"), FakeClock(0), io.StringIO()
        )


def test_a_second_attempt_is_refused(upsert_corpus: str, tmp_path: Path) -> None:
    offer(upsert_corpus, tmp_path, 1)
    args = arguments(upsert_corpus, tmp_path / "publish_log-0.jsonl")
    with pytest.raises(ValueError, match="already produced"):
        produce.run(args, lambda cfg: pytest.fail("no"), FakeClock(0), io.StringIO())


def test_range_reads_stay_inside_the_owned_block(upsert_corpus: str, monkeypatch: pytest.MonkeyPatch) -> None:
    record = metadata.read_manifest(upsert_corpus)[1]
    owned = pacing.owned_partitions(1, 4, 16)
    reads: list[tuple[str, int, int]] = []
    real = produce._read_corpus_range

    def recording(object_uri: str, first: int, last: int) -> bytes:
        reads.append((object_uri, first, last))
        return real(object_uri, first, last)

    monkeypatch.setattr(produce, "_read_corpus_range", recording)
    produce._fetch_slice(record, "event_id", owned)
    segments = cast(list[dict[str, object]], record.segments)
    start = cast(int, segments[owned.start]["offset"])
    end = cast(int, segments[owned.stop - 1]["offset"]) + cast(int, segments[owned.stop - 1]["length"])
    assert reads[0] == (record.uri, start, end)
    assert len(reads) == 2 and reads[1][0] == record.key_uris["event_id"]


def test_the_attempt_marker_lands_before_anything_is_sent(upsert_corpus: str, tmp_path: Path) -> None:
    class CrashingProducer(FakeProducer):
        def produce(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("the pod died mid-batch")

    clock = FakeClock(0)
    args = replace(arguments(upsert_corpus, tmp_path / "publish_log-0.jsonl"), upload_prefix=str(tmp_path / "up"))
    with pytest.raises(RuntimeError):
        produce.run(args, lambda cfg: CrashingProducer(clock), clock, io.StringIO())
    assert (tmp_path / "up" / "producer" / "publish_log-0.jsonl").exists()
    assert publish_log.read(args.publish_log_path) == []
    with pytest.raises(ValueError, match="already produced"):
        produce.run(
            replace(args, publish_log_path=tmp_path / "elsewhere" / "publish_log-0.jsonl"),
            lambda cfg: pytest.fail("no"),
            clock,
            io.StringIO(),
        )


@pytest.fixture(scope="module")
def kafka_seed_corpus(tmp_path_factory: pytest.TempPathFactory) -> str:
    sets = [*SETS, "upsert.seed_delivery=kafka", "upsert.key_layout=time_ordered", "upsert.seed_history_s=600"]
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=sets)
    out = str(tmp_path_factory.mktemp("kafka-seed"))
    generate.generate(p, out, seed=3, row_block=64)
    corpus_uri = uri.join(out, preset.corpus_dir_name(p, seed=3, row_block=64))
    for shard in range(2):
        seed.write_seed_shard(corpus_uri, shard, 2)
    seed.commit_seed(corpus_uri)
    return corpus_uri


def test_the_seed_phase_sends_every_seed_key_once_in_creation_order(kafka_seed_corpus: str, tmp_path: Path) -> None:
    schedule = metadata.read(kafka_seed_corpus).schedule
    assert schedule is not None
    sent_ids: list[int] = []
    logs = tmp_path / "seed"
    logs.mkdir()
    for shard in range(3):
        clock = FakeClock(0)
        producer = FakeProducer(clock)
        args = replace(arguments(kafka_seed_corpus, logs / f"publish_log-{shard}.jsonl", shard, 3), seed=True)
        assert produce.run(args, partial(lambda fp, cfg: fp, producer), clock, io.StringIO()) == 0
        ids = decoded_ids(kafka_seed_corpus, [value for _, value, _ in producer.sent])
        keys = schedule.keys_for_ids(ids)
        assert np.array_equal(schedule.kafka_partition_of(keys), np.array(producer.partitions))
        assert [key for _, _, key in producer.sent] == [token.encode() for token in schedule.tokens(keys)]
        for partition in set(producer.partitions):
            # Each partition receives its seed keys batch by batch, oldest batch first.
            sent = keys[np.array(producer.partitions) == partition]
            batches = sent // schedule.rows_per_batch
            assert (np.diff(batches) >= 0).all()
        sent_ids.extend(ids.tolist())
        assert shard in publish_log.shards_done(str(logs))
    assert sorted(sent_ids) == list(range(-schedule.config.seed_keys, 0))


def test_the_seed_phase_needs_a_kafka_seed(upsert_corpus: str, tmp_path: Path) -> None:
    args = replace(arguments(upsert_corpus, tmp_path / "seed_log-0.jsonl"), seed=True)
    with pytest.raises(ValueError, match="no seed for Kafka"):
        produce.run(args, lambda cfg: FakeProducer(FakeClock(0)), FakeClock(0), io.StringIO())
