# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import io
import json
import shutil
import threading
from collections.abc import Callable
from dataclasses import replace
from itertools import cycle
from pathlib import Path

import pytest

from ingest_bench import kafka_admin, kafka_auth, uri
from ingest_bench.corpus import frames, generate, preset
from ingest_bench.producer import cli as producer_cli
from ingest_bench.producer import pacing, produce, publish_log

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


class FakeClock:
    def __init__(self, start_ms: int) -> None:
        self.t = start_ms
        self.slept: list[float] = []

    def now_ms(self) -> int:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.t += int(seconds * 1000)


class FakeProducer:
    def __init__(self, clock: FakeClock, fail_every: int = 0, full_first: int = 0) -> None:
        self.clock, self.fail_every, self.full_first = clock, fail_every, full_first
        self.sent: list[tuple[str, bytes, bytes | None]] = []
        self.partitions: list[int] = []
        self.attempted_partitions: list[int] = []
        self.pending: list[Callable[[], None]] = []

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        *,
        on_delivery: Callable[[object | None, object], None],
        partition: int = -1,
    ) -> None:
        self.attempted_partitions.append(partition)
        if self.full_first > 0:
            self.full_first -= 1
            raise BufferError("Local: Queue full")
        self.sent.append((topic, value, key))
        self.partitions.append(partition)
        n = len(self.sent)
        err = RuntimeError("delivery failed") if self.fail_every and n % self.fail_every == 0 else None
        self.pending.append(lambda: on_delivery(err, object()))

    def poll(self, timeout: float) -> int:
        self.clock.t += 1
        done = len(self.pending)
        for cb in self.pending:
            cb()
        self.pending.clear()
        return done

    def flush(self, timeout: float) -> int:
        self.poll(timeout)
        return 0


@pytest.fixture(scope="module")
def corpus_uri(tmp_path_factory: pytest.TempPathFactory) -> str:
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=4", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=11, row_block=64)
    return uri.join(out, preset.corpus_dir_name(p, seed=11, row_block=64))


def test_scheduling_and_selection(corpus_uri: str) -> None:
    from ingest_bench.corpus import metadata

    records = metadata.read_manifest(corpus_uri)
    assert pacing.scheduled_ms(1_000, 3000, 6) == 1_500
    assert [r.batch for r in pacing.select_batches(records, 1, 2, None)] == [1, 3]
    assert [r.batch for r in pacing.select_batches(records, 0, 1, 2)] == [0, 1]


def test_produce_batch_acks_and_keys() -> None:
    clock = FakeClock(10_000)
    fp = FakeProducer(clock)
    out = produce.produce_batch(fp, "t", [b"a", b"bb"], [b"k1", b"k2"], clock)
    assert out.rows == 2 and out.bytes == 3 and out.errors == 0
    assert out.first_ack_ms <= out.last_ack_ms
    assert [k for _, _, k in fp.sent] == [b"k1", b"k2"]
    assert fp.partitions == [-1, -1]


def test_produce_batch_when_every_message_fails_still_reports_real_timestamps() -> None:
    """Failed delivery reports still provide the batch's timing bounds."""
    clock = FakeClock(10_000)
    fp = FakeProducer(clock, fail_every=1)
    out = produce.produce_batch(fp, "t", [b"a", b"bb"], None, clock)
    assert out.rows == 2 and out.errors == 2
    assert out.first_ack_ms == clock.now_ms() and out.last_ack_ms == clock.now_ms()
    assert out.first_error == "delivery failed"


def test_round_robin_preserves_keys_and_retries_the_same_partition() -> None:
    clock = FakeClock(0)
    fp = FakeProducer(clock, full_first=2)
    partitions = cycle([0, 1, 2])
    for values in ([b"a", b"b"], [b"c", b"d"]):
        produce.produce_batch(fp, "t", values, [b"hot", b"hot"], clock, partition_iter=partitions)
    assert fp.sent == [("t", value, b"hot") for value in (b"a", b"b", b"c", b"d")]
    assert fp.partitions == [0, 1, 2, 0]
    assert fp.attempted_partitions == [0, 0, 0, 1, 2, 0]


@pytest.mark.parametrize("key_column", [None, "partition_key"])
@pytest.mark.parametrize("shard", [0, 1])
@pytest.mark.parametrize("partitions", [1, 3])
def test_run_round_robin_continues_across_batches(
    tmp_path: Path,
    corpus_uri: str,
    key_column: str | None,
    shard: int,
    partitions: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(kafka_admin, "topic_partition_count", lambda *args: partitions)
    clock = FakeClock(0)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1000,
        shard=shard,
        shards=2,
        seconds=None,
        key_column=key_column,
        value_prefix=b"",
        publish_log_path=tmp_path / "rr.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
        partition_assignment="round_robin",
        partitions=partitions,
    )
    fp = FakeProducer(clock)
    assert produce.run(args, lambda cfg: fp, clock, io.StringIO()) == 0
    records = publish_log.read(args.publish_log_path)
    assert len(records) == 2
    expected_cycle = [0] if partitions == 1 else [0, 1, 2]
    assert fp.partitions == (expected_cycle * (len(fp.sent) + 1))[shard : shard + len(fp.sent)]

    legacy = FakeProducer(clock)
    assert (
        produce.run(
            replace(args, partition_assignment="key", partitions=None, publish_log_path=tmp_path / "key.jsonl"),
            lambda cfg: legacy,
            clock,
            io.StringIO(),
        )
        == 0
    )
    assert fp.sent == legacy.sent
    assert set(legacy.partitions) == {-1}


@pytest.mark.parametrize("partitions", [2, 4])
def test_round_robin_rejects_topic_partition_count_mismatch(
    tmp_path: Path, corpus_uri: str, partitions: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    def partition_count(bootstrap: str, topic: str, client: dict[str, str]) -> int:
        assert (bootstrap, topic, client) == ("fake:9092", "t", {"security.protocol": "PLAINTEXT"})
        return 3

    monkeypatch.setattr(kafka_admin, "topic_partition_count", partition_count)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "p.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={"security.protocol": "PLAINTEXT"},
        partition_assignment="round_robin",
        partitions=partitions,
    )
    with pytest.raises(ValueError, match="topic 't' has 3 partitions"):
        produce.run(args, lambda cfg: pytest.fail("must reject before producing"), FakeClock(0), io.StringIO())
    assert not args.publish_log_path.exists()


@pytest.mark.parametrize("partitions", [None, 0, -1])
def test_round_robin_requires_a_positive_partition_count(tmp_path: Path, partitions: int | None) -> None:
    args = produce.ProduceArgs(
        corpus_uri="missing",
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "p.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
        partition_assignment="round_robin",
        partitions=partitions,
    )
    clock = FakeClock(0)
    with pytest.raises(ValueError, match="positive.*--partitions"):
        produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO())


def test_queue_full_backs_off_and_retries() -> None:
    clock = FakeClock(0)
    fp = FakeProducer(clock, full_first=3)
    out = produce.produce_batch(fp, "t", [b"a"], None, clock)
    assert out.rows == 1 and len(fp.sent) == 1 and clock.slept


def test_run_fetches_the_next_batch_while_the_current_one_publishes(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock(0)
    requested: list[tuple[int, int]] = []
    real_fetch = produce._fetch_batch

    def recording_fetch(record: generate.BatchRecord, key_column: str | None) -> produce.DownloadedBatch:
        requested.append((record.batch, clock.now_ms()))
        return real_fetch(record, key_column)

    monkeypatch.setattr(produce, "_fetch_batch", recording_fetch)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1000,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "prefetch.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
        partition_assignment="key",
        partitions=None,
    )
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO()) == 0
    records = publish_log.read(args.publish_log_path)
    assert [batch for batch, _ in requested] == [record.batch for record in records]
    # Every batch after the first is requested no later than the previous batch's
    # last acknowledgement: the download overlaps the publish instead of following it.
    for (_, requested_ms), previous in zip(requested[1:], records, strict=False):
        assert requested_ms <= previous.last_ack_ms


def test_run_decompresses_off_the_publish_thread_after_the_previous_batch_is_sent(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = FakeClock(0)
    decompressed: list[tuple[str, int]] = []
    real_decompress = frames.decompress

    def recording_decompress(data: bytes) -> bytes:
        decompressed.append((threading.current_thread().name, clock.now_ms()))
        return real_decompress(data)

    monkeypatch.setattr(frames, "decompress", recording_decompress)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1000,
        shard=0,
        shards=1,
        seconds=None,
        key_column="user_id",
        value_prefix=b"",
        publish_log_path=tmp_path / "decompress.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    fp = FakeProducer(clock)
    assert produce.run(args, lambda cfg: fp, clock, io.StringIO()) == 0
    records = publish_log.read(args.publish_log_path)
    assert len(records) > 1 and len(fp.sent) == sum(record.rows for record in records)
    # Values and keys of every batch, none of them on the publishing thread.
    assert len(decompressed) == 2 * len(records)
    assert all(name.startswith("decompress") for name, _ in decompressed)
    # Batch k is decompressed only after batch k-1's last acknowledgement, so it never
    # competes with a send.
    for (_, decompressed_ms), previous in zip(decompressed[2::2], records, strict=False):
        assert decompressed_ms >= previous.last_ack_ms
    for record in records:
        assert record.send_start_ms is not None and record.send_start_ms >= record.scheduled_ms
        assert record.fetch_ms is not None and record.fetch_ms >= 0
        assert record.decompress_ms is not None and record.decompress_ms >= 0


def test_publish_log_reads_records_written_before_the_timing_fields(tmp_path: Path) -> None:
    path = tmp_path / "publish_log-0.jsonl"
    path.write_text('{"batch":0,"bytes":100,"errors":0,"first_ack_ms":5,"last_ack_ms":50,"rows":10,"scheduled_ms":0}\n')
    publish_log.append(path, publish_log.PublishRecord(1, 1000, 1005, 1050, 10, 100, 0, 1001, 300, 40))
    old, new = publish_log.read(path)
    assert (old.send_start_ms, old.fetch_ms, old.decompress_ms) == (None, None, None)
    assert (new.send_start_ms, new.fetch_ms, new.decompress_ms) == (1001, 300, 40)


class RangeStore:
    """An object store holding one object, failing the listed range reads once each."""

    def __init__(self, blob: bytes, fail: dict[int, BaseException] | None = None) -> None:
        self.blob = blob
        self.fail = dict(fail or {})
        self.reads: list[tuple[int, int]] = []

    def size(self, path: str) -> int:
        return len(self.blob)

    def cat_file(self, path: str, start: int, end: int) -> bytes:
        self.reads.append((start, end))
        error = self.fail.pop(start, None)
        if error is not None:
            raise error
        return self.blob[start:end]


def _corpus_store(monkeypatch: pytest.MonkeyPatch, store: RangeStore, range_bytes: int = 4) -> list[float]:
    slept: list[float] = []
    monkeypatch.setattr(uri, "filesystem_for", lambda object_uri: (store, "b/batches/000001.bin.zst"))
    monkeypatch.setattr(produce, "FETCH_RANGE_BYTES", range_bytes)
    monkeypatch.setattr(produce, "FETCH_BYTES_PER_S", 10**12)
    monkeypatch.setattr("time.sleep", slept.append)
    return slept


def test_corpus_read_assembles_the_object_from_ranges(monkeypatch: pytest.MonkeyPatch) -> None:
    store = RangeStore(b"0123456789")
    _corpus_store(monkeypatch, store)
    assert produce._read_corpus_object("s3://b/batches/000001.bin.zst") == b"0123456789"
    assert store.reads == [(0, 4), (4, 8), (8, 10)]


@pytest.mark.parametrize("retry", [False, True])
def test_corpus_read_verifies_digest_across_ranges(monkeypatch: pytest.MonkeyPatch, retry: bool) -> None:
    blob = b"0123456789"
    store = RangeStore(blob, fail={4: ConnectionResetError()} if retry else None)
    _corpus_store(monkeypatch, store)
    assert produce._read_corpus_object("s3://b/batch", expected_sha256=frames.sha256_hex(blob)) == blob
    assert len(store.reads) == (4 if retry else 3)


@pytest.mark.parametrize("blob", [b"012345678X", b"01234567", b"0123456789extra"])
def test_corpus_read_rejects_changed_or_truncated_objects(monkeypatch: pytest.MonkeyPatch, blob: bytes) -> None:
    _corpus_store(monkeypatch, RangeStore(blob))
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        produce._read_corpus_object("s3://b/batch", expected_sha256=frames.sha256_hex(b"0123456789"))


def test_corrupt_append_batch_stops_before_sending(tmp_path: Path, corpus_uri: str) -> None:
    from ingest_bench.corpus import metadata

    copied = shutil.copytree(corpus_uri, tmp_path / "corpus")
    record = metadata.read_manifest(str(copied))[0]
    data = bytearray(frames.decompress(uri.read_bytes(record.uri)))
    data[-1] ^= 1
    uri.write_bytes(record.uri, frames.compress(bytes(data), 1))
    clock = FakeClock(0)
    producer = FakeProducer(clock)
    args = produce.ProduceArgs(
        corpus_uri=str(copied),
        bootstrap="fake",
        topic="test",
        epoch_ms=0,
        speed=1,
        shard=0,
        shards=1,
        seconds=1,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "publish.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="none",
        kafka_props={},
    )
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        produce.run(args, lambda config: producer, clock, io.StringIO())
    assert producer.sent == []
    assert not publish_log.shard_done(args.publish_log_path)


def test_corpus_read_retries_only_the_failed_range(monkeypatch: pytest.MonkeyPatch) -> None:
    store = RangeStore(b"0123456789", fail={4: ConnectionResetError("Connection reset by peer")})
    slept = _corpus_store(monkeypatch, store)
    assert produce._read_corpus_object("s3://b/batches/000001.bin.zst") == b"0123456789"
    assert store.reads == [(0, 4), (4, 8), (4, 8), (8, 10)]
    assert slept == [0.5]


def test_corpus_read_gives_up_after_the_last_attempt(monkeypatch: pytest.MonkeyPatch) -> None:
    class StalledStore(RangeStore):
        def cat_file(self, path: str, start: int, end: int) -> bytes:
            self.reads.append((start, end))
            raise TimeoutError("read timed out")

    store = StalledStore(b"0123456789")
    slept = _corpus_store(monkeypatch, store)
    with pytest.raises(TimeoutError):
        produce._read_corpus_object("s3://b/batches/000001.bin.zst")
    assert len(store.reads) == produce.FETCH_ATTEMPTS
    assert slept == [0.5, 1.0, 2.0]


def test_corpus_read_fails_at_once_on_a_missing_object(monkeypatch: pytest.MonkeyPatch) -> None:
    store = RangeStore(b"0123456789", fail={0: FileNotFoundError("b/batches/000001.bin.zst")})
    _corpus_store(monkeypatch, store)
    with pytest.raises(FileNotFoundError):
        produce._read_corpus_object("s3://b/batches/000001.bin.zst")
    assert store.reads == [(0, 4)]


def test_corpus_read_paces_ranges_to_the_byte_rate(monkeypatch: pytest.MonkeyPatch) -> None:
    store = RangeStore(b"01234567")
    slept = _corpus_store(monkeypatch, store)
    monkeypatch.setattr(produce, "FETCH_BYTES_PER_S", 4)
    monkeypatch.setattr("time.monotonic", lambda: 0.0)
    assert produce._read_corpus_object("s3://b/batches/000001.bin.zst") == b"01234567"
    # With no time passing, each 4-byte range waits until the 4 B/s budget allows it.
    assert slept == [1.0, 2.0]


def test_run_writes_publish_log_and_fails_on_delivery_error(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    log = io.StringIO()
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="run-x",
        epoch_ms=clock.now_ms() + 2_000,
        speed=2.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="user_id",
        value_prefix=b"",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    fp = FakeProducer(clock)
    assert produce.run(args, lambda cfg: fp, clock, log) == 0
    records = publish_log.read(tmp_path / "publish_log-0.jsonl")
    assert [r.batch for r in records] == [0, 1, 2, 3]
    assert [r.scheduled_ms for r in records] == [args.epoch_ms + o for o in (0, 500, 1000, 1500)]
    assert all(r.first_ack_ms >= r.scheduled_ms and r.last_ack_ms >= r.first_ack_ms and r.errors == 0 for r in records)
    assert sum(r.rows for r in records) == len(fp.sent)
    # Rank 0 uses the bare vocabulary entry "u"; other ranks use "u-<n>".
    keys_sent = [key for _, _, key in fp.sent]
    assert all(key is not None and key.startswith(b"u") for key in keys_sent)
    assert sum(1 for key in keys_sent if key is not None and key.startswith(b"u-")) > len(keys_sent) // 2
    text = log.getvalue()
    assert "PRODUCE DONE" in text and "PROGRESS" in text
    assert publish_log.behind_ms(records) < 100
    assert publish_log.emit_times(records)[3] == records[3].last_ack_ms

    failing = FakeProducer(clock, fail_every=50)
    log2 = io.StringIO()
    args2 = produce.ProduceArgs(
        **{**args.__dict__, "publish_log_path": tmp_path / "publish_log-1.jsonl", "key_column": None}
    )
    assert produce.run(args2, lambda cfg: failing, clock, log2) == 1
    assert "PRODUCE FAILED" in log2.getvalue() and "delivery failed" in log2.getvalue()
    assert all(k is None for _, _, k in failing.sent)


def test_key_column_must_have_a_sidecar(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(0)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="x",
        topic="t",
        epoch_ms=0,
        speed=1.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="country",
        value_prefix=b"",
        publish_log_path=tmp_path / "p.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    with pytest.raises(ValueError, match="kafka_key_columns"):
        produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO())


def test_the_confluent_header_precedes_every_value_and_is_counted(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    header = b"\x00\x00\x00\x00\x07"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=header,
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    prefixed = FakeProducer(clock)
    assert produce.run(args, lambda cfg: prefixed, clock, io.StringIO()) == 0
    values = [value for _, value, _ in prefixed.sent]
    assert values and all(value.startswith(header) for value in values)

    raw = FakeProducer(clock)
    plain = produce.ProduceArgs(
        **{**args.__dict__, "value_prefix": b"", "publish_log_path": tmp_path / "publish_log-1.jsonl"}
    )
    assert produce.run(plain, lambda cfg: raw, clock, io.StringIO()) == 0
    assert [value for _, value, _ in raw.sent] == [value[len(header) :] for value in values]

    # The publish log counts the wire bytes, which are five more per row.
    prefixed_rows = publish_log.read(tmp_path / "publish_log-0.jsonl")
    raw_rows = publish_log.read(tmp_path / "publish_log-1.jsonl")
    assert [record.rows for record in prefixed_rows] == [record.rows for record in raw_rows]
    assert [record.bytes for record in prefixed_rows] == [
        record.bytes + len(header) * record.rows for record in raw_rows
    ]


def test_the_cli_builds_the_header_and_refuses_a_pair_that_cannot_mean_anything(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[produce.ProduceArgs] = []

    def capture(
        args: produce.ProduceArgs,
        factory: Callable[[dict[str, object]], produce.FrameProducer],
        clock: object,
        log: object,
    ) -> int:
        seen.append(args)
        return 0

    monkeypatch.setattr(producer_cli, "run", capture)
    base = [
        "--corpus",
        corpus_uri,
        "--bootstrap",
        "fake:9092",
        "--topic",
        "t",
        "--epoch",
        "1700000000",
        "--publish-log",
        str(tmp_path / "publish_log-0.jsonl"),
    ]
    assert producer_cli.main(base) == 0
    assert seen[-1].value_prefix == b""

    assert producer_cli.main([*base, "--value-encoding", "confluent", "--schema-id", "7"]) == 0
    assert seen[-1].value_prefix == b"\x00\x00\x00\x00\x07"

    with pytest.raises(ValueError, match="requires --schema-id"):
        producer_cli.main([*base, "--value-encoding", "confluent"])
    with pytest.raises(ValueError, match="only sent in the Confluent wire format"):
        producer_cli.main([*base, "--schema-id", "7"])
    with pytest.raises(SystemExit):
        producer_cli.main([*base, "--value-encoding", "protobuf"])


def test_read_all_merges_shards(tmp_path: Path) -> None:
    for shard, batches in ((0, [0, 2]), (1, [1, 3])):
        path = tmp_path / f"publish_log-{shard}.jsonl"
        for b in batches:
            publish_log.append(path, publish_log.PublishRecord(b, b * 1000, b * 1000 + 5, b * 1000 + 50, 10, 100, 0))
    merged = publish_log.read_all(str(tmp_path))
    assert [r.batch for r in merged] == [0, 1, 2, 3]
    publish_log.append(tmp_path / "publish_log-2.jsonl", publish_log.PublishRecord(3, 0, 0, 0, 1, 1, 0))
    with pytest.raises(ValueError, match="duplicate"):
        publish_log.read_all(str(tmp_path))


def test_done_trailer_marks_the_shard_finished(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    log = io.StringIO()
    finished = tmp_path / "publish_log-2.jsonl"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1.0,
        shard=2,
        shards=4,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=finished,
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, log) == 0
    assert "PRODUCE DONE" in log.getvalue()
    assert json.loads(finished.read_text().splitlines()[-1]) == {"done": True, "shard": 2, "batches": 1}
    assert publish_log.shard_done(finished)
    assert [r.batch for r in publish_log.read(finished)] == [2]
    assert [r.batch for r in publish_log.read_all(str(tmp_path))] == [2]

    stopped = tmp_path / "publish_log-3.jsonl"
    failing = FakeProducer(clock, fail_every=1)
    stopped_args = replace(args, publish_log_path=stopped, shard=3, shards=4)
    assert produce.run(stopped_args, lambda cfg: failing, clock, io.StringIO()) == 1
    assert not publish_log.shard_done(stopped)
    assert not publish_log.shard_done(tmp_path / "publish_log-9.jsonl")


def test_shards_done_names_the_finished_shards(tmp_path: Path) -> None:
    assert publish_log.shards_done(str(tmp_path / "absent")) == set()
    for shard in (0, 1):
        publish_log.append(
            tmp_path / f"publish_log-{shard}.jsonl", publish_log.PublishRecord(shard, 0, 5, 50, 10, 100, 0)
        )
    assert publish_log.shards_done(str(tmp_path)) == set()
    publish_log.append_done(tmp_path / "publish_log-1.jsonl", 1, 1)
    assert publish_log.shards_done(str(tmp_path)) == {1}
    publish_log.append_done(tmp_path / "publish_log-0.jsonl", 0, 1)
    assert publish_log.shards_done(str(tmp_path)) == {0, 1}

    publish_log.append_done(tmp_path / "publish_log-x.jsonl", 0, 1)
    with pytest.raises(ValueError, match="does not name a shard index"):
        publish_log.shards_done(str(tmp_path))


def test_upload_prefix_publishes_the_log(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    uploads = tmp_path / "uploads"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column="partition_key",
        value_prefix=b"",
        publish_log_path=tmp_path / "local" / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=str(uploads),
        compression="zstd",
        kafka_props={},
    )
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO()) == 0
    uploaded = uploads / "producer" / "publish_log-0.jsonl"
    assert uploaded.read_bytes() == (tmp_path / "local" / "publish_log-0.jsonl").read_bytes()
    assert [r.batch for r in publish_log.read_all(str(uploads / "producer"))] == [0, 1, 2, 3]
    assert publish_log.shard_done(uploaded)


def test_kafka_props_apply_over_the_producer_defaults(tmp_path: Path, corpus_uri: str) -> None:
    clock = FakeClock(1_700_000_000_000)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={"security.protocol": "SASL_SSL", "linger.ms": "20"},
    )
    configs: list[dict[str, object]] = []

    def factory(config: dict[str, object]) -> produce.FrameProducer:
        configs.append(config)
        return FakeProducer(clock)

    assert produce.run(args, factory, clock, io.StringIO()) == 0
    # The site's properties win, and the defaults it says nothing about stand.
    assert configs[0]["security.protocol"] == "SASL_SSL" and configs[0]["linger.ms"] == "20"
    assert configs[0]["bootstrap.servers"] == "fake:9092" and configs[0]["enable.idempotence"] is True


def test_the_offer_is_compressed_with_the_codec_the_run_asked_for(tmp_path: Path, corpus_uri: str) -> None:
    assert produce.default_producer_config("fake:9092", "lz4")["compression.type"] == "lz4"
    assert produce.default_producer_config("fake:9092", "none")["compression.type"] == "none"

    clock = FakeClock(1_700_000_000_000)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="snappy",
        kafka_props={},
    )
    configs: list[dict[str, object]] = []

    def factory(config: dict[str, object]) -> produce.FrameProducer:
        configs.append(config)
        return FakeProducer(clock)

    assert produce.run(args, factory, clock, io.StringIO()) == 0
    assert configs[0]["compression.type"] == "snappy"


def test_the_cli_takes_the_codec_and_refuses_one_no_client_has(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[produce.ProduceArgs] = []

    def capture(
        args: produce.ProduceArgs,
        factory: Callable[[dict[str, object]], produce.FrameProducer],
        clock: object,
        log: object,
    ) -> int:
        seen.append(args)
        return 0

    monkeypatch.setattr(producer_cli, "run", capture)
    base = [
        "--corpus",
        corpus_uri,
        "--bootstrap",
        "fake:9092",
        "--topic",
        "t",
        "--epoch",
        "1700000000",
        "--publish-log",
        str(tmp_path / "publish_log-0.jsonl"),
    ]
    assert producer_cli.main(base) == 0
    assert seen[-1].compression == "zstd"
    assert (seen[-1].partition_assignment, seen[-1].partitions) == ("key", None)

    assert producer_cli.main([*base, "--partition-assignment", "round_robin", "--partitions", "8"]) == 0
    assert (seen[-1].partition_assignment, seen[-1].partitions) == ("round_robin", 8)
    with pytest.raises(SystemExit):
        producer_cli.main([*base, "--partition-assignment", "random"])

    assert producer_cli.main([*base, "--compression", "gzip"]) == 0
    assert seen[-1].compression == "gzip"

    with pytest.raises(SystemExit):
        producer_cli.main([*base, "--compression", "brotli"])


def test_a_kafka_prop_may_not_choose_the_wire_codec(tmp_path: Path, corpus_uri: str) -> None:
    """Client properties override producer configuration, so accepting compression
    here could contradict the published spec.
    """
    clock = FakeClock(1_700_000_000_000)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={"security.protocol": "SASL_SSL", "compression.type": "gzip"},
    )
    with pytest.raises(ValueError, match=r"compression\.type.*producer\.compression"):
        produce.run(args, lambda cfg: FakeProducer(clock), clock, io.StringIO())


def test_the_cli_resolves_a_kafka_prop_reference(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("IB_TEST_SASL_PASSWORD", "s3cret")
    seen: list[produce.ProduceArgs] = []

    def capture(
        args: produce.ProduceArgs,
        factory: Callable[[dict[str, object]], produce.FrameProducer],
        clock: object,
        log: object,
    ) -> int:
        seen.append(args)
        return 0

    # Patch the module attribute used by the CLI.
    monkeypatch.setattr(producer_cli, "run", capture)
    assert (
        producer_cli.main(
            [
                "--corpus",
                corpus_uri,
                "--bootstrap",
                "fake:9092",
                "--topic",
                "t",
                "--epoch",
                "1700000000",
                "--publish-log",
                str(tmp_path / "publish_log-0.jsonl"),
                "--kafka-prop",
                "security.protocol=SASL_SSL",
                "--kafka-prop",
                "sasl.password=${env:IB_TEST_SASL_PASSWORD}",
            ]
        )
        == 0
    )
    assert seen[0].kafka_props == {"security.protocol": "SASL_SSL", "sasl.password": "s3cret"}

    monkeypatch.delenv("IB_TEST_SASL_PASSWORD")
    with pytest.raises(ValueError, match="IB_TEST_SASL_PASSWORD"):
        producer_cli.main(
            [
                "--corpus",
                corpus_uri,
                "--bootstrap",
                "fake:9092",
                "--topic",
                "t",
                "--epoch",
                "1700000000",
                "--publish-log",
                str(tmp_path / "publish_log-0.jsonl"),
                "--kafka-prop",
                "sasl.password=${env:IB_TEST_SASL_PASSWORD}",
            ]
        )


def test_the_region_pseudo_key_never_reaches_the_producer(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch
) -> None:

    def signed(region: str) -> tuple[str, int]:
        return "token", 1_700_000_000_000

    monkeypatch.setattr(kafka_auth, "msk_token_provider", lambda: signed)
    clock = FakeClock(1_700_000_000_000)
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9098",
        topic="t",
        epoch_ms=clock.now_ms(),
        speed=1000.0,
        shard=0,
        shards=1,
        seconds=None,
        key_column=None,
        value_prefix=b"",
        publish_log_path=tmp_path / "publish_log-0.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": "eu-west-1"},
    )
    configs: list[dict[str, object]] = []

    def factory(config: dict[str, object]) -> produce.FrameProducer:
        configs.append(config)
        return FakeProducer(clock)

    assert produce.run(args, factory, clock, io.StringIO()) == 0
    assert "aws.region" not in configs[0] and callable(configs[0]["oauth_cb"])
    assert configs[0]["sasl.mechanism"] == "OAUTHBEARER" and configs[0]["enable.idempotence"] is True


@pytest.mark.parametrize("delays", [[], [-8, -4, -9, -7], [5, 20, 10, 1]])
def test_run_reports_maximum_ack_delay_and_batch_count(
    tmp_path: Path, corpus_uri: str, monkeypatch: pytest.MonkeyPatch, delays: list[int]
) -> None:
    clock = FakeClock(0)
    log = io.StringIO()
    path = tmp_path / "publish_log-0.jsonl"
    args = produce.ProduceArgs(
        corpus_uri=corpus_uri,
        bootstrap="fake:9092",
        topic="t",
        epoch_ms=0,
        speed=1.0,
        shard=0,
        shards=1,
        seconds=len(delays),
        key_column=None,
        value_prefix=b"",
        publish_log_path=path,
        behind_max_ms=15,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
    )
    acknowledgements = iter(batch * 1000 + delay for batch, delay in enumerate(delays))

    def outcome(*args: object, **kwargs: object) -> produce.BatchOutcome:
        ack = next(acknowledgements)
        return produce.BatchOutcome(ack, ack, 1, 1, 0)

    monkeypatch.setattr(produce, "produce_batch", outcome)
    assert produce.run(args, lambda cfg: FakeProducer(clock), clock, log) == 0
    records = publish_log.read(path)
    assert len(records) == len(delays)
    assert log.getvalue().splitlines()[-1] == (
        f"PRODUCE DONE batches={len(delays)} rows={len(delays)} behind_ms={publish_log.behind_ms(records)}"
    )
    assert log.getvalue().count("BEHIND ") == int(any(delay > args.behind_max_ms for delay in delays))
    assert json.loads(path.read_text().splitlines()[-1]) == {"done": True, "shard": 0, "batches": len(delays)}


def test_publish_windows_measure_each_shard_and_keep_real_offer_errors() -> None:
    records = [
        publish_log.PublishRecord(0, 1000, 1010, 1020, 10, 100, 0),
        publish_log.PublishRecord(1, 2000, 2030, 2040, 10, 100, 2),
        publish_log.PublishRecord(2, 3000, 3050, 3060, 10, 100, 0),
        publish_log.PublishRecord(3, 61_000, 67_000, 68_000, 10, 100, 0),
    ]
    windows = publish_log.windows(records, epoch_ms=1000, window_s=60, shards=2)
    assert [(w.start_ms, w.shard, w.batches, w.behind_ms_median, w.behind_ms_max, w.errors) for w in windows] == [
        (1000, 0, 2, 30, 50, 0),
        (1000, 1, 1, 30, 30, 2),
        (61_000, 1, 1, 6000, 6000, 0),
    ]


def _slice(batch: int, shard: int, first: int, last: int, rows: int = 10, slices: int = 3) -> publish_log.PublishRecord:
    return publish_log.PublishRecord(
        batch, batch * 1000, first, last, rows, rows * 10, 0, send_start_ms=first - 5, shard=shard, slices=slices
    )


def test_complete_slices_merge_into_the_batch_and_partial_ones_wait(tmp_path: Path) -> None:
    for shard in range(3):
        path = tmp_path / f"publish_log-{shard}.jsonl"
        publish_log.append(path, _slice(0, shard, 10 + shard, 40 + shard * 10))
        if shard < 2:
            publish_log.append(path, _slice(1, shard, 1010, 1020))
    logs = publish_log.read_logs(str(tmp_path))
    assert [record.batch for record in logs.batches] == [0]
    merged = logs.batches[0]
    assert (merged.rows, merged.bytes, merged.first_ack_ms, merged.last_ack_ms, merged.send_start_ms) == (
        30,
        300,
        12,
        60,
        5,
    )
    assert [(part.batch, part.shard) for part in logs.slices] == [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)]
    assert publish_log.read_all(str(tmp_path)) == logs.batches


def test_a_repeated_slice_is_a_duplicate(tmp_path: Path) -> None:
    publish_log.append(tmp_path / "publish_log-0.jsonl", _slice(0, 0, 10, 20))
    publish_log.append(tmp_path / "publish_log-1.jsonl", _slice(0, 0, 10, 20))
    with pytest.raises(ValueError, match="duplicate"):
        publish_log.read_all(str(tmp_path))


def test_slices_must_agree_on_their_count(tmp_path: Path) -> None:
    publish_log.append(tmp_path / "publish_log-0.jsonl", _slice(0, 0, 10, 20, slices=2))
    publish_log.append(tmp_path / "publish_log-1.jsonl", _slice(0, 1, 10, 20, slices=3))
    with pytest.raises(ValueError, match="disagree"):
        publish_log.read_all(str(tmp_path))


def test_append_records_stay_unchanged_on_disk() -> None:
    assert "shard" not in json.loads(publish_log.PublishRecord(0, 0, 0, 0, 1, 1, 0).to_json())


def test_publish_windows_attribute_slices_to_their_shard() -> None:
    slices = [_slice(0, 0, 1010, 1020), _slice(0, 2, 1030, 1040), _slice(1, 2, 2050, 2060)]
    windows = publish_log.windows(slices, epoch_ms=0, window_s=60, shards=3)
    assert [(w.shard, w.batches, w.behind_ms_max) for w in windows] == [(0, 1, 1010), (2, 2, 1050)]


def test_read_logs_uses_one_version_for_records_and_completion(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "publish_log-0.jsonl"
    record = publish_log.PublishRecord(0, 0, 5, 50, 10, 100, 0)
    publish_log.append(path, record)
    finished = tmp_path / "publish_log-1.jsonl"
    publish_log.append_done(finished, 1, 0)
    original = uri.read_text
    calls: list[str] = []

    def replaced_after_read(log_uri: str) -> str:
        calls.append(log_uri)
        text = original(log_uri)
        if log_uri == str(path):
            publish_log.append(path, replace(record, batch=2))
            publish_log.append_done(path, 0, 2)
        return text

    monkeypatch.setattr(uri, "read_text", replaced_after_read)
    logs = publish_log.read_logs(str(tmp_path))
    assert logs.batches == [record]
    assert logs.slices == []
    assert logs.done_shards == {1}
    assert sorted(calls) == [str(path), str(finished)]
    assert publish_log.shard_done(path)


def test_read_logs_overlaps_reads_within_the_worker_limit(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    for shard in range(6):
        publish_log.append(
            tmp_path / f"publish_log-{shard}.jsonl", publish_log.PublishRecord(shard, 0, 5, 50, 10, 100, 0)
        )
    original = uri.read_text
    barrier = threading.Barrier(2, timeout=5)
    lock = threading.Lock()
    active = peak = 0
    calls: list[str] = []

    def overlapping(log_uri: str) -> str:
        nonlocal active, peak
        with lock:
            active += 1
            peak = max(peak, active)
            calls.append(log_uri)
        try:
            barrier.wait()
            return original(log_uri)
        finally:
            with lock:
                active -= 1

    monkeypatch.setattr(uri, "read_text", overlapping)
    logs = publish_log.read_logs(str(tmp_path), read_workers=2)
    assert [record.batch for record in logs.batches] == list(range(6))
    assert peak == 2
    assert len(calls) == len(set(calls)) == 6


def test_read_logs_missing_prefix_is_unfinished(tmp_path: Path) -> None:
    logs = publish_log.read_logs(str(tmp_path / "absent"))
    assert logs.batches == logs.slices == []
    assert logs.done_shards == set()


@pytest.mark.parametrize("read_workers", [0, -1])
def test_read_logs_requires_positive_workers(tmp_path: Path, read_workers: int) -> None:
    with pytest.raises(ValueError, match="positive"):
        publish_log.read_logs(str(tmp_path), read_workers=read_workers)


def test_read_logs_propagates_read_failure_and_can_retry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "publish_log-0.jsonl"
    record = publish_log.PublishRecord(0, 0, 5, 50, 10, 100, 0)
    publish_log.append(path, record)
    original = uri.read_text

    def fail(log_uri: str) -> str:
        raise FileNotFoundError(log_uri)

    monkeypatch.setattr(uri, "read_text", fail)
    with pytest.raises(FileNotFoundError):
        publish_log.read_logs(str(tmp_path))
    publish_log.append_done(path, 0, 1)
    monkeypatch.setattr(uri, "read_text", original)
    logs = publish_log.read_logs(str(tmp_path))
    assert logs.batches == [record]
    assert logs.done_shards == {0}
