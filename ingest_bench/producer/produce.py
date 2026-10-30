# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Publish corpus batches to Kafka on their recorded schedule.

Send encoded corpus values without decoding and record broker acknowledgements.
Abort on delivery errors: retrying a partially delivered batch could duplicate
rows or leave gaps that the scorer would attribute to the engine.
"""

from __future__ import annotations

import hashlib
import sys
import time
from collections import deque
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from functools import partial
from itertools import chain, count, repeat
from pathlib import Path
from typing import Protocol, TextIO, cast

from ingest_bench import kafka_auth, uri
from ingest_bench.clock import Clock
from ingest_bench.corpus import frames, metadata
from ingest_bench.corpus.generate import BatchRecord, read_segment
from ingest_bench.producer import pacing, publish_log
from ingest_bench.specs.model import (
    PARTITION_ASSIGNMENT_CORPUS,
    PARTITION_ASSIGNMENT_KEY,
    PARTITION_ASSIGNMENT_ROUND_ROBIN,
    PARTITION_ASSIGNMENTS,
    refuse_compression_props,
)

PROGRESS_INTERVAL_MS = 60_000
UPLOAD_INTERVAL_MS = 5_000

# librdkafka's delivery reports are served from `poll`, and nothing else frees
# the send queue, so a long batch has to yield to it before the queue fills.
POLL_EVERY_ROWS = 4096

# Match the configured message timeout when draining the queue.
FLUSH_TIMEOUT_S = 120.0

# A failed corpus range backs off 0.5, 1 and 2 s. The prefetch runs two batches ahead,
# so a retry that ends within a batch interval does not delay the schedule.
FETCH_ATTEMPTS = 4
FETCH_BACKOFF_S = 0.5
PREFETCH_BATCHES = 2
# 4 MiB ranges paced to 100 MB/s: a 600 MB/s offer needs ~20 MB/s per shard at 30 shards,
# and an unpaced shard pulled at line rate, which r6i.2xlarge drops past its inbound allowance.
FETCH_RANGE_BYTES = 4 * 2**20
FETCH_BYTES_PER_S = 100_000_000


class FrameProducer(Protocol):
    """Producer interface used for publishing and test doubles.

    Keep ``on_delivery`` keyword-only: librdkafka's fourth positional argument
    is the partition.
    """

    def produce(
        self,
        topic: str,
        value: bytes,
        key: bytes | None,
        *,
        on_delivery: Callable[[object | None, object], None],
        partition: int = -1,
    ) -> None: ...

    def poll(self, timeout: float) -> int: ...

    def flush(self, timeout: float) -> int: ...


@dataclass(frozen=True)
class BatchOutcome:
    """Delivery totals and timing for one batch.

    ``first_ack_ms`` and ``last_ack_ms`` bound all delivery reports, including
    failures; they are acknowledgement times only when every delivery succeeds.
    """

    first_ack_ms: int
    last_ack_ms: int
    rows: int
    bytes: int
    errors: int
    # The first delivery error's text, for diagnostics; None when errors is 0.
    first_error: str | None = None


@dataclass(frozen=True)
class ProduceArgs:
    corpus_uri: str
    bootstrap: str
    topic: str
    epoch_ms: int
    speed: float
    shard: int
    shards: int
    seconds: int | None
    key_column: str | None
    # Prebuilt value header; empty for raw Avro and JSON.
    value_prefix: bytes
    publish_log_path: Path
    behind_max_ms: int
    upload_prefix: str | None
    # The codec the batches are compressed with, which the run's spec chose and
    # its `facts.json` published: a consumer is configured against it.
    compression: str
    # Resolved site properties, applied over defaults through `kafka_auth`.
    kafka_props: dict[str, str]
    partition_assignment: str = PARTITION_ASSIGNMENT_KEY
    partitions: int | None = None
    value_encoding: str = "avro"
    # Send the corpus's Kafka seed instead of its stream: the rows the engine writes before the
    # scored offer.
    seed: bool = False


def default_producer_config(bootstrap: str, compression: str) -> dict[str, object]:
    """Configure idempotent delivery with full acknowledgements and large buffers.

    Use the caller's compression codec. Buffering absorbs brief broker stalls;
    acknowledgement timing records resulting producer delay.
    """
    return {
        "bootstrap.servers": bootstrap,
        "acks": "all",
        "enable.idempotence": True,
        # round_robin sends consecutive records to different partitions, so at 5 ms a
        # partition batch held a few records: at 1 GB/s each broker took ~8.5k produce
        # requests/s of ~13 KB and ran short of CPU. 100 ms makes batches ~20x larger
        # for 0.1 s of producer latency.
        "linger.ms": 100,
        "batch.size": 1048576,
        "compression.type": compression,
        "queue.buffering.max.messages": 1000000,
        "queue.buffering.max.kbytes": 1048576,
        # Carry more partition batches per request to amortize replication latency
        # with acks=all. Per-partition batches stay at batch.size.
        "message.max.bytes": 16777216,
        "message.timeout.ms": 120000,
    }


def produce_batch(
    producer: FrameProducer,
    topic: str,
    frames_iter: Iterable[bytes],
    keys: Iterable[bytes] | None,
    clock: Clock,
    queue_full_backoff_s: float = 0.005,
    value_prefix: bytes = b"",
    partition_iter: Iterator[int] | None = None,
) -> BatchOutcome:
    """Send one batch and wait for all delivery reports.

    Flush before the next batch so acknowledgement boundaries remain distinct.
    Include ``value_prefix`` in each value and in the reported byte count.
    """
    acked = errors = 0
    # Time every delivery report, failed ones included, so a batch that fails
    # outright still records real clock values.
    first_ms: int | None = None
    last_ms: int | None = None
    first_error: str | None = None

    def on_delivery(err: object | None, _msg: object) -> None:
        nonlocal acked, errors, first_ms, last_ms, first_error
        now = clock.now_ms()
        if first_ms is None:
            first_ms = now
        last_ms = now
        if err is None:
            acked += 1
            return
        errors += 1
        if first_error is None:
            first_error = str(err)

    rows = 0
    total_bytes = 0
    key_iter = iter(keys) if keys is not None else None
    for frame in frames_iter:
        key: bytes | None = None
        if key_iter is not None:
            key = next(key_iter, None)
            if key is None:
                raise ValueError(
                    f"the key sidecar ran out at frame {rows}; it holds fewer keys than the batch has rows"
                )
        value = value_prefix + frame if value_prefix else frame
        # Choose once per record so queue-full retries retain their partition.
        partition = -1 if partition_iter is None else next(partition_iter)
        while True:
            try:
                producer.produce(topic, value, key, on_delivery=on_delivery, partition=partition)
                break
            except BufferError:
                # librdkafka's queue is bounded; draining delivery reports frees it.
                producer.poll(queue_full_backoff_s)
                clock.sleep(queue_full_backoff_s)
        rows += 1
        total_bytes += len(value)
        if rows % POLL_EVERY_ROWS == 0:
            producer.poll(0)
    if key_iter is not None and next(key_iter, None) is not None:
        raise ValueError(f"the key sidecar holds more keys than the batch's {rows} rows")
    producer.flush(FLUSH_TIMEOUT_S)
    if acked + errors != rows:
        raise RuntimeError(f"{rows} frames sent but {acked + errors} delivery reports received")
    if first_ms is None or last_ms is None:
        # Only an empty batch has no delivery report to time.
        first_ms = last_ms = clock.now_ms()
    return BatchOutcome(first_ms, last_ms, rows, total_bytes, errors, first_error)


@dataclass(frozen=True)
class DownloadedBatch:
    """A batch's compressed values and key sidecar, and how long they took to download.

    An upsert slice holds only the shard's block of segments, rebased to the block's start.
    """

    values: bytes
    keys: bytes | None
    fetch_ms: int
    segments: list[dict[str, object]] | None = None
    key_segments: list[dict[str, object]] | None = None


@dataclass(frozen=True)
class PreparedBatch:
    """A batch's decompressed values and keys, ready to send.

    ``partitions`` lists an upsert slice's Kafka partitions and their row counts, in send order.
    """

    values: bytes
    keys: bytes | None
    fetch_ms: int
    decompress_ms: int
    partitions: tuple[tuple[int, int], ...] | None = None


def _fetch_batch(record: BatchRecord, key_column: str | None) -> DownloadedBatch:
    """Download a batch's value bytes and, when keyed, its key sidecar."""
    started = time.monotonic()
    values = _read_corpus_object(record.uri, expected_sha256=record.sha256)
    keys = None if key_column is None else _read_corpus_object(record.key_uris[key_column])
    return DownloadedBatch(values, keys, _elapsed_ms(started))


def _prepare_batch(download: Future[DownloadedBatch]) -> PreparedBatch:
    """Wait for a batch's download, then decompress it."""
    batch = download.result()
    started = time.monotonic()
    values = frames.decompress(batch.values)
    keys = None if batch.keys is None else frames.decompress(batch.keys)
    return PreparedBatch(values, keys, batch.fetch_ms, _elapsed_ms(started))


def _owned_block(entries: list[dict[str, object]], owned: range) -> tuple[int, int, list[dict[str, object]]]:
    """The byte range covering the owned segments, and those segments rebased to it."""
    chosen = entries[owned.start : owned.stop]
    start = cast(int, chosen[0]["offset"])
    end = cast(int, chosen[-1]["offset"]) + cast(int, chosen[-1]["length"])
    return start, end, [{**entry, "offset": cast(int, entry["offset"]) - start} for entry in chosen]


def _fetch_slice(record: BatchRecord, key_column: str | None, owned: range) -> DownloadedBatch:
    """Download only this shard's block of an upsert batch and of its key sidecar."""
    started = time.monotonic()
    if not owned:
        return DownloadedBatch(b"", None if key_column is None else b"", 0, [], [])
    start, end, segments = _owned_block(cast(list[dict[str, object]], record.segments), owned)
    values = _read_corpus_range(record.uri, start, end)
    keys = key_segments = None
    if key_column is not None:
        key_entries = cast(dict[str, list[dict[str, object]]], record.key_segments)[key_column]
        key_start, key_end, key_segments = _owned_block(key_entries, owned)
        keys = _read_corpus_range(record.key_uris[key_column], key_start, key_end)
    return DownloadedBatch(values, keys, _elapsed_ms(started), segments, key_segments)


def _prepare_slice(download: Future[DownloadedBatch]) -> PreparedBatch:
    """Verify and decompress each owned segment, so a corrupt one stops the shard before it sends."""
    batch = download.result()
    started = time.monotonic()
    segments = cast(list[dict[str, object]], batch.segments)
    values = b"".join(read_segment(batch.values, segment) for segment in segments)
    keys = None
    if batch.keys is not None:
        keys = b"".join(
            read_segment(batch.keys, segment) for segment in cast(list[dict[str, object]], batch.key_segments)
        )
    partitions = tuple((cast(int, segment["partition"]), cast(int, segment["rows"])) for segment in segments)
    return PreparedBatch(values, keys, batch.fetch_ms, _elapsed_ms(started), partitions)


def _elapsed_ms(started: float) -> int:
    return round((time.monotonic() - started) * 1000)


def _read_corpus_object(object_uri: str, *, expected_sha256: str | None = None) -> bytes:
    """Read one corpus object in paced ranges, retrying a range that a second read can clear.

    A shard's exit fails the producer Job and stops every shard, so one stalled or reset
    object-store response would end the run. Corpus objects are immutable and nothing has
    been published from a batch that is still downloading, so reading a range again is safe,
    and a reset costs one range rather than the whole object. Pacing keeps a shard from
    pulling at line rate, which overran the node's inbound bandwidth allowance and caused the
    resets. A missing or forbidden object fails at once.
    """
    fs, path = uri.filesystem_for(object_uri)
    return _read_corpus_range(
        object_uri, 0, _retried(partial(fs.size, path), object_uri), expected_sha256=expected_sha256
    )


def _read_corpus_range(object_uri: str, first: int, last: int, *, expected_sha256: str | None = None) -> bytes:
    """Read bytes ``[first, last)`` of a corpus object in paced, individually retried ranges."""
    fs, path = uri.filesystem_for(object_uri)
    data = bytearray()
    digest = hashlib.sha256() if expected_sha256 is not None else None
    started = time.monotonic()
    for start in range(first, last, FETCH_RANGE_BYTES):
        end = min(start + FETCH_RANGE_BYTES, last)
        chunk = _retried(partial(fs.cat_file, path, start=start, end=end), object_uri)
        # Hash in the prefetch thread before pacing, so it uses the download's existing time budget.
        if digest is not None:
            digest.update(chunk)
        data += chunk
        ahead_s = len(data) / FETCH_BYTES_PER_S - (time.monotonic() - started)
        if ahead_s > 0:
            time.sleep(ahead_s)
    if digest is not None and digest.hexdigest() != expected_sha256:
        raise ValueError(f"SHA-256 mismatch for corpus object {object_uri}; refusing to publish altered bytes")
    return bytes(data)


def _retried[T](read: Callable[[], T], object_uri: str) -> T:
    for attempt in range(1, FETCH_ATTEMPTS):
        try:
            return read()
        except (FileNotFoundError, PermissionError):
            raise
        except Exception as error:
            delay_s = FETCH_BACKOFF_S * 2 ** (attempt - 1)
            print(
                f"reading {object_uri} failed ({type(error).__name__}: {error}); retry {attempt} in {delay_s:g}s",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay_s)
    return read()


def _upload(upload_prefix: str, path: Path) -> None:
    uri.write_bytes(uri.join(upload_prefix, "producer", path.name), path.read_bytes())


def _corpus_partitions(args: ProduceArgs, meta: metadata.CorpusMetadata) -> int | None:
    """Check the corpus and the assignment agree, returning an upsert offer's Kafka partition count.

    An upsert offer is sent once: a second attempt would replay old versions after newer ones.
    """
    corpus_mode = args.partition_assignment == PARTITION_ASSIGNMENT_CORPUS
    if meta.schedule is None:
        if corpus_mode:
            raise ValueError("an append-only corpus needs --partition-assignment key or round_robin")
        return None
    if not corpus_mode:
        raise ValueError("an upsert corpus needs --partition-assignment corpus")
    produced = args.publish_log_path.exists() and args.publish_log_path.stat().st_size > 0
    if not produced and args.upload_prefix is not None:
        produced = uri.exists(uri.join(args.upload_prefix, "producer", args.publish_log_path.name))
    if produced:
        raise ValueError(
            f"shard {args.shard} already produced this upsert run; replaying would put old versions after newer "
            "ones, so stage a fresh run with a new topic and table"
        )
    # Record the attempt durably before sending: a shard that dies after Kafka acknowledged records but before
    # its first log upload must still find this marker when relaunched.
    publish_log.append_attempt(args.publish_log_path, args.shard)
    if args.upload_prefix is not None:
        _upload(args.upload_prefix, args.publish_log_path)
    return meta.schedule.config.kafka_partitions


def run(
    args: ProduceArgs,
    producer_factory: Callable[[dict[str, object]], FrameProducer],
    clock: Clock,
    log: TextIO,
) -> int:
    if args.partition_assignment not in PARTITION_ASSIGNMENTS:
        raise ValueError(f"--partition-assignment must be one of {sorted(PARTITION_ASSIGNMENTS)}")
    partition_iter = None
    if args.partition_assignment == PARTITION_ASSIGNMENT_ROUND_ROBIN:
        partition_count = args.partitions
        if partition_count is None or partition_count < 1:
            raise ValueError("round_robin requires a positive --partitions count matching the topic")
        # Each shard cycles over every partition, continuing across corpus batches.
        partition_iter = (index % partition_count for index in count(args.shard))
    meta = metadata.read(args.corpus_uri)
    meta.validate_value_encoding(args.value_encoding)
    corpus_partitions = _corpus_partitions(args, meta)
    if args.value_encoding == "json" and args.value_prefix:
        raise ValueError("JSON values must not have a framing prefix")
    if args.key_column is not None and args.key_column not in meta.key_columns:
        raise ValueError(
            f"--key-column {args.key_column!r} is not in the corpus's kafka_key_columns {meta.key_columns}; "
            "regenerate the corpus with it or choose another key"
        )
    if args.seed and (corpus_partitions is None or args.seconds is not None):
        raise ValueError("--seed sends an upsert corpus's whole Kafka seed: use corpus assignment and no --seconds")
    manifest = metadata.read_seed_manifest(args.corpus_uri) if args.seed else metadata.read_manifest(args.corpus_uri)
    if corpus_partitions is None:
        selected = pacing.select_batches(manifest, args.shard, args.shards, args.seconds)
        fetch: Callable[[BatchRecord, str | None], DownloadedBatch] = _fetch_batch
        prepare: Callable[[Future[DownloadedBatch]], PreparedBatch] = _prepare_batch
    else:
        # Every shard sends its own Kafka partitions of every batch.
        selected = pacing.select_batches(manifest, 0, 1, args.seconds)
        fetch = partial(_fetch_slice, owned=pacing.owned_partitions(args.shard, args.shards, corpus_partitions))
        prepare = _prepare_slice
    # The second gate on the codec: these properties win the merge below, and
    # they reach here from a command line as well as from a site.
    refuse_compression_props(args.kafka_props, "--kafka-prop")
    if args.partition_assignment in {PARTITION_ASSIGNMENT_ROUND_ROBIN, PARTITION_ASSIGNMENT_CORPUS}:
        # kafka_admin imports confluent_kafka; defer it so helpers and CLI parsing work without librdkafka.
        from ingest_bench import kafka_admin

        actual_partitions = kafka_admin.topic_partition_count(args.bootstrap, args.topic, args.kafka_props)
        if corpus_partitions is None and args.partitions != actual_partitions:
            raise ValueError(
                f"--partitions is {args.partitions}, but topic {args.topic!r} has {actual_partitions} partitions"
            )
        if corpus_partitions is not None and corpus_partitions != actual_partitions:
            raise ValueError(
                f"the corpus is laid out for {corpus_partitions} Kafka partitions, but topic {args.topic!r} has "
                f"{actual_partitions} partitions"
            )
    producer = producer_factory(
        kafka_auth.librdkafka_config({**default_producer_config(args.bootstrap, args.compression), **args.kafka_props})
    )
    # Download and decompress upcoming batches on their own threads. A fetch that only starts
    # at the due time counts every slow object-store connection against the schedule, and
    # decompressing at the due time delays the first acknowledgement by the whole
    # decompression. Downloads run two batches ahead, which leaves a retried read a whole
    # batch interval of slack; only the next batch is held decompressed, to bound memory.
    fetches = ThreadPoolExecutor(max_workers=1, thread_name_prefix="prefetch")
    decompression = ThreadPoolExecutor(max_workers=1, thread_name_prefix="decompress")
    downloads: deque[Future[DownloadedBatch]] = deque(
        fetches.submit(fetch, record, args.key_column) for record in selected[:PREFETCH_BATCHES]
    )
    try:
        return _publish(
            args, producer, selected, downloads, fetches, decompression, partition_iter, clock, log, fetch, prepare
        )
    finally:
        decompression.shutdown(wait=True, cancel_futures=True)
        fetches.shutdown(wait=True, cancel_futures=True)


def _publish(
    args: ProduceArgs,
    producer: FrameProducer,
    selected: list[BatchRecord],
    downloads: deque[Future[DownloadedBatch]],
    fetches: ThreadPoolExecutor,
    decompression: ThreadPoolExecutor,
    partition_iter: Iterator[int] | None,
    clock: Clock,
    log: TextIO,
    fetch: Callable[[BatchRecord, str | None], DownloadedBatch],
    prepare: Callable[[Future[DownloadedBatch]], PreparedBatch],
) -> int:
    batches = 0
    behind = 0
    offered = 0
    published = 0
    errors = 0
    next_progress_ms = 0
    next_upload_ms = 0
    reported_behind = False
    prepared: deque[Future[PreparedBatch]] = deque()
    if downloads:
        prepared.append(decompression.submit(prepare, downloads.popleft()))

    for index, batch_record in enumerate(selected):
        due_ms = pacing.scheduled_ms(args.epoch_ms, batch_record.offset_ms, args.speed)
        delay_ms = due_ms - clock.now_ms()
        if delay_ms > 0:
            clock.sleep(delay_ms / 1000)
        batch = prepared.popleft().result()
        partitions = partition_iter
        if batch.partitions is not None:
            partitions = chain.from_iterable(repeat(partition, rows) for partition, rows in batch.partitions)
        send_start_ms = clock.now_ms()
        outcome = produce_batch(
            producer,
            args.topic,
            frames.iter_frames(batch.values),
            None if batch.keys is None else frames.iter_frames(batch.keys),
            clock,
            value_prefix=args.value_prefix,
            partition_iter=partitions,
        )
        # Prepare the next batches only once this one is sent: a shard sends for a fraction of
        # each batch interval, so both finish in the idle remainder without taking CPU from the
        # send loop.
        if downloads:
            prepared.append(decompression.submit(prepare, downloads.popleft()))
        if index + PREFETCH_BATCHES < len(selected):
            downloads.append(fetches.submit(fetch, selected[index + PREFETCH_BATCHES], args.key_column))
        record = publish_log.PublishRecord(
            batch=batch_record.batch,
            scheduled_ms=due_ms,
            first_ack_ms=outcome.first_ack_ms,
            last_ack_ms=outcome.last_ack_ms,
            rows=outcome.rows,
            bytes=outcome.bytes,
            errors=outcome.errors,
            send_start_ms=send_start_ms,
            fetch_ms=batch.fetch_ms,
            decompress_ms=batch.decompress_ms,
            shard=None if batch.partitions is None else args.shard,
            slices=None if batch.partitions is None else args.shards,
        )
        publish_log.append(args.publish_log_path, record)
        batch_delay = record.first_ack_ms - record.scheduled_ms
        behind = max(behind, batch_delay) if batches else batch_delay
        batches += 1
        offered += outcome.rows
        published += outcome.rows - outcome.errors
        errors += outcome.errors

        now_ms = clock.now_ms()
        if now_ms >= next_progress_ms:
            print(
                f"PROGRESS offered={offered} published={published} behind_ms={batch_delay} "
                f"behind_ms_max={behind} errors={errors}",
                file=log,
                flush=True,
            )
            next_progress_ms = now_ms + PROGRESS_INTERVAL_MS
        # Falling behind does not stop the run; whether it invalidates one is the
        # scorer's call, made from the same publish log this line is derived from.
        if not reported_behind and behind > args.behind_max_ms:
            print(f"BEHIND behind_ms={behind} behind_max_ms={args.behind_max_ms}", file=log, flush=True)
            reported_behind = True
        if args.upload_prefix is not None and now_ms >= next_upload_ms:
            _upload(args.upload_prefix, args.publish_log_path)
            next_upload_ms = now_ms + UPLOAD_INTERVAL_MS
        if outcome.errors:
            print(
                f"PRODUCE FAILED batch={batch_record.batch} errors={outcome.errors} error={outcome.first_error!r}",
                file=log,
                flush=True,
            )
            if args.upload_prefix is not None:
                _upload(args.upload_prefix, args.publish_log_path)
            return 1

    publish_log.append_done(args.publish_log_path, args.shard, batches)
    if args.upload_prefix is not None:
        _upload(args.upload_prefix, args.publish_log_path)
    print(
        f"PRODUCE DONE batches={batches} rows={offered} behind_ms={behind}",
        file=log,
        flush=True,
    )
    return 0
