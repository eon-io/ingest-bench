# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Generate a corpus: batch files, key sidecars, manifest, partition truth, column stats, corpus.json.

The manifest is the frozen scoring input. Every figure in it is re-derived
from the stored bytes by `verify_batch` before the corpus is published, so the
scorer trusts the manifest without trusting the encoder that wrote it.
"""

from __future__ import annotations

import io
import json
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import cast

import fastavro
import numpy as np

from ingest_bench import uri
from ingest_bench.corpus import DEFAULT_ROW_BLOCK, GENERATOR_VERSION, frames, json_values, upsert
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import values as v
from ingest_bench.corpus.preset import Preset, corpus_dir_name, corpus_hash, effective_dict
from ingest_bench.corpus.stats import (
    ColumnStats,
    column_stats_summary,
    expected_distinct_values,
    new_column_stats,
    observe_block,
    stats_stride,
)

PARTITION_SHARE_MAX_DEVIATION = 0.05
# Gate byte shares only when the coldest key has enough expected rows.
# At 10,000 rows, roughly 1% sampling noise is below the 5% tolerance.
PARTITION_SHARE_GATE_MIN_COLD_ROWS = 10_000
MEAN_ROW_MAX_DEVIATION = 0.02
CARDINALITY_MAX_DEVIATION = 0.10
# Unbounded blobs provide the incompressibility axis; validate their byte entropy.
UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE = 7.5


@dataclass
class BatchRecord:
    """Batch location and scoring truth: row count, checksum, and byte digest.

    Upsert batches also record their per-Kafka-partition segments and upsert truth.
    Append records omit those fields so their manifest lines stay unchanged.
    """

    batch: int
    offset_ms: int
    rows: int
    id_min: int
    id_max: int
    checksum: int
    encoded_bytes: int
    stored_bytes: int
    sha256: str
    uri: str
    key_uris: dict[str, str]
    segments: list[dict[str, object]] | None = None
    key_segments: dict[str, list[dict[str, object]]] | None = None
    upsert: dict[str, int] | None = None

    def to_json(self) -> str:
        data = {key: value for key, value in asdict(self).items() if value is not None}
        return json.dumps(data, sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_json(cls, line: str) -> BatchRecord:
        raw = json.loads(line)
        return cls(**raw)


@dataclass
class BatchFill:
    """Encoded batch rows and the measurements accumulated while generating them."""

    records: bytes
    sizes: np.ndarray
    ids: np.ndarray
    keys: np.ndarray
    key_values: dict[str, list[str]]
    partition_counts: np.ndarray
    partition_sum_mod: np.ndarray
    partition_encoded: np.ndarray
    upsert_keys: np.ndarray | None = None


def epoch_ms(preset: Preset) -> int:
    """Parse the corpus epoch as Unix milliseconds.

    Require an explicit timezone to keep generated event times independent of
    the host timezone.
    """
    moment = datetime.fromisoformat(preset.corpus_epoch.replace("Z", "+00:00"))
    if moment.tzinfo is None:
        raise ValueError(f"corpus_epoch {preset.corpus_epoch!r} needs an explicit UTC offset or a trailing Z")
    return round(moment.timestamp() * 1000)


def fill_batch(
    seed: int,
    batch: int,
    preset: Preset,
    payload_width: int,
    cdf: np.ndarray,
    row_block: int,
    column_stats: dict[str, ColumnStats] | None,
    stride: int,
    rows_per_batch_estimate: int,
    schedule: upsert.KeySchedule | None = None,
) -> BatchFill:
    """Fill the sizing encoding's byte budget with whole row blocks.

    Allow the last block to overshoot so row positions remain reproducible.
    Upsert batches instead hold the schedule's fixed row count, keyed by the schedule.
    """
    start_ms = epoch_ms(preset) + batch * preset.batch_interval_ms
    chunks: list[bytes] = []
    sizes: list[np.ndarray] = []
    ids: list[np.ndarray] = []
    keys: list[np.ndarray] = []
    key_values: dict[str, list[str]] = {name: [] for name in preset.kafka_key_columns}
    counts = np.zeros(preset.partition_count, dtype=np.int64)
    sums = np.zeros(preset.partition_count, dtype=np.int64)
    encoded = np.zeros(preset.partition_count, dtype=np.int64)
    rows = 0
    total = 0
    batch_keys = None if schedule is None else schedule.batch_keys(batch)
    while total < preset.batch_bytes if schedule is None else rows < schedule.rows_per_batch:
        replaced: dict[str, v.ColumnBlock] | None = None
        if schedule is None or batch_keys is None:
            block_keys = v.draw_partition_keys(seed, batch, rows, row_block, cdf)
        else:
            row_keys = batch_keys[rows : rows + row_block]
            block_keys = schedule.partition_of(row_keys)
            replaced = {schedule.key_column: schedule.key_column_block(row_keys)}
        block = v.build_row_block(
            seed,
            batch,
            rows,
            block_keys,
            payload_width,
            start_ms,
            preset.batch_interval_ms,
            preset.columns,
            replaced=replaced,
        )
        data, encoded_sizes = _encode_block(block, preset)
        chunks.append(data)
        sizes.append(encoded_sizes)
        ids.append(block.ids)
        keys.append(block_keys)
        for name, collected_values in key_values.items():
            collected_values.extend(v.column_strings(block, name))
        counts += np.bincount(block_keys, minlength=preset.partition_count)
        # Residues are below P and a block holds at most row_block of them, so the
        # partial sums stay exact in int64 before the reduction.
        sums += np.bincount(block_keys, weights=block.ids % c.P, minlength=preset.partition_count).astype(np.int64)
        sums %= c.P
        encoded += np.bincount(block_keys, weights=encoded_sizes, minlength=preset.partition_count).astype(np.int64)
        if column_stats is not None:
            observe_block(block, column_stats, batch * rows_per_batch_estimate + rows, stride)
        rows += block.rows
        budget_sizes = block.encoded_sizes if preset.budget_encoding == "avro" else encoded_sizes
        total += int(budget_sizes.sum())
        # Reject overflow into the next batch's reserved ID range.
        if rows >= c.ID_BLOCK:
            raise ValueError(f"batch {batch} exceeded its id block")
    return BatchFill(
        b"".join(chunks),
        np.concatenate(sizes),
        np.concatenate(ids),
        np.concatenate(keys),
        key_values,
        counts,
        sums,
        encoded,
        upsert_keys=batch_keys,
    )


def closed_form_checksum(id_min: int, id_max: int, rows: int) -> int:
    return ((id_min + id_max) * rows // 2) % c.P


def verify_batch(
    record: BatchRecord,
    data: bytes,
    key_data: dict[str, bytes],
    columns: tuple[c.ColumnDistribution, ...],
    fill_partition_counts: dict[int, int],
    value_encoding: str = "avro",
) -> None:
    """Recompute batch truth by decoding stored frames with the published schema.

    This independent read checks the encoder's output before publication.
    """
    label = f"batch {record.batch}"
    if frames.sha256_hex(data) != record.sha256:
        raise AssertionError(f"{label}: stored bytes do not match the manifest sha256")
    schema = fastavro.parse_schema(c.avro_schema(columns))
    key_frames = {name: list(frames.iter_frames(frames.decompress(blob))) for name, blob in key_data.items()}
    # Sidecars must have exactly one frame per batch row before lockstep decoding.
    for name, framed in key_frames.items():
        if len(framed) != record.rows:
            raise AssertionError(f"{label}: sidecar {name} holds {len(framed)} frames for {record.rows} rows")
    expected_id = record.id_min
    total = 0
    counts: dict[int, int] = {}
    decoded = 0
    for frame in frames.iter_frames(frames.decompress(data)):
        row = (
            cast(dict[str, object], json.loads(frame))
            if value_encoding == "json"
            else cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
        )
        row_id = cast(int, row["id"])
        if row_id != expected_id:
            raise AssertionError(f"{label}: id {row_id} where {expected_id} was expected")
        expected_id += 1
        total = (total + row_id) % c.P
        key = int(str(row["partition_key"])[1:])
        counts[key] = counts.get(key, 0) + 1
        for name, framed in key_frames.items():
            if framed[decoded].decode("utf-8") != str(row[name]):
                raise AssertionError(f"{label}: sidecar {name} disagrees with row {decoded}")
        decoded += 1
    if decoded != record.rows or expected_id != record.id_max + 1:
        raise AssertionError(f"{label}: decoded {decoded} rows, manifest says {record.rows}")
    if total != record.checksum:
        raise AssertionError(f"{label}: checksum mismatch")
    if fill_partition_counts and counts != fill_partition_counts:
        raise AssertionError(f"{label}: per-key counts differ from the generator's tally")


def _segment_entry(blob: bytes, offset: int) -> dict[str, object]:
    return {"offset": offset, "length": len(blob), "sha256": frames.sha256_hex(blob)}


def _encode_block(block: v.RowBlock, preset: Preset) -> tuple[bytes, np.ndarray]:
    """The block's records in the preset's value encoding, and each record's size."""
    if preset.value_encoding == "json":
        records = json_values.encode_records(block.avro_records(), preset.columns)
        return b"".join(records), np.array([len(record) for record in records], dtype=np.int64)
    return block.avro_records_bytes(), block.encoded_sizes


def fill_seed_batch(
    seed: int,
    value_batch: int,
    preset: Preset,
    payload_width: int,
    row_block: int,
    schedule: upsert.KeySchedule,
    keys: np.ndarray,
    start_ms: int,
) -> BatchFill:
    """Encode one row per seed key, with seed ids, as a producer batch."""
    chunks: list[bytes] = []
    sizes: list[np.ndarray] = []
    ids: list[np.ndarray] = []
    partitions: list[np.ndarray] = []
    key_values: dict[str, list[str]] = {name: [] for name in preset.kafka_key_columns}
    for start in range(0, keys.size, row_block):
        row_keys = keys[start : start + row_block]
        block_partitions = schedule.partition_of(row_keys)
        block = v.build_row_block(
            seed,
            value_batch,
            start,
            block_partitions,
            payload_width,
            start_ms,
            preset.batch_interval_ms,
            preset.columns,
            ids=-(row_keys + 1),
            replaced={schedule.key_column: schedule.key_column_block(row_keys)},
        )
        data, encoded_sizes = _encode_block(block, preset)
        chunks.append(data)
        sizes.append(encoded_sizes)
        ids.append(block.ids)
        partitions.append(block_partitions)
        for name, collected_values in key_values.items():
            collected_values.extend(v.column_strings(block, name))
    empty = np.zeros(preset.partition_count, dtype=np.int64)
    return BatchFill(
        b"".join(chunks),
        np.concatenate(sizes),
        np.concatenate(ids),
        np.concatenate(partitions),
        key_values,
        empty,
        empty,
        empty,
        upsert_keys=keys,
    )


def segment_batch(
    fill: BatchFill, kafka_partitions: np.ndarray, partition_count: int, zstd_level: int
) -> tuple[bytes, list[dict[str, object]], dict[str, bytes], dict[str, list[dict[str, object]]]]:
    """Group rows by Kafka partition, keeping row order, and compress each group on its own.

    A producer shard range-reads only its partitions, so every segment must decompress alone.
    """
    order = np.argsort(kafka_partitions, kind="stable")
    bounds = np.searchsorted(kafka_partitions[order], np.arange(partition_count + 1))
    starts = np.cumsum(fill.sizes) - fill.sizes
    buffer = np.frombuffer(fill.records, dtype=np.uint8)
    value_blobs: list[bytes] = []
    segments: list[dict[str, object]] = []
    key_blobs: dict[str, list[bytes]] = {name: [] for name in fill.key_values}
    key_segments: dict[str, list[dict[str, object]]] = {name: [] for name in fill.key_values}
    offset = 0
    key_offsets = dict.fromkeys(fill.key_values, 0)
    for partition in range(partition_count):
        rows = order[bounds[partition] : bounds[partition + 1]]
        sizes = fill.sizes[rows]
        data = buffer[np.repeat(starts[rows], sizes) + v.segment_positions(sizes)].tobytes()
        blob = frames.compress(frames.frame_stream(data, sizes), zstd_level)
        segments.append({"partition": partition, "rows": int(rows.size), **_segment_entry(blob, offset)})
        value_blobs.append(blob)
        offset += len(blob)
        for name, values in fill.key_values.items():
            key_blob = frames.compress(frames.string_frames([values[row] for row in rows.tolist()]), zstd_level)
            key_segments[name].append(_segment_entry(key_blob, key_offsets[name]))
            key_blobs[name].append(key_blob)
            key_offsets[name] += len(key_blob)
    joined_keys = {name: b"".join(blobs) for name, blobs in key_blobs.items()}
    return b"".join(value_blobs), segments, joined_keys, key_segments


def read_segment(data: bytes, segment: dict[str, object]) -> bytes:
    """Return one segment's framed rows after checking its digest.

    ``frames.decompress`` stops at the first zstd frame, so a segmented object is read one segment at a time.
    """
    start = cast(int, segment["offset"])
    blob = data[start : start + cast(int, segment["length"])]
    if frames.sha256_hex(blob) != segment["sha256"]:
        raise AssertionError(f"segment at offset {start} does not match its sha256")
    return frames.decompress(blob)


def verify_segmented_batch(
    record: BatchRecord,
    data: bytes,
    key_data: dict[str, bytes],
    preset: Preset,
    schedule: upsert.KeySchedule,
    *,
    batch_keys: np.ndarray | None = None,
) -> None:
    """Decode every segment and check ids, Kafka placement, key-derived columns and sidecars against the schedule.

    ``batch_keys`` gives each row position's key when the batch is not a stream batch.
    """
    label = f"batch {record.batch}"
    if frames.sha256_hex(data) != record.sha256:
        raise AssertionError(f"{label}: stored bytes do not match the manifest sha256")
    schema = fastavro.parse_schema(c.avro_schema(preset.columns))
    segments = cast(list[dict[str, object]], record.segments)
    key_segments = cast(dict[str, list[dict[str, object]]], record.key_segments)
    if batch_keys is None:
        batch_keys = schedule.batch_keys(record.batch)
    seen = np.zeros(record.rows, dtype=bool)
    total = 0
    for index, segment in enumerate(segments):
        rows = [
            cast(dict[str, object], json.loads(frame))
            if preset.value_encoding == "json"
            else cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
            for frame in frames.iter_frames(read_segment(data, segment))
        ]
        if len(rows) != segment["rows"]:
            raise AssertionError(f"{label}: segment {index} holds {len(rows)} rows, manifest says {segment['rows']}")
        if not rows:
            continue
        ids = np.array([cast(int, row["id"]) for row in rows], dtype=np.int64)
        positions = ids - record.id_min
        if (np.diff(ids) <= 0).any() or positions.min() < 0 or positions.max() >= record.rows:
            raise AssertionError(f"{label}: segment {index} ids are out of order or outside the batch")
        if seen[positions].any():
            raise AssertionError(f"{label}: segment {index} repeats an id")
        seen[positions] = True
        total = (total + int(np.mod(ids, c.P).sum())) % c.P
        keys = batch_keys[positions]
        if (schedule.kafka_partition_of(keys) != segment["partition"]).any():
            raise AssertionError(f"{label}: segment {index} holds a row of another Kafka partition")
        if [row[schedule.key_column] for row in rows] != schedule.tokens(keys):
            raise AssertionError(f"{label}: segment {index} key tokens disagree with the schedule")
        labels = [v.partition_label(key) for key in schedule.partition_of(keys).tolist()]
        if [row["partition_key"] for row in rows] != labels:
            raise AssertionError(f"{label}: segment {index} partition keys disagree with the schedule")
        for name, entries in key_segments.items():
            framed = [
                frame.decode("utf-8") for frame in frames.iter_frames(read_segment(key_data[name], entries[index]))
            ]
            if framed != [str(row[name]) for row in rows]:
                raise AssertionError(f"{label}: sidecar {name} disagrees with segment {index}")
    if not seen.all():
        raise AssertionError(f"{label}: {int((~seen).sum())} rows are missing from the segments")
    if total != record.checksum:
        raise AssertionError(f"{label}: checksum mismatch")


def batch_upsert_truth(schedule: upsert.KeySchedule, record: BatchRecord) -> dict[str, int]:
    """New keys, distinct keys and the checksum of each key's last version in one batch."""
    unique, first = np.unique(schedule.batch_keys(record.batch)[::-1], return_index=True)
    last_ids = record.id_min + (record.rows - 1 - first)
    return {
        "new_keys": schedule.new_per_batch,
        "distinct_keys": int(unique.size),
        "last_version_checksum": int(np.mod(last_ids, c.P).sum() % c.P),
    }


def generate(
    preset: Preset,
    out_uri: str,
    *,
    seed: int = 1,
    shard_index: int = 0,
    shard_count: int = 1,
    zstd_level: int = 3,
    row_block: int = DEFAULT_ROW_BLOCK,
) -> dict[str, object]:
    if shard_count < 1 or not 0 <= shard_index < shard_count:
        raise ValueError("shard_index must satisfy 0 <= shard_index < shard_count")
    corpus_uri = uri.join(out_uri, corpus_dir_name(preset, seed, row_block=row_block))
    sizing = v.corpus_sizing(preset, seed)
    payload_width = sizing.payload_width
    rows_per_batch_estimate = max(1, int(preset.batch_bytes / sizing.mean_budget_row_bytes))
    stride = stats_stride(rows_per_batch_estimate * preset.batch_count)
    cdf = v.zipf_cdf(preset.partition_count, preset.alpha)
    schedule = None if preset.upsert is None else upsert.KeySchedule.for_preset(preset, seed)
    column_stats = new_column_stats(preset.columns)
    records: list[BatchRecord] = []
    partition_rows = np.zeros(preset.partition_count, dtype=np.int64)
    partition_sum = np.zeros(preset.partition_count, dtype=np.int64)
    partition_encoded = np.zeros(preset.partition_count, dtype=np.int64)
    for batch in range(shard_index, preset.batch_count, shard_count):
        fill = fill_batch(
            seed, batch, preset, payload_width, cdf, row_block, column_stats, stride, rows_per_batch_estimate, schedule
        )
        segments: list[dict[str, object]] | None = None
        key_segments: dict[str, list[dict[str, object]]] | None = None
        if schedule is None or fill.upsert_keys is None:
            stored = frames.compress(frames.frame_stream(fill.records, fill.sizes), zstd_level)
            key_blobs = {
                name: frames.compress(frames.string_frames(key_column_values), zstd_level)
                for name, key_column_values in fill.key_values.items()
            }
        else:
            stored, segments, key_blobs, key_segments = segment_batch(
                fill, schedule.kafka_partition_of(fill.upsert_keys), schedule.config.kafka_partitions, zstd_level
            )
        rel = f"batches/{frames.batch_file_name(batch)}"
        uri.write_bytes(uri.join(corpus_uri, rel), stored)
        key_uris: dict[str, str] = {}
        for name, blob in key_blobs.items():
            key_rel = f"batches/{frames.key_file_name(batch, name)}"
            uri.write_bytes(uri.join(corpus_uri, key_rel), blob)
            key_uris[name] = key_rel
        rows = int(fill.ids.size)
        record = BatchRecord(
            batch=batch,
            offset_ms=batch * preset.batch_interval_ms,
            rows=rows,
            id_min=int(fill.ids[0]),
            id_max=int(fill.ids[-1]),
            checksum=closed_form_checksum(int(fill.ids[0]), int(fill.ids[-1]), rows),
            encoded_bytes=int(fill.sizes.sum()),
            stored_bytes=len(stored),
            sha256=frames.sha256_hex(stored),
            uri=rel,
            key_uris=key_uris,
            segments=segments,
            key_segments=key_segments,
        )
        # Compare the dense-ID formula with the independently accumulated checksum.
        if int(fill.partition_sum_mod.sum() % c.P) != record.checksum:
            raise AssertionError(f"batch {batch}: closed-form checksum disagrees with the accumulated residues")
        counts = {key: int(count) for key, count in enumerate(fill.partition_counts.tolist()) if count}
        if schedule is None:
            verify_batch(record, stored, key_blobs, preset.columns, counts, preset.value_encoding)
        else:
            record.upsert = batch_upsert_truth(schedule, record)
            verify_segmented_batch(record, stored, key_blobs, preset, schedule)
        records.append(record)
        partition_rows += fill.partition_counts
        partition_sum = (partition_sum + fill.partition_sum_mod) % c.P
        partition_encoded += fill.partition_encoded
    manifest_text = "".join(record.to_json() + "\n" for record in records)
    uri.write_text(uri.join(corpus_uri, "manifest.jsonl"), manifest_text)
    uri.write_text(uri.join(corpus_uri, "schema.avsc"), json.dumps(c.avro_schema(preset.columns), indent=2))
    truth = {
        v.partition_label(key): {
            "rows": int(partition_rows[key]),
            "sum_mod": int(partition_sum[key]),
            "encoded_bytes": int(partition_encoded[key]),
        }
        for key in range(preset.partition_count)
    }
    uri.write_text(uri.join(corpus_uri, "partition_truth.json"), json.dumps(truth, indent=2))
    uri.write_text(uri.join(corpus_uri, "column_stats.json"), dump_column_stats(column_stats))
    meta = finalize_corpus_json(
        preset,
        seed,
        records,
        truth,
        column_stats,
        stride,
        payload_width,
        row_block,
        zstd_level,
        shard_index,
        shard_count,
    )
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(meta, indent=2, sort_keys=True))
    return meta


def dump_column_stats(column_stats: dict[str, ColumnStats]) -> str:
    """Write mergeable sampler state beside the derived column statistics.

    Merging requires sketches, not per-shard estimates. Store the opaque sketch
    values compactly.
    """
    document = {name: stats.to_dict() for name, stats in column_stats.items()}
    return json.dumps(document, sort_keys=True, separators=(",", ":"))


def load_column_stats(document: str) -> dict[str, ColumnStats]:
    raw = cast(dict[str, object], json.loads(document))
    return {name: ColumnStats.from_dict(cast(dict[str, object], entry)) for name, entry in raw.items()}


def partition_weights(preset: Preset) -> np.ndarray:
    """The byte share each partition key is asked for, normalized over the key space."""
    weights = np.arange(1, preset.partition_count + 1, dtype=np.float64) ** -preset.alpha
    return weights / weights.sum()


def partition_share_deviation(preset: Preset, truth: dict[str, dict[str, int]], weights: np.ndarray) -> float | None:
    """Return the largest deviation from the requested partition byte shares.

    Use bytes so varying row widths cannot conceal a skew mismatch.
    """
    total = sum(entry["encoded_bytes"] for entry in truth.values())
    if total == 0:
        return None
    realized = (
        np.array(
            [truth[v.partition_label(key)]["encoded_bytes"] for key in range(preset.partition_count)], dtype=np.float64
        )
        / total
    )
    return float(np.max(np.abs(realized - weights) / weights))


def finalize_corpus_json(
    preset: Preset,
    seed: int,
    records: list[BatchRecord],
    truth: dict[str, dict[str, int]],
    column_stats: dict[str, ColumnStats],
    stride: int,
    payload_width: int,
    row_block: int,
    zstd_level: int,
    shard_index: int,
    shard_count: int,
) -> dict[str, object]:
    """Build corpus metadata and enforce corpus-wide validation gates.

    Validate before publishing ``corpus.json``. Individual shards defer these
    gates until merge, when all batches are available.
    """
    row_count = sum(record.rows for record in records)
    encoded = sum(record.encoded_bytes for record in records)
    mean_row = encoded / row_count if row_count else 0.0
    sizing = v.corpus_sizing(preset, seed) if preset.sizing_encoding is not None else None
    target = preset.target_row_bytes if sizing is None else sizing.target_row_bytes
    mean_dev = (mean_row - target) / target
    if shard_count == 1 and abs(mean_dev) > MEAN_ROW_MAX_DEVIATION:
        raise ValueError(f"mean encoded row {mean_row:.2f} deviates {mean_dev:+.2%} from target {target}")
    # Upsert rows follow their keys' partitions, so a hot key pins its share to one partition.
    schedule = None if preset.upsert is None else upsert.KeySchedule.for_preset(preset, seed)
    weights = partition_weights(preset) if schedule is None else schedule.expected_partition_weights()
    share_dev = partition_share_deviation(preset, truth, weights) if shard_count == 1 else None
    cold_rows = row_count * float(weights.min())
    share_gate_enforced = share_dev is not None and preset.alpha > 0 and cold_rows >= PARTITION_SHARE_GATE_MIN_COLD_ROWS
    if share_dev is not None and share_gate_enforced and share_dev > PARTITION_SHARE_MAX_DEVIATION:
        raise ValueError(f"partition byte share deviates {share_dev:.2%} from the Zipf weights")
    # Require every partition key even when the share gate is disabled. Hourly partitions are
    # exempt: hours no update reaches hold only seed rows.
    hourly = schedule is not None and schedule.config.partition_layout == upsert.CREATION_HOUR
    if shard_count == 1 and not hourly and any(entry["rows"] == 0 for entry in truth.values()):
        raise ValueError("a partition key received no rows")
    cardinality_devs: dict[str, float] = {}
    summaries: dict[str, object] = {}
    for column in preset.columns:
        if column.name not in column_stats:
            continue
        stats = column_stats[column.name]
        # Null cells are not sampled, so each column's expectation uses its own sample count.
        expected = expected_distinct_values(column, stats.sampled_rows)
        dev = None if expected is None else abs(stats.distinct_values() - expected) / expected
        if dev is not None:
            cardinality_devs[column.name] = dev
        summaries[column.name] = column_stats_summary(column, stats, dev)
    max_card_dev = max(cardinality_devs.values(), default=None)
    if shard_count == 1 and max_card_dev is not None and max_card_dev > CARDINALITY_MAX_DEVIATION:
        raise ValueError(f"realized column cardinality deviates {max_card_dev:.2%} from the declaration")
    # Use the minimum across unbounded blobs; one compressible blob fails the axis.
    # No unbounded blobs means there is nothing to check.
    entropies = [
        column_stats[column.name].value_byte_entropy_bits_per_byte()
        for column in preset.columns
        if column.kind == c.KIND_BLOB and column.cardinality == c.UNBOUNDED_CARDINALITY
    ]
    blob_entropy = min(entropies) if entropies else None
    if shard_count == 1 and blob_entropy is not None and blob_entropy < UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE:
        raise ValueError(
            f"unbounded blob column entropy {blob_entropy:.2f} bits/byte is below "
            f"{UNBOUNDED_BLOB_MIN_ENTROPY_BITS_PER_BYTE}"
        )
    upsert_block: dict[str, object] | None = None
    if schedule is not None and shard_count == 1:
        live = schedule.live_truth()
        new_keys = sum(cast(dict[str, int], record.upsert)["new_keys"] for record in records)
        if new_keys != live.rows - schedule.config.seed_keys:
            raise ValueError("manifest new-key counts disagree with the key schedule")
        upsert_block = {
            "rows_per_batch": schedule.rows_per_batch,
            "new_keys_per_batch": schedule.new_per_batch,
            "seed_keys": schedule.config.seed_keys,
            "kafka_partitions": schedule.config.kafka_partitions,
            "live_rows": live.rows,
            "live_checksum": live.checksum,
            "schedule_version": schedule.config.schedule_version,
        }
    manifest_sha = frames.sha256_hex("".join(record.to_json() + "\n" for record in records).encode())
    return {
        "name": preset.name,
        "corpus_hash": corpus_hash(preset, seed, row_block=row_block),
        "generator_version": GENERATOR_VERSION,
        "seed": seed,
        "effective_preset": effective_dict(preset),
        "schema": c.avro_schema(preset.columns),
        "schema_name": preset.schema_name,
        "value_encoding": preset.value_encoding,
        "column_roles": {
            role: c.role_column(preset.columns, role).name
            for role in (c.ROLE_EVENT_TIME, c.ROLE_ENTITY, c.ROLE_SUM_MEASURE, c.ROLE_PAYLOAD)
        },
        "iceberg_types": {column.name: c.iceberg_type_text(column) for column in preset.columns},
        "optional_columns": [column.name for column in preset.columns if column.nullable],
        "key_columns": list(preset.kafka_key_columns),
        "p": c.P,
        "id_block": c.ID_BLOCK,
        "batch_count": preset.batch_count,
        "batch_interval_ms": preset.batch_interval_ms,
        "offered_bytes_per_s": preset.offered_bytes_per_s if sizing is None else sizing.offered_bytes_per_s,
        "target_row_bytes": target,
        "duration_s": preset.duration_s,
        "row_count": row_count,
        "encoded_bytes": encoded,
        "stored_bytes": sum(record.stored_bytes for record in records),
        "rows_per_s": row_count / preset.duration_s if shard_count == 1 else None,
        "mean_encoded_row_size": mean_row,
        "mean_encoded_row_size_relative_deviation": mean_dev,
        "payload_width": payload_width,
        "row_block": row_block,
        "partition_rows": {key: entry["rows"] for key, entry in truth.items()},
        "partition_sum_mod": {key: entry["sum_mod"] for key, entry in truth.items()},
        "partition_encoded_bytes": {key: entry["encoded_bytes"] for key, entry in truth.items()},
        "partition_byte_share_max_relative_deviation": share_dev,
        "partition_byte_share_gate_enforced": share_gate_enforced,
        "unbounded_blob_min_entropy_bits_per_byte": blob_entropy,
        "column_stats": summaries,
        "column_stats_stride": stride,
        "column_cardinality_max_relative_deviation": max_card_dev,
        "column_cardinality_checked_columns": len(cardinality_devs),
        "zstd_level": zstd_level,
        "shard_index": shard_index,
        "shard_count": shard_count,
        "manifest_sha256": manifest_sha,
        **({} if upsert_block is None else {"upsert": upsert_block}),
    }
