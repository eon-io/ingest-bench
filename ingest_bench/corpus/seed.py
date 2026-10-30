# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Write the seed an upsert run starts from.

Every seed key holds one row whose id, ``-(key + 1)``, sorts below every stream id, so
engines that keep the largest id per key replace it.

A table seed is a Parquet table the harness writes. Shards split the table partitions, so
each partition's files come from one writer and roll at the declared target size. The commit
registers the files through a throwaway SQLite catalog; runs copy the resulting metadata.

A Kafka seed is a sequence of producer batches the engine writes itself before the scored
offer, laid out like stream batches under ``seed/`` with its own manifest.
"""

from __future__ import annotations

import json
import tempfile
from typing import IO, cast

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.io.pyarrow import schema_to_pyarrow

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import frames
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.corpus import values as v
from ingest_bench.corpus.generate import (
    BatchRecord,
    closed_form_checksum,
    epoch_ms,
    fill_seed_batch,
    segment_batch,
    verify_segmented_batch,
)
from ingest_bench.corpus.preset import Preset, preset_from_effective
from ingest_bench.corpus.upsert import SEED_KAFKA, KeySchedule
from ingest_bench.table.create import DEFAULT_SEED_PARTITION, iceberg_schema, parse_partition, partition_spec

SEED_DIR = corpus_metadata.SEED_DIR
SEED_FILE = corpus_metadata.SEED_FILE
FSSPEC_FILE_IO = "pyiceberg.io.fsspec.FsspecFileIO"
DEFAULT_TARGET_FILE_BYTES = 128 * 2**20
SEED_BLOCK = 65_536
_KEY_CHUNK = 1 << 24
_FLAT_KINDS = frozenset(
    {
        c.KIND_RESERVED,
        c.KIND_CATEGORICAL,
        c.KIND_INTEGER,
        c.KIND_INT32,
        c.KIND_DECIMAL,
        c.KIND_FLOAT32,
        c.KIND_BOOLEAN,
        c.KIND_TIMESTAMP,
        c.KIND_BLOB,
    }
)


def check_seed_columns(columns: tuple[c.ColumnDistribution, ...]) -> None:
    unsupported = [column.name for column in columns if column.kind not in _FLAT_KINDS]
    if unsupported:
        raise ValueError(f"the seed writer supports flat scalar schemas; unsupported columns: {unsupported}")


def _seed_batch(partition: int) -> int:
    """A value-stream batch index per seed partition, disjoint from stream batches."""
    return -(partition + 1)


def _schedule(meta: corpus_metadata.CorpusMetadata) -> KeySchedule:
    if meta.schedule is None:
        raise ValueError(f"{meta.uri} is not an upsert corpus")
    return meta.schedule


def keys_by_partition(schedule: KeySchedule, partitions: list[int]) -> dict[int, np.ndarray]:
    """Collect each wanted partition's seed keys in ascending order."""
    collected: dict[int, list[np.ndarray]] = {partition: [] for partition in partitions}
    for start in range(0, schedule.config.seed_keys, _KEY_CHUNK):
        keys = np.arange(start, min(schedule.config.seed_keys, start + _KEY_CHUNK), dtype=np.int64)
        owners = schedule.partition_of(keys)
        order = np.argsort(owners, kind="stable")
        bounds = np.searchsorted(owners[order], np.arange(int(owners.max()) + 2))
        for partition in partitions:
            if partition + 1 < bounds.size:
                collected[partition].append(keys[order[bounds[partition] : bounds[partition + 1]]])
    return {
        partition: np.concatenate(parts) if parts else np.empty(0, dtype=np.int64)
        for partition, parts in collected.items()
    }


def _arrow_table(block: v.RowBlock, schema: pa.Schema, columns: tuple[c.ColumnDistribution, ...]) -> pa.Table:
    arrays = []
    for column, field in zip(columns, schema, strict=True):
        cells = block.values[column.name]
        if column.kind == c.KIND_TIMESTAMP:
            # Corpus timestamps are epoch milliseconds; Iceberg timestamps are microseconds.
            arrays.append(pa.array(np.asarray(cells, dtype=np.int64) * 1000, type=field.type))
        else:
            arrays.append(pa.array(cells, type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


class _RollingFile:
    """A Parquet file under one partition directory that closes once it reaches the target size."""

    def __init__(self, file_uri: str, schema: pa.Schema) -> None:
        filesystem, path = uri.filesystem_for(file_uri)
        filesystem.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
        self.uri = file_uri
        self.rows = 0
        self._sink = cast(IO[bytes], filesystem.open(path, "wb"))
        self._writer = pq.ParquetWriter(self._sink, schema, compression="zstd")

    def write(self, table: pa.Table) -> None:
        self._writer.write_table(table)
        self.rows += table.num_rows

    @property
    def size(self) -> int:
        return self._sink.tell()

    def close(self, partition: int) -> dict[str, object]:
        self._writer.close()
        size = self._sink.tell()
        self._sink.close()
        return {"uri": self.uri, "partition": partition, "rows": self.rows, "bytes": size}


def seed_batch_count(schedule: KeySchedule) -> int:
    return -(-schedule.config.seed_keys // schedule.rows_per_batch)


def seed_batch_keys(schedule: KeySchedule, batch: int) -> np.ndarray:
    """The seed keys of one Kafka seed batch, in row order.

    Batches take keys in creation order. Within a batch the rows run newest first, so their
    ids ascend like a stream batch's.
    """
    low = batch * schedule.rows_per_batch
    high = min(low + schedule.rows_per_batch, schedule.config.seed_keys)
    return np.arange(high - 1, low - 1, -1, dtype=np.int64)


def _write_seed_batches(
    corpus_uri: str, shard_index: int, shard_count: int, schedule: KeySchedule, preset: Preset, raw: dict[str, object]
) -> list[dict[str, object]]:
    """Write every Kafka seed batch this shard owns, segmented by Kafka partition as stream batches are."""
    meta_seed = c.as_int_value(raw["seed"])
    payload_width = c.as_int_value(raw["payload_width"])
    row_block = c.as_int_value(raw["row_block"])
    zstd_level = c.as_int_value(raw["zstd_level"])
    written: list[dict[str, object]] = []
    for batch in range(shard_index, seed_batch_count(schedule), shard_count):
        keys = seed_batch_keys(schedule, batch)
        start_ms = int(schedule.creation_ms(keys[-1:])[0])
        fill = fill_seed_batch(meta_seed, -(batch + 1), preset, payload_width, row_block, schedule, keys, start_ms)
        stored, segments, key_blobs, key_segments = segment_batch(
            fill, schedule.kafka_partition_of(keys), schedule.config.kafka_partitions, zstd_level
        )
        rel = f"batches/{frames.batch_file_name(batch)}"
        uri.write_bytes(uri.join(corpus_uri, SEED_DIR, rel), stored)
        key_uris: dict[str, str] = {}
        for name, blob in key_blobs.items():
            key_rel = f"batches/{frames.key_file_name(batch, name)}"
            uri.write_bytes(uri.join(corpus_uri, SEED_DIR, key_rel), blob)
            key_uris[name] = key_rel
        rows = int(keys.size)
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
        verify_segmented_batch(record, stored, key_blobs, preset, schedule, batch_keys=keys)
        written.append(json.loads(record.to_json()))
    return written


def write_seed_shard(
    corpus_uri: str, shard_index: int, shard_count: int, target_file_bytes: int = DEFAULT_TARGET_FILE_BYTES
) -> list[dict[str, object]]:
    """Write this shard's part of the seed: table partitions for a table seed, batches for a Kafka seed."""
    if uri.exists(uri.join(corpus_uri, SEED_FILE)):
        # Shard file names repeat across attempts: rewriting them would change every run table's seed snapshot.
        raise ValueError(f"{corpus_uri} already published its seed; build a new corpus to change it")
    meta = corpus_metadata.read(corpus_uri)
    schedule = _schedule(meta)
    check_seed_columns(meta.columns)
    raw = cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_uri, "corpus.json"))))
    preset = preset_from_effective(cast(dict[str, object], raw["effective_preset"]), meta.schema_name)
    if schedule.config.seed_delivery == SEED_KAFKA:
        batches = _write_seed_batches(corpus_uri, shard_index, shard_count, schedule, preset, raw)
        manifest = {"shard_index": shard_index, "shard_count": shard_count, "records": batches}
        uri.write_text(uri.join(corpus_uri, SEED_DIR, "shards", f"{shard_index}.json"), json.dumps(manifest))
        return batches
    payload_width = c.as_int_value(raw["payload_width"])
    schema = schema_to_pyarrow(iceberg_schema(meta), include_field_ids=False)
    # Seed rows predate the stream, so their event times fall in the interval before the first batch.
    seed_start_ms = epoch_ms(preset) - preset.batch_interval_ms
    partitions = list(range(shard_index, preset.partition_count, shard_count))
    written: list[dict[str, object]] = []
    for partition, keys in keys_by_partition(schedule, partitions).items():
        label = v.partition_label(partition)
        current: _RollingFile | None = None
        files = 0
        for start in range(0, keys.size, SEED_BLOCK):
            chunk = keys[start : start + SEED_BLOCK]
            if current is None:
                name = f"s{shard_index}-{files:05d}.parquet"
                current = _RollingFile(uri.join(corpus_uri, SEED_DIR, "data", f"partition_key={label}", name), schema)
                files += 1
            block = v.build_row_block(
                meta.seed,
                _seed_batch(partition),
                start,
                np.full(chunk.size, partition, dtype=np.int64),
                payload_width,
                seed_start_ms,
                preset.batch_interval_ms,
                meta.columns,
                ids=-(chunk + 1),
                replaced={schedule.key_column: schedule.key_column_block(chunk)},
            )
            current.write(_arrow_table(block, schema, meta.columns))
            if current.size >= target_file_bytes:
                written.append(current.close(partition))
                current = None
        if current is not None:
            written.append(current.close(partition))
    manifest = {"shard_index": shard_index, "shard_count": shard_count, "files": written}
    uri.write_text(uri.join(corpus_uri, SEED_DIR, "shards", f"{shard_index}.json"), json.dumps(manifest))
    return written


def _shard_manifests(corpus_uri: str) -> list[dict[str, object]]:
    shards_uri = uri.join(corpus_uri, SEED_DIR, "shards")
    manifests = [
        cast(dict[str, object], json.loads(uri.read_text(uri.join(shards_uri, name.rsplit("/", 1)[-1]))))
        for name in sorted(uri.listdir(shards_uri))
    ]
    counts = {c.as_int_value(manifest["shard_count"]) for manifest in manifests}
    indices = sorted(c.as_int_value(manifest["shard_index"]) for manifest in manifests)
    if len(counts) != 1 or indices != list(range(counts.pop())):
        raise ValueError(f"seed shard manifests are incomplete or mixed: indices {indices}")
    return manifests


def _commit_seed_batches(corpus_uri: str, schedule: KeySchedule) -> dict[str, object]:
    """Publish the Kafka seed's manifest, in send order, and ``seed.json``."""
    records = sorted(
        (
            BatchRecord(**cast(dict[str, object], entry))  # type: ignore[arg-type]
            for manifest in _shard_manifests(corpus_uri)
            for entry in cast(list[object], manifest["records"])
        ),
        key=lambda record: record.batch,
    )
    if [record.batch for record in records] != list(range(seed_batch_count(schedule))):
        raise ValueError(f"the shards hold seed batches {[record.batch for record in records]}, not every one")
    rows = sum(record.rows for record in records)
    if rows != schedule.config.seed_keys:
        raise ValueError(f"the shards hold {rows} seed rows, and the corpus has {schedule.config.seed_keys} seed keys")
    uri.write_text(
        uri.join(corpus_uri, SEED_DIR, "manifest.jsonl"), "".join(record.to_json() + "\n" for record in records)
    )
    facts: dict[str, object] = {
        "delivery": SEED_KAFKA,
        "rows": rows,
        "batches": len(records),
        "encoded_bytes": sum(record.encoded_bytes for record in records),
    }
    uri.write_text(uri.join(corpus_uri, SEED_FILE), json.dumps(facts, indent=2, sort_keys=True))
    return facts


def commit_seed(corpus_uri: str, target_file_bytes: int = DEFAULT_TARGET_FILE_BYTES) -> dict[str, object]:
    """Publish the seed with ``seed.json``: one registered snapshot for a table seed, a manifest for a Kafka seed."""
    meta = corpus_metadata.read(corpus_uri)
    schedule = _schedule(meta)
    if schedule.config.seed_delivery == SEED_KAFKA:
        return _commit_seed_batches(corpus_uri, schedule)
    manifests = _shard_manifests(corpus_uri)
    files = [
        cast(dict[str, object], entry) for manifest in manifests for entry in cast(list[object], manifest["files"])
    ]
    rows = sum(c.as_int_value(entry["rows"]) for entry in files)
    if rows != schedule.config.seed_keys:
        raise ValueError(f"the shards hold {rows} seed rows, and the corpus has {schedule.config.seed_keys} seed keys")
    partition_rows: dict[str, int] = {}
    for entry in files:
        label = v.partition_label(c.as_int_value(entry["partition"]))
        partition_rows[label] = partition_rows.get(label, 0) + c.as_int_value(entry["rows"])
    with tempfile.TemporaryDirectory() as scratch:
        # fsspec honours the container's AWS_ENDPOINT_URL, as the corpus writer does; pyarrow's file IO does not.
        catalog = SqlCatalog(
            "seed",
            uri=f"sqlite:///{scratch}/seed.db",
            warehouse=uri.join(corpus_uri, SEED_DIR),
            **{"py-io-impl": FSSPEC_FILE_IO},
        )
        try:
            catalog.create_namespace("seed")
            table = catalog.create_table(
                ("seed", "table"),
                schema=iceberg_schema(meta),
                partition_spec=partition_spec(meta, parse_partition(DEFAULT_SEED_PARTITION)),
                location=uri.join(corpus_uri, SEED_DIR, "table"),
                properties={
                    "write.target-file-size-bytes": str(target_file_bytes),
                    "write.parquet.compression-codec": "zstd",
                },
            )
            table.add_files([str(entry["uri"]) for entry in files])
            table = catalog.load_table(("seed", "table"))
        finally:
            catalog.engine.dispose()
    snapshot = table.current_snapshot()
    if snapshot is None:
        raise ValueError("the seed commit produced no snapshot")
    facts: dict[str, object] = {
        "metadata_location": table.metadata_location,
        "snapshot_id": snapshot.snapshot_id,
        "rows": rows,
        "files": len(files),
        "target_file_bytes": target_file_bytes,
        "partition_rows": dict(sorted(partition_rows.items())),
    }
    uri.write_text(uri.join(corpus_uri, SEED_FILE), json.dumps(facts, indent=2, sort_keys=True))
    return facts
