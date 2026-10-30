# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""A tiny upsert corpus with its seed, and a toy engine that upserts it into a pyiceberg table."""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pyarrow as pa
from pyiceberg.expressions import AlwaysFalse, BooleanExpression, In, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.table import Table

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import generate, preset, seed, values
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.table import create

ROOT = Path(__file__).resolve().parents[1]
SETS = ["offered_bytes_per_s=100KB", "duration_s=4", "partition_count=4", "upsert.seed_keys=2000"]


KAFKA_SEED_SETS = ("upsert.seed_delivery=kafka", "upsert.key_layout=time_ordered", "upsert.seed_history_s=600")


def build_upsert_corpus(root: Path, *extra: str) -> corpus_metadata.CorpusMetadata:
    p = preset.load_preset("smoke-upsert", workloads_dir=ROOT / "workloads", overrides=[*SETS, *extra])
    generate.generate(p, str(root), seed=5, row_block=64)
    corpus_uri = uri.join(str(root), preset.corpus_dir_name(p, seed=5, row_block=64))
    seed.write_seed_shard(corpus_uri, 0, 1, target_file_bytes=1 << 20)
    seed.commit_seed(corpus_uri, target_file_bytes=1 << 20)
    return corpus_metadata.read(corpus_uri)


def seed_metadata_location(meta: corpus_metadata.CorpusMetadata) -> str:
    return str(json.loads(uri.read_text(uri.join(meta.uri, seed.SEED_FILE)))["metadata_location"])


def create_empty(props: dict[str, str], name: str, meta: corpus_metadata.CorpusMetadata, warehouse: Path) -> Table:
    """The empty keyed table a Kafka-seeded run starts from."""
    return create.create_upsert_table(
        props,
        name,
        meta,
        create.parse_partition(create.DEFAULT_SEED_PARTITION),
        {},
        location=f"file://{warehouse}/{name.replace('.', '/')}",
    )


def register(props: dict[str, str], name: str, meta: corpus_metadata.CorpusMetadata, warehouse: Path) -> Table:
    return create.register_seed_table(
        props, name, meta, seed_metadata_location(meta), {}, location=f"file://{warehouse}/{name.replace('.', '/')}"
    )


def version_rows(meta: corpus_metadata.CorpusMetadata, ids: np.ndarray) -> pa.Table:
    """Rows for the given ids with their true key columns; other columns are typed constants."""
    schedule = meta.schedule
    assert schedule is not None
    keys = schedule.keys_for_ids(ids)
    n = int(ids.size)
    columns: dict[str, pa.Array] = {
        "id": pa.array(ids, pa.int64()),
        schedule.key_column: pa.array(schedule.tokens(keys)),
        "partition_key": pa.array([values.partition_label(k) for k in schedule.partition_of(keys).tolist()]),
    }
    for column in meta.columns:
        if column.name in columns:
            continue
        columns[column.name] = {
            "long": pa.array(np.zeros(n, np.int64)),
            "string": pa.array(["x"] * n),
            "double": pa.array(np.zeros(n)),
            "boolean": pa.array([True] * n),
            "timestamp": pa.array(np.zeros(n, np.int64)).cast(pa.timestamp("us")),
            "binary": pa.array([b"x"] * n, pa.binary()),
        }[c.iceberg_type_text(column)]
    order = meta.field_names()
    schema = pa.schema([pa.field(name, columns[name].type, nullable=False) for name in order])
    return pa.table({name: columns[name] for name in order}).cast(schema)


class ToyEngine:
    """Upsert batches by copy-on-write: delete each key's live version, append the new one."""

    def __init__(self, table: Table, meta: corpus_metadata.CorpusMetadata) -> None:
        assert meta.schedule is not None
        self.table, self.meta, self.schedule = table, meta, meta.schedule
        self.live = {key: -(key + 1) for key in range(self.schedule.config.seed_keys)}

    def batch_versions(self, batch: int, *, collapse: bool) -> np.ndarray:
        keys = self.schedule.batch_keys(batch)
        positions = np.arange(keys.size)
        if collapse:
            _, first = np.unique(keys[::-1], return_index=True)
            positions = np.sort(keys.size - 1 - first)
        return batch * c.ID_BLOCK + positions.astype(np.int64)

    def upsert(self, ids: np.ndarray) -> None:
        """Commit ``ids`` as one overwrite, leaving one live row per key, at its newest version."""
        keys = self.schedule.keys_for_ids(ids)
        newest: dict[int, int] = {}
        for key, row_id in zip(keys.tolist(), ids.tolist(), strict=True):
            newest[key] = max(newest.get(key, row_id), row_id)
        superseded = [self.live[key] for key in newest if key in self.live]
        self.live.update(newest)
        rows = version_rows(self.meta, np.array(sorted(newest.values()), dtype=np.int64))
        condition: BooleanExpression = (
            In(term=Reference("id"), values={literal(row_id) for row_id in superseded}) if superseded else AlwaysFalse()
        )
        self.table.overwrite(rows, overwrite_filter=condition)
        self.table.refresh()

    def write_seed(self) -> None:
        """Commit every seed key's row, as an engine loading a Kafka seed does before the offer."""
        self.table.append(version_rows(self.meta, -(np.arange(self.schedule.config.seed_keys, dtype=np.int64) + 1)))
        self.table.refresh()

    def append(self, ids: np.ndarray) -> None:
        """A broken engine's commit: new versions land, superseded ones stay live."""
        keys = self.schedule.keys_for_ids(ids)
        for key, row_id in zip(keys.tolist(), ids.tolist(), strict=True):
            self.live[key] = max(self.live.get(key, row_id), row_id)
        self.table.append(version_rows(self.meta, ids))
        self.table.refresh()

    def dedupe(self) -> None:
        """Repair the table in one commit: delete every row that is not its key's newest version."""
        present = self.table.scan(selected_fields=("id",)).to_arrow().column("id").to_numpy()
        newest = set(self.live.values())
        stale = [int(row_id) for row_id in present if int(row_id) not in newest]
        if stale:
            self.table.delete(In(term=Reference("id"), values={literal(row_id) for row_id in stale}))
            self.table.refresh()
