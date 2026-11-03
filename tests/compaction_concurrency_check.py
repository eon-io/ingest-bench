# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Scan every retained snapshot and compare complete rows with the corpus.

This helper runs inside the real harness container. It is intentionally test
only: production terminal validation independently proves exact final IDs,
while this small qualification can afford full-row checks at every commit.
"""

from __future__ import annotations

import argparse
import io
import json
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import fastavro
import numpy as np
import pyarrow as pa

from ingest_bench import uri
from ingest_bench.corpus import frames, metadata
from ingest_bench.scorer.geometry import data_files_at
from ingest_bench.scorer.snapshots import load_table, read_metadata, snapshots_in_order

CATALOG_PROPS = {
    "uri": "http://iceberg-rest:8181",
    "warehouse": "s3://warehouse/",
    "s3.endpoint": "http://minio:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}


def _expected_batches(corpus_uri: str, schema: pa.Schema) -> tuple[pa.Table, np.ndarray, np.ndarray, np.ndarray]:
    """Decode the frozen corpus into the table's exact Arrow projection."""
    corpus = metadata.read(corpus_uri)
    manifest = metadata.read_manifest(corpus_uri)
    avro = fastavro.parse_schema(json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc"))))
    tables: list[pa.Table] = []
    ids: list[np.ndarray] = []
    for record in manifest:
        blob = frames.decompress(uri.read_bytes(record.uri))
        rows = [
            cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), avro))
            for frame in frames.iter_frames(blob)
        ]
        batch = pa.Table.from_pylist(rows, schema=schema)
        tables.append(batch)
        ids.append(np.asarray(batch.column("id").combine_chunks().to_numpy(zero_copy_only=False), dtype=np.int64))
    expected = pa.concat_tables(tables) if tables else pa.table({name: [] for name in schema.names}, schema=schema)
    expected_ids = np.concatenate(ids) if ids else np.empty(0, dtype=np.int64)
    if expected.num_rows != corpus.row_count:
        raise AssertionError(f"decoded {expected.num_rows} corpus rows, metadata declares {corpus.row_count}")
    if expected_ids.size > 1 and bool(np.any(expected_ids[1:] <= expected_ids[:-1])):
        raise AssertionError("corpus manifest rows must have strictly increasing IDs")
    batch_rows = np.array([record.rows for record in manifest], dtype=np.int64)
    batch_starts = np.cumsum(batch_rows, dtype=np.int64) - batch_rows
    id_min = np.array([record.id_min for record in manifest], dtype=np.int64)
    id_max = np.array([record.id_max for record in manifest], dtype=np.int64)
    return expected, expected_ids, batch_starts, np.stack((id_min, id_max))


def _positions(ids: np.ndarray, batch_starts: np.ndarray, id_bounds: np.ndarray, id_block: int) -> np.ndarray:
    if ids.size == 0:
        return np.empty(0, dtype=np.int64)
    batches = ids // id_block
    if int(batches.min()) < 0 or int(batches.max()) >= batch_starts.size:
        raise AssertionError(
            f"table holds an ID from an unexpected batch: {ids[(batches < 0) | (batches >= batch_starts.size)][0]}"
        )
    starts = id_bounds[0, batches]
    ends = id_bounds[1, batches]
    valid = (ids >= starts) & (ids <= ends)
    if not bool(valid.all()):
        raise AssertionError(f"table holds unexpected ID {ids[~valid][0]}")
    return cast(np.ndarray, batch_starts[batches] + ids - starts)


def _scan(args: argparse.Namespace) -> dict[str, object]:
    facts = cast(dict[str, object], json.loads(Path(args.facts).read_text()))
    # A cluster run's facts name its own catalog; the constant is the local qualification fixture.
    catalog_props = cast(dict[str, str], facts.get("catalog_props") or CATALOG_PROPS)
    table = load_table(catalog_props, str(facts["table"]))
    document = read_metadata(table)
    ordered = snapshots_in_order(document)
    if not ordered:
        raise AssertionError("table has no snapshots")

    corpus = metadata.read(str(facts["corpus_uri"]))
    field_names = corpus.field_names()
    first = table.scan(snapshot_id=ordered[0].snapshot_id, selected_fields=tuple(field_names)).to_arrow()
    projection_schema = first.schema
    expected, expected_ids, batch_starts, id_bounds = _expected_batches(str(facts["corpus_uri"]), projection_schema)

    previous_ids = np.empty(0, dtype=np.int64)
    previous_files: dict[str, str] = {}
    reports: list[dict[str, object]] = []
    for info in ordered:
        # PyIceberg infers large_string for empty scans and string for populated
        # scans. An engine's first checkpoint may be empty, so cast before comparing.
        actual = (
            table.scan(snapshot_id=info.snapshot_id, selected_fields=tuple(field_names))
            .to_arrow()
            .cast(projection_schema)
            .sort_by("id")
        )
        actual_ids = np.asarray(actual.column("id").combine_chunks().to_numpy(zero_copy_only=False), dtype=np.int64)
        if np.unique(actual_ids).size != actual_ids.size:
            raise AssertionError(f"snapshot {info.snapshot_id} holds duplicate row IDs")
        positions = _positions(actual_ids, batch_starts, id_bounds, corpus.id_block)
        expected_visible = expected.take(pa.array(positions, type=pa.int64()))
        if not actual.equals(expected_visible, check_metadata=False):
            raise AssertionError(f"snapshot {info.snapshot_id} changed one or more complete row values")
        missing_previous = np.setdiff1d(previous_ids, actual_ids, assume_unique=True)
        if missing_previous.size:
            raise AssertionError(f"snapshot {info.snapshot_id} lost previously visible ID {int(missing_previous[0])}")
        if info.operation != "append" and not np.array_equal(actual_ids, previous_ids):
            raise AssertionError(
                f"rewrite snapshot {info.snapshot_id} changed the live ID multiset "
                f"from {previous_ids.size} to {actual_ids.size} rows"
            )
        current_files = {file.path: file.partition for file in data_files_at(document, info.snapshot_id, table.io)}
        reports.append(
            {
                "snapshot_id": info.snapshot_id,
                "parent_id": info.parent_id,
                "timestamp_ms": info.timestamp_ms,
                "operation": info.operation,
                "rows": int(actual_ids.size),
                "added_partitions": sorted(
                    {partition for path, partition in current_files.items() if path not in previous_files}
                ),
                "removed_partitions": sorted(
                    {partition for path, partition in previous_files.items() if path not in current_files}
                ),
            }
        )
        previous_ids = actual_ids
        previous_files = current_files

    if not np.array_equal(previous_ids, expected_ids):
        missing = np.setdiff1d(expected_ids, previous_ids, assume_unique=True)
        unexpected = np.setdiff1d(previous_ids, expected_ids, assume_unique=True)
        raise AssertionError(
            f"final snapshot differs from offered corpus: missing={missing[:3].tolist()} "
            f"unexpected={unexpected[:3].tolist()}"
        )
    return {
        "table": str(facts["table"]),
        "table_uuid": str(document.table_uuid),
        "metadata_uri": table.metadata_location,
        "final_snapshot_id": ordered[-1].snapshot_id,
        "snapshot_count": len(ordered),
        "expected_rows": int(expected_ids.size),
        "full_row_multisets_equal": True,
        "visible_ids_monotonic": True,
        "final_ids_exact": True,
        "snapshots": reports,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--facts", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args(argv)
    report = _scan(args)
    path = Path(args.out)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(f"validated {report['snapshot_count']} snapshots and {report['expected_rows']} complete rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
