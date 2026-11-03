# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Verify an upsert table's live rows at sample snapshots against the scorer's key stamps.

A reader dumps each sampled snapshot's live ``(id, key, partition_key)`` rows to Parquet:
pyiceberg here, which applies position deletes and deletion vectors, or the Spark script for
tables with equality deletes. The check then requires, at every sample and the final snapshot,
one live row per created key, holding the version its stamp names, with the key columns the
corpus derives from that row's id.
"""

from __future__ import annotations

import json
import math
import time
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import numpy as np
import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pyarrow.parquet as pq
from pyiceberg.catalog.noop import NoopCatalog
from pyiceberg.io.pyarrow import ArrowScan, schema_to_pyarrow
from pyiceberg.table import StaticTable
from pyiceberg.table.metadata import TableMetadata

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.corpus.upsert import KeySchedule, LiveTruth
from ingest_bench.scorer.geometry import open_metadata_document
from ingest_bench.scorer.snapshots import check_table_schema, snapshot_by_id, visible_snapshot_ids
from ingest_bench.scorer.table_health import snapshot_files
from ingest_bench.scorer.tally import _pyarrow_filesystem
from ingest_bench.specs.model import LATEST_PER_KEY

TERMINAL_UPSERT_FILE = "terminal-upsert.json"
CHECKPOINTS_FILE = "checkpoints.json"
KEY_MAP_DIR = "keymap"
STAMP_SCRATCH_DIR = "stamps"
FILES_IN_FLIGHT = 4
# Written last by the Spark scan: its presence marks a complete attempt.
SCAN_MANIFEST = "scan.json"
FINAL = "final"
FROZEN = "frozen"
PYICEBERG = "pyiceberg"
NO_READER = "none"
READERS = (PYICEBERG, NO_READER)

PASS = 0
ERROR = 1
DATA_FAILURE = 2

_ROW_CHUNK = 1 << 22
_FAULTS = ("invalid_rows", "duplicate_keys", "key_mismatch_rows", "stale_rows", "missing_keys")


def live_file_name(snapshot_id: int) -> str:
    return f"live-{snapshot_id}"


def scan_live_rows(
    metadata_location: str, snapshot_ids: list[int], key_column: str, out_dir: str, props: dict[str, str]
) -> dict[int, float]:
    """Write each snapshot's live key rows with pyiceberg, returning seconds per scan.

    pyiceberg applies position deletes and deletion vectors but not equality deletes; those
    tables need the Spark reader.
    """
    # StaticTable.from_metadata reads any path not ending in .metadata.json as a table root, and
    # finish.sh fetches the final metadata as table-metadata.final.json.
    metadata, io = open_metadata_document(metadata_location, props)
    table = StaticTable(
        identifier=("static-table", metadata_location),
        metadata=metadata,
        metadata_location=metadata_location,
        io=io,
        catalog=NoopCatalog("static-table"),
    )
    seconds: dict[int, float] = {}
    for snapshot_id in snapshot_ids:
        started = time.monotonic()
        filesystem, path = _pyarrow_filesystem(uri.join(out_dir, live_file_name(snapshot_id) + ".parquet"))
        filesystem.create_dir(path.rsplit("/", 1)[0], recursive=True)
        try:
            scan = table.scan(snapshot_id=snapshot_id, selected_fields=("id", key_column, "partition_key"))
            tasks = list(scan.plan_files())
            reader = ArrowScan(table.metadata, table.io, scan.projection(), scan.row_filter, scan.case_sensitive)
            # Seed files carry no field ids and stream files do, so cast every batch to one plain schema.
            projected = schema_to_pyarrow(scan.projection(), include_field_ids=False)
            target = pa.schema([field.with_nullable(True) for field in projected])
            with pq.ParquetWriter(path, target, filesystem=filesystem) as writer:
                # A few files at a time: pyiceberg's own batch reader reads every file ahead of a slow first
                # one, and the final snapshot holds every key.
                for start in range(0, len(tasks), FILES_IN_FLIGHT):
                    for batch in reader.to_record_batches(tasks[start : start + FILES_IN_FLIGHT]):
                        writer.write_table(pa.Table.from_batches([batch]).select(target.names).cast(target))
        except (ValueError, NotImplementedError) as error:
            if "equality delete" in str(error):
                raise ValueError(
                    f"snapshot {snapshot_id} holds equality deletes, which pyiceberg cannot apply; rerun finish.sh "
                    "with --verify-in-cluster, which reads it with Spark"
                ) from error
            raise
        seconds[snapshot_id] = time.monotonic() - started
    return seconds


def read_scan_manifest(live_uri: str, metadata_location: str, snapshot_ids: list[int]) -> dict[int, float]:
    """Seconds per snapshot from a Spark scan attempt, refusing an incomplete or mismatched one."""
    location = uri.join(live_uri, SCAN_MANIFEST)
    if not uri.exists(location):
        raise ValueError(f"{live_uri} has no {SCAN_MANIFEST}; the scan attempt did not finish")
    manifest = cast(dict[str, object], json.loads(uri.read_text(location)))
    if manifest.get("metadata") != metadata_location:
        raise ValueError(f"{location} scanned different metadata: {manifest.get('metadata')!r}")
    durations = cast(dict[str, object], manifest.get("snapshots", {}))
    seconds: dict[int, float] = {}
    for snapshot_id in snapshot_ids:
        if str(snapshot_id) not in durations:
            raise ValueError(f"{location} holds no scan of snapshot {snapshot_id}")
        value = durations[str(snapshot_id)]
        if not isinstance(value, int | float) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{location} records an unusable duration {value!r} for snapshot {snapshot_id}")
        seconds[snapshot_id] = float(value)
    return seconds


def _live_dataset(live_dir: str, snapshot_id: int) -> pa_dataset.Dataset:
    """A reader's output: one Parquet file (pyiceberg) or a directory of parts (Spark)."""
    for name in (live_file_name(snapshot_id) + ".parquet", live_file_name(snapshot_id)):
        location = uri.join(live_dir, name)
        if uri.exists(location):
            filesystem, path = _pyarrow_filesystem(location)
            return pa_dataset.dataset(path, filesystem=filesystem, format="parquet")
    raise FileNotFoundError(f"no live rows for snapshot {snapshot_id} under {live_dir}")


def _batches(dataset: pa_dataset.Dataset, key_column: str) -> Iterator[pa.RecordBatch]:
    yield from dataset.to_batches(columns=["id", key_column, "partition_key"], batch_size=_ROW_CHUNK)


@dataclass
class SnapshotCheck:
    """Live-state faults found at one snapshot."""

    rows: int = 0
    expected_keys: int = 0
    live_checksum: int = 0
    invalid_rows: int = 0
    duplicate_keys: int = 0
    key_mismatch_rows: int = 0
    stale_rows: int = 0
    missing_keys: int = 0

    def faults(self) -> dict[str, int]:
        return {name: cast(int, getattr(self, name)) for name in _FAULTS}

    @property
    def verdict(self) -> bool:
        return not any(self.faults().values())


def _fixed_width_matches(column: pa.Array, expected: np.ndarray) -> np.ndarray:
    """Whether each string equals its row of ``expected``, compared as bytes without building Python strings."""
    strings = cast(pa.StringArray, column.cast(pa.string()))
    rows, width = expected.shape
    offsets = np.frombuffer(strings.buffers()[1], dtype=np.int32)[strings.offset : strings.offset + rows + 1]
    data = strings.buffers()[2]
    content = np.zeros(0, dtype=np.uint8) if data is None else np.frombuffer(data, dtype=np.uint8)
    matches = np.diff(offsets) == width
    if strings.null_count:
        matches &= ~np.asarray(strings.is_null())
    if matches.any() and content.size:
        starts = offsets[:-1][matches].astype(np.int64)
        matches[matches] = (content[starts[:, None] + np.arange(width)] == expected[matches]).all(axis=1)
    result: np.ndarray = matches
    return result


def _label_matrix(partitions: np.ndarray) -> np.ndarray:
    """``partition_label`` as bytes: ``p`` and five zero-padded digits."""
    digits = (partitions[:, None] // (10 ** np.arange(4, -1, -1))) % 10 + ord("0")
    matrix: np.ndarray = np.concatenate([np.full((partitions.size, 1), ord("p")), digits], axis=1).astype(np.uint8)
    return matrix


def check_snapshot(schedule: KeySchedule, batches: Iterator[pa.RecordBatch], stamp: np.ndarray) -> SnapshotCheck:
    """Compare one snapshot's live rows with the stamps the scorer held at that snapshot.

    Seed keys are always live. A stream key is live once its first version landed, which the
    stamp records. Each live key must hold exactly its stamped version, once, with the key
    columns its id derives.
    """
    check = SnapshotCheck()
    seed_keys = schedule.config.seed_keys
    created = stamp > 0
    created[:seed_keys] = True
    check.expected_keys = int(created.sum())
    seen = np.zeros(stamp.size, dtype=np.uint8)
    matched = 0
    for batch in batches:
        ids = batch.column("id").to_numpy(zero_copy_only=False).astype(np.int64)
        check.rows += ids.size
        owners, positions = ids // c.ID_BLOCK, ids % c.ID_BLOCK
        valid = np.where(
            ids >= 0,
            (owners < schedule.batch_count) & (positions < schedule.rows_per_batch),
            -ids - 1 < seed_keys,
        )
        check.invalid_rows += int((~valid).sum())
        if not valid.any():
            continue
        selected = pa.array(valid)
        ids = ids[valid]
        tokens = batch.column(schedule.key_column).filter(selected)
        labels = batch.column("partition_key").filter(selected)
        check.live_checksum = (check.live_checksum + int(np.mod(ids, c.P).sum())) % c.P
        keys = schedule.keys_for_ids(ids)
        consistent = _fixed_width_matches(tokens, schedule.token_matrix(keys)) & _fixed_width_matches(
            labels, _label_matrix(schedule.partition_of(keys))
        )
        check.key_mismatch_rows += int((~consistent).sum())
        unique, counts = np.unique(keys, return_counts=True)
        check.duplicate_keys += int((counts - 1).sum()) + int(seen[unique].sum())
        seen[unique] = 1
        fresh = (ids == schedule.ids_for_ordinals(stamp[keys], keys)) & created[keys]
        check.stale_rows += int((~fresh).sum())
        matched += int(np.unique(keys[fresh]).size)
    check.missing_keys = check.expected_keys - matched
    return check


def _load_stamp(location: str, scratch: Path) -> np.ndarray:
    """Map a stamp checkpoint read-only, downloading a remote one to ``scratch`` first."""
    if uri.is_remote(location):
        filesystem, path = uri.filesystem_for(location)
        local = scratch / location.rsplit("/", 1)[-1]
        filesystem.get_file(path, str(local))
        location = str(local)
    stamp: np.ndarray = np.load(location, mmap_mode="r")
    return stamp


def _write_amplification(metadata: TableMetadata, seed_snapshot_id: int | None, offered_bytes: int) -> float | None:
    """Bytes of data and delete files committed after the seed snapshot, per offered byte.

    With no seed snapshot the engine wrote the seed itself, and every commit counts.
    """
    written = 0
    after_seed = seed_snapshot_id is None
    for entry in metadata.snapshot_log:
        snapshot = snapshot_by_id(metadata, entry.snapshot_id)
        if after_seed and snapshot.summary is not None and snapshot.summary["added-files-size"] is not None:
            written += int(snapshot.summary["added-files-size"])
        after_seed = after_seed or entry.snapshot_id == seed_snapshot_id
    return written / offered_bytes if offered_bytes else None


def offered_batches(checkpoints: dict[str, object], schedule: KeySchedule) -> int:
    """The batches the run offered: all of them unless the offer was shortened."""
    return cast(int, checkpoints.get("offered_batches", schedule.batch_count))


def expected_truth(meta: corpus_metadata.CorpusMetadata, offered: int) -> LiveTruth:
    """The live rows and checksum a correct table holds after ``offered`` batches."""
    schedule, truth = meta.schedule, meta.upsert_truth
    if schedule is None or truth is None:
        raise ValueError(f"{meta.uri} is not an upsert corpus")
    if offered < schedule.batch_count:
        return schedule.live_truth(offered)
    return LiveTruth(truth["live_rows"], truth["live_checksum"])


def scan_snapshot_ids(metadata_location: str, checkpoints_uri: str, props: dict[str, str]) -> list[int]:
    """The snapshots a reader must dump: every checkpoint's, plus the frozen table's current one."""
    checkpoints = cast(dict[str, object], json.loads(uri.read_text(uri.join(checkpoints_uri, CHECKPOINTS_FILE))))
    entries = cast(list[dict[str, object]], checkpoints["checkpoints"])
    document, _ = open_metadata_document(metadata_location, props)
    current = set() if document.current_snapshot_id is None else {document.current_snapshot_id}
    return sorted({cast(int, entry["snapshot_id"]) for entry in entries} | current)


@dataclass(frozen=True)
class UpsertVerifyArgs:
    metadata_location: str
    corpus_uri: str
    checkpoints_uri: str
    live_uri: str
    out_dir: Path
    catalog_props: dict[str, str]
    reader: str = PYICEBERG


def verify(args: UpsertVerifyArgs) -> dict[str, object]:
    meta = corpus_metadata.read(args.corpus_uri)
    schedule = meta.schedule
    if schedule is None or meta.upsert_truth is None:
        raise ValueError(f"{args.corpus_uri} is not an upsert corpus")
    schedule.build_key_map(args.out_dir / KEY_MAP_DIR / "keys.npy")
    checkpoints = cast(dict[str, object], json.loads(uri.read_text(uri.join(args.checkpoints_uri, CHECKPOINTS_FILE))))
    entries = cast(list[dict[str, object]], checkpoints["checkpoints"])
    absent = cast(list[dict[str, object]], checkpoints.get("absent", []))
    # Every configured sample needs a checkpoint or a recorded reason, so none can drop out silently.
    accounted = {entry["offset_s"] for entry in entries + absent if entry["offset_s"] is not None}
    unaccounted = sorted(set(cast(list[int], checkpoints.get("offsets", []))) - accounted)
    document, io = open_metadata_document(args.metadata_location, args.catalog_props)
    # A schema-only commit after drain keeps the snapshot but can still break the corpus contract.
    schema_mismatches = check_table_schema(document.schema(), meta)
    # A checkpoint must name a state readers could see. The metadata log keeps a bounded window of
    # versions, so only checkpoints inside that window can be judged here; the scorer enforces the rest.
    visible = visible_snapshot_ids(document, io, set())
    window_start = min((entry.timestamp_ms for entry in document.metadata_log), default=0)
    never_current = sorted(
        cast(int, entry["snapshot_id"])
        for entry in entries
        if cast(int, entry["timestamp_ms"]) >= window_start and entry["snapshot_id"] not in visible
    )
    final_entry = next((entry for entry in entries if entry["name"] == FINAL), None)
    if final_entry is not None and document.current_snapshot_id != final_entry["snapshot_id"]:
        # Commits after drain (late writes, compaction) change the table readers see, so verify it too.
        entries = [*entries, {**final_entry, "name": FROZEN, "snapshot_id": document.current_snapshot_id}]
    snapshot_ids = sorted({cast(int, entry["snapshot_id"]) for entry in entries})
    if args.reader == PYICEBERG:
        scan_seconds = scan_live_rows(
            args.metadata_location, snapshot_ids, schedule.key_column, args.live_uri, args.catalog_props
        )
    else:
        scan_seconds = read_scan_manifest(args.live_uri, args.metadata_location, snapshot_ids)
    seed = cast(dict[str, object], json.loads(uri.read_text(uri.join(args.corpus_uri, "seed.json"))))
    scratch = args.out_dir / STAMP_SCRATCH_DIR
    scratch.mkdir(parents=True, exist_ok=True)
    results = []
    final: SnapshotCheck | None = None
    for entry in entries:
        snapshot_id = cast(int, entry["snapshot_id"])
        stamp_name = FINAL if entry["name"] == FROZEN else entry["name"]
        stamp = _load_stamp(uri.join(args.checkpoints_uri, f"stamp-{stamp_name}.npy"), scratch)
        check = check_snapshot(
            schedule, _batches(_live_dataset(args.live_uri, snapshot_id), schedule.key_column), stamp
        )
        # The frozen snapshot, when it differs from the drained one, is the table readers see.
        if entry["name"] in (FINAL, FROZEN):
            final = check
        results.append(
            {
                "name": entry["name"],
                "offset_s": entry["offset_s"],
                "snapshot_id": snapshot_id,
                "rows": check.rows,
                "expected_keys": check.expected_keys,
                "faults": check.faults(),
                "verdict": check.verdict,
                "scan_s": scan_seconds.get(snapshot_id),
                "files": snapshot_files(document, snapshot_id, io),
            }
        )
    offered = offered_batches(checkpoints, schedule)
    truth = expected_truth(meta, offered)
    checksum = None if final is None else {"expected": truth.checksum, "found": final.live_checksum}
    verdict = (
        final is not None
        and not schema_mismatches
        and not unaccounted
        and not never_current
        and all(cast(bool, result["verdict"]) for result in results)
        and final.live_checksum == truth.checksum
        and final.rows == truth.rows
    )
    offered_bytes = sum(
        record.encoded_bytes for record in corpus_metadata.read_manifest(args.corpus_uri) if record.batch < offered
    )
    # A Kafka seed is offered to the engine too; a registered seed snapshot is not the engine's work.
    seed_snapshot_id = cast(int, seed["snapshot_id"]) if "snapshot_id" in seed else None
    if seed_snapshot_id is None:
        offered_bytes += cast(int, seed["encoded_bytes"])
    return {
        "mode": LATEST_PER_KEY,
        "reader": args.reader,
        "verdict": verdict,
        "snapshots": results,
        "absent": absent,
        "unaccounted_offsets": unaccounted,
        "schema_mismatches": schema_mismatches,
        "never_current": never_current,
        "live_checksum": checksum,
        "write_amplification": _write_amplification(document, seed_snapshot_id, offered_bytes),
    }


def run(args: UpsertVerifyArgs) -> int:
    """Write ``terminal-upsert.json`` and return 0 when it passes, 2 when it fails, 1 on error."""
    args.out_dir.mkdir(parents=True, exist_ok=True)
    path = args.out_dir / TERMINAL_UPSERT_FILE
    try:
        result = verify(args)
    except Exception as error:
        path.write_text(
            json.dumps({"mode": LATEST_PER_KEY, "verdict": False, "error": f"{type(error).__name__}: {error}"}) + "\n"
        )
        raise
    path.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return PASS if result["verdict"] else DATA_FAILURE
