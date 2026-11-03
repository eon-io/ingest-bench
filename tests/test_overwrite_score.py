# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Append delivery committed with position deletes, independently of engine summaries."""

from __future__ import annotations

import io
import json
import zlib
from dataclasses import replace
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestContent,
    ManifestEntry,
    ManifestEntryStatus,
    ManifestWriterV2,
    write_manifest_list,
)
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadataUtil
from pyiceberg.table.snapshots import Operation, Summary
from pyiceberg.typedef import Record
from pyroaring import BitMap

from ingest_bench.catalog import open_catalog
from ingest_bench.corpus import metadata
from ingest_bench.scorer import score, terminal
from ingest_bench.scorer.snapshots import added_files, read_metadata
from ingest_bench.table import create
from tests.test_score_loop import StepClock, _finished_producer, _props
from tests.test_terminal import _all_rows, _run
from tests.test_terminal import corpus as corpus


class DeleteManifestWriter(ManifestWriterV2):
    def content(self) -> ManifestContent:
        return ManifestContent.DELETES

    @property
    def _meta(self) -> dict[str, str]:
        return {**super()._meta, "content": "deletes"}


def _mask(
    table: Table, root: Path, path: str, position: int, *, vector: bool, second: tuple[str, int] | None = None
) -> None:
    snapshot = table.current_snapshot()
    assert snapshot is not None
    if vector:
        payload = b"PFA1"
        blobs: list[dict[str, object]] = []
        for target, pos in [(path, position), *([] if second is None else [second])]:
            bitmap = (1).to_bytes(8, "little") + (0).to_bytes(4, "little") + BitMap([pos]).serialize()
            body = bytes.fromhex("d1d33964") + bitmap
            blob = len(body).to_bytes(4, "big") + body + zlib.crc32(body).to_bytes(4, "big")
            blobs.append(
                {
                    "type": "deletion-vector-v1",
                    "fields": [2147483545],
                    "snapshot-id": snapshot.snapshot_id,
                    "sequence-number": snapshot.sequence_number,
                    "offset": len(payload),
                    "length": len(blob),
                    "properties": {"referenced-data-file": target, "cardinality": "1"},
                }
            )
            payload += blob
        footer = json.dumps({"blobs": blobs}).encode()
        delete_path = root / "delete.puffin"
        delete_path.write_bytes(payload + b"PFA1" + footer + len(footer).to_bytes(4, "little") + bytes(4) + b"PFA1")
    else:
        delete_path = root / "delete.parquet"
        pq.write_table(pa.table({"file_path": [path], "pos": pa.array([position], pa.int64())}), delete_path)
    delete = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=str(delete_path),
        file_format=FileFormat.PUFFIN if vector else FileFormat.PARQUET,
        partition=Record(),
        record_count=1,
        file_size_in_bytes=delete_path.stat().st_size,
    )
    delete.spec_id = 0
    delete_manifest = root / "delete.avro"
    with DeleteManifestWriter(
        table.spec(), table.schema(), table.io.new_output(str(delete_manifest)), snapshot.snapshot_id, "null"
    ) as writer:
        writer.add(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED,
                snapshot_id=snapshot.snapshot_id,
                sequence_number=None,
                file_sequence_number=None,
                data_file=delete,
            )
        )
        if second is not None:
            writer.add(
                ManifestEntry.from_args(
                    status=ManifestEntryStatus.ADDED,
                    snapshot_id=snapshot.snapshot_id,
                    sequence_number=None,
                    file_sequence_number=None,
                    data_file=delete,
                )
            )
    manifest_list = root / "manifest-list.avro"
    with write_manifest_list(
        2,
        table.io.new_output(str(manifest_list)),
        snapshot.snapshot_id,
        snapshot.parent_snapshot_id,
        snapshot.sequence_number,
        "null",
    ) as listing:
        listing.add_manifests([*snapshot.manifests(table.io), writer.to_manifest_file()])
    changed = snapshot.model_copy(update={"manifest_list": str(manifest_list), "summary": Summary(Operation.OVERWRITE)})
    document = read_metadata(table).model_dump(mode="json", by_alias=True)
    document["snapshots"] = [
        item.model_dump(mode="json", by_alias=True) for item in [*table.metadata.snapshots[:-1], changed]
    ]
    if vector:
        document["format-version"] = 3
        document["next-row-id"] = 0
    table.metadata = TableMetadataUtil.parse_obj(document)
    saved = root / "final.metadata.json"
    saved.write_text(json.dumps(document), encoding="utf-8")
    table.metadata_location = str(saved)


def _recovery_table(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    *,
    vector: bool,
    lose: bool = False,
    mask_position: int | None = None,
) -> Table:
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), "bench.recovery", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_all_rows(records[:1], corpus))
    incoming = _all_rows(records[1:], corpus)
    incoming = pa.concat_tables([incoming, _all_rows(records[:1], corpus).slice(0, 1)])
    table.append(incoming)
    snapshot = table.current_snapshot()
    assert snapshot is not None
    file = added_files(read_metadata(table), snapshot.snapshot_id, table.io)[0]
    position = (0 if lose else len(incoming) - 1) if mask_position is None else mask_position
    _mask(table, tmp_path, file.path, position, vector=vector)
    return table


def _state(
    tmp_path: Path, corpus: metadata.CorpusMetadata, table: Table, monkeypatch: pytest.MonkeyPatch
) -> score.ScoreState:
    logs = tmp_path / "logs"
    _finished_producer(logs, metadata.read_manifest(corpus.uri), 0)
    args = score.ScoreArgs(
        corpus.uri, "bench.recovery", _props(tmp_path), str(logs), 0, tmp_path / "scores", read_workers=1
    )
    monkeypatch.setattr(score, "load_table", lambda props, name: table)
    return score._load_inputs(args, StepClock(1), io.StringIO())


@pytest.mark.parametrize("vector", [False, True], ids=["v2-position", "v3-puffin"])
def test_recovery_overwrite_counts_only_live_incoming_rows(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, vector: bool
) -> None:
    table = _recovery_table(tmp_path, corpus, vector=vector)
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    assert state.tally.committed_rows() == corpus.row_count
    assert state.tally.prefix() == corpus.batch_count - 1
    code, result = _run(tmp_path, corpus, table, metadata.read_manifest(corpus.uri))
    assert code == 0 and result["verdict"] is True
    assert result["scanned_rows"] == corpus.row_count


@pytest.mark.parametrize("vector", [False, True], ids=["v2-position", "v3-puffin"])
def test_live_deletes_cannot_hide_loss_behind_a_duplicate(
    tmp_path: Path, corpus: metadata.CorpusMetadata, vector: bool
) -> None:
    table = _recovery_table(tmp_path, corpus, vector=vector, lose=True)
    code, result = _run(tmp_path, corpus, table, metadata.read_manifest(corpus.uri))
    assert code == terminal.DATA_FAILURE
    assert result["loss_rows"] == 1 and result["duplicate_rows"] == 1


def _rewrite(table: Table, root: Path, rows: pa.Table, operation: Operation) -> None:
    parent = table.current_snapshot()
    assert parent is not None
    replacement = open_catalog(_props(root)).create_table(
        ("bench", "replacement"), schema=table.schema(), partition_spec=table.spec()
    )
    replacement.append(rows)
    latest = replacement.current_snapshot()
    assert latest is not None
    sequence = (parent.sequence_number or 0) + 1
    manifests = root / "rewrite-list.avro"
    with write_manifest_list(
        2, table.io.new_output(str(manifests)), latest.snapshot_id, parent.snapshot_id, sequence, "null"
    ) as listing:
        listing.add_manifests(latest.manifests(replacement.io))
    changed = latest.model_copy(
        update={
            "parent_snapshot_id": parent.snapshot_id,
            "sequence_number": sequence,
            "manifest_list": str(manifests),
            "summary": Summary(operation),
        }
    )
    document = table.metadata.model_dump(mode="json", by_alias=True)
    document["snapshots"] = [
        snapshot.model_dump(mode="json", by_alias=True) for snapshot in [*table.metadata.snapshots, changed]
    ]
    document["current-snapshot-id"] = latest.snapshot_id
    document["last-sequence-number"] = sequence
    table.metadata = TableMetadataUtil.parse_obj(document)
    saved = root / "rewrite.metadata.json"
    saved.write_text(json.dumps(document), encoding="utf-8")
    table.metadata_location = str(saved)


@pytest.mark.parametrize("operation", [Operation.REPLACE, Operation.OVERWRITE])
@pytest.mark.parametrize("vector", [False, True], ids=["v2-position", "v3-puffin"])
def test_compaction_does_not_inflate_delivery_or_freshness(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, operation: Operation, vector: bool
) -> None:
    records = metadata.read_manifest(corpus.uri)
    table = _recovery_table(tmp_path, corpus, vector=vector)
    rows = _all_rows(records, corpus)
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    state.args = replace(state.args, scoring_mode="saturation", measurement_s=10)
    observations = list(state.observations)
    _rewrite(table, tmp_path, rows, operation)
    assert score._read_inputs(state, StepClock(2), io.StringIO()) == 0
    assert state.observations == observations and state.commits == []
    assert state.tally.committed_rows() == corpus.row_count
    code, result = _run(tmp_path, corpus, table, records)
    assert code == 0 and result["duplicate_rows"] == 0


def test_mixed_rewrite_is_unsupported_before_incoming_rows_are_applied(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ingest_bench.scorer.snapshots import TableHistoryError

    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), "bench.recovery", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_all_rows(records[:1], corpus))
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    _rewrite(table, tmp_path, _all_rows(records, corpus), Operation.OVERWRITE)
    with pytest.raises(TableHistoryError, match="unsupported.*removes existing data files"):
        score._read_inputs(state, StepClock(2), io.StringIO())
    assert state.tally.committed_rows() == records[0].rows
    with pytest.raises(TableHistoryError, match="unsupported"):
        score.run(state.args, StepClock(2), io.StringIO())
    summary = json.loads((state.args.out_dir / score.SUMMARY_FILE).read_text())
    assert summary["state"] == "failed" and summary["run_valid"] is False
    assert summary["exactness"] is None


@pytest.mark.parametrize("vector", [False, True], ids=["v2-position", "v3-puffin"])
def test_deleting_previously_tallied_rows_is_refused_before_new_delivery(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, vector: bool
) -> None:
    from ingest_bench.scorer.snapshots import TableHistoryError

    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), "bench.recovery", corpus, create.parse_partition("unpartitioned"), {})
    rows = _all_rows(records[:1], corpus)
    table.append(rows)
    original = table.current_snapshot()
    assert original is not None
    old_path = added_files(read_metadata(table), original.snapshot_id, table.io)[0].path
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    table.append(_all_rows(records[1:], corpus))
    _mask(table, tmp_path, old_path, 0, vector=vector)
    with pytest.raises(TableHistoryError, match="changes previously tallied live rows"):
        score._read_inputs(state, StepClock(2), io.StringIO())
    assert state.tally.committed_rows() == records[0].rows


def test_partial_live_file_failure_retries_only_unapplied_files(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    from ingest_bench.scorer import live_rows
    from ingest_bench.scorer.snapshots import AddedFile

    table = _recovery_table(tmp_path, corpus, vector=True)
    state = _state(tmp_path, corpus, table, monkeypatch)
    snapshot_id = table.metadata.current_snapshot_id
    assert snapshot_id is not None
    tasks = live_rows.plan_files(table.metadata, snapshot_id, table.io)
    reader = live_rows.LiveIdReader(table.io, tasks.values())
    files = sorted(terminal._live_files(table.metadata, snapshot_id, table.io), key=lambda file: file.record_count)
    reads: list[str] = []
    failed = False

    def read(file: AddedFile) -> np.ndarray:
        nonlocal failed
        reads.append(file.path)
        ids = reader.read(tasks[file.path])
        if file.path == files[1].path and not failed:
            failed = True
            raise OSError("interrupted live file read")
        return ids

    with pytest.raises(OSError, match="interrupted"):
        score._apply_added_files(state, files, read)
    assert state.applied_files == {files[0].path}
    assert score._apply_added_files(state, files, read) == 1
    assert reads.count(files[0].path) == 1 and reads.count(files[1].path) == 2
    assert state.tally.committed_rows() == corpus.row_count


def test_shared_puffin_applies_vectors_for_both_data_files(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), "bench.shared", corpus, create.parse_partition("unpartitioned"), {})
    first = _all_rows(records[:1], corpus)
    table.append(pa.concat_tables([first, first.slice(0, 1)]))
    snapshot = table.current_snapshot()
    assert snapshot is not None
    first_file = added_files(read_metadata(table), snapshot.snapshot_id, table.io)[0]
    rest = _all_rows(records[1:], corpus)
    table.append(pa.concat_tables([rest, rest.slice(0, 1)]))
    snapshot = table.current_snapshot()
    assert snapshot is not None
    second_file = added_files(read_metadata(table), snapshot.snapshot_id, table.io)[0]
    _mask(table, tmp_path, first_file.path, len(first), vector=True, second=(second_file.path, len(rest)))
    code, result = _run(tmp_path, corpus, table, records)
    assert code == terminal.PASS and result["scanned_rows"] == corpus.row_count
    assert result["duplicate_rows"] == 0 and result["loss_rows"] == 0


@pytest.mark.parametrize(("vector", "position"), [(False, -1), (False, 10**9), (True, 10**9)])
def test_invalid_delete_positions_are_reader_errors(
    tmp_path: Path, corpus: metadata.CorpusMetadata, vector: bool, position: int
) -> None:
    table = _recovery_table(tmp_path, corpus, vector=vector, mask_position=position)
    code, result = _run(tmp_path, corpus, table, metadata.read_manifest(corpus.uri))
    assert code == terminal.ERROR and result["status"] == "error"
    assert result["verdict"] is False and "delete position" in str(result["reason"])


def test_broad_v2_delete_attachment_does_not_change_retained_rows(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    from ingest_bench.scorer import live_rows

    table = _recovery_table(tmp_path, corpus, vector=False)
    snapshot_id = table.metadata.current_snapshot_id
    assert snapshot_id is not None
    tasks = live_rows.plan_files(table.metadata, snapshot_id, table.io)
    retained = min(tasks.values(), key=lambda task: task.file.record_count)
    assert retained.delete_files, "the V2 planner attaches the incoming mask to this old file too"
    reader = live_rows.LiveIdReader(table.io, tasks.values())
    actual = reader.read(retained)
    expected = _all_rows(metadata.read_manifest(corpus.uri)[:1], corpus).column("id").to_numpy()
    assert np.array_equal(actual, expected)


@pytest.mark.parametrize("positions", [[None], [1.5]], ids=["null", "fractional"])
def test_noninteger_delete_positions_are_reader_errors(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    monkeypatch: pytest.MonkeyPatch,
    positions: list[float | None],
) -> None:
    from ingest_bench.scorer import live_rows

    table = _recovery_table(tmp_path, corpus, vector=False)
    snapshot_id = table.metadata.current_snapshot_id
    assert snapshot_id is not None
    tasks = live_rows.plan_files(table.metadata, snapshot_id, table.io)
    incoming = max(tasks.values(), key=lambda task: task.file.record_count)
    monkeypatch.setattr(
        live_rows,
        "_read_deletes",
        lambda io, file: {
            incoming.file.file_path: pa.chunked_array([positions]),
        },
    )
    reader = live_rows.LiveIdReader(table.io, tasks.values())
    with pytest.raises(ValueError, match="delete positions must be non-null integers"):
        reader.read(incoming)


@pytest.mark.parametrize("file_format", [FileFormat.PARQUET, FileFormat.ORC])
def test_native_id_reads_preserve_physical_delete_positions(tmp_path: Path, file_format: FileFormat) -> None:
    import pyarrow.orc as orc
    from pyiceberg.io.pyarrow import PyArrowFileIO
    from pyiceberg.table import FileScanTask

    from ingest_bench.scorer import live_rows

    data_path = tmp_path / f"ids.{file_format.name.lower()}"
    rows = pa.table({"id": np.arange(32, dtype=np.int64)})
    if file_format == FileFormat.PARQUET:
        pq.write_table(rows, data_path, row_group_size=3)
    else:
        orc.write_table(rows, data_path)
    data = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=str(data_path),
        file_format=file_format,
        partition=Record(),
        record_count=len(rows),
        file_size_in_bytes=data_path.stat().st_size,
    )
    positions = [0, 4, 4, 11, 12, 31]
    delete_path = tmp_path / "positions.parquet"
    pq.write_table(pa.table({"file_path": [str(data_path)] * len(positions), "pos": positions}), delete_path)
    delete = DataFile.from_args(
        content=DataFileContent.POSITION_DELETES,
        file_path=str(delete_path),
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=len(positions),
        file_size_in_bytes=delete_path.stat().st_size,
    )
    task = FileScanTask(data, {delete})
    reader = live_rows.LiveIdReader(PyArrowFileIO(), [task])
    assert reader.read(task).tolist() == [index for index in range(32) if index not in positions]


@pytest.mark.parametrize("vector", [False, True], ids=["v2-position", "v3-puffin"])
def test_masked_files_still_require_their_physical_manifest_count(
    tmp_path: Path, corpus: metadata.CorpusMetadata, vector: bool
) -> None:
    from ingest_bench.scorer import live_rows

    table = _recovery_table(tmp_path, corpus, vector=vector)
    snapshot_id = table.metadata.current_snapshot_id
    assert snapshot_id is not None
    tasks = live_rows.plan_files(table.metadata, snapshot_id, table.io)
    incoming = max(tasks.values(), key=lambda task: task.file.record_count)
    incoming.file[4] += 1
    reader = live_rows.LiveIdReader(table.io, tasks.values())
    with pytest.raises(ValueError, match="data row count disagrees"):
        reader.read(incoming)


def test_compaction_reads_are_concurrent_and_bound_unreduced_arrays(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    import weakref
    from collections.abc import Callable
    from concurrent.futures import Future, ThreadPoolExecutor
    from threading import Barrier, Lock

    from pyiceberg.table import FileScanTask

    from ingest_bench.scorer import live_rows
    from ingest_bench.scorer.tally import BatchTally, read_id_column

    records = metadata.read_manifest(corpus.uri)
    rows = _all_rows(records, corpus)
    table = create.create_table(_props(tmp_path), "bench.recovery", corpus, create.parse_partition("unpartitioned"), {})
    for part in np.array_split(np.arange(len(rows)), 6):
        table.append(rows.slice(int(part[0]), len(part)))
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    state.args = replace(state.args, read_workers=2)
    observations = list(state.observations)
    _rewrite(table, tmp_path, rows, Operation.OVERWRITE)
    barrier = Barrier(2, timeout=10)
    lock = Lock()
    submitted = reduced = started = 0
    widths: list[int] = []
    arrays: list[weakref.ReferenceType[np.ndarray]] = []
    original_read = read_id_column
    original_add = BatchTally.add_ids

    def read(path: str, file_format: str) -> np.ndarray:
        nonlocal started
        ids = original_read(path, file_format)
        with lock:
            index = started
            started += 1
            arrays.append(weakref.ref(ids))
        if index < 2:
            barrier.wait()
        return ids

    def add(tally: BatchTally, ids: np.ndarray) -> None:
        nonlocal reduced
        original_add(tally, ids)
        with lock:
            reduced += 1

    def pool(*, max_workers: int) -> ThreadPoolExecutor:
        widths.append(max_workers)
        executor = ThreadPoolExecutor(max_workers=max_workers)
        original_submit = executor.submit

        def submit(fn: Callable[[FileScanTask], np.ndarray], task: FileScanTask) -> Future[np.ndarray]:
            nonlocal submitted
            with lock:
                assert submitted - reduced < 2
                assert sum(reference() is not None for reference in arrays) < 2
                submitted += 1
            return original_submit(fn, task)

        monkeypatch.setattr(executor, "submit", submit)
        return executor

    monkeypatch.setattr(live_rows, "read_id_column", read)
    monkeypatch.setattr(live_rows, "ThreadPoolExecutor", pool)
    monkeypatch.setattr(BatchTally, "add_ids", add)
    assert score._read_inputs(state, StepClock(2), io.StringIO()) == 0
    assert widths == [2, 2]
    assert submitted == reduced == started == 7
    assert all(reference() is None for reference in arrays)
    assert state.observations == observations
    assert state.tally.committed_rows() == corpus.row_count


def test_failed_compaction_validation_retries_without_mutating_delivery(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    from pyiceberg.table import FileScanTask

    from ingest_bench.scorer import live_rows

    records = metadata.read_manifest(corpus.uri)
    table = _recovery_table(tmp_path, corpus, vector=True)
    state = _state(tmp_path, corpus, table, monkeypatch)
    score._read_inputs(state, StepClock(1), io.StringIO())
    state.args = replace(state.args, read_workers=2)
    observations = list(state.observations)
    _rewrite(table, tmp_path, _all_rows(records, corpus), Operation.OVERWRITE)
    failed = False
    original_read = live_rows.LiveIdReader.read

    def read(reader: live_rows.LiveIdReader, task: FileScanTask) -> np.ndarray:
        nonlocal failed
        if not failed:
            failed = True
            raise OSError("interrupted compaction validation")
        return original_read(reader, task)

    monkeypatch.setattr(live_rows.LiveIdReader, "read", read)
    with pytest.raises(OSError, match="interrupted compaction"):
        score._read_inputs(state, StepClock(2), io.StringIO())
    assert state.tally.committed_rows() == corpus.row_count
    assert state.observations == observations
    assert score._read_inputs(state, StepClock(2), io.StringIO()) == 0
    assert state.tally.committed_rows() == corpus.row_count
    assert state.observations == observations
