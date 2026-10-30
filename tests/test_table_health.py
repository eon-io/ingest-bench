# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from types import SimpleNamespace
from typing import Any, cast

import pytest
from pyiceberg.manifest import DataFileContent, FileFormat
from pyiceberg.typedef import Record

from ingest_bench.scorer import table_health

PARTITIONED, UNPARTITIONED = 0, 1


def entry(
    content: DataFileContent,
    path: str,
    *,
    size: int = 100,
    rows: int = 10,
    sequence: int = 1,
    partition: str = "p0",
    spec_id: int = PARTITIONED,
    file_format: FileFormat = FileFormat.PARQUET,
) -> object:
    data_file = SimpleNamespace(
        content=content,
        file_path=path,
        file_format=file_format,
        file_size_in_bytes=size,
        record_count=rows,
        spec_id=spec_id,
        partition=Record() if spec_id == UNPARTITIONED else Record(partition),
    )
    return SimpleNamespace(data_file=data_file, sequence_number=sequence)


def files_of(monkeypatch: pytest.MonkeyPatch, entries: list[object]) -> dict[str, Any]:
    manifest = SimpleNamespace(fetch_manifest_entry=lambda io, discard_deleted: entries)
    snapshot = SimpleNamespace(manifests=lambda io: [manifest])
    monkeypatch.setattr(table_health, "snapshot_by_id", lambda metadata, snapshot_id: snapshot)
    specs = {
        PARTITIONED: SimpleNamespace(is_unpartitioned=lambda: False),
        UNPARTITIONED: SimpleNamespace(is_unpartitioned=lambda: True),
    }
    metadata = SimpleNamespace(specs=lambda: specs)
    return cast(dict[str, Any], table_health.snapshot_files(cast(Any, metadata), 1, cast(Any, None)))


def test_deletion_vectors_count_once_per_puffin_file_and_once_per_data_file(monkeypatch: pytest.MonkeyPatch) -> None:
    files = files_of(
        monkeypatch,
        [
            entry(DataFileContent.DATA, "d1.parquet"),
            entry(DataFileContent.DATA, "d2.parquet"),
            entry(DataFileContent.POSITION_DELETES, "dv.puffin", size=4000, rows=3, file_format=FileFormat.PUFFIN),
            entry(DataFileContent.POSITION_DELETES, "dv.puffin", size=4000, rows=4, file_format=FileFormat.PUFFIN),
        ],
    )
    assert files["delete_files"]["deletion_vector"] == {"files": 1, "bytes": 4000, "rows": 7, "vectors": 2}
    assert files["data_files_with_deletion_vectors"] == 2 and files["data_file_rows"] == 20
    assert files["equality_fanout"] == {"mean_files": 0.0, "max_files": 0, "max_rows": 0}


def test_an_equality_delete_applies_to_older_data_files_in_its_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    files = files_of(
        monkeypatch,
        [
            entry(DataFileContent.DATA, "old.parquet", sequence=1),
            entry(DataFileContent.DATA, "newer.parquet", sequence=2),
            entry(DataFileContent.EQUALITY_DELETES, "e2.parquet", rows=5, sequence=2),
            entry(DataFileContent.EQUALITY_DELETES, "e3.parquet", rows=7, sequence=3),
            # Another partition's deletes never apply here.
            entry(DataFileContent.EQUALITY_DELETES, "other.parquet", rows=100, sequence=3, partition="p1"),
        ],
    )
    # old.parquet takes both p0 deletes; newer.parquet only the later one, as a same-sequence delete skips it.
    assert files["equality_fanout"] == {"mean_files": 1.5, "max_files": 2, "max_rows": 12}
    assert files["delete_files"]["equality"] == {"files": 3, "bytes": 300, "rows": 112}


def test_an_unpartitioned_equality_delete_applies_to_every_older_data_file(monkeypatch: pytest.MonkeyPatch) -> None:
    files = files_of(
        monkeypatch,
        [
            entry(DataFileContent.DATA, "a.parquet", sequence=1, partition="p0"),
            entry(DataFileContent.DATA, "b.parquet", sequence=1, partition="p1"),
            entry(DataFileContent.DATA, "c.parquet", sequence=4, partition="p1"),
            entry(DataFileContent.EQUALITY_DELETES, "global.parquet", rows=9, sequence=3, spec_id=UNPARTITIONED),
        ],
    )
    assert files["equality_fanout"] == {"mean_files": pytest.approx(2 / 3), "max_files": 1, "max_rows": 9}


def test_a_table_without_data_files_has_no_fanout(monkeypatch: pytest.MonkeyPatch) -> None:
    files = files_of(monkeypatch, [])
    assert files["equality_fanout"] == {"mean_files": None, "max_files": 0, "max_rows": 0}


def test_the_report_relates_decoded_rows_to_live_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    files = files_of(monkeypatch, [entry(DataFileContent.DATA, "d.parquet", rows=150)])
    monkeypatch.setattr(table_health, "snapshot_files", lambda metadata, snapshot_id, io: files)
    report = table_health.health_report(cast(Any, SimpleNamespace(current_snapshot_id=7)), cast(Any, None), 100)
    assert report["snapshot_id"] == 7 and report["live_rows"] == 100
    assert report["dead_rows"] == 50 and report["rows_per_live_row"] == 1.5
