# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
from contextlib import suppress
from pathlib import Path

import pyarrow as pa
import pytest
from pyiceberg.exceptions import NamespaceAlreadyExistsError
from pyiceberg.io import FileIO
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.serializers import ToOutputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import LongType, NestedField, StringType

from ingest_bench.catalog import open_catalog
from ingest_bench.scorer import cli, geometry, snapshots
from ingest_bench.scorer.geometry import MIB

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "part", StringType(), required=True),
    NestedField(3, "payload", StringType(), required=True),
)
SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="part"))

# A fixed instant, so every offset in these tests is arithmetic rather than a
# race against the clock the appends are stamped with.
EPOCH_MS = 1_600_000_000_000

# Where each commit is placed relative to the epoch, once the timestamps the
# appends stamped are rewritten.
COMMIT_OFFSETS_S = (100, 700, 1300, 1900)
LADDER_S = (60, 600, 1200, 3600)


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _table(props: dict[str, str], name: str, *, partitioned: bool = True) -> Table:
    catalog = open_catalog(props)
    with suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("bench", properties={"location": f"{props['warehouse']}/bench"})
    return catalog.create_table(("bench", name), schema=SCHEMA, partition_spec=SPEC if partitioned else PartitionSpec())


def _rows(first_id: int, parts: list[str], rows_per_part: int, payload_repeat: int) -> pa.Table:
    """One append's rows, sized by its row count and payload width."""
    ids: list[int] = []
    part_values: list[str] = []
    payloads: list[str] = []
    for part in parts:
        for _ in range(rows_per_part):
            row_id = first_id + len(ids)
            ids.append(row_id)
            part_values.append(part)
            payloads.append(f"{row_id:012d}" * payload_repeat)
    schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("part", pa.string(), nullable=False),
            pa.field("payload", pa.string(), nullable=False),
        ]
    )
    return pa.table({"id": ids, "part": part_values, "payload": payloads}, schema=schema)


def _appends(table: Table) -> list[int]:
    """Four commits: two files, then one, one and one. Returns the rows each added."""
    table.append(_rows(0, ["a", "b"], 20, 3))
    table.append(_rows(1000, ["a"], 10, 7))
    table.append(_rows(2000, ["b"], 30, 2))
    table.append(_rows(3000, ["a"], 50, 5))
    return [40, 10, 30, 50]


def _retimed(metadata: TableMetadata, offsets_s: tuple[int, ...]) -> TableMetadata:
    """The same table with its commits placed at chosen instants after the epoch."""
    ordered = sorted(metadata.snapshots, key=lambda snapshot: snapshot.sequence_number or 0)
    assert len(ordered) == len(offsets_s)
    retimed = [
        snapshot.model_copy(update={"timestamp_ms": EPOCH_MS + offset_s * 1000})
        for snapshot, offset_s in zip(ordered, offsets_s, strict=True)
    ]
    return metadata.model_copy(update={"snapshots": retimed})


def _final_sizes_on_disk(metadata: TableMetadata, io: FileIO) -> list[int]:
    """Every live file's size at the last commit, as the filesystem reports it."""
    last = snapshots.snapshots_in_order(metadata)[-1].snapshot_id
    files = geometry.data_files_at(metadata, last, io)
    return sorted(Path(info.path.removeprefix("file://")).stat().st_size for info in files)


def _document(metadata: TableMetadata, io: FileIO, path: Path) -> Path:
    ToOutputFile.table_metadata(metadata, io.new_output(str(path)), overwrite=True)
    return path


def test_live_geometry_reports_quantiles_shares_and_a_log2_histogram() -> None:
    sizes = [512 * 1024, 6 * MIB, 6 * MIB, 16 * MIB, 40 * MIB]
    files = [
        geometry.DataFileInfo(
            path=f"s3://b/{index}.parquet",
            size_bytes=size,
            record_count=10,
            added_snapshot_id=1,
            partition="Record[a]",
        )
        for index, size in enumerate(sizes)
    ]
    live = geometry.live_geometry(files)
    assert live["files"] == 5
    assert live["rows"] == 50
    assert live["bytes"] == sum(sizes)
    quantiles = live["size_quantiles"]
    assert isinstance(quantiles, dict)
    assert quantiles["min"] == 512 * 1024 and quantiles["max"] == 40 * MIB
    assert quantiles["p50"] == pytest.approx(6 * MIB)
    # Linear interpolation between the fourth and fifth file: 0.9 × 4 = 3.6.
    assert quantiles["p90"] == pytest.approx(16 * MIB + 0.6 * (40 * MIB - 16 * MIB))
    assert quantiles["p99"] == pytest.approx(16 * MIB + 0.96 * (40 * MIB - 16 * MIB))
    assert live["small_file_share_8mib"] == pytest.approx(0.6)
    assert live["small_file_share_32mib"] == pytest.approx(0.8)
    assert live["log2_histogram"] == {
        "2^19..2^20": 1,
        "2^22..2^23": 2,
        "2^24..2^25": 1,
        "2^25..2^26": 1,
    }


def test_live_geometry_of_an_empty_table_reports_no_quantiles() -> None:
    live = geometry.live_geometry([])
    assert live["files"] == 0 and live["rows"] == 0 and live["bytes"] == 0
    assert live["size_quantiles"] == {"p50": None, "p90": None, "p99": None, "min": None, "max": None}
    assert live["small_file_share_8mib"] is None and live["small_file_share_32mib"] is None
    assert live["log2_histogram"] == {}


def test_data_files_at_is_the_whole_live_set_not_one_commits_addition(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "live")
    _appends(table)
    metadata = snapshots.read_metadata(snapshots.load_table(props, "bench.live"))
    ordered = snapshots.snapshots_in_order(metadata)
    at_second = geometry.data_files_at(metadata, ordered[1].snapshot_id, table.io)
    assert len(at_second) == 3
    assert sum(info.record_count for info in at_second) == 50
    assert {info.added_snapshot_id for info in at_second} == {ordered[0].snapshot_id, ordered[1].snapshot_id}
    at_last = geometry.data_files_at(metadata, ordered[-1].snapshot_id, table.io)
    assert len(at_last) == 5 and sum(info.record_count for info in at_last) == 130
    # The manifest's own figure, against the file the filesystem holds.
    for info in at_last:
        assert info.size_bytes == Path(info.path.removeprefix("file://")).stat().st_size


def test_the_prefix_ends_at_the_commit_a_reader_would_have_seen(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "prefix")
    _appends(table)
    metadata = _retimed(snapshots.read_metadata(snapshots.load_table(props, "bench.prefix")), COMMIT_OFFSETS_S)
    ordered = snapshots.snapshots_in_order(metadata)
    assert geometry.prefix_end(ordered, EPOCH_MS + 50_000) is None
    assert geometry.prefix_end(ordered, EPOCH_MS + 100_000) == 0
    assert geometry.prefix_end(ordered, EPOCH_MS + 699_000) == 0
    assert geometry.prefix_end(ordered, EPOCH_MS + 1_900_000) == 3
    assert geometry.prefix_end(ordered, EPOCH_MS + 9_999_000) == 3


def _assert_ladder(report: dict[str, object], sizes: list[int]) -> None:
    """Every claim the ladder makes about the four-commit table above."""
    assert report["epoch_ms"] == EPOCH_MS
    assert report["offsets_s"] == list(LADDER_S)
    at = report["at"]
    assert isinstance(at, dict)
    # Before the first commit and after the last one there is no rung.
    assert at["60"] == "absent" and at["3600"] == "absent"

    first = at["600"]
    assert isinstance(first, dict)
    assert first["timestamp_ms"] == EPOCH_MS + 100_000
    first_live, first_commits = first["live"], first["per_commit"]
    assert isinstance(first_live, dict) and isinstance(first_commits, dict)
    assert first_live["files"] == 2 and first_live["rows"] == 40
    assert first_commits["commits"] == 1
    assert first_commits["files_added_quantiles"] == {"p50": 2.0, "p90": 2.0, "p99": 2.0}

    second = at["1200"]
    assert isinstance(second, dict)
    second_live, second_commits = second["live"], second["per_commit"]
    assert isinstance(second_live, dict) and isinstance(second_commits, dict)
    assert second["timestamp_ms"] == EPOCH_MS + 700_000
    assert second_live["files"] == 3 and second_live["rows"] == 50
    assert second_commits["commits"] == 1
    assert second_commits["files_added_quantiles"] == {"p50": 1.0, "p90": 1.0, "p99": 1.0}

    final = report["final"]
    assert isinstance(final, dict)
    final_live, final_commits = final["live"], final["per_commit"]
    assert isinstance(final_live, dict) and isinstance(final_commits, dict)
    assert final["timestamp_ms"] == EPOCH_MS + 1_900_000
    assert final_live["files"] == 5 and final_live["rows"] == 130
    assert final_live["bytes"] == sum(sizes)
    quantiles = final_live["size_quantiles"]
    assert isinstance(quantiles, dict)
    assert quantiles["min"] == sizes[0] and quantiles["max"] == sizes[-1]
    assert quantiles["p50"] == pytest.approx(float(sizes[2]))
    # Every file a unit test writes is far below the small-file thresholds.
    assert final_live["small_file_share_8mib"] == 1.0 and final_live["small_file_share_32mib"] == 1.0
    assert sum(int(count) for count in final_live["log2_histogram"].values()) == 5
    # The two commits after the last rung that was present.
    assert final_commits["commits"] == 2
    assert final_commits["files_added_quantiles"] == {"p50": 1.0, "p90": 1.0, "p99": 1.0}


def test_geometry_report_walks_the_ladder_and_ends_at_the_final_snapshot(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "ladder")
    _appends(table)
    metadata = _retimed(snapshots.read_metadata(snapshots.load_table(props, "bench.ladder")), COMMIT_OFFSETS_S)
    report = geometry.geometry_report(metadata, table.io, EPOCH_MS, LADDER_S)
    _assert_ladder(report, _final_sizes_on_disk(metadata, table.io))


def test_geometry_report_of_a_table_with_no_commits_has_no_final(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "empty")
    metadata = snapshots.read_metadata(snapshots.load_table(props, "bench.empty"))
    report = geometry.geometry_report(metadata, table.io, EPOCH_MS, LADDER_S)
    assert report["final"] is None
    assert report["at"] == {"60": "absent", "600": "absent", "1200": "absent", "3600": "absent"}


def test_file_sizes_reads_a_copied_document_without_a_catalog(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "copied")
    _appends(table)
    metadata = _retimed(snapshots.read_metadata(snapshots.load_table(props, "bench.copied")), COMMIT_OFFSETS_S)
    sizes = _final_sizes_on_disk(metadata, table.io)
    # The name a teardown gives the copy, which is not Iceberg's own suffix.
    document = _document(metadata, table.io, tmp_path / "table-metadata.final.json")
    # Nothing but the document and the files it names is left to read from.
    (tmp_path / "cat.db").unlink()

    out = tmp_path / "scores"
    assert (
        cli.file_sizes(
            [
                "--metadata",
                str(document),
                "--epoch",
                str(EPOCH_MS / 1000),
                "--offsets",
                ",".join(str(offset) for offset in LADDER_S),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    _assert_ladder(json.loads((out / geometry.GEOMETRY_FILE).read_text()), sizes)


def test_file_sizes_reads_a_live_table_through_its_catalog(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "through-catalog")
    _appends(table)
    out = tmp_path / "scores"
    assert (
        cli.file_sizes(
            [
                "--table",
                "bench.through-catalog",
                "--catalog-prop",
                f"type={props['type']}",
                "--catalog-prop",
                f"uri={props['uri']}",
                "--catalog-prop",
                f"warehouse={props['warehouse']}",
                "--epoch",
                str(EPOCH_MS / 1000),
                "--out",
                str(out),
            ]
        )
        == 0
    )
    report = json.loads((out / geometry.GEOMETRY_FILE).read_text())
    final = report["final"]
    assert final["live"]["files"] == 5 and final["live"]["rows"] == 130
    # The appends were stamped with the wall clock, hours past this epoch.
    assert set(report["at"].values()) == {"absent"}


def test_file_sizes_reports_a_table_that_never_committed(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "never")
    document = _document(
        snapshots.read_metadata(snapshots.load_table(props, "bench.never")),
        table.io,
        tmp_path / "table-metadata.final.json",
    )
    out = tmp_path / "scores"
    assert cli.file_sizes(["--metadata", str(document), "--epoch", "0", "--out", str(out)]) == cli.NO_GEOMETRY
    assert json.loads((out / geometry.GEOMETRY_FILE).read_text())["final"] is None


def test_file_sizes_needs_exactly_one_of_metadata_and_table(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.file_sizes(["--epoch", "0", "--out", str(tmp_path)])
    with pytest.raises(SystemExit):
        cli.file_sizes(["--metadata", "m", "--table", "bench.t", "--epoch", "0", "--out", str(tmp_path)])


def test_a_ladder_that_does_not_ascend_is_refused(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "descending")
    _appends(table)
    metadata = snapshots.read_metadata(snapshots.load_table(props, "bench.descending"))
    with pytest.raises(ValueError, match="ascend"):
        geometry.geometry_report(metadata, table.io, EPOCH_MS, (1200, 600))
    with pytest.raises(SystemExit):
        cli.file_sizes(["--metadata", "m", "--epoch", "0", "--offsets", "1200,600", "--out", str(tmp_path)])


def test_file_sizes_reads_through_fsspec_unless_told_otherwise(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[dict[str, str]] = []

    def capture(location: str, props: dict[str, str]) -> tuple[TableMetadata, FileIO]:
        seen.append(dict(props))
        raise SystemExit(0)

    monkeypatch.setattr(geometry, "open_metadata_document", capture)
    argv = ["--metadata", "s3://bucket/doc.json", "--epoch", "0", "--out", str(tmp_path)]
    with pytest.raises(SystemExit):
        cli.file_sizes(argv)
    with pytest.raises(SystemExit):
        cli.file_sizes([*argv, "--catalog-prop", "py-io-impl=x.Y"])
    assert [props["py-io-impl"] for props in seen] == [cli.FSSPEC_FILE_IO, "x.Y"]
