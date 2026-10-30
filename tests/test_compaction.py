# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Compaction measurement over real Iceberg tables built by a local catalog."""

from __future__ import annotations

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
from ingest_bench.scorer import cli, compaction, geometry
from ingest_bench.scorer.snapshots import read_metadata, snapshots_in_order

SCHEMA = Schema(
    NestedField(1, "id", LongType(), required=True),
    NestedField(2, "part", StringType(), required=True),
)
SPEC = PartitionSpec(PartitionField(source_id=2, field_id=1000, transform=IdentityTransform(), name="part"))

ARROW_SCHEMA = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("part", pa.string(), nullable=False)])


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _table(props: dict[str, str], name: str) -> Table:
    catalog = open_catalog(props)
    with suppress(NamespaceAlreadyExistsError):
        catalog.create_namespace("bench", properties={"location": f"{props['warehouse']}/bench"})
    return catalog.create_table(("bench", name), schema=SCHEMA, partition_spec=SPEC)


def _rows(ids: list[int], parts: list[str]) -> pa.Table:
    return pa.table({"id": ids, "part": parts}, schema=ARROW_SCHEMA)


@pytest.fixture
def compacted(tmp_path: Path) -> tuple[TableMetadata, FileIO]:
    """Three appends, then one rewrite that replaces all three files.

    pyiceberg's ``overwrite`` lands the rewrite as a ``delete`` commit followed
    by an ``append`` commit, which is exactly the shape that defeats
    label-based rewrite detection.
    """
    props = _props(tmp_path)
    table = _table(props, "compacted")
    table.append(_rows([1, 2], ["a", "a"]))
    table.append(_rows([3, 4], ["a", "a"]))
    table.append(_rows([5, 6], ["b", "b"]))
    table.overwrite(_rows([1, 2, 3, 4, 5, 6], ["a", "a", "a", "a", "b", "b"]))
    loaded = open_catalog(props).load_table(("bench", "compacted"))
    return read_metadata(loaded), loaded.io


def test_data_files_carry_their_partition(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, io = compacted
    final = snapshots_in_order(metadata)[-1].snapshot_id
    partitions = sorted(info.partition for info in geometry.data_files_at(metadata, final, io))
    assert partitions == ["Record[a]", "Record[b]"]


def test_partition_floor_counts_each_partition_separately() -> None:
    files = [
        geometry.DataFileInfo("a1", 600, 10, None, "Record[a]"),
        geometry.DataFileInfo("a2", 600, 10, None, "Record[a]"),
        geometry.DataFileInfo("b1", 600, 10, None, "Record[b]"),
    ]
    # Per partition: a holds 1200 bytes -> 2 files at a 1000-byte target; b holds
    # 600 -> 1. Summing the table's 1800 bytes instead would give 2, which is the
    # error this exists to prevent: a file cannot span partitions.
    assert compaction.partition_floor(files, 1000) == 3


def test_partition_floor_never_returns_zero_for_a_nonempty_partition() -> None:
    files = [geometry.DataFileInfo("tiny", 1, 1, None, "Record[a]")]
    assert compaction.partition_floor(files, 1000) == 1


def test_partition_floor_of_no_files_is_zero() -> None:
    assert compaction.partition_floor([], 1000) == 0


def test_partition_floor_refuses_a_nonpositive_target() -> None:
    with pytest.raises(ValueError, match="target_bytes"):
        compaction.partition_floor([], 0)


def test_file_count_series_tracks_every_commit(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, _ = compacted
    series = compaction.file_count_series(metadata)
    assert [point.operation for point in series] == ["append", "append", "append", "delete", "append"]
    # Three appended files, then the rewrite empties the table and lands two.
    assert [point.files for point in series] == [1, 2, 3, 0, 2]
    # Bytes per commit, from the same summary read: the commit that only
    # removed files added none, and every other one added what it wrote.
    added = [point.added_bytes for point in series]
    assert added[3] == 0
    assert all(value > 0 for index, value in enumerate(added) if index != 3)
    # The series comes from the metadata, so it holds every commit. The
    # scorer's snapshot poll stops before teardown and is short on rewrites.
    assert len(series) == len(metadata.snapshots)


def test_file_count_series_reports_an_absent_total_as_unknown(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, _ = compacted
    first = metadata.snapshots[0]
    assert first.summary is not None
    del first.summary.additional_properties["total-data-files"]
    # A summary that omits the total was not a commit of zero files.
    assert compaction.file_count_series(metadata)[0].files is None


def test_file_lifecycles_pair_each_addition_with_its_removal(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    lives = compaction.file_lifecycles(metadata, io)
    # Three files written by the appends, two written by the rewrite.
    assert len(lives) == 5
    removed = [life for life in lives if life.removed_ms is not None]
    still_live = [life for life in lives if life.removed_ms is None]
    assert len(removed) == 3
    assert len(still_live) == 2
    for life in removed:
        assert life.removed_ms is not None and life.removed_ms >= life.added_ms


def test_removals_are_recovered_from_the_live_set_when_a_commit_tombstones_nothing(
    compacted: tuple[TableMetadata, FileIO], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An engine may rewrite a manifest as kept EXISTING plus new ADDED and declare
    the dropped inputs only in its summary; the walk must still see them leave."""
    metadata, io = compacted
    with_tombstones = compaction.file_lifecycles(metadata, io)
    real_events = compaction._events

    def without_tombstones(
        document: TableMetadata, snapshot_id: int, file_io: FileIO
    ) -> tuple[list[geometry.DataFileInfo], list[str]]:
        added, _removed = real_events(document, snapshot_id, file_io)
        return added, []

    monkeypatch.setattr(compaction, "_events", without_tombstones)
    recovered = compaction.file_lifecycles(metadata, io)

    def removals(lives: list[compaction.FileLife]) -> list[tuple[str, int | None, int | None]]:
        return sorted((life.path, life.removed_ms, life.removed_by) for life in lives)

    assert removals(recovered) == removals(with_tombstones)
    assert sum(life.removed_by is not None for life in recovered) == 3


def test_rewritten_bytes_counts_only_files_that_were_replaced(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    lives = compaction.file_lifecycles(metadata, io)
    expected = sum(life.size_bytes for life in lives if life.removed_ms is not None)
    assert compaction.rewritten_bytes(lives) == expected
    assert compaction.rewritten_bytes(lives) > 0


def test_the_rewrite_share_is_rewritten_over_everything_written(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    lives = compaction.file_lifecycles(metadata, io)
    written = compaction.written_bytes(lives)
    assert written == sum(life.size_bytes for life in lives)
    share = compaction.rewrite_share_of_writes(lives)
    assert share == pytest.approx(compaction.rewritten_bytes(lives) / written)
    # Bounded by 1 by construction, which is why it is not an amplification.
    assert share is not None and 0.0 <= share <= 1.0


def test_the_rewrite_share_of_an_empty_table_is_unknown() -> None:
    # No bytes written is not a share of zero; there is nothing to divide.
    assert compaction.rewrite_share_of_writes([]) is None


def test_bytes_written_per_byte_appended_counts_the_rewrites_on_top() -> None:
    """The figure a reader means by amplification: above 1 once anything is rewritten."""
    commits = [("append", 100), ("append", 100), ("replace", 150), ("append", 100)]
    assert compaction.written_over_appended(commits) == pytest.approx(450 / 300)
    # Nothing appended is not a ratio of zero; there is nothing to divide.
    assert compaction.written_over_appended([("replace", 50)]) is None
    assert compaction.written_over_appended([]) is None


def test_a_rewrite_committed_as_an_append_cannot_be_told_from_arriving_data(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    """pyiceberg's `overwrite` commits a delete and then an append.

    No operation label then separates the rewrite's output from data arriving,
    so the ratio reads 1.0. Both compared arms commit rewrites as `replace`,
    where it reads 2.2 to 2.7.
    """
    metadata, _ = compacted
    assert compaction.written_per_ingested_byte(metadata) == pytest.approx(1.0)


def test_report_states_the_floor_and_the_ratio(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, io = compacted
    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )
    # Two partitions, each far below the target, so one file each is the floor.
    assert report["files"] == 2
    assert report["floor"] == 2
    assert report["floor_ratio"] == pytest.approx(1.0)
    assert report["target_file_bytes"] == 512 * 1024 * 1024
    assert report["target_source"] == compaction.TARGET_DECLARED


def test_report_records_the_tier_it_reached(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, io = compacted
    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )
    assert report["tier"] == "lifecycle"
    assert isinstance(report["rewritten_bytes"], int) and report["rewritten_bytes"] > 0
    share = report["rewrite_share_of_writes"]
    assert isinstance(share, float) and 0.0 < share <= 1.0
    assert report["written_per_ingested_byte"] is not None
    # Every byte written is either still stored or was rewritten away, so the
    # total is emitted rather than left to a reader to rebuild from two fields.
    # Guaranteed by the benchmark forbidding snapshot expiry before scoring,
    # not by this code: an expired adding commit drops its still-live files
    # from the lifecycle walk while they remain in the live set.
    written = report["written_bytes"]
    rewritten = report["rewritten_bytes"]
    assert isinstance(written, int) and isinstance(rewritten, int)
    live = report["live"]
    assert isinstance(live, dict)
    assert written == rewritten + live["bytes"]
    assert share == pytest.approx(rewritten / written)


def test_report_of_an_uncompacted_table_is_the_control(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "uncompacted")
    table.append(_rows([1, 2], ["a", "a"]))
    table.append(_rows([3, 4], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "uncompacted"))
    report = compaction.compaction_report(
        read_metadata(loaded),
        loaded.io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
    )
    # Two files in one partition that would fit in one: nothing compacted them.
    assert report["files"] == 2
    assert report["floor"] == 1
    assert report["floor_ratio"] == pytest.approx(2.0)
    assert report["rewritten_bytes"] == 0
    assert report["rewrite_share_of_writes"] == pytest.approx(0.0)
    # Nothing rewritten, so every byte written arrived once.
    assert report["written_per_ingested_byte"] == pytest.approx(1.0)


def test_report_of_an_empty_table_measures_nothing(tmp_path: Path) -> None:
    props = _props(tmp_path)
    _table(props, "empty")
    loaded = open_catalog(props).load_table(("bench", "empty"))
    report = compaction.compaction_report(
        read_metadata(loaded),
        loaded.io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
    )
    assert report["files"] == 0
    assert report["floor"] == 0
    # No files is not a ratio of zero; there is nothing to divide.
    assert report["floor_ratio"] is None
    assert report["rewrite_share_of_writes"] is None
    assert report["written_per_ingested_byte"] is None


def test_cli_writes_the_report_for_a_copied_metadata_document(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "cli")
    table.append(_rows([1, 2], ["a", "a"]))
    table.append(_rows([3, 4], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "cli"))
    out = tmp_path / "out"
    code = cli.compaction(
        ["--metadata", loaded.metadata_location, "--target-file-bytes", "536870912", "--out", str(out)]
    )
    assert code == 0
    report = json.loads((out / "compaction.json").read_text(encoding="utf-8"))
    assert report["files"] == 2
    assert report["floor"] == 1
    assert report["target_source"] == "declared"
    assert report["observed_at_ms"] is None


def test_cli_refuses_both_metadata_sources(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.compaction(["--metadata", "x", "--table", "bench.t", "--target-file-bytes", "1", "--out", str(tmp_path)])


def test_report_does_not_infer_row_identity_from_equal_totals(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )
    # The count recovered, but totals cannot show that the replacement files
    # contain the same row identities.
    assert "rows_preserved" not in report
    assert report["records_dropped"] == 0


def test_row_deficit_can_be_zero_after_a_delete_and_later_appends(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "delete_then_append")
    table.append(_rows([1, 2], ["a", "a"]))
    table.delete(delete_filter="id = 1")
    table.append(_rows([3, 4], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "delete_then_append"))

    report = compaction.compaction_report(
        read_metadata(loaded),
        loaded.io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
    )

    # Totals rose 2 -> 1 -> 3, so max(total-records) - final is zero even
    # though row 1 left the table.
    assert "rows_preserved" not in report
    assert report["records_dropped"] == 0


def test_missing_earlier_totals_do_not_make_the_final_deficit_unknown(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    first = metadata.snapshots[0]
    assert first.summary is not None
    del first.summary.additional_properties["total-records"]

    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )

    assert "rows_preserved" not in report
    assert report["records_dropped"] == 0


def test_report_cannot_measure_row_deficit_without_the_final_total(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    final = metadata.snapshots[-1]
    assert final.summary is not None
    del final.summary.additional_properties["total-records"]
    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )
    assert report["records_dropped"] is None


def test_report_flags_rows_that_left_the_table(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "erased")
    table.append(_rows([1, 2], ["a", "a"]))
    table.append(_rows([3, 4], ["b", "b"]))
    table.delete(delete_filter="part = 'b'")
    loaded = open_catalog(props).load_table(("bench", "erased"))
    report = compaction.compaction_report(
        read_metadata(loaded),
        loaded.io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
    )
    # A deficit proves loss even though removed-file bytes enter rewrite totals.
    assert "rows_preserved" not in report
    assert report["records_dropped"] == 2


def test_report_keeps_a_proven_drop_when_an_earlier_total_is_missing(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "partial_erasure_evidence")
    table.append(_rows([1, 2], ["a", "a"]))
    table.append(_rows([3, 4], ["b", "b"]))
    table.delete(delete_filter="part = 'b'")
    loaded = open_catalog(props).load_table(("bench", "partial_erasure_evidence"))
    metadata = read_metadata(loaded)
    first = metadata.snapshots[0]
    assert first.summary is not None
    del first.summary.additional_properties["total-records"]

    report = compaction.compaction_report(
        metadata,
        loaded.io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
    )

    assert "rows_preserved" not in report
    assert report["records_dropped"] == 2


def test_target_prefers_the_declared_value(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, _ = compacted
    assert compaction.resolve_target(metadata, 4096) == (4096, compaction.TARGET_DECLARED)


def test_target_falls_back_to_the_table_property(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "targeted")
    table.transaction().set_properties({compaction.TARGET_TABLE_PROPERTY: "8388608"}).commit_transaction()
    loaded = open_catalog(props).load_table(("bench", "targeted"))
    assert compaction.resolve_target(read_metadata(loaded), None) == (8388608, compaction.TARGET_PROPERTY)


def test_target_falls_back_to_the_format_default(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, _ = compacted
    assert compaction.resolve_target(metadata, None) == (536870912, compaction.TARGET_DEFAULT)


def test_cli_target_is_optional_and_names_its_source(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "clidefault")
    table.append(_rows([1, 2], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "clidefault"))
    out = tmp_path / "out"
    assert cli.compaction(["--metadata", loaded.metadata_location, "--out", str(out)]) == 0
    report = json.loads((out / "compaction.json").read_text(encoding="utf-8"))
    assert report["target_file_bytes"] == 536870912
    assert report["target_source"] == "format-default"


def test_rates_compare_files_produced_against_files_replaced(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    lives = compaction.file_lifecycles(metadata, io)
    rates = compaction.rates(lives)
    # Five files written over the run, three of them later replaced.
    assert rates["files_written"] == 5
    assert rates["files_replaced"] == 3
    span = rates["span_s"]
    assert isinstance(span, float) and span >= 0.0


def test_rates_of_a_single_instant_have_no_span_to_divide_by() -> None:
    lives = [compaction.FileLife("a", 10, 1, "Record[a]", 1000, None)]
    rates = compaction.rates(lives)
    # One file at one instant is not a rate; there is no interval.
    assert rates["span_s"] == 0.0
    assert rates["files_written_per_s"] is None
    assert rates["files_replaced_per_s"] is None


def test_convergence_needs_quiet_and_a_table_at_target() -> None:
    # Quiet but far from target: a compactor that stopped, not one that finished.
    assert compaction.converged(quiet_for_s=600.0, floor_ratio=4.0, quiet_hold_s=300.0, max_ratio=1.1) is False
    # At target but still rewriting: not settled yet.
    assert compaction.converged(quiet_for_s=10.0, floor_ratio=1.0, quiet_hold_s=300.0, max_ratio=1.1) is False
    # Both: finished.
    assert compaction.converged(quiet_for_s=600.0, floor_ratio=1.0, quiet_hold_s=300.0, max_ratio=1.1) is True
    # Nothing ever compacted, so there is no convergence to claim either way.
    assert compaction.converged(quiet_for_s=None, floor_ratio=1.0, quiet_hold_s=300.0, max_ratio=1.1) is None


def test_report_ages_the_last_rewrite_to_the_observation_time(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    latest_commit_ms = snapshots_in_order(metadata)[-1].timestamp_ms
    observed_at_ms = latest_commit_ms + 600_000
    report = compaction.compaction_report(
        metadata,
        io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
        observed_at_ms=observed_at_ms,
    )

    assert report["observed_at_ms"] == observed_at_ms
    quiet = report["quiet_for_s"]
    assert isinstance(quiet, float) and quiet >= 600.0
    assert report["converged"] is True


def test_report_without_an_observation_time_has_unknown_quietness(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    report = compaction.compaction_report(
        metadata, io, target_file_bytes=512 * 1024 * 1024, target_source=compaction.TARGET_DECLARED
    )

    assert report["observed_at_ms"] is None
    assert report["quiet_for_s"] is None
    assert report["converged"] is None


def test_report_refuses_an_observation_before_the_latest_commit(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    latest_commit_ms = snapshots_in_order(metadata)[-1].timestamp_ms

    with pytest.raises(ValueError, match="observation time"):
        compaction.compaction_report(
            metadata,
            io,
            target_file_bytes=512 * 1024 * 1024,
            target_source=compaction.TARGET_DECLARED,
            observed_at_ms=latest_commit_ms - 1,
        )


def test_report_checks_observation_against_the_latest_writer_clock(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    last_in_sequence_ms = snapshots_in_order(metadata)[-1].timestamp_ms
    metadata.snapshots[0] = metadata.snapshots[0].model_copy(update={"timestamp_ms": last_in_sequence_ms + 2_000})

    with pytest.raises(ValueError, match="observation time"):
        compaction.compaction_report(
            metadata,
            io,
            target_file_bytes=512 * 1024 * 1024,
            target_source=compaction.TARGET_DECLARED,
            observed_at_ms=last_in_sequence_ms + 1_000,
        )


def test_cli_passes_an_explicit_observation_time(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "observed")
    table.append(_rows([1, 2], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "observed"))
    observed_at_ms = snapshots_in_order(read_metadata(loaded))[-1].timestamp_ms + 1_000
    out = tmp_path / "out"

    assert (
        cli.compaction(
            [
                "--metadata",
                loaded.metadata_location,
                "--observed-at",
                str(observed_at_ms / 1000),
                "--out",
                str(out),
            ]
        )
        == 0
    )

    report = json.loads((out / "compaction.json").read_text(encoding="utf-8"))
    assert report["observed_at_ms"] == observed_at_ms


def test_cli_live_read_captures_its_observation_after_loading_metadata(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    props = _props(tmp_path)
    table = _table(props, "live_observed")
    table.append(_rows([1, 2], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "live_observed"))
    observed_at_ms = snapshots_in_order(read_metadata(loaded))[-1].timestamp_ms + 1_000
    out = tmp_path / "out"
    now = observed_at_ms - 2_000

    class FixedClock:
        def now_ms(self) -> int:
            return now

    def load(_props: dict[str, str], name: str) -> Table:
        nonlocal now
        now = observed_at_ms
        return loaded

    monkeypatch.setattr(cli, "SystemClock", FixedClock)
    monkeypatch.setattr(cli, "load_table", load)

    assert cli.compaction(["--table", "bench.live_observed", "--out", str(out)]) == 0

    report = json.loads((out / "compaction.json").read_text(encoding="utf-8"))
    assert report["observed_at_ms"] == observed_at_ms


def test_cli_live_read_leaves_quietness_unknown_when_writer_clock_is_ahead(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    props = _props(tmp_path)
    table = _table(props, "raced")
    table.append(_rows([1, 2], ["a", "a"]))
    loaded = open_catalog(props).load_table(("bench", "raced"))
    latest_commit_ms = snapshots_in_order(read_metadata(loaded))[-1].timestamp_ms
    out = tmp_path / "out"

    class EarlyClock:
        def now_ms(self) -> int:
            return latest_commit_ms - 1

    monkeypatch.setattr(cli, "SystemClock", EarlyClock)
    monkeypatch.setattr(cli, "load_table", lambda _props, _name: loaded)

    assert cli.compaction(["--table", "bench.raced", "--out", str(out)]) == 0

    report = json.loads((out / "compaction.json").read_text(encoding="utf-8"))
    assert report["observed_at_ms"] is None
    assert report["quiet_for_s"] is None
    assert report["converged"] is None


def test_report_samples_the_ratio_at_offsets(compacted: tuple[TableMetadata, FileIO]) -> None:
    metadata, io = compacted
    first = compaction.file_count_series(metadata)[0].timestamp_ms
    report = compaction.compaction_report(
        metadata,
        io,
        target_file_bytes=512 * 1024 * 1024,
        target_source=compaction.TARGET_DECLARED,
        epoch_ms=first,
        offsets_s=(0, 3600),
    )
    sampled = report["ratio_at"]
    assert isinstance(sampled, dict)
    # The first commit holds one file in one partition, which is its floor.
    at_zero = sampled["0"]
    assert isinstance(at_zero, dict)
    assert at_zero["floor_ratio"] == pytest.approx(1.0)
    # An offset past the last commit is absent, not a repeat of the end state.
    assert sampled["3600"] == geometry.ABSENT


def _retime(metadata: TableMetadata, epoch_ms: int, offsets_s: list[float]) -> TableMetadata:
    """Place each commit, in sequence order, at its offset from the epoch."""
    ordered = snapshots_in_order(metadata)
    assert len(ordered) == len(offsets_s), f"{len(ordered)} commits, {len(offsets_s)} offsets"
    at_ms = {info.snapshot_id: epoch_ms + round(offset * 1000) for info, offset in zip(ordered, offsets_s, strict=True)}
    for index, snapshot in enumerate(metadata.snapshots):
        metadata.snapshots[index] = snapshot.model_copy(update={"timestamp_ms": at_ms[snapshot.snapshot_id]})
    return metadata


def _every(metadata: TableMetadata, step_s: float) -> list[float]:
    return [index * step_s for index in range(len(metadata.snapshots))]


# The campaign reading. Commits land at whatever instant the test host manages,
# so each case builds a real table and then places its commits on a schedule;
# the manifests, sizes and partitions stay real, only the clock is chosen.
GRID_S = 10
WARMUP_S = 60.0
WINDOW_S = 60.0
TARGET_BYTES = 4096


def _campaign(metadata: TableMetadata, io: FileIO, epoch_ms: int, **overrides: object) -> dict[str, object]:
    settings: dict[str, object] = {
        "target_file_bytes": TARGET_BYTES,
        "target_source": compaction.TARGET_DECLARED,
        "epoch_ms": epoch_ms,
        "grid_s": GRID_S,
        "warmup_s": WARMUP_S,
        "window_s": WINDOW_S,
        "min_input_files": 5,
    }
    settings.update(overrides)
    return compaction.campaign_report(metadata, io, **settings)  # type: ignore[arg-type]


def _live(report: dict[str, object], offset_s: int) -> dict[str, object]:
    grid = report["grid"]
    assert isinstance(grid, list)
    sample = next(point for point in grid if point["offset_s"] == offset_s)
    live = sample["live"]
    assert isinstance(live, dict), f"offset {offset_s} has no reading"
    return live


def _windows(report: dict[str, object]) -> dict[str, object]:
    windows = report["windows"]
    assert isinstance(windows, dict)
    return windows


def _growing_table(tmp_path: Path, name: str, rounds: int) -> tuple[TableMetadata, FileIO]:
    """A table that gains a partition each round and is fully rewritten each round.

    Live file counts climb because the table keeps widening, while the rewrite
    returns every round to one file per partition.
    """
    props = _props(tmp_path)
    table = _table(props, name)
    parts: list[str] = []
    for index in range(rounds):
        parts.append(f"p{index}")
        table.append(_rows(list(range(len(parts))), list(parts)))
        table.overwrite(_rows(list(range(2 * len(parts))), parts + parts))
    loaded = open_catalog(props).load_table(("bench", name))
    return read_metadata(loaded), loaded.io


def test_campaign_does_not_fail_a_widening_table_with_enough_rewrites(tmp_path: Path) -> None:
    metadata, io = _growing_table(tmp_path, "widening", rounds=9)
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 10.0))

    report = _campaign(metadata, io, epoch_ms)

    counts = [point.files for point in compaction.file_count_series(metadata) if point.files]
    # The raw file count really does climb, which is what the floor ratio has
    # to see past: the table is widening, not falling behind.
    assert counts[-1] > counts[0]
    windows = _windows(report)
    ratio_median = windows["floor_ratio_median"]
    assert isinstance(ratio_median, float) and ratio_median <= 2.0
    cycles = windows["rewrite_cycles"]
    assert isinstance(cycles, int) and cycles >= 5
    # Sufficient rewrite activity does not prove keep-up.
    assert report["campaign_pass"] is None
    reasons = report["reasons"]
    assert isinstance(reasons, list) and len(reasons) == 1
    assert "no term fired" in reasons[0]


def test_a_partition_holding_one_small_file_is_not_a_backlog(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "singleton")
    for index in range(6):
        table.append(_rows([index], ["hot"]))
    table.append(_rows([99], ["cold"]))
    loaded = open_catalog(props).load_table(("bench", "singleton"))
    metadata, io = read_metadata(loaded), loaded.io
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 10.0))

    live = _live(_campaign(metadata, io, epoch_ms, min_input_files=5), 60)

    assert live["files"] == 7
    cold = live["cold"]
    hot = live["hot"]
    assert isinstance(cold, dict) and isinstance(hot, dict)
    assert cold == {"partitions": 1, "small_files": 1, "small_bytes": cold["small_bytes"]}
    assert hot["partitions"] == 1 and hot["small_files"] == 6
    eligible = live["eligible"]
    assert isinstance(eligible, dict)
    # The lone cold file has nothing to merge with, so it is counted apart and
    # left out of the work the compactor is judged on.
    assert eligible["files"] == 6 and eligible["partitions"] == 1


def _flat_samples(
    epoch_ms: int, *, eligible: int, floor_ratio: float, count: int, step_ms: int
) -> list[dict[str, object]]:
    """Samples on a fixed grid whose backlog and geometry both stay put."""
    return [
        {
            "at_ms": epoch_ms + index * step_ms,
            "live": {"eligible": {"bytes": eligible}, "floor_ratio": floor_ratio},
        }
        for index in range(count)
    ]


def test_the_measured_span_covers_the_offer_and_counts_each_instant_once() -> None:
    """The span is closed at its right end so the offer-end instant is inside it.

    Every window half-open left that instant in none of them, which on one
    published row moved a reading across the 1 GB line it was being compared
    against. Nothing is counted twice, because only the right end closes.
    """
    epoch_ms = 1_700_000_000_000
    from_ms = epoch_ms + 600_000
    samples = [
        {"at_ms": from_ms, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 1.0}},
        {"at_ms": from_ms + 600_000, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 3.0}},
        # The offer ends here, on the right edge of the span.
        {"at_ms": from_ms + 1_200_000, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 5.0}},
    ]
    verdict = compaction.window_verdicts(samples, [], epoch_ms=epoch_ms, window_s=600.0, windows=2)
    # All three instants, so the median is the middle one rather than 2.0.
    assert verdict["floor_ratio_median"] == pytest.approx(3.0)
    assert verdict["measured_to_ms"] == from_ms + 1_200_000


def test_the_terms_are_read_over_offered_load_and_not_over_drain() -> None:
    """After the offer the writers have stopped, so a drain reading flatters the run."""
    epoch_ms = 1_700_000_000_000
    from_ms = epoch_ms + 600_000
    offered = [
        {"at_ms": from_ms + offset, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 6.0}}
        for offset in (0, 600_000)
    ]
    draining = [
        {"at_ms": from_ms + 1_200_000, "phase": "drain", "live": {"eligible": {"bytes": 1}, "floor_ratio": 1.0}}
    ]
    # A cycle that ran after the last offered instant is drain work.
    late = compaction.RewriteCycle(snapshot_id=1, timestamp_ms=from_ms + 1_100_000, files_removed=9, bytes_removed=9)
    verdict = compaction.window_verdicts(offered + draining, [late], epoch_ms=epoch_ms, window_s=600.0, windows=2)
    assert verdict["basis"] == "offered"
    assert verdict["floor_ratio_median"] == pytest.approx(6.0)
    assert verdict["rewrite_cycles"] == 0

    unlabelled = [{k: v for k, v in s.items() if k != "phase"} for s in offered + draining]
    read_all = compaction.window_verdicts(unlabelled, [late], epoch_ms=epoch_ms, window_s=600.0, windows=2)
    assert read_all["basis"] == "all_instants"
    assert read_all["floor_ratio_median"] == pytest.approx(6.0)
    assert read_all["rewrite_cycles"] == 1


def test_the_span_reading_is_a_median_and_not_another_quantile() -> None:
    """A mean or an upper quantile would let one bad instant set the figure.

    The only remaining reading over the span is N/F, and nothing else pins it
    to the middle: the test that did was removed with the window medians.
    """
    epoch_ms = 1_700_000_000_000
    from_ms = epoch_ms + 600_000
    ratios = (1.0, 2.0, 100.0)
    samples = [
        {
            "at_ms": from_ms + index * 60_000,
            "phase": "offered",
            "live": {"eligible": {"bytes": 1}, "floor_ratio": ratio},
        }
        for index, ratio in enumerate(ratios)
    ]
    verdict = compaction.window_verdicts(samples, [], epoch_ms=epoch_ms, window_s=600.0, windows=1)
    # The mean is 34.3 and the 90th percentile is 80.4; neither is the middle.
    assert verdict["floor_ratio_median"] == pytest.approx(2.0)


def test_a_grid_that_measured_nothing_is_null_and_not_a_failure() -> None:
    """Absence of evidence is null, which is the rule the removed terms followed.

    The cycle count is the only term left, so a grid the history could not read
    would otherwise publish "a compactor did not run" on the strength of having
    seen nothing at all.
    """
    epoch_ms = 1_700_000_000_000
    assert compaction.window_verdicts([], [], epoch_ms=epoch_ms)["campaign_pass"] is None
    # A grid that did reach instants and saw no removals is a real finding.
    reached = [
        {
            "at_ms": epoch_ms + 600_000,
            "phase": "offered",
            "live": {"eligible": {"bytes": 1}, "floor_ratio": 1.0},
        }
    ]
    assert compaction.window_verdicts(reached, [], epoch_ms=epoch_ms)["campaign_pass"] is False


def test_too_few_rewrite_cycles_alone_fails_the_verdict() -> None:
    """Fail on insufficient rewrite activity even when geometry is near the floor."""
    epoch_ms = 1_700_000_000_000
    samples = _flat_samples(epoch_ms, eligible=1_000, floor_ratio=1.0, count=90, step_ms=60_000)
    plenty = [
        compaction.RewriteCycle(
            snapshot_id=index, timestamp_ms=epoch_ms + 700_000 + index * 60_000, files_removed=9, bytes_removed=1_000
        )
        for index in range(5)
    ]
    verdict = compaction.window_verdicts(samples, plenty, epoch_ms=epoch_ms)
    assert verdict["campaign_pass"] is None, "the control fires no term, which is not a pass"

    verdict = compaction.window_verdicts(samples, plenty[:1], epoch_ms=epoch_ms)
    ratio = verdict["floor_ratio_median"]
    assert isinstance(ratio, float) and ratio <= 2.0, "the reported geometry remains near the floor"
    assert verdict["campaign_pass"] is False, "too few cycles has to fail the verdict by itself"
    reasons = verdict["reasons"]
    assert isinstance(reasons, list) and len(reasons) == 1
    assert "commits removed files" in reasons[0]


def test_an_offer_that_outran_the_windows_says_how_much_it_left_uncovered() -> None:
    """Report offered load beyond the fixed span instead of stretching its windows."""
    epoch_ms = 1_700_000_000_000
    from_ms = epoch_ms + 600_000
    samples = [
        {"at_ms": from_ms + offset, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 1.0}}
        # The span covers 2 x 600 s; the offer ran 120 s past it.
        for offset in (0, 600_000, 1_200_000, 1_320_000)
    ]
    verdict = compaction.window_verdicts(samples, [], epoch_ms=epoch_ms, window_s=600.0, windows=2)
    assert verdict["measured_to_ms"] == from_ms + 1_200_000
    # Without an offer end, the last offered instant is all there is to read.
    assert verdict["offered_beyond_span_s"] == 120.0
    # With one, the gap is measured to it: a grid instant can sit a step short.
    told = compaction.window_verdicts(
        samples, [], epoch_ms=epoch_ms, offer_end_ms=from_ms + 1_380_000, window_s=600.0, windows=2
    )
    assert told["offered_beyond_span_s"] == 180.0


def test_rewrites_that_ran_after_the_offer_do_not_count_as_a_compactor_running() -> None:
    """Drain rewrites cannot satisfy the offered-load cycle-count check."""
    epoch_ms = 1_700_000_000_000
    from_ms = epoch_ms + 600_000
    samples = [
        {"at_ms": from_ms + offset, "phase": "offered", "live": {"eligible": {"bytes": 10}, "floor_ratio": 1.0}}
        for offset in (0, 600_000, 1_200_000)
    ]
    # Five cycles, every one of them after the last offered instant.
    cycles = [
        compaction.RewriteCycle(
            snapshot_id=index,
            timestamp_ms=from_ms + 1_200_000 + (index + 1) * 10_000,
            files_removed=4,
            bytes_removed=1,
        )
        for index in range(5)
    ]
    verdict = compaction.window_verdicts(samples, cycles, epoch_ms=epoch_ms, window_s=600.0, windows=3)
    assert verdict["rewrite_cycles"] == 0
    assert verdict["campaign_pass"] is False
    reasons = verdict["reasons"]
    assert isinstance(reasons, list)
    assert any("commits removed files" in str(reason) for reason in reasons)


def test_campaign_rejects_a_compactor_that_never_removes_a_file(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "noop")
    for index in range(30):
        table.append(_rows([index], ["a"]))
    loaded = open_catalog(props).load_table(("bench", "noop"))
    metadata, io = read_metadata(loaded), loaded.io
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 8.0))

    report = _campaign(metadata, io, epoch_ms)

    windows = _windows(report)
    assert windows["rewrite_cycles"] == 0
    # No removal is visible, so the rewrite's input bytes are unknown rather
    # than a rewrite that consumed nothing.
    assert windows["rewrite_input_bytes"] is None
    assert report["campaign_pass"] is False
    reasons = report["reasons"]
    assert isinstance(reasons, list)
    assert any("commits removed files" in reason for reason in reasons)


def test_a_commit_gap_carries_its_snapshot_forward(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "irregular")
    table.append(_rows([1], ["a"]))
    table.append(_rows([2], ["a"]))
    loaded = open_catalog(props).load_table(("bench", "irregular"))
    metadata, io = read_metadata(loaded), loaded.io
    epoch_ms = 1_700_000_000_000
    # A gap of ten grid steps between the two commits.
    _retime(metadata, epoch_ms, [0.0, 100.0])

    grid = _campaign(metadata, io, epoch_ms)["grid"]

    assert isinstance(grid, list)
    assert [point["offset_s"] for point in grid] == list(range(0, 101, GRID_S))
    held = [point for point in grid if point["offset_s"] < 100]
    # The table really did hold that one file for the whole gap, so every
    # instant inside it reads the carried-forward snapshot.
    assert {point["snapshot_id"] for point in held} == {grid[0]["snapshot_id"]}
    assert all(point["live"]["files"] == 1 for point in held)
    assert grid[-1]["live"]["files"] == 2


def test_an_instant_before_the_first_commit_has_no_reading(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "before")
    table.append(_rows([1], ["a"]))
    table.append(_rows([2], ["a"]))
    loaded = open_catalog(props).load_table(("bench", "before"))
    metadata, io = read_metadata(loaded), loaded.io
    first_ms = 1_700_000_000_000
    _retime(metadata, first_ms, [0.0, 40.0])
    epoch_ms = first_ms - 30_000

    grid = _campaign(metadata, io, epoch_ms)["grid"]

    assert isinstance(grid, list)
    early = [point for point in grid if point["offset_s"] < 30]
    assert early and all(point["snapshot_id"] is None for point in early)
    # No history reached is not a table of zero files, and must never satisfy
    # a bound: the reading is absent, not good.
    assert all(point["live"] is None for point in early)


def test_instants_are_classified_at_their_boundaries() -> None:
    offer_end_ms, writers_stopped_ms = 1_000, 2_000
    classify = compaction.classify_instant
    assert classify(999, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms) == compaction.PHASE_OFFERED
    # An instant carries the state its phase produced, so each boundary belongs
    # to the phase before it: the table at the offer end holds an hour of
    # offered load and none of the drain.
    assert classify(1_000, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms) == compaction.PHASE_OFFERED
    assert classify(1_001, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms) == compaction.PHASE_DRAIN
    assert classify(2_000, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms) == compaction.PHASE_DRAIN
    assert (
        classify(2_001, offer_end_ms=offer_end_ms, writers_stopped_ms=writers_stopped_ms) == compaction.PHASE_SHUTDOWN
    )


def test_an_unbounded_run_never_classifies_an_instant_as_offered() -> None:
    classify = compaction.classify_instant
    assert classify(10, offer_end_ms=None, writers_stopped_ms=None) == compaction.PHASE_UNKNOWN
    assert classify(10, offer_end_ms=None, writers_stopped_ms=2_000) == compaction.PHASE_UNKNOWN
    # After the offer, without a writers-stopped instant, drain and shutdown
    # cannot be told apart; neither may be guessed.
    assert classify(1_500, offer_end_ms=1_000, writers_stopped_ms=None) == compaction.PHASE_UNKNOWN
    assert classify(500, offer_end_ms=1_000, writers_stopped_ms=None) == compaction.PHASE_OFFERED


def test_classification_refuses_writers_that_stopped_before_the_offer_ended() -> None:
    with pytest.raises(ValueError, match="before the offer ended"):
        compaction.classify_instant(0, offer_end_ms=2_000, writers_stopped_ms=1_000)


def test_the_grid_labels_each_sample_with_its_phase(tmp_path: Path) -> None:
    props = _props(tmp_path)
    table = _table(props, "phases")
    for index in range(5):
        table.append(_rows([index], ["a"]))
    loaded = open_catalog(props).load_table(("bench", "phases"))
    metadata, io = read_metadata(loaded), loaded.io
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 10.0))

    grid = _campaign(metadata, io, epoch_ms, offer_end_ms=epoch_ms + 20_000, writers_stopped_ms=epoch_ms + 30_000)[
        "grid"
    ]

    assert isinstance(grid, list)
    # Instants at 0, 10, 20, 30 and 40 s against an offer ending at 20 and
    # writers stopping at 30: each boundary reads as the phase that produced it.
    assert [point["phase"] for point in grid] == [
        compaction.PHASE_OFFERED,
        compaction.PHASE_OFFERED,
        compaction.PHASE_OFFERED,
        compaction.PHASE_DRAIN,
        compaction.PHASE_SHUTDOWN,
    ]


def test_rewrite_cycles_group_removals_by_the_commit_that_made_them(
    compacted: tuple[TableMetadata, FileIO],
) -> None:
    metadata, io = compacted
    lives = compaction.file_lifecycles(metadata, io)
    cycles = compaction.rewrite_cycles(lives)
    # One commit removed all three appended files; the appends removed nothing.
    assert len(cycles) == 1
    assert cycles[0].files_removed == 3
    assert cycles[0].bytes_removed == compaction.rewritten_bytes(lives)


def test_the_grid_stops_at_the_last_commit() -> None:
    offsets = compaction.grid_offsets_s(epoch_ms=1_000, last_ms=1_000 + 125_000, grid_s=60)
    assert offsets == [0, 60, 120]
    # An epoch after the last commit is a run with nothing to sample.
    assert compaction.grid_offsets_s(epoch_ms=2_000, last_ms=1_000, grid_s=60) == []


def test_the_small_threshold_is_never_zero() -> None:
    # A zero threshold would make every file large and the backlog permanently
    # empty, which reads as a compactor that kept up.
    assert compaction.small_file_bytes(1, 0.5) == 1
    assert compaction.small_file_bytes(4096, 0.5) == 2048
    with pytest.raises(ValueError, match="small_fraction"):
        compaction.small_file_bytes(4096, 0.0)


def test_diagnostics_land_in_the_cell_they_happened_in() -> None:
    epoch_ms = 1_000_000
    placed = compaction.place_on_grid(
        [
            (epoch_ms + 59_999, {"pass": 1}),
            (epoch_ms + 60_000, {"pass": 2}),
            (epoch_ms + 119_000, {"pass": 3}),
            (epoch_ms + 900_000, {"pass": 4}),
        ],
        epoch_ms=epoch_ms,
        grid_s=60,
        offsets_s=(0, 60, 120),
    )
    assert placed == {"0": [{"pass": 1}], "60": [{"pass": 2}, {"pass": 3}], "120": []}


def _save(metadata: TableMetadata, io: FileIO, path: Path) -> str:
    """Write retimed metadata where the report command can read it back."""
    ToOutputFile.table_metadata(metadata, io.new_output(str(path)), overwrite=True)
    return str(path)


def test_report_command_writes_the_campaign_document(tmp_path: Path) -> None:
    metadata, io = _growing_table(tmp_path, "commanded", rounds=9)
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 10.0))
    saved = _save(metadata, io, tmp_path / "final.metadata.json")
    out = tmp_path / "out"

    code = cli.campaign_compaction(
        [
            "--metadata",
            saved,
            "--epoch",
            str(epoch_ms / 1000),
            "--offer-end",
            # Past the 60 + 3 x 60 s measured span, which is read over offered load only.
            "2023-11-14T22:17:30+00:00",
            "--target-bytes",
            str(TARGET_BYTES),
            "--grid-s",
            str(GRID_S),
            "--warmup-s",
            str(WARMUP_S),
            "--window-s",
            str(WINDOW_S),
            "--out",
            str(out),
        ]
    )

    assert code == 0
    report = json.loads((out / "compaction-report.json").read_text(encoding="utf-8"))
    assert report["campaign_pass"] is None
    assert report["epoch_ms"] == epoch_ms
    # 2023-11-14T22:17:30Z is the epoch plus 250 seconds; the grid runs to 260.
    assert report["offer_end_ms"] == epoch_ms + 250_000
    assert report["target_source"] == "declared"
    assert report["small_file_bytes"] == TARGET_BYTES // 2
    phases = {point["phase"] for point in report["grid"]}
    # Without a writers-stopped instant, everything after the offer is unknown.
    assert phases == {"offered", "unknown"}
    assert report["engine_diagnostics"] == {"rewrite_passes": None, "committer_samples": None}


def test_report_command_joins_engine_diagnostics_onto_the_grid(tmp_path: Path) -> None:
    metadata, io = _growing_table(tmp_path, "joined", rounds=9)
    epoch_ms = 1_700_000_000_000
    _retime(metadata, epoch_ms, _every(metadata, 10.0))
    saved = _save(metadata, io, tmp_path / "final.metadata.json")
    log = tmp_path / "compactor.log"
    log.write_text(
        "rewriting bench.joined every 30s\n"
        + json.dumps({"event": "spark_rewrite_start", "pass": 1})
        + "\n"
        + "[pod/spark-compactor/x] 2023-11-14T22:13:45.000000Z "
        + json.dumps(
            {
                "event": "spark_rewrite_pass",
                "pass": 1,
                "status": "success",
                "ended_at": "2023-11-14T22:13:45Z",
                "duration_s": 4.0,
                "rewritten_data_files_count": 6,
                "added_data_files_count": 2,
                "rewritten_bytes_count": 4096,
                "failure_type": None,
            }
        )
        + "\nnot json at all\n",
        encoding="utf-8",
    )
    samples = tmp_path / "committer.json"
    samples.write_text(
        json.dumps(
            [
                {"ts_ms": epoch_ms + 21_000, "in_flight_tasks": 2, "completed_total": 7},
                {"ts_ms": epoch_ms + 11_000, "in_flight_tasks": 1, "completed_total": 3},
            ]
        ),
        encoding="utf-8",
    )
    out = tmp_path / "out"

    assert (
        cli.campaign_compaction(
            [
                "--metadata",
                saved,
                "--epoch",
                str(epoch_ms / 1000),
                "--target-bytes",
                str(TARGET_BYTES),
                "--grid-s",
                str(GRID_S),
                "--sidecar-log",
                str(log),
                "--committer-metrics",
                str(samples),
                "--out",
                str(out),
            ]
        )
        == 0
    )

    diagnostics = json.loads((out / "compaction-report.json").read_text(encoding="utf-8"))["engine_diagnostics"]
    # The pass ended 25 s in, so it belongs beside the geometry at offset 20.
    assert diagnostics["rewrite_passes"]["20"] == [
        {
            "pass": 1,
            "status": "success",
            "duration_s": 4.0,
            "rewritten_data_files_count": 6,
            "added_data_files_count": 2,
            "rewritten_bytes_count": 4096,
            "failure_type": None,
        }
    ]
    assert diagnostics["rewrite_passes"]["10"] == []
    # The gauge is cumulative, so the first sample has no change to report and
    # the second carries the four tasks finished since it.
    assert diagnostics["committer_samples"]["10"] == [
        {"in_flight_tasks": 1, "completed_total": 3, "completed_delta": None}
    ]
    assert diagnostics["committer_samples"]["20"] == [
        {"in_flight_tasks": 2, "completed_total": 7, "completed_delta": 4}
    ]


def test_report_command_refuses_a_naive_iso_instant(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.campaign_compaction(["--metadata", "x", "--epoch", "2023-11-14T22:16:40", "--out", str(tmp_path)])


def test_report_command_needs_the_offer_end_to_read_drain(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.campaign_compaction(["--metadata", "x", "--epoch", "0", "--writers-stopped", "10", "--out", str(tmp_path)])


def test_report_command_refuses_a_live_table(tmp_path: Path) -> None:
    with pytest.raises(SystemExit):
        cli.campaign_compaction(["--metadata", "x", "--table", "bench.t", "--epoch", "0", "--out", str(tmp_path)])
