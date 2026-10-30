# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Read table schema, commit history, and added data files.

Use table metadata and manifests as evidence of visible data, independently
of the engine writer.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.schema import Schema
from pyiceberg.serializers import FromInputFile
from pyiceberg.table import Table
from pyiceberg.table.metadata import TableMetadata
from pyiceberg.table.snapshots import Snapshot, Summary

from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.corpus import columns as c
from ingest_bench.corpus.metadata import CorpusMetadata


@dataclass(frozen=True)
class SnapshotInfo:
    """Commit metadata used by scoring, independent of optional summary row counts."""

    snapshot_id: int
    parent_id: int | None
    timestamp_ms: int
    operation: str


@dataclass(frozen=True)
class AddedFile:
    """Data-file metadata from the snapshot that added it."""

    path: str
    file_format: str
    record_count: int
    size_bytes: int


def load_table(props: dict[str, str], table: str) -> Table:
    return open_catalog(props).load_table(table_identifier(table))


def read_metadata(table: Table) -> TableMetadata:
    """Read full metadata through the table's configured IO.

    REST catalogs may hydrate only the current snapshot; scoring needs the
    complete history in ``metadata_location``.
    """
    return FromInputFile.table_metadata(table.io.new_input(table.metadata_location))


def check_table_schema(schema: Schema, meta: CorpusMetadata) -> list[str]:
    """Find missing, renamed or retyped corpus columns, and columns whose nullability differs.

    Allow extra columns, but require each corpus field to retain its name, type,
    and required status. Compare types without field IDs, including the
    required status of nested fields, since an engine may number them differently.
    """
    held = {field.name: field for field in schema.fields}
    mismatches: list[str] = []
    for column in meta.columns:
        name = column.name
        published = c.iceberg_type_text(column)
        if name not in held:
            mismatches.append(f"the table has no column {name!r}, which the corpus publishes as {published}")
            continue
        field = held[name]
        found = c.iceberg_text(field.field_type)
        if found != published:
            mismatches.append(f"column {name!r} is {found} in the table and {published} in the corpus")
        if field.required == column.nullable:
            table, corpus = ("required", "optional") if field.required else ("optional", "required")
            mismatches.append(f"column {name!r} is {table} in the table and {corpus} in the corpus")
    return mismatches


def _summary(snapshot: Snapshot) -> Summary:
    """Read the required snapshot summary without defaulting its operation."""
    if snapshot.summary is None:
        raise ValueError(f"snapshot {snapshot.snapshot_id} carries no summary, so its operation is unknown")
    return snapshot.summary


def snapshots_in_order(metadata: TableMetadata) -> list[SnapshotInfo]:
    """Sort snapshots by commit sequence, then timestamp.

    Sequence numbers preserve order when writer clocks disagree or timestamps tie.
    """
    ordered = sorted(metadata.snapshots, key=lambda snapshot: (snapshot.sequence_number or 0, snapshot.timestamp_ms))
    infos: list[SnapshotInfo] = []
    for snapshot in ordered:
        summary = _summary(snapshot)
        infos.append(
            SnapshotInfo(
                snapshot_id=snapshot.snapshot_id,
                parent_id=snapshot.parent_snapshot_id,
                timestamp_ms=snapshot.timestamp_ms,
                operation=summary.operation.value,
            )
        )
    return infos


class TableHistoryError(ValueError):
    """The table can no longer support the scorer's accumulated tally."""


def current_snapshot_ids(metadata: TableMetadata) -> set[int]:
    """Return IDs for the current snapshot and its ancestors; reject missing or cyclic history."""
    snapshots_by_id = {snapshot.snapshot_id: snapshot for snapshot in metadata.snapshots}
    ancestry: set[int] = set()
    snapshot_id = metadata.current_snapshot_id
    while snapshot_id is not None:
        if snapshot_id in ancestry or snapshot_id not in snapshots_by_id:
            raise TableHistoryError(f"snapshot history is cyclic or missing snapshot {snapshot_id}")
        ancestry.add(snapshot_id)
        snapshot_id = snapshots_by_id[snapshot_id].parent_snapshot_id
    return ancestry


def snapshot_by_id(metadata: TableMetadata, snapshot_id: int) -> Snapshot:
    for snapshot in metadata.snapshots:
        if snapshot.snapshot_id == snapshot_id:
            return snapshot
    raise ValueError(f"snapshot {snapshot_id} is not in the metadata at {metadata.location}")


def added_files(metadata: TableMetadata, snapshot_id: int, io: FileIO) -> list[AddedFile]:
    """Yield data files newly added by this snapshot.

    Filter by entry status and snapshot ID to avoid recounting inherited files.
    Skip delete files, which do not contain the scored data rows.
    """
    added: list[AddedFile] = []
    for manifest in snapshot_by_id(metadata, snapshot_id).manifests(io):
        # Skip manifests from other commits to keep reads linear. Rewritten manifests
        # carry inherited entries as EXISTING. Parse older lists without this field
        # to avoid dropping files; the entry filter remains authoritative.
        if manifest.added_snapshot_id is not None and manifest.added_snapshot_id != snapshot_id:
            continue
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            if entry.status != ManifestEntryStatus.ADDED or entry.snapshot_id != snapshot_id:
                continue
            data_file = entry.data_file
            if data_file.content != DataFileContent.DATA:
                continue
            added.append(
                AddedFile(
                    path=data_file.file_path,
                    file_format=data_file.file_format.name.lower(),
                    record_count=data_file.record_count,
                    size_bytes=data_file.file_size_in_bytes,
                )
            )
    return added


def visible_snapshot_ids(metadata: TableMetadata, io: FileIO, seen_logs: set[str]) -> set[int]:
    """Snapshots some metadata version made current: the states a reader could have seen.

    An engine may commit several snapshots in one metadata write; the ones inside it were never
    current. Reads each metadata-log file not in ``seen_logs`` once, adding it there only when
    every read succeeds, so a failed call loses no file's snapshot to a retry.
    """
    visible = set() if metadata.current_snapshot_id is None else {metadata.current_snapshot_id}
    read: list[str] = []
    for entry in metadata.metadata_log:
        if entry.metadata_file in seen_logs:
            continue
        previous = FromInputFile.table_metadata(io.new_input(entry.metadata_file))
        if previous.current_snapshot_id is not None:
            visible.add(previous.current_snapshot_id)
        read.append(entry.metadata_file)
    seen_logs.update(read)
    return visible
