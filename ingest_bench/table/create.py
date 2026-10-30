# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Create Iceberg tables from published corpus schemas.

Use recorded corpus columns and types, then apply the run's partitioning and
table properties. This avoids depending on presets edited after generation.
"""

from __future__ import annotations

import itertools
import json
import re
import uuid
from dataclasses import dataclass
from typing import cast

from pyiceberg.exceptions import NamespaceAlreadyExistsError, NoSuchTableError
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.metadata import new_table_metadata
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER
from pyiceberg.transforms import BucketTransform, IdentityTransform
from pyiceberg.types import NestedField

from ingest_bench import uri
from ingest_bench.catalog import open_catalog, table_identifier
from ingest_bench.corpus import columns as c
from ingest_bench.corpus.metadata import CorpusMetadata

IDENTITY = "identity"
BUCKET = "bucket"
UNPARTITIONED = "unpartitioned"

PARTITION_RE = (
    r"^(?:(identity)\(([A-Za-z_][A-Za-z0-9_]*)\)"
    r"|(bucket)\((\d+)\s*,\s*([A-Za-z_][A-Za-z0-9_]*)\)"
    r"|(unpartitioned))$"
)

# Use Iceberg's initial partition-field ID for consistent table specs.
PARTITION_FIELD_ID = 1000

# Seed tables are written partitioned by key, and every upsert run inherits that spec.
DEFAULT_SEED_PARTITION = "identity(partition_key)"


@dataclass(frozen=True)
class Partition:
    """Requested partition transform before binding to a schema."""

    transform: str
    column: str | None
    buckets: int | None

    def __post_init__(self) -> None:
        if self.transform == UNPARTITIONED:
            if self.column is not None or self.buckets is not None:
                raise ValueError("an unpartitioned scheme names no column and no bucket count")
        elif self.transform == IDENTITY:
            if self.column is None or self.buckets is not None:
                raise ValueError("an identity scheme names a column and no bucket count")
        elif self.transform == BUCKET:
            if self.column is None or self.buckets is None:
                raise ValueError("a bucket scheme names a column and a bucket count")
        else:
            raise ValueError(f"unknown partition transform {self.transform!r}")

    def source_column(self) -> str:
        if self.column is None:
            raise ValueError(f"a {self.transform} scheme has no source column")
        return self.column

    def bucket_count(self) -> int:
        if self.buckets is None:
            raise ValueError(f"a {self.transform} scheme has no bucket count")
        return self.buckets


def parse_partition(text: str) -> Partition:
    """Parse a supported benchmark partition transform.

    Restrict transforms to those covered by corpus partition truth.
    """
    match = re.match(PARTITION_RE, text.strip())
    if match is None:
        raise ValueError(f"partition must be identity(<column>), bucket(<N>, <column>) or unpartitioned, got {text!r}")
    identity, identity_column, _bucket, buckets, bucket_column, unpartitioned = match.groups()
    if unpartitioned is not None:
        return Partition(UNPARTITIONED, None, None)
    if identity is not None:
        return Partition(IDENTITY, identity_column, None)
    return Partition(BUCKET, bucket_column, int(buckets))


def iceberg_schema(meta: CorpusMetadata) -> Schema:
    """Build fields in corpus order, optional where the corpus declares nulls.

    Top-level columns take IDs 1..N in schema order, and nested fields follow.
    """
    ids = itertools.count(len(meta.columns) + 1)
    return Schema(
        *(
            NestedField(index + 1, column.name, c.iceberg_type(column, ids), required=not column.nullable)
            for index, column in enumerate(meta.columns)
        )
    )


def partition_spec(meta: CorpusMetadata, partition: Partition) -> PartitionSpec:
    """Bind the requested transform to the Iceberg schema field IDs."""
    if partition.transform == UNPARTITIONED:
        return PartitionSpec()
    names = meta.field_names()
    column = partition.source_column()
    if column not in names:
        raise ValueError(f"partition column {column!r} is not in the corpus schema; columns are {names}")
    source_id = names.index(column) + 1
    if c.is_nested(meta.columns[source_id - 1]):
        raise ValueError(f"partition column {column!r} is nested, and partition transforms take a primitive column")
    if partition.transform == IDENTITY:
        return PartitionSpec(
            PartitionField(source_id=source_id, field_id=PARTITION_FIELD_ID, transform=IdentityTransform(), name=column)
        )
    return PartitionSpec(
        PartitionField(
            source_id=source_id,
            field_id=PARTITION_FIELD_ID,
            transform=BucketTransform(num_buckets=partition.bucket_count()),
            name=f"{column}_bucket",
        )
    )


def _namespace_properties(
    props: dict[str, str], namespace: str, namespace_location: str | None, location: str | None
) -> dict[str, str]:
    """Choose namespace properties from an explicit location or storage warehouse.

    Only URI warehouses can place namespaces; Glue REST may use an account ID.
    If neither is available, use the parent of an explicit table location.
    """
    if namespace_location is not None:
        return {"location": namespace_location}
    warehouse = props.get("warehouse")
    if warehouse is not None and "://" in warehouse:
        return {"location": f"{warehouse}/{namespace}"}
    if location is not None:
        return {"location": location.rstrip("/").rsplit("/", 1)[0]}
    return {}


def create_table(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    partition: Partition,
    properties: dict[str, str],
    location: str | None = None,
    namespace_location: str | None = None,
) -> Table:
    """Create the namespace if needed, then create the table with its properties.

    ``location`` is the table root; ``namespace_location`` is the shared namespace
    root. Keep them separate so later tables do not inherit one run's directory.
    """
    catalog = open_catalog(props)
    namespace, name = table_identifier(table)
    schema = iceberg_schema(meta)
    spec = partition_spec(meta, partition)
    try:
        catalog.create_namespace(
            namespace, properties=_namespace_properties(props, namespace, namespace_location, location)
        )
    except NamespaceAlreadyExistsError:
        pass
    return catalog.create_table(
        (namespace, name), schema=schema, partition_spec=spec, location=location, properties=properties
    )


def drop_table(props: dict[str, str], table: str) -> None:
    """Delete the table if present, allowing teardown retries."""
    catalog = open_catalog(props)
    try:
        catalog.drop_table(table_identifier(table))
    except NoSuchTableError:
        pass


def register_seed_table(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    seed_metadata_location: str,
    properties: dict[str, str],
    location: str,
    namespace_location: str | None = None,
    format_version: int = 2,
) -> Table:
    """Register a run's table as a copy of the seed table's metadata.

    The copy keeps the seed snapshot, whose data files stay at the shared seed path, and takes
    its own UUID and location so every write lands under the run.
    """
    _refuse_format_property(properties)
    document = cast(dict[str, object], json.loads(uri.read_text(seed_metadata_location)))
    # The seed's write settings, such as its file-size target, set how the seed was built, not how the run
    # writes. Others stay: the name mapping is how readers find columns in the seed's files.
    seed_properties = cast(dict[str, str], document.get("properties", {}))
    kept = {key: value for key, value in seed_properties.items() if not key.startswith("write.")}
    document["properties"] = {**kept, **properties}
    return _register_upsert_document(props, table, meta, document, location, namespace_location, format_version)


def create_upsert_table(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    partition: Partition,
    properties: dict[str, str],
    location: str,
    namespace_location: str | None = None,
    format_version: int = 2,
) -> Table:
    """Create an empty upsert table, for an engine that writes the seed itself from Kafka."""
    _refuse_format_property(properties)
    metadata = new_table_metadata(
        iceberg_schema(meta), partition_spec(meta, partition), UNSORTED_SORT_ORDER, location, properties
    )
    document = cast(dict[str, object], json.loads(metadata.model_dump_json()))
    return _register_upsert_document(props, table, meta, document, location, namespace_location, format_version)


def _refuse_format_property(properties: dict[str, str]) -> None:
    # pyiceberg would build the table at that version, not the one the spec asks for.
    if "format-version" in properties:
        raise ValueError("spec.table.properties sets format-version; set spec.table.format_version instead")


def _register_upsert_document(
    props: dict[str, str],
    table: str,
    meta: CorpusMetadata,
    document: dict[str, object],
    location: str,
    namespace_location: str | None,
    format_version: int,
) -> Table:
    """Register a metadata document under a fresh UUID at ``location``, keyed for upserts.

    Identifier fields name the upsert key, which Flink's upsert sink requires to include the
    partition source column. pyiceberg cannot write format version 3, so a v3 table adds the
    row-lineage counter itself; rows written before the upgrade carry no row ids, as the spec
    allows. Edit the document directly, since pyiceberg cannot commit to a v3 table either.
    """
    if meta.schedule is None:
        raise ValueError(f"corpus {meta.name} is not an upsert corpus")
    table_uuid = str(uuid.uuid4())
    document["table-uuid"] = table_uuid
    document["location"] = location
    document["metadata-log"] = []
    if format_version == 3:
        document["format-version"] = 3
        document.setdefault("next-row-id", 0)
    elif format_version != 2:
        raise ValueError(f"an upsert table takes format version 2 or 3, not {format_version}")
    key_names = (meta.schedule.key_column, "partition_key")
    for schema in cast(list[dict[str, object]], document["schemas"]):
        if schema["schema-id"] == document["current-schema-id"]:
            fields = {str(field["name"]): field["id"] for field in cast(list[dict[str, object]], schema["fields"])}
            schema["identifier-field-ids"] = [fields[name] for name in key_names]
    metadata_location = uri.join(location, "metadata", f"00000-{table_uuid}.metadata.json")
    uri.write_text(metadata_location, json.dumps(document))
    catalog = open_catalog(props)
    namespace, name = table_identifier(table)
    try:
        catalog.create_namespace(
            namespace, properties=_namespace_properties(props, namespace, namespace_location, location)
        )
    except NamespaceAlreadyExistsError:
        pass
    return catalog.register_table((namespace, name), metadata_location)
