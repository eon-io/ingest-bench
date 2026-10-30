# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Render Spark SQL equivalent to harness table creation.

Preserve corpus field types, required status, partitioning, and properties
for engines that create their own tables.
"""

from __future__ import annotations

from ingest_bench.corpus import columns as c
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.table.create import IDENTITY, UNPARTITIONED, Partition


def _holds_uuid(column: c.ColumnDistribution) -> bool:
    return column.kind == c.KIND_UUID or any(_holds_uuid(child) for child in c.children(column))


def _refuse_undeclarable(column: c.ColumnDistribution) -> None:
    """Reject types Spark SQL cannot create as the corpus declares them."""
    if _holds_uuid(column):
        raise ValueError(f"corpus column {column.name!r} holds a uuid, which Spark SQL cannot declare")
    if c.has_required_collection_member(column):
        raise ValueError(
            f"corpus column {column.name!r} nests a required list element or map value, which Spark SQL cannot declare"
        )


def _partition_clause(partition: Partition) -> str:
    """The ``PARTITIONED BY`` argument for a scheme, empty when there is none."""
    if partition.transform == UNPARTITIONED:
        return ""
    if partition.transform == IDENTITY:
        return partition.source_column()
    return f"bucket({partition.bucket_count()}, {partition.source_column()})"


def spark_sql_ddl(meta: CorpusMetadata, table: str, partition: Partition, properties: dict[str, str]) -> str:
    """The ``CREATE TABLE`` statement for this corpus, scheme and properties."""
    columns = []
    for column in meta.columns:
        _refuse_undeclarable(column)
        columns.append(f"  {column.name} {c.spark_type(column)}{'' if column.nullable else ' NOT NULL'}")
    lines = [f"CREATE TABLE {table} (", ",\n".join(columns), ") USING iceberg"]
    clause = _partition_clause(partition)
    if clause:
        lines.append(f"PARTITIONED BY ({clause})")
    if properties:
        rendered = ", ".join(f"'{key}' = '{value}'" for key, value in properties.items())
        lines.append(f"TBLPROPERTIES ({rendered})")
    return "\n".join(lines)
