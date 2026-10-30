# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Run inside the Spark image: the upsert MERGE keeps one row per key at its newest version.

Exercises the job's own collapse and MERGE statements on a v3 merge-on-read table.
"""

from pyspark.sql import SparkSession

from engines.spark.stream_to_iceberg import BATCH_VIEW, LATEST_VIEW, collapse_sql, merge_statement

TABLE = "ice.db.t"
COLUMNS = ("id", "event_id", "partition_key")
KEYS = ("event_id", "partition_key")

spark = SparkSession.builder.getOrCreate()
spark.sql(
    f"CREATE TABLE {TABLE} (id BIGINT NOT NULL, event_id STRING NOT NULL, partition_key STRING NOT NULL) "
    "USING iceberg PARTITIONED BY (partition_key) TBLPROPERTIES ('format-version'='3', "
    "'write.merge.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.delete.mode'='merge-on-read')"
)
spark.sql(f"INSERT INTO {TABLE} VALUES (-1, 'k1', 'p0'), (-2, 'k2', 'p0'), (-3, 'k9', 'p1')")


def statement(rows: list[tuple[int, str, str]]) -> str:
    spark.createDataFrame(rows, "id BIGINT, event_id STRING, partition_key STRING").createOrReplaceTempView(BATCH_VIEW)
    spark.sql(collapse_sql(COLUMNS, KEYS, "id")).createOrReplaceTempView(LATEST_VIEW)
    return merge_statement(TABLE, KEYS, "id", "partition_key", sorted({row[2] for row in rows}))


def merge(rows: list[tuple[int, str, str]]) -> None:
    spark.sql(statement(rows))


def live() -> list[tuple[int, str]]:
    return sorted((row.id, row.event_id) for row in spark.table(TABLE).collect())


batch = [(10, "k1", "p0"), (11, "k1", "p0"), (12, "k1", "p0"), (13, "k3", "p0")]
# The batch touches p0 only, so the target scan must carry the partition filter.
plan = spark.sql(f"EXPLAIN {statement(batch)}").collect()[0][0]
scans = [line for line in plan.splitlines() if "BatchScan" in line and TABLE in line]
assert scans and all("filters=partition_key = 'p0'" in line for line in scans), plan
merge(batch)
expected = [(-3, "k9"), (-2, "k2"), (12, "k1"), (13, "k3")]
assert live() == expected, live()
merge(batch)
assert live() == expected, f"a replayed batch changed the table: {live()}"
merge([(5, "k1", "p0")])
assert live() == expected, f"an older version replaced a newer one: {live()}"
formats = {row.file_format for row in spark.sql(f"SELECT file_format FROM {TABLE}.delete_files").collect()}
assert formats == {"PUFFIN"}, f"merge-on-read on v3 should write deletion vectors, found {formats}"
print("MERGE_CHECK_OK", flush=True)
