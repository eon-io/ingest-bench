# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Run a rendered Structured Streaming job inside the Spark image.

Released connectors handle Kafka, Avro, JSON, Parquet, and Iceberg. Settings come
from spark-submit's properties file and the documents under RUN_DIR.
Import PySpark inside main so this module remains testable outside the image.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from engines.spark.env import substitute_env_values

# Mount beside the job; /run is reserved for container runtime files.
RUN_DIR = Path("/opt/bench/run")
JOB_DOCUMENT = RUN_DIR / "job.json"
READER_SCHEMA = RUN_DIR / "reader-schema.avsc"

# Keep encoding names aligned with the spec; the image lacks the harness package.
VALUE_ENCODING_AVRO = "avro"
VALUE_ENCODING_CONFLUENT = "confluent"
VALUE_ENCODING_JSON = "json"

# Confluent framing adds a magic byte and a four-byte schema ID. Drop those
# five bytes using Spark's 1-based substring. Each run uses one known schema,
# so the job needs no registry lookup.
VALUE_EXPRESSIONS = {
    VALUE_ENCODING_AVRO: "value",
    VALUE_ENCODING_JSON: "CAST(value AS STRING)",
    VALUE_ENCODING_CONFLUENT: "substring(value, 6, length(value) - 5)",
}

# Source options the job derives from the run, which spec source options may not override.
RESERVED_SOURCE_OPTIONS = frozenset(
    {"kafka.bootstrap.servers", "subscribe", "startingOffsets", "kafka.group.id", "maxOffsetsPerTrigger"}
)


@dataclass(frozen=True)
class Job:
    """Rendered source, sink, and trigger settings for one run."""

    topic: str
    bootstrap: str
    group_id: str
    value_encoding: str
    table: str
    columns: tuple[str, ...]
    json_schema: str
    json_via_avro: bool
    kafka_options: dict[str, str]
    source_options: dict[str, str]
    write_options: dict[str, str]
    trigger_interval: str
    max_offsets_per_trigger: int | None
    # Upsert runs: the key columns and the version column a MERGE resolves versions by.
    upsert_keys: tuple[str, ...] = ()
    upsert_version: str | None = None
    upsert_partition: str | None = None


# Views a micro-batch is registered as, and its newest version per key.
BATCH_VIEW = "upsert_batch"
LATEST_VIEW = "upsert_latest"


def collapse_sql(columns: tuple[str, ...], keys: tuple[str, ...], version: str) -> str:
    """Keep each key's newest row of the micro-batch: MERGE refuses two source rows matching one target row."""
    fields = ", ".join(f"`{name}`" for name in columns)
    grouped = ", ".join(f"`{name}`" for name in keys)
    return (
        f"SELECT latest.* FROM (SELECT max_by(struct({fields}), `{version}`) AS latest "
        f"FROM {BATCH_VIEW} GROUP BY {grouped})"
    )


def _sql_string(value: str) -> str:
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def merge_statement(
    table: str, keys: tuple[str, ...], version: str, partition: str, partition_values: Sequence[str]
) -> str:
    """Replace each key's live row with a newer version, or insert a new key.

    The version guard makes a replayed micro-batch a no-op: its rows match themselves and change nothing.
    Iceberg prunes the target scan only by literal predicates in ON, so the batch's partition values
    are spelled out; the join on the partition column alone reads every partition.
    """
    on = " AND ".join(f"t.`{name}` = s.`{name}`" for name in keys)
    literals = ", ".join(_sql_string(value) for value in partition_values)
    return (
        f"MERGE INTO {table} t USING {LATEST_VIEW} s ON {on} AND t.`{partition}` IN ({literals}) "
        f"WHEN MATCHED AND s.`{version}` > t.`{version}` THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )


def _str_at(document: dict[str, object], key: str, path: Path) -> str:
    value = document[key]
    if not isinstance(value, str):
        raise ValueError(f"{path} key {key!r} must be a string, got {value!r}")
    return value


def _bool_at(document: dict[str, object], key: str, path: Path) -> bool:
    value = document[key]
    if not isinstance(value, bool):
        raise ValueError(f"{path} key {key!r} must be a boolean, got {value!r}")
    return value


def _string_map_at(document: dict[str, object], key: str, path: Path) -> dict[str, str]:
    value = document[key]
    if not isinstance(value, dict):
        raise ValueError(f"{path} key {key!r} must be a mapping, got {value!r}")
    return {str(name): str(entry) for name, entry in cast(dict[object, object], value).items()}


def read_job(path: Path) -> Job:
    """Read and validate the job document at ``path``.

    Require every key so renderer omissions fail explicitly. Resolve Kafka option
    placeholders using the container's environment.
    """
    loaded = json.loads(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must hold a mapping, got {type(loaded).__name__}")
    document = cast(dict[str, object], loaded)
    columns = document["columns"]
    if not isinstance(columns, list):
        raise ValueError(f"{path} key 'columns' must be a list of column names, got {columns!r}")
    limit = document["max_offsets_per_trigger"]
    if limit is not None and not isinstance(limit, int):
        raise ValueError(f"{path} key 'max_offsets_per_trigger' must be an integer or null, got {limit!r}")
    upsert = document.get("upsert")
    if upsert is not None and not isinstance(upsert, dict):
        raise ValueError(f"{path} key 'upsert' must be a mapping or null, got {upsert!r}")
    upsert_block = cast(dict[str, object], upsert or {})
    return Job(
        topic=_str_at(document, "topic", path),
        bootstrap=_str_at(document, "bootstrap", path),
        group_id=_str_at(document, "group_id", path),
        value_encoding=_str_at(document, "value_encoding", path),
        table=_str_at(document, "table", path),
        columns=tuple(str(name) for name in cast(list[object], columns)),
        json_schema=_str_at(document, "json_schema", path),
        json_via_avro=_bool_at(document, "json_via_avro", path),
        # Keep resolved values in memory; archived documents retain their references.
        kafka_options=substitute_env_values(_string_map_at(document, "kafka_options", path)),
        source_options=_string_map_at(document, "source_options", path),
        write_options=_string_map_at(document, "write_options", path),
        trigger_interval=_str_at(document, "trigger_interval", path),
        max_offsets_per_trigger=limit,
        upsert_keys=tuple(str(name) for name in cast(list[object], upsert_block.get("key_columns", []))),
        upsert_version=None if not upsert_block else _str_at(upsert_block, "version_column", path),
        upsert_partition=None if not upsert_block else _str_at(upsert_block, "partition_column", path),
    )


def source_options(job: Job) -> dict[str, str]:
    """Build Kafka source options, applying the spec's source options and then site properties.

    Read from earliest because records may arrive before the engine starts.
    """
    options = {
        "kafka.bootstrap.servers": job.bootstrap,
        "subscribe": job.topic,
        "startingOffsets": "earliest",
        # Use the run ID to identify consumer groups left by abandoned runs.
        "kafka.group.id": job.group_id,
        **job.source_options,
        **job.kafka_options,
    }
    if job.max_offsets_per_trigger is not None:
        options["maxOffsetsPerTrigger"] = str(job.max_offsets_per_trigger)
    return options


def value_expression(encoding: str) -> str:
    """Return the Kafka value expression consumed by the selected decoder."""
    if encoding not in VALUE_EXPRESSIONS:
        raise ValueError(f"value encoding {encoding!r} is not one this job decodes: {sorted(VALUE_EXPRESSIONS)}")
    return VALUE_EXPRESSIONS[encoding]


def main() -> int:
    job = read_job(JOB_DOCUMENT)
    schema = READER_SCHEMA.read_text()
    # Reject unsupported encodings before starting Spark.
    value = value_expression(job.value_encoding)

    from pyspark.sql import SparkSession
    from pyspark.sql.avro.functions import from_avro, to_avro
    from pyspark.sql.functions import col, expr, from_json

    # spark-submit supplies the complete session configuration.
    spark = SparkSession.builder.getOrCreate()
    records = spark.readStream.format("kafka").options(**source_options(job)).load()
    if job.value_encoding == VALUE_ENCODING_JSON:
        decoded = from_json(
            expr(value),
            job.json_schema,
            {"mode": "FAILFAST", "timestampNTZFormat": "yyyy-MM-dd'T'HH:mm:ss.SSS"},
        )
        if job.json_via_avro:
            # from_json reads every list element and map value as nullable, which Iceberg
            # refuses to write into required ones. An Avro round trip restores the corpus
            # nullability and fails on a null required member.
            decoded = from_avro(to_avro(decoded, schema), schema)
    else:
        decoded = from_avro(expr(value), schema)
    # Select explicitly so a missing schema field cannot silently drop a column.
    rows = records.select(decoded.alias("record")).select(*(col(f"record.{name}").alias(name) for name in job.columns))
    if job.upsert_version is None:
        query = (
            rows.writeStream.format("iceberg")
            .outputMode("append")
            .options(**job.write_options)
            .trigger(processingTime=job.trigger_interval)
            .toTable(job.table)
        )
    else:
        collapse = collapse_sql(job.columns, job.upsert_keys, job.upsert_version)
        version, partition = job.upsert_version, cast(str, job.upsert_partition)

        def merge_batch(batch: object, _batch_id: int) -> None:
            frame = cast(Any, batch)
            frame.createOrReplaceTempView(BATCH_VIEW)
            session = frame.sparkSession
            # Cached so reading the partition values and the MERGE share one Kafka read and collapse.
            latest = session.sql(collapse).persist()
            try:
                values = sorted(row[0] for row in latest.select(partition).distinct().collect())
                if values:
                    latest.createOrReplaceTempView(LATEST_VIEW)
                    session.sql(merge_statement(job.table, job.upsert_keys, version, partition, values))
            finally:
                latest.unpersist()

        query = (
            rows.writeStream.foreachBatch(merge_batch)
            .option("checkpointLocation", job.write_options["checkpointLocation"])
            .trigger(processingTime=job.trigger_interval)
            .start()
        )
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
