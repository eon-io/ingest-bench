# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Dump an upsert table's live key rows at chosen snapshots, applying every delete type.

One reader serves every engine's run, so live-state checks and their scan times compare engines
on the same footing. The frozen metadata is registered in the session-local JDBC catalog that
``render-spark-scan`` configures, so no live catalog is needed and table files are read through
the engines' own file IO. Each snapshot's rows land as Parquet parts under ``<out>/live-<id>``,
and ``<out>/scan.json`` is written last, so only a finished attempt can be checked:

    spark-submit --master 'local[8]' --properties-file spark-scan.conf upsert_scan.py \\
        --metadata s3://<bucket>/runs/<run_id>/table-metadata.final.json --snapshots 1,2,3 \\
        --out s3://<bucket>/runs/<run_id>/scores/live/<attempt>
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections.abc import Iterable, Sequence

from engines.spark.env import substitute_env_values

CATALOG = "scan"
TABLE = f"{CATALOG}.verify.frozen"
MANIFEST = "scan.json"
# Registration accepts only Iceberg's metadata file names (<version>-<id>.metadata.json).
REGISTERED_COPY = "00000-frozen.metadata.json"


def hadoop_uri(location: str) -> str:
    """Spark writes Parquet through Hadoop, which registers S3 storage under ``s3a``."""
    return "s3a://" + location[len("s3://") :] if location.startswith("s3://") else location


def resolved_settings(settings: Iterable[tuple[str, str]]) -> dict[str, str]:
    """Resolve ``${env:NAME}`` references: a cluster site's Secret-backed values arrive as references."""
    return substitute_env_values(dict(settings))


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="upsert_scan", description=__doc__.splitlines()[0])
    parser.add_argument("--metadata", required=True, help="the run's frozen table metadata file")
    parser.add_argument("--snapshots", required=True, help="comma-separated snapshot ids")
    parser.add_argument("--out", required=True, help="this attempt's directory; live-<id> outputs and scan.json")
    parser.add_argument("--key-column", default="event_id")
    args = parser.parse_args(argv)
    snapshots = [int(part) for part in str(args.snapshots).split(",") if part]
    out = hadoop_uri(str(args.out).rstrip("/"))

    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession

    # Connect to spark-submit's JVM first: until then SparkConf cannot see the submitted settings.
    SparkContext._ensure_initialized()
    conf = SparkConf()
    conf.setAll(list(resolved_settings(conf.getAll()).items()))
    spark = SparkSession.builder.appName("upsert-scan").config(conf=conf).getOrCreate()
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    def hadoop_path(location: str):  # type: ignore[no-untyped-def]
        return jvm.org.apache.hadoop.fs.Path(location)

    # Copy the frozen document under a name registration accepts, beside this attempt's outputs.
    source = hadoop_path(hadoop_uri(args.metadata))
    copy = hadoop_path(f"{out}/{REGISTERED_COPY}")
    jvm.org.apache.hadoop.fs.FileUtil.copy(
        source.getFileSystem(hadoop_conf), source, copy.getFileSystem(hadoop_conf), copy, False, hadoop_conf
    )
    registered = f"{str(args.out).rstrip('/')}/{REGISTERED_COPY}"
    # The procedure resolves its table name against the current catalog.
    spark.sql(f"USE {CATALOG}")
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.verify")
    spark.sql(f"CALL {CATALOG}.system.register_table(table => '{TABLE}', metadata_file => '{registered}')")
    seconds: dict[str, float] = {}
    for snapshot in snapshots:
        started = time.monotonic()
        rows = spark.sql(f"SELECT id, `{args.key_column}`, partition_key FROM {TABLE} VERSION AS OF {snapshot}")
        rows.write.mode("overwrite").parquet(f"{out}/live-{snapshot}")
        seconds[str(snapshot)] = time.monotonic() - started
        print(f"UPSERT_SCAN snapshot={snapshot} seconds={seconds[str(snapshot)]:.1f}", flush=True)
    manifest = {
        "metadata": args.metadata,
        "snapshots": seconds,
        "parallelism": spark.sparkContext.defaultParallelism,
    }
    path = hadoop_path(f"{out}/{MANIFEST}")
    stream = path.getFileSystem(hadoop_conf).create(path, True)
    stream.write(bytearray(json.dumps(manifest).encode()))
    stream.close()
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
