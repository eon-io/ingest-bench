# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""The Spark upsert MERGE against real Spark and Iceberg, in the engine image.

Opt in with ``INGEST_BENCH_SPARK_SCAN=1``, which also builds the Spark image.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy/compose/local/docker-compose.yml"
IMAGE = "lakehouse-ingest-bench/spark:local"
CONF = {
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.catalog.ice": "org.apache.iceberg.spark.SparkCatalog",
    "spark.sql.catalog.ice.type": "hadoop",
    "spark.sql.catalog.ice.warehouse": "file:///tmp/warehouse",
    # The upsert job sets this: decoded fields are nullable and the table's are required.
    "spark.sql.iceberg.check-nullability": "false",
}


@pytest.mark.integration
@pytest.mark.skipif(os.environ.get("INGEST_BENCH_SPARK_SCAN") != "1", reason="set INGEST_BENCH_SPARK_SCAN=1")
def test_the_merge_keeps_one_row_per_key_at_its_newest_version() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE), "build", "spark-job"],
        check=True,
        capture_output=True,
        env={**os.environ, "RUN_DIR": "/tmp"},
    )
    confs = [argument for key, value in CONF.items() for argument in ("--conf", f"{key}={value}")]
    result = subprocess.run(
        ["docker", "run", "--rm", "-v", f"{ROOT / 'tests/fixtures/spark_merge_check.py'}:/opt/check.py:ro", IMAGE]
        + ["/opt/spark/bin/spark-submit", "--master", "local[2]", *confs, "/opt/check.py"],
        text=True,
        capture_output=True,
    )
    assert "MERGE_CHECK_OK" in result.stdout, "\n".join(
        line for line in result.stderr.splitlines() if "Error" in line or "assert" in line.lower()
    )
