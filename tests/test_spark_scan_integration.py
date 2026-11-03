# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Scan a frozen table with the real Spark image while no catalog runs.

Opt in with ``INGEST_BENCH_SPARK_SCAN=1``: it builds the Spark image and starts MinIO in an
isolated Compose project.
"""

from __future__ import annotations

import json
import os
import subprocess
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as pa_dataset
import pytest
import s3fs
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import EqualTo, Reference
from pyiceberg.expressions.literals import literal
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType

from engines.spark import scan_conf
from ingest_bench.specs import model

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy/compose/local/docker-compose.yml"
ENABLED = os.environ.get("INGEST_BENCH_SPARK_SCAN") == "1"
HOST_S3 = {
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "admin",
    "s3.secret-access-key": "password",
    "s3.path-style-access": "true",
    "s3.region": "us-east-1",
}


def compose(project: str, *args: str, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        ["docker", "compose", "-p", project, "-f", str(COMPOSE), *args],
        check=False,
        text=True,
        capture_output=True,
        env={**os.environ, "RUN_DIR": "/tmp", **(env or {})},
    )
    if result.returncode:
        lines = result.stderr.splitlines()
        causes = [line for line in lines if "Exception" in line or "Error" in line or 'File "' in line][:25]
        causes = causes or (lines + result.stdout.splitlines())[-25:]
        pytest.fail(f"docker compose {' '.join(args[:3])} exited {result.returncode}:\n" + "\n".join(causes))
    return result


def two_snapshot_table(warehouse: str) -> tuple[str, int, int]:
    """A table whose first snapshot holds ids 1..10 and whose second deletes 1..4 and adds 11..12."""
    scratch = tempfile.mkdtemp()
    catalog = SqlCatalog("t", uri=f"sqlite:///{scratch}/c.db", warehouse=warehouse, **HOST_S3)
    catalog.create_namespace("s")
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "event_id", StringType(), required=True),
        NestedField(3, "partition_key", StringType(), required=True),
    )
    table = catalog.create_table(("s", "t"), schema=schema)

    def rows(ids: list[int]) -> pa.Table:
        fields = [pa.field("id", pa.int64(), False), pa.field("event_id", pa.string(), False)]
        fields.append(pa.field("partition_key", pa.string(), False))
        return pa.table(
            {"id": ids, "event_id": [f"k{i}" for i in ids], "partition_key": ["p00000"] * len(ids)},
            schema=pa.schema(fields),
        )

    table.append(rows(list(range(1, 11))))
    first = table.current_snapshot()
    for doomed in range(1, 5):
        table.delete(EqualTo(term=Reference("id"), value=literal(doomed)))
    table.append(rows([11, 12]))
    table = catalog.load_table(("s", "t"))
    last = table.current_snapshot()
    assert first is not None and last is not None
    catalog.engine.dispose()
    return table.metadata_location, first.snapshot_id, last.snapshot_id


@pytest.mark.integration
@pytest.mark.skipif(not ENABLED, reason="set INGEST_BENCH_SPARK_SCAN=1 to build the Spark image and scan in Docker")
def test_the_scan_reads_two_snapshots_of_a_frozen_table_without_a_catalog(tmp_path: Path) -> None:
    project = f"ib-scan-{uuid.uuid4().hex[:8]}"
    compose(project, "build", "spark-job")
    compose(project, "up", "-d", "--wait", "minio")
    # Run bucket creation to completion; a detached start returns before the buckets exist.
    compose(project, "run", "--rm", "minio-init")
    try:
        fs = s3fs.S3FileSystem(key="admin", secret="password", client_kwargs={"endpoint_url": "http://localhost:9000"})
        metadata, first, last = two_snapshot_table("s3://warehouse/scan-test")
        frozen = "s3://runs/scan-test/table-metadata.final.json"
        fs.copy(metadata.removeprefix("s3://"), frozen.removeprefix("s3://"))
        site = model.load_site(ROOT / "deploy/compose/local/site.yaml")
        # Cluster sites render Secret-backed values as references; the scan must resolve them itself.
        conf = scan_conf.render(site, cores=2, memory_mb=2048)
        secret_keys = ("spark.sql.catalog.scan.s3.secret-access-key", "spark.hadoop.fs.s3a.secret.key")
        lines = [
            f"{line.split(' ', 1)[0]} ${{env:SCAN_SECRET}}" if line.startswith(secret_keys) else line
            for line in conf.splitlines()
        ]
        assert sum("${env:SCAN_SECRET}" in line for line in lines) == 2
        (tmp_path / "spark-scan.conf").write_text("\n".join(lines) + "\n")
        attempt = "s3://runs/scan-test/live/attempt-1"
        command = [
            "/opt/spark/bin/spark-submit",
            "--master",
            "local[2]",
            "--properties-file",
            "/opt/bench/scan/spark-scan.conf",
            "/opt/bench/engines/spark/upsert_scan.py",
            "--metadata",
            frozen,
            "--snapshots",
            f"{first},{last}",
            "--out",
            attempt,
        ]
        compose(
            project,
            "run",
            "--rm",
            "-e",
            "SCAN_SECRET=password",
            "upsert-scan",
            *command,
            env={"SCAN_DIR": str(tmp_path)},
        )
        manifest = json.loads(fs.cat(f"{attempt.removeprefix('s3://')}/scan.json"))
        assert manifest["metadata"] == frozen and set(manifest["snapshots"]) == {str(first), str(last)}

        def ids(snapshot: int) -> list[int]:
            path = f"{attempt.removeprefix('s3://')}/live-{snapshot}"
            return sorted(pa_dataset.dataset(path, filesystem=fs, format="parquet").to_table().column("id").to_pylist())

        assert ids(first) == list(range(1, 11))
        assert ids(last) == list(range(5, 13))
    finally:
        compose(project, "down", "-v", "--remove-orphans")
