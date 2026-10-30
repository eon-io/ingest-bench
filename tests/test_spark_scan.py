# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import dataclasses
import json
import math
from pathlib import Path

import pytest

from engines.spark import scan_conf, upsert_scan
from ingest_bench.scorer import upsert_verify
from ingest_bench.specs import model

ROOT = Path(__file__).resolve().parents[1]


def local_site() -> model.SiteConfig:
    return model.load_site(ROOT / "deploy/compose/local/site.yaml")


def conf_lines(text: str) -> dict[str, str]:
    return dict(line.split(" ", 1) for line in text.splitlines() if line)


def test_the_scan_catalog_is_session_local_with_the_engine_file_io() -> None:
    conf = conf_lines(scan_conf.render(local_site(), cores=4, memory_mb=8192))
    assert conf["spark.sql.catalog.scan"] == "org.apache.iceberg.spark.SparkCatalog"
    assert conf["spark.sql.catalog.scan.catalog-impl"] == "org.apache.iceberg.jdbc.JdbcCatalog"
    assert conf["spark.sql.catalog.scan.uri"] == "jdbc:sqlite:/tmp/upsert-scan-catalog.db"
    assert conf["spark.sql.catalog.scan.io-impl"] == "org.apache.iceberg.aws.s3.S3FileIO"
    assert conf["spark.sql.catalog.scan.s3.endpoint"] == "http://minio:9000"
    assert "spark.sql.catalog.scan.type" not in conf
    # Parquet output goes through Hadoop S3A.
    assert conf["spark.hadoop.fs.s3a.endpoint"] == "http://minio:9000"
    assert conf["spark.driver.memory"] == "8192m" and conf["spark.sql.shuffle.partitions"] == "4"


@pytest.mark.parametrize(
    "catalog",
    [
        {"type": "sql", "uri": "jdbc:postgresql://db/catalog", "jdbc.user": "bench"},
        {"type": "glue", "warehouse": "123456789012", "glue.skip-name-validation": "true"},
    ],
)
def test_the_scan_reads_tables_from_any_catalog_the_run_used(catalog: dict[str, str]) -> None:
    site = local_site()
    storage = {key: value for key, value in site.catalog_props.items() if key.startswith("s3.")}
    site = dataclasses.replace(site, catalog_props={**catalog, **storage})
    conf = conf_lines(scan_conf.render(site, cores=2, memory_mb=1024))
    assert conf["spark.sql.catalog.scan.uri"] == "jdbc:sqlite:/tmp/upsert-scan-catalog.db"
    assert conf["spark.sql.catalog.scan.warehouse"] == site.warehouse
    assert conf["spark.sql.catalog.scan.s3.endpoint"] == "http://minio:9000"
    # Only storage settings travel: the run's catalog type and its own settings do not.
    assert not [key for key in conf if key.startswith("spark.sql.catalog.scan.") and ("jdbc" in key or "glue" in key)]
    assert "spark.sql.catalog.scan.type" not in conf


def test_secret_backed_settings_resolve_inside_the_scan(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SCAN_SECRET", "resolved")
    settings = [("spark.sql.catalog.scan.s3.secret-access-key", "${env:SCAN_SECRET}"), ("spark.app.name", "scan")]
    assert upsert_scan.resolved_settings(settings) == {
        "spark.sql.catalog.scan.s3.secret-access-key": "resolved",
        "spark.app.name": "scan",
    }
    monkeypatch.delenv("SCAN_SECRET")
    with pytest.raises(ValueError, match="SCAN_SECRET"):
        upsert_scan.resolved_settings(settings)


def write_manifest(live: Path, metadata: str, snapshots: dict[str, float]) -> None:
    live.mkdir(parents=True, exist_ok=True)
    (live / upsert_verify.SCAN_MANIFEST).write_text(
        json.dumps({"metadata": metadata, "snapshots": snapshots, "parallelism": 4})
    )


def test_scan_seconds_come_from_a_complete_manifest(tmp_path: Path) -> None:
    write_manifest(tmp_path, "s3://runs/r/table-metadata.final.json", {"1": 2.5, "2": 3.0})
    seconds = upsert_verify.read_scan_manifest(str(tmp_path), "s3://runs/r/table-metadata.final.json", [1, 2])
    assert seconds == {1: 2.5, 2: 3.0}


@pytest.mark.parametrize(
    ("metadata", "snapshots", "message"),
    [
        ("s3://runs/r/table-metadata.final.json", {"1": 2.5}, "snapshot 2"),
        ("s3://runs/other.json", {"1": 2.5, "2": 3.0}, "different metadata"),
        ("s3://runs/r/table-metadata.final.json", {"1": -1.0, "2": 3.0}, "duration"),
        ("s3://runs/r/table-metadata.final.json", {"1": math.inf, "2": 3.0}, "duration"),
    ],
)
def test_an_unusable_manifest_is_refused(
    tmp_path: Path, metadata: str, snapshots: dict[str, float], message: str
) -> None:
    write_manifest(tmp_path, metadata, snapshots)
    with pytest.raises(ValueError, match=message):
        upsert_verify.read_scan_manifest(str(tmp_path), "s3://runs/r/table-metadata.final.json", [1, 2])


def test_a_scan_without_a_manifest_is_refused(tmp_path: Path) -> None:
    (tmp_path / "live-1").mkdir()
    with pytest.raises(ValueError, match="scan.json"):
        upsert_verify.read_scan_manifest(str(tmp_path), "s3://runs/r/table-metadata.final.json", [1])
