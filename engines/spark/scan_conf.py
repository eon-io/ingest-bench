# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Render the Spark settings for scanning a frozen upsert table, for any engine's run.

The scan registers the run's frozen metadata in a session-local JDBC catalog backed by a SQLite
file inside the container, so it needs no live catalog and reads table files through the same
file IO the engines use, whatever catalog the run used: only storage settings carry over.
Iceberg's in-memory catalog cannot serve: it reads only its own in-memory files. A file, not
SQLite's in-memory mode, because each pooled connection would get its own empty in-memory
database. Parquet output goes through Hadoop S3A.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from engines.java_properties import storage_properties
from engines.spark.knobs import _ICEBERG_EXTENSIONS, _S3A_FROM_CATALOG_PROP, _SPARK_CATALOG_CLASS
from ingest_bench.specs.model import SiteConfig, load_site

SCAN_CATALOG = "scan"
JDBC_CATALOG = "org.apache.iceberg.jdbc.JdbcCatalog"
SESSION_DATABASE = "jdbc:sqlite:/tmp/upsert-scan-catalog.db"


def render(site: SiteConfig, *, cores: int, memory_mb: int) -> str:
    """Return a spark-submit properties file for the scan."""
    prefix = f"spark.sql.catalog.{SCAN_CATALOG}"
    props = storage_properties(site.catalog_props, site.warehouse)
    conf = {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        prefix: _SPARK_CATALOG_CLASS,
        f"{prefix}.catalog-impl": JDBC_CATALOG,
        f"{prefix}.uri": SESSION_DATABASE,
        **{f"{prefix}.{key}": value for key, value in props.items()},
        "spark.driver.memory": f"{memory_mb}m",
        "spark.sql.shuffle.partitions": str(cores),
    }
    # A cluster reaches storage through pod identity; static keys exist only on local stacks.
    if site.kubernetes is None:
        conf.update(
            {
                setting: site.catalog_props[prop]
                for prop, setting in _S3A_FROM_CATALOG_PROP
                if prop in site.catalog_props
            }
        )
    return "".join(f"{key} {value}\n" for key, value in conf.items())


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="render-spark-scan", description=__doc__.splitlines()[0])
    parser.add_argument("--site", required=True, type=Path)
    parser.add_argument("--cores", required=True, type=int)
    parser.add_argument("--memory-mb", required=True, type=int)
    parser.add_argument("--out", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.cores < 1 or args.memory_mb < 1:
        parser.error("--cores and --memory-mb must be positive")
    args.out.write_text(render(load_site(args.site), cores=args.cores, memory_mb=args.memory_mb))
    return 0


if __name__ == "__main__":
    sys.exit(main())
