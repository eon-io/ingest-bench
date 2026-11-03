# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Commands to create, inspect, and drop run tables.

``--ddl-only`` renders a statement for engine-owned tables without contacting
a catalog. ``table-metadata`` prints the current metadata URI for collection.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from pyiceberg.exceptions import NoSuchTableError

from ingest_bench.catalog import load_catalog_props, open_catalog, parse_key_values, table_identifier
from ingest_bench.corpus import metadata
from ingest_bench.table import ddl
from ingest_bench.table.create import create_table, drop_table, parse_partition


def add_catalog_arguments(parser: argparse.ArgumentParser, *, table_required: bool = True) -> None:
    """Add table and catalog options.

    Make the table optional for commands that also accept a metadata file.
    """
    parser.add_argument("--table", required=table_required, metavar="NAMESPACE.NAME", help="the table to act on")
    parser.add_argument(
        "--catalog-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="a catalog property (uri=…, warehouse=…, token=…)",
    )
    parser.add_argument(
        "--catalog-prop-file",
        action="append",
        default=[],
        metavar="FILE",
        help="file containing one catalog KEY=VALUE property per line; repeat for multiple files. "
        "Use files for credentials to avoid exposing them in command-line arguments",
    )


def build_create_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="create-table", description="Create an Iceberg table from a corpus schema.")
    add_catalog_arguments(parser)
    parser.add_argument(
        "--corpus",
        required=True,
        metavar="URI",
        help="source corpus whose schema defines the table's columns and types",
    )
    parser.add_argument(
        "--partition",
        required=True,
        metavar="SPEC",
        help="partition scheme: identity(<column>), bucket(<N>, <column>), or unpartitioned",
    )
    parser.add_argument(
        "--table-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Iceberg table property applied at creation; repeat for multiple properties. "
        "Use for compression, row-group size, and dictionary settings",
    )
    parser.add_argument(
        "--location",
        metavar="URI",
        help="the table's location root, for a catalog that cannot derive one from its warehouse",
    )
    parser.add_argument(
        "--ddl-only",
        action="store_true",
        help="print the equivalent Spark SQL and exit, for an engine that creates its own table",
    )
    return parser


def build_drop_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="drop-table", description="Drop a run's table if it exists.")
    add_catalog_arguments(parser)
    return parser


# Use a distinct exit code for an absent table so teardown can distinguish
# it from catalog failures.
TABLE_ABSENT = 3


def build_metadata_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="table-metadata", description="Print the location of a table's current metadata file."
    )
    add_catalog_arguments(parser)
    return parser


def create(argv: Sequence[str] | None = None) -> int:
    parser = build_create_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        # Validate the identifier before contacting the catalog.
        table_identifier(table)
        partition = parse_partition(str(args.partition))
        properties = parse_key_values([str(prop) for prop in args.table_prop], "--table-prop")
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    meta = metadata.read(str(args.corpus))
    if args.ddl_only:
        print(ddl.spark_sql_ddl(meta, table, partition, properties))
        return 0
    location = None if args.location is None else str(args.location)
    created = create_table(props, table, meta, partition, properties, location=location)
    print(f"created {table} at {created.location()}")
    return 0


def metadata_location(argv: Sequence[str] | None = None) -> int:
    """Print the catalog's current metadata URI; exit ``TABLE_ABSENT`` if absent."""
    parser = build_metadata_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        identifier = table_identifier(table)
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    try:
        loaded = open_catalog(props).load_table(identifier)
    except NoSuchTableError:
        print(f"table {table} does not exist in this catalog; no metadata is available", file=sys.stderr)
        return TABLE_ABSENT
    print(loaded.metadata_location)
    return 0


def drop(argv: Sequence[str] | None = None) -> int:
    parser = build_drop_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        table_identifier(table)
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    drop_table(props, table)
    print(f"dropped {table}")
    return 0
