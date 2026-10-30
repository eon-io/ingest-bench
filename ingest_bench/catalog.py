# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Shared catalog client creation and property loading.

Select the catalog implementation from its properties so every command honors
the same configuration.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path

from pyiceberg.catalog import Catalog, load_catalog

from ingest_bench.specs.env import resolve_env_placeholders


def _parse_key_value(raw: str, source: str) -> tuple[str, str]:
    """Parse a ``KEY=VALUE`` setting, reporting malformed input with its source."""
    if "=" not in raw:
        raise ValueError(f"{source} must contain KEY=VALUE, got {raw!r}")
    key, value = raw.split("=", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"{source} has an empty key")
    return key, value.strip()


def parse_key_values(pairs: Sequence[str], source: str) -> dict[str, str]:
    return dict(_parse_key_value(pair, source) for pair in pairs)


def load_catalog_props(values: Sequence[str], files: Sequence[str] = ()) -> dict[str, str]:
    """Load catalog properties from files, then apply command-line overrides.

    Skip blank lines and comments. Resolve ``${env:NAME}`` references before
    returning. Use files or environment references for credentials to avoid
    exposing them in process arguments.
    """
    props: dict[str, str] = {}
    for filename in files:
        path = Path(filename)
        try:
            lines = path.read_text().splitlines()
        except OSError as error:
            raise ValueError(f"could not read catalog property file {path}: {error}") from error
        for line_number, line in enumerate(lines, 1):
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            key, value = _parse_key_value(stripped, f"{path}:{line_number}")
            props[key] = value
    for raw in values:
        key, value = _parse_key_value(raw, "--catalog-prop")
        props[key] = value
    return resolve_env_placeholders(props)


def open_catalog(props: dict[str, str]) -> Catalog:
    """Open the catalog implementation selected by ``props``."""
    return load_catalog("bench", **props)


def table_identifier(name: str) -> tuple[str, str]:
    """Return ``(namespace, table)`` from a two- or three-part table name.

    Drop the optional catalog segment because the client is already scoped to
    that catalog. Namespaces are always a single segment.
    """
    parts = name.split(".")
    if len(parts) not in (2, 3) or not all(parts):
        raise ValueError(f"table must be namespace.name or catalog.namespace.name, got {name!r}")
    return parts[-2], parts[-1]
