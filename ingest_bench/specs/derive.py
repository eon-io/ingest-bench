# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Derive consistent topic, table, and directory names from a run spec and stamp."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from ingest_bench import uri
from ingest_bench.specs.model import RunSpec, SiteConfig

STAMP_FORMAT = "%Y%m%dT%H%M%SZ"

# Use one namespace to make run tables easy to find.
TABLE_NAMESPACE = "ingest_bench"


@dataclass(frozen=True)
class Derived:
    run_id: str
    topic: str
    table: str
    run_root: str
    corpus_uri: str


def utc_stamp() -> str:
    return datetime.now(UTC).strftime(STAMP_FORMAT)


def derive(spec: RunSpec, site: SiteConfig, *, stamp: str | None = None, corpus_dir: str) -> Derived:
    """Derive resource names for the resolved corpus directory.

    An explicit ``stamp`` allows callers to reconstruct an existing run's names.
    """
    resolved = utc_stamp() if stamp is None else stamp
    run_id = f"{spec.name}-{resolved}"
    return Derived(
        run_id=run_id,
        topic=run_id,
        # Normalize hyphens once so engine SQL needs no identifier quoting.
        table=f"{TABLE_NAMESPACE}.t_{run_id.replace('-', '_')}",
        run_root=uri.join(site.runs_root, run_id),
        corpus_uri=uri.join(site.corpus_root, corpus_dir),
    )
