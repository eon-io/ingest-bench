# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Canonical paths for published result documents."""

from datetime import UTC, datetime
from pathlib import Path

from ingest_bench.collect.schema import ResultDocument
from ingest_bench.specs.derive import STAMP_FORMAT
from ingest_bench.workloads import canonical_workload_name


def _path_component(value: str, field: str) -> str:
    if not value or value in {".", ".."} or "/" in value or "\\" in value:
        raise ValueError(f"{field}: must be a single path component")
    return value


def run_timestamp(run_id: str) -> datetime:
    """Return the UTC staging timestamp encoded in a canonical run ID."""
    try:
        return datetime.strptime(run_id.rsplit("-", 1)[-1], STAMP_FORMAT).replace(tzinfo=UTC)
    except ValueError as err:
        raise ValueError("run.run_id: must end with a UTC timestamp in YYYYMMDDTHHMMSSZ format") from err


def result_relative_path(document: ResultDocument) -> Path:
    """Return the canonical path for a parsed result document."""
    run = document["run"]
    corpus = run["corpus"]
    if corpus is None:
        raise ValueError("run.corpus: is null; fetch corpus.json into the run directory to derive a result path")

    staged_at = run_timestamp(run["run_id"])
    engine = _path_component(run["engine"], "run.engine")
    preset = _path_component(canonical_workload_name(corpus["name"]), "run.corpus.name")
    return Path(engine) / preset / staged_at.date().isoformat() / staged_at.strftime("%H-%M-%SZ") / "result.json"
