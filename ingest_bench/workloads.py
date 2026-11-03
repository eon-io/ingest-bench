# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Locate the shipped workloads: schemas, presets and leaderboard tracks.

Kept free of generator imports so spec readers can resolve tracks cheaply.
"""

from __future__ import annotations

import os
from pathlib import Path

WORKLOADS_ENV = "INGEST_BENCH_WORKLOADS"

# Published runs retain their original names and hashes. Only these two labels changed;
# mapping them must not make retired workloads or different corpus hashes equivalent.
WORKLOAD_ALIASES = {
    "events200-1mrps-uniform": "events-1mrps-uniform",
    "events200-1mrps-skew": "events-1mrps-skew",
}


def canonical_workload_name(name: str) -> str:
    """Resolve a historical standard-corpus label without changing its recorded identity."""
    return WORKLOAD_ALIASES.get(name, name)


def workloads_dir(explicit: str | None) -> Path:
    """Resolve the workloads directory from an argument, environment, or checkout."""
    if explicit:
        return Path(explicit)
    from_env = os.environ.get(WORKLOADS_ENV)
    if from_env:
        return Path(from_env)
    return Path(__file__).resolve().parents[1] / "workloads"
