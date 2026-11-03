# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Declare the rewrite sidecar's role counts, machine types and heap sizing.

Cluster costs use captured pod requests, including non-heap overhead. The
sidecar's long-lived fleet is charged on top of the ingest engine's fleet.
"""

from __future__ import annotations

from compactors.spark_rewrite.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

_MB_PER_GIB = 1024

DRIVER_ROLE = "compactor-driver"
EXECUTOR_ROLE = "compactor-executor"


def fleet(spec: RunSpec) -> tuple[FleetRole, ...]:
    """Return the sidecar's driver and executor roles."""
    if spec.compaction is None:
        raise ValueError(f"run {spec.name!r} declares no compaction layer, so it has no compactor fleet")
    knobs = read(spec.compaction.driver_block)
    # Use the shared missing-machine sentinel recognized by collect.validate.
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return (
        FleetRole(
            DRIVER_ROLE,
            1,
            knobs.driver_cores,
            knobs.driver_mem_mb / _MB_PER_GIB,
            knobs.driver_machine_type or machine_type,
        ),
        FleetRole(
            EXECUTOR_ROLE,
            knobs.executors,
            knobs.executor_cores,
            knobs.executor_mem_mb / _MB_PER_GIB,
            machine_type,
        ),
    )
