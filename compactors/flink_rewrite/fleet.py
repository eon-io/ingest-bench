# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Declare the maintenance sidecar's role counts, machine types and sizing.

Cluster costs use captured pod requests, including non-heap overhead. The
sidecar's fleet is charged on top of the ingest engine's for the whole run.
"""

from __future__ import annotations

from compactors.flink_rewrite.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

_MB_PER_GIB = 1024

JOBMANAGER_ROLE = "compactor-jobmanager"
TASKMANAGER_ROLE = "compactor-taskmanager"


def fleet(spec: RunSpec) -> tuple[FleetRole, ...]:
    """Return the sidecar's JobManager and TaskManager roles."""
    if spec.compaction is None:
        raise ValueError(f"run {spec.name!r} declares no compaction layer, so it has no compactor fleet")
    knobs = read(spec.compaction.driver_block)
    # Use the shared missing-machine sentinel recognized by collect.validate.
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return (
        FleetRole(
            JOBMANAGER_ROLE, 1, knobs.jm_cpu, knobs.jm_mem_mb / _MB_PER_GIB, knobs.jm_machine_type or machine_type
        ),
        FleetRole(TASKMANAGER_ROLE, knobs.taskmanagers(), knobs.tm_cpu, knobs.tm_mem_mb / _MB_PER_GIB, machine_type),
    )
