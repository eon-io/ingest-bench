# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Report Flink's declared role counts, machine types and sizing.

Cluster costs use captured pod requests, including effective overrides.
"""

from __future__ import annotations

from engines.flink.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

JOBMANAGER = "jobmanager"
TASKMANAGER = "taskmanager"

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> list[FleetRole]:
    """Return one JobManager role and the requested TaskManager fleet."""
    knobs = read(spec.engine_block)
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return [
        FleetRole(JOBMANAGER, 1, knobs.jm_cpu, knobs.jm_mem_mb / _MB_PER_GIB, knobs.jm_machine_type or machine_type),
        FleetRole(TASKMANAGER, knobs.taskmanagers, knobs.tm_cpu, knobs.tm_mem_mb / _MB_PER_GIB, machine_type),
    ]
