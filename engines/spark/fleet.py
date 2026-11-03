# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Report Spark's declared role counts, machine types and heap sizing.

Cluster costs use captured pod requests, including non-heap overhead.
"""

from __future__ import annotations

from engines.spark.knobs import read
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, FleetRole, RunSpec

_MB_PER_GIB = 1024


def fleet(spec: RunSpec) -> tuple[FleetRole, ...]:
    """Return driver and executor roles, including the driver's compute cost."""
    knobs = read(spec.engine_block)
    # Use the shared missing-machine sentinel recognized by collect.validate.
    machine_type = knobs.machine_type or MACHINE_TYPE_UNSPECIFIED
    return (
        FleetRole(
            "driver",
            1,
            knobs.driver_cores,
            knobs.driver_mem_mb / _MB_PER_GIB,
            knobs.driver_machine_type or machine_type,
        ),
        FleetRole("executor", knobs.executors, knobs.executor_cores, knobs.executor_mem_mb / _MB_PER_GIB, machine_type),
    )
