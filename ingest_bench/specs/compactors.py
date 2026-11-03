# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Register compaction drivers and load them on demand.

A compaction driver is the layer that rewrites a run's data files. It is
registered apart from the ingest engine because the two vary independently:
any engine can run with a sidecar compactor, without one, or against an engine
that compacts as it writes.

Drivers declare no verification modules. Compaction results are read uniformly
from table metadata by ``ingest_bench.scorer.compaction``.
"""

from __future__ import annotations

import importlib
from types import ModuleType

from ingest_bench.specs.kubernetes import CompactorKubernetes

MANAGED: dict[str, str] = {
    "spark-rewrite": "compactors.spark_rewrite.knobs",
    "flink-rewrite": "compactors.flink_rewrite.knobs",
}

# The results comparison each managed driver's runs belong to. Rows compare only
# with rows that did the same rewriting work.
COMPARISON_CLASSES: dict[str, str] = {
    "spark-rewrite": "spark-compaction",
    "flink-rewrite": "flink-maintenance",
}

KNOBS_MODULE = "knobs"
FLEET_MODULE = "fleet"
KUBERNETES = "KUBERNETES"

# The files a managed driver renders for the cluster. Shell drivers apply and
# delete them by name; compactor descriptors identify pods but expose no
# lifecycle or status fields.
DOCUMENT_FILE = "compactor.yaml"
CONFIGMAP_FILE = "compactor-configmap.yaml"


def _driver_module(driver: str, submodule: str) -> ModuleType:
    """Import a driver module using its registered package."""
    if driver not in MANAGED:
        raise ValueError(f"{driver!r} is not a managed compaction driver; the registered ones are {sorted(MANAGED)}")
    package = MANAGED[driver].rsplit(".", 1)[0]
    name = f"{package}.{submodule}"
    try:
        return importlib.import_module(name)
    except ImportError as error:
        raise ValueError(
            f"managed compaction driver {driver!r} declares module {name!r}, which does not import: {error}"
        ) from error


def knobs_for(driver: str) -> ModuleType:
    """The knobs module of a managed compaction driver.

    Every such module exposes ``validate(block, spec, meta) -> None`` and
    ``render(spec, site, derived, meta, *, image_tag) -> dict[str, str]``, the
    second returning the files to write into the run directory keyed by
    filename. ``image_tag`` is the tag of the images a run on a cluster starts,
    and is ``None`` for a site that declares no cluster.
    """
    return _driver_module(driver, KNOBS_MODULE)


def fleet_for(driver: str) -> ModuleType:
    """The fleet module of a managed compaction driver.

    Every such module exposes ``fleet(spec) -> tuple[FleetRole, ...]``: the
    compute the compaction layer holds for the run, which is charged on top of
    the ingest engine's own.
    """
    return _driver_module(driver, FLEET_MODULE)


def kubernetes_for(driver: str) -> CompactorKubernetes:
    """Return the pod fleet descriptor declared beside the driver's renderer."""
    module = knobs_for(driver)
    if not hasattr(module, KUBERNETES):
        raise ValueError(
            f"managed compaction driver {driver!r} declares no {KUBERNETES}, "
            "so nothing here knows how to identify its pods"
        )
    declared = getattr(module, KUBERNETES)
    if not isinstance(declared, CompactorKubernetes):
        raise ValueError(
            f"managed compaction driver {driver!r} declares {KUBERNETES} as "
            f"{type(declared).__name__}, not CompactorKubernetes"
        )
    return declared
