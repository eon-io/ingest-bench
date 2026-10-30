# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Register managed engine modules and load them on demand.

Lazy imports let spec readers run without engine-specific dependencies.
"""

from __future__ import annotations

import importlib
from types import ModuleType

from ingest_bench.specs.kubernetes import EngineKubernetes

MANAGED: dict[str, str] = {"flink": "engines.flink.knobs", "spark": "engines.spark.knobs"}

KNOBS_MODULE = "knobs"
FLEET_MODULE = "fleet"
VERIFY_MODULE = "verify"

# The attribute a knobs module declares its cluster shape under.
KUBERNETES = "KUBERNETES"


def _engine_module(engine: str, submodule: str) -> ModuleType:
    """Import an engine module using its registered package.

    Derive the package from the knobs registry to keep module lookup consistent.
    """
    if engine not in MANAGED:
        raise ValueError(f"{engine!r} is not a managed engine; the registered ones are {sorted(MANAGED)}")
    package = MANAGED[engine].rsplit(".", 1)[0]
    name = f"{package}.{submodule}"
    try:
        return importlib.import_module(name)
    except ImportError as error:
        raise ValueError(
            f"managed engine {engine!r} declares module {name!r}, which does not import: {error}"
        ) from error


def knobs_for(engine: str) -> ModuleType:
    """The knobs module of a managed engine.

    Every such module exposes ``validate(block, spec, meta) -> None`` and
    ``render(spec, site, derived, meta, *, image_tag) -> dict[str, str]``, the
    second returning the files to write into the run directory keyed by
    filename. ``image_tag`` is the tag of the images a run on a cluster starts,
    and is ``None`` for a site that declares no cluster.
    """
    return _engine_module(engine, KNOBS_MODULE)


def fleet_for(engine: str) -> ModuleType:
    """The fleet module of a managed engine.

    Every such module exposes ``fleet(spec) -> list[FleetRole]``: declared role
    counts, machine types and sizing estimates. Cluster cost uses pod requests.
    """
    return _engine_module(engine, FLEET_MODULE)


def verify_for(engine: str) -> ModuleType:
    """The verify module of a managed engine.

    Every such module exposes ``verify(spec, run_id, fetch, ...) -> list[str]``:
    one line per setting the running engine does not honour, and an empty list
    for a run it does. An engine whose descriptor names a `pods_selector` takes
    the pod list as a further argument, since some of what it checks is the
    shape of the pods rather than anything the engine reports about itself.
    """
    return _engine_module(engine, VERIFY_MODULE)


def kubernetes_for(engine: str) -> EngineKubernetes:
    """Return the descriptor declared beside the engine's rendering code."""
    module = knobs_for(engine)
    if not hasattr(module, KUBERNETES):
        raise ValueError(
            f"managed engine {engine!r} declares no {KUBERNETES}, so nothing here knows how to address it on a cluster"
        )
    declared = getattr(module, KUBERNETES)
    if not isinstance(declared, EngineKubernetes):
        raise ValueError(f"managed engine {engine!r} declares {KUBERNETES} as {type(declared).__name__}")
    return declared
