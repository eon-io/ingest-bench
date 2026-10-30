# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate the Flink maintenance sidecar's knobs and render the files that start it.

The sidecar is Iceberg's own Flink table maintenance (``TableMaintenance`` with
one ``RewriteDataFiles`` task) run as a second FlinkDeployment on the Flink
engine's image, operator and catalog settings. It shares nothing else with the
ingest job: it reads no Kafka, keeps no checkpoint, and rewrites the table
beside whichever engine is appending to it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, replace
from typing import cast

import yaml

from engines.flink import knobs as engine
from engines.java_properties import catalog_properties
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.specs.compactors import CONFIGMAP_FILE, DOCUMENT_FILE
from ingest_bench.specs.derive import Derived, utc_stamp
from ingest_bench.specs.env import PLACEHOLDER_FORM, has_placeholder
from ingest_bench.specs.kubernetes import NAME, CompactorKubernetes, object_name
from ingest_bench.specs.model import CompactionSpec, RunSpec, SiteConfig

# The document the runner reads its table, cadence and options from, the Flink
# settings shared by both deployment modes, and the sizing Compose needs before
# it can start the sidecar's own cluster.
JOB_FILE = "compactor-job.yaml"
CONF_FILE = "compactor-flink-conf.yaml"
ENV_FILE = "compactor-flink.env"

# Bundled in the Flink image beside the SQL runner.
_RUNNER_JAR = "local:///opt/bench/maintenance-runner.jar"
_RUNNER_ENTRY_CLASS = "org.ingestbench.flink.MaintenanceRunner"

# Distinguish the sidecar's deployment from the engine's. The operator derives
# Service names from it and enforces the same 45-character limit it holds the
# engine to, so a run with this sidecar has two fewer characters for its name.
NAME_SUFFIX = "-m"
OBJECT_NAME_LIMIT = engine.KUBERNETES.max_object_name_length or 45

# How far back the monitor reads the table's history when it starts; harmless
# on the fresh table a run creates.
MAX_READ_BACK = 100

# The Flink operator labels both deployments' pods the same way; the run name
# in the selector is the sidecar's own object name.
KUBERNETES = CompactorKubernetes(
    fleet_selector=f"app={NAME}",
    pod_role_label="component",
    role_map=(("jobmanager", "compactor-jobmanager"), ("taskmanager", "compactor-taskmanager")),
)

# Reject unknown keys so misspelled tuning options cannot be silently ignored.
KNOBS: dict[str, type] = {
    "rewrite_parallelism": int,
    "slots": int,
    "tm_cpu": int,
    "tm_mem_mb": int,
    "jm_cpu": int,
    "jm_mem_mb": int,
    "rate_limit_s": int,
    "lock_check_delay_s": int,
    "rewrite_options": dict,
    "machine_type": str,
    "jm_machine_type": str,
    "extra_flink_conf": dict,
    "node_selector": dict,
    "tolerations": list,
}

# Require explicit fleet sizing. Everything else has a defensible default.
REQUIRED_KNOBS = frozenset({"rewrite_parallelism", "slots", "tm_cpu", "tm_mem_mb"})

WHERE = "spec.compaction.flink-rewrite"

# The options the runner maps to RewriteDataFiles builder calls: the shared
# planner options plus the builder's name for the byte cap.
REWRITE_OPTIONS: dict[str, engine.RewriteOptionKind] = {**engine.PLANNER_REWRITE_OPTIONS, "max-rewrite-bytes": int}
TARGET_OPTION = "target-file-size-bytes"


def _int_at(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _str_at(value: object, where: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _string_map_at(value: object, where: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping of setting to value, got {value!r}")
    return {str(key): _str_at(entry, f"{where}.{key}") for key, entry in cast(dict[object, object], value).items()}


def _string_maps_at(value: object, where: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list of mappings, got {value!r}")
    return [_string_map_at(entry, f"{where}[{index}]") for index, entry in enumerate(cast(list[object], value))]


def _rewrite_options_at(value: object, where: str) -> dict[str, str]:
    """Return the options as strings, each one the runner knows how to apply."""
    options = _string_map_at(value, where)
    if TARGET_OPTION in options:
        raise ValueError(
            f"{where} may not set {TARGET_OPTION!r}: Iceberg reads the target from the table's own "
            "write.target-file-size-bytes, which spec.table.properties declares once for both arms"
        )
    engine.validate_rewrite_options(options, where, REWRITE_OPTIONS)
    return options


@dataclass(frozen=True)
class Knobs:
    """Sidecar sizing, cadence limits and rewrite options with defaults applied."""

    # Concurrent file-group rewrites: the Rewrite operator's parallelism, one
    # group in flight per subtask.
    rewrite_parallelism: int
    slots: int
    tm_cpu: int
    tm_mem_mb: int
    jm_cpu: int
    jm_mem_mb: int
    # The monitor's poll period and the least time between two passes.
    rate_limit_s: int
    # How often a trigger blocked by a running pass re-checks the lock.
    lock_check_delay_s: int
    rewrite_options: dict[str, str]
    machine_type: str | None
    jm_machine_type: str | None
    extra_flink_conf: dict[str, str]
    # Added to the site's placement, which applies to every pod the harness
    # starts; this is how the sidecar gets nodes of its own.
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]

    def taskmanagers(self) -> int:
        """TaskManagers holding the rewrite subtasks; the singleton operators share their slots."""
        return math.ceil(self.rewrite_parallelism / self.slots)


def read(block: dict[str, object]) -> Knobs:
    """Validate the knob block and apply explicit defaults."""
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"{WHERE} has unknown keys {unknown}; supported keys are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"{WHERE} must set {missing}")

    def count(name: str, default: int | None = None) -> int:
        value = default if default is not None and name not in block else _int_at(block[name], f"{WHERE}.{name}")
        if value < 1:
            raise ValueError(f"{WHERE}.{name} must be at least 1, got {value}")
        return value

    return Knobs(
        rewrite_parallelism=count("rewrite_parallelism"),
        slots=count("slots"),
        tm_cpu=count("tm_cpu"),
        tm_mem_mb=count("tm_mem_mb"),
        jm_cpu=count("jm_cpu", 1),
        jm_mem_mb=count("jm_mem_mb", 2048),
        rate_limit_s=count("rate_limit_s", 15),
        lock_check_delay_s=count("lock_check_delay_s", 10),
        rewrite_options=(
            {}
            if "rewrite_options" not in block
            else _rewrite_options_at(block["rewrite_options"], f"{WHERE}.rewrite_options")
        ),
        machine_type=None if "machine_type" not in block else _str_at(block["machine_type"], f"{WHERE}.machine_type"),
        jm_machine_type=(
            None if "jm_machine_type" not in block else _str_at(block["jm_machine_type"], f"{WHERE}.jm_machine_type")
        ),
        extra_flink_conf=(
            {}
            if "extra_flink_conf" not in block
            else _string_map_at(block["extra_flink_conf"], f"{WHERE}.extra_flink_conf")
        ),
        node_selector=(
            {} if "node_selector" not in block else _string_map_at(block["node_selector"], f"{WHERE}.node_selector")
        ),
        tolerations=(
            [] if "tolerations" not in block else _string_maps_at(block["tolerations"], f"{WHERE}.tolerations")
        ),
    )


def _compaction(spec: RunSpec) -> CompactionSpec:
    if spec.compaction is None or spec.compaction.interval_s is None:
        raise ValueError(f"run {spec.name!r} declares no maintenance sidecar to render")
    return spec.compaction


def kubernetes_name(run_id: str) -> str:
    """Return the sidecar's deployment name, distinct from the engine's."""
    return object_name(run_id) + NAME_SUFFIX


def configmap_name(derived: Derived) -> str:
    return f"{kubernetes_name(derived.run_id)}-job"


def validate(block: dict[str, object], spec: RunSpec, meta: CorpusMetadata) -> None:
    """Validate the sidecar's block before staging creates anything.

    ``meta`` is retained for the shared driver validator interface. The
    rewrite target is not validated here: Iceberg reads it from the table's
    own ``write.target-file-size-bytes``.
    """
    knobs = read(block)
    compaction = _compaction(spec)
    interval_s = cast(int, compaction.interval_s)
    if knobs.rate_limit_s > interval_s:
        raise ValueError(
            f"{WHERE}.rate_limit_s {knobs.rate_limit_s} is longer than spec.compaction.interval_s {interval_s}: "
            "the trigger fires at most once per rate limit, so passes would run on the rate limit instead"
        )
    stamp = utc_stamp()
    name = kubernetes_name(f"{spec.name}-{stamp}")
    if len(name) > OBJECT_NAME_LIMIT:
        allowed = OBJECT_NAME_LIMIT - len(NAME_SUFFIX) - len(stamp) - 1
        raise ValueError(
            f"spec.name {spec.name!r} is {len(spec.name)} characters, and the maintenance sidecar's deployment "
            f"name is that plus the run's stamp and {NAME_SUFFIX!r}: {len(name)} characters, past the "
            f"{OBJECT_NAME_LIMIT} the Flink operator accepts. Name this run in at most {allowed} characters"
        )


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def render_conf(spec: RunSpec) -> dict[str, str]:
    """Render the sidecar deployment's Flink settings.

    No checkpoint interval: the maintenance operators hold nothing a run needs
    to recover, and a checkpoint would only wait on the file group in flight.
    A failed job stays failed for the same reason the Spark sidecar is never
    restarted: a restart would compact one stretch twice and leave a gap.
    """
    knobs = read(_compaction(spec).driver_block)
    conf = {
        "parallelism.default": str(knobs.rewrite_parallelism),
        "taskmanager.numberOfTaskSlots": str(knobs.slots),
        "taskmanager.memory.process.size": f"{knobs.tm_mem_mb}m",
        "jobmanager.memory.process.size": f"{knobs.jm_mem_mb}m",
        # No state backend or batch operator runs here, so managed memory would
        # sit idle; leave it to the heap the rewrite readers and writers use.
        "taskmanager.memory.managed.size": "0m",
        "restart-strategy.type": "none",
    }
    conf.update(knobs.extra_flink_conf)
    referenced = sorted(key for key, value in conf.items() if has_placeholder(value))
    if referenced:
        raise ValueError(
            f"the maintenance sidecar renders {referenced} as Flink settings, and nothing resolves a "
            f"{PLACEHOLDER_FORM} in one"
        )
    return conf


def _conf_text(spec: RunSpec) -> str:
    """Render settings as flat `key: value` lines, which Flink's config.yaml accepts."""
    return "".join(f"{key}: {value}\n" for key, value in sorted(render_conf(spec).items()))


def render_job(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """Render the document the runner builds its maintenance job from.

    The catalog properties are the ingest job's ``CREATE CATALOG`` options less
    the connector's own two keys; the runner resolves any environment
    references in them the way the SQL runner does.
    """
    compaction = _compaction(spec)
    knobs = read(compaction.driver_block)
    document = {
        "catalog": engine.CATALOG_NAME,
        "catalog_properties": catalog_properties(site.catalog_props, site.warehouse, engine="Flink"),
        "table": derived.table,
        "uid_suffix": derived.run_id,
        "parallelism": knobs.rewrite_parallelism,
        "interval_s": compaction.interval_s,
        "rate_limit_s": knobs.rate_limit_s,
        "lock_check_delay_s": knobs.lock_check_delay_s,
        "max_read_back": MAX_READ_BACK,
        "rewrite_options": knobs.rewrite_options,
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_env(spec: RunSpec) -> str:
    """Render the sizing Compose needs before the sidecar's cluster starts."""
    knobs = read(_compaction(spec).driver_block)
    return (
        f"COMPACTOR_TASKMANAGERS={knobs.taskmanagers()}\n"
        f"COMPACTOR_SLOTS={knobs.slots}\n"
        f"COMPACTOR_TM_MEM_MB={knobs.tm_mem_mb}\n"
        f"COMPACTOR_JM_MEM_MB={knobs.jm_mem_mb}\n"
        f"COMPACTOR_PARALLELISM={knobs.rewrite_parallelism}\n"
    )


def render_flinkdeployment(spec: RunSpec, site: SiteConfig, derived: Derived, image_tag: str) -> str:
    """Render the sidecar's FlinkDeployment on the engine's image and pod template."""
    knobs = read(_compaction(spec).driver_block)
    cluster = engine.cluster_for(site)
    cluster = replace(
        cluster,
        node_selector={**cluster.node_selector, **knobs.node_selector},
        tolerations=[*cluster.tolerations, *knobs.tolerations],
    )
    conf = render_conf(spec)
    document: dict[str, object] = {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": kubernetes_name(derived.run_id), "namespace": cluster.namespace},
        "spec": {
            "image": f"{cluster.registry}/{engine.IMAGE_REPOSITORY}:{image_tag}",
            "flinkVersion": engine.FLINK_VERSION_LABEL,
            "mode": "standalone",
            "serviceAccount": cluster.flink_service_account,
            "flinkConfiguration": conf,
            "jobManager": {"resource": {"memory": conf["jobmanager.memory.process.size"], "cpu": knobs.jm_cpu}},
            "taskManager": {
                "resource": {"memory": conf["taskmanager.memory.process.size"], "cpu": knobs.tm_cpu},
                "replicas": knobs.taskmanagers(),
            },
            "job": {
                "jarURI": _RUNNER_JAR,
                "entryClass": _RUNNER_ENTRY_CLASS,
                # Not `--job`: the operator's application entrypoint reads it as an
                # ambiguous abbreviation of its own `--job-classname` and `--job-id`.
                "args": ["--maintenance", f"{engine.RUN_MOUNT}/{JOB_FILE}"],
                "parallelism": knobs.rewrite_parallelism,
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": engine.pod_template(cluster, configmap_name(derived)),
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_configmap(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """Render the ConfigMap holding the runner's job document."""
    cluster = engine.cluster_for(site)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name(derived), "namespace": cluster.namespace},
        "data": {JOB_FILE: render_job(spec, site, derived)},
    }
    return yaml.safe_dump(document, sort_keys=False)


def render(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, *, image_tag: str | None = None
) -> dict[str, str]:
    """Return the sidecar's rendered files keyed by filename.

    ``meta`` is retained for the shared driver renderer interface. A site with
    no cluster receives the job's own files, which record what a sidecar would
    have been asked to do; only a cluster site gets manifests to start it.
    """
    files = {
        JOB_FILE: render_job(spec, site, derived),
        CONF_FILE: _conf_text(spec),
        ENV_FILE: render_env(spec),
    }
    if site.kubernetes is None:
        return files
    if image_tag is None:
        raise ValueError("render requires image_tag for Kubernetes runs; use the tag of the published image")
    files[DOCUMENT_FILE] = render_flinkdeployment(spec, site, derived, image_tag)
    files[CONFIGMAP_FILE] = render_configmap(spec, site, derived)
    return files
