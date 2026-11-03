# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate the rewrite sidecar's knobs and render the files that start it.

The sidecar shares the Spark engine's image, operator and catalog settings, so
it also shares that engine's rendering helpers. It shares nothing else: it
reads no Kafka, keeps no checkpoint, and runs beside whichever engine is
ingesting, including one the harness does not start.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from typing import cast

import yaml

from compactors.spark_rewrite.rewrite_loop import JOB_DOCUMENT, RUN_DIR
from engines.spark.knobs import (
    CATALOG_NAME,
    IMAGE_REPOSITORY,
    SPARK_VERSION,
    catalog_conf,
    cluster_for,
    placement,
    region_env,
    secret_env_from,
)
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.specs.compactors import CONFIGMAP_FILE, DOCUMENT_FILE
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.env import PLACEHOLDER_FORM, has_placeholder
from ingest_bench.specs.kubernetes import NAME, CompactorKubernetes, object_name
from ingest_bench.specs.model import CompactionSpec, KubernetesConfig, RunSpec, SiteConfig

# Enable Iceberg SQL extensions; rewrite_data_files is one of them.
_ICEBERG_EXTENSIONS = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

JOB_FILE = JOB_DOCUMENT.name
CONF_FILE = "compactor-spark-defaults.conf"
ENV_FILE = "compactor-job.env"

# Environment variable names consumed by Compose's submission command. They are
# prefixed because the engine's own submission reads LOCAL_CORES and
# DRIVER_MEM_MB from job.env in the same run directory.
LOCAL_CORES_VAR = "COMPACTOR_LOCAL_CORES"
DRIVER_MEM_VAR = "COMPACTOR_DRIVER_MEM_MB"

# local:// points to the script bundled in the image; the operator need not
# stage a separate file.
_JOB_SCRIPT = "local:///opt/bench/compactors/spark_rewrite/rewrite_loop.py"

_RUN_MOUNT = str(RUN_DIR)
_JOB_VOLUME = "job"

# Distinguish the sidecar's objects from the engine's. The operator publishes
# the application name as a label value, so the suffixed name has to stay
# within Kubernetes' 63-character limit on those; spec.model.NAME_RE caps the
# run name short enough that it does, and a test holds the two together.
NAME_SUFFIX = "-compact"
LABEL_VALUE_LIMIT = 63

KUBERNETES = CompactorKubernetes(
    fleet_selector=f"sparkoperator.k8s.io/app-name={NAME}",
    pod_role_label="spark-role",
    role_map=(("driver", "compactor-driver"), ("executor", "compactor-executor")),
)

# Reject unknown keys so misspelled tuning options cannot be silently ignored.
KNOBS: dict[str, type] = {
    "executors": int,
    "executor_cores": int,
    "executor_slots": int,
    "executor_mem_mb": int,
    "driver_cores": int,
    "driver_mem_mb": int,
    "rewrite_options": dict,
    "start_delay_s": int,
    "machine_type": str,
    "driver_machine_type": str,
    "extra_spark_conf": dict,
    "node_selector": dict,
    "tolerations": list,
}

# Require explicit fleet sizing. Everything else has a defensible default.
REQUIRED_KNOBS = frozenset({"executors", "executor_cores", "executor_mem_mb"})

WHERE = "spec.compaction.spark-rewrite"


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


# Iceberg reads the target from the table's own `write.target-file-size-bytes`,
# which `spec.table.properties` declares once and the reading of what the rewrite
# achieved reads back. A rewrite option of the same name would let the sidecar
# rewrite to a size the spec and the table property both deny, so it is refused
# here as the Flink driver refuses it.
TARGET_OPTION = "target-file-size-bytes"


def _rewrite_options_at(value: object, where: str) -> dict[str, str]:
    options = _string_map_at(value, where)
    if TARGET_OPTION in options:
        raise ValueError(
            f"{where} may not set {TARGET_OPTION!r}: Iceberg reads the target from the table's own "
            "write.target-file-size-bytes, which spec.table.properties declares once for both arms"
        )
    return options


@dataclass(frozen=True)
class Knobs:
    """Sidecar sizing and rewrite options with defaults applied."""

    executors: int
    executor_cores: int
    # Task slots per executor (`spark.executor.cores`). Defaults to the vCPU request; more
    # slots than vCPU let I/O-bound rewrite tasks overlap on the same pod.
    executor_slots: int
    executor_mem_mb: int
    driver_cores: int
    driver_mem_mb: int
    rewrite_options: dict[str, str]
    # Seconds the fleet sits idle after the table's first commit that adds rows. The
    # table meanwhile builds a backlog of small files, so the first passes run
    # saturated and their throughput reads as the sidecar's capacity.
    start_delay_s: int
    machine_type: str | None
    driver_machine_type: str | None
    extra_spark_conf: dict[str, str]
    # Placement for the sidecar's own pods, merged over the site's. Sharing nodes
    # with ingest cost the Flink arm its freshness bound, so both arms have to be
    # able to move compaction off the engine's nodes or neither reading compares.
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]

    def cores_total(self) -> int:
        return self.executors * self.executor_cores

    def slots_total(self) -> int:
        return self.executors * self.executor_slots


def read(block: dict[str, object]) -> Knobs:
    """Validate the knob block and apply explicit defaults."""
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"{WHERE} has unknown keys {unknown}; supported keys are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"{WHERE} must set {missing}")

    executors = _int_at(block["executors"], f"{WHERE}.executors")
    executor_cores = _int_at(block["executor_cores"], f"{WHERE}.executor_cores")
    executor_slots = (
        executor_cores if "executor_slots" not in block else _int_at(block["executor_slots"], f"{WHERE}.executor_slots")
    )
    driver_cores = 1 if "driver_cores" not in block else _int_at(block["driver_cores"], f"{WHERE}.driver_cores")
    for name, count in (
        ("executors", executors),
        ("executor_cores", executor_cores),
        ("executor_slots", executor_slots),
        ("driver_cores", driver_cores),
    ):
        if count < 1:
            raise ValueError(f"{WHERE}.{name} must be at least 1, got {count}")
    start_delay_s = 0 if "start_delay_s" not in block else _int_at(block["start_delay_s"], f"{WHERE}.start_delay_s")
    if start_delay_s < 0:
        raise ValueError(f"{WHERE}.start_delay_s must not be negative, got {start_delay_s}")

    return Knobs(
        executors=executors,
        executor_cores=executor_cores,
        executor_slots=executor_slots,
        executor_mem_mb=_int_at(block["executor_mem_mb"], f"{WHERE}.executor_mem_mb"),
        driver_cores=driver_cores,
        driver_mem_mb=2048
        if "driver_mem_mb" not in block
        else _int_at(block["driver_mem_mb"], f"{WHERE}.driver_mem_mb"),
        rewrite_options=(
            {}
            if "rewrite_options" not in block
            else _rewrite_options_at(block["rewrite_options"], f"{WHERE}.rewrite_options")
        ),
        start_delay_s=start_delay_s,
        machine_type=None if "machine_type" not in block else _str_at(block["machine_type"], f"{WHERE}.machine_type"),
        driver_machine_type=(
            None
            if "driver_machine_type" not in block
            else _str_at(block["driver_machine_type"], f"{WHERE}.driver_machine_type")
        ),
        extra_spark_conf=(
            {}
            if "extra_spark_conf" not in block
            else _string_map_at(block["extra_spark_conf"], f"{WHERE}.extra_spark_conf")
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
        raise ValueError(f"run {spec.name!r} declares no rewrite sidecar to render")
    return spec.compaction


def validate(block: dict[str, object], spec: RunSpec, meta: CorpusMetadata) -> None:
    """Validate the sidecar's block before staging creates anything.

    ``meta`` is retained for the shared driver validator interface. The
    rewrite target is not validated here: Iceberg reads it from the table's
    own ``write.target-file-size-bytes``, which the spec declares once under
    ``table.properties`` and the measurement reads back from the metadata.
    """
    read(block)
    _compaction(spec)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def kubernetes_name(run_id: str) -> str:
    """Return the sidecar's object name, distinct from the engine's."""
    return object_name(run_id) + NAME_SUFFIX


def configmap_name(derived: Derived) -> str:
    return f"{kubernetes_name(derived.run_id)}-job"


def render_conf(spec: RunSpec, site: SiteConfig, derived: Derived) -> dict[str, str]:
    """Render the Spark settings the sidecar is submitted with."""
    knobs = read(_compaction(spec).driver_block)
    conf = {
        "spark.sql.extensions": _ICEBERG_EXTENSIONS,
        **catalog_conf(site),
        "spark.executor.cores": str(knobs.executor_slots),
        # The JVM heap, not the pod's request. Spark adds
        # `spark.kubernetes.memoryOverheadFactor` on top, and because the
        # application is submitted as `type: Python` that factor defaults to
        # 0.4 rather than the JVM's 0.1, so a pod asks for 1.4x this and is
        # costed at 1.4x. The driver does run Python; the executors run a SQL
        # CALL with no Python worker, so their share of that is a default
        # rather than a measurement. Set the factor in `extra_spark_conf` to
        # size them deliberately. The Flink sidecar's `tm_mem_mb` is the whole
        # container, so the two knobs do not denote the same thing.
        "spark.executor.memory": f"{knobs.executor_mem_mb}m",
        "spark.driver.cores": str(knobs.driver_cores),
        "spark.driver.memory": f"{knobs.driver_mem_mb}m",
        # One shuffle partition per task slot. A rewrite that sorts would
        # otherwise fan out 200 ways and undo its own purpose.
        "spark.sql.shuffle.partitions": str(knobs.slots_total()),
        "spark.app.name": kubernetes_name(derived.run_id),
    }
    conf.update(knobs.extra_spark_conf)
    referenced = sorted(key for key, value in conf.items() if has_placeholder(value))
    if referenced:
        raise ValueError(
            f"the rewrite sidecar renders {referenced} as Spark settings, and nothing resolves a "
            f"{PLACEHOLDER_FORM} in one"
        )
    return conf


def render_conf_file(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    return "".join(f"{key} {value}\n" for key, value in render_conf(spec, site, derived).items())


def render_env(spec: RunSpec) -> str:
    """Render the local submission environment.

    Compose needs the core count and driver heap before the Spark session
    starts, because `local[N]` is an argument to spark-submit rather than a
    setting it reads.
    """
    knobs = read(_compaction(spec).driver_block)
    return f"{LOCAL_CORES_VAR}={knobs.slots_total()}\n{DRIVER_MEM_VAR}={knobs.driver_mem_mb}\n"


def render_job(spec: RunSpec, derived: Derived) -> str:
    """Render the document the sidecar reads its table and cadence from."""
    compaction = _compaction(spec)
    knobs = read(compaction.driver_block)
    return (
        json.dumps(
            {
                "catalog": CATALOG_NAME,
                "table": derived.table,
                "interval_s": compaction.interval_s,
                "start_delay_s": knobs.start_delay_s,
                "options": knobs.rewrite_options,
            },
            indent=2,
        )
        + "\n"
    )


def _mount() -> list[dict[str, object]]:
    """Return a fresh mount specification to avoid YAML anchors between pods."""
    return [{"name": _JOB_VOLUME, "mountPath": _RUN_MOUNT, "readOnly": True}]


def _pod(cluster: KubernetesConfig, base: dict[str, object]) -> dict[str, object]:
    pod = {**base, "volumeMounts": _mount(), **placement(cluster)}
    if cluster.aws_region is not None:
        pod["env"] = region_env(cluster)
    if cluster.secret_name is not None:
        pod["envFrom"] = secret_env_from(cluster)
    return pod


def render_sparkapplication(spec: RunSpec, site: SiteConfig, derived: Derived, image_tag: str) -> str:
    """Render the sidecar's SparkApplication.

    Match CPU requests and limits for Guaranteed QoS, as the engine does, so
    the compaction layer's cost is the fleet it was given rather than what it
    happened to burst to.
    """
    knobs = read(_compaction(spec).driver_block)
    site_cluster = cluster_for(site)
    cluster = replace(
        site_cluster,
        node_selector={**site_cluster.node_selector, **knobs.node_selector},
        tolerations=[*site_cluster.tolerations, *knobs.tolerations],
    )
    driver = _pod(
        cluster,
        {
            "cores": knobs.driver_cores,
            "coreLimit": str(knobs.driver_cores),
            "memory": f"{knobs.driver_mem_mb}m",
            # The driver creates executors; this service account supplies fleet identity.
            "serviceAccount": cluster.spark_service_account,
        },
    )
    executor = _pod(
        cluster,
        {
            "instances": knobs.executors,
            # `cores` is the slot count Spark schedules by; the pod asks the node for
            # `coreRequest` and is capped at `coreLimit`, both the vCPU the fleet is costed at.
            "cores": knobs.executor_slots,
            "coreRequest": str(knobs.executor_cores),
            "coreLimit": str(knobs.executor_cores),
            "memory": f"{knobs.executor_mem_mb}m",
        },
    )
    document: dict[str, object] = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {"name": kubernetes_name(derived.run_id), "namespace": cluster.namespace},
        "spec": {
            "type": "Python",
            "pythonVersion": "3",
            "mode": "cluster",
            "image": f"{cluster.registry}/{IMAGE_REPOSITORY}:{image_tag}",
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": _JOB_SCRIPT,
            "sparkVersion": SPARK_VERSION,
            # A restarted sidecar would compact a stretch of the run twice and
            # leave a gap where it was down, which no reading can separate.
            "restartPolicy": {"type": "Never"},
            "sparkConf": render_conf(spec, site, derived),
            "driver": driver,
            "executor": executor,
            "volumes": [{"name": _JOB_VOLUME, "configMap": {"name": configmap_name(derived)}}],
        },
    }
    return yaml.safe_dump(document, sort_keys=False)


def render_configmap(spec: RunSpec, site: SiteConfig, derived: Derived) -> str:
    """Render the ConfigMap holding the sidecar's rendered files."""
    cluster = cluster_for(site)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name(derived), "namespace": cluster.namespace},
        "data": {CONF_FILE: render_conf_file(spec, site, derived), JOB_FILE: render_job(spec, derived)},
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
        CONF_FILE: render_conf_file(spec, site, derived),
        ENV_FILE: render_env(spec),
        JOB_FILE: render_job(spec, derived),
    }
    if site.kubernetes is None:
        return files
    if image_tag is None:
        raise ValueError("render requires image_tag for Kubernetes runs; use the tag of the published image")
    files[DOCUMENT_FILE] = render_sparkapplication(spec, site, derived, image_tag)
    files[CONFIGMAP_FILE] = render_configmap(spec, site, derived)
    return files
