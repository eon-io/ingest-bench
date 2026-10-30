# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate Flink knobs and render the files needed for a run.

Render SQL, configuration, local fleet sizing, and Kubernetes manifests without
contacting a cluster. The engine uses released Flink connectors throughout.
"""

from __future__ import annotations

from dataclasses import dataclass, fields
from pathlib import Path
from typing import cast

import yaml

from engines.flink.script import join_statements
from engines.java_properties import ZSTD_LEVEL_1, catalog_properties, kafka_properties
from ingest_bench import uri
from ingest_bench.catalog import table_identifier
from ingest_bench.corpus import columns as c
from ingest_bench.corpus.metadata import CorpusMetadata
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.env import PLACEHOLDER_FORM, has_placeholder
from ingest_bench.specs.kubernetes import NAME, EngineKubernetes, object_name
from ingest_bench.specs.model import (
    LATEST_PER_KEY,
    VALUE_ENCODING_AVRO,
    VALUE_ENCODING_CONFLUENT,
    VALUE_ENCODING_JSON,
    KubernetesConfig,
    RunSpec,
    SiteConfig,
)

# Names are local to the submitted script, so they need no run-specific suffix.
SOURCE_TABLE = "kafka_source"
CATALOG_NAME = "ice"

# Rendered files; the deployment and ConfigMap are only needed on Kubernetes.
SQL_FILE = "job.sql"
CONF_FILE = "flink-conf.yaml"
ENV_FILE = "flink.env"
FLINKDEPLOYMENT_FILE = "flinkdeployment.yaml"
CONFIGMAP_FILE = "flink-job-configmap.yaml"

NONE = "none"
HASH = "hash"
RANGE = "range"
DISTRIBUTION_MODES = frozenset({NONE, HASH, RANGE})

# Keep this repository name aligned with push-images.sh and AWS setup.sh.
IMAGE_REPOSITORY = "lakehouse-ingest-bench/flink"

# Pinned Flink version in the operator's label format.
FLINK_VERSION_LABEL = "v2_2"
_SQL_RUNNER_JAR = "local:///opt/bench/sql-runner.jar"
_SQL_RUNNER_ENTRY_CLASS = "org.ingestbench.flink.SqlRunner"

# Mount beside the submitter; /run is reserved for container runtime files.
RUN_MOUNT = "/opt/bench/run"
_JOB_VOLUME = "job"

# The operator merges this container name into its Flink container. Any other
# name would create a sidecar.
_FLINK_CONTAINER = "flink-main-container"

REST = "rest"

# Operator-generated service names and pod labels, used to locate the run.
# Read image provenance from the JobManager, whose image is the engine image
# in every submission mode. Verification reads fleet settings from the job.
KUBERNETES = EngineKubernetes(
    kind="flinkdeployment",
    running_state="RUNNING",
    # Terminal jobs cannot become ready, even when they completed successfully.
    failed_states=("FAILED", "CANCELED", "FINISHED"),
    state_jsonpath="{.status.jobStatus.state}",
    # Rejected deployments may have no jobStatus. Treat reconciliation errors as
    # terminal only when the lifecycle is FAILED; other states may recover.
    error_jsonpath="{.status.error}",
    lifecycle_jsonpath="{.status.lifecycleState}",
    rest_service_suffix="-rest",
    rest_port=8081,
    log_target=f"deploy/{NAME}",
    provenance_selector=f"app={NAME},component=jobmanager",
    pods_selector="",
    fleet_selector=f"app={NAME}",
    pod_role_label="component",
    document_file=FLINKDEPLOYMENT_FILE,
    configmap_file=CONFIGMAP_FILE,
    # The operator derives a Service name from this label and enforces a
    # 45-character DNS-1035 limit before creating resources.
    max_object_name_length=45,
)

# Reject unknown keys so misspelled tuning options cannot be silently ignored.
KNOBS: dict[str, type] = {
    "taskmanagers": int,
    "slots": int,
    "tm_cpu": float,
    "tm_mem_mb": int,
    "jm_cpu": float,
    "jm_mem_mb": int,
    "checkpoint_interval": str,
    "min_pause": str,
    "unaligned_checkpoints": bool,
    "source_parallelism": int,
    "max_parallelism": int,
    "distribution_mode": str,
    "write_parallelism": int,
    "maintenance": dict,
    "machine_type": str,
    "jm_machine_type": str,
    "extra_flink_conf": dict,
}

# Require explicit choices for the fleet size, memory, commit cadence, and
# write distribution: these are the benchmark's main comparison dimensions.
REQUIRED_KNOBS = frozenset({"taskmanagers", "slots", "tm_cpu", "tm_mem_mb", "checkpoint_interval", "distribution_mode"})

# The reference tier: a run that sets these values, and differs only in the
# sizing knobs below, measures shipped Flink rather than tuning. A 60 s checkpoint
# interval spends a third of the tracks' 180 s freshness bound on cadence and leaves
# the rest for checkpoint completion and backlog. Hash distribution gives each
# partition one writer without a statistics phase.
REFERENCE_KNOBS: dict[str, object] = {"checkpoint_interval": "60s", "min_pause": "2s", "distribution_mode": "hash"}
# The table settings an upsert run may hold and stay reference. On format version 3 the job
# converts its equality deletes into deletion vectors (see converts_equality_deletes), so
# readers need not apply equality deletes. The only property is the zstd level 1 codec.
REFERENCE_UPSERT_TABLE: dict[str, object] = {"format_version": 3, "properties": dict(ZSTD_LEVEL_1)}
SIZING_KNOBS = frozenset(
    {
        "taskmanagers",
        "slots",
        "tm_cpu",
        "tm_mem_mb",
        "jm_cpu",
        "jm_mem_mb",
        "source_parallelism",
        "max_parallelism",
        "write_parallelism",
        "machine_type",
        "jm_machine_type",
    }
)

# Max parallelism is stored in checkpoints and cannot increase on restore.
# Leave room to expand the fleet without discarding state.
_MAX_PARALLELISM_FACTOR = 4

# Map supported wire encodings to source formats; reject others before staging.
_SOURCE_FORMATS = {
    VALUE_ENCODING_AVRO: "avro",
    VALUE_ENCODING_CONFLUENT: "avro-confluent",
    VALUE_ENCODING_JSON: "json",
}

# The non-legacy mapping preserves zoneless TIMESTAMP semantics without
# changing the encoded long. This option belongs to avro only; the confluent
# format rejects it.
_AVRO_OPTIONS: tuple[tuple[str, str], ...] = (("avro.timestamp_mapping.legacy", "false"),)

# Static options by encoding. Confluent registry options come from the site.
_FORMAT_OPTIONS: dict[str, tuple[tuple[str, str], ...]] = {
    VALUE_ENCODING_AVRO: _AVRO_OPTIONS,
    VALUE_ENCODING_CONFLUENT: (),
    VALUE_ENCODING_JSON: (
        ("json.timestamp-format.standard", "ISO-8601"),
        ("json.fail-on-missing-field", "true"),
        ("json.ignore-parse-errors", "false"),
    ),
}

# Confluent decoding requires the registry to resolve schema IDs. Set the
# credentials source with user info or the format ignores those credentials.
_REGISTRY_URL_KEY = "avro-confluent.url"
_REGISTRY_USER_INFO_SOURCE = ("avro-confluent.basic-auth.credentials-source", "USER_INFO")
_REGISTRY_USER_INFO_KEY = "avro-confluent.basic-auth.user-info"

# ---------------------------------------------------------------------------
# Reading the block
# ---------------------------------------------------------------------------


def _int_at(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _float_at(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{where} must be a number, got {value!r}")
    return float(value)


def _str_at(value: object, where: str) -> str:
    # Require strings for durations; an integer YAML scalar is not a Flink duration.
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _bool_at(value: object, where: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{where} must be true or false, got {value!r}")
    return value


def _conf_at(value: object, where: str) -> dict[str, str]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping of Flink setting to value, got {value!r}")
    return {str(key): _str_at(entry, f"{where}.{key}") for key, entry in cast(dict[object, object], value).items()}


# Iceberg attaches its post-commit table maintenance to the SinkV2 sink only; the
# v1 FlinkSink has no post-commit hook.
V2_SINK_KEY = "table.exec.iceberg.use-v2-sink"

# Iceberg's maintenance write options. The rewrite planner's own options are read
# from write options only (RewriteDataFilesConfig.properties() strips this prefix
# and hands the rest to the planner), so everything goes on the INSERT hint.
_MAINTENANCE_PREFIX = "flink-maintenance."
_REWRITE_PREFIX = _MAINTENANCE_PREFIX + "rewrite."
# Iceberg 1.12's in-job conversion of the upsert sink's equality deletes into deletion vectors.
# The sink commits to a staging branch and the converter promotes data files and deletion vectors
# to main, which readers use: converting in place on the branch the sink keeps committing to fails
# every cycle on conflicting files. The converter keeps a key index of the live rows in Flink
# state, which Iceberg recommends holding in RocksDB.
CONVERT_EQUALITY_DELETES_KEY = _MAINTENANCE_PREFIX + "convert-equality-deletes.enabled"
CONVERT_TARGET_BRANCH_KEY = _MAINTENANCE_PREFIX + "convert-equality-deletes.target-branch"
STAGING_BRANCH = "staging"
# The converter's key index logs an INFO line for every key it first sees, millions on a real seed,
# so the log, not the conversion, would set the TaskManager's load. The deployment keeps Flink's own
# console configuration (vendored from Flink 2.2.1) and raises this one logger to WARN.
PK_INDEX_LOGGER = "org.apache.iceberg.flink.maintenance.operator.EqualityConvertPKIndex"
PK_INDEX_LOGGER_ID = "iceberg_pk_index"
_FLINK_LOG_CONFIG = Path(__file__).with_name("log4j-console.properties")
STATE_BACKEND_KEY = "state.backend.type"
# Iceberg registers a commit-count, data-file-count and data-file-size schedule
# beside the interval, OR-ed, with defaults of 10 commits, 1,000 files and 100 GB.
# The trigger predicates compare with >=, so these values put them out of reach
# and leave the interval as the only trigger, which is the cadence the sidecar
# arm is compared on.
_INT_MAX = str(2**31 - 1)
_LONG_MAX = str(2**63 - 1)
_TARGET_OPTION = "target-file-size-bytes"
# Rendered from the block's own fields; a rewrite_options entry would silently
# duplicate the hint key.
_RESERVED_REWRITE_OPTIONS = frozenset(
    {
        "enabled",
        "schedule.interval-second",
        "schedule.commit-count",
        "schedule.data-file-count",
        "schedule.data-file-size",
        _TARGET_OPTION,
    }
)
_MAINTENANCE_KEYS: dict[str, type] = {
    "rewrite_parallelism": int,
    "slot_sharing_group": str,
    "schedule_interval_s": int,
    "rate_limit_s": int,
    "lock_check_delay_s": int,
    "rewrite_options": dict,
}

# An option's type, or for an enumerated string option the values it takes.
RewriteOptionKind = type | tuple[str, ...]

# Iceberg's RewriteJobOrder names. File groups reach the rewrite subtasks
# round-robin in planner order, so on a skewed table "bytes-desc" keeps the
# largest groups on distinct subtasks.
_JOB_ORDERS = ("none", "bytes-asc", "bytes-desc", "files-asc", "files-desc")

# Both Flink maintenance paths accept these planner options. Iceberg's Flink
# planner ignores unknown names, so validate them before submission.
# `use-starting-sequence-number` is absent because the Flink committer always
# uses the starting sequence number.
PLANNER_REWRITE_OPTIONS: dict[str, RewriteOptionKind] = {
    "min-input-files": int,
    "min-file-size-bytes": int,
    "max-file-size-bytes": int,
    "max-file-group-size-bytes": int,
    "max-file-group-input-files": int,
    "max-files-to-rewrite": int,
    "rewrite-all": bool,
    "partial-progress.enabled": bool,
    "partial-progress.max-commits": int,
    "rewrite-job-order": _JOB_ORDERS,
}
# Inline maintenance hands its options to Iceberg's RewriteDataFilesConfig, which
# reads the rewrite byte cap as `max-bytes`.
_INLINE_REWRITE_OPTIONS: dict[str, RewriteOptionKind] = {**PLANNER_REWRITE_OPTIONS, "max-bytes": int}
_BOOL_VALUES = ("true", "false")


def validate_rewrite_options(options: dict[str, str], where: str, supported: dict[str, RewriteOptionKind]) -> None:
    """Refuse option names outside ``supported`` and values of the wrong kind."""
    unknown = sorted(set(options) - set(supported))
    if unknown:
        raise ValueError(
            f"{where} has options {unknown} the RewriteDataFiles planner takes no setting for; "
            f"it takes {sorted(supported)}"
        )
    for key, text in options.items():
        kind = supported[key]
        if kind is bool:
            if text not in _BOOL_VALUES:
                raise ValueError(f"{where}.{key} must be {' or '.join(_BOOL_VALUES)}, got {text!r}")
        elif isinstance(kind, tuple):
            if text not in kind:
                raise ValueError(f"{where}.{key} must be one of {list(kind)}, got {text!r}")
        elif not text.isdigit():
            raise ValueError(f"{where}.{key} must be a non-negative integer written as a string, got {text!r}")


@dataclass(frozen=True)
class Maintenance:
    """Iceberg's RewriteDataFiles task running inside the job, after each commit.

    ``rewrite_parallelism`` is the runner's parallelism: file groups rewritten at
    once, one per subtask. The runner sits in ``slot_sharing_group`` so it takes
    separate slots from the source, writer and committer's default group. The
    fleet needs ``max(source_parallelism, write_parallelism) + rewrite_parallelism``
    slots because a sharing group's demand is its greatest parallelism.
    """

    rewrite_parallelism: int
    slot_sharing_group: str
    schedule_interval_s: int
    rate_limit_s: int
    lock_check_delay_s: int
    rewrite_options: dict[str, str]


def _maintenance_at(value: object, where: str) -> Maintenance:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping, got {value!r}")
    block = {str(key): entry for key, entry in cast(dict[object, object], value).items()}
    unknown = sorted(set(block) - set(_MAINTENANCE_KEYS))
    if unknown:
        raise ValueError(f"{where} has unknown keys {unknown}; supported keys are {sorted(_MAINTENANCE_KEYS)}")
    if "rewrite_parallelism" not in block:
        raise ValueError(f"{where} must set rewrite_parallelism")
    options = {} if "rewrite_options" not in block else _conf_at(block["rewrite_options"], f"{where}.rewrite_options")
    reserved = sorted(set(options) & _RESERVED_REWRITE_OPTIONS)
    if reserved:
        raise ValueError(
            f"{where}.rewrite_options sets {reserved}: the schedule and the enable flag are rendered from the "
            f"block's own fields, and the target comes from the table's write.target-file-size-bytes, where "
            f"table.properties declares it and the compaction reading reads it back"
        )
    validate_rewrite_options(options, f"{where}.rewrite_options", _INLINE_REWRITE_OPTIONS)
    maintenance = Maintenance(
        rewrite_parallelism=_int_at(block["rewrite_parallelism"], f"{where}.rewrite_parallelism"),
        slot_sharing_group=(
            "maintenance"
            if "slot_sharing_group" not in block
            else _str_at(block["slot_sharing_group"], f"{where}.slot_sharing_group")
        ),
        schedule_interval_s=(
            120
            if "schedule_interval_s" not in block
            else _int_at(block["schedule_interval_s"], f"{where}.schedule_interval_s")
        ),
        rate_limit_s=60 if "rate_limit_s" not in block else _int_at(block["rate_limit_s"], f"{where}.rate_limit_s"),
        lock_check_delay_s=(
            10
            if "lock_check_delay_s" not in block
            else _int_at(block["lock_check_delay_s"], f"{where}.lock_check_delay_s")
        ),
        rewrite_options=options,
    )
    for name in ("rewrite_parallelism", "schedule_interval_s", "rate_limit_s", "lock_check_delay_s"):
        if getattr(maintenance, name) < 1:
            raise ValueError(f"{where}.{name} must be at least 1, got {getattr(maintenance, name)}")
    if maintenance.rate_limit_s > maintenance.schedule_interval_s:
        # The trigger manager evaluates nothing for rate_limit_s after each fire, so a
        # longer rate limit would set the cadence instead of the declared interval.
        raise ValueError(
            f"{where}.rate_limit_s {maintenance.rate_limit_s} exceeds schedule_interval_s "
            f"{maintenance.schedule_interval_s}, so it would set the cadence"
        )
    return maintenance


@dataclass(frozen=True)
class Knobs:
    """Flink sizing and tuning with defaults applied."""

    taskmanagers: int
    slots: int
    tm_cpu: float
    tm_mem_mb: int
    jm_cpu: float
    jm_mem_mb: int
    checkpoint_interval: str
    min_pause: str
    unaligned_checkpoints: bool
    source_parallelism: int
    max_parallelism: int
    distribution_mode: str
    write_parallelism: int | None
    maintenance: Maintenance | None
    machine_type: str | None
    jm_machine_type: str | None
    extra_flink_conf: dict[str, str]

    def slots_total(self) -> int:
        """Return the fleet's task slots."""
        return self.taskmanagers * self.slots

    def writer_parallelism(self) -> int:
        """Return the Iceberg writer parallelism: the knob, else every fleet slot."""
        return self.slots_total() if self.write_parallelism is None else self.write_parallelism

    def v2_sink(self) -> bool:
        """Whether the job runs the SinkV2 Iceberg sink, whose graph verification reads differently."""
        return self.maintenance is not None or self.extra_flink_conf.get(V2_SINK_KEY) == "true"

    def parallelism_default(self) -> int:
        """Return the default parallelism inherited by Kafka readers.

        The pinned connector has no source parallelism option. Set the job default
        to the reader count and use a sink hint to give writers the full fleet.
        """
        return self.source_parallelism


# Keep accepted keys aligned with the parsed dataclass so none are dropped.
if frozenset(KNOBS) != {field.name for field in fields(Knobs)}:
    raise ValueError(f"KNOBS declares {sorted(KNOBS)} and Knobs holds {sorted(field.name for field in fields(Knobs))}")


def read(block: dict[str, object]) -> Knobs:
    """Validate the knob block and apply explicit defaults."""
    unknown = sorted(set(block) - set(KNOBS))
    if unknown:
        raise ValueError(f"spec.flink has unknown keys {unknown}; supported keys are {sorted(KNOBS)}")
    missing = sorted(REQUIRED_KNOBS - set(block))
    if missing:
        raise ValueError(f"spec.flink must set {missing}")

    taskmanagers = _int_at(block["taskmanagers"], "spec.flink.taskmanagers")
    slots = _int_at(block["slots"], "spec.flink.slots")
    source_parallelism = (
        1
        if "source_parallelism" not in block
        else _int_at(block["source_parallelism"], "spec.flink.source_parallelism")
    )
    max_parallelism = (
        _MAX_PARALLELISM_FACTOR * taskmanagers * slots
        if "max_parallelism" not in block
        else _int_at(block["max_parallelism"], "spec.flink.max_parallelism")
    )
    # Parallelism and container counts must be positive to run a job.
    for name, count in (
        ("taskmanagers", taskmanagers),
        ("slots", slots),
        ("source_parallelism", source_parallelism),
        ("max_parallelism", max_parallelism),
        ("write_parallelism", 1 if "write_parallelism" not in block else block["write_parallelism"]),
    ):
        if not isinstance(count, int) or count < 1:
            raise ValueError(f"spec.flink.{name} must be at least 1, got {count!r}")

    distribution_mode = _str_at(block["distribution_mode"], "spec.flink.distribution_mode")
    if distribution_mode not in DISTRIBUTION_MODES:
        raise ValueError(
            f"spec.flink.distribution_mode must be one of {sorted(DISTRIBUTION_MODES)}, got {distribution_mode!r}"
        )

    return Knobs(
        taskmanagers=taskmanagers,
        slots=slots,
        tm_cpu=_float_at(block["tm_cpu"], "spec.flink.tm_cpu"),
        tm_mem_mb=_int_at(block["tm_mem_mb"], "spec.flink.tm_mem_mb"),
        jm_cpu=1.0 if "jm_cpu" not in block else _float_at(block["jm_cpu"], "spec.flink.jm_cpu"),
        jm_mem_mb=2048 if "jm_mem_mb" not in block else _int_at(block["jm_mem_mb"], "spec.flink.jm_mem_mb"),
        checkpoint_interval=_str_at(block["checkpoint_interval"], "spec.flink.checkpoint_interval"),
        min_pause="0s" if "min_pause" not in block else _str_at(block["min_pause"], "spec.flink.min_pause"),
        unaligned_checkpoints=(
            False
            if "unaligned_checkpoints" not in block
            else _bool_at(block["unaligned_checkpoints"], "spec.flink.unaligned_checkpoints")
        ),
        source_parallelism=source_parallelism,
        max_parallelism=max_parallelism,
        distribution_mode=distribution_mode,
        write_parallelism=(
            None
            if "write_parallelism" not in block
            else _int_at(block["write_parallelism"], "spec.flink.write_parallelism")
        ),
        maintenance=None
        if "maintenance" not in block
        else _maintenance_at(block["maintenance"], "spec.flink.maintenance"),
        machine_type=None if "machine_type" not in block else _str_at(block["machine_type"], "spec.flink.machine_type"),
        jm_machine_type=(
            None if "jm_machine_type" not in block else _str_at(block["jm_machine_type"], "spec.flink.jm_machine_type")
        ),
        extra_flink_conf=(
            {}
            if "extra_flink_conf" not in block
            else _conf_at(block["extra_flink_conf"], "spec.flink.extra_flink_conf")
        ),
    )


def _required_in_nullable_row(column: c.ColumnDistribution, in_nullable_row: bool) -> bool:
    """Find a required member of a nullable struct.

    Flink's planner reads every field of a nullable ROW, and of the rows nested
    in it, as nullable. Array elements and map values keep their own nullability.
    """
    if in_nullable_row and not column.nullable:
        return True
    if column.kind == c.KIND_STRUCT:
        return any(_required_in_nullable_row(field, in_nullable_row or column.nullable) for field in column.fields)
    return any(_required_in_nullable_row(child, False) for child in c.children(column))


def validate(block: dict[str, object], spec: RunSpec, meta: CorpusMetadata) -> None:
    """Validate fleet sizing, the run's wire encoding and the corpus column kinds before staging.

    Reject encodings without a matching Flink source format.
    """
    knobs = read(block)
    # Validate the encoding now; render its format at submission time.
    _source_format(spec)
    unwritable = [column.name for column in meta.columns if not c.flink_supported(column)]
    if unwritable:
        raise ValueError(
            f"corpus column(s) {', '.join(unwritable)} hold uuid values, which Flink cannot write: Iceberg's Flink "
            "sink takes uuid as BINARY(16), and Flink SQL cannot convert the corpus's uuid strings to those bytes"
        )
    # The plain Avro reader schema comes from the DDL alone, so it must match the wire exactly.
    if spec.kafka.value_encoding == VALUE_ENCODING_AVRO:
        forced = [column.name for column in meta.columns if _required_in_nullable_row(column, False)]
        if forced:
            raise ValueError(
                f"corpus column(s) {', '.join(forced)} nest required fields in a nullable struct, which plain Avro "
                "cannot carry to Flink: it reads every field of a nullable ROW as nullable, so the reader schema it "
                "derives expects union branches the corpus does not encode. Use confluent or json encoding"
            )
    if knobs.source_parallelism > spec.kafka.partitions:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} exceeds the topic's "
            f"{spec.kafka.partitions} partitions; each source reader needs a partition"
        )
    if knobs.slots_total() < knobs.source_parallelism:
        raise ValueError(
            f"spec.flink.source_parallelism {knobs.source_parallelism} exceeds available task slots: "
            f"{knobs.taskmanagers} taskmanagers with {knobs.slots} slots each provide {knobs.slots_total()}"
        )
    if knobs.max_parallelism < knobs.slots_total():
        raise ValueError(
            f"spec.flink.max_parallelism {knobs.max_parallelism} is below the fleet's {knobs.slots_total()} "
            "task slots, so the job could never occupy them"
        )
    if knobs.writer_parallelism() > knobs.slots_total():
        raise ValueError(
            f"spec.flink.write_parallelism {knobs.writer_parallelism()} exceeds the fleet's "
            f"{knobs.slots_total()} task slots"
        )
    if spec.scoring.exactness == LATEST_PER_KEY:
        if knobs.distribution_mode != HASH:
            # An equality delete removes only rows with an older sequence number, so two writers holding
            # versions of one key in the same checkpoint would both stay live. Hashing on the partition,
            # which derives from the key, gives every key one writer.
            raise ValueError(
                f"an upsert run needs spec.flink.distribution_mode {HASH!r}, so every version of a key reaches "
                f"one writer within a checkpoint; got {knobs.distribution_mode!r}"
            )
    inline = spec.compaction is not None and spec.compaction.is_inline()
    if knobs.maintenance is None:
        if inline:
            raise ValueError(
                "spec.compaction.driver 'inline' on a Flink run means the job's own Iceberg table maintenance, "
                "which spec.flink.maintenance configures; without it nothing compacts"
            )
        return
    if not inline:
        raise ValueError(
            "spec.flink.maintenance compacts inside this job's fleet, so spec.compaction.driver must be 'inline'"
        )
    if converts_equality_deletes(spec):
        raise ValueError(
            "spec.flink.maintenance cannot run on a format version 3 upsert run: the sink commits to the "
            f"{STAGING_BRANCH!r} branch for the equality-delete converter, which cannot promote a commit that "
            "rewrites data files"
        )
    # Source, writer and committer share Flink's default slot-sharing group, whose
    # demand is the max parallelism among them, not the writers' alone.
    default_group = max(knobs.source_parallelism, knobs.writer_parallelism())
    needed = default_group + knobs.maintenance.rewrite_parallelism
    if knobs.slots_total() < needed:
        raise ValueError(
            f"spec.flink.maintenance needs {knobs.maintenance.rewrite_parallelism} slots in its own sharing group "
            f"beside the {default_group} slots the default sharing group needs (the greater of source_parallelism "
            f"{knobs.source_parallelism} and write_parallelism {knobs.writer_parallelism()}), {needed} in all; "
            f"{knobs.taskmanagers} taskmanagers with {knobs.slots} slots each provide {knobs.slots_total()}"
        )
    if knobs.extra_flink_conf.get(V2_SINK_KEY) == "false":
        raise ValueError(f"spec.flink.maintenance requires the SinkV2 sink; extra_flink_conf sets {V2_SINK_KEY} false")


# ---------------------------------------------------------------------------
# Rendering SQL
# ---------------------------------------------------------------------------


def _literal(value: str) -> str:
    """Quote ``value`` as a SQL string literal, doubling embedded quotes."""
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def _with_clause(options: list[tuple[str, str]]) -> str:
    return ",\n".join(f"  {_literal(key)} = {_literal(value)}" for key, value in options)


def _column_ddl(column: c.ColumnDistribution) -> str:
    """Declare a source column matching the corpus's Avro encoding.

    Flink derives its Avro reader schema from this DDL, where a nullable type
    reads a union branch index. Nullability must match the corpus at every level.
    """
    return f"  {c.quoted_identifier(column.name)} {c.flink_type(column)}"


def _kafka_options(security: dict[str, str]) -> list[tuple[str, str]]:
    return [(f"properties.{key}", value) for key, value in kafka_properties(security).items()]


def _source_format(spec: RunSpec) -> str:
    """Return the source format for the encoding, or reject an unsupported value."""
    if spec.kafka.value_encoding not in _SOURCE_FORMATS:
        raise ValueError(
            f"spec.kafka.value_encoding is {spec.kafka.value_encoding!r} and a Flink source reads "
            f"{sorted(_SOURCE_FORMATS)}"
        )
    return _SOURCE_FORMATS[spec.kafka.value_encoding]


def _format_options(spec: RunSpec, site: SiteConfig) -> list[tuple[str, str]]:
    """Return the validated source format and its decoding options."""
    encoding = spec.kafka.value_encoding
    options: list[tuple[str, str]] = [("format", _source_format(spec)), *_FORMAT_OPTIONS[encoding]]
    if encoding != VALUE_ENCODING_CONFLUENT:
        return options
    registry = site.schema_registry
    if registry is None:
        raise ValueError(
            f"spec.kafka.value_encoding {VALUE_ENCODING_CONFLUENT!r} requires kafka.schema_registry.url "
            "in the site configuration to resolve schema IDs"
        )
    options.append((_REGISTRY_URL_KEY, registry.url))
    # Keep environment placeholders intact for substitution inside the container.
    if registry.basic_auth_user_info is not None:
        options.append(_REGISTRY_USER_INFO_SOURCE)
        options.append((_REGISTRY_USER_INFO_KEY, registry.basic_auth_user_info))
    return options


def _source_ddl(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    columns = ",\n".join(_column_ddl(column) for column in meta.columns)
    options: list[tuple[str, str]] = [
        ("connector", "kafka"),
        ("topic", derived.topic),
        ("properties.bootstrap.servers", site.kafka_bootstrap),
        # Use the run ID to identify consumer groups left by abandoned runs.
        ("properties.group.id", derived.run_id),
        # Read from the beginning because records may arrive before the engine starts.
        ("scan.startup.mode", "earliest-offset"),
        *_kafka_options(site.kafka_security),
        *_format_options(spec, site),
    ]
    return f"CREATE TABLE {SOURCE_TABLE} (\n{columns}\n) WITH (\n{_with_clause(options)}\n)"


def _catalog_ddl(site: SiteConfig) -> str:
    options = {
        "type": "iceberg",
        "catalog-type": REST,
        **catalog_properties(site.catalog_props, site.warehouse, engine="Flink"),
    }
    return f"CREATE CATALOG {CATALOG_NAME} WITH (\n{_with_clause(list(options.items()))}\n)"


def converts_equality_deletes(spec: RunSpec) -> bool:
    """Whether an upsert run converts its equality deletes in the job: on format version 3, which has
    deletion vectors."""
    return spec.scoring.exactness == LATEST_PER_KEY and spec.table.format_version >= 3


def uses_v2_sink(knobs: Knobs, spec: RunSpec) -> bool:
    """Whether the job runs the SinkV2 Iceberg sink, which carries Iceberg's post-commit maintenance."""
    return knobs.v2_sink() or converts_equality_deletes(spec)


def _insert(derived: Derived, meta: CorpusMetadata, knobs: Knobs, spec: RunSpec) -> str:
    namespace, table = table_identifier(derived.table)
    hints: list[tuple[str, str]] = [("distribution-mode", knobs.distribution_mode)]
    if spec.scoring.exactness == LATEST_PER_KEY:
        # Each insert replaces the key's live row; the equality fields are the table's identifier fields.
        hints.append(("upsert-enabled", "true"))
    if converts_equality_deletes(spec):
        hints += [
            ("branch", STAGING_BRANCH),
            (CONVERT_EQUALITY_DELETES_KEY, "true"),
            (CONVERT_TARGET_BRANCH_KEY, "main"),
            # Maintenance runs at most once a minute by default and converts one staging commit per run;
            # the sink commits every checkpoint, so the converter must be free to run after each one.
            (_MAINTENANCE_PREFIX + "rate-limit-seconds", "1"),
        ]
    if knobs.writer_parallelism() != knobs.parallelism_default():
        # Override the reader default so writers use the slots meant for them.
        hints.append(("write-parallelism", str(knobs.writer_parallelism())))
    if knobs.maintenance is not None:
        hints.extend(_maintenance_hints(knobs.maintenance))
    rendered = ", ".join(f"{_literal(key)} = {_literal(value)}" for key, value in hints)
    columns = ", ".join(c.quoted_identifier(name) for name in meta.field_names())
    return (
        f"INSERT INTO {CATALOG_NAME}.`{namespace}`.`{table}` /*+ OPTIONS({rendered}) */\n"
        f"SELECT {columns} FROM {SOURCE_TABLE}"
    )


def _maintenance_hints(maintenance: Maintenance) -> list[tuple[str, str]]:
    """Return the write options that run RewriteDataFiles after each commit."""
    hints = [
        (_REWRITE_PREFIX + "enabled", "true"),
        (_REWRITE_PREFIX + "schedule.interval-second", str(maintenance.schedule_interval_s)),
        (_REWRITE_PREFIX + "schedule.commit-count", _INT_MAX),
        (_REWRITE_PREFIX + "schedule.data-file-count", _INT_MAX),
        (_REWRITE_PREFIX + "schedule.data-file-size", _LONG_MAX),
        (_MAINTENANCE_PREFIX + "parallelism", str(maintenance.rewrite_parallelism)),
        (_MAINTENANCE_PREFIX + "slot-sharing-group", maintenance.slot_sharing_group),
        (_MAINTENANCE_PREFIX + "rate-limit-seconds", str(maintenance.rate_limit_s)),
        (_MAINTENANCE_PREFIX + "lock-check-delay-seconds", str(maintenance.lock_check_delay_s)),
    ]
    hints.extend((_REWRITE_PREFIX + key, value) for key, value in sorted(maintenance.rewrite_options.items()))
    return hints


def render_sql(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the source, catalog, and INSERT statements."""
    knobs = read(spec.engine_block)
    return join_statements(
        (_source_ddl(spec, site, derived, meta), _catalog_ddl(site), _insert(derived, meta, knobs, spec))
    )


# ---------------------------------------------------------------------------
# Rendering configuration
# ---------------------------------------------------------------------------


def _flag(value: bool) -> str:
    return "true" if value else "false"


def render_conf(spec: RunSpec, derived: Derived) -> dict[str, str]:
    """Render Flink job settings.

    Fleet sizing also appears in ``flink.env`` because Compose starts containers
    before the job is submitted.
    """
    knobs = read(spec.engine_block)
    conf = {
        "execution.checkpointing.interval": knobs.checkpoint_interval,
        "execution.checkpointing.min-pause": knobs.min_pause,
        "execution.checkpointing.unaligned.enabled": _flag(knobs.unaligned_checkpoints),
        # Exactly-once mode is required by the benchmark's duplication metric.
        "execution.checkpointing.mode": "EXACTLY_ONCE",
        "parallelism.default": str(knobs.parallelism_default()),
        "pipeline.max-parallelism": str(knobs.max_parallelism),
        "taskmanager.numberOfTaskSlots": str(knobs.slots),
        "taskmanager.memory.process.size": f"{knobs.tm_mem_mb}m",
        "jobmanager.memory.process.size": f"{knobs.jm_mem_mb}m",
        # Use the run ID to identify the job in listings.
        "pipeline.name": derived.run_id,
    }
    if uses_v2_sink(knobs, spec):
        conf[V2_SINK_KEY] = "true"
    if converts_equality_deletes(spec):
        conf[STATE_BACKEND_KEY] = "rocksdb"
        # The key index holds every live key; incremental checkpoints upload only what changed.
        conf["execution.checkpointing.incremental"] = "true"
    # Apply explicit overrides last.
    conf.update(knobs.extra_flink_conf)
    return conf


def _conf_yaml(spec: RunSpec, derived: Derived) -> str:
    """Serialize the submitter's settings as YAML."""
    return yaml.safe_dump(render_conf(spec, derived), sort_keys=True, default_flow_style=False)


# ---------------------------------------------------------------------------
# Rendering the Kubernetes documents
# ---------------------------------------------------------------------------


def cluster_for(site: SiteConfig) -> KubernetesConfig:
    if site.kubernetes is None:
        raise ValueError("Flink on Kubernetes requires site.kubernetes; no cluster is configured")
    return site.kubernetes


def kubernetes_name(run_id: str) -> str:
    """Convert the run ID using the shared Kubernetes naming rules."""
    return object_name(run_id)


def configmap_name(derived: Derived) -> str:
    """Return the name of the ConfigMap containing the run's files."""
    return f"{kubernetes_name(derived.run_id)}-flink-job"


def pod_template(cluster: KubernetesConfig, configmap: str) -> dict[str, object]:
    """Return the pod template shared by every Flink pod the harness starts.

    The run's rendered files are mounted read-only from ``configmap``; the
    site's region and Secret reach the Flink container; placement follows the
    site. The maintenance sidecar uses the same template with its own files.
    """
    container: dict[str, object] = {
        "name": _FLINK_CONTAINER,
        "volumeMounts": [{"name": _JOB_VOLUME, "mountPath": RUN_MOUNT, "readOnly": True}],
    }
    if cluster.aws_region is not None:
        # Java SDKs read AWS_REGION; botocore reads AWS_DEFAULT_REGION. Supply both
        # for broker authentication and S3 access.
        container["env"] = [
            {"name": name, "value": cluster.aws_region} for name in ("AWS_REGION", "AWS_DEFAULT_REGION")
        ]
    if cluster.secret_name is not None:
        # Resolve rendered environment references from the Secret. ConfigMaps and
        # archived run files retain only the references.
        container["envFrom"] = [{"secretRef": {"name": cluster.secret_name}}]
    pod_spec: dict[str, object] = {
        "nodeSelector": cluster.node_selector,
        "tolerations": cluster.tolerations,
        "volumes": [{"name": _JOB_VOLUME, "configMap": {"name": configmap}}],
        "containers": [container],
    }
    return {"apiVersion": "v1", "kind": "Pod", "spec": pod_spec}


def render_flinkdeployment(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, image_tag: str
) -> str:
    """Render the run's FlinkDeployment.

    ``meta`` is unused but retained for the shared Kubernetes renderer interface.
    """
    knobs = read(spec.engine_block)
    cluster = cluster_for(site)
    # The operator applies flinkConfiguration verbatim, so a placeholder stays literal
    # there even though SqlRunner resolves the same value from its ConfigMap copy.
    referenced = sorted(key for key, value in knobs.extra_flink_conf.items() if has_placeholder(value))
    if referenced:
        raise ValueError(
            f"spec.flink.extra_flink_conf sets {referenced} to a {PLACEHOLDER_FORM} value, which the "
            "FlinkDeployment's flinkConfiguration would carry unresolved; only local Compose sites accept one"
        )
    conf = {
        # Keep checkpoints under the run for cleanup; allow extra_flink_conf to
        # override this default.
        "execution.checkpointing.dir": uri.join(site.runs_root, derived.run_id, "checkpoints"),
        **render_conf(spec, derived),
    }
    document: dict[str, object] = {
        "apiVersion": "flink.apache.org/v1beta1",
        "kind": "FlinkDeployment",
        "metadata": {"name": kubernetes_name(derived.run_id), "namespace": cluster.namespace},
        "spec": {
            "image": f"{cluster.registry}/{IMAGE_REPOSITORY}:{image_tag}",
            "flinkVersion": FLINK_VERSION_LABEL,
            # Standalone mode honors taskmanagers; native mode derives the fleet from
            # job parallelism.
            "mode": "standalone",
            "serviceAccount": cluster.flink_service_account,
            "flinkConfiguration": conf,
            # CRD fields override flinkConfiguration, so read these values from the
            # effective configuration to preserve extra_flink_conf overrides. Replica
            # count, slots, and CPU have no competing settings here.
            "jobManager": {"resource": {"memory": conf["jobmanager.memory.process.size"], "cpu": knobs.jm_cpu}},
            "taskManager": {
                "resource": {"memory": conf["taskmanager.memory.process.size"], "cpu": knobs.tm_cpu},
                "replicas": knobs.taskmanagers,
            },
            "job": {
                "jarURI": _SQL_RUNNER_JAR,
                "entryClass": _SQL_RUNNER_ENTRY_CLASS,
                "args": [
                    "--sql",
                    f"{RUN_MOUNT}/{SQL_FILE}",
                    "--conf",
                    f"{RUN_MOUNT}/{CONF_FILE}",
                ],
                "parallelism": int(conf["parallelism.default"]),
                # Runs are scored once, so object updates do not restore prior state.
                "upgradeMode": "stateless",
                "state": "running",
            },
            "podTemplate": pod_template(cluster, configmap_name(derived)),
        },
    }
    if converts_equality_deletes(spec):
        cast(dict[str, object], document["spec"])["logConfiguration"] = {"log4j-console.properties": _quiet_pk_index()}
    return yaml.safe_dump(document, sort_keys=False)


def _quiet_pk_index() -> str:
    """Flink's console log configuration with the converter's key index raised to WARN."""
    override = f"logger.{PK_INDEX_LOGGER_ID}.name = {PK_INDEX_LOGGER}\nlogger.{PK_INDEX_LOGGER_ID}.level = WARN\n"
    return _FLINK_LOG_CONFIG.read_text().rstrip("\n") + "\n\n" + override


def render_job_configmap(spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata) -> str:
    """Render the ConfigMap containing SQL and Flink settings."""
    cluster = cluster_for(site)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {"name": configmap_name(derived), "namespace": cluster.namespace},
        "data": {SQL_FILE: render_sql(spec, site, derived, meta), CONF_FILE: _conf_yaml(spec, derived)},
    }
    return yaml.safe_dump(document, sort_keys=False)


# ---------------------------------------------------------------------------
# The run directory
# ---------------------------------------------------------------------------


def render(
    spec: RunSpec, site: SiteConfig, derived: Derived, meta: CorpusMetadata, *, image_tag: str | None = None
) -> dict[str, str]:
    """Return rendered run files keyed by filename.

    Kubernetes sites also receive deployment manifests and require an image tag.
    The local stack builds and submits its own image.
    """
    knobs = read(spec.engine_block)
    files = {
        SQL_FILE: render_sql(spec, site, derived, meta),
        CONF_FILE: _conf_yaml(spec, derived),
        # Compose needs fleet sizing before submission, so provide it as environment.
        ENV_FILE: (
            f"TASKMANAGERS={knobs.taskmanagers}\n"
            f"SLOTS={knobs.slots}\n"
            f"TM_MEM_MB={knobs.tm_mem_mb}\n"
            f"JM_MEM_MB={knobs.jm_mem_mb}\n"
        ),
    }
    if site.kubernetes is None:
        return files
    if image_tag is None:
        raise ValueError("render requires image_tag for Kubernetes runs; use the tag of the published image")
    files[FLINKDEPLOYMENT_FILE] = render_flinkdeployment(spec, site, derived, meta, image_tag)
    files[CONFIGMAP_FILE] = render_job_configmap(spec, site, derived, meta)
    return files
