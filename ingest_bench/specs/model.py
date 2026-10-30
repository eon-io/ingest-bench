# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Load and validate run specs and site configuration.

Reject unknown keys so misspelled settings cannot silently use defaults.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.kafka_auth import refuse_mechanism_alias
from ingest_bench.pricing import MachinePrice, Pricing
from ingest_bench.specs import compactors, engines
from ingest_bench.specs.env import names_a_secret, refuse_literal_secrets
from ingest_bench.workloads import canonical_workload_name, workloads_dir

# Allow names shared by Kafka, SQL, and Kubernetes. A 35-character spec name
# plus a 17-character stamp and the longest Job prefix (`drop-topic-`, 11)
# fits Kubernetes' 63-character job-name label.
NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{2,34}$")

EXTERNAL = "external"
HARNESS = "harness"
ENGINE_OWNED = "engine"

# Compaction is a layer of its own: any engine runs with or without one.
# `inline` records an engine that compacts as it writes, which starts nothing
# and adds no fleet because the engine fleet already performs that work.
COMPACTION_INLINE = "inline"
_COMPACTION_KEYS = frozenset({"driver", "interval_s", "external", "fleet"})

# Raw Avro is the default; Confluent framing adds a five-byte schema header.
VALUE_ENCODING_AVRO = "avro"
VALUE_ENCODING_CONFLUENT = "confluent"
VALUE_ENCODING_JSON = "json"
VALUE_ENCODINGS = frozenset({VALUE_ENCODING_AVRO, VALUE_ENCODING_CONFLUENT, VALUE_ENCODING_JSON})

# No Kafka message key; records are not partitioned by a column value.
KEY_NONE = "none"

PARTITION_ASSIGNMENT_KEY = "key"
PARTITION_ASSIGNMENT_ROUND_ROBIN = "round_robin"
# Upsert corpora store rows by Kafka partition; each producer shard sends its own partitions.
PARTITION_ASSIGNMENT_CORPUS = "corpus"
PARTITION_ASSIGNMENTS = frozenset(
    {PARTITION_ASSIGNMENT_KEY, PARTITION_ASSIGNMENT_ROUND_ROBIN, PARTITION_ASSIGNMENT_CORPUS}
)

EXACTLY_ONCE = "EXACTLY_ONCE"
AT_LEAST_ONCE = "AT_LEAST_ONCE"
# Upsert corpora: the table must hold exactly the newest version of every key.
LATEST_PER_KEY = "LATEST_PER_KEY"
EXACTNESS_MODES = frozenset({EXACTLY_ONCE, AT_LEAST_ONCE, LATEST_PER_KEY})

FORMAT_VERSIONS = frozenset({2, 3})

SUSTAINABLE = "sustainable"
SATURATION = "saturation"
SCORING_MODES = frozenset({SUSTAINABLE, SATURATION})

# Warmup before the measurement window, shared by both scoring modes.
DEFAULT_WARMUP_S = 120

# Use librdkafka codec names. Compression is a workload setting shared by
# compared runs.
COMPRESSION_DEFAULT = "zstd"
COMPRESSIONS = frozenset({"gzip", "lz4", "none", "snappy", COMPRESSION_DEFAULT})

# Reject compression overrides in client properties so the actual codec
# matches the spec and published facts.
_COMPRESSION_PROP_PREFIX = "compression."

# Default geometry sample offsets, in seconds from run start.
DEFAULT_GEOMETRY_OFFSETS_S: tuple[int, ...] = (600, 1200, 1800, 2700, 3600)

_SPEC_KEYS = frozenset(
    {
        "name",
        "engine",
        "track",
        "tier",
        "corpus",
        "table",
        "kafka",
        "producer",
        "scoring",
        "external",
        "fleet",
        "compaction",
    }
)
_SITE_KEYS = frozenset({"corpus_root", "runs_root", "warehouse", "kafka", "catalog", "kubernetes", "pricing"})
_KUBERNETES_KEYS = frozenset(
    {
        "context",
        "namespace",
        "harness_service_account",
        "flink_service_account",
        "spark_service_account",
        "service_account_annotations",
        "registry",
        "aws_region",
        "secret_name",
        "node_selector",
        "tolerations",
    }
)

# AWS setup creates this default Spark service account.
_SPARK_SERVICE_ACCOUNT = "ingest-bench-spark"

# Share the placeholder prefix with result-publication validation.
PLACEHOLDER = "YOUR_"

# Default to the partition column used by shipped corpora.
_PARTITION_DEFAULT_COLUMN = "partition_key"

DEFAULT_PARTITION = f"identity({_PARTITION_DEFAULT_COLUMN})"
DEFAULT_BEHIND_MAX_MS = 5000

# Leaderboard tiers. A reference run changes only its engine's sizing knobs from
# the shipped settings. A tuned run may change anything, with disclosure.
TIER_REFERENCE = "reference"
TIER_TUNED = "tuned"
TIERS = frozenset({TIER_REFERENCE, TIER_TUNED})


# ---------------------------------------------------------------------------
# Reading YAML values
# ---------------------------------------------------------------------------


def _as_mapping(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} must be a mapping, got {type(value).__name__}")
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _as_str(value: object, where: str) -> str:
    # Require quoted strings: YAML booleans and numbers can change spelling
    # when coerced, producing invalid client properties or version labels.
    if not isinstance(value, str):
        raise ValueError(f"{where} must be a string; quote it in the YAML. Got {value!r}")
    return value


def _as_int(value: object, where: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} must be an integer, got {value!r}")
    return value


def _as_float(value: object, where: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise ValueError(f"{where} must be a number, got {value!r}")
    return float(value)


def _as_string_map(value: object, where: str) -> dict[str, str]:
    return {key: _as_str(entry, f"{where}.{key}") for key, entry in _as_mapping(value, where).items()}


def _as_string_maps(value: object, where: str) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise ValueError(f"{where} must be a list of mappings, got {value!r}")
    return [_as_string_map(entry, f"{where}[{index}]") for index, entry in enumerate(cast(list[object], value))]


def refuse_compression_props(props: Mapping[str, str], where: str) -> None:
    """Refuse a client property that would choose the wire codec behind the spec."""
    named = sorted(key for key in props if key.startswith(_COMPRESSION_PROP_PREFIX))
    if named:
        raise ValueError(
            f"{where} sets {named}, which would choose the wire codec instead of the run's spec; "
            f"set producer.compression (one of {sorted(COMPRESSIONS)}) and leave the property out"
        )


def _refuse_unknown(block: dict[str, object], allowed: frozenset[str], where: str) -> None:
    unknown = sorted(set(block) - allowed)
    if unknown:
        raise ValueError(f"{where} has unknown keys {unknown}; the ones it takes are {sorted(allowed)}")


def _block(raw: dict[str, object], key: str, where: str) -> dict[str, object]:
    """The optional ``key`` block, empty when the file leaves it out."""
    if key not in raw:
        return {}
    return _as_mapping(raw[key], f"{where}.{key}")


def _required(raw: dict[str, object], key: str, where: str) -> object:
    if key not in raw:
        raise ValueError(f"{where} must set {key}")
    return raw[key]


def _load_yaml(path: Path, where: str) -> dict[str, object]:
    try:
        text = path.read_text()
    except OSError as error:
        raise ValueError(f"could not read {where} {path}: {error}") from error
    return _as_mapping(yaml.safe_load(text), str(path))


# ---------------------------------------------------------------------------
# Run spec
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class TableSpec:
    """Table ownership, partitioning, and properties.

    ``managed_by`` selects whether the harness creates the table or supplies DDL
    for the engine to execute.
    """

    managed_by: str
    partition: str
    properties: dict[str, str]
    format_version: int = 2


@dataclass(frozen=True)
class KafkaSpec:
    """Topic partition count, message key, and value framing.

    These settings define the workload and must match across compared engines.
    ``avro`` and ``json`` send matching corpus bytes directly; ``confluent``
    adds a five-byte schema registry header to Avro records.
    """

    partitions: int
    key: str
    value_encoding: str
    partition_assignment: str = PARTITION_ASSIGNMENT_KEY


@dataclass(frozen=True)
class ProducerSpec:
    speed: float
    seconds: int | None
    shards: int
    behind_max_ms: int
    compression: str


@dataclass(frozen=True)
class ScoringSpec:
    """Scoring settings and optional live-gate overrides.

    Absent gate fields use the gate's own defaults.
    """

    freshness_bound_s: float
    warmup_s: int
    exactness: str
    geometry_offsets_s: tuple[int, ...]
    gate_adaptation_s: int | None
    gate_window_s: int | None
    mode: str = SUSTAINABLE
    measurement_s: int | None = None
    # The freshness max bound when it is not twice the p95 bound.
    freshness_max_bound_s: float | None = None

    @property
    def max_bound_s(self) -> float:
        return 2 * self.freshness_bound_s if self.freshness_max_bound_s is None else self.freshness_max_bound_s


# Shared sentinel for an undisclosed machine type; publication rejects it.
MACHINE_TYPE_UNSPECIFIED = "unspecified"


@dataclass(frozen=True)
class FleetRole:
    """Compute resources for one fleet role, used to calculate cost."""

    role: str
    count: int
    vcpu: float
    gib: float
    machine_type: str


@dataclass(frozen=True)
class ExternalSpec:
    name: str
    version: str
    notes: str


@dataclass(frozen=True)
class CompactionSpec:
    """What rewrites the run's data files, and on what cadence.

    ``driver`` is ``inline`` for an engine that compacts as it writes,
    ``external`` for a compactor started outside the harness, or a driver
    registered in ``specs.compactors`` that the harness renders and starts.
    Only a registered driver has an ``interval_s``; the other two control their
    own cadence. All drivers are measured from table metadata.
    """

    driver: str
    interval_s: int | None
    driver_block: dict[str, object]
    external: ExternalSpec | None
    fleet: tuple[FleetRole, ...]

    def is_inline(self) -> bool:
        return self.driver == COMPACTION_INLINE

    def is_external(self) -> bool:
        return self.driver == EXTERNAL

    def is_managed(self) -> bool:
        return self.driver in compactors.MANAGED


@dataclass(frozen=True)
class RunSpec:
    name: str
    engine: str
    corpus: str
    table: TableSpec
    kafka: KafkaSpec
    producer: ProducerSpec
    scoring: ScoringSpec
    engine_block: dict[str, object]
    external: ExternalSpec | None
    fleet: tuple[FleetRole, ...]
    compaction: CompactionSpec | None
    track: str | None = None
    # Only an external engine declares its tier. The harness derives a managed one.
    declared_tier: str | None = None

    def is_external(self) -> bool:
        return self.engine == EXTERNAL


def _table_spec(raw: dict[str, object], partition_default: str) -> TableSpec:
    block = _block(raw, "table", "spec")
    _refuse_unknown(block, frozenset({"managed_by", "partition", "properties", "format_version"}), "spec.table")
    managed_by = HARNESS if "managed_by" not in block else _as_str(block["managed_by"], "spec.table.managed_by")
    if managed_by not in (HARNESS, ENGINE_OWNED):
        raise ValueError(f"spec.table.managed_by must be {HARNESS!r} or {ENGINE_OWNED!r}, got {managed_by!r}")
    partition = partition_default if "partition" not in block else _as_str(block["partition"], "spec.table.partition")
    properties = {} if "properties" not in block else _as_string_map(block["properties"], "spec.table.properties")
    credentials = sorted(key for key in properties if names_a_secret(key))
    if credentials:
        raise ValueError(
            f"spec.table.properties sets credential properties {credentials}. Table properties are stored in "
            "table metadata and published with the spec, so supply credentials through site.catalog.props"
        )
    format_version = (
        2 if "format_version" not in block else _as_int(block["format_version"], "spec.table.format_version")
    )
    if format_version not in FORMAT_VERSIONS:
        raise ValueError(f"spec.table.format_version must be one of {sorted(FORMAT_VERSIONS)}, got {format_version!r}")
    return TableSpec(managed_by=managed_by, partition=partition, properties=properties, format_version=format_version)


def _kafka_spec(raw: dict[str, object]) -> KafkaSpec:
    block = _as_mapping(_required(raw, "kafka", "spec"), "spec.kafka")
    _refuse_unknown(block, frozenset({"partitions", "key", "value_encoding", "partition_assignment"}), "spec.kafka")
    assignment = _as_str(block.get("partition_assignment", PARTITION_ASSIGNMENT_KEY), "spec.kafka.partition_assignment")
    if assignment not in PARTITION_ASSIGNMENTS:
        raise ValueError(
            f"spec.kafka.partition_assignment must be one of {sorted(PARTITION_ASSIGNMENTS)}, got {assignment!r}"
        )
    partitions = _as_int(_required(block, "partitions", "spec.kafka"), "spec.kafka.partitions")
    if partitions < 1:
        raise ValueError("spec.kafka.partitions must be positive")
    encoding = (
        VALUE_ENCODING_AVRO
        if "value_encoding" not in block
        else _as_str(block["value_encoding"], "spec.kafka.value_encoding")
    )
    if encoding not in VALUE_ENCODINGS:
        raise ValueError(f"spec.kafka.value_encoding must be one of {sorted(VALUE_ENCODINGS)}, got {encoding!r}")
    return KafkaSpec(
        partitions=partitions,
        key=_as_str(_required(block, "key", "spec.kafka"), "spec.kafka.key"),
        value_encoding=encoding,
        partition_assignment=assignment,
    )


def _producer_spec(raw: dict[str, object]) -> ProducerSpec:
    block = _block(raw, "producer", "spec")
    _refuse_unknown(block, frozenset({"speed", "seconds", "shards", "behind_max_ms", "compression"}), "spec.producer")
    compression = (
        COMPRESSION_DEFAULT
        if "compression" not in block
        else _as_str(block["compression"], "spec.producer.compression")
    )
    if compression not in COMPRESSIONS:
        raise ValueError(f"spec.producer.compression must be one of {sorted(COMPRESSIONS)}, got {compression!r}")
    producer = ProducerSpec(
        speed=1.0 if "speed" not in block else _as_float(block["speed"], "spec.producer.speed"),
        seconds=None if "seconds" not in block else _as_int(block["seconds"], "spec.producer.seconds"),
        shards=1 if "shards" not in block else _as_int(block["shards"], "spec.producer.shards"),
        behind_max_ms=DEFAULT_BEHIND_MAX_MS
        if "behind_max_ms" not in block
        else _as_int(block["behind_max_ms"], "spec.producer.behind_max_ms"),
        compression=compression,
    )

    if not math.isfinite(producer.speed) or producer.speed <= 0:
        raise ValueError("spec.producer.speed must be finite and positive")
    for key, value in (("seconds", producer.seconds), ("shards", producer.shards)):
        if value is not None and value <= 0:
            raise ValueError(f"spec.producer.{key} must be positive")
    if producer.behind_max_ms < 0:
        raise ValueError("spec.producer.behind_max_ms must be nonnegative")
    return producer


def validate_scoring_mode(mode: str, measurement_s: int | None) -> None:
    """Keep spec and direct scorer invocations on the same mode contract."""
    if mode not in SCORING_MODES:
        raise ValueError(f"scoring.mode must be one of {sorted(SCORING_MODES)}, got {mode!r}")
    if mode == SATURATION:
        if isinstance(measurement_s, bool) or not isinstance(measurement_s, int) or measurement_s <= 0:
            raise ValueError("scoring.measurement_s must be a positive integer in saturation mode")
    elif measurement_s is not None:
        raise ValueError("scoring.measurement_s is only used in saturation mode")


def _scoring_spec(raw: dict[str, object]) -> ScoringSpec:
    block = _block(raw, "scoring", "spec")
    _refuse_unknown(
        block,
        frozenset(
            {
                "freshness_bound_s",
                "freshness_max_bound_s",
                "warmup_s",
                "exactness",
                "geometry_offsets_s",
                "gate_adaptation_s",
                "gate_window_s",
                "mode",
                "measurement_s",
            }
        ),
        "spec.scoring",
    )
    exactness = EXACTLY_ONCE if "exactness" not in block else _as_str(block["exactness"], "spec.scoring.exactness")
    if exactness not in EXACTNESS_MODES:
        raise ValueError(f"spec.scoring.exactness must be one of {sorted(EXACTNESS_MODES)}, got {exactness!r}")
    if "geometry_offsets_s" not in block:
        offsets = DEFAULT_GEOMETRY_OFFSETS_S
    else:
        raw_offsets = block["geometry_offsets_s"]
        if not isinstance(raw_offsets, list):
            raise ValueError(f"spec.scoring.geometry_offsets_s must be a list of seconds, got {raw_offsets!r}")
        offsets = tuple(
            _as_int(offset, f"spec.scoring.geometry_offsets_s[{index}]")
            for index, offset in enumerate(cast(list[object], raw_offsets))
        )
    scoring = ScoringSpec(
        mode=_as_str(block.get("mode", SUSTAINABLE), "spec.scoring.mode"),
        measurement_s=None
        if "measurement_s" not in block
        else _as_int(block["measurement_s"], "spec.scoring.measurement_s"),
        freshness_bound_s=180.0
        if "freshness_bound_s" not in block
        else _as_float(block["freshness_bound_s"], "spec.scoring.freshness_bound_s"),
        freshness_max_bound_s=None
        if "freshness_max_bound_s" not in block
        else _as_float(block["freshness_max_bound_s"], "spec.scoring.freshness_max_bound_s"),
        warmup_s=DEFAULT_WARMUP_S if "warmup_s" not in block else _as_int(block["warmup_s"], "spec.scoring.warmup_s"),
        exactness=exactness,
        geometry_offsets_s=offsets,
        gate_adaptation_s=None
        if "gate_adaptation_s" not in block
        else _as_int(block["gate_adaptation_s"], "spec.scoring.gate_adaptation_s"),
        gate_window_s=None
        if "gate_window_s" not in block
        else _as_int(block["gate_window_s"], "spec.scoring.gate_window_s"),
    )

    validate_scoring_mode(scoring.mode, scoring.measurement_s)
    if scoring.exactness == LATEST_PER_KEY and scoring.mode != SUSTAINABLE:
        raise ValueError(f"spec.scoring.exactness {LATEST_PER_KEY} is scored in {SUSTAINABLE} mode only")
    if not math.isfinite(scoring.freshness_bound_s) or scoring.freshness_bound_s <= 0:
        raise ValueError("spec.scoring.freshness_bound_s must be finite and positive")
    if scoring.freshness_max_bound_s is not None and not (
        math.isfinite(scoring.freshness_max_bound_s) and scoring.freshness_max_bound_s >= scoring.freshness_bound_s
    ):
        raise ValueError(
            "spec.scoring.freshness_max_bound_s must be finite and at least spec.scoring.freshness_bound_s"
        )
    for key, value in (("warmup_s", scoring.warmup_s), ("gate_adaptation_s", scoring.gate_adaptation_s)):
        if value is not None and value < 0:
            raise ValueError(f"spec.scoring.{key} must be nonnegative")
    if scoring.gate_window_s is not None and scoring.gate_window_s <= 0:
        raise ValueError("spec.scoring.gate_window_s must be positive")
    if any(offset < 0 for offset in offsets):
        raise ValueError("spec.scoring.geometry_offsets_s must be nonnegative")
    if any(right <= left for left, right in zip(offsets, offsets[1:], strict=False)):
        raise ValueError("spec.scoring.geometry_offsets_s must strictly ascend")
    return scoring


def _external_spec(raw: dict[str, object], owner: str) -> ExternalSpec:
    where = f"{owner}.external"
    block = _as_mapping(_required(raw, "external", owner), where)
    _refuse_unknown(block, frozenset({"name", "version", "notes"}), where)
    return ExternalSpec(
        name=_as_str(_required(block, "name", where), f"{where}.name"),
        version=_as_str(_required(block, "version", where), f"{where}.version"),
        notes=_as_str(_required(block, "notes", where), f"{where}.notes"),
    )


def _fleet(raw: dict[str, object], owner: str) -> tuple[FleetRole, ...]:
    entries = _required(raw, "fleet", owner)
    if not isinstance(entries, list) or not entries:
        raise ValueError(f"{owner}.fleet must be a non-empty list of roles, got {entries!r}")
    roles: list[FleetRole] = []
    for index, entry in enumerate(cast(list[object], entries)):
        where = f"{owner}.fleet[{index}]"
        block = _as_mapping(entry, where)
        _refuse_unknown(block, frozenset({"role", "count", "vcpu", "gib", "machine_type"}), where)
        role = FleetRole(
            role=_as_str(_required(block, "role", where), f"{where}.role"),
            count=_as_int(_required(block, "count", where), f"{where}.count"),
            vcpu=_as_float(_required(block, "vcpu", where), f"{where}.vcpu"),
            gib=_as_float(_required(block, "gib", where), f"{where}.gib"),
            machine_type=_as_str(_required(block, "machine_type", where), f"{where}.machine_type"),
        )
        if role.count < 1:
            raise ValueError(f"{where}.count must be at least 1, got {role.count!r}")
        for key, value in (("vcpu", role.vcpu), ("gib", role.gib)):
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{where}.{key} must be finite and positive, got {value!r}")
        roles.append(role)
    return tuple(roles)


def _compaction_spec(raw: dict[str, object]) -> CompactionSpec | None:
    """Parse an optional compaction layer; omission means no compactor."""
    if "compaction" not in raw:
        return None
    where = "spec.compaction"
    block = _as_mapping(raw["compaction"], where)
    driver = _as_str(_required(block, "driver", where), f"{where}.driver")
    managed = driver in compactors.MANAGED
    if not managed and driver not in (COMPACTION_INLINE, EXTERNAL):
        raise ValueError(
            f"{where}.driver must be {COMPACTION_INLINE!r}, {EXTERNAL!r}, or a registered driver "
            f"({sorted(compactors.MANAGED)}); got {driver!r}"
        )
    _refuse_unknown(block, _COMPACTION_KEYS | ({driver} if managed else frozenset()), where)

    if managed:
        for key in ("external", "fleet"):
            if key in block:
                raise ValueError(f"{where}.driver {driver!r} is rendered here, so it declares no {key} block")
        interval_s = _as_int(_required(block, "interval_s", where), f"{where}.interval_s")
        if interval_s <= 0:
            raise ValueError(f"{where}.interval_s must be positive")
        return CompactionSpec(
            driver=driver,
            interval_s=interval_s,
            driver_block=_as_mapping(_required(block, driver, where), f"{where}.{driver}"),
            external=None,
            fleet=(),
        )

    # Inline and external drivers control their own cadence. Reject interval_s
    # because the harness cannot apply it; commits record the actual cadence.
    if "interval_s" in block:
        raise ValueError(
            f"{where}.driver {driver!r} keeps its own cadence, so {where}.interval_s would set nothing; "
            f"the run reads the cadence it kept from the table's commits"
        )
    if driver == COMPACTION_INLINE:
        for key in ("external", "fleet"):
            if key in block:
                raise ValueError(
                    f"{where}.driver {COMPACTION_INLINE!r} means the ingest engine compacts as it writes, "
                    f"so that work is already inside the run's own fleet and it declares no {key} block"
                )
        return CompactionSpec(driver=driver, interval_s=None, driver_block={}, external=None, fleet=())
    return CompactionSpec(
        driver=driver,
        interval_s=None,
        driver_block={},
        external=_external_spec(block, where),
        fleet=_fleet(block, where),
    )


# ---------------------------------------------------------------------------
# Leaderboard tracks
# ---------------------------------------------------------------------------

TRACKS_DIR = "tracks"

# What a track fixes, shaped like a run spec. A spec naming the track may omit
# these, and any it sets must equal the track's value.
_TRACK_KEYS: dict[str, frozenset[str]] = {
    "kafka": frozenset({"partitions", "key", "partition_assignment", "value_encoding"}),
    "producer": frozenset({"compression", "behind_max_ms"}),
    "scoring": frozenset({"freshness_bound_s", "exactness"}),
    "table": frozenset({"partition"}),
}

# What a track may fix as well. Upsert tracks pin their sample snapshots, so engines are verified at the
# same published points. A track may set a max bound other than twice its p95 bound.
_TRACK_OPTIONAL: dict[str, frozenset[str]] = {"scoring": frozenset({"geometry_offsets_s", "freshness_max_bound_s"})}

# Settings that change the offered workload or the scoring mode, which no track run may.
_TRACK_REFUSED = (("producer", "speed"), ("scoring", "mode"), ("scoring", "measurement_s"))


@dataclass(frozen=True)
class Track:
    """The workload and scoring contract every row in a results section shares.

    ``settings`` holds the fixed values shaped like a run spec. Warmup is a
    ceiling rather than a value: a shorter warmup scores more of the run,
    including more of its cold start.
    """

    name: str
    settings: dict[str, object]
    max_warmup_s: int
    corpus: str
    kafka: KafkaSpec
    compression: str
    behind_max_ms: int
    freshness_bound_s: float
    freshness_max_bound_s: float
    exactness: str
    partition: str
    geometry_offsets_s: tuple[int, ...] | None = None


def track_names(workloads: Path) -> list[str]:
    return sorted(path.stem for path in (workloads / TRACKS_DIR).glob("*.yaml"))


def load_track(name: str, workloads: Path) -> Track:
    canonical = canonical_workload_name(name)
    path = workloads / TRACKS_DIR / f"{canonical}.yaml"
    if not path.is_file():
        raise ValueError(f"track {name!r} is not shipped. The shipped tracks are {track_names(workloads)}")
    where = f"track {name}"
    raw = _load_yaml(path, where)
    _refuse_unknown(raw, frozenset({"corpus", *_TRACK_KEYS}), where)
    corpus = _as_str(_required(raw, "corpus", where), f"{where}.corpus")
    # An archived expanded spec must still match the corpus name it actually ran.
    if name != canonical and corpus == canonical:
        corpus = name
    settings: dict[str, object] = {"corpus": corpus}
    for section, keys in _TRACK_KEYS.items():
        block = _as_mapping(_required(raw, section, where), f"{where}.{section}")
        optional = _TRACK_OPTIONAL.get(section, frozenset())
        required = keys | {"max_warmup_s"} if section == "scoring" else keys
        _refuse_unknown(block, required | optional, f"{where}.{section}")
        for key in sorted(required):
            _required(block, key, f"{where}.{section}")
        settings[section] = {key: block[key] for key in sorted(keys | (optional & set(block)))}
    scoring = _as_mapping(raw["scoring"], f"{where}.scoring")
    max_warmup_s = _as_int(scoring["max_warmup_s"], f"{where}.scoring.max_warmup_s")
    if max_warmup_s < 0:
        raise ValueError(f"{where}.scoring.max_warmup_s must be nonnegative")
    # Reuse the spec validators so a track cannot fix a value no spec could hold.
    producer = _producer_spec(settings)
    scoring_spec = _scoring_spec(settings)
    return Track(
        name=name,
        settings=settings,
        max_warmup_s=max_warmup_s,
        corpus=cast(str, settings["corpus"]),
        kafka=_kafka_spec(settings),
        compression=producer.compression,
        behind_max_ms=producer.behind_max_ms,
        freshness_bound_s=scoring_spec.freshness_bound_s,
        freshness_max_bound_s=scoring_spec.max_bound_s,
        exactness=scoring_spec.exactness,
        partition=_table_spec(settings, partition_default=DEFAULT_PARTITION).partition,
        geometry_offsets_s=scoring_spec.geometry_offsets_s
        if "geometry_offsets_s" in cast(dict[str, object], settings["scoring"])
        else None,
    )


def expand_track(raw: dict[str, object], workloads: Path) -> dict[str, object]:
    """Fill a track's settings into a spec that names it.

    Refuse any setting that differs from the track, so a spec cannot claim a
    track while changing its workload. Expanding an expanded spec changes nothing.
    """
    if "track" not in raw:
        return raw
    name = _as_str(raw["track"], "spec.track")
    track = load_track(name, workloads)
    for section, key in _TRACK_REFUSED:
        if key in _block(raw, section, "spec"):
            raise ValueError(f"spec.{section}.{key} would change what track {name!r} fixes. Leave it out")
    corpus = raw.get("corpus", track.corpus)
    if corpus != track.corpus:
        raise ValueError(f"spec.corpus is {corpus!r}, but track {name!r} fixes it at {track.corpus!r}. Leave it out")
    expanded: dict[str, object] = {**raw, "corpus": track.corpus}
    for section in _TRACK_KEYS:
        merged = dict(_block(raw, section, "spec"))
        for key, value in cast(dict[str, object], track.settings[section]).items():
            if key in merged and merged[key] != value:
                raise ValueError(
                    f"spec.{section}.{key} is {merged[key]!r}, but track {name!r} fixes it at {value!r}. Leave it out"
                )
            merged[key] = value
        expanded[section] = merged
    scoring = cast(dict[str, object], expanded["scoring"])
    warmup_s = _as_int(scoring.get("warmup_s", track.max_warmup_s), "spec.scoring.warmup_s")
    if warmup_s > track.max_warmup_s:
        raise ValueError(f"spec.scoring.warmup_s is {warmup_s}, above track {name!r}'s {track.max_warmup_s} s ceiling")
    scoring["warmup_s"] = warmup_s
    return expanded


_FRESHNESS_BOUND_KEYS = frozenset({"freshness_bound_s", "freshness_max_bound_s"})


def without_freshness_bounds(raw: dict[str, object]) -> dict[str, object]:
    """An archived spec naming a track, less the freshness bounds it was scored under.

    A track's bounds are a publication criterion checked against measured freshness, so an
    archived run still parses after its track tightens them.
    """
    scoring = raw.get("scoring")
    if "track" not in raw or not isinstance(scoring, dict):
        return raw
    return {**raw, "scoring": {key: value for key, value in scoring.items() if key not in _FRESHNESS_BOUND_KEYS}}


def load_archived_run_spec(path: Path) -> RunSpec:
    """Load the expanded spec a run staged, under its track's current freshness bounds."""
    return parse_run_spec(without_freshness_bounds(_load_yaml(path, "run spec")), workloads=workloads_dir(None))


def load_run_spec(path: Path) -> RunSpec:
    """Load a run spec and apply defaults.

    Leave the engine block for its own validator so reading specs does not require
    engine-specific dependencies.
    """
    return parse_run_spec(_load_yaml(path, "run spec"), workloads=workloads_dir(None))


def parse_run_spec(document: dict[str, object], *, workloads: Path) -> RunSpec:
    """Validate a run spec mapping, such as one embedded in a result, and apply defaults."""
    raw = expand_track(document, workloads)
    engine = _as_str(_required(raw, "engine", "spec"), "spec.engine")
    managed = engine in engines.MANAGED
    if not managed and engine != EXTERNAL:
        raise ValueError(
            f"spec.engine {engine!r} is neither {EXTERNAL!r} nor a registered managed engine "
            f"({sorted(engines.MANAGED)})"
        )
    _refuse_unknown(raw, _SPEC_KEYS | ({engine} if managed else frozenset()), "spec")

    name = _as_str(_required(raw, "name", "spec"), "spec.name")
    if NAME_RE.fullmatch(name) is None:
        raise ValueError(
            f"spec.name names a topic, a table and a Kubernetes object, so it must match {NAME_RE.pattern}; "
            f"got {name!r}"
        )
    corpus = _as_str(_required(raw, "corpus", "spec"), "spec.corpus")
    kafka = _kafka_spec(raw)
    table = _table_spec(raw, partition_default=DEFAULT_PARTITION)

    if engine == EXTERNAL:
        external = _external_spec(raw, "spec")
        fleet = _fleet(raw, "spec")
        engine_block: dict[str, object] = {}
    else:
        if "external" in raw or "fleet" in raw:
            raise ValueError(f"spec.engine {engine!r} is managed here, so it declares no external or fleet block")
        external = None
        fleet = ()
        engine_block = _as_mapping(_required(raw, engine, "spec"), f"spec.{engine}")
    compaction = _compaction_spec(raw)

    declared_tier = None
    if "tier" in raw:
        if engine != EXTERNAL:
            raise ValueError(f"spec.tier is derived from the knobs of managed engine {engine!r}. Leave it out")
        declared_tier = _as_str(raw["tier"], "spec.tier")
        if declared_tier not in TIERS:
            raise ValueError(f"spec.tier must be one of {sorted(TIERS)}, got {declared_tier!r}")
        if declared_tier == TIER_REFERENCE and (table.properties or compaction is not None):
            raise ValueError("spec.tier reference allows no table properties and no compaction layer")

    return RunSpec(
        name=name,
        engine=engine,
        corpus=corpus,
        table=table,
        kafka=kafka,
        producer=_producer_spec(raw),
        scoring=_scoring_spec(raw),
        engine_block=engine_block,
        external=external,
        fleet=fleet,
        compaction=compaction,
        track=None if "track" not in raw else _as_str(raw["track"], "spec.track"),
        declared_tier=declared_tier,
    )


# ---------------------------------------------------------------------------
# Site config
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KubernetesConfig:
    """Cluster placement, identities, and environment for run workloads.

    Namespaces and service accounts belong to the site so runs can reuse their
    configured cloud identities. ``aws_region`` supplies the pod region when set.
    ``secret_name`` names a Secret whose keys become environment variables in
    harness and engine pods, resolving ``${env:NAME}`` references at runtime.
    """

    context: str
    namespace: str
    harness_service_account: str
    flink_service_account: str
    spark_service_account: str
    service_account_annotations: dict[str, str]
    registry: str
    aws_region: str | None
    secret_name: str | None
    node_selector: dict[str, str]
    tolerations: list[dict[str, str]]


@dataclass(frozen=True)
class SchemaRegistryConfig:
    """Confluent-compatible registry URL and optional basic-auth configuration.

    Preserve ``${env:NAME}`` references until the registration request needs them.
    """

    url: str
    basic_auth_user_info: str | None


@dataclass(frozen=True)
class SiteConfig:
    """Operator-specific storage, Kafka, catalog, cluster, and pricing settings."""

    corpus_root: str
    runs_root: str
    warehouse: str
    kafka_bootstrap: str
    kafka_security: dict[str, str]
    schema_registry: SchemaRegistryConfig | None
    catalog_props: dict[str, str]
    kubernetes: KubernetesConfig | None
    pricing_vcpu_hour_usd: float
    pricing_gib_hour_usd: float
    kafka_deployment: str | None = None
    pricing_machine_types: dict[str, MachinePrice] = field(default_factory=dict)

    @property
    def pricing(self) -> Pricing:
        if self.pricing_machine_types:
            return {"machine_types": self.pricing_machine_types}
        return {"vcpu_hour_usd": self.pricing_vcpu_hour_usd, "gib_hour_usd": self.pricing_gib_hour_usd}


def _refuse_placeholders(value: object, where: str) -> None:
    """Reject unreplaced example placeholders with their configuration paths."""
    if isinstance(value, str):
        if PLACEHOLDER in value:
            raise ValueError(f"{where} still holds the {PLACEHOLDER} placeholder: {value!r}")
        return
    if isinstance(value, dict):
        for key, entry in cast(dict[object, object], value).items():
            _refuse_placeholders(key, f"{where} key {key!r}")
            _refuse_placeholders(entry, f"{where}.{key}")
        return
    if isinstance(value, list):
        for index, entry in enumerate(cast(list[object], value)):
            _refuse_placeholders(entry, f"{where}[{index}]")


def _kubernetes_config(raw: dict[str, object]) -> KubernetesConfig | None:
    """Read the Kubernetes block; return ``None`` when absent or empty."""
    where = "site.kubernetes"
    block = _block(raw, "kubernetes", "site")
    if not block:
        return None
    _refuse_unknown(block, _KUBERNETES_KEYS, where)
    # Reject empty regions before they reach pod configuration.
    aws_region: str | None = None
    if "aws_region" in block:
        aws_region = _as_str(block["aws_region"], f"{where}.aws_region")
        if not aws_region:
            raise ValueError(f"{where}.aws_region is empty; leave the key out where there is no AWS region")
    # Reject empty Secret names before Kubernetes resource validation.
    secret_name: str | None = None
    if "secret_name" in block:
        secret_name = _as_str(block["secret_name"], f"{where}.secret_name")
        if not secret_name:
            raise ValueError(f"{where}.secret_name is empty; leave the key out where no property names a variable")
    return KubernetesConfig(
        context=_as_str(_required(block, "context", where), f"{where}.context"),
        namespace=_as_str(_required(block, "namespace", where), f"{where}.namespace"),
        harness_service_account=_as_str(
            _required(block, "harness_service_account", where), f"{where}.harness_service_account"
        ),
        flink_service_account=_as_str(
            _required(block, "flink_service_account", where), f"{where}.flink_service_account"
        ),
        spark_service_account=_SPARK_SERVICE_ACCOUNT
        if "spark_service_account" not in block
        else _as_str(block["spark_service_account"], f"{where}.spark_service_account"),
        service_account_annotations={}
        if "service_account_annotations" not in block
        else _as_string_map(block["service_account_annotations"], f"{where}.service_account_annotations"),
        registry=_as_str(_required(block, "registry", where), f"{where}.registry"),
        aws_region=aws_region,
        secret_name=secret_name,
        node_selector={}
        if "node_selector" not in block
        else _as_string_map(block["node_selector"], f"{where}.node_selector"),
        tolerations=[] if "tolerations" not in block else _as_string_maps(block["tolerations"], f"{where}.tolerations"),
    )


def _schema_registry_config(kafka: dict[str, object]) -> SchemaRegistryConfig | None:
    """Read the registry block; return ``None`` when absent or empty.

    Staging rejects Confluent-framed runs if no registry is configured.
    """
    where = "site.kafka.schema_registry"
    block = _block(kafka, "schema_registry", "site.kafka")
    if not block:
        return None
    _refuse_unknown(block, frozenset({"url", "basic_auth_user_info"}), where)
    user_info: str | None = None
    if "basic_auth_user_info" in block:
        user_info = _as_str(block["basic_auth_user_info"], f"{where}.basic_auth_user_info")
        if not user_info:
            raise ValueError(f"{where}.basic_auth_user_info is empty; leave the key out where the registry is open")
    return SchemaRegistryConfig(
        url=_as_str(_required(block, "url", where), f"{where}.url"), basic_auth_user_info=user_info
    )


def load_site(path: Path) -> SiteConfig:
    raw = _load_yaml(path, "site config")
    _refuse_placeholders(raw, "site")
    _refuse_unknown(raw, _SITE_KEYS, "site")

    kafka = _as_mapping(_required(raw, "kafka", "site"), "site.kafka")
    _refuse_unknown(kafka, frozenset({"deployment", "bootstrap_servers", "security", "schema_registry"}), "site.kafka")
    deployment: str | None = None
    if "deployment" in kafka:
        deployment = _as_str(kafka["deployment"], "site.kafka.deployment")
        if deployment not in ("managed", "in-cluster", "external"):
            raise ValueError("site.kafka.deployment must be managed, in-cluster, or external")
    security = {} if "security" not in kafka else _as_string_map(kafka["security"], "site.kafka.security")
    # Allow arbitrary client properties, except conflicting harness settings.
    refuse_mechanism_alias(security, "site.kafka.security")
    refuse_compression_props(security, "site.kafka.security")
    catalog = _as_mapping(_required(raw, "catalog", "site"), "site.catalog")
    _refuse_unknown(catalog, frozenset({"props"}), "site.catalog")
    pricing = _as_mapping(_required(raw, "pricing", "site"), "site.pricing")
    _refuse_unknown(pricing, frozenset({"vcpu_hour_usd", "gib_hour_usd", "machine_types"}), "site.pricing")
    machine_prices: dict[str, MachinePrice] = {}
    if "machine_types" in pricing:
        if "vcpu_hour_usd" in pricing or "gib_hour_usd" in pricing:
            raise ValueError("site.pricing: choose machine_types or flat rates, not both")
        entries = _as_mapping(pricing["machine_types"], "site.pricing.machine_types")
        if not entries:
            raise ValueError("site.pricing.machine_types: must not be empty")
        for machine, entry in entries.items():
            where = f"site.pricing.machine_types.{machine}"
            values = _as_mapping(entry, where)
            _refuse_unknown(values, frozenset({"vcpu", "gib", "usd_per_hour"}), where)
            parsed = {
                key: _as_float(_required(values, key, where), f"{where}.{key}")
                for key in ("vcpu", "gib", "usd_per_hour")
            }
            for key, value in parsed.items():
                if not math.isfinite(value) or value <= 0:
                    raise ValueError(f"{where}.{key}: must be finite and positive")
            machine_prices[machine] = cast(MachinePrice, parsed)
    else:
        for key in ("vcpu_hour_usd", "gib_hour_usd"):
            value = _as_float(_required(pricing, key, "site.pricing"), f"site.pricing.{key}")
            if not math.isfinite(value) or value < 0:
                raise ValueError(f"site.pricing.{key}: must be finite and nonnegative")

    catalog_props = _as_string_map(_required(catalog, "props", "site.catalog"), "site.catalog.props")
    kubernetes = _kubernetes_config(raw)
    registry = _schema_registry_config(kafka)
    # Cluster properties are rendered into ConfigMaps and uploaded to storage,
    # so require credential references. Local sites may use literal defaults.
    if kubernetes is not None:
        refuse_literal_secrets(security, "site.kafka.security")
        refuse_literal_secrets(catalog_props, "site.catalog.props")
        if registry is not None and registry.basic_auth_user_info is not None:
            refuse_literal_secrets(
                {"basic_auth_user_info": registry.basic_auth_user_info}, "site.kafka.schema_registry"
            )

    return SiteConfig(
        corpus_root=_as_str(_required(raw, "corpus_root", "site"), "site.corpus_root"),
        runs_root=_as_str(_required(raw, "runs_root", "site"), "site.runs_root"),
        warehouse=_as_str(_required(raw, "warehouse", "site"), "site.warehouse"),
        kafka_bootstrap=_as_str(_required(kafka, "bootstrap_servers", "site.kafka"), "site.kafka.bootstrap_servers"),
        kafka_security=security,
        kafka_deployment=deployment,
        schema_registry=registry,
        catalog_props=catalog_props,
        kubernetes=kubernetes,
        pricing_vcpu_hour_usd=float(cast(float, pricing.get("vcpu_hour_usd", 0.0))),
        pricing_gib_hour_usd=float(cast(float, pricing.get("gib_hour_usd", 0.0))),
        pricing_machine_types=machine_prices,
    )
