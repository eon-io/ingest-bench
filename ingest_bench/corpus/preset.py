# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Define workloads through a schema and a stream schedule.

Hash the preset to identify its corpus directory. The generated
``corpus.json`` records what was built.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from dataclasses import asdict, dataclass, replace
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import upsert as u
from ingest_bench.workloads import WORKLOAD_ALIASES, canonical_workload_name

KEYS = (
    "schema",
    "value_encoding",
    "sizing_encoding",
    "offered_bytes_per_s",
    "duration_s",
    "partition_count",
    "alpha",
    "target_row_bytes",
    "batch_interval_ms",
    "corpus_epoch",
    "kafka_key_columns",
    "column_overrides",
    "upsert",
)

_QUANTITY = re.compile(r"^\s*(\d+)\s*([KMGT]?i?B)?\s*$")
_DECIMAL = {"B": 1, "KB": 10**3, "MB": 10**6, "GB": 10**9, "TB": 10**12}
_BINARY = {"KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40}


def parse_quantity(text: str | int) -> int:
    if isinstance(text, int):
        return text
    match = _QUANTITY.match(text)
    if not match:
        raise ValueError(f"not a byte quantity: {text!r}")
    number, unit = int(match.group(1)), match.group(2) or "B"
    scale = _DECIMAL.get(unit) or _BINARY.get(unit)
    if scale is None:
        raise ValueError(f"unknown unit in {text!r}")
    return number * scale


@dataclass(frozen=True)
class Preset:
    name: str
    schema_name: str
    offered_bytes_per_s: int
    duration_s: int
    partition_count: int
    alpha: float
    target_row_bytes: int
    batch_interval_ms: int
    corpus_epoch: str
    kafka_key_columns: tuple[str, ...]
    column_overrides: dict[str, dict[str, object]]
    columns: tuple[c.ColumnDistribution, ...]
    value_encoding: str = "avro"
    sizing_encoding: str | None = None
    upsert: u.UpsertConfig | None = None

    def __post_init__(self) -> None:
        if self.value_encoding not in {"avro", "json"}:
            raise ValueError("corpus value_encoding must be avro or json")
        if self.sizing_encoding is not None and (self.value_encoding != "json" or self.sizing_encoding != "avro"):
            raise ValueError("sizing_encoding may only be avro for a JSON corpus")

    @property
    def budget_encoding(self) -> str:
        return self.sizing_encoding or self.value_encoding

    @property
    def batch_count(self) -> int:
        total_ms = self.duration_s * 1000
        if total_ms % self.batch_interval_ms:
            raise ValueError("duration_s * 1000 must be a multiple of batch_interval_ms")
        return total_ms // self.batch_interval_ms

    @property
    def batch_bytes(self) -> int:
        return self.offered_bytes_per_s * self.batch_interval_ms // 1000


def _apply_override(raw: dict[str, object], assignment: str) -> None:
    key, sep, value = assignment.partition("=")
    if not sep:
        raise ValueError(f"--set expects key=value, got {assignment!r}")
    parsed: object = yaml.safe_load(value)
    if key.startswith("column_overrides."):
        _, column, attribute = key.split(".", 2)
        overrides = cast(dict[str, dict[str, object]], raw.setdefault("column_overrides", {}))
        overrides.setdefault(column, {})[attribute] = parsed
        return
    if key.startswith("upsert."):
        block = cast(dict[str, object], raw.setdefault("upsert", {}))
        block[key.split(".", 1)[1]] = parsed
        return
    if key not in KEYS:
        raise ValueError(f"unknown preset key {key!r}; known keys: {', '.join(KEYS)}")
    raw[key] = parsed


def _refuse_event_time_finer_than_the_batch(preset: Preset) -> None:
    """Reject event-time cardinality beyond the batch's millisecond window.

    A window of N milliseconds supports at most N distinct timestamps. The
    unbounded form uses the available window and needs no cardinality check.
    """
    for column in preset.columns:
        if column.role != c.ROLE_EVENT_TIME or column.cardinality == c.UNBOUNDED_CARDINALITY:
            continue
        if column.cardinality > preset.batch_interval_ms:
            raise ValueError(
                f"column {column.name} declares cardinality {column.cardinality} inside a "
                f"{preset.batch_interval_ms} ms batch window, which holds {preset.batch_interval_ms} distinct "
                "event times; lower the cardinality or raise batch_interval_ms"
            )


def resolve_preset_path(source: str, workloads_dir: Path) -> Path:
    path = Path(source)
    if path.suffix in (".yaml", ".yml") and path.exists():
        return path
    candidate = workloads_dir / "presets" / f"{canonical_workload_name(source)}.yaml"
    if not candidate.exists():
        raise FileNotFoundError(f"no preset {source!r}: not a file and {candidate} does not exist")
    return candidate


def load_preset(source: str, *, workloads_dir: Path, overrides: Sequence[str] = ()) -> Preset:
    path = resolve_preset_path(source, workloads_dir)
    raw = cast(dict[str, object], yaml.safe_load(path.read_text()))
    unknown = sorted(set(raw) - set(KEYS))
    if unknown:
        raise ValueError(f"{path}: unknown preset key(s): {', '.join(unknown)}")
    for assignment in overrides:
        _apply_override(raw, assignment)
    missing = [key for key in KEYS if key not in {"value_encoding", "sizing_encoding", "upsert"} and key not in raw]
    if missing:
        raise ValueError(f"{path}: missing preset key(s): {', '.join(missing)}")
    schema_name = str(raw["schema"])
    columns = c.load_schema(workloads_dir / "schemas" / f"{schema_name}.json")
    column_overrides = cast(dict[str, dict[str, object]], raw["column_overrides"])
    columns = c.apply_column_overrides(columns, column_overrides)
    key_columns = tuple(str(k) for k in cast(list[object], raw["kafka_key_columns"]))
    by_name = {column.name: column for column in columns}
    bad = [k for k in key_columns if k not in by_name]
    if bad:
        raise ValueError(f"kafka_key_columns name unknown column(s): {', '.join(bad)}")
    # Kafka keys require categorical strings or the reserved `partition_key`.
    # The reserved numeric `id` cannot be used.
    non_string = [k for k in key_columns if k != "partition_key" and by_name[k].kind != c.KIND_CATEGORICAL]
    if non_string:
        raise ValueError(f"kafka_key_columns must be string columns: {', '.join(non_string)}")
    # A sidecar frames one key string per row.
    nullable = [k for k in key_columns if by_name[k].nullable]
    if nullable:
        raise ValueError(f"kafka_key_columns must be required columns: {', '.join(nullable)}")
    preset = Preset(
        name=source if source in WORKLOAD_ALIASES else path.stem,
        schema_name=schema_name,
        offered_bytes_per_s=parse_quantity(cast(str | int, raw["offered_bytes_per_s"])),
        duration_s=int(cast(int, raw["duration_s"])),
        partition_count=int(cast(int, raw["partition_count"])),
        alpha=float(cast(float, raw["alpha"])),
        target_row_bytes=int(cast(int, raw["target_row_bytes"])),
        batch_interval_ms=int(cast(int, raw["batch_interval_ms"])),
        corpus_epoch=str(raw["corpus_epoch"]),
        kafka_key_columns=key_columns,
        column_overrides=column_overrides,
        columns=columns,
        value_encoding=str(raw.get("value_encoding", "avro")),
        sizing_encoding=None if raw.get("sizing_encoding") is None else str(raw["sizing_encoding"]),
        upsert=None if raw.get("upsert") is None else u.parse_upsert(raw["upsert"]),
    )
    if (
        preset.partition_count < 1
        or preset.duration_s < 1
        or preset.batch_interval_ms < 1
        or preset.offered_bytes_per_s < 1
    ):
        raise ValueError("partition_count, duration_s, batch_interval_ms and offered_bytes_per_s must be positive")
    preset.batch_count  # noqa: B018 - validates divisibility at load time
    # This check needs the preset's batch window, not just the column declaration.
    _refuse_event_time_finer_than_the_batch(preset)
    # The partition column must have a key sidecar.
    if "partition_key" not in preset.kafka_key_columns:
        preset = replace(preset, kafka_key_columns=(*preset.kafka_key_columns, "partition_key"))
    if preset.upsert is not None:
        key_column = by_name.get(preset.upsert.key_column)
        if key_column is None or key_column.role != c.ROLE_ENTITY or key_column.cardinality != c.UNBOUNDED_CARDINALITY:
            raise ValueError(
                f"upsert.key_column {preset.upsert.key_column!r} must be the schema's role entity column, "
                "an unbounded categorical, so key tokens can derive from key ordinals"
            )
        # The upsert key is the Kafka key, so it needs a sidecar.
        if preset.upsert.key_column not in preset.kafka_key_columns:
            preset = replace(preset, kafka_key_columns=(preset.upsert.key_column, *preset.kafka_key_columns))
    return preset


def effective_dict(preset: Preset) -> dict[str, object]:
    data = asdict(preset)
    # Omit defaults so existing corpus identities remain stable.
    if preset.sizing_encoding is None:
        del data["sizing_encoding"]
    if preset.value_encoding == "avro":
        del data["value_encoding"]
    if preset.upsert is None:
        del data["upsert"]
    else:
        data["upsert"] = u.effective(preset.upsert)
    data["columns"] = [c.declaration(column) for column in preset.columns]
    return data


def canonical_json(data: dict[str, object]) -> str:
    """Serialize a preset consistently for comparison and hashing."""
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def corpus_hash(preset: Preset, seed: int, *, row_block: int) -> str:
    """Identify the workload and the generator inputs that determine its rows."""
    effective = effective_dict(preset)
    # The standard corpus was renamed without changing its records. Retain the original
    # hash input for those two labels so stored corpora and published runs remain reusable.
    effective["name"] = next(
        (legacy for legacy, current in WORKLOAD_ALIASES.items() if current == preset.name), preset.name
    )
    identity = {
        "preset": effective,
        "seed": seed,
        "generator_version": GENERATOR_VERSION,
        "row_block": row_block,
    }
    return hashlib.sha256(canonical_json(identity).encode()).hexdigest()[:8]


def preset_from_effective(effective: dict[str, object], schema_name: str) -> Preset:
    """Rebuild a preset from the effective form recorded in ``corpus.json``.

    Require an exact round trip so merge gates use the generated workload,
    independently of schema files on the merging machine.
    """
    if str(effective["schema_name"]) != schema_name:
        raise ValueError(f"the effective preset names schema {effective['schema_name']!r}, not {schema_name!r}")
    declared = [cast(dict[str, object], entry) for entry in cast(list[object], effective["columns"])]
    preset = Preset(
        name=str(effective["name"]),
        schema_name=schema_name,
        value_encoding=str(effective.get("value_encoding", "avro")),
        sizing_encoding=None if effective.get("sizing_encoding") is None else str(effective["sizing_encoding"]),
        upsert=None if effective.get("upsert") is None else u.from_effective(effective["upsert"]),
        offered_bytes_per_s=c.as_int_value(effective["offered_bytes_per_s"]),
        duration_s=c.as_int_value(effective["duration_s"]),
        partition_count=c.as_int_value(effective["partition_count"]),
        alpha=c.as_float_value(effective["alpha"]),
        target_row_bytes=c.as_int_value(effective["target_row_bytes"]),
        batch_interval_ms=c.as_int_value(effective["batch_interval_ms"]),
        corpus_epoch=str(effective["corpus_epoch"]),
        kafka_key_columns=tuple(str(name) for name in cast(list[object], effective["kafka_key_columns"])),
        column_overrides=cast(dict[str, dict[str, object]], effective["column_overrides"]),
        # Restore implicit reserved columns before rebuilding generated columns.
        columns=c.build_columns(
            tuple(c.column_from_dict(entry) for entry in declared if str(entry["kind"]) != c.KIND_RESERVED)
        ),
    )
    if canonical_json(effective_dict(preset)) != canonical_json(effective):
        raise ValueError(
            f"the preset rebuilt for {preset.name} does not reproduce the effective preset it was read from"
        )
    return preset


def corpus_dir_name_for(name: str, hash_value: str) -> str:
    """The corpus directory rule, for a caller holding only what `corpus.json` published."""
    return f"{name}-{hash_value}"


def corpus_dir_name(preset: Preset, seed: int, *, row_block: int) -> str:
    return corpus_dir_name_for(preset.name, corpus_hash(preset, seed, row_block=row_block))
