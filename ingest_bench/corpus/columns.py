# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Declare and validate generated column distributions, and type each column kind.

Schema files list generated columns; reserved columns are implicit and come
first. Validate declarations before generation. ``KINDS`` is the registry of
how each kind validates and how Avro, Iceberg, Flink, Spark and JSON type its
values. Every layer that names a column type derives it from this registry.
"""

from __future__ import annotations

import base64
import itertools
import json
from collections.abc import Callable, Iterator
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta
from decimal import Context, Decimal, DivisionByZero, Inexact, InvalidOperation, Overflow
from pathlib import Path
from typing import TypedDict, cast

from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    PrimitiveType,
    StringType,
    StructType,
    TimestampType,
    UUIDType,
)

# Checksum modulus; changing it changes published scoring truth.
P = 1_000_000_007

# Reserve a disjoint ID range per batch: batch * ID_BLOCK + position.
ID_BLOCK = 2**32


class SchemaField(TypedDict):
    name: str
    type: object


# ---------------------------------------------------------------------------
# Per-column value distributions
# ---------------------------------------------------------------------------

KIND_RESERVED = "reserved"
KIND_CATEGORICAL = "categorical"
KIND_INTEGER = "integer"
# A binary double rounded to hundredths. The name is part of corpus identity,
# so `exact_decimal` is the fixed-precision decimal.
KIND_DECIMAL = "decimal"
KIND_BOOLEAN = "boolean"
KIND_TIMESTAMP = "timestamp"
KIND_BLOB = "blob"
KIND_DATE = "date"
KIND_INT32 = "int32"
KIND_FLOAT32 = "float32"
KIND_EXACT_DECIMAL = "exact_decimal"
KIND_UUID = "uuid"
KIND_STRUCT = "struct"
KIND_LIST = "list"
KIND_MAP = "map"

NESTED_KINDS = frozenset({KIND_STRUCT, KIND_LIST, KIND_MAP})
# Whole numbers drawn over [minimum, maximum]. A date counts days since 1970-01-01.
INTEGRAL_KINDS = frozenset({KIND_INTEGER, KIND_INT32, KIND_DATE})

# Reserved fields are computed from row position and typed like the kind whose
# values they share, in schema order.
RESERVED_KINDS = {"id": KIND_INTEGER, "partition_key": KIND_CATEGORICAL}
RESERVED_FIELDS = frozenset(RESERVED_KINDS)

# Roles let queries and generation logic identify columns independently of names.
ROLE_GENERIC = "generic"
ROLE_EVENT_TIME = "event_time"
ROLE_ENTITY = "entity"
ROLE_SUM_MEASURE = "sum_measure"
ROLE_PAYLOAD = "payload"

ROLES = frozenset({ROLE_GENERIC, ROLE_EVENT_TIME, ROLE_ENTITY, ROLE_SUM_MEASURE, ROLE_PAYLOAD})

# Require one event clock, lookup key, aggregate measure, and calibrated payload.
SINGLETON_ROLES: dict[str, frozenset[str]] = {
    ROLE_EVENT_TIME: frozenset({KIND_TIMESTAMP}),
    ROLE_ENTITY: frozenset({KIND_CATEGORICAL}),
    ROLE_SUM_MEASURE: frozenset({KIND_INTEGER, KIND_DECIMAL}),
    ROLE_PAYLOAD: frozenset({KIND_BLOB}),
}

# Zero selects the kind-specific unbounded distribution.
UNBOUNDED_CARDINALITY = 0

# A 16-character hex token provides a 64-bit value space. Enforce this
# minimum to limit collisions in unbounded token columns.
MIN_UNBOUNDED_TOKEN_WIDTH = 16

# Bound memory for materialized Zipf CDFs. Uniform ranks use modulus
# and do not need this allocation.
COLUMN_ZIPF_MAX_CARDINALITY = 1_000_000

INT32_MIN = -(2**31)
INT32_MAX = 2**31 - 1
# JSON writes dates as ISO strings, which span 0001-01-01 to 9999-12-31.
DATE_MIN_DAYS = (date.min - date(1970, 1, 1)).days
DATE_MAX_DAYS = (date.max - date(1970, 1, 1)).days
# float32 keeps 24 significant bits, which tell hundredths apart only below 2**17.
FLOAT32_HUNDREDTHS_LIMIT = 2**17
# Iceberg's widest decimal.
MAX_DECIMAL_PRECISION = 38
# Decimal arithmetic rounds to the caller's context, 28 digits by default. This
# context holds any exact_decimal and traps rounding, so values and their bytes
# never depend on the caller.
DECIMAL_CONTEXT = Context(prec=MAX_DECIMAL_PRECISION, traps=[InvalidOperation, DivisionByZero, Overflow, Inexact])


@dataclass(frozen=True)
class ColumnDistribution:
    """Distribution and semantic role of one generated field.

    ``cardinality`` counts ranks; zero selects the unbounded form. ``alpha`` is
    the Zipf exponent, with zero selecting uniform ranks. Generation is
    deterministic for each row position.

    Ranks need not equal distinct corpus values: numeric values are bounded by
    their range, and timestamps map ranks to millisecond buckets within each
    batch window. Realized-cardinality checks apply only where the mapping can
    be compared meaningfully.

    ``null_fraction`` is the share of null rows, and zero keeps the column
    required. ``precision`` and ``scale`` shape an ``exact_decimal``. A
    ``struct`` nests ``fields``, a ``list`` its ``element`` and a ``map`` its
    ``key`` and ``value``. A list or map holds ``[minimum, maximum]`` entries per row.
    """

    name: str
    kind: str
    role: str = ROLE_GENERIC
    cardinality: int = UNBOUNDED_CARDINALITY
    alpha: float = 0.0
    vocabulary: tuple[str, ...] = ()
    minimum: float = 0.0
    maximum: float = 0.0
    width: int = 0
    null_fraction: float = 0.0
    precision: int = 0
    scale: int = 0
    fields: tuple[ColumnDistribution, ...] = ()
    element: ColumnDistribution | None = None
    key: ColumnDistribution | None = None
    value: ColumnDistribution | None = None

    @property
    def nullable(self) -> bool:
        return self.null_fraction > 0.0


def children(column: ColumnDistribution) -> tuple[ColumnDistribution, ...]:
    """Nested declarations in encoding order: struct fields, a list element, or a map key then value."""
    return column.fields + tuple(child for child in (column.element, column.key, column.value) if child is not None)


def nested_child(column: ColumnDistribution, child: ColumnDistribution | None) -> ColumnDistribution:
    if child is None:
        raise ValueError(f"column {column.name} is a {column.kind} without its nested declaration")
    return child


def reserved_column(name: str) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_RESERVED)


def categorical_column(
    name: str, cardinality: int, vocabulary: tuple[str, ...], alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_CATEGORICAL, role=role, cardinality=cardinality, alpha=alpha, vocabulary=vocabulary
    )


def token_column(name: str, width: int, role: str = ROLE_GENERIC) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_CATEGORICAL, role=role, cardinality=UNBOUNDED_CARDINALITY, width=width
    )


def integer_column(
    name: str, cardinality: int, minimum: int, maximum: int, alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name,
        kind=KIND_INTEGER,
        role=role,
        cardinality=cardinality,
        alpha=alpha,
        minimum=float(minimum),
        maximum=float(maximum),
    )


def decimal_column(
    name: str, cardinality: int, minimum: float, maximum: float, alpha: float = 0.0, role: str = ROLE_GENERIC
) -> ColumnDistribution:
    return ColumnDistribution(
        name=name, kind=KIND_DECIMAL, role=role, cardinality=cardinality, alpha=alpha, minimum=minimum, maximum=maximum
    )


def boolean_column(name: str, alpha: float = 0.0, cardinality: int = 2) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_BOOLEAN, cardinality=cardinality, alpha=alpha)


def timestamp_column(name: str, cardinality: int) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_TIMESTAMP, role=ROLE_EVENT_TIME, cardinality=cardinality)


def blob_column(name: str, cardinality: int = UNBOUNDED_CARDINALITY) -> ColumnDistribution:
    return ColumnDistribution(name=name, kind=KIND_BLOB, role=ROLE_PAYLOAD, cardinality=cardinality)


def unscaled_bounds(column: ColumnDistribution) -> tuple[int, int]:
    """Return an ``exact_decimal`` range as unscaled integers, reading each bound's shortest decimal form."""
    low, high = (
        int(Decimal(repr(bound)).scaleb(column.scale, DECIMAL_CONTEXT).to_integral_value(context=DECIMAL_CONTEXT))
        for bound in (column.minimum, column.maximum)
    )
    return low, high


def categorical_label(column: ColumnDistribution, rank: int) -> str:
    vocabulary = column.vocabulary
    if rank < len(vocabulary):
        return vocabulary[rank]
    return f"{vocabulary[rank % len(vocabulary)]}-{rank // len(vocabulary):05d}"


def labels_collide(column: ColumnDistribution) -> bool:
    """Find two ranks of a bounded categorical that share a label, in time proportional to its vocabulary.

    A suffix holds digits alone, so suffixed labels differ unless entries repeat.
    A suffixed label can only equal an entry shaped like an earlier-used entry
    plus the suffix of a rank within the cardinality.
    """
    vocabulary = column.vocabulary
    used = vocabulary[: column.cardinality]
    if len(set(used)) < len(used):
        return True
    # A key without a vocabulary is rejected by its own validation.
    if not vocabulary or column.cardinality <= len(vocabulary):
        return False
    index_of = {entry: index for index, entry in enumerate(vocabulary)}
    # A reachable suffix is no wider than the largest quotient or the five-digit pad,
    # which also keeps int() within Python's digit limit for any entry.
    widest = max(5, len(str((column.cardinality - 1) // len(vocabulary))))
    for entry in vocabulary:
        prefix, hyphen, digits = entry.rpartition("-")
        if not hyphen or not (digits.isascii() and digits.isdigit()) or len(digits) > widest:
            continue
        quotient = int(digits)
        index = index_of.get(prefix)
        if index is None or quotient < 1 or f"{quotient:05d}" != digits:
            continue
        if quotient * len(vocabulary) + index < column.cardinality:
            return True
    return False


def numeric_span(column: ColumnDistribution) -> int:
    """Count the distinct values the rounded numeric range holds."""
    if column.kind in INTEGRAL_KINDS:
        return int(column.maximum) - int(column.minimum) + 1
    if column.kind == KIND_EXACT_DECIMAL:
        low, high = unscaled_bounds(column)
        return high - low + 1
    return int(round((column.maximum - column.minimum) * 100)) + 1


# ---------------------------------------------------------------------------
# Validating one kind
# ---------------------------------------------------------------------------


def _validate_categorical(column: ColumnDistribution) -> None:
    if column.minimum or column.maximum:
        raise ValueError(f"column {column.name} is categorical, so it has no numeric range to declare")
    if column.cardinality != UNBOUNDED_CARDINALITY:
        if not column.vocabulary:
            raise ValueError(f"column {column.name} needs a vocabulary")
        if column.width:
            raise ValueError(
                f"column {column.name} is a bounded categorical, whose labels come from its vocabulary "
                "rather than from a width"
            )
    else:
        if column.vocabulary:
            raise ValueError(
                f"column {column.name} is an unbounded categorical, whose tokens are digests "
                "rather than vocabulary labels"
            )
        if column.width < MIN_UNBOUNDED_TOKEN_WIDTH:
            raise ValueError(
                f"column {column.name} needs width >= {MIN_UNBOUNDED_TOKEN_WIDTH} to emit a fresh token per row"
            )


def _validate_numeric(column: ColumnDistribution) -> None:
    if column.vocabulary or column.width:
        raise ValueError(f"column {column.name} is numeric, so it has no vocabulary or width to declare")
    if column.maximum < column.minimum:
        raise ValueError(f"column {column.name} has an inverted numeric range")
    if column.kind == KIND_INT32 and not INT32_MIN <= column.minimum <= column.maximum <= INT32_MAX:
        raise ValueError(f"column {column.name} is int32, so its range must fit in 32 bits: [{INT32_MIN}, {INT32_MAX}]")
    if column.kind == KIND_DATE and not DATE_MIN_DAYS <= column.minimum <= column.maximum <= DATE_MAX_DAYS:
        raise ValueError(
            f"column {column.name} is a date, so its range must lie within [{DATE_MIN_DAYS}, {DATE_MAX_DAYS}], "
            "the days from 0001-01-01 to 9999-12-31"
        )
    limit = FLOAT32_HUNDREDTHS_LIMIT
    if column.kind == KIND_FLOAT32 and not -limit < column.minimum <= column.maximum < limit:
        raise ValueError(
            f"column {column.name} is float32, which tells hundredths apart only within (-{limit}, {limit})"
        )
    # The rounded numeric range must accommodate the declared ranks.
    # A single-rank column is constant at the minimum.
    span = numeric_span(column)
    needed = column.cardinality if column.cardinality != UNBOUNDED_CARDINALITY else 2
    if column.cardinality != 1 and needed > span:
        raise ValueError(
            f"column {column.name} needs a numeric range holding at least {needed} values, "
            f"but [{column.minimum}, {column.maximum}] holds {span}"
        )


def _validate_exact_decimal(column: ColumnDistribution) -> None:
    if not 1 <= column.precision <= MAX_DECIMAL_PRECISION:
        raise ValueError(f"column {column.name} needs a precision from 1 to {MAX_DECIMAL_PRECISION}")
    if not 0 <= column.scale <= column.precision:
        raise ValueError(f"column {column.name} needs a scale from 0 to its precision {column.precision}")
    _validate_numeric(column)
    limit = 10**column.precision
    low, high = unscaled_bounds(column)
    if low <= -limit or high >= limit:
        raise ValueError(
            f"column {column.name} ranges over [{column.minimum}, {column.maximum}], which "
            f"DECIMAL({column.precision}, {column.scale}) cannot hold"
        )


def _validate_boolean(column: ColumnDistribution) -> None:
    if column.vocabulary or column.width or column.minimum or column.maximum:
        raise ValueError(f"column {column.name} is boolean, so it has no vocabulary, width or numeric range to declare")
    if column.cardinality not in {1, 2}:
        raise ValueError(
            f"column {column.name} is boolean, so its cardinality must be 1 or 2, not {column.cardinality}"
        )


def _validate_timestamp(column: ColumnDistribution) -> None:
    if column.vocabulary or column.width or column.minimum or column.maximum:
        raise ValueError(
            f"column {column.name} is a timestamp, so it has no vocabulary, width or numeric range to declare"
        )


def _validate_blob(column: ColumnDistribution) -> None:
    if column.vocabulary or column.minimum or column.maximum:
        raise ValueError(f"column {column.name} is a blob, so it has no vocabulary or numeric range to declare")
    # Only the payload role uses calibrated width; other blobs declare their own.
    if column.role == ROLE_PAYLOAD:
        if column.width:
            raise ValueError(
                f"column {column.name} holds the {ROLE_PAYLOAD} role, whose width is the calibrated remainder "
                "rather than a declaration"
            )
    elif column.width <= 0:
        raise ValueError(
            f"column {column.name} needs a positive width, since only the {ROLE_PAYLOAD} column takes the "
            "calibrated width"
        )


def _validate_uuid(column: ColumnDistribution) -> None:
    if column.vocabulary or column.width or column.minimum or column.maximum:
        raise ValueError(f"column {column.name} is a uuid, so it has no vocabulary, width or numeric range to declare")


def _validate_nested_member(parent: ColumnDistribution, child: ColumnDistribution) -> None:
    if child.role != ROLE_GENERIC:
        raise ValueError(
            f"column {parent.name} nests {child.name} with role {child.role}, but roles name top-level columns"
        )
    validate_column(child)


def _refuse_rank_attributes(column: ColumnDistribution) -> None:
    if column.cardinality or column.alpha or column.vocabulary or column.width:
        raise ValueError(
            f"column {column.name} is a {column.kind}, whose values come from its nested declarations, so it has no "
            "cardinality, alpha, vocabulary or width to declare"
        )


def _validate_struct(column: ColumnDistribution) -> None:
    _refuse_rank_attributes(column)
    if column.minimum or column.maximum:
        raise ValueError(f"column {column.name} is a struct, so it has no length range to declare")
    if not column.fields or column.element or column.key or column.value:
        raise ValueError(f"column {column.name} is a struct, so it nests one or more fields and nothing else")
    names = [field.name for field in column.fields]
    if len(set(names)) != len(names):
        raise ValueError(f"column {column.name} nests fields with duplicate names")
    for field in column.fields:
        _validate_nested_member(column, field)


def _validate_length_range(column: ColumnDistribution) -> None:
    whole = column.minimum == int(column.minimum) and column.maximum == int(column.maximum)
    if not whole or not 0 <= column.minimum <= column.maximum or column.maximum < 1:
        raise ValueError(
            f"column {column.name} is a {column.kind}, whose minimum and maximum bound its entries per row, "
            "so they must be whole numbers with 0 <= minimum <= maximum and maximum >= 1"
        )


def _validate_list(column: ColumnDistribution) -> None:
    _refuse_rank_attributes(column)
    _validate_length_range(column)
    if column.element is None or column.fields or column.key or column.value:
        raise ValueError(f"column {column.name} is a list, so it nests one element and nothing else")
    _validate_nested_member(column, column.element)


def _validate_map(column: ColumnDistribution) -> None:
    _refuse_rank_attributes(column)
    _validate_length_range(column)
    if column.key is None or column.value is None or column.fields or column.element:
        raise ValueError(f"column {column.name} is a map, so it nests one key, one value and nothing else")
    # A row's keys are consecutive ranks, so the key space must hold its longest map.
    key = column.key
    if key.kind != KIND_CATEGORICAL or key.cardinality < column.maximum or key.nullable:
        raise ValueError(
            f"column {column.name} needs a required, bounded categorical key with cardinality at least its "
            f"maximum {int(column.maximum)}, so the keys in one map stay distinct"
        )
    # Labels repeat for a vocabulary holding "k" twice, or "k" and "k-00001".
    if labels_collide(key):
        raise ValueError(f"column {column.name} has a key whose ranks share labels, so a map could repeat a key")
    _validate_nested_member(column, key)
    _validate_nested_member(column, column.value)


# ---------------------------------------------------------------------------
# The kind registry
# ---------------------------------------------------------------------------

JsonValue = Callable[[object], object]

_EPOCH = datetime(1970, 1, 1)
_EPOCH_DATE = date(1970, 1, 1)


def _iso_millis(value: object) -> object:
    # Iceberg timestamps are zoneless. Express the UTC wall clock without an offset.
    return (_EPOCH + timedelta(milliseconds=cast(int, value))).isoformat(timespec="milliseconds")


def _iso_date(value: object) -> object:
    return (_EPOCH_DATE + timedelta(days=cast(int, value))).isoformat()


def _base64(value: object) -> object:
    return base64.b64encode(cast(bytes, value)).decode("ascii")


def _fixed_point(value: object) -> object:
    # A string keeps every declared digit, which a JSON number would round through a double.
    return f"{cast(Decimal, value):f}"


@dataclass(frozen=True)
class Kind:
    """How one column kind validates and how each system types its values.

    ``avro`` takes the namespace of the enclosing record, which keeps nested
    record names unique. ``iceberg`` draws field IDs for nested types from the
    iterator. ``flink`` is None where Flink cannot write the kind to Iceberg.
    ``json`` returns the conversion to a value's JSON representation, or None
    where the generated value serializes as is.
    """

    validate: Callable[[ColumnDistribution], None]
    avro: Callable[[ColumnDistribution, str], object]
    iceberg: Callable[[ColumnDistribution, Iterator[int]], IcebergType]
    flink: Callable[[ColumnDistribution], str] | None
    spark: Callable[[ColumnDistribution], str]
    json: Callable[[ColumnDistribution], JsonValue | None]


def _scalar(
    validate: Callable[[ColumnDistribution], None],
    avro: object,
    iceberg: PrimitiveType,
    flink: str | None,
    spark: str,
    json_value: JsonValue | None = None,
) -> Kind:
    return Kind(
        validate=validate,
        avro=lambda column, namespace: avro,
        iceberg=lambda column, ids: iceberg,
        flink=None if flink is None else (lambda column: flink),
        spark=lambda column: spark,
        json=lambda column: json_value,
    )


def _decimal_type(column: ColumnDistribution) -> str:
    return f"DECIMAL({column.precision}, {column.scale})"


def quoted_identifier(name: str) -> str:
    """Quote ``name`` as a backtick identifier, so a column named after a SQL keyword still parses."""
    escaped = name.replace("`", "``")
    return f"`{escaped}`"


def _not_null(column: ColumnDistribution) -> str:
    return "" if column.nullable else " NOT NULL"


def _struct_avro(column: ColumnDistribution, namespace: str) -> object:
    inner = f"{namespace}.{column.name}"
    fields = [{"name": field.name, "type": avro_type(field, inner)} for field in column.fields]
    return {"type": "record", "name": column.name, "namespace": namespace, "fields": fields}


def _struct_iceberg(column: ColumnDistribution, ids: Iterator[int]) -> IcebergType:
    return StructType(
        *(
            NestedField(next(ids), field.name, iceberg_type(field, ids), required=not field.nullable)
            for field in column.fields
        )
    )


def _struct_flink(column: ColumnDistribution) -> str:
    return "ROW<" + ", ".join(f"{quoted_identifier(field.name)} {flink_type(field)}" for field in column.fields) + ">"


def _struct_spark(column: ColumnDistribution) -> str:
    members = ", ".join(
        f"{quoted_identifier(field.name)}: {spark_type(field)}{_not_null(field)}" for field in column.fields
    )
    return f"STRUCT<{members}>"


def _struct_json(column: ColumnDistribution) -> JsonValue | None:
    converters = [(field.name, json_converter(field)) for field in column.fields]
    if all(convert is None for _, convert in converters):
        return None

    def convert(value: object) -> object:
        row = cast(dict[str, object], value)
        return {name: row[name] if field is None else field(row[name]) for name, field in converters}

    return convert


def _list_avro(column: ColumnDistribution, namespace: str) -> object:
    return {"type": "array", "items": avro_type(nested_child(column, column.element), f"{namespace}.{column.name}")}


def _list_iceberg(column: ColumnDistribution, ids: Iterator[int]) -> IcebergType:
    element = nested_child(column, column.element)
    element_id = next(ids)
    return ListType(element_id, iceberg_type(element, ids), element_required=not element.nullable)


def _list_flink(column: ColumnDistribution) -> str:
    return f"ARRAY<{flink_type(nested_child(column, column.element))}>"


def _list_spark(column: ColumnDistribution) -> str:
    return f"ARRAY<{spark_type(nested_child(column, column.element))}>"


def _list_json(column: ColumnDistribution) -> JsonValue | None:
    element = json_converter(nested_child(column, column.element))
    if element is None:
        return None
    return lambda value: [element(entry) for entry in cast(list[object], value)]


def _map_avro(column: ColumnDistribution, namespace: str) -> object:
    # Avro map keys are strings, so the key declaration shapes values only.
    return {"type": "map", "values": avro_type(nested_child(column, column.value), f"{namespace}.{column.name}")}


def _map_iceberg(column: ColumnDistribution, ids: Iterator[int]) -> IcebergType:
    key, value = nested_child(column, column.key), nested_child(column, column.value)
    key_id = next(ids)
    key_type = iceberg_type(key, ids)
    value_id = next(ids)
    return MapType(key_id, key_type, value_id, iceberg_type(value, ids), value_required=not value.nullable)


def _map_flink(column: ColumnDistribution) -> str:
    return f"MAP<{flink_type(nested_child(column, column.key))}, {flink_type(nested_child(column, column.value))}>"


def _map_spark(column: ColumnDistribution) -> str:
    return f"MAP<{spark_type(nested_child(column, column.key))}, {spark_type(nested_child(column, column.value))}>"


def _map_json(column: ColumnDistribution) -> JsonValue | None:
    entry = json_converter(nested_child(column, column.value))
    if entry is None:
        return None
    return lambda value: {key: entry(item) for key, item in cast(dict[str, object], value).items()}


KINDS: dict[str, Kind] = {
    KIND_CATEGORICAL: _scalar(_validate_categorical, "string", StringType(), "STRING", "STRING"),
    KIND_INTEGER: _scalar(_validate_numeric, "long", LongType(), "BIGINT", "BIGINT"),
    KIND_DECIMAL: _scalar(_validate_numeric, "double", DoubleType(), "DOUBLE", "DOUBLE"),
    KIND_BOOLEAN: _scalar(_validate_boolean, "boolean", BooleanType(), "BOOLEAN", "BOOLEAN"),
    # Iceberg timestamps use microsecond precision; widen millisecond values.
    # Flink's TIMESTAMP(3) matches the corpus precision, and avro-confluent's
    # legacy timestamp mapping cannot plan higher precision.
    KIND_TIMESTAMP: _scalar(
        _validate_timestamp,
        {"type": "long", "logicalType": "timestamp-millis"},
        TimestampType(),
        "TIMESTAMP(3)",
        "TIMESTAMP_NTZ",
        _iso_millis,
    ),
    KIND_BLOB: _scalar(_validate_blob, "bytes", BinaryType(), "BYTES", "BINARY", _base64),
    KIND_DATE: _scalar(
        _validate_numeric, {"type": "int", "logicalType": "date"}, DateType(), "DATE", "DATE", _iso_date
    ),
    KIND_INT32: _scalar(_validate_numeric, "int", IntegerType(), "INT", "INT"),
    KIND_FLOAT32: _scalar(_validate_numeric, "float", FloatType(), "FLOAT", "FLOAT"),
    # Iceberg's Flink sink writes uuid as BINARY(16), and Flink SQL cannot turn
    # the corpus's uuid strings into those bytes. Spark reads and writes uuid as STRING.
    KIND_UUID: _scalar(_validate_uuid, {"type": "string", "logicalType": "uuid"}, UUIDType(), None, "STRING"),
    KIND_EXACT_DECIMAL: Kind(
        validate=_validate_exact_decimal,
        avro=lambda column, namespace: {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": column.precision,
            "scale": column.scale,
        },
        iceberg=lambda column, ids: DecimalType(column.precision, column.scale),
        flink=_decimal_type,
        spark=_decimal_type,
        json=lambda column: _fixed_point,
    ),
    KIND_STRUCT: Kind(_validate_struct, _struct_avro, _struct_iceberg, _struct_flink, _struct_spark, _struct_json),
    KIND_LIST: Kind(_validate_list, _list_avro, _list_iceberg, _list_flink, _list_spark, _list_json),
    KIND_MAP: Kind(_validate_map, _map_avro, _map_iceberg, _map_flink, _map_spark, _map_json),
}


def kind_of(column: ColumnDistribution) -> Kind:
    return KINDS[RESERVED_KINDS[column.name] if column.kind == KIND_RESERVED else column.kind]


def is_nested(column: ColumnDistribution) -> bool:
    return column.kind in NESTED_KINDS


def has_required_collection_member(column: ColumnDistribution) -> bool:
    """Find a required list element or map value at any depth, which Spark SQL types cannot declare."""
    member = column.element if column.kind == KIND_LIST else column.value if column.kind == KIND_MAP else None
    if member is not None and not member.nullable:
        return True
    return any(has_required_collection_member(child) for child in children(column))


def avro_type(column: ColumnDistribution, namespace: str) -> object:
    """Return the column's Avro type, a union led by null when the column is nullable."""
    declared = kind_of(column).avro(column, namespace)
    return ["null", declared] if column.nullable else declared


def iceberg_type(column: ColumnDistribution, ids: Iterator[int]) -> IcebergType:
    return kind_of(column).iceberg(column, ids)


def _requirement(required: bool) -> str:
    return "required" if required else "optional"


def iceberg_text(field_type: IcebergType) -> str:
    """Render an Iceberg type without field IDs, so tables that number fields differently compare equal."""
    if isinstance(field_type, StructType):
        members = ", ".join(
            f"{field.name}: {_requirement(field.required)} {iceberg_text(field.field_type)}"
            for field in field_type.fields
        )
        return f"struct<{members}>"
    if isinstance(field_type, ListType):
        return f"list<{_requirement(field_type.element_required)} {iceberg_text(field_type.element_type)}>"
    if isinstance(field_type, MapType):
        value = f"{_requirement(field_type.value_required)} {iceberg_text(field_type.value_type)}"
        return f"map<{iceberg_text(field_type.key_type)}, {value}>"
    return str(field_type)


def iceberg_type_text(column: ColumnDistribution) -> str:
    """Return the Iceberg type a corpus publishes for a column."""
    return iceberg_text(iceberg_type(column, itertools.count(1)))


def flink_supported(column: ColumnDistribution) -> bool:
    return kind_of(column).flink is not None and all(flink_supported(child) for child in children(column))


def flink_type(column: ColumnDistribution) -> str:
    """Return the Flink SQL type, NOT NULL unless the column is nullable."""
    render = kind_of(column).flink
    if render is None:
        raise ValueError(f"column {column.name} is {column.kind}, which Flink cannot write to Iceberg")
    return render(column) + _not_null(column)


def spark_type(column: ColumnDistribution) -> str:
    """Return the Spark SQL type. Struct fields carry NOT NULL, which list elements and map values cannot."""
    return kind_of(column).spark(column)


def json_converter(column: ColumnDistribution) -> JsonValue | None:
    """Return the conversion to the column's JSON representation, or None where values serialize as is."""
    convert = kind_of(column).json(column)
    if convert is None or not column.nullable:
        return convert
    return lambda value: None if value is None else convert(value)


# ---------------------------------------------------------------------------
# Validating a schema
# ---------------------------------------------------------------------------


def validate_value_space(column: ColumnDistribution) -> None:
    """Reject distributions that the column kind cannot realize.

    Check ranges, cardinality, skew, and unsupported options before sampling;
    realized-cardinality gates skip small expectations and cannot replace this
    validation.
    """
    bounded = column.cardinality != UNBOUNDED_CARDINALITY
    if column.alpha > 0.0:
        if not bounded:
            raise ValueError(
                f"column {column.name} declares alpha on an unbounded column, whose values come from the row identity "
                "rather than from ranks"
            )
        if column.cardinality > COLUMN_ZIPF_MAX_CARDINALITY:
            raise ValueError(
                f"column {column.name} declares alpha over cardinality {column.cardinality}, above the "
                f"{COLUMN_ZIPF_MAX_CARDINALITY} a rank CDF may be materialized for"
            )
    if column.kind != KIND_EXACT_DECIMAL and (column.precision or column.scale):
        raise ValueError(f"column {column.name} is {column.kind}, so it has no precision or scale to declare")
    if not is_nested(column) and children(column):
        raise ValueError(f"column {column.name} is {column.kind}, so it nests no fields, element, key or value")
    KINDS[column.kind].validate(column)


def validate_column(column: ColumnDistribution) -> None:
    """Validate one generated column, top-level or nested, apart from its schema's roles."""
    if not column.name.isidentifier():
        raise ValueError(f"column name {column.name!r} is not a valid Avro/SQL identifier")
    if column.kind not in KINDS:
        raise ValueError(f"column {column.name} has unknown kind {column.kind!r}")
    if column.role not in ROLES:
        raise ValueError(f"column {column.name} has unknown role {column.role!r}")
    if column.cardinality < 0:
        raise ValueError(f"column {column.name} has a negative cardinality")
    if column.alpha < 0:
        raise ValueError(f"column {column.name} has a negative alpha")
    if not 0.0 <= column.null_fraction < 1.0:
        raise ValueError(f"column {column.name} needs a null_fraction of at least 0 and below 1")
    # Generation, calibration and queries read role columns on every row.
    if column.nullable and column.role != ROLE_GENERIC:
        raise ValueError(f"column {column.name} holds role {column.role}, so it cannot be nullable")
    # Timestamp generation depends on the batch arrival window.
    if column.kind == KIND_TIMESTAMP and column.role != ROLE_EVENT_TIME:
        raise ValueError(f"column {column.name} is a timestamp, which only the {ROLE_EVENT_TIME} column may be")
    validate_value_space(column)


def validate_columns(columns: tuple[ColumnDistribution, ...]) -> None:
    reserved_names = list(RESERVED_KINDS)
    if [column.name for column in columns[: len(reserved_names)]] != reserved_names:
        raise ValueError("the reserved columns must lead the schema, in reserved order")
    generated = columns[len(reserved_names) :]
    if not generated:
        raise ValueError("a schema needs at least one generated column")
    names = [column.name for column in generated]
    if len(set(names)) != len(names):
        raise ValueError("column names must be unique")
    for role, kinds in SINGLETON_ROLES.items():
        holders = [column for column in generated if column.role == role]
        if len(holders) != 1:
            raise ValueError(f"a schema needs exactly one {role} column, found {len(holders)}")
        if holders[0].kind not in kinds:
            raise ValueError(f"column {holders[0].name} cannot hold role {role} with kind {holders[0].kind!r}")
    for column in generated:
        if column.name in RESERVED_FIELDS or column.kind == KIND_RESERVED:
            raise ValueError(f"column {column.name} collides with a reserved column")
        validate_column(column)


RESERVED_COLUMNS: tuple[ColumnDistribution, ...] = tuple(reserved_column(name) for name in RESERVED_KINDS)


def build_columns(configured: tuple[ColumnDistribution, ...]) -> tuple[ColumnDistribution, ...]:
    columns = RESERVED_COLUMNS + configured
    validate_columns(columns)
    return columns


def role_column(columns: tuple[ColumnDistribution, ...], role: str) -> ColumnDistribution:
    return next(column for column in columns if column.role == role)


AVRO_NAMESPACE = "ingest_bench"
AVRO_RECORD = "event"


def schema_fields(columns: tuple[ColumnDistribution, ...]) -> list[SchemaField]:
    namespace = f"{AVRO_NAMESPACE}.{AVRO_RECORD}"
    return [{"name": column.name, "type": avro_type(column, namespace)} for column in columns]


def avro_schema(columns: tuple[ColumnDistribution, ...]) -> dict[str, object]:
    return {
        "type": "record",
        "name": AVRO_RECORD,
        "namespace": AVRO_NAMESPACE,
        "fields": schema_fields(columns),
    }


# ---------------------------------------------------------------------------
# Declaring a schema
# ---------------------------------------------------------------------------

COLUMN_DECLARATION_KEYS = frozenset(
    {
        "name",
        "kind",
        "role",
        "cardinality",
        "alpha",
        "vocabulary",
        "minimum",
        "maximum",
        "width",
        "null_fraction",
        "precision",
        "scale",
        "fields",
        "element",
        "key",
        "value",
    }
)


def as_int_value(value: object) -> int:
    return int(cast(int | float | str, value))


def as_float_value(value: object) -> float:
    return float(cast(int | float | str, value))


def member_from_dict(parent: str, member: str, raw: object) -> ColumnDistribution:
    """Read a list element or map key or value, which is named after its position."""
    declared = cast(dict[str, object], raw)
    if "name" in declared:
        raise ValueError(f"column {parent} names its {member}, which takes the name {member!r} from its position")
    return column_from_dict({"name": member, **declared})


def apply_column_override(column: ColumnDistribution, raw: dict[str, object]) -> ColumnDistribution:
    updated = column
    for key, value in raw.items():
        match key:
            case "kind":
                updated = replace(updated, kind=str(value))
            case "role":
                updated = replace(updated, role=str(value))
            case "cardinality":
                updated = replace(updated, cardinality=as_int_value(value))
            case "alpha":
                updated = replace(updated, alpha=as_float_value(value))
            case "vocabulary":
                updated = replace(updated, vocabulary=tuple(str(item) for item in cast(list[object], value)))
            case "minimum":
                updated = replace(updated, minimum=as_float_value(value))
            case "maximum":
                updated = replace(updated, maximum=as_float_value(value))
            case "width":
                updated = replace(updated, width=as_int_value(value))
            case "null_fraction":
                updated = replace(updated, null_fraction=as_float_value(value))
            case "precision":
                updated = replace(updated, precision=as_int_value(value))
            case "scale":
                updated = replace(updated, scale=as_int_value(value))
            case "fields":
                entries = cast(list[object], value)
                updated = replace(
                    updated, fields=tuple(column_from_dict(cast(dict[str, object], entry)) for entry in entries)
                )
            case "element":
                updated = replace(updated, element=member_from_dict(column.name, key, value))
            case "key":
                updated = replace(updated, key=member_from_dict(column.name, key, value))
            case "value":
                updated = replace(updated, value=member_from_dict(column.name, key, value))
            case _:
                raise ValueError(f"unknown column override key {key!r} for column {column.name}")
    return updated


def column_from_dict(raw: dict[str, object]) -> ColumnDistribution:
    """Read required ``name`` and ``kind`` fields and optional distribution settings.

    Reject unknown keys and use dataclass defaults for omitted settings.
    """
    name = str(raw["name"])
    unknown = sorted(set(raw) - COLUMN_DECLARATION_KEYS)
    if unknown:
        raise ValueError(f"column {name} declares unknown key(s): {', '.join(unknown)}")
    base = ColumnDistribution(name=name, kind=str(raw["kind"]))
    return apply_column_override(base, {key: value for key, value in raw.items() if key not in {"name", "kind"}})


def declaration(column: ColumnDistribution) -> dict[str, object]:
    """Return the declaration ``column_from_dict`` rebuilds ``column`` from.

    Attributes added after corpora were published appear only when they differ
    from their defaults, so the corpus hash of an existing preset is unchanged.
    """
    declared: dict[str, object] = {
        "name": column.name,
        "kind": column.kind,
        "role": column.role,
        "cardinality": column.cardinality,
        "alpha": column.alpha,
        "vocabulary": list(column.vocabulary),
        "minimum": column.minimum,
        "maximum": column.maximum,
        "width": column.width,
    }
    if column.null_fraction:
        declared["null_fraction"] = column.null_fraction
    if column.precision:
        declared["precision"] = column.precision
    if column.scale:
        declared["scale"] = column.scale
    if column.fields:
        declared["fields"] = [declaration(field) for field in column.fields]
    for member, child in (("element", column.element), ("key", column.key), ("value", column.value)):
        if child is not None:
            declared[member] = {name: entry for name, entry in declaration(child).items() if name != "name"}
    return declared


def load_schema(path: Path) -> tuple[ColumnDistribution, ...]:
    """A schema file is `{"name": str, "columns": [ColumnDistribution as dict, ...]}`; reserved columns are implicit."""
    raw = json.loads(path.read_text())
    declared = tuple(column_from_dict(cast(dict[str, object], entry)) for entry in cast(list[object], raw["columns"]))
    return build_columns(declared)


def apply_column_overrides(
    columns: tuple[ColumnDistribution, ...], overrides: dict[str, dict[str, object]]
) -> tuple[ColumnDistribution, ...]:
    by_name = {column.name: column for column in columns}
    unknown = sorted(set(overrides) - set(by_name))
    if unknown:
        raise ValueError(f"column_overrides name unknown column(s): {', '.join(unknown)}")
    reserved = [name for name in overrides if name in RESERVED_FIELDS]
    if reserved:
        raise ValueError(f"column_overrides may not touch reserved column(s): {', '.join(reserved)}")
    patched = tuple(
        apply_column_override(column, overrides[column.name]) if column.name in overrides else column
        for column in columns
    )
    validate_columns(patched)
    return patched
