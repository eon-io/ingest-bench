# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Column kinds beyond the events schema: new scalars, nullability and nesting."""

import base64
import decimal
import io
import json
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from typing import cast

import fastavro
import numpy as np
import pytest
from pyiceberg.schema import Schema
from pyiceberg.types import IcebergType, ListType, MapType, NestedField, StringType, StructType

from engines.flink import knobs as flink
from engines.spark import knobs as spark
from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import generate, json_values, metadata, preset
from ingest_bench.corpus import values as v
from ingest_bench.scorer import snapshots
from ingest_bench.specs import derive, model
from ingest_bench.table import create, ddl

ROOT = Path(__file__).resolve().parents[1]
WORKLOADS = ROOT / "workloads"
COLUMNS = c.load_schema(WORKLOADS / "schemas" / "kinds.json")
BY_NAME = {column.name: column for column in COLUMNS}
EPOCH_MS = 1_767_225_600_000


def _block(start: int = 0, rows: int = 256, columns: tuple[c.ColumnDistribution, ...] = COLUMNS) -> v.RowBlock:
    keys = np.arange(start, start + rows, dtype=np.int64) % 4
    return v.build_row_block(5, 2, start, keys, 32, EPOCH_MS, 1000, columns)


def _small(name: str) -> preset.Preset:
    return preset.load_preset(
        name,
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=100KB", "duration_s=2", "partition_count=4"],
    )


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = _small("kinds")
    out = str(tmp_path_factory.mktemp("kinds"))
    generate.generate(p, out, seed=3, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p, 3, row_block=64)))


def _kinds(columns: tuple[c.ColumnDistribution, ...]) -> set[str]:
    return {kind for column in columns for kind in {column.kind, *_kinds(c.children(column))}}


def _field_ids(field_type: IcebergType) -> list[int]:
    if isinstance(field_type, StructType):
        return [field_id for field in field_type.fields for field_id in (field.field_id, *_field_ids(field.field_type))]
    if isinstance(field_type, ListType):
        return [field_type.element_id, *_field_ids(field_type.element_type)]
    if isinstance(field_type, MapType):
        return [field_type.key_id, field_type.value_id, *_field_ids(field_type.value_type)]
    return []


def _with(raw: dict[str, object]) -> tuple[c.ColumnDistribution, ...]:
    return c.build_columns((*COLUMNS[2:], c.column_from_dict(raw)))


@pytest.mark.parametrize(
    "raw, message",
    [
        ({"name": "x", "kind": "exact_decimal", "minimum": 0, "maximum": 9}, "needs a precision"),
        ({"name": "x", "kind": "exact_decimal", "precision": 4, "scale": 5, "maximum": 1}, "needs a scale"),
        ({"name": "x", "kind": "exact_decimal", "precision": 4, "scale": 2, "maximum": 100}, "cannot hold"),
        ({"name": "x", "kind": "int32", "minimum": 0, "maximum": 2**31}, "fit in 32 bits"),
        ({"name": "x", "kind": "integer", "precision": 4, "maximum": 9}, "no precision or scale"),
        ({"name": "x", "kind": "uuid", "width": 16}, "is a uuid"),
        ({"name": "x", "kind": "boolean", "cardinality": 2, "null_fraction": 1.0}, "null_fraction"),
        ({"name": "x", "kind": "boolean", "cardinality": 2, "fields": [{"name": "y", "kind": "uuid"}]}, "nests no"),
        ({"name": "x", "kind": "struct"}, "one or more fields"),
        ({"name": "x", "kind": "struct", "fields": [{"name": "y", "kind": "uuid"}] * 2}, "duplicate"),
        ({"name": "x", "kind": "struct", "fields": [{"name": "y", "kind": "timestamp"}]}, "only the event_time"),
        (
            {"name": "x", "kind": "struct", "fields": [{"name": "y", "kind": "uuid", "role": "sum_measure"}]},
            "roles name top-level columns",
        ),
        ({"name": "x", "kind": "list", "maximum": 2}, "one element"),
        ({"name": "x", "kind": "list", "minimum": 3, "maximum": 2, "element": {"kind": "uuid"}}, "whole numbers"),
        ({"name": "x", "kind": "list", "maximum": 2, "cardinality": 3, "element": {"kind": "uuid"}}, "no cardinality"),
        (
            {"name": "x", "kind": "list", "maximum": 2, "element": {"name": "e", "kind": "uuid"}},
            "takes the name 'element'",
        ),
        (
            {
                "name": "x",
                "kind": "map",
                "maximum": 3,
                "key": {"kind": "categorical", "cardinality": 2, "vocabulary": ["k"]},
                "value": {"kind": "uuid"},
            },
            "bounded categorical key",
        ),
        (
            {"name": "x", "kind": "map", "maximum": 1, "key": {"kind": "uuid"}, "value": {"kind": "uuid"}},
            "bounded categorical key",
        ),
        (
            {
                "name": "x",
                "kind": "map",
                "minimum": 3,
                "maximum": 3,
                "key": {"kind": "categorical", "cardinality": 3, "vocabulary": ["k", "k-00001"]},
                "value": {"kind": "uuid"},
            },
            "share labels",
        ),
        (
            {
                "name": "x",
                "kind": "map",
                "maximum": 2,
                "key": {"kind": "categorical", "cardinality": 2, "vocabulary": ["k", "k"]},
                "value": {"kind": "uuid"},
            },
            "share labels",
        ),
        ({"name": "x", "kind": "float32", "cardinality": 100, "minimum": 1e8, "maximum": 1e8 + 1}, "hundredths"),
        ({"name": "x", "kind": "date", "cardinality": 1, "minimum": 3_000_000, "maximum": 3_000_000}, "9999-12-31"),
    ],
)
def test_the_loader_rejects_what_a_kind_cannot_realize(raw: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        _with(raw)


def test_label_collisions_are_found_without_enumerating_ranks(monkeypatch: pytest.MonkeyPatch) -> None:
    def label(column: c.ColumnDistribution, rank: int) -> str:
        raise AssertionError("validation enumerated a rank")

    monkeypatch.setattr(c, "categorical_label", label)

    def key(cardinality: int, vocabulary: list[str]) -> c.ColumnDistribution:
        return c.column_from_dict(
            {"name": "k", "kind": "categorical", "cardinality": cardinality, "vocabulary": vocabulary}
        )

    assert not c.labels_collide(key(1_000_000_000, ["k"]))
    # Rank 4 of two entries is entry 0 with quotient 2, which is "k-00002" again.
    assert c.labels_collide(key(5, ["k", "k-00002"]))
    assert not c.labels_collide(key(4, ["k", "k-00002"]))
    assert not c.labels_collide(key(1, ["a", "a"])), "the repeated entry is never drawn"
    assert not c.labels_collide(key(9, ["k", "k-2", "k-000001"])), "only zero-padded suffixes of used ranks collide"
    _with(
        {
            "name": "x",
            "kind": "map",
            "maximum": 3,
            "key": {"kind": "categorical", "cardinality": 1_000_000_000, "vocabulary": ["k"]},
            "value": {"kind": "uuid"},
        }
    )


def test_role_columns_reserved_columns_and_key_columns_stay_required() -> None:
    events = c.load_schema(WORKLOADS / "schemas" / "events.json")
    with pytest.raises(ValueError, match="cannot be nullable"):
        c.apply_column_overrides(events, {"event_id": {"null_fraction": 0.1}})
    with pytest.raises(ValueError, match="reserved"):
        c.apply_column_overrides(events, {"id": {"null_fraction": 0.1}})
    with pytest.raises(ValueError, match="must be required columns"):
        preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.user_id.null_fraction=0.1"])


def test_declarations_round_trip_and_new_attributes_hide_at_their_defaults() -> None:
    for column in COLUMNS[2:]:
        assert c.column_from_dict(c.declaration(column)) == column
    assert set(c.declaration(BY_NAME["event_id"])) == {
        "name",
        "kind",
        "role",
        "cardinality",
        "alpha",
        "vocabulary",
        "minimum",
        "maximum",
        "width",
    }
    effective = preset.effective_dict(_small("kinds"))
    assert preset.canonical_json(preset.effective_dict(preset.preset_from_effective(effective, "kinds"))) == (
        preset.canonical_json(effective)
    )


def test_every_kind_types_itself_in_every_system() -> None:
    assert set(c.KINDS) == {
        "categorical",
        "integer",
        "decimal",
        "boolean",
        "timestamp",
        "blob",
        "date",
        "int32",
        "float32",
        "exact_decimal",
        "uuid",
        "struct",
        "list",
        "map",
    }
    assert _kinds(COLUMNS[2:]) == set(c.KINDS), "the kinds schema exercises every kind"
    device = BY_NAME["device"]
    assert c.iceberg_type_text(device) == (
        "struct<os: required string, screen_width: required int, model: optional string, fingerprint: required binary, "
        "settings: optional struct<theme: optional string, volume: optional int>>"
    )
    assert c.flink_type(device) == (
        "ROW<`os` STRING NOT NULL, `screen_width` INT NOT NULL, `model` STRING, `fingerprint` BYTES NOT NULL, "
        "`settings` ROW<`theme` STRING, `volume` INT>> NOT NULL"
    )
    location = BY_NAME["location"]
    assert c.iceberg_type_text(location) == "struct<latitude: required float, longitude: required double>"
    assert c.flink_type(location) == "ROW<`latitude` FLOAT NOT NULL, `longitude` DOUBLE NOT NULL>"
    assert c.spark_type(location) == "STRUCT<`latitude`: FLOAT NOT NULL, `longitude`: DOUBLE NOT NULL>"
    scores = BY_NAME["scores"]
    assert c.iceberg_type_text(scores) == "map<string, required list<optional float>>"
    assert c.flink_type(scores) == "MAP<STRING NOT NULL, ARRAY<FLOAT> NOT NULL> NOT NULL"
    assert c.spark_type(scores) == "MAP<STRING, ARRAY<FLOAT>>"
    assert c.avro_type(BY_NAME["balance"], "ns") == [
        "null",
        {"type": "bytes", "logicalType": "decimal", "precision": 38, "scale": 6},
    ]
    assert c.avro_type(BY_NAME["event_date"], "ns") == {"type": "int", "logicalType": "date"}
    assert c.avro_type(BY_NAME["session_id"], "ns") == {"type": "string", "logicalType": "uuid"}
    assert c.iceberg_type_text(BY_NAME["price"]) == "decimal(10, 2)" and c.flink_type(BY_NAME["rating"]) == "FLOAT"
    assert not c.flink_supported(BY_NAME["session_id"])
    with pytest.raises(ValueError, match="Flink cannot write"):
        c.flink_type(BY_NAME["session_id"])


def test_nested_avro_records_have_unique_names() -> None:
    fastavro.parse_schema(c.avro_schema(COLUMNS))
    device = cast(dict[str, object], c.avro_type(BY_NAME["device"], "ingest_bench.event"))
    settings = cast(list[dict[str, object]], device["fields"])[-1]["type"]
    assert cast(list[dict[str, object]], settings)[1]["namespace"] == "ingest_bench.event.device"


def test_generation_is_deterministic_by_row_position() -> None:
    whole = _block(rows=256)
    assert whole.avro_records_bytes() == _block(rows=256).avro_records_bytes()
    tail = _block(start=128, rows=128)
    assert whole.avro_records()[128:] == tail.avro_records()
    assert whole.avro_records_bytes()[int(whole.encoded_sizes[:128].sum()) :] == tail.avro_records_bytes()


def test_records_decode_under_the_published_schema_as_generated() -> None:
    block = _block(rows=512)
    schema = fastavro.parse_schema(c.avro_schema(COLUMNS))
    data = block.avro_records_bytes()
    offsets = np.concatenate([[0], np.cumsum(block.encoded_sizes)])
    for index, row in enumerate(block.avro_records()):
        expected = io.BytesIO()
        fastavro.schemaless_writer(expected, schema, row)
        expected.seek(0)
        ours = io.BytesIO(data[offsets[index] : offsets[index + 1]])
        assert fastavro.schemaless_reader(ours, schema) == fastavro.schemaless_reader(expected, schema)
        assert ours.read() == b""


def test_values_take_their_declared_shapes() -> None:
    block = _block(rows=4096)
    values = block.values
    for name in ("rating", "balance", "referrer", "location", "digest", "is_visible", "line_items"):
        share = sum(cell is None for cell in values[name]) / block.rows
        assert abs(share - BY_NAME[name].null_fraction) < 0.03, name
    assert all(20089 <= cast(int, day) <= 20453 for day in values["event_date"])
    prices = cast(list[Decimal], values["price"])
    assert all(price.as_tuple().exponent == -2 and Decimal("0.01") <= price <= Decimal("9999.99") for price in prices)
    balances = [cast(Decimal, cell) for cell in values["balance"] if cell is not None]
    assert all(balance.as_tuple().exponent == -6 for balance in balances) and len(set(balances)) == len(balances)
    assert len(set(values["session_id"])) == block.rows
    assert all(str(cell)[14] == "4" for cell in values["session_id"])
    for tags in cast(list[list[str]], values["tags"]):
        assert 0 <= len(tags) <= 4
    for attributes in cast(list[dict[str, object]], values["attributes"]):
        assert len(attributes) <= 3
    assert {len(cast(dict[str, object], scores)) for scores in values["scores"]} == {1, 2}
    settings = [cast(dict[str, object], device)["settings"] for device in values["device"]]
    assert 0.15 < sum(entry is None for entry in settings) / block.rows < 0.25


def test_exact_decimals_keep_every_digit_whatever_the_decimal_context() -> None:
    wide = c.column_from_dict(
        {"name": "x", "kind": "exact_decimal", "precision": 38, "scale": 6, "minimum": -1e30, "maximum": 1e30}
    )
    columns = c.build_columns((*COLUMNS[2:], wide))
    block = _block(columns=columns)
    values = cast(list[Decimal], block.values["x"])
    assert all(value.as_tuple().exponent == -6 for value in values)
    assert max(len(value.as_tuple().digits) for value in values) > 28, "the default context would round these"
    with decimal.localcontext(decimal.Context(prec=5)):
        assert _block(columns=columns).avro_records_bytes() == block.avro_records_bytes()


def test_a_nullable_column_keeps_the_values_of_its_required_form() -> None:
    required = replace(BY_NAME["rating"], null_fraction=0.0)
    columns = tuple(required if column.name == "rating" else column for column in COLUMNS)
    nullable, plain = _block(rows=512).values["rating"], _block(rows=512, columns=columns).values["rating"]
    assert [cell for cell in nullable if cell is not None] == [
        cell for cell, held in zip(plain, nullable, strict=True) if held is not None
    ]


def test_json_follows_each_kind_representation() -> None:
    block = _block(rows=64)
    rows = [json.loads(record) for record in json_values.encode_records(block.avro_records(), COLUMNS)]
    for row, source in zip(rows, block.avro_records(), strict=True):
        assert row["event_date"] == str(np.datetime64(cast(int, source["event_date"]), "D"))
        assert row["price"] == f"{cast(Decimal, source['price']):f}" and "." in row["price"]
        assert row["session_id"] == source["session_id"]
        device = cast(dict[str, object], source["device"])
        assert base64.b64decode(row["device"]["fingerprint"]) == device["fingerprint"]
        assert row["balance"] is None if source["balance"] is None else Decimal(row["balance"]) == source["balance"]
        items = cast(list[dict[str, object]], source["line_items"] or [])
        for item, generated in zip(row["line_items"] or [], items, strict=True):
            assert Decimal(item["unit_price"]) == generated["unit_price"]
            assert (item["shipped"] is None) == (generated["shipped"] is None)
        assert row["scores"] == source["scores"]


def test_calibration_measures_nested_columns() -> None:
    for encoding, target in (("avro", 512), ("json", 1024)):
        width = v.calibrate_payload_width(5, target, COLUMNS, encoding)
        assert abs(v.realized_encoded_row_size(5, width, COLUMNS, encoding) - target) / target < 0.02


def test_the_corpus_publishes_types_and_skips_nested_statistics(corpus: metadata.CorpusMetadata) -> None:
    raw = json.loads(uri.read_text(uri.join(corpus.uri, "corpus.json")))
    assert raw["iceberg_types"]["scores"] == "map<string, required list<optional float>>"
    assert raw["iceberg_types"]["session_id"] == "uuid"
    assert raw["optional_columns"] == [
        "rating",
        "balance",
        "referrer",
        "location",
        "line_items",
        "is_visible",
        "digest",
    ]
    stats = raw["column_stats"]
    assert "device" not in stats and "tags" not in stats and "quantity" in stats
    assert stats["referrer"]["sampled_rows"] < stats["quantity"]["sampled_rows"]
    assert corpus.columns == _small("kinds").columns


def test_the_table_and_scorer_agree_with_the_corpus(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/c.db", "warehouse": f"file://{tmp_path}/wh"}
    table = create.create_table(props, "bench.kinds", corpus, create.parse_partition("identity(partition_key)"), {})
    schema = table.schema()
    assert [field.field_id for field in schema.fields] == list(range(1, len(COLUMNS) + 1))
    ids = _field_ids(schema.as_struct())
    assert len(ids) == len(set(ids)) > len(COLUMNS)
    assert snapshots.check_table_schema(schema, corpus) == []
    optional = [field.name for field in schema.fields if not field.required]
    assert optional == [column.name for column in corpus.columns if column.nullable]
    with pytest.raises(ValueError, match="nested"):
        create.partition_spec(corpus, create.parse_partition("bucket(4, device)"))

    fields = list(create.iceberg_schema(corpus).fields)
    retyped = Schema(
        *(
            NestedField(f.field_id, f.name, ListType(900, StringType(), element_required=False), required=True)
            if f.name == "tags"
            else NestedField(f.field_id, f.name, f.field_type, required=True)
            if f.name == "rating"
            else f
            for f in fields
        )
    )
    assert snapshots.check_table_schema(retyped, corpus) == [
        "column 'rating' is required in the table and optional in the corpus",
        "column 'tags' is list<optional string> in the table and list<required string> in the corpus",
    ]


def _site() -> model.SiteConfig:
    props = {"uri": "http://iceberg-rest:8181", "warehouse": "s3://warehouse/", "s3.region": "us-east-1"}
    return model.SiteConfig("s3://corpus", "s3://runs", "s3://warehouse", "kafka:9092", {}, None, props, None, 0.0, 0.0)


def test_flink_refuses_what_it_cannot_read_and_declares_the_rest(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    spec = model.load_run_spec(ROOT / "specs" / "smoke" / "smoke-flink-kinds.yaml")
    with pytest.raises(ValueError, match="session_id hold uuid values"):
        flink.validate(spec.engine_block, spec, corpus)
    token = {"kind": "categorical", "width": 36}
    tokens = tuple(c.apply_column_override(col, token) if col.name == "session_id" else col for col in corpus.columns)
    with pytest.raises(ValueError, match="location nest required fields in a nullable struct"):
        flink.validate(spec.engine_block, spec, replace(corpus, columns=tokens))
    json_spec = replace(spec, kafka=replace(spec.kafka, value_encoding="json"))
    flink.validate(json_spec.engine_block, json_spec, replace(corpus, columns=tokens))
    p = _small("kinds-flink")
    generate.generate(p, str(tmp_path), seed=3, row_block=64)
    readable = metadata.read(uri.join(str(tmp_path), preset.corpus_dir_name(p, 3, row_block=64)))
    flink.validate(spec.engine_block, spec, readable)
    derived = derive.derive(spec, _site(), stamp="20260925T000000Z", corpus_dir=readable.name + "-x")
    sql = flink.render_sql(spec, _site(), derived, readable)
    assert "  `rating` FLOAT,\n" in sql and "  `session_id` STRING NOT NULL,\n" in sql
    assert "  `attributes` MAP<STRING NOT NULL, BIGINT> NOT NULL,\n" in sql


def test_spark_reads_json_with_the_registry_schema_and_declares_what_it_can(
    corpus: metadata.CorpusMetadata,
) -> None:
    schema = spark.render_json_schema(corpus)
    site = _site()
    spec = model.load_run_spec(ROOT / "specs" / "smoke" / "smoke-spark-kinds-json.yaml")
    derived = derive.derive(spec, site, stamp="20260925T000000Z", corpus_dir=corpus.name + "-x")
    assert json.loads(spark.render_job(spec, site, derived, corpus))["json_via_avro"] is True
    assert "`session_id` STRING" in schema and "`balance` DECIMAL(38, 6)" in schema
    assert "`event_date` DATE" in schema and "`scores` MAP<STRING, ARRAY<FLOAT>>" in schema
    partition = create.parse_partition("identity(partition_key)")
    with pytest.raises(ValueError, match="session_id.*uuid"):
        ddl.spark_sql_ddl(corpus, "bench.t", partition, {})
    without_uuid = replace(corpus, columns=tuple(col for col in corpus.columns if col.kind != c.KIND_UUID))
    with pytest.raises(ValueError, match="required list element or map value"):
        ddl.spark_sql_ddl(without_uuid, "bench.t", partition, {})
    nullable = tuple(
        replace(col, element=replace(cast(c.ColumnDistribution, col.element), null_fraction=0.5))
        if col.kind == c.KIND_LIST
        else replace(col, value=replace(cast(c.ColumnDistribution, col.value), null_fraction=0.5))
        if col.kind == c.KIND_MAP
        else col
        for col in without_uuid.columns
    )
    text = ddl.spark_sql_ddl(replace(without_uuid, columns=nullable), "bench.t", partition, {})
    assert "  rating FLOAT,\n" in text and "  tags ARRAY<STRING> NOT NULL,\n" in text


def test_a_numeric_suffix_too_long_to_be_a_rank_is_not_parsed() -> None:
    """Python refuses to convert very long digit strings, and no reachable rank has one."""
    column = c.ColumnDistribution(name="k", kind=c.KIND_CATEGORICAL, cardinality=3, vocabulary=("k", "k-" + "9" * 4301))
    assert not c.labels_collide(column)


def test_a_map_key_without_a_vocabulary_gets_the_vocabulary_rejection() -> None:
    key = c.ColumnDistribution(name="key", kind=c.KIND_CATEGORICAL, cardinality=3)
    assert not c.labels_collide(key)
