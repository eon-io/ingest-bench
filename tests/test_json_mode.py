# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import base64
import io
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast

import pytest

from engines.flink import knobs as flink
from engines.spark import knobs as spark
from engines.spark import stream_to_iceberg
from ingest_bench import uri
from ingest_bench.corpus import frames, generate, merge, metadata, preset
from ingest_bench.producer import cli, produce, publish_log
from ingest_bench.specs import derive, model
from tests.test_producer import FakeClock, FakeProducer

ROOT = Path(__file__).resolve().parents[1]


def json_preset() -> preset.Preset:
    return preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=[
            "value_encoding=json",
            "offered_bytes_per_s=100KB",
            "duration_s=2",
            "partition_count=4",
            "target_row_bytes=1024",
        ],
    )


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = json_preset()
    out = str(tmp_path_factory.mktemp("json-corpus"))
    generate.generate(p, out, seed=3, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p, 3, row_block=64)))


def test_json_corpus_bytes_truth_and_logical_types(corpus: metadata.CorpusMetadata) -> None:
    assert corpus.value_encoding == "json"
    truth = json.loads(uri.read_text(uri.join(corpus.uri, "partition_truth.json")))
    counts: dict[str, int] = {}
    byte_counts: dict[str, int] = {}
    for record in metadata.read_manifest(corpus.uri):
        values = list(frames.iter_frames(frames.decompress(uri.read_bytes(record.uri))))
        rows = [json.loads(value) for value in values]
        assert sum(map(len, values)) == record.encoded_bytes
        assert len(rows) == record.rows
        assert [row["id"] for row in rows] == list(range(record.id_min, record.id_max + 1))
        assert sum(row["id"] for row in rows) % corpus.p == record.checksum
        for name, path in record.key_uris.items():
            keys = list(frames.iter_frames(frames.decompress(uri.read_bytes(path))))
            assert keys == [row[name].encode() for row in rows]
        for row, value in zip(rows, values, strict=True):
            timestamp = datetime.fromisoformat(row["event_time"])
            assert timestamp.tzinfo is None
            assert timestamp.microsecond % 1000 == 0
            assert row["event_time"].startswith("2026-01-01T00:00:")
            assert len(base64.b64decode(row["payload"], validate=True)) > 0
            assert isinstance(row["is_visible"], bool)
            label = row["partition_key"]
            counts[label] = counts.get(label, 0) + 1
            byte_counts[label] = byte_counts.get(label, 0) + len(value)
    assert {key: value["rows"] for key, value in truth.items()} == counts
    assert {key: value["encoded_bytes"] for key, value in truth.items()} == byte_counts


def test_json_identity_and_shard_merge(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    p = json_preset()
    assert preset.corpus_hash(p, 3, row_block=64) != preset.corpus_hash(
        replace(p, value_encoding="avro"), 3, row_block=64
    )
    parts = []
    for index in range(2):
        out = str(tmp_path / str(index))
        generate.generate(p, out, seed=3, row_block=64, shard_index=index, shard_count=2)
        parts.append(uri.join(out, preset.corpus_dir_name(p, 3, row_block=64)))
    out = str(tmp_path / "merged")
    merged = merge.merge(parts, out)
    assert merged["value_encoding"] == "json"
    merged_uri = uri.join(out, preset.corpus_dir_name(p, 3, row_block=64))
    assert [r.sha256 for r in metadata.read_manifest(merged_uri)] == [
        r.sha256 for r in metadata.read_manifest(corpus.uri)
    ]


def test_json_producer_replays_stored_values_and_counts_bytes(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    clock = FakeClock(0)
    producer = FakeProducer(clock)
    args = produce.ProduceArgs(
        corpus_uri=corpus.uri,
        bootstrap="fake:9092",
        topic="test",
        epoch_ms=0,
        speed=1,
        shard=0,
        shards=1,
        seconds=None,
        key_column="user_id",
        value_prefix=b"",
        publish_log_path=tmp_path / "publish.jsonl",
        behind_max_ms=5000,
        upload_prefix=None,
        compression="zstd",
        kafka_props={},
        value_encoding="json",
    )
    assert produce.run(args, lambda config: producer, clock, io.StringIO()) == 0
    expected = [
        value
        for record in metadata.read_manifest(corpus.uri)
        for value in frames.iter_frames(frames.decompress(uri.read_bytes(record.uri)))
    ]
    assert [value for _, value, _ in producer.sent] == expected
    assert sum(entry.bytes for entry in publish_log.read(args.publish_log_path)) == sum(map(len, expected))
    for encoding in ("avro", "confluent"):
        with pytest.raises(ValueError, match="encoding"):
            produce.run(replace(args, value_encoding=encoding), lambda config: producer, clock, io.StringIO())
    with pytest.raises(ValueError, match="prefix"):
        produce.run(replace(args, value_prefix=b"bad"), lambda config: producer, clock, io.StringIO())


def test_json_engine_readers(corpus: metadata.CorpusMetadata) -> None:
    site = model.SiteConfig(
        "s3://corpus",
        "s3://runs",
        "s3://warehouse",
        "kafka:9092",
        {},
        None,
        {"uri": "http://iceberg-rest:8181", "warehouse": "s3://warehouse", "s3.region": "us-east-1"},
        None,
        0.0,
        0.0,
    )
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink.yaml")
    spec = replace(spec, kafka=replace(spec.kafka, value_encoding="json"))
    derived = derive.derive(spec, site, stamp="20260924T000000Z", corpus_dir="json-corpus")
    sql = flink.render_sql(spec, site, derived, corpus)
    assert "'format' = 'json'" in sql
    assert "'json.timestamp-format.standard' = 'ISO-8601'" in sql
    assert "'json.fail-on-missing-field' = 'true'" in sql
    assert "avro." not in sql and "avro-confluent" not in sql
    ddl = spark.render_json_schema(corpus)
    assert "`event_time` TIMESTAMP_NTZ" in ddl
    assert "`payload` BINARY" in ddl
    assert "`id` BIGINT" in ddl
    assert stream_to_iceberg.value_expression("json") == "CAST(value AS STRING)"


def test_json_cli_and_spec(tmp_path: Path) -> None:
    raw = (
        (ROOT / "specs/smoke/smoke-flink.yaml")
        .read_text()
        .replace("  partitions:", "  value_encoding: json\n  partitions:")
    )
    path = tmp_path / "run.yaml"
    path.write_text(raw)
    assert model.load_run_spec(path).kafka.value_encoding == "json"
    parsed = cli.build_parser().parse_args(
        [
            "--corpus",
            "corpus",
            "--bootstrap",
            "kafka:9092",
            "--topic",
            "topic",
            "--epoch",
            "0",
            "--publish-log",
            "publish.jsonl",
            "--value-encoding",
            "json",
        ]
    )
    assert parsed.value_encoding == "json"
    assert cli._value_prefix("json", None) == b""
    with pytest.raises(ValueError, match="schema-id"):
        cli._value_prefix("json", 1)


def test_json_preserves_scalars_and_escapes() -> None:
    from ingest_bench.corpus import json_values

    p = json_preset()
    row: dict[str, object] = {
        "event_time": -1,
        "payload": b"\x00\xff\x80",
        "id": 9223372036854775807,
        "user_id": 'quote" slash\\ newline\n snowman\u2603',
        "is_visible": False,
        "amount": 1.25,
    }
    encoded = json_values.encode_records([row], p.columns)[0]
    assert json.loads(encoded) == {
        **row,
        "event_time": "1969-12-31T23:59:59.999",
        "payload": "AP+A",
    }
    assert b"\n" not in encoded
    with pytest.raises(ValueError, match="JSON compliant"):
        json_values.encode_records([{"amount": float("nan")}], p.columns)


def test_json_stage_checks_encoding_before_creating_resources(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    import yaml

    from ingest_bench import stage
    from tests.test_specs import FakeAdmin, FakeRegistration, _site_file

    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["corpus"] = corpus.name
    spec_path = tmp_path / "run.yaml"
    site_path = _site_file(tmp_path, str(Path(corpus.uri).parent))
    admin = FakeAdmin()
    for encoding in ("avro", "confluent"):
        raw["kafka"]["value_encoding"] = encoding
        spec_path.write_text(yaml.safe_dump(raw))
        with pytest.raises(ValueError, match="encoding"):
            stage.stage(spec_path, site_path, tmp_path / "runs", admin, stamp="20260924T000000Z")
    assert admin.created == [] and admin.clients == []
    registry = FakeRegistration(refuse=True)
    monkeypatch.setattr(stage, "register_schema", registry.register)
    raw["kafka"]["value_encoding"] = "json"
    spec_path.write_text(yaml.safe_dump(raw))
    staged = stage.stage(spec_path, site_path, tmp_path / "runs", admin, stamp="20260924T000000Z")
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["value_encoding"] == "json"
    assert facts["schema_id"] is None and facts["schema_registry_url"] is None
    assert not registry.calls


def test_avro_corpus_rejects_json_replay(tmp_path: Path) -> None:
    p = replace(json_preset(), value_encoding="avro")
    generate.generate(p, str(tmp_path), seed=3, row_block=64)
    corpus_uri = str(tmp_path / preset.corpus_dir_name(p, 3, row_block=64))
    # Legacy metadata has no explicit encoding field.
    path = Path(corpus_uri) / "corpus.json"
    raw = json.loads(path.read_text())
    del raw["value_encoding"]
    path.write_text(json.dumps(raw))
    meta = metadata.read(corpus_uri)
    meta.validate_value_encoding("avro")
    meta.validate_value_encoding("confluent")
    with pytest.raises(ValueError, match="encoding"):
        meta.validate_value_encoding("json")


def test_json_encoding_cannot_be_changed_without_changing_identity(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    raw = json.loads(uri.read_text(uri.join(corpus.uri, "corpus.json")))
    raw["value_encoding"] = "avro"
    (tmp_path / "corpus.json").write_text(json.dumps(raw))
    with pytest.raises(ValueError, match="encoding"):
        metadata.read(str(tmp_path))


def test_json_payload_budget_rejects_a_target_below_nonpayload_size() -> None:
    from ingest_bench.corpus import values

    with pytest.raises(ValueError, match="target_row_bytes"):
        values.calibrate_payload_width(1, 256, json_preset().columns, "json")


@pytest.mark.parametrize("seed,row_block", [(1, 64), (7, 1024)])
def test_avro_sized_json_preserves_every_record_and_batch(tmp_path: Path, seed: int, row_block: int) -> None:
    import fastavro

    from ingest_bench.collect.run_json import corpus_fields
    from ingest_bench.corpus import json_values

    overrides = ["offered_bytes_per_s=200KB", "duration_s=3", "partition_count=8"]
    avro = preset.load_preset("events-1mrps-uniform", workloads_dir=ROOT / "workloads", overrides=overrides)
    matched = replace(avro, name="custom-json", value_encoding="json", sizing_encoding="avro")
    avro_meta = generate.generate(avro, str(tmp_path / "avro"), seed=seed, row_block=row_block)
    json_meta = generate.generate(matched, str(tmp_path / "json"), seed=seed, row_block=row_block)
    assert json_meta["payload_width"] == avro_meta["payload_width"]
    assert json_meta["row_count"] == avro_meta["row_count"]
    assert json_meta["rows_per_s"] == avro_meta["rows_per_s"]
    assert json_meta["partition_rows"] == avro_meta["partition_rows"]
    assert json_meta["column_stats"] == avro_meta["column_stats"]
    assert cast(int, json_meta["encoded_bytes"]) > cast(int, avro_meta["encoded_bytes"])
    fields = corpus_fields(json_meta)
    assert cast(int, fields["offered_bytes_per_s"]) > avro.offered_bytes_per_s
    assert cast(float, fields["target_row_bytes"]) > avro.target_row_bytes
    paths = [
        str(tmp_path / fmt / preset.corpus_dir_name(p, seed, row_block=row_block))
        for fmt, p in [("avro", avro), ("json", matched)]
    ]
    schema = json.loads(json.dumps(avro_meta["schema"]))
    schema["fields"] = [
        {**field, "type": field["type"]["type"] if isinstance(field["type"], dict) else field["type"]}
        for field in schema["fields"]
    ]
    for left, right in zip(*(metadata.read_manifest(path) for path in paths), strict=True):
        assert (left.rows, left.offset_ms, left.id_min, left.id_max, left.checksum) == (
            right.rows,
            right.offset_ms,
            right.id_min,
            right.id_max,
            right.checksum,
        )
        for key in left.key_uris:
            assert uri.read_bytes(left.key_uris[key]) == uri.read_bytes(right.key_uris[key])
        decoded = [
            cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
            for frame in frames.iter_frames(frames.decompress(uri.read_bytes(left.uri)))
        ]
        actual = list(frames.iter_frames(frames.decompress(uri.read_bytes(right.uri))))
        assert actual == json_values.encode_records(decoded, avro.columns)

    shards = []
    for index in range(2):
        out = str(tmp_path / f"shard-{index}")
        generate.generate(matched, out, seed=seed, row_block=row_block, shard_index=index, shard_count=2)
        shards.append(uri.join(out, preset.corpus_dir_name(matched, seed, row_block=row_block)))
    merged_meta = merge.merge(shards, str(tmp_path / "merged"))
    merged_uri = str(tmp_path / "merged" / preset.corpus_dir_name(matched, seed, row_block=row_block))
    assert [r.sha256 for r in metadata.read_manifest(merged_uri)] == [
        r.sha256 for r in metadata.read_manifest(paths[1])
    ]
    for key in ("row_count", "rows_per_s", "encoded_bytes", "offered_bytes_per_s", "target_row_bytes", "column_stats"):
        assert merged_meta[key] == json_meta[key]


def test_avro_sized_json_preset_and_legacy_identity() -> None:
    avro = preset.load_preset("events-1mrps-uniform", workloads_dir=ROOT / "workloads")
    matched = replace(avro, name="custom-json", value_encoding="json", sizing_encoding="avro")
    assert "sizing_encoding" not in preset.effective_dict(avro)
    assert preset.preset_from_effective(preset.effective_dict(matched), "events200") == matched
    with pytest.raises(ValueError, match="sizing_encoding"):
        replace(avro, sizing_encoding="json")


def test_avro_sized_json_plan_reports_json_bytes(capsys: pytest.CaptureFixture[str]) -> None:
    from ingest_bench.corpus.cli import print_plan
    from ingest_bench.corpus.values import corpus_sizing

    avro = preset.load_preset("events-1mrps-uniform", workloads_dir=ROOT / "workloads")
    matched = replace(avro, name="custom-json", value_encoding="json", sizing_encoding="avro")
    sizing = corpus_sizing(matched, 1)
    print_plan(matched, 1)
    output = capsys.readouterr().out
    assert "1376000000 avro budget bytes each" in output
    assert f"estimated encoded rate: {sizing.offered_bytes_per_s} bytes/s" in output
    assert f"estimated encoded bytes: {sizing.offered_bytes_per_s * 3600}" in output
