# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import io
import json
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import cast

import fastavro
import pytest

from ingest_bench import uri
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import frames, generate, preset
from ingest_bench.corpus.stats import ColumnStats

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


@pytest.fixture(scope="module")
def tiny(tmp_path_factory: pytest.TempPathFactory) -> tuple[preset.Preset, str, dict[str, object]]:
    p = preset.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["offered_bytes_per_s=200KB", "duration_s=6", "partition_count=8"]
    )
    out = str(tmp_path_factory.mktemp("corpus"))
    meta = generate.generate(p, out, seed=3, row_block=64)
    return p, uri.join(out, preset.corpus_dir_name(p, seed=3, row_block=64)), meta


def test_layout_and_manifest(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, meta = tiny
    assert meta["batch_count"] == 6 and meta["generator_version"] == "6"
    manifest = [
        generate.BatchRecord.from_json(line)
        for line in uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
    ]
    assert [r.batch for r in manifest] == list(range(6))
    assert [r.offset_ms for r in manifest] == [0, 1000, 2000, 3000, 4000, 5000]
    for r in manifest:
        assert r.encoded_bytes >= p.batch_bytes and r.encoded_bytes < p.batch_bytes + 64 * 400
        assert r.id_min == r.batch * c.ID_BLOCK and r.id_max == r.id_min + r.rows - 1
        assert r.checksum == ((r.id_min + r.id_max) * r.rows // 2) % c.P
        assert set(r.key_uris) == {"user_id", "partition_key"}
        assert uri.exists(uri.join(corpus_uri, r.uri))
    assert json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc")))["name"] == "event"


def test_frames_decode_and_match_sidecars(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, corpus_uri, meta = tiny
    schema = fastavro.parse_schema(json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc"))))
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[2])
    data = frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    users = [
        b.decode()
        for b in frames.iter_frames(frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.key_uris["user_id"]))))
    ]
    rows = [
        cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(fr), schema)) for fr in frames.iter_frames(data)
    ]
    assert len(rows) == record.rows == len(users)
    assert [r["user_id"] for r in rows] == users
    assert rows[0]["id"] == record.id_min and rows[-1]["id"] == record.id_max


def test_partition_truth_matches_rows(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, corpus_uri, meta = tiny
    truth = json.loads(uri.read_text(uri.join(corpus_uri, "partition_truth.json")))
    assert sum(v["rows"] for v in truth.values()) == meta["row_count"]
    assert set(truth) == {f"p{k:05d}" for k in range(8)}
    # Published even when the corpus is too small to gate.
    assert isinstance(meta["partition_byte_share_max_relative_deviation"], float)


def test_regeneration_is_bit_identical(tmp_path: Path, tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, meta = tiny
    again = generate.generate(p, str(tmp_path), seed=3, row_block=64)
    assert again["manifest_sha256"] == meta["manifest_sha256"]
    a = uri.read_bytes(uri.join(corpus_uri, "batches", frames.batch_file_name(1)))
    b = uri.read_bytes(
        uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=3, row_block=64), "batches", frames.batch_file_name(1))
    )
    assert a == b


@pytest.mark.parametrize("seed,row_block", [(2, 1024), (1, 64)])
def test_different_generator_inputs_preserve_separate_corpora(tmp_path: Path, seed: int, row_block: int) -> None:
    p = preset.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"]
    )
    first = generate.generate(p, str(tmp_path), seed=1)
    first_dir = tmp_path / f"{p.name}-{first['corpus_hash']}"
    first_metadata = (first_dir / "corpus.json").read_bytes()
    first_batch = (first_dir / "batches" / frames.batch_file_name(0)).read_bytes()
    second = generate.generate(p, str(tmp_path), seed=seed, row_block=row_block)
    assert first["corpus_hash"] != second["corpus_hash"]
    assert first["manifest_sha256"] != second["manifest_sha256"]
    assert (first_dir / "corpus.json").read_bytes() == first_metadata
    assert (first_dir / "batches" / frames.batch_file_name(0)).read_bytes() == first_batch
    assert (tmp_path / f"{p.name}-{second['corpus_hash']}" / "corpus.json").is_file()


def test_shards_reproduce_the_whole(tmp_path: Path, tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, _ = tiny
    whole = uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
    parts: list[str] = []
    for i in range(3):
        out = str(tmp_path / f"shard-{i}")
        generate.generate(p, out, seed=3, shard_index=i, shard_count=3, row_block=64)
        parts += uri.read_text(
            uri.join(out, preset.corpus_dir_name(p, seed=3, row_block=64), "manifest.jsonl")
        ).splitlines()
    assert sorted(parts) == sorted(whole)


def test_corpus_json_publishes_types_roles_and_gates(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    _, _, meta = tiny
    assert meta["iceberg_types"]["event_time"] == "timestamp"  # type: ignore[index]
    assert meta["iceberg_types"]["id"] == "long"  # type: ignore[index]
    assert meta["column_roles"]["sum_measure"] == "amount_micros"  # type: ignore[index]
    assert abs(float(meta["mean_encoded_row_size_relative_deviation"])) <= 0.02  # type: ignore[arg-type]
    assert meta["key_columns"] == ["user_id", "partition_key"]
    assert meta["p"] == c.P and meta["id_block"] == c.ID_BLOCK


def _unbounded_payload_columns() -> tuple[c.ColumnDistribution, ...]:
    """Build a schema for the entropy gate. Bounded cardinalities exceed the sketch
    size so cardinality validation does not interfere.
    """
    return c.build_columns(
        (
            c.timestamp_column("event_time", c.UNBOUNDED_CARDINALITY),
            c.token_column("event_id", 32, role=c.ROLE_ENTITY),
            c.integer_column("amount", 2000, 0, 1_000_000, role=c.ROLE_SUM_MEASURE),
            c.blob_column("payload"),
        )
    )


def test_blob_entropy_gate_rejects_a_compressible_payload(
    tiny: tuple[preset.Preset, str, dict[str, object]],
) -> None:
    p, _, _ = tiny
    columns = _unbounded_payload_columns()
    shaped = replace(p, columns=columns, kafka_key_columns=("partition_key",))
    rows, keys = 1000, shaped.partition_count
    records = [generate.BatchRecord(0, 0, rows, 0, rows - 1, 0, rows * shaped.target_row_bytes, 1, "s", "b", {})]
    truth = {
        f"p{k:05d}": {"rows": rows // keys, "sum_mod": 0, "encoded_bytes": rows * shaped.target_row_bytes // keys}
        for k in range(keys)
    }

    def meta_for(payload: bytes) -> dict[str, object]:
        stats = {column.name: ColumnStats(column.name) for column in columns}
        for _ in range(64):
            stats["payload"].observe(payload)
        return generate.finalize_corpus_json(shaped, 1, records, truth, stats, 1, 64, 64, 3, 0, 1)

    published = meta_for(bytes(range(256)))
    assert float(published["unbounded_blob_min_entropy_bits_per_byte"]) == pytest.approx(8.0)  # type: ignore[arg-type]
    # 1000 rows leave the coldest of these keys far short of the gate's floor.
    assert published["partition_byte_share_gate_enforced"] is False
    with pytest.raises(ValueError, match="entropy"):
        meta_for(b"\x00" * 256)


def test_verify_batch_catches_a_flipped_byte(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, _ = tiny
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[0])
    data = bytearray(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    data[-1] ^= 0xFF
    keys = {k: uri.read_bytes(uri.join(corpus_uri, u)) for k, u in record.key_uris.items()}
    with pytest.raises(AssertionError, match="batch 0"):
        generate.verify_batch(record, bytes(data), keys, p.columns, {})


def _first_batch(corpus_uri: str) -> tuple[generate.BatchRecord, bytes, dict[str, bytes], list[str]]:
    """The first batch's record and stored bytes, its sidecars, and its decoded key strings."""
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[0])
    data = uri.read_bytes(uri.join(corpus_uri, record.uri))
    sidecars = {name: uri.read_bytes(uri.join(corpus_uri, rel)) for name, rel in record.key_uris.items()}
    users = [frame.decode() for frame in frames.iter_frames(frames.decompress(sidecars["user_id"]))]
    return record, data, sidecars, users


def test_verify_batch_catches_a_truncated_sidecar(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, corpus_uri, _ = tiny
    record, data, sidecars, users = _first_batch(corpus_uri)
    sidecars["user_id"] = frames.compress(frames.string_frames(users[:-1]), 3)
    expected = f"batch 0: sidecar user_id holds {record.rows - 1} frames for {record.rows} rows"
    with pytest.raises(AssertionError, match=expected):
        generate.verify_batch(record, data, sidecars, p.columns, {})


def test_verify_batch_catches_a_sidecar_with_an_extra_frame(
    tiny: tuple[preset.Preset, str, dict[str, object]],
) -> None:
    p, corpus_uri, _ = tiny
    record, data, sidecars, users = _first_batch(corpus_uri)
    sidecars["user_id"] = frames.compress(frames.string_frames([*users, users[-1]]), 3)
    expected = f"batch 0: sidecar user_id holds {record.rows + 1} frames for {record.rows} rows"
    with pytest.raises(AssertionError, match=expected):
        generate.verify_batch(record, data, sidecars, p.columns, {})


def test_naive_corpus_epoch_is_refused(tiny: tuple[preset.Preset, str, dict[str, object]]) -> None:
    p, _, _ = tiny
    assert generate.epoch_ms(p) == 1_767_225_600_000
    with pytest.raises(ValueError, match="corpus_epoch"):
        generate.epoch_ms(replace(p, corpus_epoch="2026-01-01T00:00:00"))


def test_event_times_are_whole_milliseconds_in_their_batch_window(
    tiny: tuple[preset.Preset, str, dict[str, object]],
) -> None:
    p, corpus_uri, _ = tiny
    published = cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc"))))
    fields = cast(list[dict[str, object]], published["fields"])
    event_time_field = next(field for field in fields if field["name"] == "event_time")
    assert event_time_field["type"] == {"type": "long", "logicalType": "timestamp-millis"}
    schema = fastavro.parse_schema(published)
    record = generate.BatchRecord.from_json(uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()[2])
    data = frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    start_ms = generate.epoch_ms(p) + record.offset_ms
    for frame in frames.iter_frames(data):
        row = cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
        event_time = cast(datetime, row["event_time"])
        assert event_time.microsecond % 1000 == 0
        assert start_ms <= round(event_time.timestamp() * 1000) < start_ms + p.batch_interval_ms
