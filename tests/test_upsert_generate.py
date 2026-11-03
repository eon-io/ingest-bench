# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import io
import json
from pathlib import Path
from typing import cast

import fastavro
import numpy as np
import pytest

from ingest_bench import uri
from ingest_bench.corpus import frames, generate, merge, metadata, preset, upsert

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
SETS = ["offered_bytes_per_s=200KB", "duration_s=4", "partition_count=8", "upsert.seed_keys=5000"]


def build(
    out: Path, extra: list[str] | None = None, shard_index: int = 0, shard_count: int = 1
) -> tuple[preset.Preset, str, dict[str, object]]:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=SETS + (extra or []))
    meta = generate.generate(p, str(out), seed=3, row_block=64, shard_index=shard_index, shard_count=shard_count)
    return p, uri.join(str(out), preset.corpus_dir_name(p, seed=3, row_block=64)), meta


def records(corpus_uri: str) -> list[generate.BatchRecord]:
    lines = uri.read_text(uri.join(corpus_uri, "manifest.jsonl")).splitlines()
    return [generate.BatchRecord.from_json(line) for line in lines]


def segments_of(record: generate.BatchRecord) -> list[dict[str, object]]:
    return cast(list[dict[str, object]], record.segments)


def decoded_segment(corpus_uri: str, record: generate.BatchRecord, index: int) -> list[dict[str, object]]:
    schema = fastavro.parse_schema(json.loads(uri.read_text(uri.join(corpus_uri, "schema.avsc"))))
    body = generate.read_segment(uri.read_bytes(uri.join(corpus_uri, record.uri)), segments_of(record)[index])
    return [
        cast(dict[str, object], fastavro.schemaless_reader(io.BytesIO(frame), schema))
        for frame in frames.iter_frames(body)
    ]


def test_batches_hold_fixed_rows_grouped_by_kafka_partition(tmp_path: Path) -> None:
    p, corpus_uri, _ = build(tmp_path)
    s = upsert.KeySchedule.for_preset(p, seed=3)
    for record in records(corpus_uri):
        assert record.rows == s.rows_per_batch
        segments = segments_of(record)
        assert [segment["partition"] for segment in segments] == list(range(16))
        assert sum(cast(int, segment["rows"]) for segment in segments) == record.rows
        assert cast(int, segments[-1]["offset"]) + cast(int, segments[-1]["length"]) == record.stored_bytes
        for index in range(16):
            rows = decoded_segment(corpus_uri, record, index)
            ids = np.array([cast(int, row["id"]) for row in rows], dtype=np.int64)
            assert (np.diff(ids) > 0).all()
            keys = s.keys_for_ids(ids)
            assert (s.kafka_partition_of(keys) == index).all()
            assert [row["event_id"] for row in rows] == s.tokens(keys)


def test_upsert_truth_is_published(tmp_path: Path) -> None:
    p, corpus_uri, meta = build(tmp_path)
    s = upsert.KeySchedule.for_preset(p, seed=3)
    truth = s.live_truth()
    block = cast(dict[str, object], meta["upsert"])
    assert block["live_rows"] == truth.rows and block["live_checksum"] == truth.checksum
    assert block["rows_per_batch"] == s.rows_per_batch and block["kafka_partitions"] == 16
    first = cast(dict[str, int], records(corpus_uri)[0].upsert)
    assert first["new_keys"] == s.new_per_batch
    assert first["distinct_keys"] == np.unique(s.batch_keys(0)).size
    read = metadata.read(corpus_uri)
    assert read.schedule is not None
    assert read.upsert_truth == {key: value for key, value in block.items() if key != "schedule_version"}


def test_whole_object_decompress_sees_only_the_first_segment(tmp_path: Path) -> None:
    _, corpus_uri, _ = build(tmp_path)
    record = records(corpus_uri)[0]
    whole = frames.decompress(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    assert len(list(frames.iter_frames(whole))) == segments_of(record)[0]["rows"]


def test_verification_catches_a_flipped_segment_byte(tmp_path: Path) -> None:
    p, corpus_uri, _ = build(tmp_path)
    record = records(corpus_uri)[1]
    data = bytearray(uri.read_bytes(uri.join(corpus_uri, record.uri)))
    data[cast(int, segments_of(record)[2]["offset"]) + 3] ^= 0xFF
    key_data = {name: uri.read_bytes(uri.join(corpus_uri, ref)) for name, ref in record.key_uris.items()}
    record.sha256 = frames.sha256_hex(bytes(data))
    with pytest.raises(AssertionError, match="segment"):
        generate.verify_segmented_batch(record, bytes(data), key_data, p, upsert.KeySchedule.for_preset(p, seed=3))


def test_more_kafka_partitions_than_rows_leaves_valid_empty_segments(tmp_path: Path) -> None:
    _, corpus_uri, _ = build(tmp_path, ["upsert.kafka_partitions=1000"])
    segments = segments_of(records(corpus_uri)[0])
    assert len(segments) == 1000 and any(segment["rows"] == 0 for segment in segments)


def test_json_upsert_corpus_generates(tmp_path: Path) -> None:
    _, _, meta = build(tmp_path, ["value_encoding=json", "target_row_bytes=1024"])
    assert meta["value_encoding"] == "json" and meta["upsert"] is not None


def test_shards_merge_to_the_same_upsert_corpus(tmp_path: Path) -> None:
    _, _, whole = build(tmp_path / "whole")
    shard_uris = [build(tmp_path / f"s{index}", shard_index=index, shard_count=2)[1] for index in range(2)]
    merged = merge.merge(shard_uris, str(tmp_path / "merged"))
    assert merged["upsert"] == whole["upsert"]
    assert merged["row_count"] == whole["row_count"]


def test_append_records_gain_no_keys() -> None:
    record = generate.BatchRecord(0, 0, 1, 0, 0, 0, 1, 1, "x", "u", {})
    assert set(json.loads(record.to_json())) == {
        "batch",
        "offset_ms",
        "rows",
        "id_min",
        "id_max",
        "checksum",
        "encoded_bytes",
        "stored_bytes",
        "sha256",
        "uri",
        "key_uris",
    }


def test_upsert_share_gate_uses_the_key_schedule() -> None:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=SETS)
    weights = upsert.KeySchedule.for_preset(p, seed=3).expected_partition_weights()
    # A correct corpus realizes the schedule's shares, including hot keys pinned to cold partitions.
    truth = {f"p{key:05d}": {"rows": 1, "sum_mod": 0, "encoded_bytes": int(weights[key] * 10**9)} for key in range(8)}
    assert cast(float, generate.partition_share_deviation(p, truth, weights)) < 0.01


def test_hourly_partitions_may_hold_only_seed_rows(tmp_path: Path) -> None:
    # Three hours of seed history: no update reaches back more than an hour, so the oldest hours
    # hold only seed rows.
    sets = [*SETS, "upsert.seed_history_s=10800", "partition_count=4"]
    p = preset.load_preset("smoke-upsert-orders", workloads_dir=WORKLOADS, overrides=sets)
    meta = generate.generate(p, str(tmp_path), seed=3, row_block=64)
    corpus_uri = uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=3, row_block=64))
    truth = json.loads(uri.read_text(uri.join(corpus_uri, "partition_truth.json")))
    assert truth["p00000"]["rows"] == 0 and truth["p00003"]["rows"] > 0
    assert cast(dict[str, int], meta["upsert"])["seed_keys"] == 5000
