# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import numpy as np
import pytest

from ingest_bench.corpus import frames as f


def test_frame_round_trip() -> None:
    records = [b"abc", b"", b"defgh"]
    data = f.frame_stream(b"".join(records), np.array([3, 0, 5], dtype=np.int64))
    assert len(data) == 3 * 4 + 8
    assert list(f.iter_frames(data)) == records


def test_truncated_tail_is_an_error() -> None:
    data = f.frame_stream(b"abc", np.array([3], dtype=np.int64))
    with pytest.raises(ValueError, match="truncated"):
        list(f.iter_frames(data[:-1]))


def test_sizes_must_sum_to_records() -> None:
    with pytest.raises(ValueError, match="sizes"):
        f.frame_stream(b"abcd", np.array([3], dtype=np.int64))


def test_string_frames_are_utf8() -> None:
    data = f.string_frames(["u-1", "p00007", "é"])
    assert [b.decode() for b in f.iter_frames(data)] == ["u-1", "p00007", "é"]


def test_zstd_round_trip_and_names() -> None:
    payload = b"x" * 100_000
    packed = f.compress(payload, 3)
    assert len(packed) < 1000 and f.decompress(packed) == payload
    assert f.batch_file_name(42) == "000042.bin.zst"
    assert f.key_file_name(42, "user_id") == "000042.key.user_id.zst"
    assert len(f.sha256_hex(payload)) == 64
