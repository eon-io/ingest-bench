# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Batch file framing: length-prefixed encoded records, zstd-compressed.

A frame is exactly one Kafka message value, so the producer splits frames and
sends them without decoding a row. Key sidecars use the same framing with the
UTF-8 key text as the payload, read in lockstep with the batch file.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Iterator

import numpy as np
import zstandard

FRAME_HEADER = struct.Struct("<I")


def frame_stream(records: bytes, sizes: np.ndarray) -> bytes:
    if int(sizes.sum()) != len(records):
        raise ValueError(f"sizes sum to {int(sizes.sum())} but records hold {len(records)} bytes")
    out = bytearray(len(records) + FRAME_HEADER.size * int(sizes.size))
    src = 0
    dst = 0
    for size in sizes.tolist():
        FRAME_HEADER.pack_into(out, dst, size)
        dst += FRAME_HEADER.size
        out[dst : dst + size] = records[src : src + size]
        dst += size
        src += size
    return bytes(out)


def iter_frames(data: bytes) -> Iterator[bytes]:
    view = memoryview(data)
    offset = 0
    end = len(view)
    while offset < end:
        if offset + FRAME_HEADER.size > end:
            raise ValueError("truncated frame header")
        (size,) = FRAME_HEADER.unpack_from(view, offset)
        offset += FRAME_HEADER.size
        if offset + size > end:
            raise ValueError("truncated frame payload")
        yield bytes(view[offset : offset + size])
        offset += size


def string_frames(values: list[str]) -> bytes:
    encoded = [value.encode("utf-8") for value in values]
    sizes = np.fromiter((len(e) for e in encoded), dtype=np.int64, count=len(encoded))
    return frame_stream(b"".join(encoded), sizes)


def compress(data: bytes, level: int) -> bytes:
    return zstandard.ZstdCompressor(level=level).compress(data)


def decompress(data: bytes) -> bytes:
    return zstandard.ZstdDecompressor().decompressobj().decompress(data)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def batch_file_name(batch: int) -> str:
    return f"{batch:06d}.bin.zst"


def key_file_name(batch: int, column: str) -> str:
    return f"{batch:06d}.key.{column}.zst"
