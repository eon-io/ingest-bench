# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Sample generated columns and validate their realized distributions.

Publish measured statistics alongside declarations. Sampling uses a global
row stride so merged shard samples match an unsharded generation. Statistics
cover scalar columns and count only non-null values. Nested columns have none.
"""

from __future__ import annotations

import bisect
import hashlib
import math
import struct
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import cast

from ingest_bench.corpus.columns import (
    DECIMAL_CONTEXT,
    UNBOUNDED_CARDINALITY,
    ColumnDistribution,
    as_int_value,
    is_nested,
)
from ingest_bench.corpus.values import RowBlock, column_cdf_array, value_for_rank

# Keep the k smallest hashes; k bounds memory and the exact-count range.
COLUMN_SKETCH_SIZE = 1024
COLUMN_STATS_TARGET_SAMPLES = 200_000
# Skip expectations too small to distinguish from sampling noise.
COLUMN_CARDINALITY_GATE_MIN_EXPECTED = 8.0


def column_value_bytes(value: object) -> bytes:
    """Encode values for byte-distribution statistics.

    Use fixed-width big-endian numbers so statistics reflect values rather than
    Avro varint lengths.
    """
    if isinstance(value, bytes):
        return value
    # A decoded timestamp-millis arrives as a datetime; the epoch integer is
    # the value the generator actually drew.
    if isinstance(value, datetime):
        return round(value.timestamp() * 1000).to_bytes(8, "big", signed=True)
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, bool):
        return b"\x01" if value else b"\x00"
    if isinstance(value, int):
        return value.to_bytes(8, "big", signed=True)
    if isinstance(value, float):
        return struct.pack(">d", value)
    if isinstance(value, Decimal):
        # A column's decimals share one scale, so the unscaled integer identifies the value.
        unscaled = int(value.scaleb(-cast(int, value.as_tuple().exponent), DECIMAL_CONTEXT))
        return unscaled.to_bytes(16, "big", signed=True)
    raise TypeError(f"column value of type {type(value).__name__} has no byte encoding")


@dataclass
class ColumnStats:
    """Sampled column statistics with a mergeable K-minimum-values sketch.

    Distinct counts are exact for small sets and bounded in memory for large
    ones. Unioning shard sketches retains the same smallest hashes as sampling
    the complete corpus.
    """

    name: str
    sampled_rows: int = 0
    value_bytes: int = 0
    byte_counts: Counter[int] = field(default_factory=Counter)
    sketch: list[int] = field(default_factory=list)
    saturated: bool = False

    def observe(self, value: object) -> None:
        data = column_value_bytes(value)
        self.sampled_rows += 1
        self.value_bytes += len(data)
        self.byte_counts.update(data)
        digest = int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")
        position = bisect.bisect_left(self.sketch, digest)
        if position < len(self.sketch) and self.sketch[position] == digest:
            return
        if len(self.sketch) >= COLUMN_SKETCH_SIZE:
            self.saturated = True
            if position >= COLUMN_SKETCH_SIZE:
                return
            self.sketch.pop()
        self.sketch.insert(position, digest)

    def distinct_values(self) -> float:
        if not self.saturated:
            return float(len(self.sketch))
        return (COLUMN_SKETCH_SIZE - 1) * 2**64 / self.sketch[COLUMN_SKETCH_SIZE - 1]

    def value_byte_entropy_bits_per_byte(self) -> float:
        """Return byte entropy pooled across sampled values.

        This ignores repetition between rows: a repeated random blob can score as
        high as independently generated blobs.
        """
        if self.value_bytes == 0:
            return 0.0
        total = float(self.value_bytes)
        return -sum((count / total) * math.log2(count / total) for count in self.byte_counts.values())

    def entropy_bits_per_byte(self) -> float:
        """Weight byte entropy by the fraction of first-seen sampled values.

        This discounts repetition but does not estimate codec compression. Compare
        columns at equal sample counts: once a bounded vocabulary is exhausted, the
        metric falls as the sample grows.
        """
        if self.sampled_rows == 0:
            return 0.0
        return self.value_byte_entropy_bits_per_byte() * min(1.0, self.distinct_values() / self.sampled_rows)

    def mean_value_bytes(self) -> float:
        return self.value_bytes / self.sampled_rows if self.sampled_rows else 0.0

    def merge(self, other: ColumnStats) -> None:
        self.sampled_rows += other.sampled_rows
        self.value_bytes += other.value_bytes
        self.byte_counts.update(other.byte_counts)
        merged = sorted(set(self.sketch) | set(other.sketch))
        self.saturated = self.saturated or other.saturated or len(merged) > COLUMN_SKETCH_SIZE
        self.sketch = merged[:COLUMN_SKETCH_SIZE]

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "sampled_rows": self.sampled_rows,
            "value_bytes": self.value_bytes,
            "byte_counts": {str(byte): count for byte, count in sorted(self.byte_counts.items())},
            "sketch": self.sketch,
            "saturated": self.saturated,
        }

    @staticmethod
    def from_dict(raw: dict[str, object]) -> ColumnStats:
        counts = Counter(
            {int(byte): as_int_value(count) for byte, count in cast(dict[str, object], raw["byte_counts"]).items()}
        )
        return ColumnStats(
            name=str(raw["name"]),
            sampled_rows=as_int_value(raw["sampled_rows"]),
            value_bytes=as_int_value(raw["value_bytes"]),
            byte_counts=counts,
            sketch=[as_int_value(value) for value in cast(list[object], raw["sketch"])],
            saturated=bool(raw["saturated"]),
        )


def new_column_stats(columns: tuple[ColumnDistribution, ...]) -> dict[str, ColumnStats]:
    return {column.name: ColumnStats(column.name) for column in columns if not is_nested(column)}


def observe_block(block: RowBlock, column_stats: dict[str, ColumnStats], global_row_start: int, stride: int) -> None:
    """Sample a block using corpus-wide row positions.

    Use generated values directly, including timestamps as epoch milliseconds.
    """
    for index in range(-global_row_start % stride, block.rows, stride):
        for name, stats in column_stats.items():
            value = block.values[name][index]
            if value is not None:
                stats.observe(value)


def stats_stride(estimated_rows: int, target_samples: int = COLUMN_STATS_TARGET_SAMPLES) -> int:
    """Choose a sampling stride from the preset's estimated row count.

    Using the preset keeps the stride identical across shards.
    """
    return max(1, estimated_rows // target_samples)


def expected_distinct_values(column: ColumnDistribution, sampled_rows: int) -> float | None:
    """Estimate distinct sampled values where a meaningful comparison is available.

    Account for ranks that map to the same value. Return ``None`` when the value
    set exceeds the exact sketch range, the expectation is too small, or no
    closed-form estimate is available.
    """
    if column.cardinality == UNBOUNDED_CARDINALITY or sampled_rows == 0:
        return None
    if column.cardinality > COLUMN_SKETCH_SIZE:
        return None
    cdf = column_cdf_array(column.cardinality, column.alpha)
    weights: dict[str, float] = defaultdict(float)
    for rank, cumulative in enumerate(cdf):
        value = value_for_rank(column, rank)
        if value is None:
            return None
        weights[repr(value)] += cumulative - (cdf[rank - 1] if rank else 0.0)
    # Convert the NumPy scalar to a JSON-compatible number.
    expected = float(sum(1.0 - (1.0 - weight) ** sampled_rows for weight in weights.values()))
    return expected if expected >= COLUMN_CARDINALITY_GATE_MIN_EXPECTED else None


def column_stats_summary(
    column: ColumnDistribution, stats: ColumnStats, cardinality_deviation: float | None
) -> dict[str, object]:
    return {
        "kind": column.kind,
        "declared_cardinality": column.cardinality,
        "declared_alpha": column.alpha,
        "sampled_rows": stats.sampled_rows,
        "distinct_values": stats.distinct_values(),
        "distinct_exact": not stats.saturated,
        "entropy_bits_per_byte": stats.entropy_bits_per_byte(),
        "value_byte_entropy_bits_per_byte": stats.value_byte_entropy_bits_per_byte(),
        "mean_value_bytes": stats.mean_value_bytes(),
        "cardinality_checked": cardinality_deviation is not None,
        "cardinality_relative_deviation": cardinality_deviation,
    }
