# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Assign batches to shards and calculate their scheduled send times.

Due times use the corpus offsets and shared run epoch.
"""

from __future__ import annotations

from ingest_bench.corpus.generate import BatchRecord


def scheduled_ms(epoch_ms: int, offset_ms: int, speed: float) -> int:
    """Calculate a batch's due time at the requested replay speed.

    A speed of 6 replays an hour of corpus offsets in ten minutes.
    """
    if speed <= 0:
        raise ValueError(f"speed must be positive, got {speed}")
    return epoch_ms + round(offset_ms / speed)


def select_batches(records: list[BatchRecord], shard: int, shards: int, seconds: int | None) -> list[BatchRecord]:
    """Select this shard's interleaved batches in send order.

    Each shard takes every ``shards``-th batch to distribute work across the run.
    """
    if shards < 1 or not 0 <= shard < shards:
        raise ValueError(f"shard must satisfy 0 <= shard < shards, got shard={shard} shards={shards}")
    limit_ms = None if seconds is None else seconds * 1000
    return [
        record
        for record in records
        if record.batch % shards == shard and (limit_ms is None or record.offset_ms < limit_ms)
    ]


def owned_partitions(shard: int, shards: int, kafka_partitions: int) -> range:
    """The contiguous block of Kafka partitions a shard sends in an upsert offer.

    One owner per partition keeps every key's versions in corpus order, and a contiguous
    block is one range read per corpus object.
    """
    if shards < 1 or not 0 <= shard < shards:
        raise ValueError(f"shard must satisfy 0 <= shard < shards, got shard={shard} shards={shards}")
    return range(-(-shard * kafka_partitions // shards), -(-(shard + 1) * kafka_partitions // shards))
