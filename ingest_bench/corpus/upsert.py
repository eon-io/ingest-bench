# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Upsert workloads: rows that create or update keys, and the schedule that names each row's key.

A row's key is a pure function of the corpus seed and the row's batch and position,
so the generator, scorer and verifier compute it independently and batches still
generate in any order.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from functools import lru_cache
from pathlib import Path
from typing import TYPE_CHECKING, cast

import numpy as np

from ingest_bench.corpus import columns as c
from ingest_bench.corpus import values as v

if TYPE_CHECKING:
    from ingest_bench.corpus.preset import Preset

# Part of the corpus identity: changing how keys are drawn must change the corpus hash.
SCHEDULE_VERSION = "1"

RECENT = "recent"
UNIFORM = "uniform"
HOT = "hot"
TARGETS = (RECENT, UNIFORM, HOT)
EXPONENTIAL = "exponential"

# Key tokens: hashed from the key alone, or led by the key's creation time in hex milliseconds,
# as Snowflake-style IDs are.
RANDOM_LAYOUT = "random"
TIME_ORDERED = "time_ordered"
KEY_LAYOUTS = (RANDOM_LAYOUT, TIME_ORDERED)
TIME_PREFIX_WIDTH = 12
# The seed reaches the table as a registered table the harness wrote, or through Kafka for the
# engine to write before the scored offer.
SEED_TABLE = "table"
SEED_KAFKA = "kafka"
SEED_DELIVERIES = (SEED_TABLE, SEED_KAFKA)
# Table partitions: a Zipf draw hashed from the key, or the hour the key was created in, as an
# orders table partitioned by order time is.
HASHED = "hashed"
CREATION_HOUR = "creation_hour"
PARTITION_LAYOUTS = (HASHED, CREATION_HOUR)
HOUR_MS = 3_600_000

# Hot ranks map to seed keys through `rank * HOT_MULTIPLIER + offset (mod seed_keys)`, which is one to one
# while this prime exceeds the seed key count.
HOT_MULTIPLIER = 1_000_000_007

_BLOCK_KEYS = ("key_column", "kafka_partitions", "seed_keys", "new_key_fraction", "updates")
# Optional settings stay out of the effective schedule while unset, so corpora that predate them
# keep their hash.
_OPTIONAL_DEFAULTS: dict[str, object] = {
    "key_layout": RANDOM_LAYOUT,
    "seed_delivery": SEED_TABLE,
    "seed_history_s": None,
    "partition_layout": HASHED,
}


@dataclass(frozen=True)
class UpdateComponent:
    """One share of the update rows and how it picks the key it updates."""

    share: float
    target: str
    age_distribution: str | None = None
    age_mean_s: float | None = None
    age_max_s: int | None = None
    keys: int | None = None
    alpha: float | None = None


@dataclass(frozen=True)
class UpsertConfig:
    key_column: str
    kafka_partitions: int
    seed_keys: int
    new_key_fraction: float
    updates: tuple[UpdateComponent, ...]
    schedule_version: str = SCHEDULE_VERSION
    key_layout: str = RANDOM_LAYOUT
    seed_delivery: str = SEED_TABLE
    # Seed keys' creation times span this many seconds before the stream, in key order. Recent
    # updates older than the stream then reach the seed key created that long ago.
    seed_history_s: int | None = None
    partition_layout: str = HASHED


def _optional_float(value: object) -> float | None:
    return None if value is None else float(cast(float, value))


def _optional_int(value: object) -> int | None:
    return None if value is None else int(cast(int, value))


def _component(raw: object, index: int) -> UpdateComponent:
    entry = cast(dict[str, object], raw)
    target = str(entry.get("target"))
    if target not in TARGETS:
        raise ValueError(f"upsert.updates[{index}] has unknown update target {target!r}; use one of {TARGETS}")
    share = float(cast(float, entry["share"]))
    if target == RECENT:
        if "age" not in entry:
            raise ValueError(f"upsert.updates[{index}] targets recent keys and needs an age block")
        age = cast(dict[str, object], entry["age"])
        if age.get("distribution") != EXPONENTIAL:
            raise ValueError(f"upsert.updates[{index}].age.distribution must be {EXPONENTIAL}")
        return UpdateComponent(
            share,
            target,
            age_distribution=EXPONENTIAL,
            age_mean_s=_optional_float(age.get("mean_s")),
            age_max_s=_optional_int(age.get("max_s")),
        )
    if target == HOT:
        return UpdateComponent(
            share, target, keys=_optional_int(entry.get("keys")), alpha=_optional_float(entry.get("alpha"))
        )
    return UpdateComponent(share, target)


def validate(config: UpsertConfig) -> None:
    if config.kafka_partitions < 1:
        raise ValueError("upsert.kafka_partitions must be at least 1")
    if config.seed_keys < 1:
        raise ValueError("upsert.seed_keys must be at least 1, since updates need keys to target")
    if not 0.0 < config.new_key_fraction < 1.0:
        raise ValueError("upsert.new_key_fraction must lie strictly between 0 and 1")
    if not config.updates:
        raise ValueError("upsert.updates must name at least one component")
    if config.key_layout not in KEY_LAYOUTS:
        raise ValueError(f"upsert.key_layout must be one of {KEY_LAYOUTS}, not {config.key_layout!r}")
    if config.seed_delivery not in SEED_DELIVERIES:
        raise ValueError(f"upsert.seed_delivery must be one of {SEED_DELIVERIES}, not {config.seed_delivery!r}")
    if config.seed_history_s is not None and config.seed_history_s < 1:
        raise ValueError("upsert.seed_history_s must be at least 1")
    if config.key_layout == TIME_ORDERED and config.seed_history_s is None:
        raise ValueError(f"upsert.key_layout {TIME_ORDERED} needs seed_history_s to date the seed keys")
    if config.partition_layout not in PARTITION_LAYOUTS:
        raise ValueError(f"upsert.partition_layout must be one of {PARTITION_LAYOUTS}, not {config.partition_layout!r}")
    if config.partition_layout == CREATION_HOUR and config.seed_history_s is None:
        raise ValueError(f"upsert.partition_layout {CREATION_HOUR} needs seed_history_s to date the seed keys")
    if any(update.share <= 0 for update in config.updates) or not math.isclose(
        sum(update.share for update in config.updates), 1.0
    ):
        raise ValueError("upsert.updates shares must be positive and sum to 1")
    for index, update in enumerate(config.updates):
        if update.target == RECENT and (
            update.age_mean_s is None or update.age_mean_s <= 0 or update.age_max_s is None or update.age_max_s < 1
        ):
            raise ValueError(f"upsert.updates[{index}].age needs mean_s > 0 and max_s >= 1")
        if update.target == HOT:
            if update.keys is None or not 1 <= update.keys <= config.seed_keys:
                raise ValueError(f"upsert.updates[{index}] hot keys must lie between 1 and seed_keys")
            if update.alpha is None or update.alpha < 0:
                raise ValueError(f"upsert.updates[{index}] hot alpha must be at least 0")
            if config.seed_keys >= HOT_MULTIPLIER:
                raise ValueError(f"hot keys need seed_keys below {HOT_MULTIPLIER}")


def parse_upsert(raw: object) -> UpsertConfig:
    """Read the preset's ``upsert`` block, written with nested ``age`` blocks."""
    block = cast(dict[str, object], raw)
    unknown = sorted(set(block) - set(_BLOCK_KEYS) - set(_OPTIONAL_DEFAULTS))
    missing = [key for key in _BLOCK_KEYS if key not in block]
    if unknown or missing:
        raise ValueError(f"upsert block: unknown keys {unknown}, missing keys {missing}")
    config = UpsertConfig(
        key_column=str(block["key_column"]),
        kafka_partitions=int(cast(int, block["kafka_partitions"])),
        seed_keys=int(cast(int, block["seed_keys"])),
        new_key_fraction=float(cast(float, block["new_key_fraction"])),
        updates=tuple(_component(entry, index) for index, entry in enumerate(cast(list[object], block["updates"]))),
    )
    config = _with_options(config, block)
    validate(config)
    return config


def _with_options(config: UpsertConfig, block: dict[str, object]) -> UpsertConfig:
    history = block.get("seed_history_s")
    return replace(
        config,
        key_layout=str(block.get("key_layout", RANDOM_LAYOUT)),
        seed_delivery=str(block.get("seed_delivery", SEED_TABLE)),
        seed_history_s=None if history is None else int(cast(int, history)),
        partition_layout=str(block.get("partition_layout", HASHED)),
    )


def effective(config: UpsertConfig) -> dict[str, object]:
    """The flat form recorded in ``corpus.json`` and hashed into the corpus identity."""
    return {
        key: value
        for key, value in asdict(config).items()
        if key not in _OPTIONAL_DEFAULTS or value != _OPTIONAL_DEFAULTS[key]
    }


def from_effective(raw: object) -> UpsertConfig:
    block = cast(dict[str, object], raw)
    updates = tuple(
        UpdateComponent(
            share=float(cast(float, entry["share"])),
            target=str(entry["target"]),
            age_distribution=None if entry.get("age_distribution") is None else str(entry["age_distribution"]),
            age_mean_s=_optional_float(entry.get("age_mean_s")),
            age_max_s=_optional_int(entry.get("age_max_s")),
            keys=_optional_int(entry.get("keys")),
            alpha=_optional_float(entry.get("alpha")),
        )
        for entry in (cast(dict[str, object], item) for item in cast(list[object], block["updates"]))
    )
    config = UpsertConfig(
        key_column=str(block["key_column"]),
        kafka_partitions=int(cast(int, block["kafka_partitions"])),
        seed_keys=int(cast(int, block["seed_keys"])),
        new_key_fraction=float(cast(float, block["new_key_fraction"])),
        updates=updates,
        schedule_version=str(block["schedule_version"]),
    )
    config = _with_options(config, block)
    validate(config)
    return config


# The scorer stamps each key with a uint32 stream ordinal and reserves 0 for the seed version.
MAX_STREAM_ROWS = 2**32 - 1
_KEY_CHUNK = 1 << 24
_FRACTION = 2.0**-53


@dataclass(frozen=True)
class LiveTruth:
    """The final table every correct engine converges to: one row per key, at its last version."""

    rows: int
    checksum: int


def rows_per_batch(preset: Preset) -> int:
    """Fix each batch's row count so key ordinals are known before any batch is encoded."""
    return preset.batch_bytes // preset.target_row_bytes


def component_counts(config: UpsertConfig, rows: int) -> tuple[int, ...]:
    """Split a batch into new-key rows, then each update component by largest remainder."""
    new = round(rows * config.new_key_fraction)
    updates = rows - new
    raw = [updates * component.share for component in config.updates]
    counts = [math.floor(share) for share in raw]
    order = sorted(range(len(raw)), key=lambda index: (counts[index] - raw[index], index))
    for index in order[: updates - sum(counts)]:
        counts[index] += 1
    return (new, *counts)


def _salt(seed: int, purpose: str) -> int:
    return int.from_bytes(hashlib.blake2b(f"{seed}:upsert:{purpose}".encode(), digest_size=8).digest(), "little")


def _mix(keys: np.ndarray, salt: int) -> np.ndarray:
    """SplitMix64 over key ordinals. uint64 arithmetic wraps, which the finalizer relies on."""
    z = keys.astype(np.uint64) + np.uint64(salt) + np.uint64(0x9E3779B97F4A7C15)
    z = (z ^ (z >> np.uint64(30))) * np.uint64(0xBF58476D1CE4E5B9)
    z = (z ^ (z >> np.uint64(27))) * np.uint64(0x94D049BB133111EB)
    mixed: np.ndarray = z ^ (z >> np.uint64(31))
    return mixed


class KeySchedule:
    """Name the key of every corpus row, and derive each key's columns and Kafka partition."""

    def __init__(
        self,
        config: UpsertConfig,
        *,
        seed: int,
        rows_per_batch: int,
        batch_count: int,
        batch_interval_ms: int,
        partition_count: int,
        alpha: float,
        key_width: int,
        epoch_ms: int = 0,
    ) -> None:
        if rows_per_batch * batch_count > MAX_STREAM_ROWS:
            raise ValueError(
                f"{rows_per_batch * batch_count} stream rows exceed the uint32 ordinals the scorer stamps keys with"
            )
        self.config = config
        self.seed = seed
        self.rows_per_batch = rows_per_batch
        self.batch_count = batch_count
        self.batch_interval_ms = batch_interval_ms
        self.key_column = config.key_column
        self.key_width = key_width
        if config.key_layout == TIME_ORDERED and key_width < TIME_PREFIX_WIDTH + 8:
            raise ValueError(
                f"upsert.key_layout {TIME_ORDERED} needs a key column at least {TIME_PREFIX_WIDTH + 8} characters wide"
            )
        self.epoch_ms = epoch_ms
        self.component_counts = component_counts(config, rows_per_batch)
        self.new_per_batch = self.component_counts[0]
        if self.new_per_batch == 0 and any(update.target == RECENT for update in config.updates):
            raise ValueError("recent updates need at least one new key per batch; raise new_key_fraction")
        self._partition_cdf = v.zipf_cdf(partition_count, alpha)
        if config.partition_layout == CREATION_HOUR:
            span_ms = 1000 * cast(int, config.seed_history_s) + batch_count * batch_interval_ms
            hours = -(-span_ms // HOUR_MS)
            if partition_count != hours:
                raise ValueError(
                    f"seed history and stream span {hours} hourly partitions, so partition_count must be {hours}, "
                    f"not {partition_count}"
                )
            if alpha != 0.0:
                raise ValueError(f"hourly partitions follow creation times, so alpha must be 0.0, not {alpha}")
        self._hot_cdfs = {
            index: v.zipf_cdf(cast(int, update.keys), cast(float, update.alpha))
            for index, update in enumerate(config.updates)
            if update.target == HOT
        }
        self._hot_offset = _salt(seed, "hot") % config.seed_keys
        self._key_map: np.ndarray | None = None
        self._generated_keys: Callable[[int], np.ndarray] = lru_cache(maxsize=64)(self._batch_keys)

    @classmethod
    def for_preset(cls, preset: Preset, seed: int) -> KeySchedule:
        if preset.upsert is None:
            raise ValueError(f"preset {preset.name} has no upsert block")
        # The generator imports this module, so resolve its epoch parser at call time.
        from ingest_bench.corpus.generate import epoch_ms

        key = next(column for column in preset.columns if column.name == preset.upsert.key_column)
        return cls(
            preset.upsert,
            seed=seed,
            rows_per_batch=rows_per_batch(preset),
            batch_count=preset.batch_count,
            batch_interval_ms=preset.batch_interval_ms,
            partition_count=preset.partition_count,
            alpha=preset.alpha,
            key_width=key.width,
            epoch_ms=epoch_ms(preset),
        )

    def keys_created_before(self, batch: int) -> int:
        return self.config.seed_keys + batch * self.new_per_batch

    @property
    def total_keys(self) -> int:
        return self.keys_created_before(self.batch_count)

    def _draw(self, batch: int, name: str) -> np.ndarray:
        words = v.column_words(self.seed, batch, f"upsert.{name}", 0, self.rows_per_batch, 1)[::4]
        fractions: np.ndarray = v.word_fractions(words)
        return fractions

    @staticmethod
    def _pick(fractions: np.ndarray, count: int) -> np.ndarray:
        picked: np.ndarray = np.minimum(np.floor(fractions * count).astype(np.int64), count - 1)
        return picked

    def _ages(self, fractions: np.ndarray, component: UpdateComponent) -> np.ndarray:
        """Age in batches: one plus the exponential draw in whole batch intervals, capped at max_s."""
        mean_s, max_s = cast(float, component.age_mean_s), cast(int, component.age_max_s)
        ages = 1 + np.floor(-mean_s * np.log1p(-fractions) * 1000 / self.batch_interval_ms).astype(np.int64)
        capped: np.ndarray = np.minimum(ages, max(1, max_s * 1000 // self.batch_interval_ms))
        return capped

    def recent_ages(self, batch: int) -> np.ndarray:
        """The age draw every recent row of ``batch`` would take, exposed to test the distribution."""
        component = next(update for update in self.config.updates if update.target == RECENT)
        return self._ages(self._draw(batch, "draw"), component)

    def hot_key(self, ranks: np.ndarray) -> np.ndarray:
        keys: np.ndarray = (ranks.astype(np.int64) * HOT_MULTIPLIER + self._hot_offset) % self.config.seed_keys
        return keys

    def _batch_keys(self, batch: int) -> np.ndarray:
        rows = self.rows_per_batch
        stream = np.random.Philox(key=v.column_stream_key(self.seed, batch, "upsert.component"))
        positions = np.random.Generator(stream).permutation(rows)
        draw, pick = self._draw(batch, "draw"), self._draw(batch, "pick")
        keys = np.empty(rows, dtype=np.int64)
        start = self.new_per_batch
        keys[np.sort(positions[:start])] = self.keys_created_before(batch) + np.arange(start)
        for index, (component, count) in enumerate(zip(self.config.updates, self.component_counts[1:], strict=True)):
            here = positions[start : start + count]
            start += count
            if component.target == RECENT:
                cohort = batch - self._ages(draw[here], component)
                offsets = self._pick(pick[here], self.new_per_batch)
                from_cohort = self.config.seed_keys + cohort * self.new_per_batch + offsets
                if self.config.seed_history_s is None:
                    before = self._pick(pick[here], self.config.seed_keys)
                else:
                    before = self.seed_key_at_age(-cohort * self.batch_interval_ms, pick[here])
                keys[here] = np.where(cohort >= 0, from_cohort, before)
            elif component.target == UNIFORM:
                keys[here] = self._pick(pick[here], self.keys_created_before(batch))
            else:
                cdf = self._hot_cdfs[index]
                ranks = np.minimum(np.searchsorted(cdf, draw[here], side="right"), cdf.size - 1)
                keys[here] = self.hot_key(ranks)
        keys.setflags(write=False)
        return keys

    def creation_ms(self, keys: np.ndarray) -> np.ndarray:
        """When each key was created: seed keys spread over the seed history, stream keys at their batch."""
        keys = keys.astype(np.int64)
        seed_keys = self.config.seed_keys
        history_ms = 1000 * (self.config.seed_history_s or 0)
        seeded = self.epoch_ms - history_ms + (np.minimum(keys, seed_keys) * history_ms) // seed_keys
        streamed = self.epoch_ms + ((keys - seed_keys) * self.batch_interval_ms) // max(1, self.new_per_batch)
        created: np.ndarray = np.where(keys < seed_keys, seeded, streamed)
        return created

    def seed_key_at_age(self, before_ms: np.ndarray, fractions: np.ndarray) -> np.ndarray:
        """The seed key created ``before_ms`` before the stream, picked within one batch interval of it.

        Ages past the seed history land on the oldest seed key.
        """
        seed_keys = self.config.seed_keys
        history_ms = 1000 * cast(int, self.config.seed_history_s)
        offset_ms = before_ms.astype(np.float64) - fractions * self.batch_interval_ms
        back = np.floor(np.maximum(offset_ms, 0.0) * seed_keys / history_ms).astype(np.int64)
        picked: np.ndarray = np.clip(seed_keys - 1 - back, 0, seed_keys - 1)
        return picked

    def build_key_map(self, path: Path) -> None:
        """Write every stream row's key to ``path`` once, then answer lookups from it.

        A rewritten or compacted file holds rows of many batches. Without the map, each lookup
        regenerates every batch it touches, and a file spanning thousands of batches costs minutes.
        """
        if self.total_keys >= 2**31:
            raise ValueError(f"{self.total_keys} keys do not fit the int32 key map")
        path.parent.mkdir(parents=True, exist_ok=True)
        rows = self.rows_per_batch
        key_map = np.lib.format.open_memmap(path, mode="w+", dtype=np.int32, shape=(self.batch_count * rows,))
        for batch in range(self.batch_count):
            key_map[batch * rows : (batch + 1) * rows] = self._batch_keys(batch)
        key_map.flush()
        del key_map
        self._key_map = np.load(path, mmap_mode="r")

    def batch_keys(self, batch: int) -> np.ndarray:
        """Key ordinals of one batch's rows, in row order (read-only)."""
        if self._key_map is None:
            return self._generated_keys(batch)
        keys = self._key_map[batch * self.rows_per_batch : (batch + 1) * self.rows_per_batch].astype(np.int64)
        keys.setflags(write=False)
        return keys

    def _token_payload(self, keys: np.ndarray) -> tuple[list[str], np.ndarray]:
        """Hex tokens of the key column's width, drawn from the key alone, in the unbounded token layout."""
        width = self.key_width
        byte_width = width // 2 + 1
        words = -(-byte_width // 8)
        salt = _salt(self.seed, f"token:{self.key_column}")
        matrix = np.stack([_mix(keys, salt + index) for index in range(words)], axis=1)
        raw = matrix.astype("<u8").view(np.uint8).reshape(keys.size, words * 8)[:, :byte_width]
        hexed = np.ascontiguousarray(raw).tobytes().hex()
        stride = 2 * byte_width
        payload = np.frombuffer(hexed.encode("ascii"), dtype=np.uint8).reshape(keys.size, stride)[:, :width]
        if self.config.key_layout == TIME_ORDERED:
            stamps = "".join(f"{stamp:0{TIME_PREFIX_WIDTH}x}" for stamp in self.creation_ms(keys).tolist())
            prefix = np.frombuffer(stamps.encode("ascii"), dtype=np.uint8).reshape(keys.size, TIME_PREFIX_WIDTH)
            payload = np.concatenate([prefix, payload[:, : width - TIME_PREFIX_WIDTH]], axis=1)
        payload = np.ascontiguousarray(payload)
        text = payload.tobytes().decode("ascii")
        tokens = [text[index * width : (index + 1) * width] for index in range(keys.size)]
        return tokens, payload.reshape(-1)

    def tokens(self, keys: np.ndarray) -> list[str]:
        return self._token_payload(keys)[0]

    def token_matrix(self, keys: np.ndarray) -> np.ndarray:
        """Each key's token as one row of ``key_width`` ASCII bytes, for vectorized comparison."""
        matrix: np.ndarray = self._token_payload(keys)[1].reshape(keys.size, self.key_width)
        return matrix

    def key_column_block(self, keys: np.ndarray) -> v.ColumnBlock:
        tokens, payload = self._token_payload(keys)
        return v.prefixed_column(cast(list[object], tokens), payload, int(keys.size), self.key_width)

    def partition_of(self, keys: np.ndarray) -> np.ndarray:
        if self.config.partition_layout == CREATION_HOUR:
            start_ms = self.epoch_ms - 1000 * cast(int, self.config.seed_history_s)
            hours: np.ndarray = (self.creation_ms(keys) - start_ms) // HOUR_MS
            return hours
        fractions = (_mix(keys, _salt(self.seed, "partition")) >> np.uint64(11)) * _FRACTION
        found = np.searchsorted(self._partition_cdf, fractions, side="right")
        partitions: np.ndarray = np.minimum(found, self._partition_cdf.size - 1).astype(np.int64)
        return partitions

    def kafka_partition_of(self, keys: np.ndarray) -> np.ndarray:
        mixed = _mix(keys, _salt(self.seed, "kafka"))
        partitions: np.ndarray = (mixed % np.uint64(self.config.kafka_partitions)).astype(np.int64)
        return partitions

    def ordinals(self, batch: int, positions: np.ndarray) -> np.ndarray:
        ordinals: np.ndarray = (batch * self.rows_per_batch + positions.astype(np.int64) + 1).astype(np.uint32)
        return ordinals

    def keys_for_ids(self, ids: np.ndarray) -> np.ndarray:
        """Map row ids, seed or stream, to key ordinals."""
        out = np.empty(ids.size, dtype=np.int64)
        seed_rows = ids < 0
        seed_keys = -ids[seed_rows] - 1
        stream_ids = ids[~seed_rows]
        batches, positions = stream_ids // c.ID_BLOCK, stream_ids % c.ID_BLOCK
        if (seed_keys >= self.config.seed_keys).any() or (
            (positions >= self.rows_per_batch) | (batches >= self.batch_count)
        ).any():
            raise ValueError("an id names no corpus row")
        out[seed_rows] = seed_keys
        if self._key_map is not None:
            out[~seed_rows] = self._key_map[batches * self.rows_per_batch + positions]
            return out
        stream_out = np.empty(stream_ids.size, dtype=np.int64)
        for batch in np.unique(batches).tolist():
            selected = batches == batch
            stream_out[selected] = self.batch_keys(batch)[positions[selected]]
        out[~seed_rows] = stream_out
        return out

    def last_ordinals(self, batches: int | None = None) -> np.ndarray:
        """Each key's last stream ordinal over the first ``batches`` (default all); 0 keeps the seed version."""
        batches = self.batch_count if batches is None else batches
        last = np.zeros(self.keys_created_before(batches), dtype=np.uint32)
        for batch in range(batches):
            unique, first = np.unique(self.batch_keys(batch)[::-1], return_index=True)
            last[unique] = self.ordinals(batch, self.rows_per_batch - 1 - first)
        return last

    def ids_for_ordinals(self, ordinals: np.ndarray, keys: np.ndarray) -> np.ndarray:
        """Row ids of stamped versions: stream ids for positive ordinals, seed ids for 0."""
        stream = ordinals.astype(np.int64) - 1
        ids: np.ndarray = np.where(
            ordinals > 0,
            (stream // self.rows_per_batch) * c.ID_BLOCK + stream % self.rows_per_batch,
            -(keys.astype(np.int64) + 1),
        )
        return ids

    def live_truth(self, batches: int | None = None) -> LiveTruth:
        last = self.last_ordinals(batches)
        checksum = 0
        for start in range(0, last.size, _KEY_CHUNK):
            chunk = last[start : start + _KEY_CHUNK]
            ids = self.ids_for_ordinals(chunk, np.arange(start, start + chunk.size, dtype=np.int64))
            checksum = (checksum + int(np.mod(ids, c.P).sum())) % c.P
        return LiveTruth(int(last.size), checksum)

    def expected_partition_weights(self) -> np.ndarray:
        """Row share per table partition: the key Zipf, plus each hot component's pinned keys."""
        zipf = np.diff(self._partition_cdf, prepend=0.0)
        hot = np.zeros_like(zipf)
        hot_rows = 0
        for index, cdf in self._hot_cdfs.items():
            count = self.component_counts[1 + index]
            hot_rows += count
            np.add.at(hot, self.partition_of(self.hot_key(np.arange(cdf.size))), np.diff(cdf, prepend=0.0) * count)
        if hot_rows == 0:
            return zipf
        share = hot_rows / self.rows_per_batch
        weights: np.ndarray = (1 - share) * zipf + share * hot / hot_rows
        return weights
