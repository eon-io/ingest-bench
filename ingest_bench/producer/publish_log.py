# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Record producer acknowledgements and completion, batch by batch.

The scorer uses these logs to distinguish producer delay from engine lag.
Incremental writes preserve progress if the producer stops unexpectedly.
"""

from __future__ import annotations

import json
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from pathlib import Path
from statistics import median
from typing import cast

from ingest_bench import uri

LOG_NAME_PREFIX = "publish_log-"
LOG_NAME_SUFFIX = ".jsonl"

_DONE_KEY = "done"
_ATTEMPT_KEY = "attempt"


@dataclass(frozen=True)
class PublishRecord:
    """One batch's delivery outcome.

    Use the last acknowledgement as the batch emit time for scoring.
    """

    batch: int
    scheduled_ms: int
    first_ack_ms: int
    last_ack_ms: int
    rows: int
    bytes: int
    errors: int
    # Split a late first acknowledgement: a send_start_ms after scheduled_ms means the batch was
    # still downloading or decompressing; otherwise the delay is Kafka's. Older logs lack these.
    send_start_ms: int | None = None
    fetch_ms: int | None = None
    decompress_ms: int | None = None
    # Upsert offers send every batch from every shard, each shard its own Kafka partitions:
    # a record is then one shard's slice, and the batch is offered once all ``slices`` arrive.
    shard: int | None = None
    slices: int | None = None

    def to_json(self) -> str:
        data = {key: value for key, value in asdict(self).items() if value is not None or key not in _SLICE_KEYS}
        return json.dumps(data, sort_keys=True, separators=(",", ":"))


_SLICE_KEYS = frozenset({"shard", "slices"})


def _record_from_json(raw: dict[str, object]) -> PublishRecord:
    return PublishRecord(
        batch=int(cast(int, raw["batch"])),
        scheduled_ms=int(cast(int, raw["scheduled_ms"])),
        first_ack_ms=int(cast(int, raw["first_ack_ms"])),
        last_ack_ms=int(cast(int, raw["last_ack_ms"])),
        rows=int(cast(int, raw["rows"])),
        bytes=int(cast(int, raw["bytes"])),
        errors=int(cast(int, raw["errors"])),
        send_start_ms=_optional_int(raw, "send_start_ms"),
        fetch_ms=_optional_int(raw, "fetch_ms"),
        decompress_ms=_optional_int(raw, "decompress_ms"),
        shard=_optional_int(raw, "shard"),
        slices=_optional_int(raw, "slices"),
    )


def _optional_int(raw: dict[str, object], key: str) -> int | None:
    value = raw.get(key)
    return None if value is None else int(cast(int, value))


def _parse(text: str, source: str) -> list[PublishRecord]:
    """Parse batch records, excluding completion trailers."""
    records = []
    for line in text.splitlines():
        if not line.strip():
            continue
        raw = cast(dict[str, object], json.loads(line))
        if _DONE_KEY in raw or _ATTEMPT_KEY in raw:
            continue
        try:
            records.append(_record_from_json(raw))
        except KeyError as err:
            raise ValueError(f"{source} holds a line missing key {err.args[0]!r}: {line}") from err
    return records


def append(path: Path, record: PublishRecord) -> None:
    """Append and flush a batch record to preserve incremental progress."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(record.to_json() + "\n")
        handle.flush()


def append_attempt(path: Path, shard: int) -> None:
    """Mark that a shard is about to send, before any record reaches Kafka.

    An upsert offer may be sent only once; this marker is what a relaunched shard finds.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({_ATTEMPT_KEY: True, "shard": shard}, sort_keys=True) + "\n")
        handle.flush()


def append_done(path: Path, shard: int, batches: int) -> None:
    """Append a completion trailer after the shard's final batch.

    The trailer distinguishes completion from a producer that stopped early.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({_DONE_KEY: True, "shard": shard, "batches": batches}, sort_keys=True) + "\n")
        handle.flush()


def read(path: Path) -> list[PublishRecord]:
    return _parse(path.read_text(encoding="utf-8"), str(path))


def _trailed(text: str) -> bool:
    """Whether a log's last line is the trailer rather than a batch."""
    for line in reversed(text.splitlines()):
        if line.strip():
            return _DONE_KEY in cast(dict[str, object], json.loads(line))
    return False


def shard_done(path: Path) -> bool:
    """Return whether the log has a completion trailer; absent logs are unfinished."""
    if not path.exists():
        return False
    return _trailed(path.read_text(encoding="utf-8"))


def log_names(uri_prefix: str) -> list[str]:
    return [
        name for name in uri.listdir(uri_prefix) if name.startswith(LOG_NAME_PREFIX) and name.endswith(LOG_NAME_SUFFIX)
    ]


def shard_index(name: str) -> int:
    """Extract the shard index from a log filename, rejecting malformed indices."""
    raw = name[len(LOG_NAME_PREFIX) : -len(LOG_NAME_SUFFIX)]
    if not raw.isdigit():
        raise ValueError(f"publish log {name!r} does not name a shard index")
    return int(raw)


def shards_done(uri_prefix: str) -> set[int]:
    """Return shard indices whose logs contain completion trailers."""
    if not uri.exists(uri_prefix):
        return set()
    return {shard_index(name) for name in log_names(uri_prefix) if _trailed(uri.read_text(uri.join(uri_prefix, name)))}


@dataclass(frozen=True)
class PublishLogs:
    """Offered batches, their upsert slices, and completion from the same log versions."""

    batches: list[PublishRecord]
    slices: list[PublishRecord]
    done_shards: set[int] = field(default_factory=set)


def _merge_slices(batch: int, parts: list[PublishRecord]) -> PublishRecord | None:
    """Merge one batch's slices, or return None while a shard's slice is still missing.

    The latest first and last acknowledgements stand for the batch, so the schedule check
    and the emit time both see the slowest shard.
    """
    expected = {part.slices for part in parts}
    scheduled = {part.scheduled_ms for part in parts}
    if len(expected) != 1 or len(scheduled) != 1:
        raise ValueError(f"batch {batch} slices disagree on their slice count or schedule")
    if len(parts) != cast(int, expected.pop()):
        return None
    starts = [part.send_start_ms for part in parts if part.send_start_ms is not None]
    fetches = [part.fetch_ms for part in parts if part.fetch_ms is not None]
    decompressions = [part.decompress_ms for part in parts if part.decompress_ms is not None]
    return PublishRecord(
        batch=batch,
        scheduled_ms=scheduled.pop(),
        first_ack_ms=max(part.first_ack_ms for part in parts),
        last_ack_ms=max(part.last_ack_ms for part in parts),
        rows=sum(part.rows for part in parts),
        bytes=sum(part.bytes for part in parts),
        errors=sum(part.errors for part in parts),
        send_start_ms=min(starts) if starts else None,
        fetch_ms=max(fetches) if fetches else None,
        decompress_ms=max(decompressions) if decompressions else None,
        slices=len(parts),
    )


def _read_log(log_uri: str) -> tuple[list[PublishRecord], bool]:
    text = uri.read_text(log_uri)
    return _parse(text, log_uri), _trailed(text)


def read_logs(uri_prefix: str, *, read_workers: int = 32) -> PublishLogs:
    """Read all shard logs in batch order, merging complete upsert slices into batches.

    Fetch each log once, with at most ``read_workers`` concurrent reads. Completion
    and records use the same fetched version even if a producer replaces its log.
    Reject duplicate batches or slices rather than silently altering the offered totals.
    """
    if read_workers <= 0:
        raise ValueError("publish log read_workers must be positive")
    # A prefix with nothing under it is the normal state before the first
    # upload lands, not a missing input.
    if not uri.exists(uri_prefix):
        return PublishLogs([], [])
    records: list[PublishRecord] = []
    slices: dict[int, list[PublishRecord]] = defaultdict(list)
    owner: dict[tuple[int, int | None], str] = {}
    done_shards: set[int] = set()
    names = log_names(uri_prefix)
    log_uris = [uri.join(uri_prefix, name) for name in names]
    with ThreadPoolExecutor(max_workers=read_workers) as executor:
        for name, (shard_records, done) in zip(names, executor.map(_read_log, log_uris), strict=True):
            if done:
                done_shards.add(shard_index(name))
            for record in shard_records:
                identity = (record.batch, record.shard)
                if (
                    identity in owner
                    or (record.batch, None) in owner
                    or (record.shard is None and record.batch in slices)
                ):
                    raise ValueError(
                        f"batch {record.batch} is duplicate: published in both {owner.get(identity)} and {name}"
                    )
                owner[identity] = name
                if record.shard is None:
                    records.append(record)
                else:
                    slices[record.batch].append(record)
    for batch, parts in slices.items():
        merged = _merge_slices(batch, parts)
        if merged is not None:
            records.append(merged)
    records.sort(key=lambda record: record.batch)
    flat = sorted((part for parts in slices.values() for part in parts), key=lambda part: (part.batch, part.shard or 0))
    return PublishLogs(records, flat, done_shards)


def read_all(uri_prefix: str) -> list[PublishRecord]:
    """Read all offered batches in batch order; see ``read_logs``."""
    return read_logs(uri_prefix).batches


def behind_ms(records: list[PublishRecord]) -> int:
    """Return the maximum delay from scheduled time to first acknowledgement."""
    return max((record.first_ack_ms - record.scheduled_ms for record in records), default=0)


def emit_times(records: list[PublishRecord]) -> dict[int, int]:
    return {record.batch: record.last_ack_ms for record in records}


@dataclass(frozen=True)
class ProducerWindow:
    """One shard's completed batch outcomes, grouped by their scheduled window."""

    start_ms: int
    end_ms: int
    shard: int
    batches: int
    behind_ms_median: float
    behind_ms_max: int
    errors: int


def windows(records: list[PublishRecord], *, epoch_ms: int, window_s: int, shards: int) -> list[ProducerWindow]:
    """Summarize each shard's completed batches by scheduled window.

    Shards own batch indices modulo the shard count, as in pacing.select_batches, except
    upsert slices, which name their shard. Group by scheduled time so delayed uploads can
    amend the correct window.
    """
    if window_s <= 0 or shards <= 0:
        raise ValueError("producer windows need a positive window and shard count")
    grouped: dict[tuple[int, int], list[PublishRecord]] = defaultdict(list)
    step_ms = window_s * 1000
    for record in records:
        start_ms = epoch_ms + (record.scheduled_ms - epoch_ms) // step_ms * step_ms
        grouped[start_ms, record.batch % shards if record.shard is None else record.shard].append(record)
    return [
        ProducerWindow(
            start_ms=start,
            end_ms=start + step_ms,
            shard=shard,
            batches=len(batch_records),
            behind_ms_median=float(median(record.first_ack_ms - record.scheduled_ms for record in batch_records)),
            behind_ms_max=behind_ms(batch_records),
            errors=sum(record.errors for record in batch_records),
        )
        for (start, shard), batch_records in sorted(grouped.items())
    ]
