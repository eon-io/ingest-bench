# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Wait until the engine has committed an upsert run's Kafka seed, and hold it to the freshness bound.

The scored offer starts only after the seed is in the table. An engine that buffered the seed and
wrote it late could start the offer from a layout no freshness-bound engine could build, so the
seed is scored like the stream: a seed batch is covered once every one of its keys is in a
committed data file, and lag on the commit clock must meet the same post-warmup bound. Whether
every seed key holds exactly one live row is checked after the run.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

import numpy as np
from pyiceberg.exceptions import NoSuchTableError

from ingest_bench.clock import now_ms
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.producer import publish_log
from ingest_bench.scorer import freshness
from ingest_bench.scorer.snapshots import (
    TableHistoryError,
    added_files,
    current_snapshot_ids,
    load_table,
    read_metadata,
    snapshots_in_order,
    visible_snapshot_ids,
)
from ingest_bench.scorer.tally import read_id_columns

SEED_FRESHNESS_FILE = "seed_freshness.json"
GRID_MS = 1000
READ_THREADS = 8


class Clock(Protocol):
    def monotonic(self) -> float: ...

    def sleep(self, seconds: float) -> None: ...


@dataclass(frozen=True)
class SeedLoaded:
    rows: int
    snapshot_id: int
    waited_s: float
    freshness: freshness.FreshnessResult
    # The most any seed batch's first acknowledgement trailed its schedule. Past the run's limit
    # the producer, not the engine, set the seed's pace, so its freshness judges nothing.
    producer_behind_ms: int
    producer_bound: bool


class SeedCoverage:
    """Which seed keys committed data files hold, and the longest prefix of fully covered seed batches."""

    def __init__(self, seed_keys: int, rows_per_batch: int, batches: int) -> None:
        self.seed_keys, self.rows_per_batch = seed_keys, rows_per_batch
        self.seen = np.zeros(seed_keys, dtype=bool)
        self.counts = np.zeros(batches, dtype=np.int64)
        self.expected = np.full(batches, rows_per_batch, dtype=np.int64)
        self.expected[-1] = seed_keys - rows_per_batch * (batches - 1)

    def add_ids(self, ids: np.ndarray) -> None:
        keys = -ids[ids < 0] - 1
        if (keys >= self.seed_keys).any():
            raise ValueError(f"the table holds a seed id beyond the corpus's {self.seed_keys} seed keys")
        fresh = np.unique(keys[~self.seen[keys]])
        self.seen[fresh] = True
        self.counts += np.bincount(fresh // self.rows_per_batch, minlength=self.counts.size)

    def prefix(self) -> int:
        """The last seed batch through which every batch is covered, or -1."""
        short = np.flatnonzero(self.counts < self.expected)
        return int(short[0]) - 1 if short.size else self.counts.size - 1

    def rows(self) -> int:
        return int(self.counts.sum())


class SeedTally:
    """Seed coverage and freshness observations across polls of one table.

    Observations follow the scorer: a snapshot counts from the time its commit became visible, and a
    table that was replaced, or whose counted snapshots left its history, cannot support the tally.
    """

    def __init__(self, corpus_uri: str) -> None:
        meta = corpus_metadata.read(corpus_uri)
        if meta.schedule is None:
            raise ValueError(f"{corpus_uri} is not an upsert corpus")
        self.records = corpus_metadata.read_seed_manifest(corpus_uri)
        self.coverage = SeedCoverage(meta.schedule.config.seed_keys, self.records[0].rows, len(self.records))
        self.observations: list[freshness.Observation] = []
        self.tallied: set[int] = set()
        self.visible: set[int] = set()
        self.seen_logs: set[str] = set()
        self.table_uuid: str | None = None
        self.last_metadata_location: str | None = None

    def complete(self) -> bool:
        return self.coverage.prefix() == len(self.records) - 1

    def poll(self, props: dict[str, str], table_name: str) -> int | None:
        """Read the data files of commits not yet tallied; return the current snapshot's id."""
        try:
            table = load_table(props, table_name)
        except NoSuchTableError:
            return None
        document = read_metadata(table)
        table_uuid = str(document.table_uuid)
        if self.table_uuid not in (None, table_uuid):
            raise TableHistoryError(f"{table_name} was replaced while the seed was loading")
        self.table_uuid = table_uuid
        ancestry = current_snapshot_ids(document)
        if self.tallied - ancestry:
            raise TableHistoryError(f"a seed snapshot the barrier counted left the table's history: {table_name}")
        previous = self.last_metadata_location
        if previous not in (None, table.metadata_location) and previous not in {
            entry.metadata_file for entry in document.metadata_log
        }:
            raise TableHistoryError(
                "the metadata log no longer reaches the previous poll's document; raise "
                "write.metadata.previous-versions-max or poll more often"
            )
        self.visible |= visible_snapshot_ids(document, table.io, self.seen_logs)
        self.last_metadata_location = table.metadata_location
        pending = [
            info
            for info in snapshots_in_order(document)
            if info.snapshot_id in ancestry and info.snapshot_id not in self.tallied
        ]
        # A commit's snapshots became visible together, at the timestamp of the one that ended it.
        visible_ms: dict[int, int] = {}
        group: list[int] = []
        for info in pending:
            group.append(info.snapshot_id)
            if info.snapshot_id in self.visible or info is pending[-1]:
                visible_ms.update(dict.fromkeys(group, info.timestamp_ms))
                group = []
        seen_ms = now_ms()
        for info in pending:
            files = added_files(document, info.snapshot_id, table.io)
            self.coverage.add_ids(read_id_columns([(file.path, file.file_format) for file in files], READ_THREADS))
            observation = freshness.Observation(visible_ms[info.snapshot_id], seen_ms, self.coverage.prefix())
            self.observations.append(observation)
            self.tallied.add(info.snapshot_id)
        return document.current_snapshot_id


def wait_for_seed(
    props: dict[str, str],
    table_name: str,
    corpus_uri: str,
    publish_logs_uri: str,
    *,
    warmup_s: int,
    bound_s: float,
    max_bound_s: float | None = None,
    timeout_s: float,
    poll_s: float,
    behind_max_ms: int,
    clock: Clock = time,
) -> SeedLoaded:
    tally = SeedTally(corpus_uri)
    batches = len(tally.records)
    started = clock.monotonic()
    while True:
        snapshot_id = tally.poll(props, table_name)
        waited = clock.monotonic() - started
        if tally.complete() and snapshot_id is not None:
            break
        if waited >= timeout_s:
            coverage = tally.coverage
            raise TimeoutError(
                f"{table_name} covers {coverage.prefix() + 1} of {batches} seed batches "
                f"({coverage.rows()} of {coverage.seed_keys} seed rows) after {waited:.0f}s"
            )
        clock.sleep(poll_s)
    # The logs can trail the table. Without every batch's record, a batch's delay and the producer's
    # pace would be judged from the records that happened to arrive.
    expected = set(range(batches))
    while True:
        published = publish_log.read_all(publish_logs_uri)
        logged = expected & {record.batch for record in published}
        if logged == expected:
            break
        if clock.monotonic() - started >= timeout_s:
            raise TimeoutError(
                f"publish logs record {len(logged)} of {batches} seed batches after {clock.monotonic() - started:.0f}s"
            )
        clock.sleep(poll_s)
    result = freshness.freshness_result(
        tally.observations,
        publish_log.emit_times(published),
        epoch_ms=min((record.scheduled_ms for record in published), default=tally.observations[0].timestamp_ms),
        end_ms=tally.observations[-1].timestamp_ms,
        last_batch=batches - 1,
        warmup_s=warmup_s,
        bound_s=bound_s,
        max_bound_s=max_bound_s,
        grid_ms=GRID_MS,
    )
    behind = publish_log.behind_ms(published)
    return SeedLoaded(tally.coverage.rows(), snapshot_id, waited, result, behind, behind > behind_max_ms)
