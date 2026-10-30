# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from pathlib import Path

import numpy as np
import pytest

from ingest_bench.corpus import columns as c
from ingest_bench.corpus import generate, preset, upsert
from ingest_bench.scorer.keycoverage import KeyCoverage, upsert_exactness

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
SETS = ["offered_bytes_per_s=200KB", "duration_s=6", "partition_count=8", "upsert.seed_keys=5000"]


@pytest.fixture
def coverage() -> KeyCoverage:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=SETS)
    schedule = upsert.KeySchedule.for_preset(p, seed=3)
    rows = schedule.rows_per_batch
    records = [
        generate.BatchRecord(b, b * 1000, rows, b * c.ID_BLOCK, b * c.ID_BLOCK + rows - 1, 0, 0, 0, "", "", {})
        for b in range(schedule.batch_count)
    ]
    return KeyCoverage(schedule, records)


def batch_ids(coverage: KeyCoverage, batch: int) -> np.ndarray:
    return batch * c.ID_BLOCK + np.arange(coverage.schedule.rows_per_batch, dtype=np.int64)


def last_versions(coverage: KeyCoverage, batch: int) -> np.ndarray:
    """Only the newest version of each key in the batch: what a collapsing engine writes."""
    keys = coverage.schedule.batch_keys(batch)
    _, first = np.unique(keys[::-1], return_index=True)
    return batch * c.ID_BLOCK + (keys.size - 1 - first).astype(np.int64)


def test_every_version_covers_the_batch(coverage: KeyCoverage) -> None:
    coverage.add_ids(batch_ids(coverage, 0))
    assert coverage.prefix() == 0
    assert coverage.committed_rows() == coverage.schedule.rows_per_batch


def test_collapse_covers_the_batch(coverage: KeyCoverage) -> None:
    collapsed = last_versions(coverage, 0)
    assert collapsed.size < coverage.schedule.rows_per_batch
    coverage.add_ids(collapsed)
    assert coverage.prefix() == 0


def test_a_missing_final_version_blocks_the_prefix(coverage: KeyCoverage) -> None:
    ids = batch_ids(coverage, 0)
    keys = coverage.schedule.batch_keys(0)
    _, first = np.unique(keys[::-1], return_index=True)
    final = keys.size - 1 - first
    coverage.add_ids(np.delete(ids, final[0]))
    assert coverage.prefix() == -1
    coverage.add_ids(ids[final[:1]])
    assert coverage.prefix() == 0


def test_a_newer_version_in_a_later_batch_covers_an_earlier_one(coverage: KeyCoverage) -> None:
    coverage.add_ids(batch_ids(coverage, 1))
    keys_0 = set(coverage.schedule.batch_keys(0).tolist())
    keys_1 = set(coverage.schedule.batch_keys(1).tolist())
    assert coverage.prefix() == -1 or keys_0 <= keys_1
    coverage.add_ids(batch_ids(coverage, 0))
    assert coverage.prefix() == 1


def test_rewrites_and_seed_rows_change_nothing(coverage: KeyCoverage) -> None:
    coverage.add_ids(batch_ids(coverage, 0))
    stamp = coverage.stamp.copy()
    coverage.add_ids(batch_ids(coverage, 0)[:100])
    coverage.add_ids(-np.arange(1, 5001, dtype=np.int64))
    assert np.array_equal(coverage.stamp, stamp) and coverage.invalid_rows == 0


def test_ids_outside_the_corpus_are_invalid(coverage: KeyCoverage) -> None:
    rows = coverage.schedule.rows_per_batch
    coverage.add_ids(np.array([rows, 99 * c.ID_BLOCK, -5001], dtype=np.int64))
    assert coverage.invalid_rows == 3


def test_exactness_counts_loss_and_unoffered_rows(coverage: KeyCoverage) -> None:
    for batch in range(4):
        coverage.add_ids(batch_ids(coverage, batch))
    passing = upsert_exactness(coverage, offered_batches={0, 1, 2, 3})
    assert passing["verdict"] is True and passing["loss_rows"] == 0 and passing["mode"] == "LATEST_PER_KEY"
    failing = upsert_exactness(coverage, offered_batches={0, 1, 2, 3, 4})
    assert failing["loss_rows"] == coverage.schedule.rows_per_batch and failing["verdict"] is False
    unoffered = upsert_exactness(coverage, offered_batches={0, 1, 2})
    assert unoffered["unoffered_rows"] == coverage.schedule.rows_per_batch and unoffered["verdict"] is False


def test_a_batch_covered_by_later_versions_counts_without_new_rows(coverage: KeyCoverage) -> None:
    ids = batch_ids(coverage, 1)
    keys = coverage.schedule.batch_keys(1)
    later = set(coverage.schedule.batch_keys(2).tolist())
    superseded = np.isin(keys, list(later))
    assert superseded.any()
    # Batch 1 arrives without the versions batch 2 replaces, so it waits on batch 2.
    coverage.add_ids(ids[~superseded])
    assert coverage.committed_rows() == 0
    coverage.add_ids(batch_ids(coverage, 2))
    # Batch 0 still blocks the prefix, but batches 1 and 2 are complete.
    assert coverage.prefix() == -1
    assert coverage.committed_rows() == 2 * coverage.schedule.rows_per_batch
