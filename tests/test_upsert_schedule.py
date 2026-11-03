# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from pathlib import Path

import numpy as np
import pytest

from ingest_bench.corpus import columns as c
from ingest_bench.corpus import preset as p
from ingest_bench.corpus import upsert as u

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


def schedule(*extra: str) -> u.KeySchedule:
    sets = ["offered_bytes_per_s=200KB", "duration_s=20", "partition_count=8", "upsert.seed_keys=5000", *extra]
    return u.KeySchedule.for_preset(p.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=sets), seed=3)


def test_rows_per_batch_is_exact_and_counts_fill_the_batch() -> None:
    s = schedule()
    assert s.rows_per_batch == 200_000 // 256
    assert s.new_per_batch == round(s.rows_per_batch * 0.15)
    assert sum(s.component_counts) == s.rows_per_batch


def test_batch_keys_are_deterministic_and_read_only() -> None:
    first, second = schedule(), schedule()
    assert np.array_equal(first.batch_keys(7), second.batch_keys(7))
    assert not first.batch_keys(7).flags.writeable


def test_new_keys_are_dense_and_in_row_order() -> None:
    s = schedule()
    for batch in (0, 5):
        keys = s.batch_keys(batch)
        created = keys[keys >= s.keys_created_before(batch)]
        assert np.array_equal(created, np.arange(s.keys_created_before(batch), s.keys_created_before(batch + 1)))


def test_no_row_updates_a_key_created_in_its_own_batch_or_later() -> None:
    s = schedule()
    for batch in range(s.batch_count):
        keys = s.batch_keys(batch)
        created_here = (keys >= s.keys_created_before(batch)) & (keys < s.keys_created_before(batch + 1))
        assert created_here.sum() == s.new_per_batch
        assert (keys < s.keys_created_before(batch + 1)).all()


def test_hot_keys_concentrate_and_are_distinct_seed_keys() -> None:
    s = schedule()
    ranks = np.arange(1000)
    hot = s.hot_key(ranks)
    assert np.unique(hot).size == ranks.size and (hot < s.config.seed_keys).all()
    keys = np.concatenate([s.batch_keys(batch) for batch in range(s.batch_count)])
    assert (keys == hot[0]).sum() > (keys == hot[-1]).sum()


def test_recent_ages_follow_the_mean() -> None:
    ages = schedule().recent_ages(400)
    assert ages.min() >= 1
    assert abs(ages.mean() - 300) < 30


def test_derived_columns_are_functions_of_the_key() -> None:
    s = schedule()
    keys = np.array([0, 1, 4999, 0], dtype=np.int64)
    tokens = s.tokens(keys)
    assert tokens[0] == tokens[3] and tokens[0] != tokens[1] and all(len(token) == 32 for token in tokens)
    assert s.partition_of(keys)[0] == s.partition_of(keys)[3]
    kafka = s.kafka_partition_of(keys)
    assert ((kafka >= 0) & (kafka < 16)).all()


def test_keys_for_ids_matches_batch_keys_and_seed_ids() -> None:
    s = schedule()
    ids = np.array([3 * c.ID_BLOCK + 10, -1, -5000, 0], dtype=np.int64)
    assert s.keys_for_ids(ids).tolist() == [int(s.batch_keys(3)[10]), 0, 4999, int(s.batch_keys(0)[0])]
    with pytest.raises(ValueError, match="names no corpus row"):
        s.keys_for_ids(np.array([3 * c.ID_BLOCK + s.rows_per_batch], dtype=np.int64))
    with pytest.raises(ValueError, match="names no corpus row"):
        s.keys_for_ids(np.array([-5001], dtype=np.int64))


def test_live_truth_matches_a_brute_force_replay() -> None:
    s = schedule()
    latest = {key: -(key + 1) for key in range(s.config.seed_keys)}
    for batch in range(s.batch_count):
        for position, key in enumerate(s.batch_keys(batch).tolist()):
            latest[key] = batch * c.ID_BLOCK + position
    truth = s.live_truth()
    assert truth.rows == len(latest) == s.total_keys
    assert truth.checksum == sum(row_id % c.P for row_id in latest.values()) % c.P


def test_live_truth_of_a_batch_prefix_matches_a_brute_force_replay() -> None:
    s = schedule()
    offered = s.batch_count // 2
    latest = {key: -(key + 1) for key in range(s.config.seed_keys)}
    for batch in range(offered):
        for position, key in enumerate(s.batch_keys(batch).tolist()):
            latest[key] = batch * c.ID_BLOCK + position
    truth = s.live_truth(offered)
    assert truth.rows == len(latest) == s.keys_created_before(offered)
    assert truth.checksum == sum(row_id % c.P for row_id in latest.values()) % c.P


def test_expected_partition_weights_sum_to_one() -> None:
    weights = schedule().expected_partition_weights()
    assert weights.shape == (8,) and abs(weights.sum() - 1.0) < 1e-9


def test_a_stream_beyond_uint32_ordinals_is_refused() -> None:
    with pytest.raises(ValueError, match="uint32"):
        schedule("offered_bytes_per_s=2GB", "duration_s=3600")


def test_recent_needs_new_keys_every_batch() -> None:
    with pytest.raises(ValueError, match="new key"):
        schedule("upsert.new_key_fraction=0.0001")


def test_the_key_map_answers_like_generation(tmp_path: Path) -> None:
    generated, mapped = schedule(), schedule()
    mapped.build_key_map(tmp_path / "keys.npy")
    for batch in (0, 7, 19):
        assert np.array_equal(generated.batch_keys(batch), mapped.batch_keys(batch))
    ids = np.array([19 * c.ID_BLOCK + 5, -3, 0, 7 * c.ID_BLOCK + 100], dtype=np.int64)
    assert np.array_equal(generated.keys_for_ids(ids), mapped.keys_for_ids(ids))
    with pytest.raises(ValueError, match="names no corpus row"):
        mapped.keys_for_ids(np.array([20 * c.ID_BLOCK], dtype=np.int64))


ORDERED = ("upsert.key_layout=time_ordered", "upsert.seed_history_s=3600", "upsert.seed_delivery=kafka")


def test_unset_options_stay_out_of_the_effective_schedule() -> None:
    plain = u.effective(schedule().config)
    assert not {"key_layout", "seed_delivery", "seed_history_s"} & set(plain)
    ordered = schedule(*ORDERED).config
    recorded = u.effective(ordered)
    assert (recorded["key_layout"], recorded["seed_delivery"], recorded["seed_history_s"]) == (
        u.TIME_ORDERED,
        u.SEED_KAFKA,
        3600,
    )
    assert u.from_effective(recorded) == ordered


@pytest.mark.parametrize(
    ("setting", "message"),
    [
        ("upsert.key_layout=sorted", "key_layout"),
        ("upsert.seed_delivery=s3", "seed_delivery"),
        ("upsert.seed_history_s=0", "seed_history_s"),
    ],
)
def test_bad_options_are_refused(setting: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        schedule(setting)


def test_time_ordered_tokens_lead_with_the_creation_time() -> None:
    s = schedule(*ORDERED)
    keys = np.arange(s.total_keys, dtype=np.int64)
    tokens = s.tokens(keys)
    prefixes = [int(token[: u.TIME_PREFIX_WIDTH], 16) for token in tokens]
    assert prefixes == sorted(prefixes)
    assert prefixes == s.creation_ms(keys).tolist()
    assert len(set(tokens)) == keys.size and all(len(token) == 32 for token in tokens)
    epoch = s.creation_ms(np.array([s.config.seed_keys], dtype=np.int64))[0]
    assert s.creation_ms(np.array([0], dtype=np.int64))[0] == epoch - 3600 * 1000
    later = s.creation_ms(np.array([s.keys_created_before(3)], dtype=np.int64))[0]
    assert later == epoch + 3 * s.batch_interval_ms


def test_random_tokens_are_unchanged_by_the_new_options() -> None:
    keys = np.arange(100, dtype=np.int64)
    assert schedule().tokens(keys) == schedule("upsert.seed_delivery=kafka").tokens(keys)


def test_updates_older_than_the_stream_reach_seed_keys_created_that_long_ago() -> None:
    s = schedule(*ORDERED)
    for age_ms, picks in ((60_000, 0.0), (60_000, 0.999), (1_800_000, 0.5)):
        key = int(s.seed_key_at_age(np.array([age_ms]), np.array([picks]))[0])
        created = int(s.creation_ms(np.array([key], dtype=np.int64))[0])
        assert abs((s.creation_ms(np.array([s.config.seed_keys]))[0] - created) - age_ms) <= s.batch_interval_ms
    beyond = s.seed_key_at_age(np.array([10 * 3600 * 1000]), np.array([0.5]))
    assert beyond.tolist() == [0]
    # Batch 0's recent updates all predate the stream, so they land on the newest seed keys.
    keys = s.batch_keys(0)
    seeded = keys[(keys < s.config.seed_keys)]
    assert seeded.size and np.median(seeded) > 0.8 * s.config.seed_keys


HOURLY = (*ORDERED, "upsert.partition_layout=creation_hour", "alpha=0.0")


def test_hourly_partitions_follow_each_keys_creation_hour() -> None:
    # 3600 s of seed history and 20 s of stream span two hours.
    s = schedule(*HOURLY, "partition_count=2")
    keys = np.arange(s.total_keys, dtype=np.int64)
    partitions = s.partition_of(keys)
    start = s.creation_ms(np.array([0], dtype=np.int64))[0]
    assert np.array_equal(partitions, (s.creation_ms(keys) - start) // 3_600_000)
    assert partitions[: s.config.seed_keys].tolist() == [0] * s.config.seed_keys
    assert set(partitions[s.config.seed_keys :].tolist()) == {1}


@pytest.mark.parametrize(
    ("setting", "message"),
    [("partition_count=3", "2 hourly partitions"), ("alpha=1.0", "alpha")],
)
def test_hourly_partitions_need_a_matching_partition_count_and_no_skew(setting: str, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        schedule(*HOURLY, "partition_count=2", setting)
