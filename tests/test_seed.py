# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
from pathlib import Path
from typing import cast

import numpy as np
import pytest
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.table import StaticTable

from ingest_bench import uri
from ingest_bench.corpus import cli, generate, preset, seed, values
from ingest_bench.corpus import metadata as corpus_metadata

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
SETS = ["offered_bytes_per_s=200KB", "duration_s=2", "partition_count=8", "upsert.seed_keys=3000"]


@pytest.fixture
def corpus(tmp_path: Path) -> str:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=SETS)
    generate.generate(p, str(tmp_path), seed=3, row_block=64)
    return uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=3, row_block=64))


def test_seed_table_holds_every_seed_key_once(corpus: str) -> None:
    for index in range(3):
        seed.write_seed_shard(corpus, index, 3, target_file_bytes=64 * 1024)
    facts = seed.commit_seed(corpus, target_file_bytes=64 * 1024)
    assert facts["rows"] == 3000
    assert json.loads(uri.read_text(uri.join(corpus, seed.SEED_FILE))) == facts
    table = StaticTable.from_metadata(str(facts["metadata_location"]))
    assert table.properties.get("schema.name-mapping.default")
    rows = table.scan().to_arrow()
    ids = rows.column("id").to_numpy()
    assert np.array_equal(np.sort(ids), np.arange(-3000, 0))
    schedule = corpus_metadata.read(corpus).schedule
    assert schedule is not None
    keys = -ids - 1
    assert rows.column("event_id").to_pylist() == schedule.tokens(keys)
    labels = [values.partition_label(key) for key in schedule.partition_of(keys).tolist()]
    assert rows.column("partition_key").to_pylist() == labels


def test_small_target_rolls_files(corpus: str) -> None:
    seed.write_seed_shard(corpus, 0, 1, target_file_bytes=1)
    facts = seed.commit_seed(corpus, target_file_bytes=1)
    assert facts["rows"] == 3000 and cast(int, facts["files"]) >= 8


def test_more_shards_than_partitions_commits(corpus: str) -> None:
    for index in range(10):
        seed.write_seed_shard(corpus, index, 10, target_file_bytes=1 << 30)
    facts = seed.commit_seed(corpus, target_file_bytes=1 << 30)
    assert facts["rows"] == 3000 and facts["files"] == 8


def test_commit_refuses_a_missing_shard(corpus: str) -> None:
    seed.write_seed_shard(corpus, 0, 2, target_file_bytes=1 << 30)
    with pytest.raises(ValueError, match="incomplete"):
        seed.commit_seed(corpus, target_file_bytes=1 << 30)


def test_commit_refuses_a_shard_that_lost_rows(corpus: str) -> None:
    for index in range(2):
        seed.write_seed_shard(corpus, index, 2, target_file_bytes=1 << 30)
    shard_uri = uri.join(corpus, seed.SEED_DIR, "shards", "1.json")
    uri.write_text(shard_uri, json.dumps({"shard_index": 1, "shard_count": 2, "files": []}))
    with pytest.raises(ValueError, match="seed rows"):
        seed.commit_seed(corpus, target_file_bytes=1 << 30)


def test_nested_schemas_are_refused() -> None:
    kinds = preset.load_preset("kinds", workloads_dir=WORKLOADS)
    with pytest.raises(ValueError, match="flat scalar"):
        seed.check_seed_columns(kinds.columns)


def test_cli_writes_and_commits_the_seed(corpus: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["seed", "--corpus", corpus]) == 0
    assert cli.main(["seed-commit", "--corpus", corpus]) == 0
    assert "committed 3000 seed rows" in capsys.readouterr().out


def test_a_published_seed_cannot_be_rewritten(corpus: str) -> None:
    seed.write_seed_shard(corpus, 0, 1, target_file_bytes=1 << 30)
    seed.commit_seed(corpus, target_file_bytes=1 << 30)
    with pytest.raises(ValueError, match="already published"):
        seed.write_seed_shard(corpus, 0, 1, target_file_bytes=1)


def test_the_seed_commit_reads_storage_through_the_environment_endpoint(
    corpus: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The harness container reaches its object store through AWS_ENDPOINT_URL, which fsspec honours."""
    seed.write_seed_shard(corpus, 0, 1, target_file_bytes=1 << 30)
    created: list[dict[str, str]] = []
    real = SqlCatalog

    def recording(name: str, **properties: str) -> object:
        created.append(properties)
        return real(name, **properties)

    monkeypatch.setattr(seed, "SqlCatalog", recording)
    seed.commit_seed(corpus, target_file_bytes=1 << 30)
    assert created[0]["py-io-impl"] == "pyiceberg.io.fsspec.FsspecFileIO"


KAFKA_SETS = [*SETS, "upsert.seed_delivery=kafka", "upsert.key_layout=time_ordered", "upsert.seed_history_s=600"]


@pytest.fixture
def kafka_corpus(tmp_path: Path) -> str:
    p = preset.load_preset("smoke-upsert", workloads_dir=WORKLOADS, overrides=KAFKA_SETS)
    generate.generate(p, str(tmp_path), seed=3, row_block=64)
    return uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=3, row_block=64))


def test_a_kafka_seed_is_published_as_producer_batches(kafka_corpus: str) -> None:
    for index in range(3):
        seed.write_seed_shard(kafka_corpus, index, 3)
    facts = seed.commit_seed(kafka_corpus)
    assert facts["delivery"] == "kafka" and facts["rows"] == 3000
    records = corpus_metadata.read_seed_manifest(kafka_corpus)
    schedule = corpus_metadata.read(kafka_corpus).schedule
    assert schedule is not None
    assert [record.batch for record in records] == list(range(len(records))) == list(range(cast(int, facts["batches"])))
    assert facts["encoded_bytes"] == sum(record.encoded_bytes for record in records)
    assert [record.offset_ms for record in records] == [
        batch * schedule.batch_interval_ms for batch in range(len(records))
    ]
    assert sum(record.rows for record in records) == 3000
    # Every seed key appears once, at its seed id, and later batches hold later keys.
    ids = np.concatenate([np.arange(record.id_min, record.id_max + 1) for record in records])
    assert np.array_equal(np.sort(ids), np.arange(-3000, 0))
    assert [record.id_max for record in records] == sorted((record.id_max for record in records), reverse=True)
    meta = corpus_metadata.read(kafka_corpus)
    raw = json.loads(uri.read_text(uri.join(kafka_corpus, "corpus.json")))
    for record in records:
        data = uri.read_bytes(record.uri)
        keys = {name: uri.read_bytes(path) for name, path in record.key_uris.items()}
        generate.verify_segmented_batch(
            record,
            data,
            keys,
            preset.preset_from_effective(raw["effective_preset"], meta.schema_name),
            schedule,
            batch_keys=seed.seed_batch_keys(schedule, record.batch),
        )
    assert not uri.exists(uri.join(kafka_corpus, seed.SEED_DIR, "table"))


def test_a_kafka_seed_commit_refuses_a_missing_shard(kafka_corpus: str) -> None:
    seed.write_seed_shard(kafka_corpus, 0, 2)
    with pytest.raises(ValueError, match="incomplete"):
        seed.commit_seed(kafka_corpus)


def test_cli_publishes_a_kafka_seed(kafka_corpus: str, capsys: pytest.CaptureFixture[str]) -> None:
    assert cli.main(["seed", "--corpus", kafka_corpus]) == 0
    assert cli.main(["seed-commit", "--corpus", kafka_corpus]) == 0
    assert "published 3000 seed rows in" in capsys.readouterr().out
