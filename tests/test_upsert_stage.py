# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
from pathlib import Path

import pytest
import yaml

from ingest_bench import catalog as cat
from ingest_bench import stage, uri
from ingest_bench.corpus import generate, preset, seed
from ingest_bench.corpus import metadata as corpus_metadata
from tests.test_specs import FakeAdmin, _site_file

ROOT = Path(__file__).resolve().parents[1]
SETS = ["offered_bytes_per_s=200KB", "duration_s=2", "partition_count=8", "upsert.seed_keys=3000"]


@pytest.fixture(scope="module")
def upsert_root(tmp_path_factory: pytest.TempPathFactory) -> str:
    p = preset.load_preset("smoke-upsert", workloads_dir=ROOT / "workloads", overrides=SETS)
    root = str(tmp_path_factory.mktemp("corpora"))
    generate.generate(p, root, seed=1, row_block=64)
    corpus_uri = uri.join(root, preset.corpus_dir_name(p, seed=1, row_block=64))
    seed.write_seed_shard(corpus_uri, 0, 1, target_file_bytes=1 << 20)
    seed.commit_seed(corpus_uri, target_file_bytes=1 << 20)
    return root


def upsert_spec(tmp_path: Path, **changes: dict[str, object]) -> Path:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["corpus"] = "smoke-upsert"
    raw["kafka"] = {"partitions": 16, "key": "event_id", "partition_assignment": "corpus", "value_encoding": "avro"}
    raw.setdefault("scoring", {})["exactness"] = "LATEST_PER_KEY"
    for section, values in changes.items():
        raw.setdefault(section, {}).update(values)
    path = tmp_path / "upsert.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_stage_registers_the_run_table_over_the_seed(tmp_path: Path, upsert_root: str) -> None:
    site = _site_file(tmp_path, upsert_root)
    staged = stage.stage(upsert_spec(tmp_path), site, tmp_path / "runs", FakeAdmin(), stamp="20261006T100000Z")
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    corpus_uri = facts["corpus_uri"]
    seed_facts = json.loads(uri.read_text(uri.join(corpus_uri, "seed.json")))
    assert facts["upsert"] == {
        "key_columns": ["event_id", "partition_key"],
        "version_column": "id",
        "seed_rows": 3000,
        "seed_snapshot_id": seed_facts["snapshot_id"],
    }
    catalog = cat.open_catalog(yaml.safe_load(site.read_text())["catalog"]["props"])
    table = catalog.load_table(cat.table_identifier(facts["table"]))
    current = table.current_snapshot()
    assert current is not None and current.snapshot_id == seed_facts["snapshot_id"]
    assert table.location() != uri.join(corpus_uri, "seed", "table")
    assert sorted(table.schema().identifier_field_names()) == ["event_id", "partition_key"]
    # The seed's own write settings stay with the seed: the run's table holds only what its spec declares.
    assert "write.target-file-size-bytes" not in table.properties
    assert table.scan().to_arrow().num_rows == 3000
    catalog.drop_table(cat.table_identifier(facts["table"]))
    assert uri.exists(str(seed_facts["metadata_location"]))
    assert corpus_metadata.read(corpus_uri).schedule is not None


def test_stage_writes_a_v3_copy_on_request(tmp_path: Path, upsert_root: str) -> None:
    site = _site_file(tmp_path, upsert_root)
    spec = upsert_spec(tmp_path, table={"format_version": 3})
    staged = stage.stage(spec, site, tmp_path / "runs", FakeAdmin(), stamp="20261006T100500Z")
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    catalog = cat.open_catalog(yaml.safe_load(site.read_text())["catalog"]["props"])
    assert catalog.load_table(cat.table_identifier(facts["table"])).format_version == 3


def test_stage_accepts_a_shortened_upsert_offer(tmp_path: Path, upsert_root: str) -> None:
    site = _site_file(tmp_path, upsert_root)
    staged = stage.stage(upsert_spec(tmp_path, producer={"seconds": 1}), site, tmp_path / "runs", FakeAdmin())
    assert (staged.run_dir / "facts.json").exists()


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"kafka": {"partitions": 12}}, "spec.kafka.partitions"),
        ({"kafka": {"key": "user_id"}}, "spec.kafka.key"),
        ({"kafka": {"partition_assignment": "key"}}, "partition_assignment"),
        ({"scoring": {"exactness": "EXACTLY_ONCE"}}, "upsert corpus"),
        ({"table": {"partition": "unpartitioned"}}, "spec.table.partition"),
    ],
)
def test_stage_refuses_mismatched_upsert_settings(
    tmp_path: Path, upsert_root: str, changes: dict[str, dict[str, object]], message: str
) -> None:
    admin = FakeAdmin()
    with pytest.raises(ValueError, match=message):
        stage.stage(upsert_spec(tmp_path, **changes), _site_file(tmp_path, upsert_root), tmp_path / "runs", admin)
    assert admin.created == []


def test_append_runs_refuse_format_version_three(tmp_path: Path, upsert_root: str) -> None:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw.setdefault("table", {})["format_version"] = 3
    path = tmp_path / "append.yaml"
    path.write_text(yaml.safe_dump(raw))
    smoke = preset.load_preset("smoke", workloads_dir=ROOT / "workloads", overrides=SETS[:3])
    generate.generate(smoke, upsert_root, seed=1, row_block=64)
    with pytest.raises(ValueError, match="format_version 3"):
        stage.stage(path, _site_file(tmp_path, upsert_root), tmp_path / "runs", FakeAdmin())


@pytest.fixture(scope="module")
def kafka_seed_root(tmp_path_factory: pytest.TempPathFactory) -> str:
    sets = [*SETS, "upsert.seed_delivery=kafka", "upsert.key_layout=time_ordered", "upsert.seed_history_s=600"]
    p = preset.load_preset("smoke-upsert", workloads_dir=ROOT / "workloads", overrides=sets)
    root = str(tmp_path_factory.mktemp("kafka-seed"))
    generate.generate(p, root, seed=1, row_block=64)
    corpus_uri = uri.join(root, preset.corpus_dir_name(p, seed=1, row_block=64))
    seed.write_seed_shard(corpus_uri, 0, 1)
    seed.commit_seed(corpus_uri)
    return root


@pytest.mark.parametrize("format_version", [2, 3])
def test_a_kafka_seed_run_starts_from_an_empty_keyed_table(
    tmp_path: Path, kafka_seed_root: str, format_version: int
) -> None:
    site = _site_file(tmp_path, kafka_seed_root)
    spec = upsert_spec(tmp_path, table={"format_version": format_version})
    staged = stage.stage(spec, site, tmp_path / "runs", FakeAdmin(), stamp="20261007T100000Z")
    facts = json.loads((staged.run_dir / "facts.json").read_text())
    assert facts["upsert"] == {
        "key_columns": ["event_id", "partition_key"],
        "version_column": "id",
        "seed_rows": 3000,
        "seed_delivery": "kafka",
    }
    catalog = cat.open_catalog(yaml.safe_load(site.read_text())["catalog"]["props"])
    table = catalog.load_table(cat.table_identifier(facts["table"]))
    assert table.current_snapshot() is None and table.format_version == format_version
    assert sorted(table.schema().identifier_field_names()) == ["event_id", "partition_key"]
    assert [field.name for field in table.spec().fields] == ["partition_key"]


def test_a_kafka_seed_table_refuses_a_conflicting_format_property(tmp_path: Path, kafka_seed_root: str) -> None:
    site = _site_file(tmp_path, kafka_seed_root)
    spec = upsert_spec(tmp_path, table={"format_version": 2, "properties": {"format-version": "1"}})
    admin = FakeAdmin()
    with pytest.raises(ValueError, match="format-version"):
        stage.stage(spec, site, tmp_path / "runs", admin, stamp="20261007T110000Z")
