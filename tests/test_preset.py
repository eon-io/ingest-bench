# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from pathlib import Path

import pytest
import yaml

from ingest_bench.corpus import DEFAULT_ROW_BLOCK
from ingest_bench.corpus import preset as p
from ingest_bench.corpus import upsert as u

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


def test_parse_quantity() -> None:
    assert p.parse_quantity("100MB") == 100_000_000
    assert p.parse_quantity("5MB") == 5_000_000
    assert p.parse_quantity("64MiB") == 64 * 1024 * 1024
    assert p.parse_quantity("2GB") == 2_000_000_000
    assert p.parse_quantity("12345") == 12345
    with pytest.raises(ValueError):
        p.parse_quantity("5 bananas")


def test_smoke_preset_shape() -> None:
    preset = p.load_preset("smoke", workloads_dir=WORKLOADS)
    assert preset.schema_name == "events"
    assert preset.offered_bytes_per_s == 5_000_000
    assert preset.batch_count == 300
    assert preset.batch_bytes == 5_000_000
    assert preset.kafka_key_columns == ("user_id", "partition_key")
    assert [c.name for c in preset.columns[:2]] == ["id", "partition_key"]


@pytest.mark.parametrize(
    "name",
    [
        "events-1mrps-uniform",
        "events-1mrps-skew",
    ],
)
def test_shipped_presets_load(name: str) -> None:
    preset = p.load_preset(name, workloads_dir=WORKLOADS)
    assert preset.duration_s == 3600
    assert preset.partition_count == 512
    assert preset.alpha in (0.0, 1.0)


def test_set_overrides_change_hash_and_values() -> None:
    base = p.load_preset("smoke", workloads_dir=WORKLOADS)
    changed = p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["alpha=0.5", "duration_s=30"])
    assert changed.alpha == 0.5 and changed.duration_s == 30 and changed.batch_count == 30
    assert p.corpus_hash(base, seed=1, row_block=1024) != p.corpus_hash(changed, seed=1, row_block=1024)
    assert p.corpus_dir_name(changed, seed=1, row_block=1024).startswith("smoke-")
    with pytest.raises(ValueError, match="unknown preset key"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["colour=blue"])


def test_column_override_via_set() -> None:
    preset = p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.country.alpha=0.0"])
    assert next(c for c in preset.columns if c.name == "country").alpha == 0.0


def test_hash_is_stable_and_ignores_nothing_relevant() -> None:
    a = p.load_preset("smoke", workloads_dir=WORKLOADS)
    b = p.load_preset(str(WORKLOADS / "presets" / "smoke.yaml"), workloads_dir=WORKLOADS)
    assert p.corpus_hash(a, seed=1, row_block=1024) == p.corpus_hash(b, seed=1, row_block=1024)
    assert len(p.corpus_hash(a, seed=1, row_block=1024)) == 8


# Published results validate against these identities (seed 1, default row block).
PUBLISHED_CORPUS_HASHES = {
    "events-1mrps-skew": "59c2b4a1",
    "events-1mrps-uniform": "291a5072",
    "events200-1mrps-skew": "59c2b4a1",
    "events200-1mrps-uniform": "291a5072",
    "smoke": "e91ab1b6",
    "smoke-json": "8f62fca3",
}


@pytest.mark.parametrize("name", sorted(PUBLISHED_CORPUS_HASHES))
def test_published_preset_identities_are_unchanged(name: str) -> None:
    loaded = p.load_preset(name, workloads_dir=WORKLOADS)
    assert p.corpus_hash(loaded, seed=1, row_block=DEFAULT_ROW_BLOCK) == PUBLISHED_CORPUS_HASHES[name]


@pytest.mark.parametrize("distribution", ["skew", "uniform"])
def test_standard_preset_alias_preserves_generated_content(distribution: str) -> None:
    current = p.load_preset(f"events-1mrps-{distribution}", workloads_dir=WORKLOADS)
    historical = p.load_preset(f"events200-1mrps-{distribution}", workloads_dir=WORKLOADS)
    assert current.name == f"events-1mrps-{distribution}"
    assert historical.name == f"events200-1mrps-{distribution}"
    assert current.schema_name == historical.schema_name == "events200"
    assert p.effective_dict(current) | {"name": historical.name} == p.effective_dict(historical)
    assert p.corpus_hash(current, seed=1, row_block=DEFAULT_ROW_BLOCK) == p.corpus_hash(
        historical, seed=1, row_block=DEFAULT_ROW_BLOCK
    )


@pytest.mark.parametrize("name", ["events-100mbs-skew", "events-600mbs-uniform", "events-100mbs-upsert-cdc"])
def test_retired_full_scale_presets_do_not_load(name: str) -> None:
    with pytest.raises(FileNotFoundError):
        p.load_preset(name, workloads_dir=WORKLOADS)


def test_corpus_identity_includes_generator_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    preset = p.load_preset("smoke", workloads_dir=WORKLOADS)
    original = p.corpus_hash(preset, seed=1, row_block=1024)
    assert original != p.corpus_hash(preset, seed=2, row_block=1024)
    assert original != p.corpus_hash(preset, seed=1, row_block=64)
    assert p.corpus_dir_name(preset, seed=1, row_block=1024) != p.corpus_dir_name(preset, seed=2, row_block=1024)
    monkeypatch.setattr(p, "GENERATOR_VERSION", "next")
    assert original != p.corpus_hash(preset, seed=1, row_block=1024)


def test_key_columns_must_exist() -> None:
    with pytest.raises(ValueError, match="kafka_key_columns"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["kafka_key_columns=[nope]"])


def test_key_column_must_be_a_string_column() -> None:
    with pytest.raises(ValueError, match="string"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["kafka_key_columns=[id]"])


def test_an_event_time_finer_than_its_batch_window_is_refused() -> None:
    """A batch window of N milliseconds supports at most N distinct millisecond
    timestamps, regardless of a larger declared cardinality.
    """
    at_the_window = p.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.event_time.cardinality=1000"]
    )
    assert next(c for c in at_the_window.columns if c.name == "event_time").cardinality == 1000
    with pytest.raises(ValueError, match="event_time"):
        p.load_preset("smoke", workloads_dir=WORKLOADS, overrides=["column_overrides.event_time.cardinality=1001"])
    # The window is a preset key, so a batch long enough for the declaration
    # is the other half of the answer.
    p.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["column_overrides.event_time.cardinality=2000", "batch_interval_ms=2000"],
    )


UPSERT_BLOCK = {
    "key_column": "event_id",
    "kafka_partitions": 16,
    "seed_keys": 5000,
    "new_key_fraction": 0.15,
    "updates": [
        {"share": 0.75, "target": "recent", "age": {"distribution": "exponential", "mean_s": 300, "max_s": 3600}},
        {"share": 0.20, "target": "uniform"},
        {"share": 0.05, "target": "hot", "keys": 100, "alpha": 1.0},
    ],
}


def _write_upsert_preset(tmp_path: Path, block: dict[str, object], *, with_key: bool = True) -> Path:
    raw = yaml.safe_load((WORKLOADS / "presets" / "smoke.yaml").read_text())
    if with_key:
        raw["kafka_key_columns"] = ["event_id", "partition_key"]
    raw["upsert"] = block
    path = tmp_path / "up.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_upsert_block_parses_and_round_trips(tmp_path: Path) -> None:
    loaded = p.load_preset(str(_write_upsert_preset(tmp_path, UPSERT_BLOCK)), workloads_dir=WORKLOADS)
    assert loaded.upsert is not None and loaded.upsert.seed_keys == 5000
    assert [update.target for update in loaded.upsert.updates] == ["recent", "uniform", "hot"]
    rebuilt = p.preset_from_effective(p.effective_dict(loaded), loaded.schema_name)
    assert rebuilt.upsert == loaded.upsert


def test_upsert_block_changes_the_hash_and_append_presets_omit_it(tmp_path: Path) -> None:
    assert "upsert" not in p.effective_dict(p.load_preset("smoke", workloads_dir=WORKLOADS))
    path = str(_write_upsert_preset(tmp_path, UPSERT_BLOCK))
    loaded = p.load_preset(path, workloads_dir=WORKLOADS)
    changed = p.load_preset(path, workloads_dir=WORKLOADS, overrides=["upsert.seed_keys=6000"])
    assert changed.upsert is not None and changed.upsert.seed_keys == 6000
    assert p.corpus_hash(loaded, 1, row_block=64) != p.corpus_hash(changed, 1, row_block=64)


def test_upsert_key_column_joins_the_kafka_keys(tmp_path: Path) -> None:
    path = _write_upsert_preset(tmp_path, UPSERT_BLOCK, with_key=False)
    assert p.load_preset(str(path), workloads_dir=WORKLOADS).kafka_key_columns[0] == "event_id"


@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"key_column": "user_id"}, "role entity"),
        ({"new_key_fraction": 0.0}, "new_key_fraction"),
        ({"updates": [{"share": 0.5, "target": "uniform"}]}, "sum to 1"),
        ({"updates": [{"share": 1.0, "target": "sideways"}]}, "unknown update target"),
        ({"updates": [{"share": 1.0, "target": "hot", "keys": 6000, "alpha": 1.0}]}, "hot keys"),
        ({"updates": [{"share": 1.0, "target": "recent"}]}, "age"),
        ({"kafka_partitions": 0}, "kafka_partitions"),
        ({"spare": 1}, "unknown keys"),
    ],
)
def test_upsert_block_rejects_bad_knobs(tmp_path: Path, change: dict[str, object], message: str) -> None:
    with pytest.raises(ValueError, match=message):
        p.load_preset(str(_write_upsert_preset(tmp_path, {**UPSERT_BLOCK, **change})), workloads_dir=WORKLOADS)


def test_smoke_upsert_preset_matches_its_schedule() -> None:
    loaded = p.load_preset("smoke-upsert", workloads_dir=WORKLOADS)
    assert loaded.upsert is not None
    schedule = u.KeySchedule.for_preset(loaded, seed=1)
    assert schedule.rows_per_batch > 0
    assert schedule.total_keys == loaded.upsert.seed_keys + loaded.batch_count * schedule.new_per_batch
    assert [update.target for update in loaded.upsert.updates] == ["recent", "uniform", "hot"]
