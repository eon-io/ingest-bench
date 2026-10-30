# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Tests for declaring, registering, and validating the compaction layer."""

from collections.abc import Callable
from pathlib import Path

import pytest
import yaml

from ingest_bench.specs import compactors, model

ROOT = Path(__file__).resolve().parents[1]

SPARK_REWRITE = {"executors": 2, "executor_cores": 2, "executor_mem_mb": 2048}


def _spec(tmp_path: Path, compaction: object, base: str = "smoke-external.yaml") -> model.RunSpec:
    raw = yaml.safe_load((ROOT / "specs" / "smoke" / base).read_text())
    if compaction is not None:
        raw["compaction"] = compaction
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return model.load_run_spec(path)


def _refused(tmp_path: Path, compaction: object, message: str, base: str = "smoke-external.yaml") -> None:
    with pytest.raises(ValueError, match=message):
        _spec(tmp_path, compaction, base)


# ---------------------------------------------------------------------------
# What a spec can say about compaction
# ---------------------------------------------------------------------------


def test_a_spec_that_says_nothing_about_compaction_describes_a_run_nothing_compacts(tmp_path: Path) -> None:
    assert _spec(tmp_path, None).compaction is None


def test_an_inline_compactor_starts_nothing_and_adds_no_fleet(tmp_path: Path) -> None:
    compaction = _spec(tmp_path, {"driver": "inline"}).compaction
    assert compaction is not None
    assert compaction.is_inline() and not compaction.is_external() and not compaction.is_managed()
    assert compaction.interval_s is None and compaction.fleet == () and compaction.driver_block == {}


def test_an_external_compactor_declares_what_it_is_and_what_it_costs(tmp_path: Path) -> None:
    compaction = _spec(
        tmp_path,
        {
            "driver": "external",
            "external": {"name": "their-compactor", "version": "2.1", "notes": "runs hourly"},
            "fleet": [{"role": "worker", "count": 3, "vcpu": 4, "gib": 8, "machine_type": "m6i.xlarge"}],
        },
    ).compaction
    assert compaction is not None and compaction.is_external()
    assert compaction.external is not None and compaction.external.version == "2.1"
    assert [role.count for role in compaction.fleet] == [3]


def test_a_registered_driver_carries_its_cadence_and_its_own_block(tmp_path: Path) -> None:
    compaction = _spec(
        tmp_path, {"driver": "spark-rewrite", "interval_s": 600, "spark-rewrite": SPARK_REWRITE}
    ).compaction
    assert compaction is not None and compaction.is_managed()
    assert compaction.interval_s == 600 and compaction.driver_block["executors"] == 2
    assert compaction.external is None and compaction.fleet == ()


def test_compaction_is_a_layer_over_any_engine(tmp_path: Path) -> None:
    """The sidecar does not belong to the engine it runs beside."""
    for base in ("smoke-external.yaml", "smoke-spark.yaml", "smoke-flink.yaml"):
        spec = _spec(tmp_path, {"driver": "spark-rewrite", "interval_s": 60, "spark-rewrite": SPARK_REWRITE}, base)
        assert spec.compaction is not None and spec.compaction.driver == "spark-rewrite", base


# ---------------------------------------------------------------------------
# What it refuses
# ---------------------------------------------------------------------------


def test_an_unregistered_driver_names_the_ones_there_are(tmp_path: Path) -> None:
    _refused(tmp_path, {"driver": "broom"}, "spark-rewrite")


def test_a_compaction_block_with_no_driver_is_refused(tmp_path: Path) -> None:
    _refused(tmp_path, {"interval_s": 600}, "spec.compaction must set driver")


def test_a_misspelled_compaction_setting_cannot_silently_do_nothing(tmp_path: Path) -> None:
    _refused(tmp_path, {"driver": "inline", "intervals": 600}, "unknown")


def test_a_driver_block_belonging_to_another_driver_is_refused(tmp_path: Path) -> None:
    _refused(tmp_path, {"driver": "inline", "spark-rewrite": SPARK_REWRITE}, "unknown")


def test_a_cadence_nothing_here_sets_is_refused(tmp_path: Path) -> None:
    """An interval on a compactor the harness does not start would describe, not set."""
    for driver in ("inline", "external"):
        _refused(tmp_path, {"driver": driver, "interval_s": 600}, "would set nothing")


def test_an_inline_compactor_is_refused_a_fleet_of_its_own(tmp_path: Path) -> None:
    """Its compute is the engine's, and counting it twice would overstate the cost."""
    _refused(
        tmp_path,
        {"driver": "inline", "fleet": [{"role": "w", "count": 1, "vcpu": 1, "gib": 1, "machine_type": "laptop"}]},
        "already inside the run's own fleet",
    )
    _refused(
        tmp_path,
        {"driver": "inline", "external": {"name": "x", "version": "1", "notes": "y"}},
        "already inside the run's own fleet",
    )


def test_an_external_compactor_must_say_what_it_is_and_what_it_runs_on(tmp_path: Path) -> None:
    external = {"name": "their-compactor", "version": "2.1", "notes": "hourly"}
    fleet = [{"role": "worker", "count": 1, "vcpu": 2, "gib": 4, "machine_type": "m6i.large"}]
    _refused(tmp_path, {"driver": "external", "fleet": fleet}, "must set external")
    _refused(tmp_path, {"driver": "external", "external": external}, "must set fleet")


def test_a_registered_driver_declares_no_external_block_or_fleet(tmp_path: Path) -> None:
    """The harness renders its pods, so their sizing comes from its own block."""
    for key, value in (
        ("external", {"name": "x", "version": "1", "notes": "y"}),
        ("fleet", [{"role": "w", "count": 1, "vcpu": 1, "gib": 1, "machine_type": "laptop"}]),
    ):
        _refused(
            tmp_path,
            {"driver": "spark-rewrite", "interval_s": 600, "spark-rewrite": SPARK_REWRITE, key: value},
            f"declares no {key} block",
        )


def test_a_registered_driver_needs_a_cadence_and_a_block(tmp_path: Path) -> None:
    _refused(tmp_path, {"driver": "spark-rewrite", "spark-rewrite": SPARK_REWRITE}, "interval_s")
    _refused(tmp_path, {"driver": "spark-rewrite", "interval_s": 600}, "must set spark-rewrite")


@pytest.mark.parametrize("interval", [0, -1, True, "600"])
def test_the_cadence_must_be_a_positive_number_of_seconds(tmp_path: Path, interval: object) -> None:
    _refused(
        tmp_path,
        {"driver": "spark-rewrite", "interval_s": interval, "spark-rewrite": SPARK_REWRITE},
        "interval_s",
    )


def test_the_compaction_block_must_be_a_mapping(tmp_path: Path) -> None:
    _refused(tmp_path, ["spark-rewrite"], "spec.compaction must be a mapping")


# ---------------------------------------------------------------------------
# The registry
# ---------------------------------------------------------------------------


def test_every_registered_driver_has_the_modules_a_driver_needs() -> None:
    for driver in compactors.MANAGED:
        assert hasattr(compactors.knobs_for(driver), "render")
        assert hasattr(compactors.knobs_for(driver), "validate")
        assert hasattr(compactors.fleet_for(driver), "fleet")


def test_every_registered_driver_has_a_typed_cluster_fleet_descriptor() -> None:
    from ingest_bench.specs.kubernetes import CompactorKubernetes

    for driver in compactors.MANAGED:
        descriptor = compactors.kubernetes_for(driver)
        assert isinstance(descriptor, CompactorKubernetes)
        assert descriptor.fleet_selector and descriptor.pod_role_label and descriptor.role_map


def test_a_driver_with_an_invalid_cluster_descriptor_is_refused(monkeypatch: pytest.MonkeyPatch) -> None:
    from compactors.spark_rewrite import knobs

    monkeypatch.setattr(knobs, "KUBERNETES", object(), raising=False)
    with pytest.raises(ValueError, match="CompactorKubernetes"):
        compactors.kubernetes_for("spark-rewrite")


@pytest.mark.parametrize("lookup", [compactors.knobs_for, compactors.fleet_for])
def test_an_unregistered_driver_is_refused_by_the_registry(lookup: Callable[[str], object]) -> None:
    with pytest.raises(ValueError, match="not a managed compaction driver"):
        lookup("broom")


def test_a_driver_whose_module_is_missing_says_so(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(compactors.MANAGED, "ghost", "compactors.ghost.knobs")
    with pytest.raises(ValueError, match="does not import"):
        compactors.knobs_for("ghost")
