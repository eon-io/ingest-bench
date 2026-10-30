# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import copy
from pathlib import Path
from typing import Any, cast
from unittest.mock import Mock

import pytest
import yaml

from ingest_bench.collect.schema import parse_result
from ingest_bench.collect.validate import _cost_failures
from ingest_bench.pricing import Pricing, resource_cost
from ingest_bench.specs.model import load_site
from tests.collect_fixtures import FLINK_SPEC, build_document, build_run_dir, write_site

PRICES: Pricing = {
    "machine_types": {
        "m8g.2xlarge": {"vcpu": 8, "gib": 32, "usd_per_hour": 0.35904},
        "c8gn.4xlarge": {"vcpu": 16, "gib": 32, "usd_per_hour": 0.948},
    }
}


def _site(tmp_path: Path, pricing: object = PRICES) -> Path:
    path = write_site(tmp_path)
    document = yaml.safe_load(path.read_text())
    document["pricing"] = pricing
    path.write_text(yaml.safe_dump(document))
    return path


def test_whole_and_partial_node_requests_use_the_nodes_own_price() -> None:
    assert resource_cost(16, 32, "c8gn.4xlarge", PRICES) == pytest.approx(0.948)
    assert resource_cost(8, 16, "c8gn.4xlarge", PRICES) == pytest.approx(0.474)
    assert resource_cost(8, 32, "m8g.2xlarge", PRICES) == pytest.approx(0.35904)
    assert resource_cost(7, 11, "c8gn.4xlarge", PRICES) > resource_cost(7, 11, "m8g.2xlarge", PRICES)


def test_mixed_fleet_prices_survive_collection_and_recompute(tmp_path: Path) -> None:
    spec = FLINK_SPEC.replace("machine_type: m6i.xlarge", "machine_type: c8gn.4xlarge\n  jm_machine_type: m8g.2xlarge")
    document = parse_result(build_document(build_run_dir(tmp_path, spec=spec), _site(tmp_path)))
    assert document["run"]["site_pricing"] == PRICES
    expected = 0.35904 / 2 * (1 / 8 + 2 / 32) + 2 * 0.948 / 2 * (2 / 16 + 4 / 32)
    cost = document["derived"]["cost"]
    assert cost["usd_per_hour"] == pytest.approx(expected)
    assert cost["run_hours"] is not None
    assert cost["usd"] == pytest.approx(expected * cost["run_hours"])
    assert _cost_failures(document["run"], cost) == []
    cost["usd_per_hour"] = 0.25
    assert any("recomputed" in error for error in _cost_failures(document["run"], cost))


def test_missing_machine_price_never_uses_another_nodes_rate(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="missing price for 'm6i.xlarge'"):
        build_document(build_run_dir(tmp_path), _site(tmp_path))
    with pytest.raises(ValueError, match="missing price for 'm6i.xlarge'"):
        resource_cost(2, 4, "m6i.xlarge", PRICES)


def test_publication_reports_missing_price_without_a_traceback(tmp_path: Path) -> None:
    raw = cast(dict[str, Any], build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    raw["run"]["site_pricing"] = PRICES
    document = parse_result(raw)
    assert _cost_failures(document["run"], document["derived"]["cost"]) == [
        "site_pricing.machine_types: missing price for 'm6i.xlarge'"
    ]


@pytest.mark.parametrize("field", ["vcpu", "gib", "usd_per_hour"])
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True, "1"])
def test_site_refuses_invalid_node_capacity_or_price(tmp_path: Path, field: str, value: object) -> None:
    pricing = cast(dict[str, Any], copy.deepcopy(PRICES))
    pricing["machine_types"]["m8g.2xlarge"][field] = value
    with pytest.raises(ValueError, match=field):
        load_site(_site(tmp_path, pricing))


@pytest.mark.parametrize("field", ["vcpu", "gib", "usd_per_hour"])
@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf"), True])
def test_result_refuses_invalid_node_capacity_or_price(tmp_path: Path, field: str, value: object) -> None:
    raw = cast(dict[str, Any], build_document(build_run_dir(tmp_path), write_site(tmp_path)))
    pricing = cast(dict[str, Any], copy.deepcopy(PRICES))
    pricing["machine_types"]["m8g.2xlarge"][field] = value
    raw["run"]["site_pricing"] = pricing
    with pytest.raises(ValueError, match=field):
        parse_result(raw)


@pytest.mark.parametrize("pricing", [{"machine_types": {}}, {**PRICES, "vcpu_hour_usd": 0.04, "gib_hour_usd": 0.005}])
def test_site_refuses_empty_or_ambiguous_price_maps(tmp_path: Path, pricing: object) -> None:
    with pytest.raises(ValueError, match="site.pricing"):
        load_site(_site(tmp_path, pricing))


def test_legacy_explicit_flat_rates_remain_compatible() -> None:
    assert resource_cost(2, 4, "m6i.xlarge", {"vcpu_hour_usd": 0.04, "gib_hour_usd": 0.005}) == pytest.approx(0.1)


@pytest.mark.parametrize("compactor", [False, True])
def test_missing_prices_fail_staging_before_resource_creation(tmp_path: Path, compactor: bool) -> None:
    from ingest_bench.stage import stage

    spec = FLINK_SPEC
    prices = copy.deepcopy(PRICES)
    if compactor:
        prices["machine_types"]["m6i.xlarge"] = {"vcpu": 4, "gib": 16, "usd_per_hour": 0.2}
        spec += """compaction:
  driver: spark-rewrite
  interval_s: 600
  spark-rewrite:
    machine_type: m8g.4xlarge
    executors: 1
    executor_cores: 2
    executor_mem_mb: 4096
"""
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(spec)
    admin = Mock()
    with pytest.raises(ValueError, match="missing price for"):
        stage(spec_path, _site(tmp_path, prices), tmp_path / "runs", admin)
    assert admin.method_calls == []
    assert not (tmp_path / "runs").exists()


@pytest.mark.parametrize(
    "spec_path",
    [
        "events-1mrps-uniform/aws-1mrps-unif-flink.yaml",
        "events-1mrps-uniform/aws-1mrps-unif-spark.yaml",
        "events-1mrps-skew/aws-1mrps-skew-spark-spark.yaml",
    ],
)
@pytest.mark.parametrize("complete", [False, True])
def test_staging_checks_prices_for_spark_and_flink_fleets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, spec_path: str, complete: bool
) -> None:
    import ingest_bench.stage as stage_module

    prices = copy.deepcopy(PRICES)
    if complete:
        prices["machine_types"]["m8g.4xlarge"] = {"vcpu": 16, "gib": 64, "usd_per_hour": 0.71808}
    else:
        prices["machine_types"].pop("c8gn.4xlarge")
    corpus_lookup = Mock(side_effect=RuntimeError("passed pricing preflight"))
    monkeypatch.setattr(stage_module, "resolve_corpus_dir", corpus_lookup)
    admin = Mock()
    spec = Path(__file__).resolve().parents[1] / "specs" / "tracks" / spec_path
    error = RuntimeError if complete else ValueError
    message = "passed pricing preflight" if complete else "missing price for"
    with pytest.raises(error, match=message):
        stage_module.stage(spec, _site(tmp_path, prices), tmp_path / "runs", admin)
    assert corpus_lookup.call_count == int(complete)
    assert admin.method_calls == []
