# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import copy
import json
from dataclasses import replace
from pathlib import Path

import pytest
import yaml

from ingest_bench.k8s.fleet import compactor_fleet_from_pods, engine_fleet_from_pods, main, quantity
from ingest_bench.specs.model import load_run_spec

ROOT = Path(__file__).resolve().parents[1]


def spark_pods() -> dict[str, object]:
    return {
        "items": [
            {
                "metadata": {"name": name, "labels": {"spark-role": role}},
                "status": {"phase": "Running"},
                "spec": {
                    "containers": [
                        {
                            "name": "spark",
                            "resources": {
                                "requests": {"cpu": cpu, "memory": memory},
                                "limits": {"cpu": "99", "memory": "99Gi"},
                            },
                        }
                    ]
                },
            }
            for name, role, cpu, memory in (
                ("driver", "driver", "1000m", "2867Mi"),
                ("executor-1", "executor", "2", "5734Mi"),
                ("executor-2", "executor", "2", "5734Mi"),
            )
        ]
    }


def compactor_pods() -> dict[str, object]:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    for pod, cpu, memory in zip(items, ("1500m", "4500m", "4500m"), ("3Gi", "10Gi", "10Gi"), strict=True):
        pod["spec"]["containers"][0]["resources"]["requests"] = {"cpu": cpu, "memory": memory}
    return pods


def compactor_spec(tmp_path: Path) -> Path:
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["compaction"] = {
        "driver": "spark-rewrite",
        "interval_s": 600,
        "spark-rewrite": {"executors": 2, "executor_cores": 4, "executor_mem_mb": 8192},
    }
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def test_fleet_uses_admitted_requests_including_spark_overhead() -> None:
    spec = load_run_spec(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml")
    roles = engine_fleet_from_pods(spec, spark_pods())
    assert sum(role.count * role.gib for role in roles) == pytest.approx(14335 / 1024)
    assert sum(role.count * role.vcpu for role in roles) == 5


def test_compactor_fleet_uses_admitted_requests_and_distinct_roles(tmp_path: Path) -> None:
    roles = compactor_fleet_from_pods(load_run_spec(compactor_spec(tmp_path)), compactor_pods())
    assert [(role.role, role.count, role.vcpu, role.gib) for role in roles] == [
        ("compactor-driver", 1, 1.5, 3.0),
        ("compactor-executor", 2, 4.5, 10.0),
    ]


def test_compactor_pod_roles_come_from_its_typed_descriptor(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from compactors.spark_rewrite import knobs

    monkeypatch.setattr(
        knobs,
        "KUBERNETES",
        replace(
            knobs.KUBERNETES,
            pod_role_label="sidecar-kind",
            role_map=(("primary", "compactor-driver"), ("worker", "compactor-executor")),
        ),
    )
    pods = compactor_pods()
    items = pods["items"]
    assert isinstance(items, list)
    for pod, role in zip(items, ("primary", "worker", "worker"), strict=True):
        pod["metadata"]["labels"] = {"sidecar-kind": role}
    roles = compactor_fleet_from_pods(load_run_spec(compactor_spec(tmp_path)), pods)
    assert [(role.role, role.count) for role in roles] == [
        ("compactor-driver", 1),
        ("compactor-executor", 2),
    ]


def test_pod_role_label_comes_from_the_engine_descriptor(monkeypatch: pytest.MonkeyPatch) -> None:
    from engines.spark import knobs

    monkeypatch.setattr(knobs, "KUBERNETES", replace(knobs.KUBERNETES, pod_role_label="custom-role"))
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    for pod in items:
        labels = pod["metadata"]["labels"]
        labels["custom-role"] = labels.pop("spark-role")
    assert (
        sum(
            role.count
            for role in engine_fleet_from_pods(load_run_spec(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"), pods)
        )
        == 3
    )


@pytest.mark.parametrize("fault", ["missing", "pending", "duplicate", "no-request"])
def test_incomplete_or_invalid_fleet_cannot_be_costed(fault: str) -> None:
    pods = copy.deepcopy(spark_pods())
    items = pods["items"]
    assert isinstance(items, list)
    if fault == "missing":
        items.pop()
    elif fault == "pending":
        items[0]["status"]["phase"] = "Pending"
    elif fault == "duplicate":
        items[1]["metadata"]["name"] = items[2]["metadata"]["name"]
    else:
        items[0]["spec"]["containers"][0]["resources"]["requests"].pop("memory")
    with pytest.raises(ValueError):
        engine_fleet_from_pods(load_run_spec(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"), pods)


@pytest.mark.parametrize(
    "value, expected", [("500m", 0.5), ("4Gi", 4294967296), ("1e3", 1000), ("2G", 2e9), ("1Ei", 1024**6)]
)
def test_kubernetes_quantity_units(value: str, expected: float) -> None:
    assert quantity(value) == expected


@pytest.mark.parametrize("value", ["1e999", "nan", "-1", "0", "4GB"])
def test_invalid_or_unrepresentable_resource_quantity(value: str) -> None:
    with pytest.raises(ValueError):
        quantity(value)


def test_capture_retains_resource_evidence_without_environment_secrets(tmp_path: Path) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    items[0]["spec"]["containers"][0]["env"] = [{"name": "TOKEN", "value": "secret-token"}]
    source, output = tmp_path / "raw.json", tmp_path / "engine-pods.json"
    source.write_text(json.dumps(pods))
    assert (
        main(
            [
                "--spec",
                str(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"),
                "--pods",
                str(source),
                "--out",
                str(output),
            ]
        )
        == 0
    )
    assert "secret-token" not in output.read_text()
    assert engine_fleet_from_pods(
        load_run_spec(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"), json.loads(output.read_text())
    )


def test_capture_validates_and_retains_compactor_resource_evidence(tmp_path: Path) -> None:
    spec_path = compactor_spec(tmp_path)
    source, output = tmp_path / "raw.json", tmp_path / "compactor-pods.json"
    source.write_text(json.dumps(compactor_pods()))
    assert main(["--compaction", "--spec", str(spec_path), "--pods", str(source), "--out", str(output)]) == 0
    roles = compactor_fleet_from_pods(load_run_spec(spec_path), json.loads(output.read_text()))
    assert sum(role.count * role.vcpu for role in roles) == 10.5


@pytest.mark.parametrize("phase, deleting", [("Failed", False), ("Succeeded", False), ("Running", True)])
def test_capture_ignores_retired_pods_and_preserves_that_filter_on_reload(
    tmp_path: Path, phase: str, deleting: bool
) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    old = copy.deepcopy(items[-1])
    old["metadata"]["name"] = "old-executor"
    old["status"]["phase"] = phase
    if deleting:
        old["metadata"]["deletionTimestamp"] = "2026-09-10T12:00:00Z"
    items.append(old)
    source, output = tmp_path / "raw.json", tmp_path / "saved.json"
    source.write_text(json.dumps(pods))
    spec_path = ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"
    assert main(["--spec", str(spec_path), "--pods", str(source), "--out", str(output)]) == 0
    saved = json.loads(output.read_text())
    assert len(saved["items"]) == 3
    assert sum(role.count for role in engine_fleet_from_pods(load_run_spec(spec_path), saved)) == 3


@pytest.mark.parametrize("key", ["initContainers", "overhead", "resources"])
def test_unsupported_pod_resource_layouts_are_not_silently_undercounted(key: str) -> None:
    pods = spark_pods()
    items = pods["items"]
    assert isinstance(items, list)
    items[0]["spec"][key] = {"memory": "1Gi"}
    with pytest.raises(ValueError, match="container-only requests"):
        engine_fleet_from_pods(load_run_spec(ROOT / "specs/cluster-smoke/aws-smoke-spark.yaml"), pods)


@pytest.mark.parametrize("engine", ["spark", "flink"])
def test_admitted_engine_resources_retain_each_roles_machine_type(engine: str) -> None:
    spec = load_run_spec(ROOT / f"specs/cluster-smoke/aws-smoke-{engine}.yaml")
    coordinator_key = "driver_machine_type" if engine == "spark" else "jm_machine_type"
    spec = replace(
        spec,
        engine_block={**spec.engine_block, "machine_type": "m8g.4xlarge", coordinator_key: "m8g.2xlarge"},
    )
    pods = spark_pods()
    if engine == "flink":
        items = pods["items"]
        assert isinstance(items, list)
        for pod, role in zip(items, ("jobmanager", "taskmanager", "taskmanager"), strict=True):
            pod["metadata"]["labels"] = {"component": role}
    roles = engine_fleet_from_pods(spec, pods)
    assert [(role.machine_type, role.count, role.vcpu, role.gib) for role in roles] == [
        ("m8g.2xlarge", 1, 1.0, 2867 / 1024),
        ("m8g.4xlarge", 2, 2.0, 5734 / 1024),
    ]


@pytest.mark.parametrize("driver", ["spark-rewrite", "flink-rewrite"])
def test_admitted_compactor_resources_retain_each_roles_machine_type(tmp_path: Path, driver: str) -> None:
    path = compactor_spec(tmp_path)
    raw = yaml.safe_load(path.read_text())
    if driver == "spark-rewrite":
        block = raw["compaction"][driver]
        block["driver_machine_type"] = "m8g.2xlarge"
    else:
        block = {"rewrite_parallelism": 6, "slots": 4, "tm_cpu": 4, "tm_mem_mb": 8192, "jm_machine_type": "m8g.2xlarge"}
    block["machine_type"] = "m8g.4xlarge"
    raw["compaction"] = {"driver": driver, "interval_s": 600, driver: block}
    path.write_text(yaml.safe_dump(raw))
    pods = compactor_pods()
    if driver == "flink-rewrite":
        items = pods["items"]
        assert isinstance(items, list)
        for pod, role in zip(items, ("jobmanager", "taskmanager", "taskmanager"), strict=True):
            pod["metadata"]["labels"] = {"component": role}
    roles = compactor_fleet_from_pods(load_run_spec(path), pods)
    assert [(role.machine_type, role.count, role.vcpu, role.gib) for role in roles] == [
        ("m8g.2xlarge", 1, 1.5, 3.0),
        ("m8g.4xlarge", 2, 4.5, 10.0),
    ]
