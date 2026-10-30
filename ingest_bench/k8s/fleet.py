# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Capture the admitted container requests of a fixed engine or compactor fleet."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from ingest_bench.readings import document, documents, field, str_field
from ingest_bench.specs.compactors import fleet_for as compactor_fleet_for
from ingest_bench.specs.compactors import kubernetes_for as compactor_kubernetes_for
from ingest_bench.specs.engines import fleet_for, kubernetes_for
from ingest_bench.specs.model import FleetRole, RunSpec, load_run_spec

PODS_FILE = "engine-pods.json"
COMPACTOR_PODS_FILE = "compactor-pods.json"
_QUANTITY = re.compile(r"([+]?(?:\d+(?:\.\d*)?|\.\d+))([eE][+-]?\d+|[numkKMGTPE]|[KMGTPE]i)?")
_SCALES = {"": Decimal(1), "n": Decimal("1e-9"), "u": Decimal("1e-6"), "m": Decimal("1e-3")}
_SCALES.update({unit: Decimal(1000) ** power for power, unit in enumerate("kMGTPE", 1)})
_SCALES["K"] = _SCALES["k"]
_SCALES.update({unit + "i": Decimal(1024) ** power for power, unit in enumerate("KMGTPE", 1)})


def quantity(value: object) -> float:
    """Read a positive Kubernetes quantity in base units (cores or bytes)."""
    match = _QUANTITY.fullmatch(str(value))
    if match is None:
        raise ValueError(f"invalid resource request {value!r}")
    number, suffix = match.groups()
    suffix = suffix or ""
    scale = _SCALES[suffix] if suffix in _SCALES else Decimal(10) ** int(suffix[1:])
    result = Decimal(number) * scale
    if result <= 0 or not math.isfinite(float(result)):
        raise ValueError(f"resource request must be positive and finite, got {value!r}")
    return float(result)


def active_pods(answer: object, where: str = PODS_FILE) -> list[dict[str, object]]:
    """Exclude retired pods while preserving pending pods for readiness checks."""
    active = []
    for pod in documents(field(document(answer, where), "items", where), where):
        metadata = document(field(pod, "metadata", where), f"{where}'s metadata")
        if metadata.get("deletionTimestamp"):
            continue
        status = document(field(pod, "status", where), f"{where}'s status")
        if status.get("phase") not in ("Succeeded", "Failed"):
            active.append(pod)
    return active


def _fleet_from_pods(
    answer: object,
    expected: dict[str, FleetRole],
    *,
    label: str,
    role_map: Mapping[str, str],
    where: str,
) -> list[FleetRole]:
    """Read running containers and require the complete expected fleet."""
    if not label:
        raise ValueError("fleet descriptor returned no pod role label")
    counts: Counter[str] = Counter()
    groups: Counter[tuple[str, float, float, str]] = Counter()
    names: set[str] = set()
    for pod in active_pods(answer, where):
        metadata = document(field(pod, "metadata", where), "pod metadata")
        name = str_field(metadata, "name", "pod metadata")
        if name in names:
            raise ValueError(f"duplicate pod {name!r} in {where}")
        names.add(name)
        labels = document(field(metadata, "labels", name), name)
        observed_role = str_field(labels, label, name)
        role = role_map.get(observed_role, observed_role)
        if role not in expected:
            raise ValueError(f"unexpected fleet role {role!r} on pod {name}")
        status = document(field(pod, "status", name), name)
        if str_field(status, "phase", name) != "Running":
            raise ValueError(f"pod {name} is not Running")
        pod_spec = document(field(pod, "spec", name), name)
        if any(pod_spec.get(key) for key in ("initContainers", "overhead", "resources")):
            raise ValueError(
                f"pod {name} uses init containers or pod-level resources; "
                "fleet costing requires container-only requests"
            )
        containers = documents(field(pod_spec, "containers", name), name)
        if not containers:
            raise ValueError(f"pod {name} has no containers")
        cpu = memory = 0.0
        for container in containers:
            resources = document(field(container, "resources", name), name)
            requests = document(field(resources, "requests", name), name)
            cpu += quantity(field(requests, "cpu", name))
            memory += quantity(field(requests, "memory", name))
        counts[role] += 1
        groups[role, cpu, memory / 1024**3, expected[role].machine_type] += 1
    wanted = {name: role.count for name, role in expected.items()}
    if counts != wanted:
        raise ValueError(f"incomplete fleet: expected {wanted}, observed {dict(counts)}")
    return [FleetRole(role, count, cpu, gib, machine) for (role, cpu, gib, machine), count in sorted(groups.items())]


def engine_fleet_from_pods(spec: RunSpec, answer: object) -> list[FleetRole]:
    """Read a managed engine's admitted pod requests."""
    descriptor = kubernetes_for(spec.engine)
    expected = {role.role: role for role in fleet_for(spec.engine).fleet(spec)}
    return _fleet_from_pods(answer, expected, label=descriptor.pod_role_label, role_map={}, where=PODS_FILE)


def compactor_fleet_from_pods(spec: RunSpec, answer: object) -> list[FleetRole]:
    """Read a managed compactor's admitted pod requests."""
    declared = spec.compaction
    if declared is None or not declared.is_managed():
        raise ValueError(f"run {spec.name!r} declares no managed compactor fleet")
    descriptor = compactor_kubernetes_for(declared.driver)
    expected = {role.role: role for role in compactor_fleet_for(declared.driver).fleet(spec)}
    return _fleet_from_pods(
        answer,
        expected,
        label=descriptor.pod_role_label,
        role_map=dict(descriptor.role_map),
        where=COMPACTOR_PODS_FILE,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", required=True, type=Path)
    parser.add_argument("--pods", required=True, type=Path)
    parser.add_argument("--out", required=True, type=Path)
    parser.add_argument("--compaction", action="store_true", help="capture the managed compactor fleet")
    args = parser.parse_args(argv)
    try:
        answer = json.loads(args.pods.read_text())
        spec = load_run_spec(args.spec)
        if args.compaction:
            compactor_fleet_from_pods(spec, answer)
        else:
            engine_fleet_from_pods(spec, answer)
        # Keep resource evidence without archiving pod environment values or annotations.
        items = []
        where = COMPACTOR_PODS_FILE if args.compaction else PODS_FILE
        for pod in active_pods(answer, where):
            metadata = document(pod["metadata"], "pod metadata")
            status = document(pod["status"], "pod status")
            pod_spec = document(pod["spec"], "pod spec")
            containers = []
            for container in documents(pod_spec["containers"], "pod containers"):
                resources = document(container["resources"], "container resources")
                containers.append({"name": container["name"], "resources": {"requests": resources["requests"]}})
            items.append(
                {
                    "metadata": {"name": metadata["name"], "labels": metadata["labels"]},
                    "status": {"phase": status["phase"]},
                    "spec": {"containers": containers},
                }
            )
        snapshot = {
            "captured_at": datetime.now(UTC).isoformat(),
            "items": items,
        }
        args.out.write_text(json.dumps(snapshot, indent=2) + "\n")
    except (ValueError, KeyError, OSError) as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
