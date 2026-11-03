# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Compare the running Spark fleet with the requested configuration.

Read effective settings from the driver API and verify pod counts, Running
state, and Guaranteed QoS through kubectl. RUNNING at the operator level alone
does not prove that all settings took effect or all executors started.

Spark exposes no Structured Streaming REST endpoint for reading the trigger
interval during staging. The job receives it through job.json; reject unsupported
session settings that attempt to configure the trigger instead.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from engines.spark.knobs import KUBERNETES, Knobs, kubernetes_name, read
from ingest_bench.k8s.fleet import active_pods
from ingest_bench.readings import (
    DRIFT_EXIT,
    NOT_REPORTED,
    PENDING_EXIT,
    UNVERIFIED_EXIT,
    document,
    documents,
    fetch_json,
    field,
    line,
    optional_str_field,
    str_field,
)
from ingest_bench.specs.model import RunSpec, load_run_spec

# Application-independent endpoint; build per-application paths where read.
APPLICATIONS = "/api/v1/applications"

# Diagnostic label for the pod list supplied by kubectl.
PODS = "the pod list"

# Operator roles and required pod phase and QoS class.
_DRIVER = "driver"
_EXECUTOR = "executor"
_RUNNING_PHASE = "Running"
_GUARANTEED = "Guaranteed"

# Reject unsupported trigger settings; cadence belongs in job.json.
_TRIGGER_PREFIX = "spark.sql.streaming.trigger"


def _effective(knobs: Knobs, key: str, knob: str) -> str:
    """Return the submitted setting, including extra_spark_conf overrides."""
    return knobs.extra_spark_conf.get(key, knob)


def _application(run_id: str, answer: object) -> tuple[str, list[str]]:
    """Find exactly one application matching the run and report any mismatch.

    Match by name to detect a tunnel reaching the wrong application. Accept the
    original run ID and its lowercase Kubernetes name: the operator may replace
    the submitted spark.app.name with the latter.
    """
    accepted = sorted({run_id, kubernetes_name(run_id)})
    applications = documents(answer, APPLICATIONS)
    listed = [str_field(entry, "name", APPLICATIONS) for entry in applications]
    named = [name for name in listed if name in accepted]
    if len(named) != 1:
        # Include observed names to diagnose a misdirected tunnel or unexpected app name.
        return "", [line("applications named after the run", f"one of {accepted}", listed or "none listed")]
    return str_field(applications[listed.index(named[0])], "id", APPLICATIONS), []


def _properties(answer: object, where: str) -> dict[str, str]:
    """Convert the driver's sparkProperties pairs into a settings mapping."""
    reported = document(answer, where)
    pairs = field(reported, "sparkProperties", where)
    if not isinstance(pairs, list):
        raise ValueError(f"{where} answered sparkProperties as {type(pairs).__name__} rather than a JSON array")
    properties: dict[str, str] = {}
    for index, entry in enumerate(pairs):
        if not isinstance(entry, list) or len(entry) != 2:
            raise ValueError(f"{where} sparkProperties[{index}] is {entry!r} rather than a key and a value")
        key, value = entry
        properties[str(key)] = str(value)
    return properties


def _conf_drift(knobs: Knobs, fetch: Callable[[str], object], application: str) -> list[str]:
    """Report effective driver settings that differ from the request.

    Read the driver because the operator can override manifest settings through
    spark-submit arguments.
    """
    where = f"{APPLICATIONS}/{application}/environment"
    properties = _properties(fetch(where), where)
    settings: tuple[tuple[str, str, str], ...] = (
        ("executors", "spark.executor.instances", str(knobs.executors)),
        ("executor cores", "spark.executor.cores", str(knobs.executor_cores)),
        ("executor memory", "spark.executor.memory", f"{knobs.executor_mem_mb}m"),
        ("driver cores", "spark.driver.cores", str(knobs.driver_cores)),
        ("driver memory", "spark.driver.memory", f"{knobs.driver_mem_mb}m"),
        ("shuffle partitions", "spark.sql.shuffle.partitions", str(knobs.cores_total())),
    )
    if knobs.executor_overhead_mb is not None:
        settings += (("executor memory overhead", "spark.executor.memoryOverhead", f"{knobs.executor_overhead_mb}m"),)
    lines: list[str] = []
    for what, key, knob in settings:
        expected = _effective(knobs, key, knob)
        # A missing setting is drift, not a malformed response.
        actual = properties.get(key, NOT_REPORTED)
        if expected != actual:
            lines.append(line(what, expected, actual))
    pretended = sorted(key for key in properties if key.startswith(_TRIGGER_PREFIX))
    if pretended:
        lines.append(line("trigger settings", "none", pretended))
    return lines


@dataclass(frozen=True)
class _Pod:
    """Pod fields used to verify the fleet."""

    name: str
    role: str
    phase: str
    qos_class: str


def _pods(answer: object) -> list[_Pod]:
    items = active_pods(answer, where=PODS)
    pods: list[_Pod] = []
    for item in items:
        metadata = document(field(item, "metadata", PODS), f"{PODS}'s metadata")
        status = document(field(item, "status", PODS), f"{PODS}'s status")
        labels = {} if "labels" not in metadata else document(metadata["labels"], f"{PODS}'s labels")
        pods.append(
            _Pod(
                name=str_field(metadata, "name", PODS),
                role=optional_str_field(labels, KUBERNETES.pod_role_label, PODS),
                phase=optional_str_field(status, "phase", PODS),
                # A pod may lack QoS before admission; treat that as pending readiness.
                qos_class=optional_str_field(status, "qosClass", PODS),
            )
        )
    return pods


class FleetNotPlaced(Exception):
    """Signal that the fleet is still being scheduled and should be retried."""

    def __init__(self, lines: list[str]) -> None:
        super().__init__("; ".join(lines))
        self.lines = lines


def _pod_drift(knobs: Knobs, pods: object) -> list[str]:
    """Report fleet count, role, phase, or Guaranteed QoS mismatches."""
    fleet = _pods(pods)
    drivers = [pod for pod in fleet if pod.role == _DRIVER]
    executors = [pod for pod in fleet if pod.role == _EXECUTOR]
    lines: list[str] = []
    pending: list[str] = []
    if len(drivers) != 1:
        lines.append(line("driver pods", 1, len(drivers)))
    # The operator can report RUNNING before executors are scheduled or images
    # are pulled. Retry an incomplete fleet instead of reporting drift.
    if len(executors) < knobs.executors:
        pending.append(line("executor pods", knobs.executors, len(executors)))
    elif len(executors) > knobs.executors:
        lines.append(line("executor pods", knobs.executors, len(executors)))
    for pod in (*drivers, *executors):
        if pod.phase != _RUNNING_PHASE:
            # Check QoS once the pod is running.
            pending.append(line(f"pod {pod.name} phase", _RUNNING_PHASE, pod.phase or NOT_REPORTED))
        elif pod.qos_class != _GUARANTEED:
            lines.append(line(f"pod {pod.name} qos class", _GUARANTEED, pod.qos_class or NOT_REPORTED))
    for pod in fleet:
        if pod.role not in (_DRIVER, _EXECUTOR):
            # Reject selected pods without an expected fleet role.
            lines.append(line(f"pod {pod.name} role", f"{_DRIVER} or {_EXECUTOR}", pod.role or NOT_REPORTED))
    if pending and not lines:
        raise FleetNotPlaced(pending)
    return [*lines, *pending]


def verify(spec: RunSpec, run_id: str, fetch: Callable[[str], object], pods: object) -> list[str]:
    """Return one line per configuration mismatch, or an empty list.

    ``fetch`` supplies parsed driver API responses; ``pods`` supplies kubectl JSON.
    Malformed documents raise instead of producing a successful verdict.
    """
    knobs = read(spec.engine_block)
    application, lines = _application(run_id, fetch(APPLICATIONS))
    if application:
        lines.extend(_conf_drift(knobs, fetch, application))
    return [*lines, *_pod_drift(knobs, pods)]


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-spark",
        description="Compare a running Spark query's effective settings with its run spec.",
    )
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec the query was staged from")
    parser.add_argument(
        "--run-id",
        required=True,
        metavar="ID",
        help="run ID used as the Spark application name to identify the application to verify",
    )
    parser.add_argument(
        "--rest",
        required=True,
        metavar="URL",
        help="the driver's UI endpoint. `stage.sh` tunnels the driver's 4040 to http://localhost:18081",
    )
    parser.add_argument(
        "--pods",
        required=True,
        metavar="PATH",
        help="the run's pods as JSON, from `kubectl get pods -l <selector> -o json`",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = str(args.run_id)
    try:
        pods = _read_pods(Path(str(args.pods)))
        drift = verify(load_run_spec(Path(str(args.spec))), run_id, fetch_json(str(args.rest)), pods)
    except ValueError as error:
        # An unreadable response is retryable, not evidence of configuration drift.
        print(error, file=sys.stderr)
        return UNVERIFIED_EXIT
    except FleetNotPlaced as not_placed:
        for pending in not_placed.lines:
            print(pending, file=sys.stderr)
        return PENDING_EXIT
    for drifted in drift:
        print(drifted)
    if drift:
        return DRIFT_EXIT
    # Keep diagnostics on stderr so stdout contains only drift findings.
    print(f"verified: {run_id} matches its run spec", file=sys.stderr)
    return 0


def _read_pods(path: Path) -> object:
    """Read a pod-list file, raising a retryable error if it cannot be read."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise ValueError(f"could not read the pod list at {path}: {error}") from error


if __name__ == "__main__":
    raise SystemExit(main())
