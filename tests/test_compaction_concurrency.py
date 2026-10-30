# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Real Flink append/Spark rewrite qualification against the local stack.

These tests deliberately do not emulate Iceberg commits.  Each run creates an
isolated Compose project and removes fixed host ports except for an ephemeral
Flink REST port.  Missing Docker access is an actionable failure once enabled.

Run all three 15-minute scenarios with::

    INGEST_BENCH_RUN_COMPACTION_CONCURRENCY=1 uv run pytest -q tests/test_compaction_concurrency.py
"""

from __future__ import annotations

import json
import os
import re
import shutil
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import cast

import pytest
import yaml

from tests.catalog_barrier import CatalogBarrier

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ROOT / "deploy/compose/local/docker-compose.yml"
SPEC = ROOT / "specs/smoke/smoke-flink-compact.yaml"
CHECKER = ROOT / "tests/compaction_concurrency_check.py"
ENABLED = os.environ.get("INGEST_BENCH_RUN_COMPACTION_CONCURRENCY") == "1"
KEEP = os.environ.get("INGEST_BENCH_KEEP_COMPACTION_CONCURRENCY") == "1"
DURATION_S = int(os.environ.get("INGEST_BENCH_COMPACTION_DURATION_S", "900"))
OFFERED_RATE = os.environ.get("INGEST_BENCH_COMPACTION_OFFERED_RATE", "200KB")
MIN_NONEMPTY_PASSES = int(os.environ.get("INGEST_BENCH_COMPACTION_MIN_PASSES", "20"))
MIN_OVERLAPS = int(os.environ.get("INGEST_BENCH_COMPACTION_MIN_OVERLAPS", "10"))
RECOVERY_OBSERVATION_S = int(os.environ.get("INGEST_BENCH_COMPACTION_RECOVERY_OBSERVATION_S", "600"))
RUN_LINE = re.compile(r"run (?P<run_id>smoke-flink-[a-z0-9-]+-[0-9TZ]+) staged in (?P<path>\S+)")
BARRIER_DURATION_S = int(os.environ.get("INGEST_BENCH_COMPACTION_BARRIER_DURATION_S", "300"))
QUALIFICATION_COMMAND = " ".join(
    ("INGEST_BENCH_RUN_COMPACTION_CONCURRENCY=1", "uv run pytest -q tests/test_compaction_concurrency.py")
)


@dataclass(frozen=True)
class RewriteEvent:
    status: str
    started_at: datetime
    ended_at: datetime
    rewritten_files: int
    consecutive_failures: int


def _run(command: list[str], *, env: dict[str, str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, check=check)


def _compose(project: str, *args: str, env: dict[str, str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return _run(
        ["docker", "compose", "-f", str(COMPOSE), "-p", project, "--profile", "*", *args],
        env=env,
        check=check,
    )


def _preflight() -> str:
    missing = [tool for tool in ("docker", "jq", "yq") if shutil.which(tool) is None]
    if missing:
        pytest.fail(f"concurrency qualification is enabled but host tools are missing: {', '.join(missing)}")
    real_docker = shutil.which("docker")
    assert real_docker is not None
    docker = subprocess.run(
        [real_docker, "info", "--format", "{{.ServerVersion}}"], capture_output=True, text=True, check=False
    )
    if docker.returncode:
        pytest.fail(f"concurrency qualification is enabled but Docker is unavailable: {docker.stderr.strip()}")
    return real_docker


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _isolated_compose_env(
    tmp_path: Path, real_docker: str, project: str, *, catalog_port: int | None = None
) -> dict[str, str]:
    """Give smoke.sh an extra Compose file without changing production scripts.

    With a catalog port, the catalog is published on it for the host-side barrier,
    and the sidecar can reach the host by name.
    """
    flink_port = _free_port()
    override = tmp_path / "compose-isolation.yaml"
    catalog_ports = "!reset []" if catalog_port is None else f'!override ["127.0.0.1:{catalog_port}:8181"]'
    override.write_text(
        f"""services:
  kafka:
    ports: !reset []
  minio:
    ports: !reset []
  iceberg-rest:
    ports: {catalog_ports}
  flink-jobmanager:
    ports: !override ["${{QUAL_FLINK_REST_PORT}}:8081"]
  spark-compactor:
    extra_hosts: ["host.docker.internal:host-gateway"]
"""
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    wrapper = bin_dir / "docker"
    wrapper.write_text(
        "#!/bin/sh\n"
        "set -eu\n"
        f"REAL_DOCKER='{real_docker}'\n"
        f"BASE='{COMPOSE}'\n"
        f"OVERRIDE='{override}'\n"
        'if [ "${1:-}" = compose ] && [ "${2:-}" = -f ] && [ "${3:-}" = "$BASE" ]; then\n'
        "  shift 3\n"
        '  exec "$REAL_DOCKER" compose -f "$BASE" -f "$OVERRIDE" "$@"\n'
        "fi\n"
        'exec "$REAL_DOCKER" "$@"\n'
    )
    wrapper.chmod(0o755)
    return {
        **os.environ,
        "PATH": f"{bin_dir}:{os.environ['PATH']}",
        "COMPOSE_PROJECT_NAME": project,
        "QUAL_FLINK_REST_PORT": str(flink_port),
        "FLINK_REST": f"http://localhost:{flink_port}",
        "EPOCH_LEAD_S": "15",
        "IDLE_STOP_S": "240",
    }


def _read_log(path: Path) -> str:
    try:
        return path.read_text(errors="replace")
    except FileNotFoundError:
        return ""


def _wait_for_run(log_path: Path, process: subprocess.Popen[str], timeout_s: int = 900) -> tuple[str, Path]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        match = RUN_LINE.search(_read_log(log_path))
        if match:
            return match.group("run_id"), Path(match.group("path"))
        if process.poll() is not None:
            pytest.fail(f"smoke exited {process.returncode} before staging a run:\n{_read_log(log_path)[-8000:]}")
        time.sleep(2)
    pytest.fail(f"smoke did not stage a run within {timeout_s}s:\n{_read_log(log_path)[-8000:]}")


def _json_lines(text: str) -> list[dict[str, object]]:
    found: list[dict[str, object]] = []
    for line in text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and str(value.get("event", "")).startswith("spark_rewrite"):
            found.append(cast(dict[str, object], value))
    return found


def _rewrite_events(text: str) -> list[RewriteEvent]:
    events: list[RewriteEvent] = []
    for entry in _json_lines(text):
        if entry.get("event") != "spark_rewrite_pass":
            continue
        events.append(
            RewriteEvent(
                status=str(entry["status"]),
                started_at=datetime.fromisoformat(str(entry["started_at"]).replace("Z", "+00:00")),
                ended_at=datetime.fromisoformat(str(entry["ended_at"]).replace("Z", "+00:00")),
                rewritten_files=int(cast(int | str, entry.get("rewritten_data_files_count") or 0)),
                consecutive_failures=int(cast(int | str, entry.get("consecutive_failures") or 0)),
            )
        )
    return events


def _compactor_logs(project: str, env: dict[str, str]) -> str:
    result = _compose(project, "logs", "--no-color", "--no-log-prefix", "spark-compactor", env=env, check=False)
    return result.stdout + result.stderr


# The catalog and the engine are the other parties to every commit; their logs
# are what tells a catalog outage apart from a sidecar fault once the project is
# gone.
DIAGNOSTIC_SERVICES = ("iceberg-rest", "flink-jobmanager", "flink-taskmanager", "spark-compactor")


def _save_service_logs(project: str, run_dir: Path | None, env: dict[str, str]) -> None:
    if run_dir is None or not run_dir.is_dir():
        return
    for service in DIAGNOSTIC_SERVICES:
        result = _compose(project, "logs", "--no-color", "--timestamps", service, env=env, check=False)
        (run_dir / f"{service}.log").write_text(result.stdout + result.stderr)


def _wait_for_nonempty_rewrite(project: str, env: dict[str, str], timeout_s: int = 300) -> RewriteEvent:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        events = _rewrite_events(_compactor_logs(project, env))
        successful = [event for event in events if event.status == "success" and event.rewritten_files > 0]
        if successful:
            return successful[-1]
        time.sleep(2)
    pytest.fail(f"no successful nonempty rewrite appeared within {timeout_s}s")


def _wait_for_active_rewrite(project: str, env: dict[str, str], timeout_s: int = 300) -> dict[str, object]:
    seen_starts = 0
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        records = _json_lines(_compactor_logs(project, env))
        starts = [record for record in records if record.get("event") == "spark_rewrite_start"]
        completions = [record for record in records if record.get("event") == "spark_rewrite_pass"]
        if len(starts) > max(seen_starts, len(completions)):
            return starts[-1]
        seen_starts = max(seen_starts, len(starts))
        time.sleep(0.5)
    pytest.fail(f"no in-flight Spark rewrite appeared within {timeout_s}s")


def _restart_service(project: str, service: str, env: dict[str, str]) -> tuple[str, datetime]:
    ids = _compose(project, "ps", "-q", service, env=env).stdout.splitlines()
    if not ids:
        pytest.fail(f"Compose service {service} has no running container")
    container = ids[0]
    restarted_at = datetime.now(UTC)
    result = _run(["docker", "restart", container], env=env, check=False)
    if result.returncode:
        pytest.fail(f"could not restart {service} container {container}: {result.stderr.strip()}")
    return container, restarted_at


def _iso(moment: datetime) -> str:
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _terminal_exact(run_dir: Path) -> dict[str, object]:
    path = run_dir / "scores/terminal-exactness.json"
    if not path.is_file():
        pytest.fail(f"smoke produced no pinned terminal validation at {path}")
    result = cast(dict[str, object], json.loads(path.read_text()))
    if result.get("verdict") is not True:
        pytest.fail(f"terminal exactness failed: {json.dumps(result, sort_keys=True)}")
    return result


def _scan_snapshots(project: str, run_id: str, run_dir: Path, env: dict[str, str]) -> dict[str, object]:
    out = run_dir / "scores/concurrency-qualification.json"
    command = (
        "python /opt/compaction-concurrency-check.py "
        f"--facts /runs/{run_id}/facts.json --out /runs/{run_id}/scores/{out.name}"
    )
    result = _compose(
        project,
        "run",
        "--rm",
        "-T",
        "-v",
        f"{CHECKER}:/opt/compaction-concurrency-check.py:ro",
        "harness",
        command,
        env=env,
        check=False,
    )
    if result.returncode:
        pytest.fail(f"historical full-row validation failed:\n{result.stdout}\n{result.stderr}")
    return cast(dict[str, object], json.loads(out.read_text()))


def _clean_overlap_evidence(
    report: dict[str, object], rewrite_events: list[RewriteEvent]
) -> tuple[int, list[dict[str, object]]]:
    snapshots = cast(list[dict[str, object]], report["snapshots"])
    appends = [entry for entry in snapshots if entry["operation"] == "append"]
    replacements = [entry for entry in snapshots if entry["operation"] != "append"]
    nonempty = [event for event in rewrite_events if event.status == "success" and event.rewritten_files > 0]
    overlaps: list[dict[str, object]] = []
    for event in nonempty:
        inside_appends = [
            entry
            for entry in appends
            if event.started_at.timestamp() * 1000
            < int(cast(int | str, entry["timestamp_ms"]))
            < event.ended_at.timestamp() * 1000
        ]
        inside_replacements = [
            entry
            for entry in replacements
            if event.started_at.timestamp() * 1000
            <= int(cast(int | str, entry["timestamp_ms"]))
            <= event.ended_at.timestamp() * 1000
        ]
        evidence: dict[str, object] | None = None
        for append in inside_appends:
            append_ms = int(cast(int | str, append["timestamp_ms"]))
            for replacement in inside_replacements:
                replacement_ms = int(cast(int | str, replacement["timestamp_ms"]))
                if append_ms >= replacement_ms:
                    continue
                shared = sorted(
                    set(cast(list[str], append["added_partitions"]))
                    & set(cast(list[str], replacement["removed_partitions"]))
                )
                if shared:
                    evidence = {
                        "rewrite_started_at": _iso(event.started_at),
                        "rewrite_ended_at": _iso(event.ended_at),
                        "append_snapshot_id": append["snapshot_id"],
                        "rewrite_snapshot_id": replacement["snapshot_id"],
                        "shared_partitions": shared,
                    }
                    break
            if evidence is not None:
                break
        if evidence is not None:
            overlaps.append(evidence)
    return len(nonempty), overlaps


def test_overlap_requires_the_append_to_commit_before_the_rewrite_snapshot() -> None:
    started = datetime(2026, 9, 15, 12, tzinfo=UTC)
    rewrite = RewriteEvent(
        status="success",
        started_at=started,
        ended_at=started.replace(second=10),
        rewritten_files=2,
        consecutive_failures=0,
    )
    report: dict[str, object] = {
        "snapshots": [
            {
                "operation": "replace",
                "timestamp_ms": int(started.replace(second=5).timestamp() * 1000),
                "snapshot_id": 1,
                "removed_partitions": ["Record[p00000]"],
            },
            {
                "operation": "append",
                "timestamp_ms": int(started.replace(second=8).timestamp() * 1000),
                "snapshot_id": 2,
                "added_partitions": ["Record[p00000]"],
            },
        ]
    }

    nonempty, overlaps = _clean_overlap_evidence(report, [rewrite])

    assert nonempty == 1
    assert overlaps == []


def _assert_fault_recovery(
    fault: str,
    restarted_at: datetime,
    report: dict[str, object],
    rewrite_events: list[RewriteEvent],
) -> None:
    snapshots = cast(list[dict[str, object]], report["snapshots"])
    restart_ms = restarted_at.timestamp() * 1000
    post_appends = [
        entry
        for entry in snapshots
        if entry["operation"] == "append" and int(cast(int | str, entry["timestamp_ms"])) > restart_ms
    ]
    assert post_appends, f"Flink made no append commit after the {fault} restart"
    post_rewrites = [
        event
        for event in rewrite_events
        if event.status == "success" and event.rewritten_files > 0 and event.ended_at > restarted_at
    ]
    assert post_rewrites, f"Spark made no nonempty rewrite after the {fault} restart"
    latest_ms = max(int(cast(int | str, entry["timestamp_ms"])) for entry in snapshots)
    assert latest_ms >= restart_ms + RECOVERY_OBSERVATION_S * 1000, (
        f"only {(latest_ms - restart_ms) / 1000:.1f}s of post-restart commits; "
        f"need {RECOVERY_OBSERVATION_S}s to catch delayed duplicates"
    )


@pytest.mark.integration
@pytest.mark.skipif(
    not ENABLED,
    reason=f"run `{QUALIFICATION_COMMAND}` to run real Flink/Spark concurrency qualification",
)
@pytest.mark.parametrize("fault", ["clean", "flink-taskmanager", "spark-compactor"])
def test_flink_appends_survive_real_spark_rewrites_and_restarts(tmp_path: Path, fault: str) -> None:
    real_docker = _preflight()
    assert DURATION_S >= 900, "qualification duration must be at least 900 seconds"
    project = f"ib-qual-{uuid.uuid4().hex[:12]}"
    env = _isolated_compose_env(tmp_path, real_docker, project)
    log_path = tmp_path / f"{fault}.log"
    command = [
        str(ROOT / "scripts/smoke.sh"),
        "--engine",
        "flink",
        "--spec",
        str(SPEC),
        "--set",
        f"duration_s={DURATION_S}",
        "--set",
        f"offered_bytes_per_s={OFFERED_RATE}",
        "--set",
        "partition_count=8",
        "--keep",
    ]
    run_id = ""
    run_dir: Path | None = None
    restarted_at: datetime | None = None
    restarted_container: str | None = None
    active_rewrite: dict[str, object] | None = None
    with log_path.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, text=True)
        try:
            run_id, run_dir = _wait_for_run(log_path, process)
            env["RUN_DIR"] = str(run_dir)
            if fault == "flink-taskmanager":
                _wait_for_nonempty_rewrite(project, env)
                restarted_container, restarted_at = _restart_service(project, fault, env)
            elif fault == "spark-compactor":
                _wait_for_nonempty_rewrite(project, env)
                active_rewrite = _wait_for_active_rewrite(project, env)
                restarted_container, restarted_at = _restart_service(project, fault, env)
            try:
                returncode = process.wait(timeout=DURATION_S + 1800)
            except subprocess.TimeoutExpired:
                process.terminate()
                pytest.fail(f"smoke exceeded its {DURATION_S + 1800}s bound:\n{_read_log(log_path)[-8000:]}")
            assert returncode == 0, _read_log(log_path)[-12000:]

            assert run_dir is not None
            (run_dir / "smoke.log").write_text(_read_log(log_path))
            terminal = _terminal_exact(run_dir)
            compactor_log = _compactor_logs(project, env)
            (run_dir / "spark-compactor.log").write_text(compactor_log)
            rewrites = _rewrite_events(compactor_log)
            report = _scan_snapshots(project, run_id, run_dir, env)
            nonempty_rewrites = sum(event.status == "success" and event.rewritten_files > 0 for event in rewrites)
            overlaps: list[dict[str, object]] = []
            report.update(
                {
                    "fault": fault,
                    "compose_project": project,
                    "configuration": {
                        "duration_s": DURATION_S,
                        "offered_rate": OFFERED_RATE,
                        "minimum_nonempty_rewrites": MIN_NONEMPTY_PASSES,
                        "minimum_overlaps": MIN_OVERLAPS,
                        "recovery_observation_s": RECOVERY_OBSERVATION_S,
                    },
                    "repository_revision": _run(["git", "rev-parse", "HEAD"], env=env).stdout.strip(),
                    "images": {
                        image: _run(
                            ["docker", "image", "inspect", image, "--format", "{{.Id}}"], env=env
                        ).stdout.strip()
                        for image in ("lakehouse-ingest-bench/flink:local", "lakehouse-ingest-bench/spark:local")
                    },
                    "rewrite_events": len(rewrites),
                    "nonempty_rewrites": nonempty_rewrites,
                    "rewrite_failures": sum(event.status == "failure" for event in rewrites),
                    "max_consecutive_rewrite_failures": max(
                        (event.consecutive_failures for event in rewrites), default=0
                    ),
                    "restart": None
                    if restarted_at is None
                    else {
                        "service": fault,
                        "container": restarted_container,
                        "at": _iso(restarted_at),
                        "active_rewrite": active_rewrite,
                    },
                    "terminal_exactness": terminal,
                }
            )
            qualification_failure: str | None = None
            try:
                if fault == "clean":
                    nonempty_rewrites, overlaps = _clean_overlap_evidence(report, rewrites)
                    assert nonempty_rewrites >= MIN_NONEMPTY_PASSES, (
                        f"only {nonempty_rewrites} successful nonempty rewrites; need {MIN_NONEMPTY_PASSES}"
                    )
                    assert len(overlaps) >= MIN_OVERLAPS, (
                        f"only {len(overlaps)} rewrites overlapped an append; need {MIN_OVERLAPS}"
                    )
                else:
                    assert restarted_at is not None
                    _assert_fault_recovery(fault, restarted_at, report, rewrites)
            except AssertionError as error:
                qualification_failure = str(error)
            report.update(
                {
                    "status": "failed" if qualification_failure else "passed",
                    "qualification_status": "unqualified" if qualification_failure else "qualified",
                    "qualification_failure": qualification_failure,
                    "overlap_count": len(overlaps),
                    "overlaps": overlaps,
                }
            )
            (run_dir / "scores/concurrency-qualification.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n"
            )
            if qualification_failure:
                pytest.fail(qualification_failure)
        finally:
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
            _save_service_logs(project, run_dir, env)
            if not KEEP:
                cleanup = _compose(project, "down", "-v", "--remove-orphans", env=env, check=False)
                if cleanup.returncode:
                    pytest.fail(f"could not clean owned Compose project {project}: {cleanup.stderr.strip()}")


# ---------------------------------------------------------------------------
# The stale-metadata commit race, made deterministic
# ---------------------------------------------------------------------------


def _barrier_spec(port: int, directory: Path) -> Path:
    """A copy of the qualification spec whose sidecar commits through the host-side barrier.

    Staging refuses a Flink run whose name plus stamp would
    exceed the operator's 45 characters, which caps the name at 28; the
    six-character suffix keeps this one at 23.
    """
    spec = cast(dict[str, object], yaml.safe_load(SPEC.read_text()))
    suffix = uuid.uuid4().hex[:6]
    spec["name"] = f"smoke-flink-held-{suffix}"
    compaction = cast(dict[str, object], spec["compaction"])
    sidecar = cast(dict[str, object], compaction["spark-rewrite"])
    sidecar["extra_spark_conf"] = {"spark.sql.catalog.ice.uri": f"http://host.docker.internal:{port}"}
    path = directory / f"smoke-flink-held-{suffix}.yaml"
    path.write_text(yaml.safe_dump(spec, sort_keys=False))
    return path


@pytest.mark.integration
@pytest.mark.skipif(
    not ENABLED,
    reason=f"run `{QUALIFICATION_COMMAND} -k barrier` to run the deterministic commit-race qualification",
)
def test_a_rewrite_commit_held_until_an_append_lands_is_retried_or_fails_safely(tmp_path: Path) -> None:
    """Hold the first rewrite commit until Flink appends, then release it.

    The catalog must reject the stale commit and the client retry against the new
    metadata, or fail safely with the live rows unchanged and a later pass
    succeeding. Either way every snapshot keeps every row and the final table is
    exact. No delay lives in production code: the hold is a test-only proxy.
    """
    real_docker = _preflight()
    assert BARRIER_DURATION_S >= 300, "the commit-race run needs at least five minutes of appends"
    project = f"ib-qual-{uuid.uuid4().hex[:12]}"
    catalog_port = _free_port()
    env = _isolated_compose_env(tmp_path, real_docker, project, catalog_port=catalog_port)
    barrier = CatalogBarrier(f"http://127.0.0.1:{catalog_port}", max_hold_s=120)
    barrier_port = barrier.start()
    spec_path = _barrier_spec(barrier_port, tmp_path)
    log_path = tmp_path / "barrier.log"
    command = [
        str(ROOT / "scripts/smoke.sh"),
        "--engine",
        "flink",
        "--spec",
        str(spec_path),
        "--set",
        f"duration_s={BARRIER_DURATION_S}",
        "--set",
        f"offered_bytes_per_s={OFFERED_RATE}",
        "--set",
        "partition_count=8",
        "--keep",
    ]
    run_dir: Path | None = None
    with log_path.open("w") as log:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, text=True)
        try:
            run_id, run_dir = _wait_for_run(log_path, process)
            env["RUN_DIR"] = str(run_dir)
            try:
                returncode = process.wait(timeout=BARRIER_DURATION_S + 1800)
            except subprocess.TimeoutExpired:
                process.terminate()
                pytest.fail(f"smoke exceeded its {BARRIER_DURATION_S + 1800}s bound:\n{_read_log(log_path)[-8000:]}")
            assert returncode == 0, _read_log(log_path)[-12000:]

            (run_dir / "smoke.log").write_text(_read_log(log_path))
            terminal = _terminal_exact(run_dir)
            compactor_log = _compactor_logs(project, env)
            (run_dir / "spark-compactor.log").write_text(compactor_log)
            rewrites = _rewrite_events(compactor_log)
            report = _scan_snapshots(project, run_id, run_dir, env)
            proxy = barrier.report()
            hold = cast(dict[str, object] | None, proxy["hold"])
            commits = cast(list[dict[str, object]], proxy["commits"])
            report.update(
                {
                    "fault": "held-commit",
                    "compose_project": project,
                    "configuration": {"duration_s": BARRIER_DURATION_S, "offered_rate": OFFERED_RATE},
                    "repository_revision": _run(["git", "rev-parse", "HEAD"], env=env).stdout.strip(),
                    "barrier": proxy,
                    "rewrite_events": len(rewrites),
                    "nonempty_rewrites": sum(e.status == "success" and e.rewritten_files > 0 for e in rewrites),
                    "rewrite_failures": sum(e.status == "failure" for e in rewrites),
                    "terminal_exactness": terminal,
                }
            )
            failure: str | None = None
            try:
                assert hold is not None, "no rewrite commit reached the barrier"
                assert hold["timed_out"] is False, f"no Flink append landed during the hold: {hold}"
                held = [c for c in commits if c["held"]]
                assert len(held) == 1
                later_success = [c for c in commits if not c["held"] and int(cast(int, c["status"])) < 300]
                if int(cast(int, held[0]["status"])) < 300:
                    # The catalog accepted a commit planned against a superseded snapshot
                    # only if its requirements still held; the row checks below decide.
                    report["outcome"] = "stale-commit-accepted"
                else:
                    assert later_success, "the stale commit was rejected and no later commit succeeded"
                    report["outcome"] = "rejected-then-retried"
                successes = [e for e in rewrites if e.status == "success" and e.rewritten_files > 0]
                assert successes, "no successful nonempty rewrite after the held commit"
                assert max((e.consecutive_failures for e in rewrites), default=0) < 3, "the sidecar gave up"
            except AssertionError as error:
                failure = str(error)
            report.update(
                {
                    "status": "failed" if failure else "passed",
                    "qualification_status": "unqualified" if failure else "qualified",
                    "qualification_failure": failure,
                }
            )
            (run_dir / "scores/concurrency-qualification.json").write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n"
            )
            if failure:
                pytest.fail(failure)
        finally:
            barrier.stop()
            spec_path.unlink(missing_ok=True)
            if process.poll() is None:
                process.terminate()
                process.wait(timeout=30)
            _save_service_logs(project, run_dir, env)
            if not KEEP:
                cleanup = _compose(project, "down", "-v", "--remove-orphans", env=env, check=False)
                if cleanup.returncode:
                    pytest.fail(f"could not clean owned Compose project {project}: {cleanup.stderr.strip()}")
