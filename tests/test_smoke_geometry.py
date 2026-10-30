# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Exercise local smoke geometry without starting Docker."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    any(shutil.which(tool) is None for tool in ("bash", "jq", "yq", "curl")),
    reason="smoke.sh requires bash, jq, yq and curl",
)
RUN_ID = "smoke-external-20260910T120000Z"
TABLE = "ingest_bench.t_smoke_external_20260910T120000Z"
GEOMETRY = {
    "final": {
        "live": {
            "files": 4,
            "size_quantiles": {"p50": 40_100_000.0},
            "small_file_share_32mib": 0.25,
        }
    }
}

DOCKER_STUB = r"""
printf '%s\n' "$*" >>"$STUB_DOCKER_LOG"
if [[ $* == *" ps --all --quiet spark-compactor" ]]; then
	printf '%s' "${STUB_COMPACTOR_IDS:-}"
	exit 0
fi
if [[ ${1:-} == container && ${2:-} == inspect ]]; then
	printf '%s\n' "${STUB_COMPACTOR_INSPECT:-[]}"
	exit 0
fi
if [[ ${1:-} == wait ]]; then
	printf '0\n'
	exit 0
fi
[[ ${1:-} == compose ]] || exit 0
command="${*: -1}"
case "$command" in
stage\ *)
	mkdir -p "$STUB_REPO/runs/$STUB_RUN_ID/scores"
	cp "$STUB_FACTS" "$STUB_REPO/runs/$STUB_RUN_ID/facts.json"
	# Staging copies the spec mounted at /spec.yaml into the run directory.
	for arg in "$@"; do
		if [[ $arg == *:/spec.yaml:ro ]]; then
			cp "${arg%:/spec.yaml:ro}" "$STUB_REPO/runs/$STUB_RUN_ID/spec.yaml"
		fi
	done
	cp "$STUB_SUMMARY" "$STUB_REPO/runs/$STUB_RUN_ID/scores/summary.json"
	if [[ -n ${STUB_COMPACTOR_ENV:-} ]]; then
		printf '%s\n' "$STUB_COMPACTOR_ENV" >"$STUB_REPO/runs/$STUB_RUN_ID/compactor-job.env"
	fi
	printf 'run_id: %s\n' "$STUB_RUN_ID"
	;;
file-sizes\ *)
	printf '%s\n' "$command" >>"$STUB_FILE_SIZES_LOG"
	if [[ $STUB_GEOMETRY_STATUS == 0 ]]; then
		printf '%s\n' "$STUB_GEOMETRY" >"$STUB_REPO/runs/$STUB_RUN_ID/scores/geometry.json"
	elif [[ $STUB_GEOMETRY_STATUS == 4 ]]; then
		printf '{"final": null}\n' >"$STUB_REPO/runs/$STUB_RUN_ID/scores/geometry.json"
	fi
	exit "$STUB_GEOMETRY_STATUS"
	;;
compaction\ *)
	printf '%s\n' "$command" >>"$STUB_COMPACTION_LOG"
	printf '%s\n' "$STUB_COMPACTION" >"$STUB_REPO/runs/$STUB_RUN_ID/scores/compaction.json"
	;;
table-metadata\ *) printf 's3://warehouse/metadata/v17.json\n' ;;
verify-table\ *)
	printf '{"verdict":true,"snapshot_id":17}\n' >"$STUB_REPO/runs/$STUB_RUN_ID/scores/terminal-exactness.json"
	;;
esac
"""


COMPACTION = {
    "tier": "lifecycle",
    "files": 2,
    "floor": 1,
    "floor_ratio": 2.0,
    "records_dropped": None,
}


@dataclass(frozen=True)
class SmokeRun:
    result: subprocess.CompletedProcess[str]
    docker_calls: list[str]
    file_sizes_calls: list[str]
    compaction_calls: list[str]
    run_dir: Path


def _run_smoke(
    tmp_path: Path,
    *,
    geometry_status: int = 0,
    offsets: list[int] | None = None,
    assignment: str | None = None,
    sidecar: bool = False,
    sidecar_state: str = "running",
) -> SmokeRun:
    root = tmp_path / "repo"
    for directory in ("scripts", "runs", "engines/flink", "compactors/spark_rewrite", "deploy/compose/local"):
        (root / directory).mkdir(parents=True, exist_ok=True)
    for name in ("smoke.sh", "_lib.sh"):
        shutil.copy2(REPO_ROOT / "scripts" / name, root / "scripts" / name)
    shutil.copy2(REPO_ROOT / "engines/flink/compose.yaml", root / "engines/flink/compose.yaml")
    for name in ("compose.sh", "compose.yaml"):
        shutil.copy2(REPO_ROOT / "compactors/spark_rewrite" / name, root / "compactors/spark_rewrite" / name)

    spec: dict[str, object] = {
        "name": "smoke-external",
        "engine": "external",
        "corpus": "smoke",
        "table": {"partition": "identity(partition_key)"},
        "producer": {"shards": 1},
        "scoring": {"geometry_offsets_s": [] if offsets is None else offsets},
    }
    if sidecar:
        spec["compaction"] = {"driver": "spark-rewrite", "interval_s": 15, "spark-rewrite": {}}
    spec_path = root / "runs/spec.yaml"
    spec_path.write_text(yaml.safe_dump(spec))

    facts = root / "facts.json"
    facts.write_text(
        json.dumps(
            {
                "bootstrap": "kafka:9092",
                "corpus_uri": "s3://corpus/smoke-test",
                "table": TABLE,
                "key_column": None,
                "value_encoding": "raw",
                "schema_id": None,
                **({"partition_assignment": assignment, "kafka_partitions": 8} if assignment else {}),
            }
        )
    )
    summary = root / "summary.json"
    summary.write_text(
        json.dumps(
            {
                "run_valid": True,
                "state": "drained",
                "reason": "",
                "producer_bound": False,
                "prefix": 10,
                "last_batch": 10,
                "committed_rows": 100,
                "offered_rows": 100,
                "freshness": {"window": {}},
                "exactness": {"exact": True, "loss_rows": 0, "duplicate_rows": 0},
                "keepup": {},
            }
        )
    )

    ready = root / "ready"
    ready.touch()
    docker_log = root / "docker.log"
    docker_log.touch()
    file_sizes_log = root / "file-sizes.log"
    file_sizes_log.touch()
    compaction_log = root / "compaction.log"
    compaction_log.touch()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    docker = bin_dir / "docker"
    docker.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{DOCKER_STUB}")
    docker.chmod(0o755)

    compactor_ids = ""
    compactor_inspect: list[dict[str, object]] = []
    if sidecar and sidecar_state != "empty":
        compactor_ids = "spark-compactor-1\n"
        running = sidecar_state in {"running", "unhealthy"}
        compactor_inspect = [
            {
                "Id": "spark-compactor-1",
                "Name": "/bench-spark-compactor-1",
                "Config": {"Image": "lakehouse-ingest-bench/spark:local"},
                "Image": "sha256:spark",
                "State": {
                    "Status": "running" if running else sidecar_state,
                    "ExitCode": 0 if running else 1,
                    "Error": "" if running else "rewrite loop failed",
                    "StartedAt": "2026-09-15T12:00:00Z",
                    "FinishedAt": "" if running else "2026-09-15T12:01:00Z",
                    **(
                        {"Health": {"Status": "healthy" if sidecar_state == "running" else "unhealthy"}}
                        if running
                        else {}
                    ),
                },
            }
        ]

    result = subprocess.run(
        [
            str(root / "scripts/smoke.sh"),
            "--engine",
            "external",
            "--spec",
            str(spec_path),
            "--external-ready-file",
            str(ready),
        ],
        cwd=root,
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "EPOCH_LEAD_S": "0",
            "STUB_DOCKER_LOG": str(docker_log),
            "STUB_FILE_SIZES_LOG": str(file_sizes_log),
            "STUB_COMPACTION_LOG": str(compaction_log),
            "STUB_COMPACTION": json.dumps(COMPACTION),
            "STUB_REPO": str(root),
            "STUB_RUN_ID": RUN_ID,
            "STUB_FACTS": str(facts),
            "STUB_SUMMARY": str(summary),
            "STUB_GEOMETRY": json.dumps(GEOMETRY),
            "STUB_GEOMETRY_STATUS": str(geometry_status),
            "STUB_COMPACTOR_IDS": compactor_ids,
            "STUB_COMPACTOR_INSPECT": json.dumps(compactor_inspect),
            **({"STUB_COMPACTOR_ENV": "COMPACTOR_LOCAL_CORES=2\nCOMPACTOR_DRIVER_MEM_MB=2048"} if sidecar else {}),
        },
    )
    return SmokeRun(
        result=result,
        docker_calls=docker_log.read_text().splitlines(),
        file_sizes_calls=file_sizes_log.read_text().splitlines(),
        compaction_calls=compaction_log.read_text().splitlines(),
        run_dir=root / "runs" / RUN_ID,
    )


@pytest.mark.parametrize("assignment", [None, "round_robin"])
def test_smoke_passes_staged_partition_assignment(tmp_path: Path, assignment: str | None) -> None:
    run = _run_smoke(tmp_path, assignment=assignment)
    assert run.result.returncode == 0, run.result.stderr
    producer = next(call for call in run.docker_calls if "produce --corpus" in call)
    if assignment is None:
        assert "--partition-assignment" not in producer and "--partitions" not in producer
    else:
        assert "--partition-assignment round_robin" in producer and "--partitions 8" in producer


def test_smoke_measures_geometry_through_the_live_catalog_before_teardown(tmp_path: Path) -> None:
    run = _run_smoke(tmp_path, offsets=[5, 10])
    assert run.result.returncode == 0, run.result.stderr
    assert len(run.file_sizes_calls) == 1
    command = run.file_sizes_calls[0]
    assert f"file-sizes --table {TABLE}" in command
    assert "--catalog-prop-file /catalog.props" in command
    assert "--epoch " in command
    assert f"--out /runs/{RUN_ID}/scores" in command
    assert "--offsets 5,10" in command
    assert (run.run_dir / "scores/geometry.json").exists()
    assert "geometry: p50 38.2 MiB, small (<32 MiB) 25%, 4 files" in run.result.stdout

    scorer = next(call for call in run.docker_calls if "score --corpus" in call)
    producer = next(call for call in run.docker_calls if "produce --corpus" in call)
    scorer_epoch = re.search(r"--epoch (\d+)", scorer)
    producer_epoch = re.search(r"--epoch (\d+)", producer)
    geometry_epoch = re.search(r"--epoch (\d+)", command)
    assert scorer_epoch is not None and producer_epoch is not None and geometry_epoch is not None
    assert scorer_epoch.group(1) == producer_epoch.group(1) == geometry_epoch.group(1)

    measured = next(index for index, call in enumerate(run.docker_calls) if "file-sizes --table" in call)
    teardown = next(index for index, call in enumerate(run.docker_calls) if " down -v --remove-orphans" in call)
    assert measured < teardown


@pytest.mark.parametrize(
    ("status", "message"),
    [(4, "no geometry: the table never committed"), (7, "file-sizes exited 7")],
)
def test_smoke_handles_geometry_exit_statuses(tmp_path: Path, status: int, message: str) -> None:
    run = _run_smoke(tmp_path, geometry_status=status)
    assert run.result.returncode == (0 if status == 4 else 1)
    assert message in run.result.stderr
    assert "geometry: p50" not in run.result.stdout
    assert "--offsets" not in run.file_sizes_calls[0]


def test_smoke_measures_compaction_against_the_same_table(tmp_path: Path) -> None:
    run = _run_smoke(tmp_path)
    assert run.result.returncode == 0, run.result.stderr
    assert len(run.compaction_calls) == 1
    command = run.compaction_calls[0]
    # The same table the scorer read, written beside the other scores. No
    # target flag: the command reads the table property and names its source.
    assert "--table " in command
    assert f"--out /runs/{RUN_ID}/scores" in command
    assert "--target-file-bytes" not in command
    assert (run.run_dir / "scores" / "compaction.json").exists()


def test_smoke_validates_a_pinned_snapshot_with_a_sidecar(tmp_path: Path) -> None:
    run = _run_smoke(tmp_path, sidecar=True)
    assert run.result.returncode == 0, run.result.stderr
    calls = "\n".join(run.docker_calls)
    assert "verify-table --metadata s3://warehouse/metadata/v17.json" in calls
    assert f"--publish-logs /runs/{RUN_ID}" in calls
    assert (run.run_dir / "scores/terminal-exactness.json").exists()


def test_smoke_starts_the_sidecar_through_the_drivers_own_hooks(tmp_path: Path) -> None:
    """The spec names the driver; the driver names its service. The script names neither."""
    with_sidecar = _run_smoke(tmp_path / "with", sidecar=True)
    assert with_sidecar.result.returncode == 0, with_sidecar.result.stderr
    calls = " ".join(with_sidecar.docker_calls)
    assert "up -d spark-compactor" in calls and "build spark-compactor" in calls
    status = json.loads((with_sidecar.run_dir / "scores/compactor-status.json").read_text())
    assert status["healthy"] is True
    assert status["containers"] == [
        {
            "id": "spark-compactor-1",
            "name": "bench-spark-compactor-1",
            "image": "lakehouse-ingest-bench/spark:local",
            "image_id": "sha256:spark",
            "state": "running",
            "health": "healthy",
            "exit_code": 0,
            "error": "",
            "started_at": "2026-09-15T12:00:00Z",
            "finished_at": "",
        }
    ]

    without = _run_smoke(tmp_path / "without")
    assert without.result.returncode == 0, without.result.stderr
    assert "spark-compactor" not in " ".join(without.docker_calls)


@pytest.mark.parametrize("sidecar_state", ["exited", "empty", "unhealthy"])
def test_smoke_rejects_an_unhealthy_or_missing_managed_sidecar(tmp_path: Path, sidecar_state: str) -> None:
    run = _run_smoke(tmp_path, sidecar=True, sidecar_state=sidecar_state)

    assert run.result.returncode != 0
    assert "managed compactor is not healthy" in run.result.stderr
    assert any(" down -v --remove-orphans" in call for call in run.docker_calls)
    status = json.loads((run.run_dir / "scores/compactor-status.json").read_text())
    assert status["healthy"] is False
    if sidecar_state == "exited":
        assert status["containers"][0]["state"] == "exited"
        assert status["containers"][0]["exit_code"] == 1
    elif sidecar_state == "empty":
        assert status["containers"] == []
    else:
        assert status["containers"][0]["state"] == "running"
        assert status["containers"][0]["health"] == "unhealthy"
