# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate all Compose profiles and interpolations before starting containers."""

from __future__ import annotations

import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from engines.flink import knobs as flink_knobs
from engines.spark import knobs, stream_to_iceberg
from ingest_bench.catalog import load_catalog_props
from ingest_bench.specs.model import load_site

REPO_ROOT = Path(__file__).resolve().parents[1]
LOCAL = REPO_ROOT / "deploy" / "compose" / "local"
COMPOSE = LOCAL / "docker-compose.yml"


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_compose_config_renders() -> None:
    out = subprocess.run(
        [
            "docker",
            "compose",
            "-f",
            str(COMPOSE),
            "--profile",
            "flink",
            "--profile",
            "flink-job",
            "--profile",
            "spark",
            "--profile",
            "tools",
            "config",
        ],
        capture_output=True,
        text=True,
        # Isolate rendering from the developer's exported engine settings.
        env={"PATH": "/usr/local/bin:/usr/bin:/bin", "RUN_DIR": "/tmp"},
    )
    assert out.returncode == 0, out.stderr
    for service in (
        "kafka",
        "minio",
        "iceberg-rest",
        "schema-registry",
        "harness",
        "flink-jobmanager",
        "flink-taskmanager",
        "flink-job",
        "spark-job",
    ):
        assert f"  {service}:" in out.stdout


@pytest.mark.skipif(shutil.which("docker") is None, reason="docker not installed")
def test_the_spark_service_submits_the_files_the_renderer_writes() -> None:
    service = yaml.safe_load((REPO_ROOT / "engines" / "spark" / "compose.yaml").read_text())["services"]["spark-job"]
    mount = str(stream_to_iceberg.RUN_DIR)
    assert service["volumes"] == [f"${{RUN_DIR:-/nonexistent/run}}:{mount}:ro"]
    command = service["command"]
    assert command[-1] == "/opt/bench/engines/spark/stream_to_iceberg.py"
    assert command[command.index("--properties-file") + 1] == f"{mount}/{knobs.CONF_FILE}"
    # The two variables the driver is sized by are the ones `job.env` sets.
    rendered = " ".join(command)
    for variable in (knobs.LOCAL_CORES_VAR, knobs.DRIVER_MEM_VAR):
        assert f"${{{variable}:-" in rendered


def test_the_flink_service_submits_the_files_the_renderer_writes() -> None:
    service = yaml.safe_load((REPO_ROOT / "engines" / "flink" / "compose.yaml").read_text())["services"]["flink-job"]
    mount = "/opt/bench/run"
    assert service["volumes"] == [f"${{RUN_DIR:-/nonexistent/run}}:{mount}:ro"]
    command = service["command"]
    assert command[:4] == ["flink", "run", "-d", "-m"]
    assert command[command.index("-c") + 1] == "org.ingestbench.flink.SqlRunner"
    assert command[command.index("-c") + 2] == "/opt/bench/sql-runner.jar"
    assert command[command.index("--sql") + 1] == f"{mount}/{flink_knobs.SQL_FILE}"
    assert command[command.index("--conf") + 1] == f"{mount}/{flink_knobs.CONF_FILE}"


def test_catalog_props_file_matches_the_site_catalog_block() -> None:
    assert load_catalog_props([], [str(LOCAL / "catalog.props")]) == load_site(LOCAL / "site.yaml").catalog_props


def test_local_site_addresses_the_compose_services() -> None:
    site = load_site(LOCAL / "site.yaml")
    services = set(yaml.safe_load(COMPOSE.read_text())["services"])
    assert site.kafka_bootstrap.split(":")[0] in services
    assert site.catalog_props["uri"].split("//")[1].split(":")[0] in services
    # The registry is reached by service name over the compose network, by the
    # harness that registers a schema and by the engine that reads it back.
    assert site.schema_registry is not None
    assert site.schema_registry.url.split("//")[1].split(":")[0] in services


@pytest.mark.skipif(shutil.which("git") is None, reason="git not installed")
def test_nothing_the_stack_mounts_is_excluded_from_the_repo() -> None:
    """Docker creates a directory for a missing bind-mount source, obscuring the
    missing configuration file. Root-only ignore rules must not exclude stack files.
    """
    mounted = [
        Path(volume.split(":", 1)[0])
        for service in yaml.safe_load(COMPOSE.read_text())["services"].values()
        for volume in service.get("volumes", [])
        if volume.startswith("./")
    ]
    assert mounted, "no relative bind mounts found; has the compose file's volume syntax changed?"
    for relative in mounted:
        path = (LOCAL / relative).resolve()
        assert path.is_file(), path
        check = subprocess.run(["git", "check-ignore", "-q", str(path)], cwd=LOCAL)
        assert check.returncode != 0, f"{path} is excluded by .gitignore but the stack mounts it"
