# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""The Flink maintenance sidecar: what it is given, and what it renders."""

import json
import os
import shlex
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest
import yaml

from compactors.flink_rewrite import fleet, knobs
from engines.java_properties import catalog_properties
from ingest_bench import uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import compactors, derive, model

ROOT = Path(__file__).resolve().parents[1]
STAMP = "20260919T000000Z"

SIZING = {"rewrite_parallelism": 6, "slots": 4, "tm_cpu": 4, "tm_mem_mb": 8192}


@pytest.fixture(scope="module")
def meta(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    p = preset.load_preset(
        "smoke",
        workloads_dir=ROOT / "workloads",
        overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("c"))
    generate.generate(p, out, seed=1, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(p, row_block=64, seed=1)))


def _cluster() -> model.KubernetesConfig:
    return model.KubernetesConfig(
        context="bench",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="registry.example/ingest-bench",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={"bench-pool": "engine"},
        tolerations=[{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}],
    )


def _site(cluster: model.KubernetesConfig | None = None) -> model.SiteConfig:
    props = {
        "uri": "http://iceberg-rest:8181",
        "warehouse": "s3://warehouse/",
        "s3.endpoint": "http://minio:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.path-style-access": "true",
        "s3.region": "us-east-1",
    }
    return model.SiteConfig(
        "s3://corpus", "s3://runs", "s3://warehouse", "kafka:9092", {}, None, props, cluster, 0.0, 0.0
    )


def _spec(
    tmp_path: Path, block: object = None, base: str = "smoke-external.yaml", name: str | None = None
) -> model.RunSpec:
    raw = yaml.safe_load((ROOT / "specs" / "smoke" / base).read_text())
    raw["compaction"] = {
        "driver": "flink-rewrite",
        "interval_s": 600,
        "flink-rewrite": dict(SIZING) if block is None else block,
    }
    if name is not None:
        raw["name"] = name
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return model.load_run_spec(path)


def _derived(spec: model.RunSpec, site: model.SiteConfig, meta: metadata.CorpusMetadata) -> derive.Derived:
    return derive.derive(spec, site, stamp=STAMP, corpus_dir=meta.name + "-x")


def _block(spec: model.RunSpec) -> dict[str, object]:
    assert spec.compaction is not None
    return spec.compaction.driver_block


def _deployment(spec: model.RunSpec, site: model.SiteConfig, meta: metadata.CorpusMetadata) -> dict[str, object]:
    return cast(
        dict[str, object],
        yaml.safe_load(knobs.render_flinkdeployment(spec, site, _derived(spec, site, meta), "abc123")),
    )


# ---------------------------------------------------------------------------
# Registration and knobs
# ---------------------------------------------------------------------------


def test_the_sidecar_is_a_registered_rewrite_driver() -> None:
    assert compactors.knobs_for("flink-rewrite") is knobs
    assert compactors.fleet_for("flink-rewrite") is fleet
    assert compactors.kubernetes_for("flink-rewrite") is knobs.KUBERNETES


def test_sizing_is_required_and_the_rest_has_defaults() -> None:
    read = knobs.read(dict(SIZING))
    assert (read.jm_cpu, read.jm_mem_mb, read.rate_limit_s, read.lock_check_delay_s) == (1, 2048, 15, 10)
    assert read.rewrite_options == {} and read.extra_flink_conf == {} and read.machine_type is None
    assert read.taskmanagers() == 2, "six rewrite subtasks over four slots need two TaskManagers"
    with pytest.raises(ValueError, match=r"must set \['slots', 'tm_mem_mb'\]"):
        knobs.read({"rewrite_parallelism": 6, "tm_cpu": 4})
    with pytest.raises(ValueError, match="unknown keys"):
        knobs.read({**SIZING, "executors": 2})


@pytest.mark.parametrize("name", sorted(knobs.REQUIRED_KNOBS) + ["jm_cpu", "rate_limit_s"])
def test_a_sidecar_with_no_compute_or_cadence_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match=f"{name} must be at least 1"):
        knobs.read({**SIZING, name: 0})


def test_rewrite_options_are_checked_against_the_builder() -> None:
    """Iceberg's Flink planner ignores option names it does not know, so the driver refuses them first."""
    options = {"min-input-files": "8", "min-file-size-bytes": "268435456", "partial-progress.enabled": "false"}
    assert knobs.read({**SIZING, "rewrite_options": options}).rewrite_options == options
    with pytest.raises(ValueError, match="takes no setting for"):
        knobs.read({**SIZING, "rewrite_options": {"min_input_files": "8"}})
    with pytest.raises(ValueError, match="may not set 'target-file-size-bytes'"):
        knobs.read({**SIZING, "rewrite_options": {"target-file-size-bytes": "536870912"}})
    with pytest.raises(ValueError, match="must be true or false"):
        knobs.read({**SIZING, "rewrite_options": {"rewrite-all": "yes"}})
    assert knobs.read({**SIZING, "rewrite_options": {"rewrite-job-order": "bytes-desc"}}).rewrite_options == {
        "rewrite-job-order": "bytes-desc"
    }
    with pytest.raises(ValueError, match="must be one of"):
        knobs.read({**SIZING, "rewrite_options": {"rewrite-job-order": "largest-first"}})
    with pytest.raises(ValueError, match="non-negative integer written as a string"):
        knobs.read({**SIZING, "rewrite_options": {"min-input-files": "8 files"}})
    with pytest.raises(ValueError, match="quote it in the YAML"):
        knobs.read({**SIZING, "rewrite_options": {"min-input-files": 8}})


def test_validate_accepts_a_declared_sidecar(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    knobs.validate(_block(spec), spec, meta)


def test_a_rate_limit_longer_than_the_interval_is_refused(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "rate_limit_s": 601})
    with pytest.raises(ValueError, match="rate_limit_s 601 is longer than spec.compaction.interval_s 600"):
        knobs.validate(_block(spec), spec, meta)


def test_a_name_the_operator_would_refuse_is_refused_first(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """The spec's name limit leaves room for the engine's deployment name but not for the suffix."""
    longest = "a" * 35
    assert model.NAME_RE.match(longest) and not model.NAME_RE.match(longest + "a")
    assert len(knobs.kubernetes_name(f"{longest}-{STAMP}")) > knobs.OBJECT_NAME_LIMIT
    fits = _spec(tmp_path, name="b" * 26)
    knobs.validate(_block(fits), fits, meta)
    spec = _spec(tmp_path, name="c" * 27)
    with pytest.raises(
        ValueError, match="past the 45 the Flink operator accepts. Name this run in at most 26 characters"
    ):
        knobs.validate(_block(spec), spec, meta)


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


def test_rendering_a_run_with_no_sidecar_says_so(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml")
    site = _site()
    with pytest.raises(ValueError, match="declares no maintenance sidecar"):
        knobs.render(spec, site, _derived(spec, site, meta), meta)


def test_the_sidecar_gets_its_own_object_names(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """It runs beside the engine, which may be a FlinkDeployment of its own."""
    from engines.flink import knobs as engine

    spec = _spec(tmp_path, base="smoke-flink.yaml")
    site = _site(_cluster())
    derived = _derived(spec, site, meta)
    document = _deployment(spec, site, meta)
    name = cast(dict[str, object], document["metadata"])["name"]
    assert name == derived.run_id.lower() + "-m"
    assert name != engine.kubernetes_name(derived.run_id)
    configmap = cast(dict[str, object], yaml.safe_load(knobs.render_configmap(spec, site, derived)))
    assert cast(dict[str, object], configmap["metadata"])["name"] == f"{name}-job"
    assert list(cast(dict[str, object], configmap["data"])) == [knobs.JOB_FILE]


def test_the_deployment_runs_the_maintenance_runner_on_the_engines_image(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _spec(tmp_path, {**SIZING, "jm_cpu": 2, "jm_mem_mb": 4096})
    site = _site(_cluster())
    body = cast(dict[str, object], _deployment(spec, site, meta)["spec"])
    assert body["image"] == "registry.example/ingest-bench/lakehouse-ingest-bench/flink:abc123"
    assert (body["flinkVersion"], body["mode"], body["serviceAccount"]) == ("v2_2", "standalone", "ingest-bench-flink")
    job = cast(dict[str, object], body["job"])
    assert job["jarURI"] == "local:///opt/bench/maintenance-runner.jar"
    assert job["entryClass"] == "org.ingestbench.flink.MaintenanceRunner"
    assert job["args"] == ["--maintenance", "/opt/bench/run/compactor-job.yaml"]
    assert (job["parallelism"], job["upgradeMode"], job["state"]) == (6, "stateless", "running")
    assert body["jobManager"] == {"resource": {"memory": "4096m", "cpu": 2}}
    assert body["taskManager"] == {"resource": {"memory": "8192m", "cpu": 4}, "replicas": 2}
    conf = cast(dict[str, str], body["flinkConfiguration"])
    assert conf["parallelism.default"] == "6" and conf["taskmanager.numberOfTaskSlots"] == "4"


def test_the_sidecar_keeps_no_checkpoint_and_is_never_restarted(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """A checkpoint would wait on the group in flight; a restart would compact one stretch twice."""
    conf = knobs.render_conf(_spec(tmp_path))
    assert not any(key.startswith("execution.checkpointing") for key in conf), sorted(conf)
    assert conf["restart-strategy.type"] == "none"
    assert conf["taskmanager.memory.managed.size"] == "0m"


def test_the_pods_share_the_engines_template(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    site = _site(replace(_cluster(), secret_name="bench-env"))
    derived = _derived(spec, site, meta)
    pod = cast(dict[str, object], cast(dict[str, object], _deployment(spec, site, meta)["spec"])["podTemplate"])
    pod_spec = cast(dict[str, object], pod["spec"])
    assert pod_spec["nodeSelector"] == {"bench-pool": "engine"}
    assert pod_spec["tolerations"] == [{"key": "bench", "operator": "Exists", "effect": "NoSchedule"}]
    assert pod_spec["volumes"] == [{"name": "job", "configMap": {"name": knobs.configmap_name(derived)}}]
    container = cast(dict[str, object], cast(list[object], pod_spec["containers"])[0])
    assert container["name"] == "flink-main-container"
    assert container["volumeMounts"] == [{"name": "job", "mountPath": "/opt/bench/run", "readOnly": True}]
    assert container["envFrom"] == [{"secretRef": {"name": "bench-env"}}]
    assert {cast(dict[str, str], env)["name"] for env in cast(list[object], container["env"])} == {
        "AWS_REGION",
        "AWS_DEFAULT_REGION",
    }


def test_the_sidecar_can_take_nodes_of_its_own(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    block = dict(SIZING) | {
        "node_selector": {"bench-pool": "maintenance"},
        "tolerations": [{"key": "bench-pool", "operator": "Equal", "value": "maintenance", "effect": "NoSchedule"}],
    }
    spec = _spec(tmp_path, block)
    pod = cast(
        dict[str, object], cast(dict[str, object], _deployment(spec, _site(_cluster()), meta)["spec"])["podTemplate"]
    )
    pod_spec = cast(dict[str, object], pod["spec"])
    # The sidecar's placement is added to the site's, which keeps applying.
    assert pod_spec["nodeSelector"] == {"bench-pool": "maintenance"}
    assert pod_spec["tolerations"] == [
        {"key": "bench", "operator": "Exists", "effect": "NoSchedule"},
        {"key": "bench-pool", "operator": "Equal", "value": "maintenance", "effect": "NoSchedule"},
    ]
    with pytest.raises(ValueError, match="tolerations"):
        knobs.read(dict(SIZING) | {"tolerations": {"key": "bench-pool"}})


def test_the_job_names_the_table_the_cadence_and_the_catalog(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    options = {"min-input-files": "8", "min-file-size-bytes": "268435456", "rewrite-all": "false"}
    spec = _spec(tmp_path, {**SIZING, "rewrite_options": options, "rate_limit_s": 20, "lock_check_delay_s": 5})
    site = _site()
    derived = _derived(spec, site, meta)
    job = cast(dict[str, object], yaml.safe_load(knobs.render_job(spec, site, derived)))
    assert job["catalog"] == "ice" and job["table"] == derived.table and job["uid_suffix"] == derived.run_id
    assert (job["interval_s"], job["parallelism"], job["rate_limit_s"], job["lock_check_delay_s"]) == (600, 6, 20, 5)
    assert job["max_read_back"] == knobs.MAX_READ_BACK
    assert job["rewrite_options"] == options
    props = cast(dict[str, str], job["catalog_properties"])
    assert props == catalog_properties(site.catalog_props, site.warehouse, engine="Flink")
    assert props["uri"] == "http://iceberg-rest:8181" and "type" not in props and "catalog-type" not in props


def test_a_setting_nothing_can_resolve_is_refused(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "extra_flink_conf": {"s3.secret-key": "${env:SECRET}"}})
    with pytest.raises(ValueError, match=r"renders \['s3.secret-key'\] as Flink settings"):
        knobs.render_conf(spec)


def test_extra_flink_conf_is_applied_last(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "extra_flink_conf": {"taskmanager.memory.managed.size": "512m"}})
    assert knobs.render_conf(spec)["taskmanager.memory.managed.size"] == "512m"


def test_a_cluster_run_renders_manifests_and_a_local_one_renders_the_job(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _spec(tmp_path)
    local = _site()
    assert set(knobs.render(spec, local, _derived(spec, local, meta), meta)) == {
        knobs.JOB_FILE,
        knobs.CONF_FILE,
        knobs.ENV_FILE,
    }
    cluster = _site(_cluster())
    derived = _derived(spec, cluster, meta)
    with pytest.raises(ValueError, match="requires image_tag"):
        knobs.render(spec, cluster, derived, meta)
    files = knobs.render(spec, cluster, derived, meta, image_tag="abc123")
    assert set(files) == {
        knobs.JOB_FILE,
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        compactors.DOCUMENT_FILE,
        compactors.CONFIGMAP_FILE,
    }
    assert cast(dict[str, object], yaml.safe_load(files[compactors.DOCUMENT_FILE]))["kind"] == "FlinkDeployment"


def test_the_local_stack_learns_the_sidecars_shape(tmp_path: Path) -> None:
    assert knobs.render_env(_spec(tmp_path)) == (
        "COMPACTOR_TASKMANAGERS=2\nCOMPACTOR_SLOTS=4\nCOMPACTOR_TM_MEM_MB=8192\nCOMPACTOR_JM_MEM_MB=2048\n"
        "COMPACTOR_PARALLELISM=6\n"
    )


def test_local_start_exports_the_staged_flink_configuration(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(
        tmp_path,
        {
            **SIZING,
            "extra_flink_conf": {
                "custom.marker": "p$1\\w",
                "taskmanager.memory.managed.size": "512m",
            },
        },
    )
    site = _site()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    for name, text in knobs.render(spec, site, _derived(spec, site, meta), meta).items():
        (run_dir / name).write_text(text)
    captured = tmp_path / "flink-properties.yaml"
    script = f"""
set -euo pipefail
RUN_DIR={shlex.quote(str(run_dir))}
RUN_ID=run
log() {{ :; }}
die() {{ exit 90; }}
compose() {{
    if [[ $1 == up ]]; then
        printf '%s' "${{COMPACTOR_FLINK_PROPERTIES-}}" >{shlex.quote(str(captured))}
        exit 0
    fi
}}
source {shlex.quote(str(ROOT / "compactors/flink_rewrite/compose.sh"))}
compactor_compose_start
"""
    subprocess.run(["bash", "-c", script], check=True)

    lines = captured.read_text().splitlines()
    assert "custom.marker: p$1\\w" in lines
    assert "parallelism.default: 6" in lines
    assert "taskmanager.memory.managed.size: 512m" in lines
    assert "taskmanager.numberOfTaskSlots: 4" in lines


@pytest.mark.skipif(shutil.which("docker") is None, reason="Docker Compose is not installed")
def test_local_compose_applies_the_staged_configuration_to_every_flink_process(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _spec(tmp_path, {**SIZING, "extra_flink_conf": {"custom.marker": "p$1\\w"}})
    site = _site()
    rendered = knobs.render(spec, site, _derived(spec, site, meta), meta)
    environment = {
        **os.environ,
        "RUN_DIR": str(tmp_path),
        "COMPACTOR_FLINK_PROPERTIES": rendered[knobs.CONF_FILE],
    }
    result = subprocess.run(
        [
            "docker",
            "compose",
            "--profile",
            "compaction",
            "--profile",
            "compaction-job",
            "-f",
            str(ROOT / "deploy/compose/local/docker-compose.yml"),
            "config",
            "--format",
            "json",
        ],
        check=True,
        capture_output=True,
        env=environment,
        text=True,
    )
    services = json.loads(result.stdout)["services"]
    for service in ("flink-compactor-jobmanager", "flink-compactor-taskmanager", "flink-compactor-job"):
        lines = services[service]["environment"]["FLINK_PROPERTIES"].splitlines()
        # Compose escapes a literal dollar as $$ when serializing its reusable config model.
        assert "custom.marker: p$$1\\w" in lines
        assert "parallelism.default: 6" in lines


# ---------------------------------------------------------------------------
# Fleet
# ---------------------------------------------------------------------------


def test_the_sidecar_holds_its_own_fleet_for_the_whole_run(tmp_path: Path) -> None:
    roles = fleet.fleet(_spec(tmp_path, {**SIZING, "machine_type": "r6i.4xlarge"}))
    assert [(role.role, role.count, role.vcpu, role.gib) for role in roles] == [
        ("compactor-jobmanager", 1, 1.0, 2.0),
        ("compactor-taskmanager", 2, 4.0, 8.0),
    ]
    assert {role.machine_type for role in roles} == {"r6i.4xlarge"}


def test_an_undeclared_machine_type_uses_the_shared_sentinel(tmp_path: Path) -> None:
    assert {role.machine_type for role in fleet.fleet(_spec(tmp_path))} == {model.MACHINE_TYPE_UNSPECIFIED}


def test_the_sidecars_roles_do_not_collide_with_the_engines(tmp_path: Path) -> None:
    from engines.flink import fleet as engine_fleet

    spec = _spec(tmp_path, base="smoke-flink.yaml")
    assert {role.role for role in engine_fleet.fleet(spec)}.isdisjoint({role.role for role in fleet.fleet(spec)})


def test_a_run_with_no_sidecar_has_no_compactor_fleet() -> None:
    with pytest.raises(ValueError, match="no compaction layer"):
        fleet.fleet(model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml"))


@pytest.mark.parametrize("machine_type", [None, "m8g.4xlarge"])
@pytest.mark.parametrize("coordinator_type", [None, "m8g.2xlarge"])
def test_coordinator_machine_type_only_changes_fleet_metadata(
    tmp_path: Path, meta: metadata.CorpusMetadata, machine_type: str | None, coordinator_type: str | None
) -> None:
    spec = _spec(tmp_path)
    block: dict[str, object] = dict(SIZING)
    if machine_type is not None:
        block["machine_type"] = machine_type
    if coordinator_type is not None:
        block["jm_machine_type"] = coordinator_type
    pinned = _spec(tmp_path, block)
    roles = {role.role: role for role in fleet.fleet(pinned)}
    assert roles["compactor-jobmanager"].machine_type == (
        coordinator_type or machine_type or model.MACHINE_TYPE_UNSPECIFIED
    )
    assert roles["compactor-taskmanager"].machine_type == (machine_type or model.MACHINE_TYPE_UNSPECIFIED)
    for site in (_site(), _site(_cluster())):
        d = _derived(spec, site, meta)
        assert knobs.render(pinned, site, d, meta, image_tag="t") == knobs.render(spec, site, d, meta, image_tag="t")


def test_coordinator_machine_type_requires_a_string() -> None:
    with pytest.raises(ValueError, match="jm_machine_type must be a string"):
        knobs.read({**SIZING, "jm_machine_type": 8})
