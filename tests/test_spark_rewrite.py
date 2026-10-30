# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""The periodic Spark rewrite sidecar: what it is given, and what it does."""

import json
import subprocess
import sys
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast

import pytest
import yaml

from compactors.spark_rewrite import fleet, knobs, rewrite_loop
from ingest_bench import stage, uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.specs import compactors, derive, model

ROOT = Path(__file__).resolve().parents[1]
STAMP = "20260908T000000Z"

SIZING = {"executors": 2, "executor_cores": 4, "executor_mem_mb": 8192}


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


def _spec(tmp_path: Path, block: object = None, base: str = "smoke-external.yaml") -> model.RunSpec:
    raw = yaml.safe_load((ROOT / "specs" / "smoke" / base).read_text())
    raw["compaction"] = {
        "driver": "spark-rewrite",
        "interval_s": 600,
        "spark-rewrite": dict(SIZING) if block is None else block,
    }
    path = tmp_path / "spec.yaml"
    path.write_text(yaml.safe_dump(raw))
    return model.load_run_spec(path)


def _derived(spec: model.RunSpec, site: model.SiteConfig, meta: metadata.CorpusMetadata) -> derive.Derived:
    return derive.derive(spec, site, stamp=STAMP, corpus_dir=meta.name + "-x")


def _block(spec: model.RunSpec) -> dict[str, object]:
    assert spec.compaction is not None
    return spec.compaction.driver_block


# ---------------------------------------------------------------------------
# Registration and knobs
# ---------------------------------------------------------------------------


def test_the_sidecar_is_the_registered_rewrite_driver() -> None:
    assert compactors.knobs_for("spark-rewrite") is knobs
    assert compactors.fleet_for("spark-rewrite") is fleet


def test_sizing_is_required_and_the_rest_has_defaults() -> None:
    read = knobs.read(dict(SIZING))
    assert read.driver_cores == 1 and read.driver_mem_mb == 2048 and read.start_delay_s == 0
    assert read.rewrite_options == {} and read.extra_spark_conf == {} and read.machine_type is None
    with pytest.raises(ValueError, match="must set"):
        knobs.read({"executors": 2})
    with pytest.raises(ValueError, match="unknown keys"):
        knobs.read({**SIZING, "exectuors": 2})


@pytest.mark.parametrize("name", ["executors", "executor_cores", "driver_cores"])
def test_a_sidecar_with_no_compute_is_refused(name: str) -> None:
    with pytest.raises(ValueError, match=f"{name} must be at least 1"):
        knobs.read({**SIZING, name: 0})


@pytest.mark.parametrize("delay", [-1, True, "600"])
def test_a_start_delay_must_be_whole_non_negative_seconds(delay: object) -> None:
    with pytest.raises(ValueError, match="start_delay_s"):
        knobs.read({**SIZING, "start_delay_s": delay})


def test_the_target_file_size_is_refused_as_a_rewrite_option() -> None:
    """The table property is the single declaration of the target.

    Iceberg reads the target from `write.target-file-size-bytes`, which
    `spec.table.properties` sets once and the reading of what the rewrite
    achieved reads back. A rewrite option of the same name would let the sidecar
    rewrite to a size the spec and the table property both deny, and the Flink
    driver already refuses it.
    """
    sizing = {"executors": 1, "executor_cores": 1, "executor_mem_mb": 2048}
    with pytest.raises(ValueError, match="may not set 'target-file-size-bytes'"):
        knobs.read({**sizing, "rewrite_options": {"target-file-size-bytes": "33554432"}})
    assert knobs.read({**sizing, "rewrite_options": {"min-input-files": "8"}}).rewrite_options == {
        "min-input-files": "8"
    }


def test_rewrite_options_are_read_as_strings() -> None:
    read = knobs.read({**SIZING, "rewrite_options": {"min-input-files": "8"}})
    assert read.rewrite_options == {"min-input-files": "8"}
    with pytest.raises(ValueError, match="must be a string"):
        knobs.read({**SIZING, "rewrite_options": {"min-input-files": 8}})


def test_validate_accepts_a_declared_sidecar(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    knobs.validate(_block(spec), spec, meta)


def test_the_longest_run_name_still_fits_the_sidecars_label() -> None:
    """The spec's name limit, the run stamp and the suffix have to stay compatible."""
    longest = "a" * 35
    assert model.NAME_RE.match(longest) and not model.NAME_RE.match(longest + "a")
    assert len(knobs.kubernetes_name(f"{longest}-{STAMP}")) <= knobs.LABEL_VALUE_LIMIT


def test_rendering_a_run_with_no_sidecar_says_so(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml")
    with pytest.raises(ValueError, match="no rewrite sidecar"):
        knobs.render_job(spec, _derived(spec, _site(), meta))


# ---------------------------------------------------------------------------
# The job document
# ---------------------------------------------------------------------------


def test_the_job_names_the_table_and_the_cadence(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    site = _site()
    document = json.loads(knobs.render_job(spec, _derived(spec, site, meta)))
    assert document["table"] == _derived(spec, site, meta).table
    assert document["interval_s"] == 600 and document["catalog"] == "ice" and document["options"] == {}
    assert document["start_delay_s"] == 0


def test_the_job_carries_the_start_delay(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "start_delay_s": 600})
    assert json.loads(knobs.render_job(spec, _derived(spec, _site(), meta)))["start_delay_s"] == 600


def test_the_job_carries_no_target_file_size(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """One source of truth: the table property the spec sets and the reading reads back."""
    spec = _spec(tmp_path, {**SIZING, "rewrite_options": {"min-input-files": "5"}})
    rendered = knobs.render_job(spec, _derived(spec, _site(), meta))
    assert "target-file-size-bytes" not in rendered
    assert json.loads(rendered)["options"] == {"min-input-files": "5"}


# ---------------------------------------------------------------------------
# The manifests
# ---------------------------------------------------------------------------


def test_the_sidecar_gets_its_own_object_names(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """It runs beside the engine, which may be a SparkApplication of its own."""
    spec = _spec(tmp_path, base="smoke-spark.yaml")
    site = _site(_cluster())
    derived = _derived(spec, site, meta)
    document = cast(dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, derived, "abc123")))
    name = cast(dict[str, object], document["metadata"])["name"]
    assert name == derived.run_id.lower() + "-compact"
    assert name != derived.run_id.lower()
    configmap = cast(dict[str, object], yaml.safe_load(knobs.render_configmap(spec, site, derived)))
    assert cast(dict[str, object], configmap["metadata"])["name"] == f"{name}-job"


def test_the_sidecar_is_never_restarted(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """A restart would compact one stretch twice and leave a gap where it was down."""
    spec = _spec(tmp_path)
    site = _site(_cluster())
    document = cast(
        dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, _derived(spec, site, meta), "t"))
    )
    assert cast(dict[str, object], document["spec"])["restartPolicy"] == {"type": "Never"}


def test_the_pods_are_sized_for_guaranteed_service(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    site = _site(_cluster())
    rendered = cast(
        dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, _derived(spec, site, meta), "t"))
    )
    body = cast(dict[str, object], rendered["spec"])
    for role, cores in (("driver", 1), ("executor", 4)):
        pod = cast(dict[str, object], body[role])
        assert pod["cores"] == cores and pod["coreLimit"] == str(cores)
        assert pod["nodeSelector"] == {"bench-pool": "engine"}
        assert [entry["name"] for entry in cast(list[dict[str, str]], pod["env"])] == [
            "AWS_REGION",
            "AWS_DEFAULT_REGION",
        ]
    assert cast(dict[str, object], body["executor"])["instances"] == 2


def test_the_sidecar_can_take_nodes_of_its_own(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    """Both arms have to be movable off the engine's nodes or the readings differ by placement.

    Sharing nodes with ingest cost the Flink arm 194 s of p95 freshness against
    62.5 s apart, so a comparison where only one arm can be isolated compares
    placement as much as compactor.
    """
    block = dict(SIZING) | {
        "node_selector": {"bench-pool": "maintenance"},
        "tolerations": [{"key": "bench-pool", "operator": "Equal", "value": "maintenance", "effect": "NoSchedule"}],
    }
    spec = _spec(tmp_path, block)
    site = _site(_cluster())
    rendered = cast(
        dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, _derived(spec, site, meta), "t"))
    )
    body = cast(dict[str, object], rendered["spec"])
    for role in ("driver", "executor"):
        pod = cast(dict[str, object], body[role])
        # The sidecar's placement is added to the site's, which keeps applying.
        assert pod["nodeSelector"] == {"bench-pool": "maintenance"}
        assert pod["tolerations"] == [
            {"key": "bench", "operator": "Exists", "effect": "NoSchedule"},
            {"key": "bench-pool", "operator": "Equal", "value": "maintenance", "effect": "NoSchedule"},
        ]
    with pytest.raises(ValueError, match="tolerations"):
        knobs.read(dict(SIZING) | {"tolerations": {"key": "bench-pool"}})


def test_slots_beyond_the_vcpu_overlap_tasks_without_asking_the_node_for_more(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    """Spark schedules by slots; the pod still requests and is costed at its vCPU."""
    spec = _spec(tmp_path, {**SIZING, "executor_slots": 12})
    site = _site(_cluster())
    derived = _derived(spec, site, meta)
    rendered = cast(dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, derived, "t")))
    executor = cast(dict[str, object], cast(dict[str, object], rendered["spec"])["executor"])
    assert executor["cores"] == 12 and executor["coreRequest"] == "4" and executor["coreLimit"] == "4"
    conf = knobs.render_conf(spec, site, derived)
    assert conf["spark.executor.cores"] == "12" and conf["spark.sql.shuffle.partitions"] == "24"
    assert knobs.render_env(spec).startswith("COMPACTOR_LOCAL_CORES=24\n")
    # The fleet the run is charged for does not grow with slots.
    assert knobs.read({**SIZING, "executor_slots": 12}).cores_total() == 8
    with pytest.raises(ValueError, match="executor_slots must be at least 1"):
        knobs.read({**SIZING, "executor_slots": 0})


def test_the_pods_take_the_sites_environment_secret(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path)
    site = _site(replace(_cluster(), secret_name="bench-env"))
    rendered = cast(
        dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, _derived(spec, site, meta), "t"))
    )
    body = cast(dict[str, object], rendered["spec"])
    for role in ("driver", "executor"):
        assert cast(dict[str, object], body[role])["envFrom"] == [{"secretRef": {"name": "bench-env"}}]


def test_the_local_submission_learns_its_cores_and_heap(tmp_path: Path) -> None:
    """`local[N]` is an argument to spark-submit, not a setting it can read."""
    rendered = knobs.render_env(_spec(tmp_path, {**SIZING, "driver_mem_mb": 4096}))
    assert rendered == "COMPACTOR_LOCAL_CORES=8\nCOMPACTOR_DRIVER_MEM_MB=4096\n"


def test_the_settings_reach_the_catalog_and_the_iceberg_extensions(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _spec(tmp_path)
    site = _site()
    conf = knobs.render_conf(spec, site, _derived(spec, site, meta))
    assert conf["spark.sql.catalog.ice.uri"] == "http://iceberg-rest:8181"
    assert "IcebergSparkSessionExtensions" in conf["spark.sql.extensions"]
    # One shuffle partition per executor core, so a sort does not undo the rewrite.
    assert conf["spark.sql.shuffle.partitions"] == "8"


def test_a_setting_nothing_can_resolve_is_refused(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "extra_spark_conf": {"spark.hadoop.fs.s3a.access.key": "${env:KEY}"}})
    site = _site()
    with pytest.raises(ValueError, match="resolves"):
        knobs.render_conf(spec, site, _derived(spec, site, meta))


def test_a_cluster_run_renders_manifests_and_a_local_one_renders_the_job(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _spec(tmp_path)
    local = _site()
    assert set(knobs.render(spec, local, _derived(spec, local, meta), meta)) == {
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.JOB_FILE,
    }
    cluster = _site(_cluster())
    derived = _derived(spec, cluster, meta)
    assert set(knobs.render(spec, cluster, derived, meta, image_tag="t")) == {
        knobs.CONF_FILE,
        knobs.ENV_FILE,
        knobs.JOB_FILE,
        compactors.DOCUMENT_FILE,
        compactors.CONFIGMAP_FILE,
    }
    with pytest.raises(ValueError, match="image_tag"):
        knobs.render(spec, cluster, derived, meta)


# ---------------------------------------------------------------------------
# The fleet it holds
# ---------------------------------------------------------------------------


def test_the_sidecar_holds_its_own_fleet_for_the_whole_run(tmp_path: Path) -> None:
    roles = fleet.fleet(_spec(tmp_path, {**SIZING, "machine_type": "m6i.2xlarge"}))
    assert [(role.role, role.count, role.vcpu, role.gib) for role in roles] == [
        ("compactor-driver", 1, 1.0, 2.0),
        ("compactor-executor", 2, 4.0, 8.0),
    ]
    assert {role.machine_type for role in roles} == {"m6i.2xlarge"}


def test_an_undeclared_machine_type_uses_the_shared_sentinel(tmp_path: Path) -> None:
    roles = fleet.fleet(_spec(tmp_path))
    assert {role.machine_type for role in roles} == {model.MACHINE_TYPE_UNSPECIFIED}


def test_the_sidecars_roles_do_not_collide_with_the_engines(tmp_path: Path) -> None:
    from engines.spark import fleet as engine_fleet

    spec = _spec(tmp_path, base="smoke-spark.yaml")
    engine = {role.role for role in engine_fleet.fleet(spec)}
    assert engine.isdisjoint({role.role for role in fleet.fleet(spec)})


def test_a_run_with_no_sidecar_has_no_compactor_fleet() -> None:
    with pytest.raises(ValueError, match="no compaction layer"):
        fleet.fleet(model.load_run_spec(ROOT / "specs/smoke/smoke-external.yaml"))


# ---------------------------------------------------------------------------
# What the job does once it is running
# ---------------------------------------------------------------------------


def _job(options: dict[str, str] | None = None, start_delay_s: int = 0) -> rewrite_loop.Job:
    return rewrite_loop.Job(
        catalog="ice",
        table="bench.events",
        interval_s=600,
        start_delay_s=start_delay_s,
        options={} if options is None else options,
    )


class _FirstDataRow:
    def __init__(self, micros: int | None) -> None:
        self.micros = micros

    def asDict(self, *, recursive: bool = False) -> dict[str, int | None]:  # noqa: N802 - PySpark API
        assert recursive
        return {"first_data_micros": self.micros}


class _RewriteResultRow:
    """The part of PySpark Row's boundary that the loop consumes."""

    def __init__(self, rewritten: int = 0, added: int = 0, rewritten_bytes: int = 0) -> None:
        self.values = {
            "rewritten_data_files_count": rewritten,
            "added_data_files_count": added,
            "rewritten_bytes_count": rewritten_bytes,
        }

    def asDict(self, *, recursive: bool = False) -> dict[str, int]:  # noqa: N802 - PySpark API
        assert recursive
        return self.values


def _result(rewritten: int = 0, added: int = 0, rewritten_bytes: int = 0) -> list[_RewriteResultRow]:
    return [_RewriteResultRow(rewritten, added, rewritten_bytes)]


def _events(captured: pytest.CaptureFixture[str]) -> tuple[list[dict[str, object]], list[dict[str, object]]]:
    streams = captured.readouterr()
    return (
        [json.loads(line) for line in streams.out.splitlines()],
        [json.loads(line) for line in streams.err.splitlines()],
    )


def test_the_loop_imports_with_python_310s_datetime_api() -> None:
    path = ROOT / "compactors" / "spark_rewrite" / "rewrite_loop.py"
    code = f"""
import datetime
import importlib.util
import sys
import types

python310_datetime = types.ModuleType("datetime")
python310_datetime.datetime = datetime.datetime
python310_datetime.timedelta = datetime.timedelta
python310_datetime.timezone = datetime.timezone
sys.modules["datetime"] = python310_datetime
spec = importlib.util.spec_from_file_location("rewrite_loop_python310", {str(path)!r})
module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = module
spec.loader.exec_module(module)
"""
    subprocess.run([sys.executable, "-c", code], check=True)


def test_the_call_names_the_table_and_omits_the_target() -> None:
    assert rewrite_loop.rewrite_call(_job()) == "CALL ice.system.rewrite_data_files(table => 'bench.events')"


def test_options_reach_the_procedure_in_a_stable_order() -> None:
    call = rewrite_loop.rewrite_call(_job({"min-input-files": "8", "max-file-group-size-bytes": "1"}))
    assert call == (
        "CALL ice.system.rewrite_data_files(table => 'bench.events', "
        "options => map('max-file-group-size-bytes', '1', 'min-input-files', '8'))"
    )


def test_the_document_is_read_back_whole(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _spec(tmp_path, {**SIZING, "rewrite_options": {"min-input-files": "8"}})
    site = _site()
    path = tmp_path / "rewrite.json"
    path.write_text(knobs.render_job(spec, _derived(spec, site, meta)))
    job = rewrite_loop.read_job(path)
    assert job.catalog == "ice" and job.interval_s == 600 and job.options == {"min-input-files": "8"}
    assert job.start_delay_s == 0


@pytest.mark.parametrize("delay", [-1, "600", True])
def test_a_start_delay_that_is_not_whole_seconds_is_refused(tmp_path: Path, delay: object) -> None:
    path = tmp_path / "rewrite.json"
    path.write_text(
        json.dumps({"catalog": "ice", "table": "t", "interval_s": 60, "start_delay_s": delay, "options": {}})
    )
    with pytest.raises(ValueError, match="start_delay_s"):
        rewrite_loop.read_job(path)


@pytest.mark.parametrize("interval", [0, -1, "600", True])
def test_a_cadence_that_would_run_flat_out_is_refused(tmp_path: Path, interval: object) -> None:
    path = tmp_path / "rewrite.json"
    path.write_text(json.dumps({"catalog": "ice", "table": "t", "interval_s": interval, "options": {}}))
    with pytest.raises(ValueError, match="interval_s"):
        rewrite_loop.read_job(path)


def test_a_document_missing_a_key_fails_rather_than_defaulting(tmp_path: Path) -> None:
    path = tmp_path / "rewrite.json"
    path.write_text(json.dumps({"catalog": "ice", "interval_s": 60, "options": {}}))
    with pytest.raises(KeyError):
        rewrite_loop.read_job(path)


def test_each_pass_refreshes_the_table_before_planning(
    capsys: pytest.CaptureFixture[str],
) -> None:
    calls: list[tuple[str, str]] = []

    def refresh_table(name: str) -> None:
        calls.append(("refresh", name))

    def execute(statement: str) -> object:
        calls.append(("execute", statement))
        return _result()

    rewrite_loop.run(
        _job(),
        execute,
        refresh_table=refresh_table,
        sleep=lambda _: None,
        now=lambda: 0.0,
        passes=2,
    )

    assert calls == [
        ("refresh", "ice.bench.events"),
        ("execute", "CALL ice.system.rewrite_data_files(table => 'bench.events')"),
        ("refresh", "ice.bench.events"),
        ("execute", "CALL ice.system.rewrite_data_files(table => 'bench.events')"),
    ]
    _events(capsys)


def test_without_a_delay_the_first_pass_never_asks_when_data_arrived(capsys: pytest.CaptureFixture[str]) -> None:
    statements: list[str] = []

    def execute(statement: str) -> object:
        statements.append(statement)
        return _result()

    rewrite_loop.run(_job(), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0, passes=1)
    assert [statement.split("(")[0] for statement in statements] == ["CALL ice.system.rewrite_data_files"]
    _events(capsys)


def test_a_delayed_start_waits_from_the_first_commit_that_adds_rows(capsys: pytest.CaptureFixture[str]) -> None:
    """The backlog starts with the first rows, not with the sidecar's own start."""
    first = datetime(2026, 10, 12, 18, 0, 0, tzinfo=UTC)
    answers: list[int | None] = [None, None, int(first.timestamp() * 1_000_000)]
    order: list[str] = []
    slept: list[float] = []

    def execute(statement: str) -> object:
        if statement.startswith("SELECT"):
            order.append("poll")
            return [_FirstDataRow(answers.pop(0))]
        order.append("pass")
        return _result()

    def sleep(seconds: float) -> None:
        order.append("sleep")
        slept.append(seconds)

    rewrite_loop.run(
        _job(start_delay_s=600),
        execute,
        refresh_table=lambda _: None,
        sleep=sleep,
        now=lambda: 0.0,
        wall_now=lambda: first + timedelta(seconds=30),
        passes=1,
    )
    assert order == ["poll", "sleep", "poll", "sleep", "poll", "sleep", "pass"]
    assert slept == [rewrite_loop.FIRST_DATA_POLL_S, rewrite_loop.FIRST_DATA_POLL_S, 570.0]
    events, _ = _events(capsys)
    assert events[0] == {
        "event": "spark_rewrite_delay",
        "first_data_at": "2026-10-12T18:00:00Z",
        "start_at": "2026-10-12T18:10:00Z",
        "start_delay_s": 600,
    }


def test_the_first_data_query_reads_only_commits_that_add_rows() -> None:
    query = rewrite_loop.first_data_query(_job())
    assert "FROM ice.bench.events.snapshots" in query and "summary['added-records']" in query
    assert "unix_micros(min(committed_at))" in query


def test_a_catalog_that_never_answers_the_delay_poll_stops_the_sidecar(capsys: pytest.CaptureFixture[str]) -> None:
    def execute(statement: str) -> object:
        raise RuntimeError("catalog unavailable")

    with pytest.raises(RuntimeError, match="catalog unavailable"):
        rewrite_loop.run(
            _job(start_delay_s=60), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0
        )
    _, errors = _events(capsys)
    assert [error["consecutive_failures"] for error in errors] == [1, 2, 3]


def test_the_cadence_is_timed_from_the_start_of_each_pass(capsys: pytest.CaptureFixture[str]) -> None:
    """A pass that takes 100s of a 600s interval waits 500s, not 600."""
    elapsed = [0.0, 100.0, 100.0, 250.0]
    slept: list[float] = []
    rewrite_loop.run(
        _job(),
        lambda statement: _result(),
        refresh_table=lambda _: None,
        sleep=slept.append,
        now=lambda: elapsed.pop(0),
        passes=2,
    )
    assert slept == [500.0]
    _events(capsys)


def test_a_pass_that_overruns_its_interval_starts_the_next_at_once(capsys: pytest.CaptureFixture[str]) -> None:
    times = iter([0.0, 900.0, 900.0, 1000.0])
    slept: list[float] = []
    rewrite_loop.run(
        _job(),
        lambda statement: _result(),
        refresh_table=lambda _: None,
        sleep=slept.append,
        now=lambda: next(times),
        passes=2,
    )
    assert slept == [0.0]
    _events(capsys)


def test_a_successful_pass_logs_wall_times_monotonic_duration_and_rewrite_counts(
    capsys: pytest.CaptureFixture[str],
) -> None:
    wall_times = iter(
        [
            datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC),
            datetime(2026, 9, 15, 12, 0, 7, tzinfo=UTC),
        ]
    )
    monotonic_times = iter([10.0, 16.25])

    rewrite_loop.run(
        _job(),
        lambda statement: _result(8, 2, 4096),
        refresh_table=lambda _: None,
        now=lambda: next(monotonic_times),
        wall_now=lambda: next(wall_times),
        passes=1,
    )

    stdout, stderr = _events(capsys)
    assert stderr == []
    assert stdout == [
        {
            "event": "spark_rewrite_start",
            "pass": 1,
            "started_at": "2026-09-15T12:00:00Z",
        },
        {
            "added_data_files_count": 2,
            "consecutive_failures": 0,
            "duration_s": 6.25,
            "ended_at": "2026-09-15T12:00:07Z",
            "error_chain": [],
            "event": "spark_rewrite_pass",
            "failure_type": None,
            "pass": 1,
            "rewritten_bytes_count": 4096,
            "rewritten_data_files_count": 8,
            "started_at": "2026-09-15T12:00:00Z",
            "status": "success",
        },
    ]


def test_a_zero_work_pass_is_logged_as_a_no_op(capsys: pytest.CaptureFixture[str]) -> None:
    rewrite_loop.run(
        _job(),
        lambda statement: _result(),
        refresh_table=lambda _: None,
        now=iter([20.0, 20.5]).__next__,
        wall_now=iter(
            [
                datetime(2026, 9, 15, 12, 0, 0, tzinfo=UTC),
                datetime(2026, 9, 15, 12, 0, 1, tzinfo=UTC),
            ]
        ).__next__,
        passes=1,
    )

    stdout, stderr = _events(capsys)
    assert stderr == []
    assert stdout[-1]["status"] == "no-op"
    assert stdout[-1]["rewritten_data_files_count"] == 0
    assert stdout[-1]["added_data_files_count"] == 0
    assert stdout[-1]["rewritten_bytes_count"] == 0


def test_losing_a_commit_race_logs_the_exception_chain_and_does_not_end_the_run(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """The ingest engine is appending throughout; the occasional conflict is expected."""
    attempts: list[str] = []

    def execute(statement: str) -> object:
        attempts.append(statement)
        if len(attempts) == 1:
            try:
                raise ValueError("stale metadata")
            except ValueError as cause:
                raise RuntimeError("cannot commit") from cause
        return _result(4, 1, 2048)

    assert (
        rewrite_loop.run(_job(), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0, passes=3)
        == 3
    )
    assert len(attempts) == 3
    stdout, stderr = _events(capsys)
    assert [event["status"] for event in stdout if event["event"] == "spark_rewrite_pass"] == [
        "success",
        "success",
    ]
    assert stderr == [
        {
            "added_data_files_count": None,
            "consecutive_failures": 1,
            "duration_s": 0.0,
            "ended_at": stderr[0]["ended_at"],
            "error_chain": [
                {"message": "cannot commit", "module": "builtins", "type": "RuntimeError"},
                {"message": "stale metadata", "module": "builtins", "type": "ValueError"},
            ],
            "event": "spark_rewrite_pass",
            "failure_type": "RuntimeError",
            "pass": 1,
            "rewritten_bytes_count": None,
            "rewritten_data_files_count": None,
            "started_at": stderr[0]["started_at"],
            "status": "failure",
        }
    ]
    assert stdout[-1]["consecutive_failures"] == 0


def test_a_compactor_that_fails_every_time_stops_rather_than_reading_as_slow(
    capsys: pytest.CaptureFixture[str],
) -> None:
    def execute(statement: str) -> None:
        raise RuntimeError("no such table")

    with pytest.raises(RuntimeError, match="no such table"):
        rewrite_loop.run(
            _job(), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0, passes=10
        )
    _, stderr = _events(capsys)
    assert [event["consecutive_failures"] for event in stderr] == [1, 2, 3]


def test_a_run_of_failures_is_forgiven_once_a_pass_lands(capsys: pytest.CaptureFixture[str]) -> None:
    outcomes = iter([False, False, True, False, False, True])
    attempts = 0

    def execute(statement: str) -> object:
        nonlocal attempts
        attempts += 1
        if not next(outcomes):
            raise RuntimeError("conflict")
        return _result()

    assert (
        rewrite_loop.run(_job(), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0, passes=6)
        == 6
    )
    assert attempts == 6
    stdout, stderr = _events(capsys)
    assert [event["consecutive_failures"] for event in stderr] == [1, 2, 1, 2]
    assert [event["consecutive_failures"] for event in stdout if event["event"] == "spark_rewrite_pass"] == [0, 0]


def test_diagnostics_cannot_mask_the_original_exception(capsys: pytest.CaptureFixture[str]) -> None:
    class UnprintableError(RuntimeError):
        def __str__(self) -> str:
            raise LookupError("message lookup failed")

    original = UnprintableError()

    def execute(statement: str) -> object:
        raise original

    with pytest.raises(UnprintableError) as raised:
        rewrite_loop.run(_job(), execute, refresh_table=lambda _: None, sleep=lambda _: None, now=lambda: 0.0, passes=3)
    assert raised.value is original
    _, stderr = _events(capsys)
    assert stderr[-1]["failure_type"] == "UnprintableError"
    assert stderr[-1]["error_chain"] == [
        {
            "message": "<unprintable UnprintableError>",
            "module": "tests.test_spark_rewrite",
            "type": "UnprintableError",
        }
    ]


# ---------------------------------------------------------------------------
# What staging does with it
# ---------------------------------------------------------------------------


class _Admin:
    """A broker that records the topic staging asked for."""

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool:
        return False

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int:
        return 1

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None:
        return None

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None:
        return None


def _staging_site(tmp_path: Path, corpus_root: str, cluster: dict[str, object] | None = None) -> Path:
    """A site whose catalog Spark can read, so the sidecar's settings render."""
    path = tmp_path / "site.yaml"
    path.write_text(
        yaml.safe_dump(
            {
                "corpus_root": corpus_root,
                "runs_root": f"file://{tmp_path}/published",
                "warehouse": f"file://{tmp_path}/wh",
                "kafka": {"bootstrap_servers": "localhost:9092", "security": {}},
                "catalog": {
                    "props": {"type": "rest", "uri": "http://iceberg-rest:8181", "warehouse": "s3://warehouse"}
                },
                "kubernetes": cluster or {},
                "pricing": {"vcpu_hour_usd": 0.0, "gib_hour_usd": 0.0},
            }
        )
    )
    return path


def _staging_spec(tmp_path: Path, meta: metadata.CorpusMetadata, driver: object) -> Path:
    """An external-engine run with a compaction layer, so the two stay independent."""
    raw = yaml.safe_load((ROOT / "specs/smoke/smoke-external.yaml").read_text())
    raw["corpus"] = meta.name
    # The engine owns the table so staging renders DDL instead of contacting a catalog.
    raw["table"] = {**raw["table"], "managed_by": "engine"}
    raw["compaction"] = driver
    path = tmp_path / "run.yaml"
    path.write_text(yaml.safe_dump(raw))
    return path


def _cluster_block() -> dict[str, object]:
    return {
        "context": "bench",
        "namespace": "ingest-bench",
        "harness_service_account": "ingest-bench-harness",
        "flink_service_account": "ingest-bench-flink",
        "registry": "registry.example/ingest-bench",
        "aws_region": "eu-west-1",
        "node_selector": {"bench-pool": "engine"},
    }


def test_staging_renders_the_sidecar_beside_an_engine_it_does_not_start(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _staging_spec(tmp_path, meta, {"driver": "spark-rewrite", "interval_s": 300, "spark-rewrite": dict(SIZING)})
    site = _staging_site(tmp_path, str(Path(meta.uri).parent), _cluster_block())
    staged = stage.stage(spec, site, tmp_path / "runs", _Admin(), stamp=STAMP, image_tag="abc1234")
    written = {path.name for path in staged.run_dir.iterdir()}
    assert {knobs.CONF_FILE, knobs.JOB_FILE, compactors.DOCUMENT_FILE, compactors.CONFIGMAP_FILE} <= written
    assert json.loads((staged.run_dir / knobs.JOB_FILE).read_text())["interval_s"] == 300


def test_staging_a_cluster_run_with_a_sidecar_needs_the_image_it_starts(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _staging_spec(tmp_path, meta, {"driver": "spark-rewrite", "interval_s": 300, "spark-rewrite": dict(SIZING)})
    site = _staging_site(tmp_path, str(Path(meta.uri).parent), _cluster_block())
    with pytest.raises(ValueError, match="--image-tag"):
        stage.stage(spec, site, tmp_path / "runs", _Admin(), stamp=STAMP)


def test_staging_refuses_a_sidecar_it_could_not_render_before_it_makes_a_topic(
    tmp_path: Path, meta: metadata.CorpusMetadata
) -> None:
    spec = _staging_spec(
        tmp_path, meta, {"driver": "spark-rewrite", "interval_s": 300, "spark-rewrite": {"executors": 2}}
    )
    site = _staging_site(tmp_path, str(Path(meta.uri).parent))
    with pytest.raises(ValueError, match="must set"):
        stage.stage(spec, site, tmp_path / "runs", _Admin(), stamp=STAMP)


def test_staging_a_run_that_compacts_inline_renders_no_sidecar(tmp_path: Path, meta: metadata.CorpusMetadata) -> None:
    spec = _staging_spec(tmp_path, meta, {"driver": "inline"})
    site = _staging_site(tmp_path, str(Path(meta.uri).parent), _cluster_block())
    staged = stage.stage(spec, site, tmp_path / "runs", _Admin(), stamp=STAMP, image_tag="abc1234")
    assert not {path.name for path in staged.run_dir.iterdir()} & {compactors.DOCUMENT_FILE, knobs.JOB_FILE}


# ---------------------------------------------------------------------------
# Flink ingest with a Spark rewrite sidecar
# ---------------------------------------------------------------------------


def test_flink_ingest_renders_a_separate_sidecar_on_the_same_table_and_catalog(
    meta: metadata.CorpusMetadata,
) -> None:
    """Two applications, one table: the sidecar's job names Flink's table through Flink's catalog."""
    from engines.flink import knobs as flink_knobs

    spec = model.load_run_spec(ROOT / "specs/smoke/smoke-flink-compact.yaml")
    site = replace(
        _site(_cluster()),
        schema_registry=model.SchemaRegistryConfig(url="http://registry:8081", basic_auth_user_info=None),
    )
    derived = _derived(spec, site, meta)

    job = json.loads(knobs.render_job(spec, derived))
    assert job["table"] == derived.table and job["catalog"] == flink_knobs.CATALOG_NAME
    assert job["interval_s"] == 15 and job["options"]["partial-progress.enabled"] == "false"

    flink_sql = flink_knobs.render_sql(spec, site, derived, meta)
    namespace, table = derived.table.split(".")
    assert f"INSERT INTO {flink_knobs.CATALOG_NAME}.`{namespace}`.`{table}`" in flink_sql
    conf = knobs.render_conf(spec, site, derived)
    assert conf[f"spark.sql.catalog.{flink_knobs.CATALOG_NAME}.uri"] == site.catalog_props["uri"]
    assert site.catalog_props["uri"] in flink_sql

    sidecar = cast(dict[str, object], yaml.safe_load(knobs.render_sparkapplication(spec, site, derived, "t")))
    deployment = cast(
        dict[str, object], yaml.safe_load(flink_knobs.render_flinkdeployment(spec, site, derived, meta, "t"))
    )
    assert sidecar["kind"] == "SparkApplication" and deployment["kind"] == "FlinkDeployment"
    assert cast(dict[str, str], sidecar["metadata"])["name"] != cast(dict[str, str], deployment["metadata"])["name"]
    executor = cast(dict[str, object], cast(dict[str, object], sidecar["spec"])["executor"])
    assert executor["instances"] == 1 and executor["cores"] == 2 and executor["memory"] == "2048m"
    assert conf["spark.sql.shuffle.partitions"] == "2"


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
        block["driver_machine_type"] = coordinator_type
    pinned = _spec(tmp_path, block)
    roles = {role.role: role for role in fleet.fleet(pinned)}
    assert roles["compactor-driver"].machine_type == (
        coordinator_type or machine_type or model.MACHINE_TYPE_UNSPECIFIED
    )
    assert roles["compactor-executor"].machine_type == (machine_type or model.MACHINE_TYPE_UNSPECIFIED)
    for site in (_site(), _site(_cluster())):
        d = _derived(spec, site, meta)
        assert knobs.render(pinned, site, d, meta, image_tag="t") == knobs.render(spec, site, d, meta, image_tag="t")


def test_coordinator_machine_type_requires_a_string() -> None:
    with pytest.raises(ValueError, match="driver_machine_type must be a string"):
        knobs.read({**SIZING, "driver_machine_type": 8})
