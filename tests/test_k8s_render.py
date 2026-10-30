# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate manifest substitution and rendered Job structure without a cluster.
Enumerate expected markers so new template inputs require an explicit test update.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import cast

import pytest
import yaml

from ingest_bench.k8s.render import MARKER_RE, main, render_template

TEMPLATES = Path(__file__).resolve().parents[1] / "deploy" / "k8s"

# Use realistic command and JSON placement values for every marker.
SAMPLE = {
    "NAME": "a-job",
    "NAMESPACE": "a-namespace",
    "SERVICE_ACCOUNT": "an-account",
    "IMAGE": "123456789012.dkr.ecr.eu-west-1.amazonaws.com/lakehouse-ingest-bench/harness:abc1234",
    "COMMAND": json.dumps(["gen-corpus", "--preset", "smoke", "--out", "s3://a-bucket/a path"]),
    "ENV": '[{"name": "AWS_REGION", "value": "eu-west-1"}, {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"}]',
    "ENV_FROM": '[{"secretRef": {"name": "bench-env"}}]',
    "NODE_SELECTOR": '{"kubernetes.io/arch": "amd64"}',
    "TOLERATIONS": '[{"key": "a-taint", "operator": "Exists", "effect": "NoSchedule"}]',
    "COUNT": "4",
    "MEMORY": "6Gi",
    "CPU": "2",
    "DEADLINE_S": "7200",
    "SPEC_CONFIGMAP": "stage-a-run-spec",
    "SITE_CONFIGMAP": "stage-a-run-site",
}


@dataclass(frozen=True)
class Expectation:
    """What one shipped template takes, and what it has to render into."""

    markers: frozenset[str]
    cpu: str
    memory: str
    indexed: bool
    work_volume: bool
    container: str = "harness"
    # The Spark scan keeps its catalog file and local dirs in /tmp.
    work_mount: str = "/work"


_ONE_OFF = frozenset(
    {
        "NAME",
        "NAMESPACE",
        "SERVICE_ACCOUNT",
        "IMAGE",
        "COMMAND",
        "ENV",
        "ENV_FROM",
        "NODE_SELECTOR",
        "TOLERATIONS",
    }
)
_INDEXED = _ONE_OFF | {"COUNT"}
# Generator and producer memory scale with batch size, not shard count.
_BATCH_SIZED = _INDEXED | {"MEMORY"}
_MOUNTED = _ONE_OFF | {"SPEC_CONFIGMAP", "SITE_CONFIGMAP"}
# The validator and the scorer are sized per run: a campaign table's ID set is measured in
# GiB, and the scorer's poll width follows the commit sizes it has to keep up with.
_SIZED = _ONE_OFF | {"CPU", "MEMORY"}

# The registry is a Deployment and Service; test it with setup.sh in test_scripts.py.
NOT_A_JOB = frozenset({"schema-registry.yaml.tmpl"})

# Generation and merge Jobs need TTLs because their driver does not delete them on exit.
EXPIRING = frozenset(
    {"corpus-gen-job.yaml.tmpl", "harness-job.yaml.tmpl", "verify-job.yaml.tmpl", "spark-scan-job.yaml.tmpl"}
)
EXPIRY_S = 3600

EXPECTATIONS = {
    "harness-job.yaml.tmpl": Expectation(_ONE_OFF, "500m", "1Gi", indexed=False, work_volume=False),
    "stage-job.yaml.tmpl": Expectation(_MOUNTED, "500m", "1Gi", indexed=False, work_volume=True),
    "corpus-gen-job.yaml.tmpl": Expectation(_BATCH_SIZED, "1", "6Gi", indexed=True, work_volume=False),
    "producer-job.yaml.tmpl": Expectation(_BATCH_SIZED | {"CPU"}, "2", "6Gi", indexed=True, work_volume=True),
    "scorer-job.yaml.tmpl": Expectation(_SIZED, "2", "6Gi", indexed=False, work_volume=True),
    "verify-job.yaml.tmpl": Expectation(_SIZED, "2", "6Gi", indexed=False, work_volume=True),
    "spark-scan-job.yaml.tmpl": Expectation(
        _SIZED | {"DEADLINE_S"}, "2", "6Gi", indexed=False, work_volume=True, container="spark", work_mount="/tmp"
    ),
}


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, dict), f"expected a mapping, got {type(value).__name__}"
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _sequence(value: object) -> list[object]:
    assert isinstance(value, list), f"expected a list, got {type(value).__name__}"
    return cast(list[object], value)


def _pod_spec(document: dict[str, object]) -> dict[str, object]:
    return _mapping(_mapping(_mapping(document["spec"])["template"])["spec"])


def _container(document: dict[str, object]) -> dict[str, object]:
    containers = _sequence(_pod_spec(document)["containers"])
    assert len(containers) == 1, "a harness Job runs one container"
    return _mapping(containers[0])


# ---------------------------------------------------------------------------
# The renderer
# ---------------------------------------------------------------------------


def test_every_marker_is_substituted(tmp_path: Path) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\nalso: __NAME__\nimage: __IMAGE__\n")
    rendered = render_template(template, {"NAME": "a-job", "IMAGE": "a-registry/an-image:a-tag"})
    assert rendered == "name: a-job\nalso: a-job\nimage: a-registry/an-image:a-tag\n"


def test_a_marker_with_no_value_is_refused(tmp_path: Path) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\nimage: __IMAGE__\n")
    with pytest.raises(ValueError, match="IMAGE"):
        render_template(template, {"NAME": "a-job"})


def test_a_variable_that_matched_nothing_is_refused(tmp_path: Path) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\n")
    with pytest.raises(ValueError, match="NAMESPACE"):
        render_template(template, {"NAME": "a-job", "NAMESPACE": "a-namespace"})


def test_a_value_holding_a_marker_is_not_rendered_again(tmp_path: Path) -> None:
    """Substitute once so marker-like text in a value remains literal data."""
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("args: __COMMAND__\n")
    assert render_template(template, {"COMMAND": "echo __NAME__"}) == "args: echo __NAME__\n"


def test_the_console_script_prints_the_rendered_document(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    template = tmp_path / "t.yaml.tmpl"
    template.write_text("name: __NAME__\n")
    assert main([str(template), "--set", "NAME=a-job"]) == 0
    assert capsys.readouterr().out == "name: a-job\n"


# ---------------------------------------------------------------------------
# The shipped templates
# ---------------------------------------------------------------------------


def test_every_shipped_template_is_expected_here() -> None:
    shipped = {path.name for path in TEMPLATES.glob("*.yaml.tmpl")} - NOT_A_JOB
    assert shipped == set(EXPECTATIONS), "a template was added or renamed and this file does not know about it"


@pytest.mark.parametrize("filename", sorted(EXPECTATIONS))
def test_a_shipped_template_renders_to_the_job_the_driver_meant(filename: str) -> None:
    expectation = EXPECTATIONS[filename]
    template = TEMPLATES / filename
    assert {match[2:-2] for match in MARKER_RE.findall(template.read_text())} == set(expectation.markers), (
        f"{filename}'s markers and the ones this test fills have drifted"
    )

    document = _mapping(yaml.safe_load(render_template(template, {k: SAMPLE[k] for k in expectation.markers})))
    assert document["kind"] == "Job" and document["apiVersion"] == "batch/v1"
    metadata = _mapping(document["metadata"])
    assert metadata["name"] == "a-job" and metadata["namespace"] == "a-namespace"

    spec = _mapping(document["spec"])
    # Retrying a producer Job would replay batches and appear as engine duplication.
    assert spec["backoffLimit"] == 0
    if filename in EXPIRING:
        assert spec["ttlSecondsAfterFinished"] == EXPIRY_S
    else:
        assert "ttlSecondsAfterFinished" not in spec, "a Job its driver deletes must not also expire under it"
    if expectation.indexed:
        assert spec["completionMode"] == "Indexed"
        assert spec["completions"] == 4 and spec["parallelism"] == 4
    else:
        assert spec["completions"] == 1
        assert "completionMode" not in spec

    pod = _pod_spec(document)
    assert pod["restartPolicy"] == "Never"
    assert pod["serviceAccountName"] == "an-account"
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64"}
    assert pod["tolerations"] == [{"key": "a-taint", "operator": "Exists", "effect": "NoSchedule"}]

    container = _container(document)
    assert container["name"] == expectation.container
    assert container["image"] == SAMPLE["IMAGE"]
    assert container["command"] == json.loads(SAMPLE["COMMAND"])
    assert "args" not in container
    assert container["env"] == [
        {"name": "AWS_REGION", "value": "eu-west-1"},
        {"name": "AWS_DEFAULT_REGION", "value": "eu-west-1"},
    ]
    # Every key of the site's Secret, which is what a `${env:NAME}` in a
    # property resolves against inside this pod.
    assert container["envFrom"] == [{"secretRef": {"name": "bench-env"}}]
    requests = _mapping(_mapping(container["resources"])["requests"])
    assert requests["cpu"] == expectation.cpu and requests["memory"] == expectation.memory

    if expectation.work_volume:
        mount = _mapping(_sequence(container["volumeMounts"])[0])
        assert mount["mountPath"] == expectation.work_mount
        volume = _mapping(_sequence(pod["volumes"])[0])
        assert volume["name"] == mount["name"] and "emptyDir" in volume
    else:
        assert "volumeMounts" not in container and "volumes" not in pod


def test_the_stage_job_mounts_the_spec_and_the_site_read_only() -> None:
    template = TEMPLATES / "stage-job.yaml.tmpl"
    values = {key: SAMPLE[key] for key in EXPECTATIONS["stage-job.yaml.tmpl"].markers}
    document = _mapping(yaml.safe_load(render_template(template, values)))

    mounts = {
        str(_mapping(entry)["name"]): _mapping(entry) for entry in _sequence(_container(document)["volumeMounts"])
    }
    assert mounts["spec"]["mountPath"] == "/runs" and mounts["spec"]["readOnly"] is True
    assert mounts["site"]["mountPath"] == "/site" and mounts["site"]["readOnly"] is True

    volumes = {str(_mapping(entry)["name"]): _mapping(entry) for entry in _sequence(_pod_spec(document)["volumes"])}
    assert set(volumes) == set(mounts)
    assert _mapping(volumes["spec"]["configMap"])["name"] == SAMPLE["SPEC_CONFIGMAP"]
    assert _mapping(volumes["site"]["configMap"])["name"] == SAMPLE["SITE_CONFIGMAP"]


@pytest.mark.parametrize("filename", sorted(EXPECTATIONS))
def test_a_site_naming_no_placement_renders_a_job_the_scheduler_still_takes(filename: str) -> None:
    expectation = EXPECTATIONS[filename]
    values = {key: SAMPLE[key] for key in expectation.markers} | {"NODE_SELECTOR": "{}", "TOLERATIONS": "[]"}
    pod = _pod_spec(_mapping(yaml.safe_load(render_template(TEMPLATES / filename, values))))
    assert pod["nodeSelector"] == {} and pod["tolerations"] == []


def test_a_site_naming_no_region_renders_an_empty_env() -> None:
    markers = EXPECTATIONS["harness-job.yaml.tmpl"].markers
    values = {key: SAMPLE[key] for key in markers} | {"ENV": "[]", "ENV_FROM": "[]"}
    document = _mapping(yaml.safe_load(render_template(TEMPLATES / "harness-job.yaml.tmpl", values)))
    assert _container(document)["env"] == [] and _container(document)["envFrom"] == []


def test_the_spark_scan_job_limits_cpu_to_its_request() -> None:
    """local[N] must get exactly N cores on any node, so every engine's scan time compares."""
    markers = EXPECTATIONS["spark-scan-job.yaml.tmpl"].markers
    document = _mapping(
        yaml.safe_load(render_template(TEMPLATES / "spark-scan-job.yaml.tmpl", {k: SAMPLE[k] for k in markers}))
    )
    resources = _mapping(_container(document)["resources"])
    assert _mapping(resources["limits"]) == _mapping(resources["requests"])
