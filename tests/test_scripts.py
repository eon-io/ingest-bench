# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Check shell syntax, rendered manifests and driver control flow without a cluster.
Live AWS and Kubernetes calls use stubs; generated documents are parsed.
"""

from __future__ import annotations

import ast
import gzip
import importlib
import inspect
import json
import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from string import Template
from typing import cast

import pytest
import yaml

from ingest_bench.k8s.render import MARKER_RE, render_template
from ingest_bench.specs import compactors, engines
from ingest_bench.specs.derive import TABLE_NAMESPACE
from ingest_bench.specs.kubernetes import FIELDS, NAME, for_name
from ingest_bench.specs.model import MACHINE_TYPE_UNSPECIFIED, PLACEHOLDER, KubernetesConfig, load_site
from tests.collect_fixtures import EXTERNAL_SPEC, FLINK_SPEC, build_run_dir, fixture_workloads, write_site

REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = REPO_ROOT / "scripts"
AWS_DEPLOY = REPO_ROOT / "deploy" / "aws"
K8S_LIB = SCRIPTS / "_k8s.sh"
SMOKE = SCRIPTS / "smoke.sh"
GEN_CORPUS = SCRIPTS / "gen-corpus.sh"
PUSH_IMAGES = SCRIPTS / "push-images.sh"
STAGE = SCRIPTS / "stage.sh"
LAUNCH = SCRIPTS / "launch.sh"
GATE = SCRIPTS / "gate.sh"
TEARDOWN = SCRIPTS / "teardown.sh"
FINISH = SCRIPTS / "finish.sh"
PURGE = SCRIPTS / "purge.sh"
PREFLIGHT_KAFKA = SCRIPTS / "preflight-kafka.sh"
RUN = SCRIPTS / "run.sh"
MEASURE_PRODUCER = SCRIPTS / "measure-producer.sh"
AWS_SETUP = AWS_DEPLOY / "setup.sh"
AWS_TEARDOWN = AWS_DEPLOY / "teardown.sh"
AWS_RESOURCES = AWS_DEPLOY / "_resources.sh"
SITE_AWS_EXAMPLE = REPO_ROOT / "site.aws.example.yaml"
SITE_K8S_EXAMPLE = REPO_ROOT / "site.k8s.example.yaml"

SITE_K8S_FILLINGS = {
    "YOUR_BUCKET": "a-bucket",
    "YOUR_REGION": "eu-west-1",
    "YOUR_KUBE_CONTEXT": "a-cluster",
    "YOUR_REGISTRY": "123456789012.dkr.ecr.eu-west-1.amazonaws.com",
}

# Java reads AWS_REGION; botocore reads AWS_DEFAULT_REGION.
REGION_ENV_NAMES = ("AWS_REGION", "AWS_DEFAULT_REGION")

needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="bash not installed")

# Skip tool-dependent cases when jq or yq is unavailable.
needs_shell_tools = pytest.mark.skipif(
    any(shutil.which(tool) is None for tool in ("bash", "jq", "yq", "git")),
    reason="the cluster drivers read the site and a run's facts with jq, yq and git",
)

STACK_DEPLOY = REPO_ROOT / "deploy" / "k8s" / "stack"
KAFKA_CHART = STACK_DEPLOY / "charts" / "kafka"

# Skip chart-rendering tests when Helm is unavailable.
needs_helm = pytest.mark.skipif(shutil.which("helm") is None, reason="helm not installed")

# Use realistic values for every variable exported to IAM templates.
IAM_VALUES = {
    "ACCOUNT": "123456789012",
    "REGION": "eu-west-1",
    "BUCKET": "a-bucket",
    "MSK_ARN": "arn:aws:kafka:eu-west-1:123456789012:cluster/a-cluster/aaaa-bbbb-1",
    "MSK_TOPIC_ARN": "arn:aws:kafka:eu-west-1:123456789012:topic/a-cluster/aaaa-bbbb-1/*",
    "MSK_GROUP_ARN": "arn:aws:kafka:eu-west-1:123456789012:group/a-cluster/aaaa-bbbb-1/*",
}

# Enumerate placeholders so adding one requires a fixture update.
SITE_AWS_FILLINGS = {
    "YOUR_BUCKET": "a-bucket",
    "YOUR_MSK_IAM_BOOTSTRAP": "b-1.a-cluster.abc123.c2.kafka.eu-west-1.amazonaws.com",
    "YOUR_REGION": "eu-west-1",
    "YOUR_ACCOUNT_ID": "123456789012",
    "YOUR_KUBE_CONTEXT": "a-cluster",
}


@needs_shell_tools
@pytest.mark.parametrize("mode", ["sustainable", "saturation"])
def test_printed_verdict_explains_permitted_duplicates(tmp_path: Path, mode: str) -> None:
    exactness = {
        "mode": "AT_LEAST_ONCE",
        "verdict": True,
        "exact": False,
        "loss_rows": 0,
        "duplicate_rows": 2,
        "corrupt_batches": 0,
        "invalid_rows": 0,
        "unoffered_rows": 0,
    }
    summary = tmp_path / "summary.json"
    metrics = {"rows_per_s": 100, "append_commits": 3}
    summary.write_text(
        json.dumps({"run_valid": True, "exactness": exactness, "scoring_mode": mode, "saturation": metrics})
    )
    result = subprocess.run(
        ["bash", "-c", 'source "$1"; print_verdict "$2"', "bash", str(SCRIPTS / "_lib.sh"), str(summary)],
        capture_output=True,
        text=True,
        check=True,
    )
    printed = json.loads(result.stdout)
    assert printed["run_valid"] is True
    assert printed["exactness"] == exactness
    if mode == "saturation":
        assert printed["saturation"] == metrics
    else:
        assert "saturation" not in printed


def _engine_compose_files() -> list[Path]:
    """Each engine's Compose shape: how a run of it is started on one machine."""
    return sorted((REPO_ROOT / "engines").glob("*/compose.sh"))


def _shell_files() -> list[Path]:
    return (
        sorted(SCRIPTS.glob("*.sh"))
        + sorted(AWS_DEPLOY.glob("*.sh"))
        + sorted(STACK_DEPLOY.glob("*.sh"))
        + _engine_compose_files()
    )


def _sourced_shell_files() -> list[Path]:
    """The files that are sourced rather than run: the two libraries and the engines'."""
    return sorted([path for path in _shell_files() if path.name.startswith("_")] + _engine_compose_files())


def _shell_entrypoints() -> list[Path]:
    """The scripts meant to be run, which is everything that is not sourced."""
    sourced = set(_sourced_shell_files())
    return [path for path in _shell_files() if path not in sourced]


def _iam_documents() -> list[Path]:
    return sorted((AWS_DEPLOY / "iam").glob("*.json"))


def _mapping(value: object) -> dict[str, object]:
    """``value`` as a mapping, for reading parsed YAML and JSON under strict typing."""
    assert isinstance(value, dict), f"expected a mapping, got {type(value).__name__}"
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def _sequence(value: object) -> list[object]:
    assert isinstance(value, list), f"expected a list, got {type(value).__name__}"
    return cast(list[object], value)


def _rendered_policy(path: Path) -> dict[str, object]:
    """Render a policy using envsubst-compatible placeholders and parse the result.
    string.Template rejects placeholders absent from the exported mapping.
    """
    rendered = Template(path.read_text()).substitute(IAM_VALUES)
    assert "${" not in rendered, f"{path.name} still holds an unrendered placeholder"
    return _mapping(json.loads(rendered))


def _statements(document: dict[str, object]) -> list[dict[str, object]]:
    return [_mapping(statement) for statement in _sequence(document["Statement"])]


def _one(statements: list[dict[str, object]], resources: set[str], what: str) -> dict[str, object]:
    """The single statement whose resources are exactly ``resources``."""
    matching = [statement for statement in statements if _resources(statement) == resources]
    assert len(matching) == 1, f"expected one statement for {what}, found {len(matching)}"
    return matching[0]


def _one_or_many(value: object) -> set[str]:
    """An IAM ``Action`` or ``Resource``, which is either one string or a list of them."""
    if isinstance(value, str):
        return {value}
    return {str(entry) for entry in _sequence(value)}


def _actions(statement: dict[str, object]) -> set[str]:
    return _one_or_many(statement["Action"])


def _resources(statement: dict[str, object]) -> set[str]:
    return _one_or_many(statement["Resource"])


# ---------------------------------------------------------------------------
# The shell itself
# ---------------------------------------------------------------------------


@needs_bash
def test_every_script_parses() -> None:
    scripts = _shell_files()
    assert scripts, f"no shell scripts under {SCRIPTS} or {AWS_DEPLOY}"
    for script in scripts:
        check = subprocess.run(["bash", "-n", str(script)], capture_output=True, text=True)
        assert check.returncode == 0, f"{script.name}: {check.stderr}"


def test_every_script_is_executable() -> None:
    entrypoints = _shell_entrypoints()
    sourced = _sourced_shell_files()
    assert [path.relative_to(REPO_ROOT).as_posix() for path in sourced] == [
        "deploy/aws/_resources.sh",
        "deploy/aws/_stack_hooks.sh",
        "engines/flink/compose.sh",
        "engines/spark/compose.sh",
        "scripts/_k8s.sh",
        "scripts/_lib.sh",
    ], "the shared libraries, the AWS hook for the in-cluster stack, and one Compose shape per engine"
    for script in entrypoints:
        assert os.access(script, os.X_OK), f"{script} is not executable"
    for script in sourced:
        # Sourced libraries should not be executable entrypoints.
        assert not os.access(script, os.X_OK), f"{script} is sourced, so it should not be executable"


@dataclass(frozen=True)
class WaitedJob:
    """One run of `k8s_wait_job`, and every `kubectl` call it made."""

    result: subprocess.CompletedProcess[str]
    calls: str


# Unschedulable pods have no logs; scheduler events explain the timeout.
POD = "a-job-2xk4t"
SCHEDULER_REFUSAL = "Warning FailedScheduling 0/3 nodes are available: Insufficient cpu"


def _waited_job(answers: list[str], timeout_s: int = 1) -> WaitedJob:
    """`_k8s.sh`'s own wait, against a `kubectl` answering one reading at a time."""
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "kubectl-calls.log"
        calls.touch()
        conditions = Path(directory) / "conditions"
        conditions.write_text("".join(f"{answer}\n" for answer in answers))
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\\n' "$*"; }}
            die() {{ printf 'die %s\\n' "$*"; exit 3; }}
            KUBE_CONTEXT=a-cluster
            SITE_NAMESPACE=ingest-bench
            K8S_JOB_POLL_S=1
            kubectl() {{
                printf 'kubectl %s\\n' "$*" >>'{calls}'
                case "$*" in
                # Before the conditions arm below, which its own read would
                # otherwise match: a pod name is read with a jsonpath too.
                *"get pods"*) printf '{POD}\\n' ;;
                *"get events"*) printf '{SCHEDULER_REFUSAL}\\n' ;;
                *jsonpath*)
                    head -n 1 '{conditions}'
                    tail -n +2 '{conditions}' >'{conditions}.rest'
                    mv '{conditions}.rest' '{conditions}'
                    ;;
                *logs*) printf 'the job said this\\n' ;;
                esac
            }}
source "{K8S_LIB}"
            k8s_wait_job a-job {timeout_s}
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return WaitedJob(result=result, calls=calls.read_text())


@needs_bash
@pytest.mark.parametrize(
    ("answers", "status", "said", "tailed"),
    [
        # An active Job has no terminal condition.
        (["", "Complete"], 0, "log job/a-job completed", False),
        # A failed Job never becomes Complete; check both terminal conditions.
        (["Failed"], 3, "die job/a-job failed", True),
        (["", ""], 3, "die job/a-job did not complete within 1s", True),
    ],
)
def test_a_job_is_waited_for_until_it_reaches_one_of_its_two_ends(
    answers: list[str], status: int, said: str, tailed: bool
) -> None:
    waited = _waited_job(answers)
    assert waited.result.returncode == status, waited.result.stdout + waited.result.stderr
    assert said in waited.result.stdout, waited.result.stdout
    assert ("logs job/a-job --tail=40" in waited.calls) is tailed, waited.calls
    assert ("the job said this" in waited.result.stderr) is tailed, waited.result.stderr
    # Include scheduler events even when logs are empty.
    assert (f"get events --field-selector involvedObject.name={POD}" in waited.calls) is tailed, waited.calls
    assert (SCHEDULER_REFUSAL in waited.result.stderr) is tailed, waited.result.stderr
    # Ignore conditions whose status is False.
    assert '{range .status.conditions[?(@.status=="True")]}' in waited.calls, waited.calls


# Allocatable CPU on the example m6i.xlarge, after kubelet reservations.
NODE_ALLOCATABLE = "3920m"


def _node(name: str, taint: str | None = None) -> dict[str, object]:
    spec: dict[str, object] = {} if taint is None else {"taints": [{"key": "a-pool", "effect": taint}]}
    return {"metadata": {"name": name}, "spec": spec, "status": {"allocatable": {"cpu": NODE_ALLOCATABLE}}}


def _pod(node: str | None, *requests: str | None, phase: str = "Running") -> dict[str, object]:
    """One pod of ``requests``, a container each, with None for a container that asks for nothing."""
    containers = [{} if cpu is None else {"resources": {"requests": {"cpu": cpu}}} for cpu in requests]
    spec: dict[str, object] = {"containers": containers}
    if node is not None:
        spec["nodeName"] = node
    return {"spec": spec, "status": {"phase": phase}}


def _nodes_with_free_cpu(
    tmp_path: Path,
    nodes: list[dict[str, object]],
    pods: list[dict[str, object]],
    millicores: int,
    tolerations: str,
) -> subprocess.CompletedProcess[str]:
    """Run the shell CPU-count function against fixture files. Check integer, decimal
    and millicore quantities through its actual jq arithmetic.
    """
    (tmp_path / "nodes.json").write_text(json.dumps({"items": nodes}))
    (tmp_path / "pods.json").write_text(json.dumps({"items": pods}))
    harness = f"""
        set -euo pipefail
        log() {{ printf 'log %s\\n' "$*"; }}
        die() {{ printf 'die %s\\n' "$*"; exit 3; }}
        TOLERATIONS='{tolerations}'
source "{K8S_LIB}"
        k8s_nodes_with_free_cpu {millicores} '{tmp_path / "nodes.json"}' '{tmp_path / "pods.json"}'
    """
    return subprocess.run(["bash", "-c", harness], capture_output=True, text=True)


@needs_shell_tools
@pytest.mark.parametrize(
    ("nodes", "pods", "millicores", "tolerations", "counted"),
    [
        # Scheduling uses requests, not current utilization; one 2-CPU pod leaves no room for another.
        (
            [_node("node-a"), _node("node-b")],
            [_pod("node-a", "2"), _pod("node-a", "250m", "100m"), _pod("node-b", "350m")],
            2000,
            "[]",
            1,
        ),
        # Finished and unassigned pods reserve no CPU on these nodes.
        (
            [_node("node-a"), _node("node-b")],
            [_pod("node-a", "2", phase="Succeeded"), _pod("node-b", "2", phase="Failed"), _pod(None, "2")],
            2000,
            "[]",
            2,
        ),
        # 1.5 CPU plus 500m equals 2000m; parsing decimal CPU as millicores would overstate capacity.
        ([_node("node-a"), _node("node-b")], [_pod("node-a", "1.5", "500m"), _pod("node-b", "1")], 2000, "[]", 1),
        ([_node("node-a")], [_pod("node-a", None)], 2000, "[]", 1),
        # A NoSchedule taint keeps a pod off the node unless the site declares
        # a toleration; PreferNoSchedule keeps it off nothing.
        ([_node("node-a", "NoSchedule"), _node("node-b", "PreferNoSchedule")], [], 2000, "[]", 1),
        ([_node("node-a", "NoSchedule"), _node("node-b", "PreferNoSchedule")], [], 2000, '[{"operator":"Exists"}]', 2),
    ],
    ids=["requests-not-usage", "ended-and-unbound-pods", "quantity-spellings", "no-request", "taints", "tolerated"],
)
def test_a_nodes_free_cpu_is_its_allocatable_less_what_its_pods_request(
    tmp_path: Path,
    nodes: list[dict[str, object]],
    pods: list[dict[str, object]],
    millicores: int,
    tolerations: str,
    counted: int,
) -> None:
    out = _nodes_with_free_cpu(tmp_path, nodes, pods, millicores, tolerations)
    assert out.returncode == 0, out.stdout + out.stderr
    assert out.stdout.strip() == str(counted), out.stdout + out.stderr


@needs_shell_tools
def test_a_cluster_that_could_not_be_read_is_not_a_cluster_with_no_room(tmp_path: Path) -> None:
    (tmp_path / "nodes.json").write_text("")
    (tmp_path / "pods.json").write_text("")
    harness = f"""
        set -euo pipefail
        TOLERATIONS='[]'
source "{K8S_LIB}"
        k8s_nodes_with_free_cpu 2000 '{tmp_path / "nodes.json"}' '{tmp_path / "pods.json"}'
    """
    out = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    assert out.stdout.strip() == "", out.stdout


def test_both_workflows_install_the_same_checked_yq() -> None:
    installs = {
        path.name: (
            re.findall(r"yq/releases/download/(v[\d.]+)/yq_linux_amd64", path.read_text()),
            re.findall(r"^\s*([0-9a-f]{64}) \| sha256sum", path.read_text(), re.MULTILINE),
        )
        for path in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml"))
    }
    installing = {name: pins for name, pins in installs.items() if pins[0]}
    assert set(installing) == {"ci.yml", "smoke.yml"}, installing
    for name, (versions, digests) in installing.items():
        assert len(versions) == 1 and len(digests) == 1, f"{name} pins {versions} and checks {digests}"
    assert len({pins for pins in map(str, installing.values())}) == 1, installing


@needs_bash
def test_help_needs_no_stack() -> None:
    out = subprocess.run([str(SMOKE), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "--external-ready-file" in out.stdout


@needs_bash
def test_an_unknown_argument_is_refused() -> None:
    out = subprocess.run([str(SMOKE), "--warmup"], capture_output=True, text=True)
    assert out.returncode == 2, out.stdout
    assert "unknown argument --warmup" in out.stderr


@needs_bash
def test_measure_producer_answers_before_it_starts_a_stack() -> None:
    out = subprocess.run([str(MEASURE_PRODUCER), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "Takes no arguments" in out.stdout

    refused = subprocess.run([str(MEASURE_PRODUCER), "--warmup"], capture_output=True, text=True)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --warmup" in refused.stderr


@needs_bash
@pytest.mark.parametrize(
    "script", [GEN_CORPUS, PUSH_IMAGES, STAGE, LAUNCH, GATE, TEARDOWN, FINISH, PURGE, RUN, PREFLIGHT_KAFKA]
)
def test_a_cluster_driver_answers_before_it_reads_a_site(script: Path) -> None:
    out = subprocess.run([str(script), "--help"], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    assert "--site PATH" in out.stdout

    refused = subprocess.run([str(script), "--warmup"], capture_output=True, text=True)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --warmup" in refused.stderr


@needs_bash
def test_gen_corpus_refuses_bad_arguments_before_it_needs_a_cluster() -> None:
    out = subprocess.run([str(GEN_CORPUS), "smoke", "--shards", "two"], capture_output=True, text=True)
    assert out.returncode == 1, out.stdout
    assert "--shards must be a positive integer" in out.stderr

    both = subprocess.run([str(GEN_CORPUS), "smoke", "events-1mrps-skew"], capture_output=True, text=True)
    assert both.returncode == 1, both.stdout
    assert "expected one preset" in both.stderr


@needs_bash
def test_teardown_takes_its_argument_before_it_needs_an_account() -> None:
    environment = {key: value for key, value in os.environ.items() if key not in ("AWS_REGION", "CLUSTER_NAME")}
    out = subprocess.run([str(AWS_TEARDOWN), "--help"], capture_output=True, text=True, env=environment)
    assert out.returncode == 0, out.stderr
    assert "--all" in out.stdout

    refused = subprocess.run([str(AWS_TEARDOWN), "--everything"], capture_output=True, text=True, env=environment)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --everything" in refused.stderr


@needs_bash
def test_setup_takes_its_one_argument_before_it_needs_an_account() -> None:
    environment = {key: value for key, value in os.environ.items() if key not in ("AWS_REGION", "CLUSTER_NAME")}
    out = subprocess.run([str(AWS_SETUP), "--help"], capture_output=True, text=True, env=environment)
    assert out.returncode == 0, out.stderr
    assert "--write-site" in out.stdout

    refused = subprocess.run([str(AWS_SETUP), "--everything"], capture_output=True, text=True, env=environment)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --everything" in refused.stderr


@needs_bash
@pytest.mark.parametrize(
    "offered, chosen",
    [
        # Sort versions numerically and exclude tiered-storage variants.
        ("3.6.0\t3.10.0\t3.6.0.tiered\t2.8.1", "3.10.0"),
        ("3.6.0", "3.6.0"),
        # MSK also publishes a minor line's latest patch as a trailing `x`;
        # it must win over a numeric minor that is actually older.
        ("3.6.0\t3.7.x", "3.7.x"),
        # An `x` patch sorts after every numeric patch of the same minor.
        ("3.7.x\t3.7.5", "3.7.x"),
        # A numeric minor still beats an `x` patch of an older minor.
        ("3.9.x\t3.10.0", "3.10.0"),
        # No plain 3.x at all: the script must reach its own refusal.
        ("2.8.1\t4.0.0", None),
        ("", None),
    ],
)
def test_the_kafka_version_choice_reaches_its_refusal(offered: str, chosen: str | None) -> None:
    """An unmatched grep under pipefail must reach the explicit refusal instead of
    aborting the assignment early.
    """
    harness = f"""
        set -euo pipefail
        log() {{ printf 'log %s\\n' "$*"; }}
        die() {{ printf 'die %s\\n' "$*"; exit 3; }}
        KAFKA_VERSIONS="{offered}"
source "{AWS_RESOURCES}"
        choose_kafka_version "$KAFKA_VERSIONS"
        printf 'chose %s\\n' "$MSK_KAFKA_VERSION"
    """
    out = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    if chosen is None:
        assert out.returncode == 3, f"expected the refusal, got {out.returncode}: {out.stdout}{out.stderr}"
        assert "die no ACTIVE 3.x Kafka version" in out.stdout, out.stdout + out.stderr
    else:
        assert out.returncode == 0, out.stdout + out.stderr
        assert f"chose {chosen}" in out.stdout, out.stdout + out.stderr


# What MSK reports for a cluster, as `--output text` tab-separates the three
# fields `grow_broker_volume` reads in one call.
_CLUSTER_VERSION = "K3AEGXETSR30VB"


@dataclass(frozen=True)
class Growth:
    """One run of `grow_broker_volume`, and every `aws` call it made."""

    result: subprocess.CompletedProcess[str]
    calls: str


def _volume_growth(*, described: str, refusal: str = "", asked: int = 1000) -> Growth:
    """Run the actual MSK growth function against fixed responses. Record calls
    to a file because the function captures stdout and stderr for error handling.
    """
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "aws-calls.log"
        calls.touch()
        stub = f"""
            printf 'aws %s\n' "$*" >>'{calls}'
            case "$*" in
            *update-broker-storage*)
                if [[ -n '{refusal}' ]]; then
                    printf '%s\n' '{refusal}' >&2
                    return 254
                fi
                ;;
            *describe-cluster*) printf '%s\n' '{described}' ;;
            esac
        """
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\n' "$*"; }}
            die() {{ printf 'die %s\n' "$*"; exit 3; }}
            aws() {{{stub}}}
            MSK_NAME=a-cluster
            MSK_ARN=arn:aws:kafka:eu-west-1:123456789012:cluster/a-cluster/aaaa-1
            MSK_VOLUME_GIB={asked}
source "{AWS_RESOURCES}"
            grow_broker_volume
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return Growth(result=result, calls=calls.read_text())


@needs_bash
@pytest.mark.parametrize(
    ("state", "current", "grown"),
    [
        # Grow undersized volumes only when the cluster is ready.
        ("ACTIVE", 100, True),
        # Leave adequate volumes unchanged; MSK cannot shrink them.
        ("ACTIVE", 1000, False),
        ("ACTIVE", 2000, False),
        # An update may still report the old size. Do not request the same growth twice.
        ("UPDATING", 100, False),
        ("MAINTENANCE", 100, False),
    ],
)
def test_an_existing_broker_volume_is_grown_once_and_never_shrunk(state: str, current: int, grown: bool) -> None:
    grown_run = _volume_growth(described=f"{state}\t{_CLUSTER_VERSION}\t{current}")
    assert grown_run.result.returncode == 0, grown_run.result.stdout + grown_run.result.stderr
    issued = "update-broker-storage" in grown_run.calls
    assert issued is grown, grown_run.calls + grown_run.result.stdout
    if grown:
        assert "VolumeSizeGB=1000" in grown_run.calls, grown_run.calls
        # Use the reported version token for the update.
        assert f"--current-version {_CLUSTER_VERSION}" in grown_run.calls, grown_run.calls
    else:
        assert "log MSK broker volumes are" in grown_run.result.stdout, grown_run.result.stdout


@needs_bash
@pytest.mark.parametrize(
    "refusal",
    [
        # Allow transient non-ACTIVE and storage-update cooldown responses.
        "An error occurred (BadRequestException): The cluster must be in ACTIVE state",
        "An error occurred (BadRequestException): A previous storage update was performed in the last 6 hours",
    ],
)
def test_a_growth_msk_will_not_take_yet_leaves_the_setup_converging(refusal: str) -> None:
    refused = _volume_growth(described=f"ACTIVE\t{_CLUSTER_VERSION}\t100", refusal=refusal).result
    assert refused.returncode == 0, refused.stdout + refused.stderr
    assert "log a-cluster cannot increase broker storage to 1000 GiB yet" in refused.stdout, refused.stdout


@needs_bash
def test_a_growth_msk_refuses_for_any_other_reason_stops_the_setup() -> None:
    refused = _volume_growth(
        described=f"ACTIVE\t{_CLUSTER_VERSION}\t100",
        refusal="An error occurred (AccessDeniedException): not authorized to perform kafka:UpdateBrokerStorage",
    ).result
    assert refused.returncode == 3, refused.stdout + refused.stderr
    assert "die could not grow a-cluster's broker volumes to 1000 GiB" in refused.stdout, refused.stdout
    assert "AccessDeniedException" in refused.stdout, refused.stdout


@needs_bash
def test_a_broker_volume_size_msk_would_not_report_is_refused() -> None:
    unread = _volume_growth(described=f"ACTIVE\t{_CLUSTER_VERSION}\tNone").result
    assert unread.returncode == 3, unread.stdout + unread.stderr
    assert "die a-cluster reports no broker volume size" in unread.stdout, unread.stdout


@dataclass(frozen=True)
class BucketStep:
    """One run of a bucket step, and every `aws` call and question it made."""

    result: subprocess.CompletedProcess[str]
    calls: str
    asked: str


def _bucket_step(
    function: str, *, tags: str | None, answered: str = "yes", head_error: str | None = None
) -> BucketStep:
    """Run bucket operations against S3 stubs. tags is None for an absent bucket,
    empty for an unowned bucket, or the ownership tag value.
    """
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "aws-calls.log"
        calls.touch()
        asked = Path(directory) / "asked.log"
        asked.touch()
        if head_error is None and tags is None:
            head_error = "An error occurred (404) when calling the HeadBucket operation: Not Found"
        stub = f"""
            printf 'aws %s\n' "$*" >>'{calls}'
            case "$*" in
            *head-bucket*)
                if [[ -n {shlex.quote(head_error or "")} ]]; then
                    printf '%s\\n' {shlex.quote(head_error or "")} >&2
                    return 254
                fi
                ;;
            *get-bucket-tagging*)
                [[ -n '{tags or ""}' ]] || return 254
                printf '%s\n' '{tags or ""}'
                ;;
            *get-bucket-versioning*) printf 'None\n' ;;
            esac
        """
        harness = f"""
            set -euo pipefail
            log() {{ printf 'log %s\n' "$*"; }}
            die() {{ printf 'die %s\n' "$*"; exit 3; }}
            confirm() {{ printf '%s\n' "$*" >>'{asked}'; [[ '{answered}' == yes ]] || die "answered nothing"; }}
            aws() {{{stub}}}
            AWS_REGION=eu-west-1
            BUCKET=a-bucket
            TAG_KEY=lakehouse-ingest-bench
            ASSUME_YES=no
source "{AWS_RESOURCES}"
            {function}
        """
        result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
        return BucketStep(result=result, calls=calls.read_text(), asked=asked.read_text())


@needs_bash
def test_setup_refuses_to_reconfigure_a_bucket_it_did_not_create() -> None:
    """PutBucketTagging replaces all tags and PutBucketVersioning suspends versioning;
    either would alter an unrelated existing bucket.
    """
    refused = _bucket_step("create_bucket", tags="")
    assert refused.result.returncode == 3, refused.result.stdout
    assert "lakehouse-ingest-bench=true tag could not be verified" in refused.result.stdout, refused.result.stdout
    for mutation in ("put-bucket-tagging", "put-bucket-versioning", "put-public-access-block"):
        assert mutation not in refused.calls, refused.calls


@needs_bash
def test_setup_creates_a_bucket_and_re_runs_over_its_own() -> None:
    created = _bucket_step("create_bucket", tags=None)
    assert created.result.returncode == 0, created.result.stdout + created.result.stderr
    assert "create-bucket --bucket a-bucket" in created.calls, created.calls
    assert "put-bucket-tagging" in created.calls

    adopted = _bucket_step("create_bucket", tags="true")
    assert adopted.result.returncode == 0, adopted.result.stdout + adopted.result.stderr
    assert "create-bucket" not in adopted.calls, adopted.calls
    assert "put-bucket-tagging" in adopted.calls


@needs_bash
def test_teardown_refuses_to_empty_a_bucket_it_did_not_create() -> None:
    refused = _bucket_step("remove_bucket", tags="")
    assert refused.result.returncode == 3, refused.result.stdout
    assert "cannot verify the lakehouse-ingest-bench=true tag" in refused.result.stdout, refused.result.stdout
    assert "s3 rm" not in refused.calls and "delete-bucket" not in refused.calls, refused.calls
    assert refused.asked == "", "a bucket it will not empty is not one to ask about"


@needs_bash
def test_teardown_names_what_the_bucket_holds_and_asks_before_emptying_it() -> None:
    asked = _bucket_step("remove_bucket", tags="true", answered="no")
    assert asked.result.returncode == 3, asked.result.stdout
    assert "delete the resources listed above?" in asked.asked, asked.asked
    assert "every corpus generated into it" in asked.result.stdout, asked.result.stdout
    assert "s3 rm" not in asked.calls and "delete-bucket" not in asked.calls, asked.calls

    answered = _bucket_step("remove_bucket", tags="true", answered="yes")
    assert answered.result.returncode == 0, answered.result.stdout + answered.result.stderr
    assert "s3 rm s3://a-bucket --recursive" in answered.calls, answered.calls
    assert "delete-bucket --bucket a-bucket" in answered.calls, answered.calls


@needs_bash
def test_teardown_leaves_a_bucket_that_is_already_gone_alone() -> None:
    gone = _bucket_step("remove_bucket", tags=None)
    assert gone.result.returncode == 0, gone.result.stdout + gone.result.stderr
    assert "already gone" in gone.result.stdout, gone.result.stdout
    assert "s3 rm" not in gone.calls and gone.asked == ""


@needs_bash
@pytest.mark.parametrize(
    "error",
    [
        "An error occurred (403) when calling the HeadBucket operation: Forbidden",
        "An error occurred (500) when calling the HeadBucket operation: Internal Server Error",
        "Could not connect to the endpoint URL: https://s3.eu-west-1.amazonaws.com/a-bucket",
        "Unable to locate credentials",
    ],
)
def test_teardown_does_not_treat_a_failed_bucket_lookup_as_absence(error: str) -> None:
    refused = _bucket_step("remove_bucket", tags="true", head_error=error)
    assert refused.result.returncode == 3, refused.result.stdout + refused.result.stderr
    assert "could not inspect s3://a-bucket" in refused.result.stdout
    assert error in refused.result.stdout
    assert "already gone" not in refused.result.stdout
    assert "s3 rm" not in refused.calls and "delete-bucket" not in refused.calls
    assert refused.asked == ""


@needs_bash
def test_teardown_accepts_an_explicit_missing_bucket_error() -> None:
    gone = _bucket_step(
        "remove_bucket",
        tags=None,
        head_error="An error occurred (NoSuchBucket) when calling the HeadBucket operation: No such bucket",
    )
    assert gone.result.returncode == 0, gone.result.stdout + gone.result.stderr
    assert "already gone" in gone.result.stdout
    assert "s3 rm" not in gone.calls and "delete-bucket" not in gone.calls


# What MSK answers with: a comma-separated list of host:port. It is the value in
# a written site config that quoting has to survive, since a bare one reads as
# neither one scalar nor a mapping.
_MSK_BOOTSTRAP = (
    "b-1.a-cluster.abc123.c2.kafka.eu-west-1.amazonaws.com:9098,"
    "b-2.a-cluster.abc123.c2.kafka.eu-west-1.amazonaws.com:9098"
)


def _write_site(path: Path, *, registry: bool = False, yq_stub: str = "") -> subprocess.CompletedProcess[str]:
    """`setup.sh`'s own site writer, against the values a finished setup holds."""
    harness = f"""
        set -euo pipefail
        log() {{ printf 'log %s\\n' "$*"; }}
        die() {{ printf 'die %s\\n' "$*"; exit 3; }}
        ACCOUNT={IAM_VALUES["ACCOUNT"]}
        AWS_REGION={IAM_VALUES["REGION"]}
        BUCKET={IAM_VALUES["BUCKET"]}
        KAFKA_DEPLOYMENT=managed
        BOOTSTRAP='{_MSK_BOOTSTRAP}'
        KUBE_CONTEXT={SITE_AWS_FILLINGS["YOUR_KUBE_CONTEXT"]}
        NAMESPACE=ingest-bench
        HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
        FLINK_SERVICE_ACCOUNT=ingest-bench-flink
        SPARK_SERVICE_ACCOUNT=ingest-bench-spark
        NODE_SELECTOR='{{}}'
        TOLERATIONS='[]'
        WITH_SCHEMA_REGISTRY={"true" if registry else "false"}
        {yq_stub}
source "{AWS_RESOURCES}"
        write_site '{path}'
    """
    return subprocess.run(["bash", "-c", harness], capture_output=True, text=True)


@needs_shell_tools
@pytest.mark.parametrize("registry", [False, True], ids=["no-registry", "with-registry"])
def test_the_site_setup_writes_holds_every_key_the_example_does(tmp_path: Path, registry: bool) -> None:
    target = tmp_path / "site.yaml"
    written = _write_site(target, registry=registry)
    assert written.returncode == 0, written.stdout + written.stderr

    document = _mapping(yaml.safe_load(target.read_text()))
    example = _mapping(yaml.safe_load(SITE_AWS_EXAMPLE.read_text()))
    assert set(document) == set(example), document
    assert set(_mapping(document["catalog"])) == set(_mapping(example["catalog"]))
    assert set(_mapping(document["kubernetes"])) == set(_mapping(example["kubernetes"]))
    assert set(_mapping(document["pricing"])) == set(_mapping(example["pricing"]))
    # The example carries the registry commented out, because a site that
    # brings its own names that one instead.
    expected_kafka = set(_mapping(example["kafka"])) | ({"schema_registry"} if registry else set())
    assert set(_mapping(document["kafka"])) == expected_kafka

    site = load_site(target)
    assert site.kafka_bootstrap == _MSK_BOOTSTRAP
    # Concatenated rather than interpolated: an f-string here reads as a URI
    # naming a bucket to the leak scan in tests/test_public_surface.py.
    assert site.corpus_root == "s3://" + IAM_VALUES["BUCKET"] + "/corpus"
    assert site.catalog_props["warehouse"] == IAM_VALUES["ACCOUNT"]
    assert site.kubernetes is not None and site.kubernetes.context == SITE_AWS_FILLINGS["YOUR_KUBE_CONTEXT"]
    assert site.kubernetes.spark_service_account == "ingest-bench-spark"
    # Pricing must be filled in by the operator; zero is not publishable.
    assert (site.pricing_vcpu_hour_usd, site.pricing_gib_hour_usd) == (0.0, 0.0)
    if registry:
        assert site.schema_registry is not None and site.schema_registry.url.endswith("/apis/ccompat/v7")
    else:
        assert site.schema_registry is None


@needs_shell_tools
def test_the_written_site_is_never_an_overwrite(tmp_path: Path) -> None:
    existing = tmp_path / "site.yaml"
    kept = "corpus_root: s3://another-bucket/corpus\n"
    existing.write_text(kept)
    refused = _write_site(existing)
    assert refused.returncode == 3, refused.stdout + refused.stderr
    assert "already exists" in refused.stdout, refused.stdout
    assert existing.read_text() == kept


@needs_shell_tools
@pytest.mark.parametrize(
    "yq_stub",
    [
        "yq() { printf 'no such file\\n' >&2; return 1; }",
        "yq() { printf 'some-other-broker:9098\\n'; }",
    ],
    ids=["unreadable", "not-what-was-written"],
)
def test_a_site_that_did_not_read_back_is_removed(tmp_path: Path, yq_stub: str) -> None:
    target = tmp_path / "site.yaml"
    refused = _write_site(target, yq_stub=yq_stub)
    assert refused.returncode == 3, refused.stdout + refused.stderr
    assert "generated site configuration failed validation" in refused.stdout, refused.stdout
    assert not target.exists()


@needs_bash
def test_setup_refuses_an_existing_site_before_it_touches_the_account(tmp_path: Path) -> None:
    existing = tmp_path / "site.yaml"
    existing.write_text("corpus_root: s3://another-bucket/corpus\n")
    calls = tmp_path / "aws-calls.log"
    calls.touch()
    stubs = _stub_bin(tmp_path / "bin", {"aws": f"printf '%s\\n' \"$*\" >>'{calls}'\nexit 1\n"})
    refused = subprocess.run(
        [str(AWS_SETUP), "--write-site", str(existing)],
        capture_output=True,
        text=True,
        env={
            **os.environ,
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "AWS_REGION": IAM_VALUES["REGION"],
            "CLUSTER_NAME": SITE_AWS_FILLINGS["YOUR_KUBE_CONTEXT"],
        },
    )
    assert refused.returncode == 1, refused.stdout + refused.stderr
    assert "already exists" in refused.stderr, refused.stderr
    assert calls.read_text() == "", calls.read_text()


@needs_shell_tools
@pytest.mark.parametrize("release_status", ["missing", "deployed", "failed", "pending-install", "pending-upgrade"])
@pytest.mark.parametrize("version", [None, "2.6.0"])
def test_spark_operator_installation_is_pinned_and_idempotent(
    tmp_path: Path, release_status: str, version: str | None
) -> None:
    calls = tmp_path / "calls"
    harness = f"""
        set -euo pipefail
        source "{AWS_RESOURCES}"
        KUBE_CONTEXT=a-cluster
        NAMESPACE=ingest-bench
        log() {{ :; }}
        die() {{ printf '%s\\n' "$*" >&2; exit 1; }}
        # Retained CRDs must not suppress installation of the controller.
        kubectl() {{ return 0; }}
        helm() {{
            printf 'helm %s\\n' "$*" >>'{calls}'
            case "$*" in
                *status*)
                    if [[ '{release_status}' == missing ]]; then
                        echo 'Error: release: not found' >&2
                        return 1
                    fi
                    echo '{{"info": {{"status": "{release_status}"}}}}'
                    ;;
                *list*) printf '[]\\n' ;;
            esac
        }}
        install_spark_operator
        install_spark_operator
    """
    env = {key: value for key, value in os.environ.items() if key != "SPARK_OPERATOR_VERSION"}
    if version is not None:
        env["SPARK_OPERATOR_VERSION"] = version
    result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True, env=env)
    commands = calls.read_text().splitlines()
    installs = [command for command in commands if " upgrade --install " in command]
    if release_status.startswith("pending-"):
        assert result.returncode == 1
        assert release_status in result.stderr
        assert not installs
    else:
        assert result.returncode == 0, result.stderr
        expected = (
            "helm --kube-context a-cluster upgrade --install spark-operator spark-operator/spark-operator "
            "--namespace spark-operator --create-namespace "
            f"--version {version or '2.5.2'} --set spark.jobNamespaces={{ingest-bench}} "
            "--set spark.serviceAccount.create=false --set spark.rbac.create=false "
            "--set webhook.enable=true --wait"
        )
        assert installs == [expected, expected]


@needs_shell_tools
@pytest.mark.parametrize("answer", ["warning", "missing", "forbidden"])
def test_operator_status_keeps_diagnostics_out_of_json(tmp_path: Path, answer: str) -> None:
    harness = f"""
        set -euo pipefail
        source "{AWS_RESOURCES}"
        KUBE_CONTEXT=a-cluster
        die() {{ echo "$*" >&2; exit 1; }}
        trap 'echo caller-cleanup' EXIT
        helm() {{
            echo 'WARNING: kubeconfig is group-readable' >&2
            case '{answer}' in
                warning) echo '{{"info": {{"status": "deployed"}}}}' ;;
                missing) echo 'Error: release: not found' >&2; return 1 ;;
                forbidden) echo 'Error: forbidden' >&2; return 1 ;;
            esac
        }}
        check_operator_release operator operators
    """
    result = subprocess.run(
        ["bash", "-c", harness], capture_output=True, text=True, env={**os.environ, "TMPDIR": str(tmp_path)}
    )
    assert result.returncode == (1 if answer == "forbidden" else 0), result.stderr
    assert "WARNING: kubeconfig is group-readable" in result.stderr
    if answer == "forbidden":
        assert "Error: forbidden" in result.stderr
        assert "could not inspect operator release" in result.stderr
    assert result.stdout.strip() == "caller-cleanup"
    assert not list(tmp_path.glob("ingest-bench-helm.*"))


@needs_shell_tools
@pytest.mark.parametrize("release_status", ["missing", "failed", "pending-install"])
def test_flink_operator_reconciles_retained_crds(tmp_path: Path, release_status: str) -> None:
    calls = tmp_path / "calls"
    setup = AWS_SETUP.read_text()
    reconcile = setup[setup.index('check_operator_release "$FLINK_OPERATOR_RELEASE"') :]
    reconcile = reconcile[: reconcile.index("# Record the reconciled chart")]
    harness = f"""
        set -euo pipefail
        source "{AWS_RESOURCES}"
        KUBE_CONTEXT=a-cluster
        FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
        FLINK_OPERATOR_NAMESPACE=flink-operator
        FLINK_OPERATOR_VERSION=1.15.0
        log() {{ :; }}
        die() {{ echo "$*" >&2; exit 1; }}
        kubectl() {{ return 0; }}
        helm() {{
            echo "$*" >>'{calls}'
            if [[ "$*" == *status* ]]; then
                if [[ '{release_status}' == missing ]]; then
                    echo 'Error: release: not found' >&2
                    return 1
                fi
                echo '{{"info": {{"status": "{release_status}"}}}}'
            fi
        }}
        {reconcile}
    """
    result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    commands = calls.read_text()
    if release_status.startswith("pending-"):
        assert result.returncode == 1
        assert "upgrade --install" not in commands
    else:
        assert result.returncode == 0, result.stderr
        assert "upgrade --install flink-kubernetes-operator" in commands
        assert "--version 1.15.0 --set webhook.create=false --wait" in commands


@needs_shell_tools
@pytest.mark.parametrize("vpc_id", ["vpc-owned", "", "null"])
def test_msk_security_group_cleanup_requires_the_deployment_vpc(tmp_path: Path, vpc_id: str) -> None:
    calls = tmp_path / "calls"
    harness = f"""
        set -euo pipefail
        source "{AWS_RESOURCES}"
        VPC_ID='{vpc_id}'
        CLUSTER_NAME=a-cluster
        MSK_NAME=lakehouse-ingest-bench
        TAG_KEY=lakehouse-ingest-bench
        log() {{ :; }}
        die() {{ echo "$*" >&2; exit 1; }}
        aws() {{
            echo "$*" >>'{calls}'
            if [[ "$*" == *describe-security-groups* ]]; then
                if [[ "$*" == *Name=vpc-id,Values=vpc-owned* ]]; then
                    echo sg-owned
                else
                    echo sg-other-deployment
                fi
            fi
        }}
        remove_msk_security_group
    """
    result = subprocess.run(["bash", "-c", harness], capture_output=True, text=True)
    if vpc_id == "vpc-owned":
        assert result.returncode == 0, result.stderr
        commands = calls.read_text()
        assert "Name=tag:lakehouse-ingest-bench,Values=true" in commands
        assert "delete-security-group --group-id sg-owned" in commands
        assert "sg-other-deployment" not in commands
    else:
        assert result.returncode == 1
        assert "VPC_ID" in result.stderr
        assert not calls.exists()


def test_the_operator_chart_comes_from_the_archive_at_the_pinned_version() -> None:
    """downloads.apache.org removes old releases; archive.apache.org retains pins."""
    setup = AWS_SETUP.read_text()
    urls = re.findall(r'"(https://\S*flink-kubernetes-operator\S*)"', setup)
    assert urls == ["https://archive.apache.org/dist/flink/flink-kubernetes-operator-$FLINK_OPERATOR_VERSION/"], urls
    assert 'FLINK_OPERATOR_VERSION="${FLINK_OPERATOR_VERSION:-' in setup, "the pin should be overridable"


def test_every_engine_s_image_is_pushed_and_has_a_repository_to_be_pushed_to() -> None:
    prefix = re.search(r"^IMAGE_REPOSITORY_PREFIX=(\S+)$", (SCRIPTS / "_k8s.sh").read_text(), re.M)
    assert prefix is not None, "_k8s.sh no longer states the registry path both images are pushed under"
    listed = re.search(r'^ECR_REPOSITORIES="([^"]+)"$', AWS_SETUP.read_text(), re.M)
    assert listed is not None, "setup.sh no longer states one list of ECR repositories"
    created = set(listed.group(1).split())

    pushed = set(
        re.findall(r'^\w+_REF="\$REGISTRY/\$IMAGE_REPOSITORY_PREFIX/(\S+):\$TAG"$', PUSH_IMAGES.read_text(), re.M)
    )
    assert pushed, "push-images.sh no longer builds any reference from the registry and the tag"
    assert {f"{prefix.group(1)}/{name}" for name in pushed} == created

    # The harness image is nobody's engine, so it is the one pushed repository
    # with no renderer behind it.
    rendered = {str(engines.knobs_for(engine).IMAGE_REPOSITORY) for engine in engines.MANAGED}
    assert rendered == created - {f"{prefix.group(1)}/harness"}


@needs_bash
def test_image_pushes_use_the_requested_platform_for_every_image(tmp_path: Path) -> None:
    site = tmp_path / "site.yaml"
    site.write_text(_filled_site())
    calls = tmp_path / "calls"
    stubs = _stub_bin(
        tmp_path / "bin",
        {
            "yq": """
case "$1" in
  .kubernetes.registry) printf '%s\\n' registry.example ;;
  .kubernetes.aws_region) printf '%s\\n' eu-west-1 ;;
  *) exit 1 ;;
esac
""",
            "git": """
if [[ $* == *'rev-parse --short HEAD' ]]; then printf '%s\\n' abc1234; else exit 0; fi
""",
            "aws": "printf '%s\\n' password",
            "docker": f"printf '%s\\n' \"$*\" >>'{calls}'\n[[ $1 != login ]] || read -r _",
        },
    )
    result = subprocess.run(
        [str(PUSH_IMAGES), "--site", str(site), "--platform", "linux/arm64"],
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}"},
    )
    assert result.returncode == 0, result.stdout + result.stderr
    commands = calls.read_text().splitlines()
    builds = [command for command in commands if command.startswith("build ")]
    assert len(builds) == 3, commands
    assert all("--platform linux/arm64" in command for command in builds), builds
    assert any("engines/flink/Dockerfile" in command for command in builds), builds


def test_the_setup_script_renders_only_the_placeholders_it_exports() -> None:
    setup = AWS_SETUP.read_text()
    for path in _iam_documents():
        for name in Template(path.read_text()).get_identifiers():
            assert name in IAM_VALUES, f"{path.name} uses ${{{name}}}, which this test does not know about"
            assert f"${{{name}}}" in setup, f"{path.name} uses ${{{name}}}, which setup.sh never gives envsubst"


# ---------------------------------------------------------------------------
# The IAM documents
# ---------------------------------------------------------------------------


def test_every_iam_document_renders_to_a_policy() -> None:
    documents = _iam_documents()
    assert documents, f"no IAM documents under {AWS_DEPLOY / 'iam'}"
    for path in documents:
        document = _rendered_policy(path)
        assert document["Version"] == "2012-10-17", path.name
        for statement in _statements(document):
            assert statement["Effect"] == "Allow", f"{path.name}: {statement['Sid']} is not an Allow"


def test_the_trust_document_grants_assume_role_and_tag_session() -> None:
    """EKS Pod Identity tags sessions, so its trust policy needs both actions."""
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "trust.json"))
    pod_identity = [s for s in statements if s["Principal"] == {"Service": "pods.eks.amazonaws.com"}]
    assert len(pod_identity) == 1, statements
    assert _actions(pod_identity[0]) == {"sts:AssumeRole", "sts:TagSession"}


def test_the_harness_policy_covers_the_three_bucket_prefixes() -> None:
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))
    bucket = f"arn:aws:s3:::{IAM_VALUES['BUCKET']}"

    listing = _one(statements, {bucket}, "listing the bucket")
    assert "s3:ListBucket" in _actions(listing)

    prefixes = {f"{bucket}/corpus/*", f"{bucket}/runs/*", f"{bucket}/warehouse/*"}
    objects = _one(statements, prefixes, "the objects under the three prefixes")
    assert _actions(objects) == {"s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"}


def test_the_harness_policy_names_the_table_namespace_the_harness_uses() -> None:
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))
    region, account = IAM_VALUES["REGION"], IAM_VALUES["ACCOUNT"]
    glue = _one(
        statements,
        {
            f"arn:aws:glue:{region}:{account}:catalog",
            f"arn:aws:glue:{region}:{account}:database/{TABLE_NAMESPACE}",
            f"arn:aws:glue:{region}:{account}:table/{TABLE_NAMESPACE}/*",
        },
        "the Glue catalog, database and tables",
    )
    assert {"glue:GetCatalog", "glue:CreateDatabase", "glue:CreateTable", "glue:UpdateTable"} <= _actions(glue)


def test_the_msk_topic_statement_allows_idempotent_writes() -> None:
    """InitProducerId requires WriteDataIdempotently on the cluster ARN, not a topic."""
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "harness-policy.json"))

    topics = _one(statements, {IAM_VALUES["MSK_TOPIC_ARN"]}, "the topics")
    actions = _actions(topics)
    assert "kafka-cluster:WriteDataIdempotently" not in actions
    assert {"kafka-cluster:WriteData", "kafka-cluster:ReadData", "kafka-cluster:CreateTopic"} <= actions

    cluster = _one(statements, {IAM_VALUES["MSK_ARN"]}, "the cluster")
    assert {"kafka-cluster:Connect", "kafka-cluster:WriteDataIdempotently"} <= _actions(cluster)

    groups = _one(statements, {IAM_VALUES["MSK_GROUP_ARN"]}, "the consumer groups")
    assert _actions(groups) == {"kafka-cluster:DescribeGroup", "kafka-cluster:AlterGroup"}


# ---------------------------------------------------------------------------
# The Kubernetes manifest and the eksctl example
# ---------------------------------------------------------------------------


def test_the_namespace_manifest_renders_every_identity_and_each_engine_s_rbac() -> None:
    template = AWS_DEPLOY / "k8s" / "namespace.yaml.tmpl"
    rendered = Template(template.read_text()).substitute({"NAMESPACE": "a-namespace"})
    objects = [document for document in yaml.safe_load_all(rendered) if document is not None]

    by_kind: dict[str, list[dict[str, object]]] = {}
    for document in objects:
        mapping = _mapping(document)
        by_kind.setdefault(str(mapping["kind"]), []).append(mapping)
    assert sorted(by_kind) == ["Namespace", "Role", "RoleBinding", "ServiceAccount"]
    for kind, documents in by_kind.items():
        for document in documents:
            metadata = _mapping(document["metadata"])
            named = metadata["name"] if kind == "Namespace" else metadata["namespace"]
            assert named == "a-namespace", f"{kind} was rendered into {named!r}"

    # ServiceAccount names must agree between the example and rendered manifest.
    site = _mapping(_mapping(yaml.safe_load(SITE_AWS_EXAMPLE.read_text()))["kubernetes"])
    accounts = {str(_mapping(document["metadata"])["name"]) for document in by_kind["ServiceAccount"]}
    assert accounts == {
        site["harness_service_account"],
        site["flink_service_account"],
        site["spark_service_account"],
    }

    # Enumerate the resources and verbs each engine needs to create its workers; reject wildcard
    # grants.
    expected: dict[str, tuple[set[str], set[tuple[str, str]]]] = {
        str(site["flink_service_account"]): (
            {"get", "list", "watch", "create", "update", "patch", "delete"},
            {("", "pods"), ("", "configmaps"), ("apps", "deployments"), ("apps", "deployments/finalizers")},
        ),
        str(site["spark_service_account"]): (
            {"get", "list", "watch", "create", "update", "patch", "delete", "deletecollection"},
            {("", "pods"), ("", "configmaps"), ("", "persistentvolumeclaims"), ("", "services")},
        ),
    }
    roles = {str(_mapping(role["metadata"])["name"]): role for role in by_kind["Role"]}
    assert set(roles) == set(expected)
    for name, (verbs, resources) in expected.items():
        granted: set[tuple[str, str]] = set()
        for entry in _sequence(roles[name]["rules"]):
            rule = _mapping(entry)
            assert {str(verb) for verb in _sequence(rule["verbs"])} == verbs, name
            for group in _sequence(rule["apiGroups"]):
                for resource in _sequence(rule["resources"]):
                    granted.add((str(group), str(resource)))
        assert granted == resources, name

    # Bind each engine account to its own Role.
    bindings = {str(_mapping(binding["metadata"])["name"]): binding for binding in by_kind["RoleBinding"]}
    assert set(bindings) == set(expected)
    for name, binding in bindings.items():
        assert _mapping(binding["roleRef"])["name"] == name
        subject = _mapping(_sequence(binding["subjects"])[0])
        assert subject["name"] == name
        assert subject["namespace"] == "a-namespace"


def test_the_schema_registry_manifest_renders_a_deployment_and_a_service() -> None:
    template = REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl"
    rendered = render_template(
        template,
        {"NAMESPACE": "a-namespace", "NODE_SELECTOR": '{"kubernetes.io/arch": "amd64"}', "TOLERATIONS": "[]"},
    )
    objects = [_mapping(document) for document in yaml.safe_load_all(rendered) if document is not None]
    by_kind = {str(document["kind"]): document for document in objects}
    assert sorted(by_kind) == ["Deployment", "Service"]
    for kind, document in by_kind.items():
        metadata = _mapping(document["metadata"])
        assert metadata["name"] == "schema-registry", kind
        assert metadata["namespace"] == "a-namespace", kind

    deployment = _mapping(by_kind["Deployment"]["spec"])
    pod = _mapping(_mapping(deployment["template"])["spec"])
    assert pod["nodeSelector"] == {"kubernetes.io/arch": "amd64"} and pod["tolerations"] == []
    container = _mapping(_sequence(pod["containers"])[0])
    # The registry may accept connections before registrations; readiness must gate the Service.
    assert _mapping(_mapping(container["readinessProbe"])["httpGet"])["path"] == "/health/ready"
    assert _mapping(_mapping(container["livenessProbe"])["httpGet"])["path"] == "/health/live"

    service = _mapping(by_kind["Service"]["spec"])
    selector = _mapping(service["selector"])
    labels = _mapping(_mapping(_mapping(deployment["template"])["metadata"])["labels"])
    assert selector.items() <= labels.items(), "the Service selects labels the pod does not carry"
    assert _mapping(_sequence(service["ports"])[0])["port"] == 8080


def test_the_setup_script_substitutes_every_marker_the_registry_template_carries() -> None:
    template = REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl"
    setup = AWS_SETUP.read_text()
    markers = {match[2:-2] for match in MARKER_RE.findall(template.read_text())}
    assert markers == {"NAMESPACE", "NODE_SELECTOR", "TOLERATIONS"}
    for marker in markers:
        assert f"s|__{marker}__|$" in setup, f"setup.sh never substitutes __{marker}__"
    assert "WITH_SCHEMA_REGISTRY" in setup
    assert "schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7" in setup, "setup.sh never prints the registry URL"


def test_the_stack_and_the_cluster_run_the_same_registry_image() -> None:
    template = (REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl").read_text()
    compose = yaml.safe_load((REPO_ROOT / "deploy" / "compose" / "local" / "docker-compose.yml").read_text())
    image = str(_mapping(_mapping(_mapping(compose)["services"])["schema-registry"])["image"])
    assert f"image: {image}" in template


def test_the_eksctl_example_parses_and_holds_its_placeholders() -> None:
    config = yaml.safe_load((AWS_DEPLOY / "eksctl-cluster.example.yaml").read_text())
    assert config["kind"] == "ClusterConfig"
    assert config["metadata"]["name"] == "YOUR_CLUSTER_NAME"
    assert config["metadata"]["region"] == "YOUR_REGION"
    assert "eks-pod-identity-agent" in {addon["name"] for addon in config["addons"]}
    assert len(config["managedNodeGroups"]) == 1
    assert config["managedNodeGroups"][0]["privateNetworking"] is True
    # One zone for every node group: inter-zone bytes are billed.
    assert config["managedNodeGroups"][0]["availabilityZones"] == ["YOUR_ZONE"]


# ---------------------------------------------------------------------------
# The example site config
# ---------------------------------------------------------------------------


def test_the_aws_site_example_refuses_its_own_placeholders(tmp_path: Path) -> None:
    copied = tmp_path / "site.yaml"
    copied.write_text(SITE_AWS_EXAMPLE.read_text())
    with pytest.raises(ValueError, match="YOUR_"):
        load_site(copied)


# ---------------------------------------------------------------------------
# The in-cluster stack: the AWS hook and what it renders
# ---------------------------------------------------------------------------

STACK_HOOKS_AWS = AWS_DEPLOY / "_stack_hooks.sh"


def test_the_stack_policy_covers_the_three_prefixes_and_nothing_else() -> None:
    """The in-cluster stack needs bucket access but no MSK or Glue permissions."""
    statements = _statements(_rendered_policy(AWS_DEPLOY / "iam" / "stack-policy.json"))
    bucket = f"arn:aws:s3:::{IAM_VALUES['BUCKET']}"
    listing = _one(statements, {bucket}, "listing the bucket")
    assert _actions(listing) == {"s3:ListBucket"}
    prefixes = {f"{bucket}/corpus/*", f"{bucket}/runs/*", f"{bucket}/warehouse/*"}
    objects = _one(statements, prefixes, "the objects under the three prefixes")
    assert _actions(objects) == {"s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:DeleteObject"}
    assert len(statements) == 2
    for statement in statements:
        for action in _actions(statement):
            assert action.startswith("s3:"), action


def test_the_stack_policy_uses_only_the_bucket_placeholder() -> None:
    """The hook exports only BUCKET; additional placeholders would reach IAM unresolved."""
    identifiers = set(Template((AWS_DEPLOY / "iam" / "stack-policy.json").read_text()).get_identifiers())
    assert identifiers == {"BUCKET"}
    assert "envsubst '${BUCKET}'" in STACK_HOOKS_AWS.read_text()


def test_the_kafka_storage_class_renders_a_provisioned_gp3_class() -> None:
    template = AWS_DEPLOY / "k8s" / "kafka-storageclass.yaml.tmpl"
    values = {"KAFKA_STORAGE_CLASS": "a-class", "KAFKA_VOLUME_THROUGHPUT_MIBS": "250", "KAFKA_VOLUME_IOPS": "6000"}
    assert set(Template(template.read_text()).get_identifiers()) == set(values)
    rendered = _mapping(yaml.safe_load(Template(template.read_text()).substitute(values)))
    assert rendered["kind"] == "StorageClass"
    assert _mapping(rendered["metadata"])["name"] == "a-class"
    assert rendered["provisioner"] == "ebs.csi.aws.com"
    parameters = _mapping(rendered["parameters"])
    assert parameters["type"] == "gp3"
    # CSI parameters must be strings; the API server does not coerce numbers.
    assert parameters["throughput"] == "250" and parameters["iops"] == "6000"
    # Allow expansion and provision each volume in its broker's zone.
    assert rendered["allowVolumeExpansion"] is True
    assert rendered["volumeBindingMode"] == "WaitForFirstConsumer"
    hooks = STACK_HOOKS_AWS.read_text()
    for name in values:
        assert f"${{{name}}}" in hooks, f"the hook never gives envsubst ${{{name}}}"


def test_the_kafka_nodegroup_example_parses_tainted_and_labelled() -> None:
    """The selector targets broker nodes; the taint excludes pods without a toleration."""
    config = yaml.safe_load((AWS_DEPLOY / "eksctl-kafka-nodegroup.example.yaml").read_text())
    assert config["kind"] == "ClusterConfig"
    assert config["metadata"] == {"name": "YOUR_CLUSTER_NAME", "region": "YOUR_REGION"}
    assert len(config["managedNodeGroups"]) == 1
    group = config["managedNodeGroups"][0]
    assert group["name"] == "kafka" and group["privateNetworking"] is True
    assert group["labels"] == {"lakehouse-ingest-bench/role": "kafka"}
    assert group["taints"] == [{"key": "lakehouse-ingest-bench/kafka", "value": "true", "effect": "NoSchedule"}]
    assert group["desiredCapacity"] == group["minSize"] == group["maxSize"] == 3
    assert group["availabilityZones"] == ["YOUR_ZONE"]


@needs_bash
def test_the_aws_hook_defines_every_function_the_stack_calls() -> None:
    text = STACK_HOOKS_AWS.read_text()
    required = {
        "stack_preflight",
        "stack_preflight_storage",
        "stack_bind_identity",
        "stack_unbind_identity",
        "stack_storage_class",
        "stack_catalog_settings",
        "stack_storage_profile_json",
        "stack_storage_credential_json",
        "stack_delete_storage_class",
    }
    loaded = subprocess.run(
        ["bash", "-c", 'source "$1"; declare -F', "_", str(STACK_HOOKS_AWS)],
        capture_output=True,
        text=True,
    )
    assert loaded.returncode == 0, loaded.stderr
    assert required <= {line.split()[-1] for line in loaded.stdout.splitlines()}
    assert not text.startswith("#!"), "a sourced file, not an entrypoint"
    assert text.splitlines()[0] == "# SPDX-License-Identifier: CC-BY-NC-SA-4.0"


@needs_shell_tools
def test_the_storage_profile_keeps_the_catalog_off_the_data_path() -> None:
    """Pods use their own identities, so disable credential vending and remote signing."""
    library = f'source "{STACK_HOOKS_AWS}"'
    out = subprocess.run(
        ["bash", "-c", f"{library}\nstack_storage_profile_json"],
        capture_output=True,
        text=True,
        env={**os.environ, "BUCKET": "a-bucket", "AWS_REGION": "eu-west-1"},
    )
    assert out.returncode == 0, out.stderr
    profile = _mapping(json.loads(out.stdout))
    assert profile == {
        "type": "s3",
        "bucket": "a-bucket",
        "key-prefix": "warehouse",
        "region": "eu-west-1",
        "flavor": "aws",
        "sts-enabled": False,
        "remote-signing-enabled": False,
    }


@needs_shell_tools
def test_the_storage_credential_is_the_pods_own_identity() -> None:
    library = f'source "{STACK_HOOKS_AWS}"'
    out = subprocess.run(
        ["bash", "-c", f'{library}\nstack_storage_credential_json "$1"', "_", "an-external-id"],
        capture_output=True,
        text=True,
    )
    assert out.returncode == 0, out.stderr
    assert _mapping(json.loads(out.stdout)) == {
        "type": "s3",
        "credential-type": "aws-system-identity",
        "external-id": "an-external-id",
    }


def test_setup_creates_the_warehouse_with_hard_deletes() -> None:
    """purge.sh removes table files; soft deletion would leave their location reserved."""
    setup = STACK_SETUP.read_text()
    assert '"delete-profile": {type: "hard"}' in setup
    assert "stack_storage_profile_json" in setup and 'stack_storage_credential_json "$EXTERNAL_ID"' in setup


def test_the_aws_site_example_loads_once_every_placeholder_is_filled(tmp_path: Path) -> None:
    text = SITE_AWS_EXAMPLE.read_text()
    # Off the parsed document rather than off the file's text: the header
    # comment names the placeholder convention and would otherwise count as one.
    in_values = set(re.findall(r"YOUR_[A-Z_]+", yaml.safe_dump(yaml.safe_load(text))))
    assert in_values == set(SITE_AWS_FILLINGS), "the example's placeholders and the ones filled here have drifted"
    for placeholder, value in SITE_AWS_FILLINGS.items():
        text = text.replace(placeholder, value)
    copied = tmp_path / "site.yaml"
    copied.write_text(text)

    site = load_site(copied)
    assert site.corpus_root == "s3://a-bucket/corpus"
    assert site.runs_root == "s3://a-bucket/runs"
    assert site.warehouse == "s3://a-bucket/warehouse"
    assert site.kafka_bootstrap.endswith(":9098")
    # MSK IAM is SASL over TLS, and the region is the harness's own key for
    # signing the token rather than a librdkafka property.
    assert site.kafka_security["security.protocol"] == "SASL_SSL"
    assert site.kafka_security["sasl.mechanism"] == "OAUTHBEARER"
    assert site.kafka_security["aws.region"] == "eu-west-1"
    # Glue's `warehouse` is a catalog id, so it stays an account id and the
    # table's location comes from `site.warehouse` instead.
    assert site.catalog_props["warehouse"] == "123456789012"
    assert site.catalog_props["uri"] == "https://glue.eu-west-1.amazonaws.com/iceberg"
    assert site.catalog_props["rest.signing-name"] == "glue"
    # Compare the full Kubernetes block to catch keys lost during loading.
    assert site.kubernetes == KubernetesConfig(
        context="a-cluster",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={},
        tolerations=[],
    )


@needs_shell_tools
@pytest.mark.parametrize("region", ["eu-west-1", None])
def test_the_job_env_names_the_region_under_both_names_an_sdk_reads(tmp_path: Path, region: str | None) -> None:
    site = _filled_site()
    if region is None:
        site = "".join(line for line in site.splitlines(keepends=True) if "aws_region" not in line)
    site_file = tmp_path / "site.yaml"
    site_file.write_text(site)
    program = "\n".join(
        [
            "set -euo pipefail",
            "PREREQ_DOC=deploy/aws/README.md",
            "source scripts/_lib.sh",
            f"SITE_FILE={site_file}",
            "source scripts/_k8s.sh",
            "site_env_json",
        ]
    )
    out = subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    expected = [] if region is None else [{"name": name, "value": region} for name in REGION_ENV_NAMES]
    assert json.loads(out.stdout) == expected


def _site_reader(site_file: Path, call: str) -> subprocess.CompletedProcess[str]:
    """One of `_k8s.sh`'s site readers, run against ``site_file``, its stdout captured."""
    program = "\n".join(
        [
            "set -euo pipefail",
            "PREREQ_DOC=deploy/aws/README.md",
            "source scripts/_lib.sh",
            f"SITE_FILE={site_file}",
            "source scripts/_k8s.sh",
            call,
        ]
    )
    return subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)


@needs_shell_tools
@pytest.mark.parametrize(
    "value", ["${env:IB_KAFKA_PASSWORD}", "a user with spaces", "a'b\"c\\d", "line one\nline two", "$(HOME)"]
)
def test_site_properties_preserve_argument_boundaries(tmp_path: Path, value: str) -> None:
    site_file = tmp_path / "site.yaml"
    site_file.write_text(yaml.safe_dump({"kafka": {"security": {"sasl.password": value}}}))
    built = _site_reader(
        site_file,
        "read_site_prop_flags '.kafka.security' --kafka-prop\njob_command_json produce \"${SITE_PROP_FLAGS[@]}\"",
    )
    assert built.returncode == 0, built.stderr
    serialized = json.loads(built.stdout)
    assert serialized[-1] == f"sasl.password={value}".replace("$", "$$")
    command = [arg.replace("$$", "$") for arg in serialized]
    assert command == ["produce", "--kafka-prop", f"sasl.password={value}"]


@needs_shell_tools
def test_invalid_site_properties_fail_before_building_a_command(tmp_path: Path) -> None:
    site_file = tmp_path / "site.yaml"
    site_file.write_text("kafka:\n  security:\n    sasl.password: [not, a, string]\n")
    built = _site_reader(site_file, "read_site_prop_flags '.kafka.security' --kafka-prop")
    assert built.returncode != 0


@needs_shell_tools
@pytest.mark.parametrize("secret", ["ingest-bench-env", None])
def test_the_env_a_pod_reads_a_secret_from_is_the_one_the_site_names(tmp_path: Path, secret: str | None) -> None:
    site_file = tmp_path / "site.yaml"
    site = _filled_site()
    if secret is not None:
        site = site.replace("  aws_region:", f"  secret_name: {secret}\n  aws_region:")
    site_file.write_text(site)
    out = _site_reader(site_file, "site_env_from_json")
    assert out.returncode == 0, out.stderr
    expected = [] if secret is None else [{"secretRef": {"name": secret}}]
    assert json.loads(out.stdout) == expected


@needs_shell_tools
@pytest.mark.parametrize("path", [".corpus_root", ".runs_root", ".warehouse"])
def test_a_root_these_drivers_cannot_reach_is_refused_by_name(tmp_path: Path, path: str) -> None:
    """Drivers use aws s3 even though corpus tools also support gs URIs."""
    site_file = tmp_path / "site.yaml"
    key = path.removeprefix(".")
    site_file.write_text(
        "\n".join(
            line if not line.startswith(f"{key}:") else f"{key}: gs://a-bucket/{key}"
            for line in _filled_site().splitlines()
        )
    )
    refused = _site_reader(site_file, f"site_root '{path}'")
    assert refused.returncode != 0
    assert "must be an s3:// URI" in refused.stderr, refused.stderr
    assert key in refused.stderr, refused.stderr
    # And an S3 root is answered with itself.
    (tmp_path / "aws.yaml").write_text(_filled_site())
    answered = _site_reader(tmp_path / "aws.yaml", f"site_root '{path}'")
    assert answered.returncode == 0, answered.stderr
    assert answered.stdout.startswith("s3://a-bucket/"), answered.stdout


def test_the_shell_calls_the_harness_with_arguments_it_takes() -> None:
    checked = 0
    for script in _shell_entrypoints():
        for snippet in re.findall(r"python -c '(.*?)'", script.read_text()):
            program = ast.parse(snippet.replace('\\"', '"'))
            modules = {
                alias.asname or alias.name: importlib.import_module(f"ingest_bench.{alias.name}")
                for node in ast.walk(program)
                if isinstance(node, ast.ImportFrom) and node.module == "ingest_bench"
                for alias in node.names
            }
            for call in ast.walk(program):
                if not isinstance(call, ast.Call) or not isinstance(call.func, ast.Attribute):
                    continue
                target = call.func.value
                if not isinstance(target, ast.Name) or target.id not in modules:
                    continue
                try:
                    args = [ast.literal_eval(argument) for argument in call.args]
                    kwargs = {str(word.arg): ast.literal_eval(word.value) for word in call.keywords}
                except ValueError:
                    # An argument computed rather than written down; its own
                    # call is checked on its own turn through this loop.
                    continue
                function = getattr(modules[target.id], call.func.attr)
                signature = inspect.signature(function)
                signature.bind(*args, **kwargs)  # raises TypeError on an argument it does not take
                checked += 1
    assert checked >= 2, f"expected to find harness calls in the shell to check, bound {checked}"


def test_smoke_passes_the_exactness_mode_to_terminal_validation() -> None:
    command = next(line for line in SMOKE.read_text().splitlines() if 'harness "verify-table ' in line)
    assert "--exactness $EXACTNESS" in command


def test_the_smoke_verifies_an_upsert_run_by_its_live_state_and_key() -> None:
    text = SMOKE.read_text()
    # verify-table rejects the superseded rows a correct upsert table holds, even with compaction.
    assert "if [[ -n $COMPACTION_DRIVER && $EXACTNESS != LATEST_PER_KEY ]]; then" in text
    assert '--key-column "$(jq -r \'.upsert.key_columns[0]\' "$RUN_DIR/facts.json")"' in text


def test_the_smoke_offers_the_run_the_spec_asks_for() -> None:
    text = SMOKE.read_text()
    for key in ("speed", "seconds", "behind_max_ms", "compression"):
        assert f"yq '.producer.{key}'" in text, f"smoke.sh never reads producer.{key}"
    for flag in (
        "--speed $SPEED",
        "--seconds $REPLAY_SECONDS",
        "--behind-max-ms $BEHIND_MAX_MS",
        "--compression $COMPRESSION",
    ):
        assert flag in text, f"smoke.sh reads a producer key but never passes {flag.split()[0]}"
    assert "--speed 1" not in text, "smoke.sh still hardcodes a replay speed"


@pytest.mark.parametrize("script", (SMOKE,), ids=lambda path: path.name)
def test_smoke_tells_the_scorer_who_runs_the_ddl(script: Path) -> None:
    text = script.read_text()
    assert "yq '.table.managed_by'" in text, f"{script.name} never reads table.managed_by"
    assert "--table-managed-by $MANAGED_BY" in text, f"{script.name} reads it but never passes it"


@pytest.mark.parametrize("script", (SMOKE, LAUNCH), ids=lambda path: path.name)
def test_launchers_pass_a_resolved_exactness_mode_to_the_scorer(script: Path) -> None:
    text = script.read_text()
    assert "yq '.scoring.exactness // \"EXACTLY_ONCE\"'" in text
    assert "--exactness" in text and "$EXACTNESS" in text


@pytest.mark.parametrize("script", (SMOKE,), ids=lambda path: path.name)
def test_smoke_frames_the_values_the_way_staging_did(script: Path) -> None:
    text = script.read_text()
    for fact in ("value_encoding", "schema_id"):
        assert f"jq -r '.{fact} // empty'" in text, f"{script.name} never reads {fact} out of facts.json"
    assert "--value-encoding $VALUE_ENCODING" in text and "--schema-id $SCHEMA_ID" in text


@pytest.mark.parametrize("script", (SMOKE,), ids=lambda path: path.name)
def test_smoke_offers_the_codec_the_spec_asks_for(script: Path) -> None:
    text = script.read_text()
    assert "yq '.producer.compression'" in text, f"{script.name} never reads producer.compression"
    assert "--compression $COMPRESSION" in text, f"{script.name} reads the codec but never passes --compression"
    for codec in ("zstd", "lz4", "snappy", "gzip"):
        assert f"--compression {codec}" not in text, f"{script.name} hardcodes a codec"


def _compose_hooks() -> list[str]:
    """The hooks `smoke.sh` calls, read off the loop that refuses a missing one."""
    match = re.search(r"for hook in ((?:engine_compose_\w+ ?)+); do", SMOKE.read_text())
    assert match is not None, "smoke.sh no longer states which hooks an engine declares"
    return match.group(1).split()


def _shell_functions(path: Path) -> set[str]:
    return set(re.findall(r"^(\w+)\(\) \{$", path.read_text(), flags=re.MULTILINE))


def test_every_engine_declares_the_whole_compose_contract() -> None:
    hooks = _compose_hooks()
    assert len(hooks) == 4, hooks
    files = _engine_compose_files()
    assert {path.parent.name for path in files} == set(engines.MANAGED), "one Compose shape per managed engine"
    for path in files:
        assert set(hooks) <= _shell_functions(path), f"{path} declares {sorted(_shell_functions(path) & set(hooks))}"


def _compactor_compose_hooks() -> list[str]:
    """The hooks `smoke.sh` calls on a compaction driver."""
    match = re.search(r"for hook in ((?:compactor_compose_\w+ ?)+); do", SMOKE.read_text())
    assert match is not None, "smoke.sh no longer states which hooks a compaction driver declares"
    return match.group(1).split()


def test_every_compaction_driver_declares_the_whole_compose_contract() -> None:
    """The layer owns its lifecycle the way an engine does, minus readiness.

    There is no readiness hook because what the sidecar did is read from the
    table afterwards; nothing has to ask the driver whether it is working.
    """
    hooks = _compactor_compose_hooks()
    assert len(hooks) == 3, hooks
    assert not any(hook.endswith("_ready") for hook in hooks), hooks
    files = sorted((REPO_ROOT / "compactors").glob("*/compose.sh"))
    directories = {path.parent.name for path in files}
    assert directories == {driver.replace("-", "_") for driver in compactors.MANAGED}
    for path in files:
        assert set(hooks) <= _shell_functions(path), f"{path} declares {sorted(_shell_functions(path))}"


def test_the_smoke_names_no_engine_or_compactor_service_of_its_own() -> None:
    services = {
        service
        for pattern in ("engines/*/compose.yaml", "compactors/*/compose.yaml")
        for path in REPO_ROOT.glob(pattern)
        for service in _mapping(yaml.safe_load(path.read_text())["services"])
    }
    assert services, "no engine or compactor compose file declares a service"
    for script in sorted(SCRIPTS.glob("*.sh")):
        text = script.read_text()
        named = sorted(service for service in services if service in text)
        assert not named, f"{script.name} names the engine service(s) {named}"


@needs_shell_tools
def test_the_stack_activates_every_profile_its_engines_and_compactors_declare() -> None:
    declared = {
        profile
        for pattern in ("engines/*/compose.yaml", "compactors/*/compose.yaml")
        for path in REPO_ROOT.glob(pattern)
        for service in _mapping(yaml.safe_load(path.read_text())["services"]).values()
        for profile in _sequence(_mapping(service).get("profiles", []))
    }
    assert declared, "no engine or compactor compose file declares a profile"
    with tempfile.TemporaryDirectory() as directory:
        calls = Path(directory) / "docker-calls.log"
        program = "\n".join(
            [
                "set -euo pipefail",
                "source scripts/_lib.sh",
                f"docker() {{ printf '%s\\n' \"$*\" >>'{calls}'; }}",
                "compose config --services",
            ]
        )
        out = subprocess.run(["bash", "-c", program], cwd=REPO_ROOT, capture_output=True, text=True)
        assert out.returncode == 0, out.stderr
        arguments = calls.read_text().split()
    activated = {arguments[index + 1] for index, word in enumerate(arguments) if word == "--profile"}
    assert activated == declared | {"tools"}, activated


def test_the_smoke_stages_the_spec_it_was_given() -> None:
    text = SMOKE.read_text()
    assert "--spec)" in text and 'SPEC_FILE="$REPO_ROOT/specs/smoke/smoke-$ENGINE.yaml"' in text
    assert '-v "$SPEC_FILE:/spec.yaml:ro" harness "stage --spec /spec.yaml' in text
    assert (REPO_ROOT / "specs/smoke/smoke-external-confluent.yaml").exists()


# ---------------------------------------------------------------------------
# The cluster drivers, against a stub kubectl and aws
# ---------------------------------------------------------------------------

# Record calls and return fixture responses so driver parsing runs without a cluster. Unmatched calls
# succeed silently.
KUBECTL_STUB = """
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"apply -f -")
	count=$(find "$STUB_APPLIED_DIR" -type f | wc -l | tr -d ' ')
	cat >"$STUB_APPLIED_DIR/$count.yaml"
	;;
*"${STUB_KUBECTL_FAIL_MATCH:-a match that no invocation uses}"*) exit "${STUB_KUBECTL_FAIL_STATUS:-1}" ;;
*"create configmap"*) printf 'apiVersion: v1\\nkind: ConfigMap\\nmetadata:\\n  name: stub\\n' ;;
*"port-forward"*)
	ports="${!#}"
	local_port="${ports%%:*}"
	printf 'Forwarding from 127.0.0.1:%s -> %s\\n' "${local_port:-${STUB_FORWARD_PORT:-43210}}" "${ports##*:}" >&2
	exec /bin/sleep 10
	;;
*"jsonpath={.metadata.name}"*) printf '%s' "${STUB_OBJECT_NAME:-}" ;;
*"get pods -l strimzi.io/cluster="*) printf '%s\\n' "${STUB_BROKERS:-broker-0}" ;;
*"-delete"*) printf '%s\\n' "${STUB_BROKER_SAMPLE:-0 1000}" ;;
*"kafka-topics.sh"*) printf '%s' "${STUB_TOPICS:-}" ;;
*"df -B1"*) printf 'Avail\\n%s\\n' "${STUB_KAFKA_AVAIL:-9000000000}" ;;
*"get pod -l"*) printf '%s\\n' "${STUB_ENGINE_IMAGE:-}" ;;
*"get job/"*) printf 'Complete\\n' ;;
*"logs job/scorer-"*) printf 'POLL t=0.1 prefix=0/0\\n' ;;
# The merge Job's own log, for a driver that reads two Jobs' logs in one run
# and is answered by what each of them wrote rather than by one text twice.
*"logs job/corpus-merge"*) cat "${STUB_MERGE_LOG:-$STUB_JOB_LOG}" ;;
*"logs job/"*) cat "$STUB_JOB_LOG" ;;
# The cluster's shape, for the check a launch makes before it applies a pod
# no node may have room for. Unset answers nothing, which is the cluster a
# driver could not read — and a driver that read none must warn about none.
*"get nodes -o json"*) cat "${STUB_NODES:-/dev/null}" ;;
*"get pods --all-namespaces -o json"*) cat "${STUB_CLUSTER_PODS:-/dev/null}" ;;
*"get pods -l"*) printf '%s\\n' "${STUB_PODS:-}" ;;
# The run object's error field, told apart from its state by the name of the
# field: both engines put their operator's rejection under one named for it.
# Unset answers empty, which is what an operator that rejected nothing reports.
*"jsonpath={.status."*error*) printf '%s\\n' "${STUB_ENGINE_ERROR:-}" ;;
# The lifecycle an operator reports beside that error, for the engine whose
# lifecycle is a field of its own. For the engine whose application state *is*
# its lifecycle the path is the state's, and the arm below answers it.
*"jsonpath={.status.lifecycleState}"*) printf '%s\\n' "${STUB_ENGINE_LIFECYCLE:-}" ;;
# `-` and not `:-`, so a test can name the empty state an operator that
# created no job reports, as against not naming one at all.
*"jsonpath={.status."*) printf '%s\\n' "${STUB_ENGINE_STATE-RUNNING}" ;;
esac
"""

# Simulate sync from fixture directories, listings from fixture text, and cp from STUB_S3_CP_DIR.
# Missing-path variables model absent objects.
AWS_STUB = """
S3_SCHEME="s3://"
printf '%s\\n' "$*" >>"$STUB_AWS_LOG"
if [[ ${1:-} == s3 && ${2:-} == sync && -d ${STUB_STAGE_DIR:-} ]]; then
	mkdir -p "$4"
	cp -R "$STUB_STAGE_DIR/." "$4"
fi
if [[ ${1:-} == s3 && ${2:-} == cp ]]; then
	[[ -z ${STUB_S3_CP_ABSENT:-} || ${3:-} != *"$STUB_S3_CP_ABSENT"* ]] || exit 1
	if [[ $4 == "$S3_SCHEME"* ]]; then
        [[ -f $3 ]] || exit 1
        if [[ -n ${STUB_S3_UPLOAD_DIR:-} ]]; then
            mkdir -p "$STUB_S3_UPLOAD_DIR"
            cp "$3" "$STUB_S3_UPLOAD_DIR/${4##*/}"
        fi
    elif [[ -n ${STUB_S3_CP_DIR:-} && -f "$STUB_S3_CP_DIR/${3##*/}" ]]; then
		cp "$STUB_S3_CP_DIR/${3##*/}" "$4"
	else
		: >"$4"
	fi
fi
if [[ ${1:-} == s3 && ${2:-} == ls ]]; then
	[[ -z ${STUB_S3_LS_ABSENT:-} || ${3:-} != *"$STUB_S3_LS_ABSENT"* ]] || exit 1
	printf '%s\\n' "${STUB_S3_LS:-}"
fi
"""

# The stub `kubectl port-forward` returns at once rather than holding a tunnel
# open, so what stands in for a jobmanager answering through one is `curl`.
CURL_STUB = """
printf '%s\\n' "$*" >>"$STUB_CURL_LOG"
"""

# All engine checks use the same stub: success, drift or unreadable endpoint.
VERIFY_STUB = """
printf '%s\\n' "$*" >>"$STUB_VERIFY_LOG"
# The pod list too, and not only its path: the driver writes it to a temporary
# file and removes it on the way out, so what the check was handed is only
# readable from inside the check.
while [[ $# -gt 0 ]]; do
	case "$1" in
	--pods)
		cat "$2" >>"$STUB_VERIFY_LOG"
		shift 2
		;;
	*) shift ;;
	esac
done
# `STUB_VERIFY_STATUSES` answers the nth call with its nth word and every call
# after that with its last, so "4 4 0" is a fleet that finishes being placed.
# The calls are counted in a file of their own because the log above holds a
# line per pod list as well as one per call.
if [[ -n ${STUB_VERIFY_STATUSES:-} ]]; then
	printf 'x' >>"$STUB_VERIFY_LOG.calls"
	read -r -a statuses <<<"$STUB_VERIFY_STATUSES"
	index=$(($(wc -c <"$STUB_VERIFY_LOG.calls") - 1))
	((index < ${#statuses[@]})) || index=$((${#statuses[@]} - 1))
	exit "${statuses[index]}"
fi
exit "${STUB_VERIFY_STATUS:-0}"
"""

# Resource parsing has dedicated tests; driver tests check capture and upload.
ENGINE_FLEET_STUB = """
pods=""
out=""
status="${STUB_FLEET_STATUS:-0}"
while [[ $# -gt 0 ]]; do
    case "$1" in
    --compaction) status="${STUB_COMPACTOR_FLEET_STATUS:-0}"; shift ;;
    --pods) pods=$2; shift 2 ;;
    --out) out=$2; shift 2 ;;
    *) shift ;;
    esac
done
[[ $status == 0 ]] || exit "$status"
cp "$pods" "$out"
"""

DROP_TABLE_STUB = """
printf '%s\\n' "$*" >>"$STUB_DROP_TABLE_LOG"
"""

RESULTS_TABLE_STUB = """
printf '%s\\n' "$*" >>"$STUB_RESULTS_TABLE_LOG"
"""

# Model the collector command boundary; publication checks use the real collector below.
COLLECT_STUB = """
printf '%s\\n' "$*" >>"$STUB_COLLECT_LOG"
run_dir=""
out=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--run-dir)
		run_dir="$2"
		shift 2
		;;
	--publish)
		out="$2"
		shift 2
		;;
	*) shift ;;
	esac
done
fleet='[{"role":"taskmanager","machine_type":"m6i.xlarge"}]'
printf '{"run": {"engine": "flink", "fleet": %s}}\\n' "${STUB_COLLECT_FLEET-$fleet}" >"$run_dir/run.json"
if [[ -n $out ]]; then
	mkdir -p "${out%/}"
	cp "$run_dir/run.json" "${out%/}/a-published-result.json"
fi
"""

# Exercise the installed collector behind the same command boundary as the
# shell driver while keeping AWS, Kubernetes, and geometry stubbed.
REAL_COLLECT_STUB = f"""
printf '%s\\n' "$*" >>"$STUB_COLLECT_LOG"
cd {shlex.quote(str(REPO_ROOT))}
exec {shlex.quote(sys.executable)} -m ingest_bench.collect.cli "$@"
"""

# `file-sizes` writes the geometry document the verdict block reads its last
# line out of, and answers the exit code a test asks it for.
FILE_SIZES_STUB = """
printf '%s\\n' "$*" >>"$STUB_FILE_SIZES_LOG"
out=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--out)
		out="$2"
		shift 2
		;;
	*) shift ;;
	esac
done
status="${STUB_FILE_SIZES_STATUS:-0}"
if [[ -n $out && $status == 0 ]]; then
	mkdir -p "$out"
	printf '%s\\n' "$STUB_GEOMETRY" >"$out/geometry.json"
fi
exit "$status"
"""

# `compaction` writes the compaction document beside the geometry one, and
# answers the exit code a test asks it for.
COMPACTION_STUB = """
printf '%s\\n' "$*" >>"$STUB_COMPACTION_LOG"
out=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--out)
		out="$2"
		shift 2
		;;
	*) shift ;;
	esac
done
status="${STUB_COMPACTION_STATUS:-0}"
if [[ -n $out && $status == 0 ]]; then
	mkdir -p "$out"
	printf '%s\\n' "$STUB_COMPACTION" >"$out/compaction.json"
fi
exit "$status"
"""

RUN_ID = "smoke-flink-20260908T120000Z"
# What the same run's Kubernetes objects are named, since an RFC 1123 name is
# lowercase and the stamp in a run id is not.
RUN_OBJECT = RUN_ID.lower()
BOOTSTRAP = SITE_AWS_FILLINGS["YOUR_MSK_IAM_BOOTSTRAP"] + ":9098"

# Use an explicit allowed fixture URI so the public-surface scan can inspect it.
CORPUS_ROOT = "s3://a-bucket/corpus"
SHARDED_PRESET = "events-1mrps-skew"
CORPUS_DIR = f"{SHARDED_PRESET}-59c2b4a1"

# Shared shard prefixes retain directories for previously generated presets.
TWO_CORPORA = f"                           PRE {CORPUS_DIR}/\n                           PRE smoke-e13842f9/"


def _stage_job_log(run_id: str) -> str:
    """What the stage Job printed, in the shape `stage` prints it."""
    return (
        f"run_id: {run_id}\n"
        f"bootstrap: {BOOTSTRAP}\n"
        f"topic: {run_id}\n"
        f"table: ingest_bench.t_{run_id.replace('-', '_')}\n"
        f"run_dir: /work/runs/{run_id}\n"
    )


STAGE_JOB_LOG = _stage_job_log(RUN_ID)

FACTS = {
    "run_id": RUN_ID,
    "bootstrap": BOOTSTRAP,
    "topic": RUN_ID,
    "corpus_uri": "s3://a-bucket/corpus/smoke-1a2b3c4d",
    "table": "ingest_bench.t_smoke_flink_20260908T120000Z",
    "key_column": "user_id",
    "epoch": None,
}


def _filled_site() -> str:
    """The shipped AWS example with its placeholders filled, as an operator's own."""
    text = SITE_AWS_EXAMPLE.read_text()
    for placeholder, value in SITE_AWS_FILLINGS.items():
        text = text.replace(placeholder, value)
    return text


def _stub_bin(directory: Path, programs: dict[str, str]) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    for name, body in programs.items():
        stub = directory / name
        stub.write_text(f"#!/usr/bin/env bash\nset -euo pipefail\n{body}")
        stub.chmod(0o755)
    return directory


@dataclass(frozen=True)
class DriverRun:
    """One driver run against the stubs, and everything it left behind."""

    result: subprocess.CompletedProcess[str]
    calls: str
    aws_calls: str
    applied: list[dict[str, object]]


def _run_driver(
    script: Path,
    arguments: list[str],
    tmp_path: Path,
    environment: dict[str, str],
    site: str | None = None,
    programs: dict[str, str] | None = None,
    job_log: str | None = None,
) -> DriverRun:
    """Run a driver in an isolated operator directory with AWS and Kubernetes stubs.
    programs overrides harness commands; job_log supplies the stage Job output.
    """
    work = tmp_path / "work"
    work.mkdir(exist_ok=True)
    (work / "site.yaml").write_text(site if site is not None else _filled_site())
    applied_dir = tmp_path / "applied"
    applied_dir.mkdir(exist_ok=True)
    calls = tmp_path / "kubectl-calls.log"
    calls.touch()
    aws_calls = tmp_path / "aws-calls.log"
    aws_calls.touch()
    job_log_file = tmp_path / "job.log"
    job_log_file.write_text(job_log if job_log is not None else STAGE_JOB_LOG)
    stubs = _stub_bin(
        tmp_path / "bin",
        {"kubectl": KUBECTL_STUB, "aws": AWS_STUB, "engine-fleet": ENGINE_FLEET_STUB, **(programs or {})},
    )

    result = subprocess.run(
        [str(script), *arguments],
        cwd=work,
        capture_output=True,
        text=True,
        # Use closed stdin so deletion prompts behave consistently in CI and local runs.
        stdin=subprocess.DEVNULL,
        env={
            **os.environ,
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "STUB_LOG": str(calls),
            "STUB_AWS_LOG": str(aws_calls),
            "STUB_APPLIED_DIR": str(applied_dir),
            "STUB_JOB_LOG": str(job_log_file),
            # Provenance polling has dedicated tests; other drivers need only one read.
            "ENGINE_IMAGE_WAIT_S": "0",
            **environment,
        },
    )
    documents = [
        _mapping(yaml.safe_load(path.read_text()))
        for path in sorted(applied_dir.glob("*.yaml"), key=lambda path: int(path.stem))
    ]
    return DriverRun(result=result, calls=calls.read_text(), aws_calls=aws_calls.read_text(), applied=documents)


PREFLIGHT_KUBECTL_STUB = """
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"get pods -l"*) printf '%s\\n' "${STUB_BROKERS:-broker-0}" ;;
*"-delete"*)
	if [[ -n ${STUB_BROKER_SAMPLE_COUNT:-} ]]; then
		printf x >>"$STUB_BROKER_SAMPLE_COUNT"
		count=$(wc -c <"$STUB_BROKER_SAMPLE_COUNT")
		if ((count > 1)) && [[ -n ${STUB_BROKER_SAMPLE_DELAY_AFTER_FIRST_S:-} ]]; then
			/bin/sleep "$STUB_BROKER_SAMPLE_DELAY_AFTER_FIRST_S"
		fi
	fi
	printf '%s\\n' "${STUB_BROKER_SAMPLE:-0 1000}"
	;;
*"kafka-topics.sh"*)
	if [[ -n ${STUB_TOPICS_ERROR:-} ]]; then
		printf '%s\\n' "$STUB_TOPICS_ERROR" >&2
		exit 42
	fi
	if [[ -n ${STUB_TOPICS_WARNING:-} ]]; then
		printf '%s\\n' "$STUB_TOPICS_WARNING" >&2
	fi
	printf '%s' "${STUB_TOPICS-}"
	;;
*"df -B1"*) printf 'Avail\\n%s\\n' "${STUB_KAFKA_AVAIL:-9000000000}" ;;
esac
"""


def _run_kafka_preflight(
    tmp_path: Path,
    producer: dict[str, object],
    environment: dict[str, str] | None = None,
    *,
    corpus: str = "smoke",
    script: Path = PREFLIGHT_KAFKA,
) -> DriverRun:
    spec = tmp_path / "spec.yaml"
    # A corpus that names a shipped track stands in for a spec that names that track.
    source = (
        {"track": corpus} if (REPO_ROOT / "workloads" / "tracks" / f"{corpus}.yaml").is_file() else {"corpus": corpus}
    )
    spec.write_text(yaml.safe_dump({**source, "producer": producer}))
    site = SITE_K8S_EXAMPLE.read_text()
    for placeholder, value in SITE_K8S_FILLINGS.items():
        site = site.replace(placeholder, value)
    return _run_driver(
        script,
        [str(spec)],
        tmp_path,
        {"BROKER_QUIET_SAMPLE_S": "1", **(environment or {})},
        site=site,
        programs={"kubectl": PREFLIGHT_KUBECTL_STUB},
    )


@needs_shell_tools
def test_kafka_preflight_fails_when_topics_cannot_be_listed(tmp_path: Path) -> None:
    run = _run_kafka_preflight(tmp_path, {}, {"STUB_TOPICS_ERROR": "broker unavailable"})

    assert run.result.returncode == 1
    assert "broker unavailable" in run.result.stderr
    assert "could not list Kafka topics through broker-0" in run.result.stderr
    assert "brokers are clear" not in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize(
    "topics, expected_status",
    [("", 0), ("__consumer_offsets\n__transaction_state\n", 0), ("__consumer_offsets\nan-old-run\n", 3)],
)
def test_kafka_preflight_ignores_only_internal_topics(tmp_path: Path, topics: str, expected_status: int) -> None:
    run = _run_kafka_preflight(tmp_path, {}, {"STUB_TOPICS": topics})

    assert run.result.returncode == expected_status, run.result.stderr
    if expected_status == 3:
        assert "UNSAFE: leftover topic(s): an-old-run" in run.result.stderr
    else:
        assert "brokers are clear" in run.result.stderr


@needs_shell_tools
def test_kafka_preflight_does_not_treat_a_listing_warning_as_a_topic(tmp_path: Path) -> None:
    run = _run_kafka_preflight(tmp_path, {}, {"STUB_TOPICS_WARNING": "using fallback DNS"})

    assert run.result.returncode == 0, run.result.stderr
    assert "using fallback DNS" in run.result.stderr
    assert "leftover topic" not in run.result.stderr


@needs_shell_tools
def test_kafka_preflight_accepts_a_quiet_sample_that_finishes_after_the_deadline(tmp_path: Path) -> None:
    sample_count = tmp_path / "broker-samples"
    sample_count.touch()
    run = _run_kafka_preflight(
        tmp_path,
        {},
        {
            "WAIT_BROKERS_S": "2",
            "STUB_BROKER_SAMPLE_COUNT": str(sample_count),
            "STUB_BROKER_SAMPLE_DELAY_AFTER_FIRST_S": "2",
        },
    )

    assert run.result.returncode == 0, run.result.stderr
    assert "brokers are quiet: no pending-delete dirs and disk use changed by 0 KiB" in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("speed", [0.5, 1, 2])
def test_kafka_preflight_storage_is_independent_of_replay_speed(tmp_path: Path, speed: float) -> None:
    run = _run_kafka_preflight(
        tmp_path,
        {"speed": speed},
        {"STUB_KAFKA_AVAIL": "1800000000"},
    )

    assert run.result.returncode == 0, run.result.stderr
    assert "1500000000 B of log, 1800000000 B per broker" in run.result.stderr


@needs_shell_tools
def test_kafka_preflight_sizes_a_track_spec_from_the_tracks_corpus(tmp_path: Path) -> None:
    run = _run_kafka_preflight(tmp_path, {}, {"STUB_KAFKA_AVAIL": str(6 * 10**12)}, corpus="events-1mrps-skew")

    assert run.result.returncode == 0, run.result.stderr
    assert f"{1376 * 10**6 * 3600} B of log" in run.result.stderr


@needs_shell_tools
def test_kafka_preflight_replicates_across_every_broker(tmp_path: Path) -> None:
    run = _run_kafka_preflight(
        tmp_path,
        {},
        {"STUB_BROKERS": "broker-0\nbroker-1\nbroker-2", "STUB_KAFKA_AVAIL": "1800000000"},
    )

    # Three brokers hold three replicas, so each carries the whole offer plus the margin.
    assert run.result.returncode == 0, run.result.stderr
    assert "at replication 3 across 3 brokers: 4500000000 B of log, 1800000000 B per broker" in run.result.stderr
    assert run.calls.count("df -B1") == 3


@needs_shell_tools
def test_kafka_preflight_fails_when_free_space_cannot_be_read(tmp_path: Path) -> None:
    run = _run_kafka_preflight(tmp_path, {}, {"STUB_KAFKA_AVAIL": "N/A"})

    assert run.result.returncode == 1
    assert "could not read free space on broker-0" in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("seconds, expected_status", [(None, 3), (100, 0)])
def test_kafka_preflight_storage_respects_probe_seconds(
    tmp_path: Path, seconds: int | None, expected_status: int
) -> None:
    producer: dict[str, object] = {} if seconds is None else {"seconds": seconds}
    run = _run_kafka_preflight(tmp_path, producer, {"STUB_KAFKA_AVAIL": "700000000"})

    assert run.result.returncode == expected_status, run.result.stderr
    expected_peak = 1500000000 if seconds is None else 500000000
    assert f"{expected_peak} B of log" in run.result.stderr


@needs_shell_tools
def test_kafka_preflight_requires_two_stable_samples_without_pending_deletes(tmp_path: Path) -> None:
    quiet = _run_kafka_preflight(tmp_path, {}, {"STUB_BROKER_SAMPLE": "0 1234"})
    assert quiet.result.returncode == 0, quiet.result.stderr
    assert quiet.calls.count("-delete") == 2
    assert "pending-delete dirs 0, used 1234 KiB" in quiet.result.stderr

    deleting_dir = tmp_path / "deleting"
    deleting_dir.mkdir()
    deleting = _run_kafka_preflight(
        deleting_dir,
        {},
        {"STUB_BROKER_SAMPLE": "1 1234", "WAIT_BROKERS_S": "1", "BROKER_QUIET_SAMPLE_S": "1"},
    )
    assert deleting.result.returncode != 0
    assert "did not become quiet within 1s" in deleting.result.stderr


def _job_argv(document: dict[str, object], shard: int = 0) -> list[str]:
    """Apply Kubernetes dollar escaping and execute indexed wrappers with a stub CLI."""
    containers = _sequence(_pod_spec(document)["containers"])
    command = [str(arg).replace("$$", "$") for arg in _sequence(_mapping(containers[0])["command"])]
    if command[0] != "/bin/sh":
        return command
    with tempfile.TemporaryDirectory() as directory:
        stubs = _stub_bin(
            Path(directory),
            {name: f"printf '%s\\0' {name} \"$@\"" for name in ("produce", "gen-corpus", "seed-corpus")},
        )
        out = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=True,
            env={**os.environ, "PATH": f"{stubs}:{os.environ['PATH']}", "JOB_COMPLETION_INDEX": str(shard)},
        )
    return out.stdout.removesuffix("\0").split("\0")


def _job_command(document: dict[str, object]) -> str:
    return " ".join(_job_argv(document))


def _pod_spec(document: dict[str, object]) -> dict[str, object]:
    return _mapping(_mapping(_mapping(document["spec"])["template"])["spec"])


def _named_job(run: DriverRun, name: str) -> dict[str, object]:
    """The one applied document of ``name``, so a driver's Jobs can be told apart."""
    matching = [document for document in run.applied if _mapping(document["metadata"])["name"] == name]
    assert len(matching) == 1, f"expected one {name}, found {len(matching)}"
    return matching[0]


def _sharded_generation(tmp_path: Path, environment: dict[str, str]) -> DriverRun:
    """A two-shard generation whose shard prefixes hold two presets' corpora."""
    # Match the real shard and merge log field positions.
    merge_log = tmp_path / "merge.log"
    merge_log.write_text(f"wrote {CORPUS_ROOT}/{CORPUS_DIR} from 2 shards: 12000000 rows, 600000000 encoded bytes\n")
    return _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--shards", "2", "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_S3_LS": TWO_CORPORA, "STUB_MERGE_LOG": str(merge_log), **environment},
        # Shard 1's, because `kubectl logs job/<indexed job>` answers with one
        # of its pods and a driver may not depend on which: every shard writes
        # a directory of the same name under a prefix of its own.
        job_log=(
            f"wrote {CORPUS_ROOT}/shards/1/{CORPUS_DIR} shard 1 of 2: "
            "6000000 rows, 300000000 encoded bytes, 250000000 stored bytes\n"
        ),
    )


@needs_shell_tools
def test_a_sharded_merge_names_the_corpus_its_generation_wrote(tmp_path: Path) -> None:
    """Shard prefixes can contain several presets. Select the directory reported
    by this generation rather than the first directory listed.
    """
    run = _sharded_generation(tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.strip() == f"{CORPUS_ROOT}/{CORPUS_DIR}"
    command = _job_command(_named_job(run, f"corpus-merge-{SHARDED_PRESET}"))
    assert command == (
        f"merge-corpus {CORPUS_ROOT}/shards/0/{CORPUS_DIR} {CORPUS_ROOT}/shards/1/{CORPUS_DIR} --out {CORPUS_ROOT}"
    )
    # Every shard's copy is read before a merge pod is paid for, and by the
    # document that says the shard finished rather than by its prefix existing.
    for shard in (0, 1):
        assert f"s3 ls {CORPUS_ROOT}/shards/{shard}/{CORPUS_DIR}/corpus.json" in run.aws_calls, run.aws_calls


@needs_shell_tools
def test_a_generation_missing_a_shard_is_refused_before_the_merge(tmp_path: Path) -> None:
    run = _sharded_generation(tmp_path, {"STUB_S3_LS_ABSENT": f"shards/1/{CORPUS_DIR}"})

    assert run.result.returncode != 0
    assert f"{CORPUS_ROOT}/shards/1/{CORPUS_DIR}" in run.result.stderr, run.result.stderr
    assert [document for document in run.applied if _mapping(document["metadata"])["name"] == "corpus-merge"] == []


@needs_shell_tools
def test_stage_reads_the_run_id_off_the_jobs_log_and_then_starts_the_engine(tmp_path: Path) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")

    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()
    spec = REPO_ROOT / "specs/smoke/smoke-flink.yaml"
    run = _run_driver(
        STAGE,
        [str(spec), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.splitlines()[-1] == f"run_id: {RUN_ID}"

    fetched = tmp_path / "work" / "runs" / RUN_ID
    assert json.loads((fetched / "facts.json").read_text())["topic"] == RUN_ID
    assert run.aws_calls.splitlines() == [
        f"s3 sync s3://a-bucket/runs/{RUN_ID}/stage/ ./runs/{RUN_ID}/ --only-show-errors",
        f"s3 cp ./runs/{RUN_ID}/engine-pods.json s3://a-bucket/runs/{RUN_ID}/stage/engine-pods.json --only-show-errors",
    ]

    # Never fall back to the caller's current Kubernetes context or namespace.
    for line in run.calls.splitlines():
        assert line.startswith("--context a-cluster --namespace ingest-bench "), line

    # The spec and the site reach the Job as ConfigMaps under the names its
    # mounts expect, and both are deleted once the run directory is fetched.
    assert f"--from-file smoke-flink.yaml={spec}" in run.calls
    assert "--from-file site.yaml=./site.yaml" in run.calls
    for deleted in ("delete job stage-smoke-flink", "configmap stage-smoke-flink-spec", "stage-smoke-flink-site"):
        assert deleted in run.calls, deleted

    # The engine is started from the documents the run directory carries, the
    # ConfigMap first because the deployment mounts it.
    engine_applies = [line for line in run.calls.splitlines() if " apply -f ./runs/" in line]
    assert [line.rsplit("/", 1)[-1] for line in engine_applies] == [
        "flink-job-configmap.yaml",
        "flinkdeployment.yaml",
    ]
    assert f"get flinkdeployment/{RUN_OBJECT}" in run.calls

    # Verify through the operator's REST Service using an absolute spec path.
    assert f"port-forward svc/{RUN_OBJECT}-rest 18081:8081" in run.calls
    checked = verify_calls.read_text().strip()
    assert Path(checked.split()[1]) == (tmp_path / "work" / "runs" / RUN_ID / "spec.yaml").resolve()
    assert checked.endswith(f"--run-id {RUN_ID} --rest http://localhost:18081")

    jobs = [document for document in run.applied if document["kind"] == "Job"]
    assert len(jobs) == 1, "staging applies one Job"
    command = _job_command(jobs[0])
    assert "stage --spec /runs/smoke-flink.yaml --site /site/site.yaml --runs-dir /work/runs" in command
    assert "--image-tag abc1234" in command
    assert "--upload-prefix s3://a-bucket/runs" in command


@needs_shell_tools
def test_stage_waits_for_quiet_in_cluster_brokers_before_creating_the_stage_job(tmp_path: Path) -> None:
    staged = _staged_engine_run(tmp_path)
    site = SITE_K8S_EXAMPLE.read_text()
    for placeholder, value in SITE_K8S_FILLINGS.items():
        site = site.replace(placeholder, value)
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234", "--wait-brokers-s", "10"],
        tmp_path,
        {
            "BROKER_QUIET_SAMPLE_S": "1",
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(tmp_path / "verify-calls.log"),
        },
        site=site,
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    calls = run.calls.splitlines()
    quiet_sample = next(i for i, call in enumerate(calls) if "-delete" in call)
    stage_apply = next(i for i, call in enumerate(calls) if "apply -f -" in call)
    assert quiet_sample < stage_apply


@needs_shell_tools
def test_stage_keeps_kafka_deployment_optional_for_operator_managed_sites(tmp_path: Path) -> None:
    staged = _staged_engine_run(tmp_path)
    site = yaml.safe_load(_filled_site())
    del site["kafka"]["deployment"]
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(tmp_path / "verify-calls.log"),
        },
        site=yaml.safe_dump(site),
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "kafka-topics.sh" not in run.calls


@needs_shell_tools
@pytest.mark.parametrize(("status", "refusal"), [(3, "setting mismatches above"), (2, "after 3 attempts")])
def test_stage_refuses_a_run_whose_engine_it_could_not_hold_to_the_spec(
    tmp_path: Path, status: int, refusal: str
) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_VERIFY_STATUS": str(status),
            # Shorten retries to keep this failure test fast.
            "ENGINE_POLL_S": "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode != 0
    assert refusal in run.result.stderr, run.result.stderr
    assert f"run_id: {RUN_ID}" not in run.result.stdout
    expected_tries = 1 if status == 3 else 3
    assert len(verify_calls.read_text().splitlines()) == expected_tries


@needs_shell_tools
@pytest.mark.parametrize(
    ("statuses", "succeeds", "tries"),
    [
        # A fleet still being placed: the check says so, is waited for, and
        # passes once the last pod has an image to start from.
        ("4 4 0", True, 3),
        # One that never finishes being placed gets the engine's own running
        # wait rather than the endpoint's tries, so the refusal names the pods.
        ("4", False, 1),
    ],
)
def test_stage_waits_out_a_fleet_that_is_still_being_placed(
    tmp_path: Path, statuses: str, succeeds: bool, tries: int
) -> None:
    """Placement uses the engine running timeout; unreadable endpoints have a
    separate retry limit.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_VERIFY_STATUSES": statuses,
            # Use a nonzero poll interval so the placement timeout advances.
            "ENGINE_POLL_S": "1",
            "ENGINE_RUNNING_WAIT_S": "60" if succeeds else "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )

    assert (run.result.returncode == 0) is succeeds, run.result.stderr
    assert len(verify_calls.read_text().splitlines()) == tries, verify_calls.read_text()
    if succeeds:
        assert run.result.stdout.splitlines()[-1] == f"run_id: {RUN_ID}"
    else:
        assert "were not fully scheduled within 0s" in run.result.stderr, run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("script", [LAUNCH, TEARDOWN])
def test_a_driver_addresses_the_topic_staging_named(tmp_path: Path, script: Path) -> None:
    topic = f"{RUN_ID}-as-staged"
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "topic": topic}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(
        script,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "table-metadata-calls.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )

    assert run.result.returncode == 0, run.result.stderr
    commands = [_job_command(document) for document in run.applied]
    published = [command for command in commands if "--topic" in command]
    assert published, commands
    for command in published:
        assert f"--topic {topic}" in command, command


@needs_bash
@pytest.mark.parametrize(
    "host, answer",
    [
        ("lakekeeper.ingest-bench.svc", "lakekeeper ingest-bench"),
        ("lakekeeper.ingest-bench.svc.cluster.local", "lakekeeper ingest-bench"),
        ("glue.eu-west-1.amazonaws.com", None),
        ("localhost", None),
        ("iceberg-rest", None),
    ],
)
def test_a_service_name_is_told_apart_from_any_other_catalog_host(host: str, answer: str | None) -> None:
    """Recognize Kubernetes Service DNS names rather than guessing from arbitrary hosts."""
    function = f'source "{K8S_LIB}"'
    out = subprocess.run(
        ["bash", "-c", f'{function}\nk8s_service_host "$1"', "_", host], capture_output=True, text=True
    )
    if answer is None:
        assert out.returncode != 0, out.stdout
        assert out.stdout == ""
    else:
        assert out.returncode == 0, out.stderr
        assert out.stdout == answer


def _svc_catalog_site(host: str) -> str:
    """The filled AWS site with its catalog moved inside the cluster."""
    return _filled_site().replace(
        "uri: https://glue.eu-west-1.amazonaws.com/iceberg", f"uri: http://{host}:8181/catalog"
    )


def _staged_for_teardown(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")


@needs_shell_tools
@pytest.mark.parametrize("host", ["lakekeeper.ingest-bench.svc", "lakekeeper.ingest-bench.svc.cluster.local"])
def test_an_in_cluster_catalog_is_reached_through_a_tunnel(tmp_path: Path, host: str) -> None:
    """Tunnel the Service URI for local commands; preserve every other catalog property."""
    _staged_for_teardown(tmp_path)
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(tmp_path / "table-metadata-calls.log"),
            "STUB_METADATA_STATUS": "3",
            "STUB_CURL_LOG": str(tmp_path / "curl.log"),
        },
        site=_svc_catalog_site(host),
        programs={"table-metadata": TABLE_METADATA_STUB, "curl": CURL_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "--namespace ingest-bench port-forward svc/lakekeeper :8181" in run.calls
    # The tunnel is probed at the catalog's health path, not at `/`.
    assert "http://localhost:43210/health" in (tmp_path / "curl.log").read_text()
    flags = (tmp_path / "table-metadata-calls.log").read_text()
    assert "--catalog-prop uri=http://localhost:43210/catalog" in flags
    assert ".svc" not in flags
    assert "--catalog-prop rest.signing-name=glue" in flags, "every other property passes through as written"


@needs_shell_tools
def test_a_catalog_off_the_cluster_is_reached_as_written(tmp_path: Path) -> None:
    _staged_for_teardown(tmp_path)
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "table-metadata-calls.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "port-forward" not in run.calls
    flags = (tmp_path / "table-metadata-calls.log").read_text()
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in flags


@needs_shell_tools
def test_a_purge_asks_an_in_cluster_catalog_through_the_same_tunnel(tmp_path: Path) -> None:
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "3",
            "STUB_CURL_LOG": str(tmp_path / "curl.log"),
            "CATALOG_FORWARD_PORT": "18181",
        },
        site=_svc_catalog_site("lakekeeper.ingest-bench.svc"),
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB, "curl": CURL_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "--namespace ingest-bench port-forward svc/lakekeeper 18181:8181" in run.calls
    assert "--catalog-prop uri=http://localhost:18181/catalog" in (tmp_path / "metadata.log").read_text()


@needs_shell_tools
def test_an_https_service_uri_is_refused_by_name(tmp_path: Path) -> None:
    """The URI rewrite supports plain HTTP only; reject HTTPS rather than downgrading it."""
    _staged_for_teardown(tmp_path)
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "table-metadata-calls.log"), "STUB_METADATA_STATUS": "3"},
        site=_filled_site().replace(
            "uri: https://glue.eu-west-1.amazonaws.com/iceberg", "uri: https://lakekeeper.ingest-bench.svc:8181/catalog"
        ),
        programs={"table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 1, run.result.stdout
    assert "plain HTTP" in run.result.stderr


@needs_shell_tools
def test_finish_reaches_an_in_cluster_catalog_through_a_tunnel_too(tmp_path: Path) -> None:
    """finish.sh reads copied metadata, but the shared property reader still opens
    and cleans up a catalog tunnel.
    """
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID],
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_CURL_LOG": str(tmp_path / "curl.log")},
        site=_svc_catalog_site("lakekeeper.ingest-bench.svc"),
        programs={**FINISH_PROGRAMS, "curl": CURL_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "--namespace ingest-bench port-forward svc/lakekeeper :8181" in run.calls


@needs_shell_tools
def test_a_failed_stage_takes_its_configmaps_with_it(tmp_path: Path) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))

    # A stage Job whose log carries no run id: the driver refuses once the
    # ConfigMaps exist and before it reaches the lines that delete them.
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_STAGE_DIR": str(staged)},
        job_log="the job printed nothing this driver reads\n",
    )

    assert run.result.returncode != 0
    assert "printed no run_id line" in run.result.stderr, run.result.stderr
    for deleted in ("delete configmap stage-smoke-flink-spec", "delete configmap stage-smoke-flink-site"):
        assert deleted in run.calls, run.calls


@needs_shell_tools
def test_launch_passes_the_scorers_read_width_only_when_it_is_set(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"SCORER_READ_WORKERS": "8"})
    assert run.result.returncode == 0, run.result.stderr
    scorer = _job_command(run.applied[0])
    assert "--read-workers 8" in scorer


def _container_resources(document: dict[str, object]) -> dict[str, object]:
    container = _mapping(_sequence(_pod_spec(document)["containers"])[0])
    return _mapping(container["resources"])


@needs_shell_tools
@pytest.mark.parametrize("sized", [False, True])
def test_launch_sizes_the_scorer_pod_from_its_environment(tmp_path: Path, sized: bool) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    env = {"SCORER_CPU": "6", "SCORER_MEMORY": "12Gi"} if sized else {}
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, env)
    assert run.result.returncode == 0, run.result.stderr
    resources = _container_resources(run.applied[0])
    cpu, memory = ("6", "12Gi") if sized else ("2", "4Gi")
    assert _mapping(resources["requests"]) == {"cpu": cpu, "memory": memory}
    # The limit follows the request so a runaway poll cannot take the node's memory.
    assert _mapping(resources["limits"]) == {"memory": memory}
    assert f"{cpu} CPU, {memory}" in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("overridden", [False, True])
def test_launch_places_the_producer_and_the_scorer_from_their_own_selectors(tmp_path: Path, overridden: bool) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")
    site = yaml.safe_load(_filled_site())
    site["kubernetes"]["node_selector"] = {"pool": "site"}

    env = {"PRODUCER_NODE_SELECTOR": '{"pool":"producer"}', "SCORER_NODE_SELECTOR": '{"pool":"scorer"}'}
    run = _run_driver(
        LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, env if overridden else {}, yaml.safe_dump(site)
    )
    assert run.result.returncode == 0, run.result.stderr
    scorer = _pod_spec(_named_job(run, f"scorer-{RUN_OBJECT}"))["nodeSelector"]
    producer = _pod_spec(_named_job(run, f"producer-{RUN_OBJECT}"))["nodeSelector"]
    # Unset, both follow the site's selector.
    assert scorer == {"pool": "scorer" if overridden else "site"}
    assert producer == {"pool": "producer" if overridden else "site"}


@needs_shell_tools
def test_launch_refuses_a_node_selector_that_is_not_a_json_object(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"PRODUCER_NODE_SELECTOR": "pool=producer"})
    assert run.result.returncode == 1
    assert "PRODUCER_NODE_SELECTOR must be a JSON object" in run.result.stderr
    assert run.applied == []


@needs_shell_tools
def test_launch_cleans_partial_epoch_write_without_changing_facts(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    original = json.dumps(FACTS)
    (run_dir / "facts.json").write_text(original)
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    real_jq = shutil.which("jq")
    assert real_jq is not None
    run = _run_driver(
        LAUNCH,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {},
        programs={
            "jq": (f'if [[ $1 == --argjson && $2 == epoch ]]; then echo partial; exit 7; fi\nexec "{real_jq}" "$@"')
        },
    )
    assert run.result.returncode == 7, run.result.stderr
    assert (run_dir / "facts.json").read_text() == original
    assert not list(run_dir.glob("facts.json.*"))


@needs_shell_tools
@pytest.mark.parametrize("lead", [None, 42])
def test_launch_dates_the_epoch_ahead_of_itself_and_records_it(tmp_path: Path, lead: int | None) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["scoring"] = {**spec["scoring"], "exactness": "AT_LEAST_ONCE"}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    before = int(time.time())
    run = _run_driver(
        LAUNCH,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {} if lead is None else {"EPOCH_LEAD_S": str(lead)},
    )
    after = int(time.time())
    assert run.result.returncode == 0, run.result.stderr

    expected_lead = 180 if lead is None else lead
    epoch = int(json.loads((run_dir / "facts.json").read_text())["epoch"])
    assert before + expected_lead <= epoch <= after + expected_lead
    assert (run_dir / "timeline.log").read_text().splitlines()[-1].endswith(f" launched epoch={epoch}")

    # Start scoring before production to record an empty baseline.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [
        f"scorer-{RUN_OBJECT}",
        f"producer-{RUN_OBJECT}",
    ]
    scorer, producer = (_job_command(document) for document in run.applied)

    assert f"--epoch {epoch}" in scorer and f"--epoch {epoch}" in producer
    assert f"--publish-logs s3://a-bucket/runs/{RUN_ID}/producer" in scorer
    assert f"--out /work/scores --upload-prefix s3://a-bucket/runs/{RUN_ID}/scores" in scorer
    assert "--idle-stop-s 600 --publish-shards 1" in scorer
    # Unset, so the scorer's own default stands rather than one this driver restates.
    assert "--read-workers" not in scorer
    # The scorer needs all catalog properties; pods do not receive the site file.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in scorer
    assert "--catalog-prop warehouse=123456789012" in scorer
    # The spec's scoring keys, so the run scored is the run the spec asks for.
    assert "--warmup-s 60" in scorer and "--freshness-bound-s 60" in scorer
    assert "--exactness AT_LEAST_ONCE" in scorer

    assert f"--topic {RUN_ID}" in producer
    assert "--shard 0" in producer and "--shards 1" in producer
    assert "--publish-log /work/publish_log-0.jsonl" in producer
    assert f"--upload-prefix s3://a-bucket/runs/{RUN_ID}" in producer
    assert "--key-column user_id" in producer
    # The MSK IAM properties, the harness's own signing region among them.
    assert "--kafka-prop security.protocol=SASL_SSL" in producer
    assert "--kafka-prop aws.region=eu-west-1" in producer

    assert _mapping(run.applied[1]["spec"])["completions"] == 1


@needs_shell_tools
@pytest.mark.parametrize("nodes", [2, 6], ids=["too-few", "enough"])
def test_launch_says_when_the_cluster_has_no_room_for_its_own_pods(tmp_path: Path, nodes: int) -> None:
    """Warn and continue: Pending pods may trigger an autoscaler to add capacity."""
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["producer"] = {**spec["producer"], "shards": 3}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    # One node already carries a pod of this size and the rest are empty, so
    # `nodes - 1` of them have room for one more.
    (tmp_path / "nodes.json").write_text(json.dumps({"items": [_node(f"node-{index}") for index in range(nodes)]}))
    (tmp_path / "pods.json").write_text(json.dumps({"items": [_pod("node-0", "2")]}))

    run = _run_driver(
        LAUNCH,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_NODES": str(tmp_path / "nodes.json"), "STUB_CLUSTER_PODS": str(tmp_path / "pods.json")},
    )

    assert run.result.returncode == 0, run.result.stderr
    # Either way both Jobs are applied: the count is advice, not a gate.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [
        f"scorer-{RUN_OBJECT}",
        f"producer-{RUN_OBJECT}",
    ]
    if nodes == 2:
        # The scorer plus its three shards, against the nodes with room for one.
        warning = f"{nodes - 1} of this cluster's nodes have 2000m of CPU free, and this run needs 4"
        assert warning in run.result.stderr, run.result.stderr
        assert f"see {AWS_DEPLOY.relative_to(REPO_ROOT).as_posix()}/README.md §Sizing the cluster" in run.result.stderr
    else:
        assert "of CPU free" not in run.result.stderr, run.result.stderr


def test_the_cpu_a_launch_counts_against_is_what_its_pods_request() -> None:
    launch = LAUNCH.read_text()
    default = re.search(r'^PRODUCER_CPU="\$\{PRODUCER_CPU:-(\d+)\}"$', launch, re.M)
    assert default is not None, "launch.sh no longer defaults the producer's CPU request"
    assert re.search(r"^POD_CPU_MILLICORES=\$\(\(PRODUCER_CPU \* 1000\)\)$", launch, re.M), (
        "launch.sh no longer counts room by the producer's request"
    )

    def cpu_request(name: str) -> str:
        template = _mapping(yaml.safe_load((REPO_ROOT / "deploy" / "k8s" / name).read_text()))
        container = _mapping(_sequence(_pod_spec(template)["containers"])[0])
        return str(_mapping(_mapping(container["resources"])["requests"])["cpu"])

    # Both requests are markers; the scorer's SCORER_CPU default matches the producer's.
    assert cpu_request("producer-job.yaml.tmpl") == "__CPU__"
    assert cpu_request("scorer-job.yaml.tmpl") == "__CPU__"
    scorer_default = re.search(r'^SCORER_CPU="\$\{SCORER_CPU:-(\d+)\}"$', launch, re.M)
    assert scorer_default is not None, "launch.sh no longer defaults the scorer's CPU request"
    assert scorer_default.group(1) == default.group(1) == "2"


@needs_shell_tools
@pytest.mark.parametrize("managed_by", ["engine", None])
def test_launch_starts_the_producer_for_a_table_its_engine_will_create(tmp_path: Path, managed_by: str | None) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    if managed_by is not None:
        spec["table"] = {**spec["table"], "managed_by": managed_by}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    scorer = _job_command(_named_job(run, f"scorer-{RUN_OBJECT}"))
    if managed_by is None:
        assert "--table-managed-by" not in scorer, scorer
    else:
        assert f"--table-managed-by {managed_by}" in scorer, scorer
    assert _job_command(_named_job(run, f"producer-{RUN_OBJECT}")).startswith("produce ")


@needs_shell_tools
def test_a_batch_sized_pod_can_be_given_the_memory_its_preset_needs(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    launched = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"PRODUCER_MEMORY": "8Gi"})
    assert launched.result.returncode == 0, launched.result.stderr
    producer = _named_job(launched, f"producer-{RUN_OBJECT}")
    requests = _mapping(_mapping(_mapping(_sequence(_pod_spec(producer)["containers"])[0])["resources"])["requests"])
    assert requests["memory"] == "8Gi"

    generated = _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--image-tag", "abc1234"],
        tmp_path,
        {"GEN_MEMORY": "6Gi"},
        job_log=f"wrote {CORPUS_ROOT}/{CORPUS_DIR}: 12000000 rows, 600000000 encoded bytes\n",
    )
    assert generated.result.returncode == 0, generated.result.stderr
    generator = _named_job(generated, f"corpus-gen-{SHARDED_PRESET}")
    requests = _mapping(_mapping(_mapping(_sequence(_pod_spec(generator)["containers"])[0])["resources"])["requests"])
    assert requests["memory"] == "6Gi"


@needs_shell_tools
@pytest.mark.parametrize(
    "preset",
    ["events-1mrps-skew", "workloads/presets/events-1mrps-skew.yaml", "events200-1mrps-skew"],
)
def test_standard_corpus_generation_uses_memory_headroom_without_extrapolating_narrow_schema_timing(
    tmp_path: Path, preset: str
) -> None:
    generated = _run_driver(
        GEN_CORPUS,
        [preset, "--image-tag", "abc1234"],
        tmp_path,
        {"GEN_WAIT_S": "14400"},
        job_log=f"wrote {CORPUS_ROOT}/events-1mrps-skew-59c2b4a1: 12000000 rows, 600000000 encoded bytes\n",
    )

    assert generated.result.returncode == 0, generated.result.stderr
    generator = next(document for document in generated.applied if document["kind"] == "Job")
    requests = _mapping(_mapping(_mapping(_sequence(_pod_spec(generator)["containers"])[0])["resources"])["requests"])
    assert requests["memory"] == "10Gi"
    assert "generation time is not estimated for this preset" in generated.result.stderr
    assert "projected generation time" not in generated.result.stderr


UPSERT_CORPUS = f"{CORPUS_ROOT}/smoke-upsert-efde9848"


@needs_shell_tools
def test_an_upsert_preset_builds_and_commits_its_seed_after_generation(tmp_path: Path) -> None:
    merge_log = tmp_path / "merge.log"
    merge_log.write_text(f"wrote {UPSERT_CORPUS}: 1500000 rows, 384000000 encoded bytes\n")
    run = _run_driver(
        GEN_CORPUS,
        ["smoke-upsert", "--shards", "3", "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_S3_LS_ABSENT": "seed.json", "STUB_MERGE_LOG": str(merge_log)},
        job_log=f"wrote {CORPUS_ROOT}/shards/0/smoke-upsert-efde9848: 500000 rows, 128000000 encoded bytes\n",
    )
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout == f"{UPSERT_CORPUS}\n"
    names = [str(_mapping(document["metadata"])["name"]) for document in run.applied]
    assert names[-2:] == ["corpus-seed-smoke-upsert", "corpus-seed-commit-smoke-upsert"], names
    seeding = _named_job(run, "corpus-seed-smoke-upsert")
    assert _mapping(seeding["spec"])["completions"] == 3
    assert _job_argv(seeding, shard=2) == [
        "seed-corpus",
        "--corpus",
        UPSERT_CORPUS,
        "--shard-index",
        "2",
        "--shard-count",
        "3",
    ]
    assert _job_command(_named_job(run, "corpus-seed-commit-smoke-upsert")) == f"commit-seed --corpus {UPSERT_CORPUS}"


@needs_shell_tools
def test_a_corpus_that_published_its_seed_is_not_seeded_again(tmp_path: Path) -> None:
    run = _run_driver(
        GEN_CORPUS,
        ["--seed-only", UPSERT_CORPUS, "--image-tag", "abc1234"],
        tmp_path,
        {},
        job_log="",
    )
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout == f"{UPSERT_CORPUS}\n"
    assert run.applied == [] and "already published its seed" in run.result.stderr


@needs_shell_tools
def test_seed_only_seeds_an_existing_corpus_without_generating(tmp_path: Path) -> None:
    run = _run_driver(
        GEN_CORPUS,
        ["--seed-only", UPSERT_CORPUS, "--seed-shards", "8", "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_S3_LS_ABSENT": "seed.json"},
        job_log="",
    )
    assert run.result.returncode == 0, run.result.stderr
    names = [str(_mapping(document["metadata"])["name"]) for document in run.applied]
    assert names == ["corpus-seed-smoke-upsert-efde9848", "corpus-seed-commit-smoke-upsert-efde9848"], names
    assert _mapping(run.applied[0]["spec"])["completions"] == 8


@needs_shell_tools
def test_an_append_preset_builds_no_seed(tmp_path: Path) -> None:
    run = _run_driver(
        GEN_CORPUS,
        ["smoke", "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_S3_LS_ABSENT": "seed.json"},
        job_log=f"wrote {CORPUS_ROOT}/smoke-12345678: 1000 rows, 600000 encoded bytes\n",
    )
    assert run.result.returncode == 0, run.result.stderr
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == ["corpus-gen-smoke"]


@needs_shell_tools
def test_two_generations_of_different_presets_are_two_jobs(tmp_path: Path) -> None:
    run = _run_driver(
        GEN_CORPUS,
        [SHARDED_PRESET, "--image-tag", "abc1234"],
        tmp_path,
        {},
        job_log=f"wrote {CORPUS_ROOT}/{CORPUS_DIR}: 12000000 rows, 600000000 encoded bytes\n",
    )
    assert run.result.returncode == 0, run.result.stderr
    name = str(_mapping(run.applied[0]["metadata"])["name"])
    assert name == f"corpus-gen-{SHARDED_PRESET}", name
    assert f"delete job corpus-gen-{SHARDED_PRESET}" in run.calls, run.calls


@needs_shell_tools
@pytest.mark.parametrize("assignment", [None, "key", "round_robin"])
@pytest.mark.parametrize("exactness", ["EXACTLY_ONCE", "AT_LEAST_ONCE"])
def test_launch_passes_staged_partition_assignment(tmp_path: Path, assignment: str | None, exactness: str) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    facts: dict[str, object] = {**FACTS, "key_column": "partition_key"}
    if assignment is not None:
        facts.update(partition_assignment=assignment, kafka_partitions=8)
    (run_dir / "facts.json").write_text(json.dumps(facts))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["scoring"]["exactness"] = exactness
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})
    assert run.result.returncode == 0, run.result.stderr
    assert f"--exactness {exactness}" in _job_command(run.applied[0])
    producer = _job_command(run.applied[1])
    assert "--key-column partition_key" in producer
    if assignment is None:
        assert "--partition-assignment" not in producer and "--partitions" not in producer
    else:
        assert f"--partition-assignment {assignment}" in producer and "--partitions 8" in producer


@needs_shell_tools
@pytest.mark.parametrize("delivery", [None, "kafka"])
def test_launch_seeds_through_kafka_before_the_scored_offer(tmp_path: Path, delivery: str | None) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    upsert: dict[str, object] = {
        "key_columns": ["event_id", "partition_key"],
        "version_column": "id",
        "seed_rows": 3000,
    }
    upsert.update({"seed_delivery": delivery} if delivery else {"seed_snapshot_id": 7})
    facts = {
        **FACTS,
        "key_column": "event_id",
        "partition_assignment": "corpus",
        "kafka_partitions": 16,
        "upsert": upsert,
    }
    (run_dir / "facts.json").write_text(json.dumps(facts))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-upsert-flink.yaml").read_text())
    spec["producer"]["behind_max_ms"] = 10000
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"SEED_WAIT_S": "900"})

    assert run.result.returncode == 0, run.result.stderr
    names = [str(_mapping(document["metadata"])["name"]) for document in run.applied]
    if delivery is None:
        assert names == [f"scorer-{RUN_OBJECT}", f"producer-{RUN_OBJECT}"]
        return
    assert names == [f"seed-{RUN_OBJECT}", f"seed-wait-{RUN_OBJECT}", f"scorer-{RUN_OBJECT}", f"producer-{RUN_OBJECT}"]
    seeder = _job_command(run.applied[0])
    assert "--seed" in seeder and f"--upload-prefix s3://a-bucket/runs/{RUN_ID}/seed" in seeder
    assert "--partition-assignment corpus" in seeder and "--seconds" not in seeder
    barrier = _job_command(run.applied[1])
    assert barrier.startswith(f"wait-seed --table {FACTS['table']} --corpus {FACTS['corpus_uri']}")
    assert f"--publish-logs s3://a-bucket/runs/{RUN_ID}/seed/producer" in barrier
    assert f"--out s3://a-bucket/runs/{RUN_ID}/scores/seed_freshness.json --timeout-s 900" in barrier
    assert "--freshness-bound-s 60 --warmup-s 60" in barrier and "--catalog-prop" in barrier
    assert "--behind-max-ms 10000" in barrier
    assert "--seed" not in _job_command(run.applied[3]).split()
    # The scored epoch is set only once the seed is in the table.
    seed_epoch = int(seeder.split("--epoch ")[1].split()[0])
    timeline = (run_dir / "timeline.log").read_text()
    assert "seeded rows=3000" in timeline
    assert json.loads((run_dir / "facts.json").read_text())["epoch"] >= seed_epoch


@needs_shell_tools
def test_launch_offers_the_codec_the_spec_names(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["producer"] = {**spec["producer"], "compression": "lz4"}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})

    assert run.result.returncode == 0, run.result.stderr
    producer = _job_command(run.applied[1])
    assert "--compression lz4" in producer


@needs_shell_tools
def test_the_jobs_a_launch_applies_name_a_credential_and_never_hold_one(tmp_path: Path) -> None:
    reference = "${env:IB_KAFKA_PASSWORD}"
    site = (
        _filled_site()
        .replace("    aws.region:", f"    sasl.password: '{reference}'\n    aws.region:")
        .replace("  aws_region:", "  secret_name: bench-env\n  aws_region:")
    )
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {}, site=site)

    assert run.result.returncode == 0, run.result.stderr
    for document in run.applied:
        container = _mapping(_sequence(_pod_spec(document)["containers"])[0])
        assert container["envFrom"] == [{"secretRef": {"name": "bench-env"}}]
    producer = _job_command(_named_job(run, f"producer-{RUN_OBJECT}"))
    assert f"--kafka-prop sasl.password={reference}" in producer
    # Resolve credentials only inside the pod process.
    assert "IB_KAFKA_PASSWORD}" in producer and "sasl.password=$" in producer


@needs_shell_tools
def test_launch_refuses_to_start_the_producer_once_its_lead_has_expired(tmp_path: Path) -> None:
    """The scorer wait can consume EPOCH_LEAD_S. Starting after that would make
    the producer late before its first connection.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")

    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {"EPOCH_LEAD_S": "10"})

    assert run.result.returncode != 0
    assert "EPOCH_LEAD_S" in run.result.stderr
    # The scorer is applied before the lead is checked; only the producer —
    # the one that would be minutes late — must not start.
    assert [str(_mapping(document["metadata"])["name"]) for document in run.applied] == [f"scorer-{RUN_OBJECT}"]


# `table-metadata` as the driver calls it, answering whatever the case under
# test needs: a location, the absent-table code, or a failure of its own.
TABLE_METADATA_STUB = """
printf '%s\\n' "$*" >>"$STUB_METADATA_LOG"
if [[ -n ${STUB_METADATA_READ_FINISHED:-} ]]; then
    jq -n now >"$STUB_METADATA_READ_FINISHED"
fi
[[ -z ${STUB_METADATA_ERROR:-} ]] || printf '%s\\n' "$STUB_METADATA_ERROR" >&2
[[ -z ${STUB_METADATA_OUT:-} ]] || printf '%s\\n' "$STUB_METADATA_OUT"
exit "${STUB_METADATA_STATUS:-0}"
"""


@needs_shell_tools
def test_launch_refuses_a_site_whose_properties_it_cannot_read(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())

    # A nested map under a properties block: `yq` refuses to concatenate it into
    # `key=value` and exits non-zero.
    broken = _filled_site().replace(
        "    rest.signing-name: glue\n",
        "    rest.signing-name: glue\n    nested:\n      deeper: value\n",
    )
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {}, site=broken)

    assert run.result.returncode != 0
    assert "could not read catalog.props from ./site.yaml" in run.result.stderr
    assert run.applied == [], "nothing may be applied once the site cannot be read"


@needs_shell_tools
@pytest.mark.parametrize(
    "status, copied, refused",
    [
        (0, True, False),
        # The absent-table code: a run that failed before it created its table
        # has no document, and a teardown converges over that.
        (3, False, False),
        # Any other failure is a catalog this machine could not reach, which
        # says nothing about whether the document exists.
        (1, False, True),
    ],
)
def test_teardown_copies_a_metadata_document_or_says_why_it_could_not(
    tmp_path: Path, status: int, copied: bool, refused: bool
) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")
    (run_dir / "flink-job-configmap.yaml").write_text("# flink-job-configmap.yaml\n")
    metadata_calls = tmp_path / "table-metadata-calls.log"
    metadata_calls.touch()

    location = "s3://a-bucket/warehouse/ingest_bench/t_x/metadata/00003-abc.metadata.json"
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(metadata_calls),
            "STUB_METADATA_STATUS": str(status),
            "STUB_METADATA_OUT": location if status == 0 else "",
            "STUB_METADATA_ERROR": "" if status == 0 else "could not reach the catalog",
        },
        programs={"table-metadata": TABLE_METADATA_STUB},
    )

    # Resource teardown must still run if metadata lookup fails.
    assert f"delete -f ./runs/{RUN_ID}/flinkdeployment.yaml" in run.calls
    assert f"delete job producer-{RUN_OBJECT}" in run.calls and f"delete job scorer-{RUN_OBJECT}" in run.calls
    # The seed producer may still be publishing, so its pods must be gone before the topic is dropped.
    assert f"delete job seed-{RUN_OBJECT} --ignore-not-found --cascade=foreground --wait=true" in run.calls
    assert f"delete job seed-wait-{RUN_OBJECT}" in run.calls
    drop = [document for document in run.applied if document["kind"] == "Job"]
    assert len(drop) == 1
    command = _job_command(drop[0])
    assert f"drop-topic --bootstrap {BOOTSTRAP} --topic {RUN_ID}" in command
    assert "--kafka-prop security.protocol=SASL_SSL" in command

    # The catalog is addressed with every property the site declares.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in metadata_calls.read_text()

    assert (f"s3 cp {location}" in run.aws_calls) is copied
    assert (run_dir / "writers-stopped").exists() is copied
    assert (run.result.returncode != 0) is refused
    if refused:
        assert "table-metadata exited 1" in run.result.stderr
        assert "could not reach the catalog" in run.result.stderr


@needs_shell_tools
def test_failed_metadata_refresh_cannot_reuse_writer_stop_evidence(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    (run_dir / "writers-stopped").touch()
    (run_dir / "table-metadata.observed-at").write_text("123")
    location = f"{TABLE_LOCATION}/metadata/new.json"
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            **_stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
            "STUB_METADATA_OUT": location,
            "STUB_S3_CP_ABSENT": "metadata/new.json",
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert "could not copy" in run.result.stderr
    assert not (run_dir / "writers-stopped").exists()
    assert not (run_dir / "table-metadata.observed-at").exists()


# `gate` as the driver calls it, answering the verdict a case asks for. The
# artifacts it would read are the scorer's, and the stub reads none of them.
GATE_STUB = """
printf '%s\\n' "$*" >>"$STUB_GATE_LOG"
exit "${STUB_GATE_STATUS:-0}"
"""


@needs_shell_tools
def test_a_teardown_that_failed_does_not_replace_the_verdict(tmp_path: Path) -> None:
    """The gate retains its documented verdict exit code even if teardown fails."""
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()

    run = _run_driver(
        GATE,
        # `--breaches 1` so this one tick reaches the teardown: how many ticks
        # it waits for is the case beside this one.
        [RUN_ID, "--teardown", "--breaches", "1"],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_GATE_STATUS": "3"},
        programs={"gate": GATE_STUB},
    )

    assert run.result.returncode == 3, run.result.stdout + run.result.stderr
    assert "so its fleet may still be running" in run.result.stderr, run.result.stderr
    # Fetch only the summary, samples and spec needed for gating.
    assert [line.split("/")[-1] for line in run.aws_calls.splitlines()] == [
        "summary.json --only-show-errors",
        "keepup_samples.jsonl --only-show-errors",
        "spec.yaml --only-show-errors",
    ]


@needs_shell_tools
def test_the_gate_reads_the_runs_own_windows_out_of_the_bucket(tmp_path: Path) -> None:
    """Read the published spec so gating from another machine uses the same windows."""
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    published = tmp_path / "published"
    published.mkdir()
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["scoring"] = {**spec["scoring"], "gate_adaptation_s": 300, "gate_window_s": 90}
    (published / "spec.yaml").write_text(yaml.safe_dump(spec))

    run = _run_driver(
        GATE,
        [RUN_ID],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_S3_CP_DIR": str(published)},
        programs={"gate": GATE_STUB},
    )

    assert run.result.returncode == 0, run.result.stdout + run.result.stderr
    assert "--adaptation-s 300" in gate_calls.read_text(), gate_calls.read_text()
    assert "--window-s 90" in gate_calls.read_text(), gate_calls.read_text()
    fetched = [line.split("/")[-1] for line in run.aws_calls.splitlines()]
    assert fetched == [
        "summary.json --only-show-errors",
        "keepup_samples.jsonl --only-show-errors",
        "spec.yaml --only-show-errors",
    ], run.aws_calls


@needs_shell_tools
def test_a_spec_the_gate_could_not_read_is_a_refusal_and_not_a_default(tmp_path: Path) -> None:
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    run = _run_driver(
        GATE,
        [RUN_ID],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_S3_CP_ABSENT": "stage/spec.yaml"},
        programs={"gate": GATE_STUB},
    )
    assert run.result.returncode != 0
    assert "stage/spec.yaml" in run.result.stderr, run.result.stderr
    assert gate_calls.read_text() == "", "the gate was asked for a verdict with no windows to judge by"


def _gated(tmp_path: Path, arguments: list[str], status: str, gate_calls: Path) -> subprocess.CompletedProcess[str]:
    """One `gate.sh` tick, in a working directory the ticks share."""
    run = _run_driver(
        GATE,
        [RUN_ID, *arguments],
        tmp_path,
        {"STUB_GATE_LOG": str(gate_calls), "STUB_GATE_STATUS": status},
        programs={"gate": GATE_STUB},
    )
    return run.result


@needs_shell_tools
@pytest.mark.parametrize("required", [None, 1])
def test_a_teardown_waits_for_the_verdict_to_repeat(tmp_path: Path, required: int | None) -> None:
    """Require consecutive breaches to avoid teardown after one transient lag spike."""
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    arguments = ["--teardown"] if required is None else ["--teardown", "--breaches", str(required)]
    ticks = 3 if required is None else 1

    for number in range(1, ticks):
        early = _gated(tmp_path, arguments, "3", gate_calls)
        assert early.returncode == 3, early.stdout + early.stderr
        assert f"breach {number} of {ticks}" in early.stderr, early.stderr
        assert "tearing" not in early.stderr, early.stderr

    acted = _gated(tmp_path, arguments, "3", gate_calls)
    assert acted.returncode == 3, acted.stdout + acted.stderr
    assert f"not been PASS for {ticks} consecutive" in acted.stderr, acted.stderr
    assert "so its fleet may still be running" in acted.stderr, acted.stderr


@needs_shell_tools
def test_a_pass_forgets_the_breaches_before_it(tmp_path: Path) -> None:
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()

    breached = _gated(tmp_path, ["--teardown"], "3", gate_calls)
    assert breached.returncode == 3 and "breach 1 of 3" in breached.stderr, breached.stderr
    passed = _gated(tmp_path, ["--teardown"], "0", gate_calls)
    assert passed.returncode == 0, passed.stdout + passed.stderr
    again = _gated(tmp_path, ["--teardown"], "3", gate_calls)
    assert "breach 1 of 3" in again.stderr, again.stderr


@needs_shell_tools
def test_a_gate_without_teardown_judges_and_leaves_the_fleet_alone(tmp_path: Path) -> None:
    gate_calls = tmp_path / "gate-calls.log"
    gate_calls.touch()
    for status in ("0", "3", "5"):
        judged = _gated(tmp_path, [], status, gate_calls)
        assert judged.returncode == int(status), judged.stdout + judged.stderr
        assert "tearing" not in judged.stderr, judged.stderr


@needs_shell_tools
def test_a_runs_kubernetes_objects_are_addressed_in_lower_case(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "metadata.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    for deleted in ("producer", "scorer", "drop-topic"):
        assert f"delete job {deleted}-{RUN_OBJECT}" in run.calls, deleted
        assert f"delete job {deleted}-{RUN_ID}" not in run.calls, deleted
    drop = [document for document in run.applied if document["kind"] == "Job"]
    assert [str(_mapping(document["metadata"])["name"]) for document in drop] == [f"drop-topic-{RUN_OBJECT}"]

    # The topic and the run directory are the run's published identifier, and
    # neither is a name Kubernetes reads.
    assert f"--topic {RUN_ID}" in _job_command(drop[0])
    assert f"delete -f ./runs/{RUN_ID}/flinkdeployment.yaml" in run.calls


@needs_shell_tools
@pytest.mark.parametrize("engine", ["flink", "spark"])
@pytest.mark.parametrize("fleet_status", [0, 2])
def test_stage_addresses_an_engine_by_the_names_its_own_module_declares(
    tmp_path: Path, engine: str, fleet_status: int
) -> None:
    descriptor = engines.kubernetes_for(engine)
    run_id = f"smoke-{engine}-20260908T120000Z"
    run_object = run_id.lower()
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps({**FACTS, "run_id": run_id, "topic": run_id}))
    for name in ("spec.yaml", descriptor.document_file, descriptor.configmap_file):
        (staged / name).write_text(f"# {name}\n")
    verify_calls = tmp_path / "verify-calls.log"
    verify_calls.touch()

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs" / "smoke" / f"smoke-{engine}.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_FLEET_STATUS": str(fleet_status),
            "STUB_CURL_LOG": str(tmp_path / "curl-calls.log"),
            "STUB_VERIFY_LOG": str(verify_calls),
            "STUB_PODS": json.dumps({"items": [{"metadata": {"name": f"{run_object}-driver"}}]}),
            "STUB_S3_UPLOAD_DIR": str(tmp_path / "uploaded"),
        },
        programs={"curl": CURL_STUB, f"verify-{engine}": VERIFY_STUB},
        job_log=_stage_job_log(run_id),
    )
    if fleet_status:
        assert run.result.returncode == fleet_status, run.result.stderr
        assert "stage/engine-pods.json" not in run.aws_calls
        assert f"run_id: {run_id}" not in run.result.stdout
        return
    assert run.result.returncode == 0, run.result.stderr
    assert run.result.stdout.splitlines()[-1] == f"run_id: {run_id}"

    # Both documents the engine's own renderer wrote, the ConfigMap first
    # because the other one mounts it.
    applied = [line.rsplit("/", 1)[-1] for line in run.calls.splitlines() if " apply -f ./runs/" in line]
    assert applied == [descriptor.configmap_file, descriptor.document_file]

    assert f"get {descriptor.kind}/{run_object} -o jsonpath={descriptor.state_jsonpath}" in run.calls
    assert f"port-forward svc/{run_object}{descriptor.rest_service_suffix} 18081:{descriptor.rest_port}" in run.calls
    assert f"get pod -l {for_name(descriptor.provenance_selector, run_object)}" in run.calls

    checked = verify_calls.read_text()
    assert f"--run-id {run_id} --rest http://localhost:18081" in checked
    if descriptor.pods_selector:
        assert f"get pods -l {for_name(descriptor.pods_selector, run_object)} -o json" in run.calls
        # Echo pod data before the temporary file is removed to prove verification received it.
        assert "--pods " in checked
        assert json.loads(checked.splitlines()[1])["items"][0]["metadata"]["name"] == f"{run_object}-driver"
    else:
        assert "--pods" not in checked
    assert f"get pods -l {for_name(descriptor.fleet_selector, run_object)} -o json" in run.calls
    captured = tmp_path / "work" / "runs" / run_id / "engine-pods.json"
    assert json.loads(captured.read_text())["items"][0]["metadata"]["name"] == f"{run_object}-driver"
    assert (tmp_path / "uploaded" / "engine-pods.json").read_bytes() == captured.read_bytes()


def test_the_shell_reads_every_field_the_descriptor_prints() -> None:
    text = K8S_LIB.read_text()
    for field in FIELDS:
        assert f"\n\t\t{field}) ENGINE_" in text, f"scripts/_k8s.sh reads no {field}"


def test_the_shell_and_the_descriptor_mark_a_run_s_name_the_same_way() -> None:
    assert f"ENGINE_NAME_MARKER='{NAME}'" in (SCRIPTS / "_k8s.sh").read_text()


@needs_shell_tools
def test_an_engine_that_failed_is_tailed_under_its_lower_case_name(tmp_path: Path) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {"STUB_STAGE_DIR": str(staged), "STUB_ENGINE_STATE": "FAILED"},
    )
    assert run.result.returncode != 0
    assert f"flinkdeployment/{RUN_OBJECT} went to FAILED" in run.result.stderr
    assert f"logs deploy/{RUN_OBJECT} --tail=40" in run.calls


@needs_shell_tools
@pytest.mark.parametrize("pods", [False, True])
def test_a_document_the_operator_rejected_ends_the_wait_at_once(tmp_path: Path, pods: bool) -> None:
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    rejection = "the length must be no more than 45 characters"

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            # A rejected deployment has no job state; it must fail before the running timeout.
            "STUB_ENGINE_STATE": "",
            "STUB_ENGINE_ERROR": rejection,
            # The operator has given up on the document, which is what makes
            # the error a refusal rather than a step on the way.
            "STUB_ENGINE_LIFECYCLE": "FAILED",
            "ENGINE_RUNNING_WAIT_S": "30",
            "ENGINE_POLL_S": "1",
            **({"STUB_ENGINE_IMAGE": f"a-registry/flink:abc1234 sha256:{'a' * 64}"} if pods else {}),
        },
    )
    assert run.result.returncode != 0
    assert rejection in run.result.stderr
    assert "failed to start" in run.result.stderr and "FAILED" in run.result.stderr
    assert "did not reach" not in run.result.stderr, "the error is the refusal, not the timeout"
    assert f"get flinkdeployment/{RUN_OBJECT} -o jsonpath={{.status.error}}" in run.calls
    assert f"get flinkdeployment/{RUN_OBJECT} -o jsonpath={{.status.lifecycleState}}" in run.calls

    tailed = f"logs deploy/{RUN_OBJECT} --tail=40" in run.calls
    assert tailed is pods, "the log is read exactly when there are pods to have written one"


@needs_shell_tools
def test_an_error_the_operator_has_not_given_up_over_does_not_end_the_wait(tmp_path: Path) -> None:
    """An error can be retryable. Use lifecycle state to distinguish reconciliation
    from terminal rejection.
    """
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    transient = "could not get the deployment, retrying"

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_STAGE_DIR": str(staged),
            "STUB_ENGINE_STATE": "",
            "STUB_ENGINE_ERROR": transient,
            # Still being reconciled, which is every lifecycle but the failed one.
            "STUB_ENGINE_LIFECYCLE": "DEPLOYED",
            "ENGINE_RUNNING_WAIT_S": "4",
            "ENGINE_POLL_S": "2",
        },
    )
    assert run.result.returncode != 0
    # The wait is what ends it, and the error it saw is named in that refusal
    # rather than being the refusal.
    assert "did not reach" in run.result.stderr
    assert f"last error: {transient}" in run.result.stderr
    assert "failed to start" not in run.result.stderr
    # Reported when it appeared and not once per poll.
    assert run.result.stderr.count("has not reported a terminal failure") == 1


# ---------------------------------------------------------------------------
# Geometry, collection and reclamation
# ---------------------------------------------------------------------------

# Read the table location from metadata and require it to be below the warehouse root.
WAREHOUSE_ROOT = "s3://a-bucket/warehouse"
TABLE_LOCATION = f"{WAREHOUSE_ROOT}/ingest_bench/t_smoke_flink_20260908T120000Z-1a2b"

# A geometry document shaped like `file-sizes` writes one, with only the fields
# the verdict's last line reads.
GEOMETRY_DOCUMENT = json.dumps(
    {
        "final": {
            "live": {
                "files": 137,
                "rows": 1000,
                "bytes": 5_000_000_000,
                "size_quantiles": {"p50": 40_100_000.0},
                "small_file_share_32mib": 0.2847,
            }
        }
    }
)

# A compaction document shaped like the command writes one, with the fields a
# reader checks first.
COMPACTION_DOCUMENT = json.dumps(
    {"tier": "lifecycle", "files": 137, "floor": 96, "floor_ratio": 1.4271, "records_dropped": None}
)


def _torn_down_run(
    tmp_path: Path, *, run_valid: bool = True, metadata: bool = True, location: str = TABLE_LOCATION
) -> Path:
    """A run directory as `teardown.sh` leaves one, in the operator's working directory."""
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    (run_dir / "scores").mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "epoch": 1757419200}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "scores" / "summary.json").write_text(
        json.dumps(
            {
                "run_valid": run_valid,
                "state": "drained" if run_valid else "idle_stop",
                "reason": "",
                "corpus_hash": "e13842f9",
                "corpus_uri": "s3://a-bucket/corpus/smoke-e13842f9",
                "freshness_bound_s": 60.0,
                "producer_bound": False,
                "speed": 1.0,
                "prefix": 10,
                "last_batch": 10,
                "committed_rows": 100,
                "offered_rows": 100,
                "freshness": {"window": {"p95": 3.0}},
                "exactness": {"exact": run_valid, "loss_rows": 0, "duplicate_rows": 0},
                "keepup": {"drain_s": 1.0},
            }
        )
    )
    if metadata:
        (run_dir / "table-metadata.final.json").write_text(json.dumps({"location": location}))
    return run_dir


def _stub_logs(tmp_path: Path, names: dict[str, str]) -> dict[str, str]:
    """Log paths for the stubs, created empty."""
    environment = {}
    for variable, name in names.items():
        path = tmp_path / name
        path.touch()
        environment[variable] = str(path)
    return environment


def _finish_environment(tmp_path: Path) -> dict[str, str]:
    return {
        **_stub_logs(
            tmp_path,
            {
                "STUB_COLLECT_LOG": "collect.log",
                "STUB_FILE_SIZES_LOG": "file-sizes.log",
                "STUB_COMPACTION_LOG": "compaction.log",
                "STUB_RESULTS_TABLE_LOG": "results-table.log",
            },
        ),
        "STUB_GEOMETRY": GEOMETRY_DOCUMENT,
        "STUB_COMPACTION": COMPACTION_DOCUMENT,
        # The fixture runs use a smoke corpus, which only the fixture tracks cover.
        "INGEST_BENCH_WORKLOADS": str(fixture_workloads(tmp_path)),
    }


FINISH_PROGRAMS = {
    "collect": COLLECT_STUB,
    "file-sizes": FILE_SIZES_STUB,
    "compaction": COMPACTION_STUB,
    "results-table": RESULTS_TABLE_STUB,
}


VERIFY_TABLE_STUB = """
printf '%s\\n' "$*" >>"$STUB_TERMINAL_LOG"
out=""
while [[ $# -gt 0 ]]; do
    case "$1" in
    --out) out=$2; shift 2 ;;
    *) shift ;;
    esac
done
status=${STUB_TERMINAL_STATUS:-0}
verdict=false
[[ $status != 0 ]] || verdict=true
printf '{"verdict":%s,"snapshot_id":17}\\n' "$verdict" >"$out/terminal-exactness.json"
exit "$status"
"""


@needs_shell_tools
@pytest.mark.parametrize("status", [0, 2, 1])
@pytest.mark.parametrize("exactness", ["EXACTLY_ONCE", "AT_LEAST_ONCE"])
def test_finish_validates_the_live_snapshot_for_compaction(tmp_path: Path, status: int, exactness: str) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    spec = yaml.safe_load((run_dir / "spec.yaml").read_text())
    spec["producer"] = {"shards": 3}
    spec["scoring"]["exactness"] = exactness
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "writers-stopped").touch()
    (run_dir / "scores/compactor-status.json").write_text('{"healthy":true}')
    environment = {
        **_finish_environment(tmp_path),
        **_stub_logs(tmp_path, {"STUB_TERMINAL_LOG": "terminal.log"}),
        "STUB_TERMINAL_STATUS": str(status),
    }
    run = _run_driver(
        FINISH, [RUN_ID], tmp_path, environment, programs={**FINISH_PROGRAMS, "verify-table": VERIFY_TABLE_STUB}
    )
    assert (run.result.returncode == 0) is (status == 0), run.result.stderr
    measured = (tmp_path / "terminal.log").read_text()
    assert f"--metadata {run_dir}/table-metadata.final.json" in measured
    assert f"--publish-logs {run_dir}/producer" in measured
    assert f"--corpus-uri {FACTS['corpus_uri']}" in measured
    assert "--publish-shards 3" in measured
    assert f"--exactness {exactness}" in measured
    assert (run_dir / "scores/terminal-exactness.json").exists()
    timing = json.loads((run_dir / "scores/terminal-validation.json").read_text())
    assert timing["mode"] == "host" and timing["resources"] is None and timing["exit_status"] == status
    assert (tmp_path / "collect.log").read_text(), "failed validation must remain collectable"


@needs_shell_tools
@pytest.mark.parametrize("verdict", ["complete", "unreadable", "no-document"])
@pytest.mark.parametrize("exactness", ["EXACTLY_ONCE", "AT_LEAST_ONCE"])
@pytest.mark.parametrize("compaction", [True, False])
def test_finish_can_validate_the_live_snapshot_from_a_cluster_job(
    tmp_path: Path, verdict: str, exactness: str, compaction: bool
) -> None:
    """The Job reads the same saved metadata, uploaded first, and its verdict is fetched back."""
    run_dir = _torn_down_run(tmp_path)
    if compaction:
        _declare_compactor(run_dir)
    spec = yaml.safe_load((run_dir / "spec.yaml").read_text())
    spec["producer"] = {"shards": 3}
    spec["scoring"]["exactness"] = exactness
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "writers-stopped").touch()
    if compaction:
        (run_dir / "scores/compactor-status.json").write_text('{"healthy":true}')
    fetched = tmp_path / "s3-objects"
    fetched.mkdir()
    (fetched / "terminal-exactness.json").write_text('{"verdict":true,"snapshot_id":17}')
    environment = {
        **_finish_environment(tmp_path),
        **_stub_logs(tmp_path, {"STUB_TERMINAL_LOG": "terminal.log"}),
        "STUB_S3_CP_DIR": str(fetched),
        "STUB_S3_UPLOAD_DIR": str(tmp_path / "uploaded"),
        "VERIFY_CPU": "6",
        "VERIFY_MEMORY": "12Gi",
        "VERIFY_READ_WORKERS": "48",
        "K8S_JOB_POLL_S": "0",
    }
    if verdict == "unreadable":
        environment["STUB_KUBECTL_FAIL_MATCH"] = f"get job/verify-table-{RUN_OBJECT}"
    elif verdict == "no-document":
        # The Job wrote no verdict: the fetch of that one object fails.
        environment["STUB_S3_CP_ABSENT"] = "scores/terminal-exactness.json"
    run = _run_driver(
        FINISH,
        [RUN_ID, "--verify-in-cluster", "--image-tag", "abc1234"],
        tmp_path,
        environment,
        programs={**FINISH_PROGRAMS, "verify-table": VERIFY_TABLE_STUB},
    )
    assert (run.result.returncode == 0) is (verdict == "complete"), run.result.stderr
    assert not (tmp_path / "terminal.log").read_text(), "nothing validates on the host"

    # The saved document goes up before the Job reads it, so both name one snapshot.
    final_uri = f"s3://a-bucket/runs/{RUN_ID}/table-metadata.final.json"
    assert f"s3 cp ./runs/{RUN_ID}/table-metadata.final.json {final_uri}" in run.aws_calls
    jobs = [document for document in run.applied if document["kind"] == "Job"]
    assert len(jobs) == 1
    job = jobs[0]
    assert cast(dict[str, str], job["metadata"])["name"] == f"verify-table-{RUN_OBJECT}"
    command = _job_command(job)
    assert f"verify-table --metadata {final_uri}" in command
    assert f"--exactness {exactness}" in command
    assert f"--publish-logs s3://a-bucket/runs/{RUN_ID}/producer" in command
    assert f"--upload-prefix s3://a-bucket/runs/{RUN_ID}/scores" in command
    assert "--publish-shards 3" in command and "--read-workers 48" in command
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in command
    container = cast(dict[str, object], cast(list[object], _pod_spec(job)["containers"])[0])
    resources = cast(dict[str, dict[str, str]], container["resources"])
    assert resources["requests"] == {"cpu": "6", "memory": "12Gi"} and resources["limits"] == {"memory": "12Gi"}
    assert "abc1234" in str(container["image"])

    assert f"s3 cp s3://a-bucket/runs/{RUN_ID}/scores/terminal-exactness.json" in run.aws_calls
    assert (run_dir / "scores/terminal-exactness.json").exists() is (verdict != "no-document")
    timing = json.loads((run_dir / "scores/terminal-validation.json").read_text())
    assert timing["mode"] == "cluster" and timing["resources"] == {"cpu": "6", "memory": "12Gi"}
    assert timing["read_workers"] == 48 and (timing["exit_status"] == 0) is (verdict == "complete")
    if verdict == "complete":
        assert f"delete job verify-table-{RUN_OBJECT} --ignore-not-found" in run.calls
    else:
        assert "kept for inspection" in run.result.stderr
    assert (tmp_path / "collect.log").read_text(), "failed validation must remain collectable"


@needs_shell_tools
@pytest.mark.parametrize("compaction", [True, False])
def test_finish_refuses_terminal_validation_before_writers_stop(tmp_path: Path, compaction: bool) -> None:
    run_dir = _torn_down_run(tmp_path)
    arguments = [RUN_ID]
    if compaction:
        _declare_compactor(run_dir)
    else:
        arguments.append("--verify-in-cluster")
    (run_dir / "scores/terminal-exactness.json").write_text('{"verdict":true}')
    run = _run_driver(FINISH, arguments, tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode != 0
    assert "writers-stopped" in run.result.stderr
    assert not (run_dir / "scores/terminal-exactness.json").exists()


@needs_shell_tools
@pytest.mark.parametrize("container_state", ["running", "waiting", "missing"])
def test_teardown_waits_for_sidecar_pods_and_saves_diagnostics(tmp_path: Path, container_state: str) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    (run_dir / "writers-stopped").touch()
    from tests.test_pod_fleet import compactor_pods

    pods = compactor_pods()
    items = pods["items"]
    assert isinstance(items, list)
    for pod in items:
        pod["status"]["containerStatuses"] = (
            [
                {"name": container["name"], "ready": container_state == "running", "state": {container_state: {}}}
                for container in pod["spec"]["containers"]
            ]
            if container_state != "missing"
            else []
        )
    kubectl = KUBECTL_STUB.replace(
        'case "$*" in',
        'case "$*" in\n*"get pod -l sparkoperator.k8s.io/app-name="*"-o name"*) '
        'printf "pod/still-writing\\n"; exit 0 ;;\n'
        '*"wait --for=delete pod"*) exit 1 ;;',
        1,
    )
    environment = {
        **_stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
        "STUB_PODS": json.dumps(pods),
    }
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        environment,
        programs={"kubectl": kubectl, "table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode != 0
    assert not (run_dir / "writers-stopped").exists()
    assert "--cascade=foreground --wait=true" in run.calls
    assert "wait --for=delete pod" in run.calls
    assert (run_dir / "scores/compactor-status.json").exists()
    assert json.loads((run_dir / "scores/compactor-status.json").read_text())["healthy"] is (
        container_state == "running"
    )
    assert "--exclude compactor-status.json" in run.aws_calls
    assert (run_dir / "scores/compactor.log").exists()
    assert not (tmp_path / "metadata.log").read_text(), "a writer may still commit"


@needs_shell_tools
@pytest.mark.parametrize("catchup_s", [0, 1])
def test_teardown_times_an_optional_sidecar_catch_up_on_its_own(tmp_path: Path, catchup_s: int) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")
    (run_dir / "flink-job-configmap.yaml").write_text("# flink-job-configmap.yaml\n")
    (run_dir / "compaction-catchup.json").write_text("stale")
    environment = {
        **_stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
        "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/00009.metadata.json",
        "STUB_PODS": json.dumps({"items": []}),
        "COMPACTION_CATCHUP_S": str(catchup_s),
    }
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        environment,
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    recorded = run_dir / "compaction-catchup.json"
    if catchup_s == 0:
        assert not recorded.exists(), "a stale record must not survive a teardown without a catch-up"
        assert "catch up" not in run.result.stderr
    else:
        catchup = json.loads(recorded.read_text())
        assert catchup["requested_s"] == 1 and catchup["started_at"] <= catchup["ended_at"]
        # The engine is already gone when the catch-up starts; the sidecar goes after it.
        lines = run.result.stderr.splitlines()
        engine = next(i for i, line in enumerate(lines) if "stopping writers declared in" in line)
        catching = next(i for i, line in enumerate(lines) if "catch up for 1s" in line)
        sidecar = next(i for i, line in enumerate(lines) if "stopping the compaction sidecar" in line)
        assert engine < catching < sidecar


@needs_shell_tools
def test_teardown_reads_the_metadata_only_after_the_last_writer_is_gone(tmp_path: Path) -> None:
    """A commit that lands while the fleet stops must be in the saved final snapshot.

    The catalog answers with a newer document once the sidecar's deletion has
    completed; a lookup issued any earlier would pin the older one.
    """
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    writers_gone = tmp_path / "writers-gone"
    early = f"{TABLE_LOCATION}/metadata/00007-early.metadata.json"
    late = f"{TABLE_LOCATION}/metadata/00008-late.metadata.json"
    kubectl = KUBECTL_STUB.replace(
        'case "$*" in',
        'case "$*" in\n*"delete -f ./runs/"*"/compactor.yaml"*) touch "$STUB_WRITERS_GONE"; exit 0 ;;',
        1,
    )
    table_metadata = TABLE_METADATA_STUB.replace(
        "[[ -z ${STUB_METADATA_OUT:-} ]] || printf '%s\\n' \"$STUB_METADATA_OUT\"",
        "if [[ -e $STUB_WRITERS_GONE ]]; then printf '%s\\n' \"$STUB_METADATA_OUT_LATE\"; "
        "else printf '%s\\n' \"$STUB_METADATA_OUT\"; fi",
        1,
    )
    environment = {
        **_stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
        "STUB_WRITERS_GONE": str(writers_gone),
        "STUB_METADATA_OUT": early,
        "STUB_METADATA_OUT_LATE": late,
        "STUB_PODS": json.dumps({"items": []}),
    }
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        environment,
        programs={"kubectl": kubectl, "table-metadata": table_metadata, "collect": COLLECT_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert writers_gone.exists(), "the sidecar document was never deleted"
    assert f"s3 cp {late}" in run.aws_calls and f"s3 cp {early}" not in run.aws_calls
    assert (run_dir / "writers-stopped").exists()
    calls = run.calls.splitlines()
    deletion = next(index for index, call in enumerate(calls) if "/compactor.yaml" in call)
    metadata_read = (tmp_path / "metadata.log").read_text()
    assert metadata_read.count("--table") == 1, "one lookup, after the fleet is gone"
    assert all("port-forward" not in call for call in calls[:deletion]), "no catalog reach before the fleet stops"


@needs_shell_tools
def test_teardown_continues_cleanup_when_the_compactor_selector_fails(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    final_metadata = (run_dir / "table-metadata.final.json").read_bytes()
    engine_k8s = (
        "if [[ $1 == --compaction ]]; then exit 2; fi\n"
        f'exec {shlex.quote(sys.executable)} -m ingest_bench.k8s.engine "$@"\n'
    )
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        _stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
        programs={"engine-k8s": engine_k8s, "table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode != 0
    assert "compactor pod selector" in run.result.stderr
    assert f"delete -f ./runs/{RUN_ID}/compactor.yaml" not in run.calls
    assert f"delete -f ./runs/{RUN_ID}/{compactors.CONFIGMAP_FILE}" in run.calls
    assert f"delete job scorer-{RUN_OBJECT}" in run.calls
    assert any(_mapping(document["metadata"])["name"] == f"drop-topic-{RUN_OBJECT}" for document in run.applied)
    assert (tmp_path / "collect.log").read_text()
    assert not (tmp_path / "metadata.log").read_text()
    assert (run_dir / "table-metadata.final.json").read_bytes() == final_metadata
    assert not (run_dir / "writers-stopped").exists()
    assert not (run_dir / "teardown-complete").exists()


@needs_shell_tools
def test_finish_recovers_pod_requests_without_overwriting_launch_facts(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    facts = (run_dir / "facts.json").read_bytes()
    archived = tmp_path / "archived"
    archived.mkdir()
    requests = '{"captured_at":"2026-09-10T12:00:00Z","items":[]}'
    (archived / "engine-pods.json").write_text(requests)
    (archived / "facts.json").write_text('{"epoch":null}')
    environment = {**_finish_environment(tmp_path), "STUB_S3_CP_DIR": str(archived)}
    run = _run_driver(FINISH, [RUN_ID], tmp_path, environment, programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    assert (run_dir / "engine-pods.json").read_text() == requests
    assert (run_dir / "facts.json").read_bytes() == facts


@needs_shell_tools
def test_finish_does_not_look_for_pod_requests_of_an_external_engine(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    spec = run_dir / "spec.yaml"
    spec.write_text(re.sub(r"^engine: .*$", "engine: external", spec.read_text(), count=1, flags=re.M))
    run = _run_driver(FINISH, [RUN_ID], tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    # Stage captures pod requests for managed engines only, so there is nothing to fetch.
    assert "stage/engine-pods.json" not in run.aws_calls
    assert not (run_dir / "engine-pods.json").exists()


@needs_shell_tools
def test_finish_recovers_compactor_requests(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    (run_dir / "compactor.yaml").write_text("kind: SparkApplication\n")
    archived = tmp_path / "archived"
    archived.mkdir()
    requests = '{"items":[{"metadata":{"name":"compact-driver"}}]}'
    (archived / "compactor-pods.json").write_text(requests)
    environment = {**_finish_environment(tmp_path), "STUB_S3_CP_DIR": str(archived)}
    run = _run_driver(FINISH, [RUN_ID], tmp_path, environment, programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    assert (run_dir / "compactor-pods.json").read_text() == requests


@needs_shell_tools
@pytest.mark.parametrize("observed_at", [None, "1757419300.125"])
def test_finish_uses_the_saved_metadata_observation_time(tmp_path: Path, observed_at: str | None) -> None:
    run_dir = _torn_down_run(tmp_path)
    if observed_at is not None:
        (run_dir / "table-metadata.observed-at").write_text(observed_at + "\n")
    run = _run_driver(FINISH, [RUN_ID], tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    measured = (tmp_path / "compaction.log").read_text()
    if observed_at is None:
        assert "--observed-at" not in measured
    else:
        assert f"--observed-at {observed_at}" in measured


@needs_shell_tools
def test_finish_measures_compaction_from_the_same_copied_document(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(FINISH, [RUN_ID], tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr

    measured = (tmp_path / "compaction.log").read_text()
    assert f"/work/runs/{RUN_ID}/table-metadata.final.json" in measured
    assert f"/work/runs/{RUN_ID}/scores" in measured
    # Absolute, for the reason `abs_path` gives.
    assert "--metadata /" in measured and "--out /" in measured
    # The same catalog properties geometry reads manifests with.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in measured
    # No run spec declares a target yet, so the command reads the table
    # property and records which source it used.
    assert "--target-file-bytes" not in measured
    assert (tmp_path / "work" / "runs" / RUN_ID / "scores" / "compaction.json").exists()


@needs_shell_tools
def test_finish_measures_the_geometry_from_the_copied_document_and_reports_it(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(FINISH, [RUN_ID], tmp_path, _finish_environment(tmp_path), programs=FINISH_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr

    measured = (tmp_path / "file-sizes.log").read_text()
    assert f"/work/runs/{RUN_ID}/table-metadata.final.json" in measured
    assert "--epoch 1757419200" in measured
    assert f"/work/runs/{RUN_ID}/scores" in measured
    # Absolute, for the reason `abs_path` gives.
    assert "--metadata /" in measured and "--out /" in measured
    # Every property the site declares, so the manifests the figures come from
    # are read with a region rather than against the global endpoint.
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in measured
    # The shipped smoke spec sets no ladder, so the flag is left off and
    # `file-sizes` applies its own default instead of one restated in the shell.
    assert "--offsets" not in measured

    assert "geometry: p50 38.2 MiB, small (<32 MiB) 28.5%, 137 files" in run.result.stdout
    # Both sides of the run are fetched: the scorer's artifacts and the publish
    # logs the offered figures are derived from.
    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/scores/ ./runs/{RUN_ID}/scores/" in run.aws_calls
    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/producer/ ./runs/{RUN_ID}/producer/" in run.aws_calls
    assert f"s3 cp s3://a-bucket/corpus/smoke-1a2b3c4d/corpus.json ./runs/{RUN_ID}/corpus.json" in run.aws_calls


@needs_shell_tools
def test_finish_reports_a_table_that_never_committed_without_failing(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID],
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_FILE_SIZES_STATUS": "4"},
        programs=FINISH_PROGRAMS,
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "no geometry: the table never committed" in run.result.stderr
    assert "geometry: p50" not in run.result.stdout
    assert (tmp_path / "collect.log").read_text().strip() != "", "the run is still collected"


@needs_shell_tools
@pytest.mark.parametrize("publish", [False, True])
def test_finish_keeps_missing_corpus_local_but_refuses_to_publish_it(tmp_path: Path, publish: bool) -> None:
    run_dir = build_run_dir(tmp_path / "work", run_id=RUN_ID)
    (run_dir / "corpus.json").unlink()
    environment = {**_finish_environment(tmp_path), "STUB_S3_CP_ABSENT": "corpus.json"}
    arguments = [RUN_ID]
    if publish:
        arguments.extend(["--publish", "results"])
    run = _run_driver(
        FINISH,
        arguments,
        tmp_path,
        environment,
        site=write_site(tmp_path).read_text(),
        programs={**FINISH_PROGRAMS, "collect": REAL_COLLECT_STUB},
    )
    assert (run.result.returncode == 0) is not publish, run.result.stderr
    if publish:
        assert "cannot publish" in run.result.stderr and "run.corpus: is null" in run.result.stderr
    assert json.loads((run_dir / "run.json").read_text())["run"]["corpus"] is None
    assert f"COLLECTED out={run_dir / 'run.json'}" in run.result.stdout
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 1
    assert not (tmp_path / "work" / "results").exists()
    assert (tmp_path / "results-table.log").read_text() == ""


@needs_shell_tools
def test_finish_publish_without_a_spec_engine_fails_cleanly(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    spec = yaml.safe_load((run_dir / "spec.yaml").read_text())
    del spec["engine"]
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    programs = {**FINISH_PROGRAMS, "collect": REAL_COLLECT_STUB}

    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        _finish_environment(tmp_path),
        programs=programs,
    )
    assert run.result.returncode != 0
    assert "spec must set engine" in run.result.stderr
    assert "Traceback" not in run.result.stderr
    assert not (tmp_path / "work" / "results").exists()


@needs_shell_tools
def test_finish_refuses_to_publish_an_invalid_run_unless_told_to(tmp_path: Path) -> None:
    build_run_dir(tmp_path / "work", run_id=RUN_ID, run_valid=False)
    refused = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_S3_CP_ABSENT": "corpus.json"},
        site=write_site(tmp_path).read_text(),
        programs={**FINISH_PROGRAMS, "collect": REAL_COLLECT_STUB},
    )
    assert refused.result.returncode != 0
    assert "run_valid is false" in refused.result.stderr and "--publish-invalid" in refused.result.stderr
    # One collect, into the run directory: nothing reached the results tree.
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 1
    assert not (tmp_path / "work" / "results").exists()
    assert (tmp_path / "results-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize("run_valid", [True, False])
@pytest.mark.parametrize(
    ("spec", "variant", "expected_variant"),
    [
        pytest.param(FLINK_SPEC, None, "default", id="flink-default"),
        pytest.param(FLINK_SPEC, "hash-fanout", "hash-fanout", id="flink-explicit"),
        pytest.param(EXTERNAL_SPEC, None, "default", id="external-default"),
        pytest.param(EXTERNAL_SPEC, "tuned", "tuned", id="external-explicit"),
    ],
)
def test_finish_publishes_once_and_preserves_the_verdict(
    tmp_path: Path, run_valid: bool, spec: str, variant: str | None, expected_variant: str
) -> None:
    """Publishing an invalid run must preserve its failing exit status."""
    build_run_dir(tmp_path / "work", run_id=RUN_ID, run_valid=run_valid, spec=spec)
    arguments = [RUN_ID, "--publish", "results"]
    if not run_valid:
        arguments.append("--publish-invalid")
    if variant is not None:
        arguments.extend(["--variant", variant])
    run = _run_driver(
        FINISH,
        arguments,
        tmp_path,
        {**_finish_environment(tmp_path), "STUB_S3_CP_ABSENT": "corpus.json"},
        site=write_site(tmp_path).read_text(),
        programs={**FINISH_PROGRAMS, "collect": REAL_COLLECT_STUB},
    )
    assert (run.result.returncode == 0) is run_valid, run.result.stderr
    if not run_valid:
        assert "run_valid is false; see the verdict above" in run.result.stderr

    collected = (tmp_path / "collect.log").read_text().splitlines()
    assert len(collected) == 1, "publication must reuse the locally collected document"
    assert "--publish /" in collected[0] and "/work/results" in collected[0]
    assert f"--workloads {fixture_workloads(tmp_path).resolve()}" in collected[0]
    assert ("--publish-invalid" in collected[0]) is not run_valid
    if variant is not None:
        assert f"--variant {variant}" in collected[0]
    else:
        assert "--variant" not in collected[0]
    published = list((tmp_path / "work" / "results").rglob("*.json"))
    assert len(published) == 1
    assert published[0].read_bytes() == (tmp_path / "work" / "runs" / RUN_ID / "run.json").read_bytes()
    assert json.loads(published[0].read_text())["run"]["variant"] == expected_variant
    # The table is generated from the published documents, never hand-edited.
    rendered = (tmp_path / "results-table.log").read_text()
    assert "/work/results --out /" in rendered and "/work/results/RESULTS.md" in rendered


@needs_shell_tools
def test_finish_reports_a_failed_results_table(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results"],
        tmp_path,
        _finish_environment(tmp_path),
        programs={**FINISH_PROGRAMS, "results-table": "exit 7"},
    )
    assert run.result.returncode == 7, run.result.stderr


@needs_shell_tools
def test_teardown_fetches_the_scores_and_collects_the_run(tmp_path: Path) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    location = f"{TABLE_LOCATION}/metadata/00003-abc.metadata.json"
    before = time.time()
    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_READ_FINISHED": str(tmp_path / "metadata-read-finished"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": location,
            "STUB_COLLECT_LOG": str(tmp_path / "collect.log"),
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    observed = float((run_dir / "table-metadata.observed-at").read_text())
    assert before <= observed <= time.time()
    assert observed >= float((tmp_path / "metadata-read-finished").read_text())
    assert f"s3://a-bucket/runs/{RUN_ID}/table-metadata.observed-at" in run.aws_calls

    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/scores/ ./runs/{RUN_ID}/scores/" in run.aws_calls
    assert f"s3 cp {location} ./runs/{RUN_ID}/table-metadata.final.json" in run.aws_calls
    assert (
        f"s3 cp ./runs/{RUN_ID}/table-metadata.final.json "
        f"s3://a-bucket/runs/{RUN_ID}/table-metadata.final.json" in run.aws_calls
    )

    # Absolute, for the reason `abs_path` gives.
    collected = (tmp_path / "collect.log").read_text()
    assert "--run-dir /" in collected and f"/work/runs/{RUN_ID}" in collected
    assert "--site /" in collected and "/work/site.yaml" in collected

    calls = run.calls.splitlines()
    producer_delete = next(i for i, call in enumerate(calls) if f"delete job producer-{RUN_OBJECT}" in call)
    topic_apply = next(i for i, call in enumerate(calls) if "apply -f -" in call)
    assert producer_delete < topic_apply
    assert "--cascade=foreground --wait=true" in calls[producer_delete]
    assert any(f"get pod -l job-name=producer-{RUN_OBJECT} -o name" in call for call in calls)

    assert (run_dir / "scores" / "scorer.log").read_text() == "POLL t=0.1 prefix=0/0\n"
    assert (run_dir / "scores" / "drop-topic.log").read_text() == STAGE_JOB_LOG
    assert (run_dir / "teardown-complete").is_file()
    assert f"logs job/scorer-{RUN_OBJECT} --timestamps" in run.calls
    assert f"logs job/drop-topic-{RUN_OBJECT} --timestamps" in run.calls
    scorer_log = next(i for i, call in enumerate(calls) if f"logs job/scorer-{RUN_OBJECT} --timestamps" in call)
    scorer_delete = next(i for i, call in enumerate(calls) if f"delete job scorer-{RUN_OBJECT}" in call)
    drop_log = next(i for i, call in enumerate(calls) if f"logs job/drop-topic-{RUN_OBJECT} --timestamps" in call)
    drop_deletes = [i for i, call in enumerate(calls) if f"delete job drop-topic-{RUN_OBJECT}" in call]
    assert scorer_log < scorer_delete
    assert topic_apply < drop_log < drop_deletes[-1]


@needs_shell_tools
@pytest.mark.parametrize("engine", ["flink", "external"])
@pytest.mark.parametrize("job_role", ["producer", "scorer"])
def test_teardown_preserves_artifacts_after_a_job_delete_timeout(tmp_path: Path, engine: str, job_role: str) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs" / "smoke" / f"smoke-{engine}.yaml").read_text())
    if engine == "flink":
        (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")
    metadata_log = tmp_path / "metadata.log"
    collect_log = tmp_path / "collect.log"

    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_KUBECTL_FAIL_MATCH": f"delete job {job_role}-{RUN_OBJECT}",
            "STUB_METADATA_LOG": str(metadata_log),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/00003-abc.metadata.json",
            "STUB_COLLECT_LOG": str(collect_log),
            "EXTERNAL_DETACH_WAIT_S": "0",
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )

    assert run.result.returncode != 0
    if engine == "flink":
        assert f"delete -f ./runs/{RUN_ID}/flinkdeployment.yaml" in run.calls
    else:
        assert "external engine remains attached" in run.result.stderr
    assert f"logs job/scorer-{RUN_OBJECT} --timestamps" in run.calls
    assert f"delete job scorer-{RUN_OBJECT}" in run.calls
    if job_role == "producer":
        assert "apply -f -" not in run.calls, "the topic must remain while the producer may still be publishing"
    else:
        assert "apply -f -" in run.calls, "a stuck scorer must not block topic deletion"
        assert (run_dir / "scores" / "drop-topic.log").exists()
    assert f"s3 sync s3://a-bucket/runs/{RUN_ID}/scores/" in run.aws_calls
    assert metadata_log.read_text() != ""
    assert (run_dir / "table-metadata.final.json").exists()
    assert collect_log.read_text() != ""
    assert not (run_dir / "teardown-complete").exists()
    assert "partially torn down" in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("signal", [True, False], ids=["signaled", "timed-out"])
@pytest.mark.parametrize("compaction", [False, True])
def test_external_teardown_waits_for_detach_before_dropping_the_topic(
    tmp_path: Path, signal: bool, compaction: bool
) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-external.yaml").read_text())
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    if compaction:
        _declare_compactor(run_dir)
        spec["compaction"] = yaml.safe_load((run_dir / "spec.yaml").read_text())["compaction"]
        (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    detached = tmp_path / "engine-detached"
    if signal:
        detached.touch()

    run = _run_driver(
        TEARDOWN,
        [
            RUN_ID,
            "--image-tag",
            "abc1234",
            "--external-detached-file",
            str(detached),
            "--external-detach-wait-s",
            "0",
        ],
        tmp_path,
        {"STUB_METADATA_LOG": str(tmp_path / "metadata.log"), "STUB_METADATA_STATUS": "3"},
        programs={"table-metadata": TABLE_METADATA_STUB},
    )
    assert (run.result.returncode == 0) is (signal or not compaction), run.result.stderr
    if signal:
        assert f"external engine detached: {detached}" in run.result.stderr
    else:
        assert "detach signal did not appear within 0s; dropping the topic" in run.result.stderr
    if compaction and not signal:
        assert not (run_dir / "writers-stopped").exists()
        assert not (run_dir / "teardown-complete").exists()
    if compaction and signal:
        assert run.result.stderr.index("external engine detached:") < run.result.stderr.index(
            "stopping the compaction sidecar"
        )

    calls = run.calls.splitlines()
    scorer_delete = next(i for i, call in enumerate(calls) if f"delete job scorer-{RUN_OBJECT}" in call)
    topic_apply = next(i for i, call in enumerate(calls) if "apply -f -" in call)
    assert scorer_delete < topic_apply


@needs_shell_tools
def test_the_engine_image_is_recorded_off_the_jobmanager_pod(tmp_path: Path) -> None:
    staged = _staged_engine_run(tmp_path)
    image = "a-registry/lakehouse-ingest-bench/flink:abc1234"
    digest = f"{image.split(':')[0]}@sha256:{'a' * 64}"
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {**_verify_environment(tmp_path), "STUB_STAGE_DIR": str(staged), "STUB_ENGINE_IMAGE": f"{image} {digest}"},
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    assert f"get pod -l app={RUN_OBJECT},component=jobmanager" in run.calls
    recorded = json.loads((tmp_path / "work" / "runs" / RUN_ID / "engine-image.json").read_text())
    assert recorded == {"image": image, "digest": digest}


@needs_shell_tools
def test_a_pod_that_reports_no_digest_still_stages(tmp_path: Path) -> None:
    staged = _staged_engine_run(tmp_path)
    image = "a-registry/lakehouse-ingest-bench/flink:abc1234"
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            **_verify_environment(tmp_path),
            "STUB_STAGE_DIR": str(staged),
            "STUB_ENGINE_IMAGE": f"{image} ",
            "ENGINE_IMAGE_WAIT_S": "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    recorded = json.loads((tmp_path / "work" / "runs" / RUN_ID / "engine-image.json").read_text())
    assert recorded == {"image": image, "digest": None}


def _staged_engine_run(tmp_path: Path) -> Path:
    """What the stage Job published, as the AWS stub copies it into the run directory."""
    staged = tmp_path / "staged"
    staged.mkdir()
    (staged / "facts.json").write_text(json.dumps(FACTS))
    for name in ("spec.yaml", "flinkdeployment.yaml", "flink-job-configmap.yaml"):
        (staged / name).write_text(f"# {name}\n")
    return staged


def _verify_environment(tmp_path: Path) -> dict[str, str]:
    """The logs the engine check and the tunnel behind it write to."""
    return _stub_logs(tmp_path, {"STUB_CURL_LOG": "curl-calls.log", "STUB_VERIFY_LOG": "verify-calls.log"})


def _purge_environment(tmp_path: Path) -> dict[str, str]:
    return _stub_logs(tmp_path, {"STUB_DROP_TABLE_LOG": "drop-table.log"})


PURGE_PROGRAMS = {"drop-table": DROP_TABLE_STUB}


@needs_shell_tools
@pytest.mark.parametrize("name", ["00003-abc.gz.metadata.json", "00003-abc.metadata.json"])
def test_a_compressed_metadata_document_is_stored_as_the_json_its_readers_parse(tmp_path: Path, name: str) -> None:
    """Detect gzip by content, including when the filename lacks a gzip suffix.
    Downstream readers require decompressed JSON.
    """
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    (run_dir / "scores").mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps({**FACTS, "epoch": 1757419200}))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    (run_dir / "flinkdeployment.yaml").write_text("# flinkdeployment.yaml\n")

    published = tmp_path / "published"
    published.mkdir()
    document = {"location": TABLE_LOCATION, "format-version": 2}
    (published / name).write_bytes(gzip.compress(json.dumps(document).encode()))

    torn = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/{name}",
            "STUB_S3_CP_DIR": str(published),
            "STUB_COLLECT_LOG": str(tmp_path / "collect.log"),
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert torn.result.returncode == 0, torn.result.stderr

    stored = run_dir / "table-metadata.final.json"
    assert json.loads(stored.read_text()) == document, "the stored copy is the JSON, not the gzip"

    # purge parses this file for the location it removes, so a gzip body would
    # end it on a syntax error with the table intact.
    purged = _run_driver(PURGE, [RUN_ID, "--yes"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert purged.result.returncode == 0, purged.result.stderr
    assert f"s3 rm --recursive {TABLE_LOCATION}" in purged.aws_calls


@needs_shell_tools
def test_purge_names_what_it_would_remove_and_removes_nothing_unasked(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(PURGE, [RUN_ID], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert run.result.returncode != 0
    assert "pass --yes to run unattended" in run.result.stderr

    assert str(FACTS["table"]) in run.result.stdout
    assert TABLE_LOCATION in run.result.stdout
    assert f"s3://a-bucket/runs/{RUN_ID}/" not in run.result.stdout, "the artifacts go only with --artifacts"

    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
def test_purge_refuses_while_the_run_is_still_being_scored(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {**_purge_environment(tmp_path), "STUB_OBJECT_NAME": f"scorer-{RUN_OBJECT}"},
        programs=PURGE_PROGRAMS,
    )
    assert run.result.returncode != 0
    assert f"job/scorer-{RUN_OBJECT} is still in ingest-bench" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize(
    "resource",
    [
        f"get job producer-{RUN_OBJECT}",
        f"get job seed-{RUN_OBJECT}",
        f"get job seed-wait-{RUN_OBJECT}",
        f"get pod -l job-name=producer-{RUN_OBJECT}",
        f"get pod -l job-name=seed-{RUN_OBJECT}",
        f"get pod -l job-name=scorer-{RUN_OBJECT}",
        f"get flinkdeployment {RUN_OBJECT}",
        f"get pod -l app={RUN_OBJECT} -o name",
        f"get SparkApplication {RUN_OBJECT}-compact",
        f"get pod -l sparkoperator.k8s.io/app-name={RUN_OBJECT}-compact",
    ],
)
@pytest.mark.parametrize("lookup_error", [False, True])
def test_purge_refuses_live_resources_or_unknown_presence(tmp_path: Path, resource: str, lookup_error: bool) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    manifest = run_dir / compactors.DOCUMENT_FILE
    manifest.write_text("kind: SparkApplication\n" + manifest.read_text())
    kubectl = (
        'if [[ $* == *"$STUB_LIVE_RESOURCE"* ]]; then\n'
        '    printf "%s\\n" "$*" >>"$STUB_LOG"\n'
        '    if [[ $STUB_LOOKUP_ERROR == yes ]]; then echo "lookup denied" >&2; exit 1; fi\n'
        '    echo "live-resource"; exit 0\n'
        "fi\n" + KUBECTL_STUB
    )
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_LIVE_RESOURCE": resource,
            "STUB_LOOKUP_ERROR": "yes" if lookup_error else "no",
        },
        programs={**PURGE_PROGRAMS, "kubectl": kubectl},
    )
    assert run.result.returncode != 0, run.result.stderr
    assert resource in run.calls
    assert ("lookup denied" if lookup_error else "still in ingest-bench") in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""
    assert not (run_dir / "writers-stopped").exists()
    assert not (run_dir / "teardown-complete").exists()


@needs_shell_tools
@pytest.mark.parametrize("driver,kind", [("spark-rewrite", "SparkApplication"), ("flink-rewrite", "FlinkDeployment")])
def test_purge_accepts_a_stopped_managed_compactor(tmp_path: Path, driver: str, kind: str) -> None:
    run_dir = _torn_down_run(tmp_path)
    spec_path = run_dir / "spec.yaml"
    spec = _mapping(yaml.safe_load(spec_path.read_text()))
    spec["compaction"] = {"driver": driver}
    spec_path.write_text(yaml.safe_dump(spec))
    (run_dir / compactors.DOCUMENT_FILE).write_text(f"kind: {kind}\nmetadata:\n  name: {RUN_OBJECT}-compact\n")
    run = _run_driver(PURGE, [RUN_ID, "--yes"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    assert f"get {kind} {RUN_OBJECT}-compact --ignore-not-found" in run.calls
    assert f"s3 rm --recursive {TABLE_LOCATION}" in run.aws_calls


@needs_shell_tools
def test_purge_refuses_when_the_managed_compactor_manifest_is_missing(tmp_path: Path) -> None:
    run_dir = _torn_down_run(tmp_path)
    _declare_compactor(run_dir)
    (run_dir / compactors.DOCUMENT_FILE).unlink()
    run = _run_driver(PURGE, [RUN_ID, "--yes"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert run.result.returncode != 0
    assert "cannot identify its writers" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize("engine", ["flink", "spark", "external"])
@pytest.mark.parametrize("compaction", [None, "inline", "external"])
def test_purge_accepts_manually_stopped_runs_without_teardown_markers(
    tmp_path: Path, engine: str, compaction: str | None
) -> None:
    run_dir = _torn_down_run(tmp_path)
    spec = run_dir / "spec.yaml"
    spec.write_text(re.sub(r"^engine: .*$", f"engine: {engine}", spec.read_text(), count=1, flags=re.M))
    if compaction is not None:
        spec.write_text(spec.read_text() + f"\ncompaction:\n  driver: {compaction}\n")
    run = _run_driver(PURGE, [RUN_ID, "--yes"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS)
    assert run.result.returncode == 0, run.result.stderr
    assert f"s3 rm --recursive {TABLE_LOCATION}" in run.aws_calls
    assert not (run_dir / "writers-stopped").exists()
    assert not (run_dir / "teardown-complete").exists()
    if engine != "external":
        kind = "flinkdeployment" if engine == "flink" else "sparkapplication"
        assert f"get {kind} {RUN_OBJECT} --ignore-not-found" in run.calls


@needs_shell_tools
def test_purge_asks_the_catalog_when_no_document_was_copied(tmp_path: Path) -> None:
    _torn_down_run(tmp_path, metadata=False)
    published = tmp_path / "published"
    published.mkdir()
    (published / "00007-live.metadata.json").write_text(json.dumps({"location": TABLE_LOCATION}))

    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "0",
            "STUB_METADATA_OUT": f"{TABLE_LOCATION}/metadata/00007-live.metadata.json",
            "STUB_S3_CP_DIR": str(published),
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert f"--table {FACTS['table']}" in (tmp_path / "metadata.log").read_text()
    # Dropped and removed exactly as a purge with the document already in hand.
    assert f"--table {FACTS['table']}" in (tmp_path / "drop-table.log").read_text()
    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    assert removals == [
        f"s3 rm --recursive {TABLE_LOCATION}",
        f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/",
    ]


@needs_shell_tools
def test_purge_reclaims_the_artifacts_of_a_run_the_catalog_holds_no_table_for(tmp_path: Path) -> None:
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            # What `table-metadata` exits for a table the catalog does not hold.
            "STUB_METADATA_STATUS": "3",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "holds no" in run.result.stdout and str(FACTS["table"]) in run.result.stdout
    # The prefix it names is the one printed under it, which is the only line
    # in that block naming something that goes.
    listed = run.result.stdout.splitlines()
    assert "only the run artifacts below will be removed" in listed[1] and f"runs/{RUN_ID}/" in listed[2]

    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    assert removals == [f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/"]
    assert (tmp_path / "drop-table.log").read_text() == "", "there is no table to drop"


@needs_shell_tools
def test_purge_claims_no_purge_when_there_is_nothing_to_remove(tmp_path: Path) -> None:
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "3",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "nothing to purge" in run.result.stdout and "--artifacts" in run.result.stdout
    assert "purged" not in run.result.stderr, "nothing was removed, so nothing is reported as purged"
    assert "delete the resources listed above?" not in run.result.stdout
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
def test_purge_refuses_when_it_cannot_ask_the_catalog(tmp_path: Path) -> None:
    _torn_down_run(tmp_path, metadata=False)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", "--artifacts"],
        tmp_path,
        {
            **_purge_environment(tmp_path),
            "STUB_METADATA_LOG": str(tmp_path / "metadata.log"),
            "STUB_METADATA_STATUS": "1",
            "STUB_METADATA_ERROR": "could not reach the catalog",
        },
        programs={**PURGE_PROGRAMS, "table-metadata": TABLE_METADATA_STUB},
    )
    assert run.result.returncode != 0
    assert "scripts/teardown.sh" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize(
    "location",
    [
        # The bucket, which is the corpus and every run's artifacts as well.
        "s3://a-bucket",
        "s3://a-bucket/",
        # The warehouse root, which is every table this site has ever held.
        WAREHOUSE_ROOT,
        f"{WAREHOUSE_ROOT}/",
        # Another site's warehouse, and a sibling prefix whose name starts the
        # same way — neither is under this site's root.
        "s3://another-bucket/warehouse/ingest_bench/t_x",
        "s3://a-bucket/warehouse-of-someone-else/t_x",
    ],
)
def test_purge_refuses_a_location_that_is_not_one_table(tmp_path: Path, location: str) -> None:
    """Recursive deletion must target a path below the warehouse, never the
    warehouse or bucket root.
    """
    _torn_down_run(tmp_path, location=location)
    run = _run_driver(
        PURGE, [RUN_ID, "--yes", "--artifacts"], tmp_path, _purge_environment(tmp_path), programs=PURGE_PROGRAMS
    )
    assert run.result.returncode != 0
    assert WAREHOUSE_ROOT in run.result.stderr, "the refusal names the root it was checked against"
    assert "nothing is removed" in run.result.stderr
    assert "s3 rm" not in run.aws_calls
    assert (tmp_path / "drop-table.log").read_text() == ""


@needs_shell_tools
@pytest.mark.parametrize("artifacts", [False, True])
def test_purge_removes_the_table_then_its_files_then_the_artifacts(tmp_path: Path, artifacts: bool) -> None:
    """Drop the catalog entry before deleting files; remove artifacts only on request."""
    _torn_down_run(tmp_path)
    run = _run_driver(
        PURGE,
        [RUN_ID, "--yes", *(["--artifacts"] if artifacts else [])],
        tmp_path,
        _purge_environment(tmp_path),
        programs=PURGE_PROGRAMS,
    )
    assert run.result.returncode == 0, run.result.stderr

    dropped = (tmp_path / "drop-table.log").read_text()
    assert f"--table {FACTS['table']}" in dropped
    assert "--catalog-prop uri=https://glue.eu-west-1.amazonaws.com/iceberg" in dropped

    removals = [line for line in run.aws_calls.splitlines() if line.startswith("s3 rm")]
    expected = [f"s3 rm --recursive {TABLE_LOCATION}"]
    if artifacts:
        expected.append(f"s3 rm --recursive s3://a-bucket/runs/{RUN_ID}/")
    assert removals == expected


# ---------------------------------------------------------------------------
# The whole run, chained
# ---------------------------------------------------------------------------

# `run.sh` calls the other drivers by absolute path out of its own directory, so
# what stands in for them is a copy of `scripts/` whose five drivers are stubs.
# The three files copied as they are are the ones under test: the driver, and
# the two libraries it reads the site and a run's paths through.
CHAINED_LIBRARIES = ("run.sh", "_lib.sh", "_k8s.sh")

STAGE_STUB = f"""
printf 'stage %s\\n' "$*" >>"$STUB_LOG"
printf 'run_id: %s\\n' '{RUN_ID}'
"""

LAUNCH_STUB = """
printf 'launch %s\\n' "$*" >>"$STUB_LOG"
"""

# The gate as `run.sh` polls it: it records the tick, publishes the state the
# scorer would have had by then — one line of `STUB_STATES` per tick — and on
# request leaves behind the one document a teardown writes.
RUN_GATE_STUB = """
printf 'gate %s\\n' "$*" >>"$STUB_LOG"
sleep "${STUB_GATE_DELAY_S:-0}"
state="$(head -n 1 "$STUB_STATES")"
tail -n +2 "$STUB_STATES" >"$STUB_STATES.rest"
mv "$STUB_STATES.rest" "$STUB_STATES"
printf '{"state":"%s"}\\n' "$state" >"$STUB_S3_CP_DIR/summary.json"
if [[ -n ${STUB_GATE_TEARS_DOWN:-} ]]; then
	mkdir -p "$(dirname -- "$STUB_TEARDOWN_MARKER")"
	: >"$STUB_TEARDOWN_MARKER"
fi
if [[ -n ${STUB_GATE_BREACHES:-} ]]; then
	mkdir -p "$(dirname -- "$STUB_BREACH_FILE")"
	printf '%s\\n' "$STUB_GATE_BREACHES" >"$STUB_BREACH_FILE"
fi
exit "${STUB_GATE_STATUS:-0}"
"""

TEARDOWN_STUB = """
printf 'teardown %s\\n' "$*" >>"$STUB_LOG"
exit "${STUB_TEARDOWN_STATUS:-0}"
"""

FINISH_STUB = """
printf 'finish %s\\n' "$*" >>"$STUB_LOG"
exit "${STUB_FINISH_STATUS:-0}"
"""

# Nothing to answer, only to be found: `run.sh` refuses up front over any tool
# its five drivers need, and these are the ones it never calls itself.
FOUND_STUB = "exit 0\n"


@dataclass(frozen=True)
class ChainedRun:
    """One `run.sh` over stubbed drivers, and the drivers it called in order."""

    result: subprocess.CompletedProcess[str]
    calls: list[str]

    def drivers(self) -> list[str]:
        return [line.split(" ", 1)[0] for line in self.calls]

    def call(self, driver: str) -> str:
        matching = [line for line in self.calls if line.startswith(f"{driver} ")]
        assert len(matching) == 1, f"expected one {driver} call, found {len(matching)}"
        return matching[0]


def _run_chained(
    tmp_path: Path,
    arguments: list[str],
    states: list[str],
    environment: dict[str, str] | None = None,
    engine: str = "flink",
) -> ChainedRun:
    repo = tmp_path / "repo"
    (repo / "scripts").mkdir(parents=True)
    for name in CHAINED_LIBRARIES:
        shutil.copy(SCRIPTS / name, repo / "scripts" / name)
    _stub_bin(
        repo / "scripts",
        {
            "stage.sh": STAGE_STUB,
            "launch.sh": LAUNCH_STUB,
            "gate.sh": RUN_GATE_STUB,
            "teardown.sh": TEARDOWN_STUB,
            "finish.sh": FINISH_STUB,
        },
    )
    work = tmp_path / "work"
    work.mkdir()
    (work / "site.yaml").write_text(_filled_site())
    spec = work / "a-run.yaml"
    spec.write_text(f"engine: {engine}\n")
    published = tmp_path / "published"
    published.mkdir()
    state_file = tmp_path / "states"
    state_file.write_text("".join(f"{state}\n" for state in states))
    calls = tmp_path / "driver-calls.log"
    calls.touch()
    stubs = _stub_bin(
        tmp_path / "bin",
        {"aws": AWS_STUB, "kubectl": FOUND_STUB, "curl": FOUND_STUB, "gzip": FOUND_STUB},
    )

    result = subprocess.run(
        [str(repo / "scripts" / "run.sh"), str(spec), "--gate-interval-s", "1", *arguments],
        cwd=work,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
        env={
            **os.environ,
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "STUB_LOG": str(calls),
            "STUB_AWS_LOG": str(tmp_path / "aws-calls.log"),
            "STUB_S3_CP_DIR": str(published),
            "STUB_STATES": str(state_file),
            "STUB_TEARDOWN_MARKER": str(work / "runs" / RUN_ID / "teardown-complete"),
            "STUB_BREACH_FILE": str(work / "runs" / RUN_ID / "gate-breaches"),
            **(environment or {}),
        },
    )
    return ChainedRun(result=result, calls=calls.read_text().splitlines())


@needs_shell_tools
@pytest.mark.parametrize(
    ("states", "environment", "drivers", "status", "said"),
    [
        # The state the scorer publishes is what says a run has ended, so the
        # loop leaves on the first tick that reads one that is not `running`.
        (["running", "drained"], {}, ["stage", "launch", "gate", "gate", "teardown", "finish"], 0, None),
        # `RUN_MAX_S` is short wherever one state is queued: the signal under
        # test is then the only thing that can end the loop, so a regression
        # fails in seconds rather than hanging out the two-hour default.
        #
        # A non-PASS tick is what a probe ladder's first leg is, so the loop
        # carries on: only the state, the gate's own teardown or the bound ends
        # it. This is also the case that pins `|| GATE_STATUS=$?`, without
        # which `set -e` would end the run on the tick instead.
        (
            ["running", "drained"],
            {"STUB_GATE_STATUS": "3"},
            ["stage", "launch", "gate", "gate", "teardown", "finish"],
            0,
            None,
        ),
        # A run the gate has already torn down is not torn down again — a
        # second one would converge, since the deletes ignore what is absent
        # and `drop-topic` is idempotent, but it is a Job and a minute for
        # nothing.
        (
            ["running"],
            {"STUB_GATE_TEARS_DOWN": "1", "RUN_MAX_S": "5"},
            ["stage", "launch", "gate", "finish"],
            0,
            None,
        ),
        # Without a completion marker (for example, from an older driver), the
        # breach count ends the loop and the idempotent teardown is retried.
        (
            ["running"],
            {"STUB_GATE_STATUS": "3", "STUB_GATE_BREACHES": "3", "RUN_MAX_S": "5"},
            ["stage", "launch", "gate", "teardown", "finish"],
            0,
            "has not passed",
        ),
        # `RUN_MAX_S` is the bound on a run that publishes nothing this can
        # read — and the verdict is still printed, because a run nobody waited
        # out has its artifacts as the only account of what it was doing.
        (
            [],
            {"RUN_MAX_S": "2"},
            ["stage", "launch", "gate", "teardown", "finish"],
            1,
            "still running after 2s",
        ),
    ],
    ids=["drained", "undersized-then-drained", "gate-tore-it-down", "gate-breached", "overran"],
)
def test_a_chained_run_ends_when_the_run_does(
    tmp_path: Path, states: list[str], environment: dict[str, str], drivers: list[str], status: int, said: str | None
) -> None:
    run = _run_chained(tmp_path, [], states, environment)
    assert run.result.returncode == status, run.result.stdout + run.result.stderr
    assert run.drivers() == drivers, run.calls
    # On stdout the moment staging returns, because it is the only handle on
    # the run: an operator who loses this shell reaches it with the drivers.
    assert f"run_id: {RUN_ID}" in run.result.stdout, run.result.stdout
    if said is not None:
        assert said in run.result.stderr, run.result.stderr


@needs_shell_tools
def test_a_teardown_that_did_not_converge_is_a_code_of_its_own(tmp_path: Path) -> None:
    """Finish and preserve the measured result, then return a distinct status for
    compute that may still be running.
    """
    run = _run_chained(tmp_path, [], ["drained"], {"STUB_TEARDOWN_STATUS": "1"})
    assert run.result.returncode == 6, run.result.stdout + run.result.stderr
    assert run.drivers() == ["stage", "launch", "gate", "teardown", "finish"], run.calls
    assert "scripts/teardown.sh " in run.result.stderr, run.result.stderr


@needs_shell_tools
def test_a_breach_file_that_holds_no_count_does_not_abort_the_loop(tmp_path: Path) -> None:
    """Under set -u, arithmetic interprets a bareword as an unset variable. Validate
    the file before arithmetic so cleanup remains reachable.
    """
    run = _run_chained(
        tmp_path,
        [],
        ["running", "drained"],
        {"STUB_GATE_STATUS": "1", "STUB_GATE_BREACHES": "abc", "RUN_MAX_S": "5"},
    )
    assert run.result.returncode == 0, run.result.stdout + run.result.stderr
    assert run.drivers() == ["stage", "launch", "gate", "gate", "teardown", "finish"], run.calls
    assert "unbound variable" not in run.result.stderr, run.result.stderr
    # Once, not once a tick: the file says the same thing every time.
    assert run.result.stderr.count("rather than a count of verdicts") == 1, run.result.stderr


@needs_shell_tools
def test_an_external_run_that_was_never_started_is_not_launched(tmp_path: Path) -> None:
    run = _run_chained(
        tmp_path,
        ["--external-ready-file", str(tmp_path / "never")],
        ["drained"],
        {"EXTERNAL_READY_WAIT_S": "1"},
        engine="external",
    )
    assert run.result.returncode == 1, run.result.stdout + run.result.stderr
    assert run.drivers() == ["stage"], run.calls
    assert "did not appear within 1s" in run.result.stderr, run.result.stderr


@needs_shell_tools
def test_an_external_chained_run_waits_before_it_launches(tmp_path: Path) -> None:
    ready = tmp_path / "engine-ready"
    ready.touch()
    waited = _run_chained(
        tmp_path / "with-a-file",
        ["--external-ready-file", str(ready)],
        ["drained"],
        engine="external",
    )
    assert waited.result.returncode == 0, waited.result.stdout + waited.result.stderr
    assert waited.drivers() == ["stage", "launch", "gate", "teardown", "finish"], waited.calls
    assert f"for {ready} to appear" in waited.result.stderr, waited.result.stderr

    # Nothing attached to answer, which is the newline form's own end: staged
    # and deliberately not launched, rather than launched into an engine that
    # may not be there.
    asked = _run_chained(tmp_path / "on-stdin", [], ["drained"], engine="external")
    assert asked.result.returncode == 1, asked.result.stdout + asked.result.stderr
    assert asked.drivers() == ["stage"], asked.calls
    assert "Use --external-ready-file for unattended runs" in asked.result.stderr, asked.result.stderr


@needs_shell_tools
def test_run_forwards_external_detach_and_broker_wait_options(tmp_path: Path) -> None:
    ready = tmp_path / "engine-ready"
    ready.touch()
    detached = tmp_path / "engine-detached"
    run = _run_chained(
        tmp_path,
        [
            "--external-ready-file",
            str(ready),
            "--external-detached-file",
            str(detached),
            "--external-detach-wait-s",
            "17",
            "--wait-brokers-s",
            "0",
        ],
        ["drained"],
        engine="external",
    )
    assert run.result.returncode == 0, run.result.stdout + run.result.stderr
    assert "--wait-brokers-s 0" in run.call("stage")
    for driver in ("gate", "teardown"):
        assert f"--external-detached-file {detached}" in run.call(driver)
        assert "--external-detach-wait-s 17" in run.call(driver)


@needs_shell_tools
def test_external_ready_file_is_refused_before_a_managed_run_is_staged(tmp_path: Path) -> None:
    """Validate before staging creates a fleet that a later refusal would leave up."""
    run = _run_chained(tmp_path, ["--external-ready-file", str(tmp_path / "never")], ["drained"])
    assert run.result.returncode == 1, run.result.stdout + run.result.stderr
    assert run.calls == [], "a fleet was started for a run that was refused"
    assert "requires engine: external" in run.result.stderr, run.result.stderr


def test_the_breach_count_run_defaults_to_is_the_one_the_gate_defaults_to() -> None:
    chained = re.search(r'^BREACHES="\$\{BREACHES:-(\d+)\}"$', RUN.read_text(), re.M)
    gated = re.search(r"^BREACHES_REQUIRED=(\d+)$", GATE.read_text(), re.M)
    assert chained and gated, "one of the two drivers no longer states a breach default"
    assert chained.group(1) == gated.group(1) == "3", "the drivers must keep the shipped three-breach threshold"


@needs_shell_tools
def test_a_chained_run_hands_each_flag_to_the_driver_that_owns_it(tmp_path: Path) -> None:
    run = _run_chained(
        tmp_path,
        [
            "--image-tag",
            "abc1234",
            "--breaches",
            "2",
            "--publish",
            "results/",
            "--variant",
            "tuned",
            "--verify-in-cluster",
        ],
        ["drained"],
        {"STUB_FINISH_STATUS": "1"},
    )
    assert run.result.returncode == 1, run.result.stdout + run.result.stderr

    assert "--image-tag abc1234" in run.call("stage")
    assert "--image-tag abc1234" in run.call("gate")
    assert "--breaches 2" in run.call("gate")
    assert "--teardown" in run.call("gate")
    assert "--publish results/" in run.call("finish")
    assert "--variant tuned" in run.call("finish")
    # The in-cluster validator is finish.sh's to run, with the same image tag the fleet ran.
    assert "--verify-in-cluster" in run.call("finish")
    assert "--image-tag abc1234" in run.call("finish")
    # The site reaches every driver explicitly, so each reads the file this run
    # was given rather than whatever `SITE_FILE` happens to hold.
    for driver in ("stage", "launch", "gate", "teardown", "finish"):
        assert "--site ./site.yaml" in run.call(driver), run.call(driver)


# ---------------------------------------------------------------------------
# The in-cluster stack: the Kafka chart
# ---------------------------------------------------------------------------


def _helm_template(chart: Path, settings: dict[str, str]) -> dict[str, dict[str, object]]:
    """The chart rendered with ``settings`` as ``--set`` pairs, keyed by kind."""
    command = ["helm", "template", "stack", str(chart), "--namespace", "a-namespace"]
    for key, value in settings.items():
        command += ["--set-string", f"{key}={value}"]
    rendered = subprocess.run(command, capture_output=True, text=True)
    assert rendered.returncode == 0, rendered.stderr
    documents = [_mapping(document) for document in yaml.safe_load_all(rendered.stdout) if document is not None]
    by_kind = {str(document["kind"]): document for document in documents}
    assert len(by_kind) == len(documents), "one object per kind"
    return by_kind


@needs_helm
@pytest.mark.parametrize("brokers, factor, isr", [("1", 1, 1), ("2", 2, 1), ("3", 3, 2), ("5", 3, 2)])
def test_the_kafka_chart_derives_replication_from_the_broker_count(brokers: str, factor: int, isr: int) -> None:
    """Match staging replication for internal topics too. Keep at least one in-sync
    replica when the broker count is small.
    """
    kafka = _mapping(_mapping(_helm_template(KAFKA_CHART, {"brokers": brokers})["Kafka"]["spec"])["kafka"])
    config = _mapping(kafka["config"])
    assert config["default.replication.factor"] == factor
    assert config["offsets.topic.replication.factor"] == factor
    assert config["transaction.state.log.replication.factor"] == factor
    assert config["min.insync.replicas"] == isr
    assert config["transaction.state.log.min.isr"] == isr
    assert config["auto.create.topics.enable"] is False, "staging creates the topic"


@needs_helm
@pytest.mark.parametrize("settings, io, network", [({}, 8, 3), ({"ioThreads": "16", "networkThreads": "8"}, 16, 8)])
def test_the_kafka_chart_keeps_kafka_s_thread_defaults_unless_asked(
    settings: dict[str, str], io: int, network: int
) -> None:
    kafka = _mapping(_mapping(_helm_template(KAFKA_CHART, settings)["Kafka"]["spec"])["kafka"])
    config = _mapping(kafka["config"])
    assert str(config["num.io.threads"]) == str(io)
    assert str(config["num.network.threads"]) == str(network)


@needs_helm
def test_the_kafka_chart_exposes_one_plain_listener() -> None:
    rendered = _helm_template(KAFKA_CHART, {})
    kafka = _mapping(_mapping(rendered["Kafka"]["spec"])["kafka"])
    listeners = [_mapping(listener) for listener in _sequence(kafka["listeners"])]
    assert listeners == [{"name": "plain", "port": 9092, "type": "internal", "tls": False}]
    assert kafka["version"] == "4.3.1" and kafka["metadataVersion"] == "4.3-IV0"
    assert _mapping(rendered["Kafka"]["metadata"])["name"] == "ingest-bench"
    assert "entityOperator" not in _mapping(rendered["Kafka"]["spec"]), "topics come from the admin API"


@needs_helm
def test_the_kafka_chart_adds_standard_metrics_only_when_enabled() -> None:
    disabled = _helm_template(KAFKA_CHART, {})
    disabled_spec = _mapping(disabled["Kafka"]["spec"])
    assert "metricsConfig" not in _mapping(disabled_spec["kafka"])
    assert "kafkaExporter" not in disabled_spec
    assert "ConfigMap" not in disabled

    enabled = _helm_template(KAFKA_CHART, {"metrics.enabled": "true"})
    enabled_spec = _mapping(enabled["Kafka"]["spec"])
    metrics = _mapping(_mapping(enabled_spec["kafka"])["metricsConfig"])
    assert metrics == {
        "type": "jmxPrometheusExporter",
        "valueFrom": {"configMapKeyRef": {"name": "ingest-bench-kafka-metrics", "key": "kafka-metrics-config.yml"}},
    }
    exporter = _mapping(enabled_spec["kafkaExporter"])
    assert exporter["topicRegex"] == ".*" and exporter["groupRegex"] == ".*"
    assert "resources" not in exporter, "use Strimzi's maintained Kafka Exporter defaults"

    config = str(_mapping(_mapping(enabled["ConfigMap"])["data"])["kafka-metrics-config.yml"])
    assert r"kafka.(\w+)<type=(.+), name=(.+)PerSec" in config
    assert "kafka.server<type=raft-metrics" in config
    assert "kafka_$1_$2_$3_total" in config


@needs_helm
def test_the_kafka_chart_places_and_sizes_its_brokers() -> None:
    """Strimzi has no pod nodeSelector field; render equivalent required node affinity."""
    rendered = _helm_template(
        KAFKA_CHART,
        {
            "brokers": "3",
            "storage.size": "500Gi",
            "storage.class": "ingest-bench-kafka",
            "resources.cpu": "4",
            "resources.memory": "16Gi",
            "jvmHeap": "6g",
            "nodeSelector.lakehouse-ingest-bench/role": "kafka",
            "tolerations[0].key": "lakehouse-ingest-bench/kafka",
            "tolerations[0].operator": "Equal",
            "tolerations[0].value": "true",
            "tolerations[0].effect": "NoSchedule",
        },
    )
    pool = _mapping(rendered["KafkaNodePool"]["spec"])
    assert pool["replicas"] == 3
    assert set(_sequence(pool["roles"])) == {"controller", "broker"}
    volume = _mapping(_sequence(_mapping(pool["storage"])["volumes"])[0])
    assert volume["type"] == "persistent-claim" and volume["size"] == "500Gi"
    assert volume["class"] == "ingest-bench-kafka"
    assert volume["deleteClaim"] is True and volume["kraftMetadata"] == "shared"
    resources = _mapping(pool["resources"])
    assert _mapping(resources["requests"]) == {"cpu": "4", "memory": "16Gi"}
    assert _mapping(resources["limits"]) == {"cpu": "4"}
    assert _mapping(pool["jvmOptions"]) == {"-Xms": "6g", "-Xmx": "6g"}
    pod = _mapping(_mapping(pool["template"])["pod"])
    assert _sequence(pod["tolerations"]) == [
        {"key": "lakehouse-ingest-bench/kafka", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
    ]
    terms = _sequence(
        _mapping(_mapping(_mapping(pod["affinity"])["nodeAffinity"])["requiredDuringSchedulingIgnoredDuringExecution"])[
            "nodeSelectorTerms"
        ]
    )
    expressions = _sequence(_mapping(terms[0])["matchExpressions"])
    assert expressions == [{"key": "lakehouse-ingest-bench/role", "operator": "In", "values": ["kafka"]}]
    labels = _mapping(_mapping(rendered["KafkaNodePool"]["metadata"])["labels"])
    assert labels["strimzi.io/cluster"] == "ingest-bench", "the pool belongs to the Kafka CR of that name"


@needs_helm
def test_the_kafka_chart_leaves_placement_and_class_out_when_unset() -> None:
    """Omit empty settings so storage uses its default class and no invalid affinity is rendered."""
    pool = _mapping(_helm_template(KAFKA_CHART, {})["KafkaNodePool"]["spec"])
    volume = _mapping(_sequence(_mapping(pool["storage"])["volumes"])[0])
    assert "class" not in volume
    assert "template" not in pool


# ---------------------------------------------------------------------------
# The in-cluster stack: the helmfile
# ---------------------------------------------------------------------------

STACK_HELMFILE = STACK_DEPLOY / "helmfile.yaml.gotmpl"
STACK_SETUP = STACK_DEPLOY / "setup.sh"


def _helmfile_environment_names() -> set[str]:
    texts = [STACK_HELMFILE.read_text()] + [
        path.read_text() for path in sorted((STACK_DEPLOY / "values").glob("*.gotmpl"))
    ]
    names: set[str] = set()
    for text in texts:
        names |= set(re.findall(r'requiredEnv "([A-Z_]+)"', text))
    return names


def test_the_helmfile_names_the_three_releases_at_their_pins() -> None:
    text = STACK_HELMFILE.read_text()
    releases = text[text.index("\nreleases:") :]
    assert releases.count("\n  - name: ") == 3
    for release in ("strimzi-kafka-operator", "kafka", "lakekeeper"):
        assert f"\n  - name: {release}\n" in text, release
    assert "chart: ./charts/kafka" in text
    assert "needs:\n      - strimzi-operator/strimzi-kafka-operator" in text, "the CRDs exist before the CR"
    assert 'version: {{ requiredEnv "STRIMZI_VERSION" }}' in text
    assert 'version: {{ requiredEnv "LAKEKEEPER_CHART_VERSION" }}' in text
    assert "oci: true" in text and "quay.io/strimzi-helm" in text
    assert "https://lakekeeper.github.io/lakekeeper-charts/" in text


def test_the_catalog_values_turn_vending_and_auth_off_and_name_its_identity() -> None:
    text = (STACK_DEPLOY / "values" / "lakekeeper.yaml.gotmpl").read_text()
    assert "name: ingest-bench-catalog" in text, "the account a pod identity association is made for"
    assert 'encryptionKeySecret: {{ requiredEnv "CATALOG_SECRET" }}' in text
    assert 'config: {{ requiredEnv "STACK_CATALOG_CONFIG_JSON" }}' in text
    assert 'extraEnv: {{ requiredEnv "STACK_CATALOG_ENV_JSON" }}' in text
    assert "providerUri" not in text and "k8s:" not in text, "authentication stays at the chart's off default"
    assert "postgresql:\n  enabled: true" in text
    assert 'className: {{ requiredEnv "CATALOG_STORAGE_CLASS" }}' in text
    assert "\nhelmWait: true\n" in text, "the migration Job must not be a post-install hook under --wait"


# ---------------------------------------------------------------------------
# The in-cluster stack: setup.sh
# ---------------------------------------------------------------------------


def _without(*names: str) -> dict[str, str]:
    return {key: value for key, value in os.environ.items() if key not in names}


@needs_bash
def test_stack_setup_answers_before_it_needs_a_cloud() -> None:
    environment = _without("CLOUD", "KUBE_CONTEXT", "CLUSTER_NAME", "BUCKET", "AWS_REGION")
    out = subprocess.run([str(STACK_SETUP), "--help"], capture_output=True, text=True, env=environment)
    assert out.returncode == 0, out.stderr
    assert "--site PATH" in out.stdout and "CLOUD" in out.stdout

    refused = subprocess.run([str(STACK_SETUP), "--brokers"], capture_output=True, text=True, env=environment)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --brokers" in refused.stderr


@needs_bash
@pytest.mark.parametrize(
    "cloud, said",
    [
        (None, "CLOUD must be set"),
        ("gcp", "CLOUD=gcp is not supported yet"),
        ("azure", "CLOUD is 'azure'"),
    ],
)
def test_stack_setup_refuses_a_cloud_it_has_no_hook_for_by_name(cloud: str | None, said: str) -> None:
    environment = _without("CLOUD")
    if cloud is not None:
        environment["CLOUD"] = cloud
    out = subprocess.run([str(STACK_SETUP)], capture_output=True, text=True, env=environment)
    assert out.returncode == 1, out.stdout
    assert said in out.stderr
    assert "missing host tool" not in out.stderr


@needs_bash
def test_stack_setup_refuses_a_heap_that_is_not_below_the_pod_s_memory() -> None:
    """Leave memory outside the JVM heap for native overhead and the page cache."""
    environment = _without(
        "CLOUD", "KUBE_CONTEXT", "CLUSTER_NAME", "BUCKET", "AWS_REGION", "KAFKA_JVM_HEAP", "KAFKA_MEM_GI"
    )
    environment["CLOUD"] = "aws"
    environment["KAFKA_JVM_HEAP"] = "16g"
    environment["KAFKA_MEM_GI"] = "16"
    out = subprocess.run([str(STACK_SETUP)], capture_output=True, text=True, env=environment)
    assert out.returncode == 1, out.stdout
    assert "must be below" in out.stderr
    assert "missing host tool" not in out.stderr


def test_stack_setup_keeps_broker_metrics_opt_in() -> None:
    setup = STACK_SETUP.read_text()
    helmfile = STACK_HELMFILE.read_text()
    assert 'KAFKA_METRICS="${KAFKA_METRICS:-false}"' in setup
    assert "KAFKA_METRICS must be true or false" in setup
    assert 'enabled: {{ requiredEnv "KAFKA_METRICS" }}' in helmfile


def test_stack_setup_and_teardown_default_the_broker_thread_pools_to_kafka_s_own() -> None:
    helmfile = STACK_HELMFILE.read_text()
    for knob, default in (("KAFKA_IO_THREADS", "8"), ("KAFKA_NETWORK_THREADS", "3")):
        assert f'{knob}="${{{knob}:-{default}}}"' in STACK_SETUP.read_text()
        assert f'{knob}="${{{knob}:-{default}}}"' in STACK_TEARDOWN.read_text(), "helmfile parses even on destroy"
        assert f'requiredEnv "{knob}"' in helmfile


# An existing stack role that stack setup created, so setup may refresh it.
TAGGED_STACK_ROLE = json.dumps(
    {
        "Role": {
            "Arn": "arn:aws:iam::123456789012:role/lakehouse-ingest-bench-stack",
            "Tags": [{"Key": "lakehouse-ingest-bench", "Value": "true"}],
        }
    }
)


@needs_shell_tools
def test_stack_setup_uses_the_ephemeral_catalog_tunnel_for_management_calls(tmp_path: Path) -> None:
    calls = tmp_path / "calls.log"
    curl_calls = tmp_path / "curl.log"
    site = tmp_path / "site.yaml"
    site.write_text("kafka:\n  deployment: in-cluster\n")
    kubectl = """
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"get nodes -o name"*) printf 'node/a\\n' ;;
*"apply -f -"*) cat >/dev/null ;;
*"get secret ingest-bench-catalog-keys -o jsonpath={.data.externalId}"*) printf 'ZXh0ZXJuYWwtaWQ=' ;;
*"port-forward svc/lakekeeper :8181"*)
	printf 'Forwarding from 127.0.0.1:43210 -> 8181\\n' >&2
	exec /bin/sleep 10
	;;
esac
"""
    curl = """
printf '%s\\n' "$*" >>"$STUB_CURL_LOG"
case "${!#}" in
*/info) printf '{"bootstrapped":true}\\n' ;;
*/warehouse) printf '{"warehouses":[{"name":"ingest-bench"}]}\\n' ;;
esac
"""
    helm = """
printf '{"spark":{"jobNamespaces":["ingest-bench"]}}\\n'
"""
    aws = """
case "$*" in
*"sts get-caller-identity"*) printf '123456789012\\n' ;;
*"iam get-role"*) printf '%s\\n' "$STUB_STACK_ROLE" ;;
esac
"""
    stubs = _stub_bin(
        tmp_path / "bin",
        {
            "kubectl": kubectl,
            "curl": curl,
            "helm": helm,
            "helmfile": ":",
            "aws": aws,
            "envsubst": "cat",
        },
    )
    environment = _without("CATALOG_FORWARD_PORT")
    environment.update(
        {
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "CLOUD": "aws",
            "KUBE_CONTEXT": "a-cluster",
            "CLUSTER_NAME": "a-cluster",
            "BUCKET": "a-bucket",
            "AWS_REGION": "eu-west-1",
            "STUB_LOG": str(calls),
            "STUB_CURL_LOG": str(curl_calls),
            "STUB_STACK_ROLE": TAGGED_STACK_ROLE,
        }
    )

    out = subprocess.run(
        [str(STACK_SETUP), "--site", str(site)], capture_output=True, text=True, env=environment, timeout=10
    )

    assert out.returncode == 0, out.stderr
    assert "port-forward svc/lakekeeper :8181" in calls.read_text()
    requested = curl_calls.read_text()
    assert "http://localhost:43210/health" in requested
    assert "http://localhost:43210/management/v1/info" in requested
    assert "http://localhost:43210/management/v1/warehouse" in requested


@pytest.mark.parametrize("lookup_error", ["", "helm", "deployment", "namespace", "absent"])
def test_stack_setup_preserves_watched_namespaces(tmp_path: Path, lookup_error: str) -> None:
    """Preserve release and live watches, prune deleted namespaces, and fail on unreadable state."""
    calls = tmp_path / "calls.log"
    site = tmp_path / "site.yaml"
    site.write_text("kafka:\n  deployment: in-cluster\n")
    kubectl = """
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"get nodes -o name"*) printf 'node/a\\n' ;;
*"apply -f -"*) cat >/dev/null ;;
*"get deployment strimzi-cluster-operator"*)
    [[ "$STUB_STRIMZI_LOOKUP_ERROR" != deployment ]] || { echo 'deployment lookup denied' >&2; exit 1; }
    [[ "$STUB_STRIMZI_LOOKUP_ERROR" != absent ]] || exit 0
    printf 'ingest-bench,live-peer,strimzi-operator' ;;
*"get namespace gone"*)
    [[ "$*" == *--ignore-not-found* ]] && exit 0
    exit 1 ;;
*"get namespace ingest-bench-rd"*|*"get namespace live-peer"*)
    [[ "$STUB_STRIMZI_LOOKUP_ERROR" != namespace ]] || { echo 'namespace lookup denied' >&2; exit 1; }
    printf 'namespace/peer' ;;
*"get secret ingest-bench-catalog-keys -o jsonpath={.data.externalId}"*) printf 'ZXh0ZXJuYWwtaWQ=' ;;
*"port-forward svc/lakekeeper :8181"*)
	printf 'Forwarding from 127.0.0.1:43211 -> 8181\\n' >&2
	exec /bin/sleep 10
	;;
esac
"""
    curl = """
case "${!#}" in
*/info) printf '{"bootstrapped":true}\\n' ;;
*/warehouse) printf '{"warehouses":[{"name":"ingest-bench"}]}\\n' ;;
esac
"""
    helm = """
case "$*" in
*strimzi-kafka-operator*)
    [[ "$STUB_STRIMZI_LOOKUP_ERROR" != helm ]] || { echo 'release lookup denied' >&2; exit 1; }
    [[ "$STUB_STRIMZI_LOOKUP_ERROR" != absent ]] || { echo 'Error: release: not found' >&2; exit 1; }
    printf '{"watchNamespaces":["ingest-bench-rd","gone"]}\\n' ;;
*) printf '{"spark":{"jobNamespaces":["ingest-bench"]}}\\n' ;;
esac
"""
    helmfile = """
printf 'helmfile STRIMZI_WATCH_NAMESPACES=%s\\n' "$STRIMZI_WATCH_NAMESPACES" >>"$STUB_LOG"
"""
    aws = """
case "$*" in
*"sts get-caller-identity"*) printf '123456789012\\n' ;;
*"iam get-role"*) printf '%s\\n' "$STUB_STACK_ROLE" ;;
esac
"""
    stubs = _stub_bin(
        tmp_path / "bin",
        {"kubectl": kubectl, "curl": curl, "helm": helm, "helmfile": helmfile, "aws": aws, "envsubst": "cat"},
    )
    environment = _without("CATALOG_FORWARD_PORT")
    environment.update(
        {
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "CLOUD": "aws",
            "KUBE_CONTEXT": "a-cluster",
            "CLUSTER_NAME": "a-cluster",
            "BUCKET": "a-bucket",
            "AWS_REGION": "eu-west-1",
            "STUB_LOG": str(calls),
            "STUB_STRIMZI_LOOKUP_ERROR": lookup_error,
            "STUB_STACK_ROLE": TAGGED_STACK_ROLE,
        }
    )

    out = subprocess.run(
        [str(STACK_SETUP), "--site", str(site)], capture_output=True, text=True, env=environment, timeout=10
    )

    logged = calls.read_text()
    if lookup_error not in ("", "absent"):
        assert out.returncode != 0
        assert "denied" in out.stderr
        assert "helmfile STRIMZI_WATCH_NAMESPACES=" not in logged
        return
    assert out.returncode == 0, out.stderr
    if lookup_error == "absent":
        assert 'helmfile STRIMZI_WATCH_NAMESPACES=["ingest-bench"]' in logged
        return
    assert 'helmfile STRIMZI_WATCH_NAMESPACES=["ingest-bench","ingest-bench-rd","live-peer"]' in logged
    assert "get namespace gone" in logged and "get namespace strimzi-operator" not in logged
    assert "dropping deleted namespace gone" in out.stderr


@pytest.mark.parametrize("other_kafka", ["", "ingest-bench-rd", "lookup-denied"])
def test_stack_teardown_all_keeps_the_shared_operator_while_another_kafka_runs(
    tmp_path: Path, other_kafka: str
) -> None:
    calls = tmp_path / "calls.log"
    site = tmp_path / "site.yaml"
    site.write_text("kafka:\n  deployment: in-cluster\n")
    kubectl = f"""
printf '%s\\n' "$*" >>"$STUB_LOG"
case "$*" in
*"get kafka --all-namespaces"*)
    [[ '{other_kafka}' != lookup-denied ]] || {{ echo 'Kafka lookup denied' >&2; exit 1; }}
    printf 'ingest-bench\\n{other_kafka}\\n' ;;
esac
"""
    helmfile = """
printf 'helmfile %s\\n' "$*" >>"$STUB_LOG"
"""
    aws = """
case "$*" in
*"list-pod-identity-associations"*) printf 'None\\n' ;;
*"iam get-role"*) echo 'An error occurred (NoSuchEntity) when calling the GetRole operation' >&2; exit 254 ;;
esac
"""
    stubs = _stub_bin(tmp_path / "bin", {"kubectl": kubectl, "helm": ":", "helmfile": helmfile, "aws": aws})
    environment = dict(os.environ)
    environment.update(
        {
            "PATH": f"{stubs}:{os.environ['PATH']}",
            "CLOUD": "aws",
            "KUBE_CONTEXT": "a-cluster",
            "CLUSTER_NAME": "a-cluster",
            "BUCKET": "a-bucket",
            "AWS_REGION": "eu-west-1",
            "STUB_LOG": str(calls),
        }
    )

    out = subprocess.run(
        [str(STACK_TEARDOWN), "--site", str(site), "--all", "--yes"],
        capture_output=True,
        text=True,
        env=environment,
        timeout=20,
    )

    logged = calls.read_text()
    destroyed = "destroy --selector name=strimzi-kafka-operator" in logged
    if other_kafka == "lookup-denied":
        assert out.returncode != 0
        assert "Kafka lookup denied" in out.stderr
        assert not destroyed
        assert "delete storageclass" not in logged
        return
    assert out.returncode == 0, out.stderr
    assert destroyed == (other_kafka == "")
    assert ("delete storageclass" in logged) == (other_kafka == "")
    if other_kafka:
        assert "keeping the Strimzi operator" in out.stderr and other_kafka in out.stderr


def test_stack_setup_substitutes_every_marker_the_registry_template_carries() -> None:
    template = REPO_ROOT / "deploy" / "k8s" / "schema-registry.yaml.tmpl"
    setup = STACK_SETUP.read_text()
    markers = {match[2:-2] for match in MARKER_RE.findall(template.read_text())}
    assert markers == {"NAMESPACE", "NODE_SELECTOR", "TOLERATIONS"}
    for marker in markers:
        assert f"s|__{marker}__|$" in setup, f"setup.sh never substitutes __{marker}__"
    assert "WITH_SCHEMA_REGISTRY" in setup
    assert "schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7" in setup


def test_stack_setup_prints_every_value_the_site_example_asks_for() -> None:
    setup = STACK_SETUP.read_text()
    for printed in (
        "kafka.bootstrap_servers:",
        "catalog.props.uri:",
        "catalog.props.warehouse:",
        "kubernetes.context:",
        "kubernetes.namespace:",
    ):
        assert printed in setup, printed
    assert "$KAFKA_NAME-kafka-bootstrap.$NAMESPACE.svc:9092" in setup
    assert "http://lakekeeper.$NAMESPACE.svc:8181/catalog" in setup


def test_stack_setup_reuses_the_namespace_manifest_rather_than_copying_it() -> None:
    setup = STACK_SETUP.read_text()
    assert "deploy/aws/k8s/namespace.yaml.tmpl" in setup
    assert not (STACK_DEPLOY / "namespace.yaml.tmpl").exists()


def test_stack_setup_binds_the_catalog_beside_the_three_run_identities() -> None:
    """The catalog also writes warehouse metadata and needs an identity association."""
    setup = STACK_SETUP.read_text()
    site = _mapping(_mapping(yaml.safe_load(SITE_AWS_EXAMPLE.read_text()))["kubernetes"])
    for account in (site["harness_service_account"], site["flink_service_account"], site["spark_service_account"]):
        assert str(account) in setup
    assert "ingest-bench-catalog" in setup
    bind = re.search(r"stack_bind_identity \"\$NAMESPACE\"(.*)", setup)
    assert bind is not None
    assert bind.group(1).count("SERVICE_ACCOUNT") == 4


def test_every_helmfile_input_is_something_setup_exports() -> None:
    names = _helmfile_environment_names()
    assert names, "the helmfile reads nothing from the environment"
    setup = STACK_SETUP.read_text()
    for name in sorted(names):
        assert re.search(rf"\bexport\b[^\n]*\b{name}\b", setup), f"setup.sh never exports {name}"


# ---------------------------------------------------------------------------
# The in-cluster stack: teardown.sh
# ---------------------------------------------------------------------------

STACK_TEARDOWN = STACK_DEPLOY / "teardown.sh"


@needs_bash
def test_stack_teardown_takes_its_arguments_before_it_needs_a_cloud() -> None:
    environment = _without("CLOUD", "KUBE_CONTEXT", "CLUSTER_NAME", "BUCKET", "AWS_REGION")
    out = subprocess.run([str(STACK_TEARDOWN), "--help"], capture_output=True, text=True, env=environment)
    assert out.returncode == 0, out.stderr
    assert "--all" in out.stdout and "--yes" in out.stdout

    refused = subprocess.run([str(STACK_TEARDOWN), "--everything"], capture_output=True, text=True, env=environment)
    assert refused.returncode == 2, refused.stdout
    assert "unknown argument --everything" in refused.stderr


def test_stack_teardown_removes_the_broker_before_the_operator_that_owns_it() -> None:
    """Keep Strimzi running until it has removed broker claims; deleting the operator
    first can orphan volumes.
    """
    text = STACK_TEARDOWN.read_text()
    kafka = text.index("--selector name=kafka")
    lakekeeper = text.index("--selector name=lakekeeper")
    namespace = text.index("delete namespace")
    operator = text.index("--selector name=strimzi-kafka-operator")
    assert kafka < lakekeeper < namespace < operator
    assert "stack_unbind_identity" in text and "stack_delete_storage_class" in text
    assert "confirm" in text and "ASSUME_YES" in text


# ---------------------------------------------------------------------------
# The in-cluster site example
# ---------------------------------------------------------------------------


def test_the_k8s_site_example_refuses_its_own_placeholders(tmp_path: Path) -> None:
    copied = tmp_path / "site.yaml"
    copied.write_text(SITE_K8S_EXAMPLE.read_text())
    with pytest.raises(ValueError, match="YOUR_"):
        load_site(copied)


def test_the_k8s_site_example_loads_once_every_placeholder_is_filled(tmp_path: Path) -> None:
    """The catalog warehouse is a name; physical locations come from site.warehouse."""
    text = SITE_K8S_EXAMPLE.read_text()
    in_values = set(re.findall(r"YOUR_[A-Z_]+", yaml.safe_dump(yaml.safe_load(text))))
    assert in_values == set(SITE_K8S_FILLINGS), "the example's placeholders and the ones filled here have drifted"
    for placeholder, value in SITE_K8S_FILLINGS.items():
        text = text.replace(placeholder, value)
    copied = tmp_path / "site.yaml"
    copied.write_text(text)

    site = load_site(copied)
    assert site.corpus_root == "s3://a-bucket/corpus"
    assert site.runs_root == "s3://a-bucket/runs"
    assert site.warehouse == "s3://a-bucket/warehouse"
    assert site.kafka_bootstrap == "ingest-bench-kafka-bootstrap.ingest-bench.svc:9092"
    assert site.kafka_security == {}
    assert site.schema_registry is None
    assert site.catalog_props == {
        "uri": "http://lakekeeper.ingest-bench.svc:8181/catalog",
        "warehouse": "ingest-bench",
        "s3.region": "eu-west-1",
    }
    assert site.kubernetes == KubernetesConfig(
        context="a-cluster",
        namespace="ingest-bench",
        harness_service_account="ingest-bench-harness",
        flink_service_account="ingest-bench-flink",
        spark_service_account="ingest-bench-spark",
        service_account_annotations={},
        registry="123456789012.dkr.ecr.eu-west-1.amazonaws.com",
        aws_region="eu-west-1",
        secret_name=None,
        node_selector={},
        tolerations=[],
    )


def test_the_k8s_site_example_names_what_setup_prints() -> None:
    example = _mapping(yaml.safe_load(SITE_K8S_EXAMPLE.read_text()))
    setup = STACK_SETUP.read_text().replace("$KAFKA_NAME", "ingest-bench").replace("$NAMESPACE", "ingest-bench")
    assert str(_mapping(example["kafka"])["bootstrap_servers"]) in setup
    props = _mapping(_mapping(example["catalog"])["props"])
    assert str(props["uri"]) in setup
    assert f"catalog.props.warehouse:        {props['warehouse']}" in setup.replace("$WAREHOUSE_NAME", "ingest-bench")


@needs_shell_tools
def test_generation_expands_only_the_shard_index(tmp_path: Path) -> None:
    run = _sharded_generation(tmp_path, {})
    assert run.result.returncode == 0, run.result.stderr
    job = _named_job(run, f"corpus-gen-{SHARDED_PRESET}")
    for shard in (0, 1):
        argv = _job_argv(job, shard)
        assert argv[0] == "gen-corpus"
        assert argv[argv.index("--out") + 1] == f"{CORPUS_ROOT}/shards/{shard}"
        assert argv[argv.index("--shard-index") + 1] == str(shard)
        assert argv[argv.index("--shard-count") + 1] == "2"


@needs_shell_tools
@pytest.mark.parametrize("security", [True, False], ids=["quoted-properties", "empty-properties-system-bash"])
def test_launch_preserves_configured_values_as_single_arguments(tmp_path: Path, security: bool) -> None:
    value = "spaces 'quotes' \"double\" \\ slash\nnewline $(HOME) ${env:PASSWORD}"
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(
        json.dumps(
            {
                **FACTS,
                "corpus_uri": f"s3://a-bucket/{value}",
                "value_encoding": "confluent",
                "schema_id": 42,
            }
        )
    )
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    spec["table"]["managed_by"] = "engine"
    spec["producer"]["shards"] = 2
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    site = yaml.safe_load(_filled_site())
    if security:
        site["kafka"]["security"]["sasl.password"] = value
    else:
        site["kafka"].pop("security")
        (tmp_path / "bin").mkdir()
        (tmp_path / "bin" / "bash").symlink_to("/bin/bash")
    site["catalog"]["props"]["token"] = value
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {}, site=yaml.safe_dump(site))
    assert run.result.returncode == 0, run.result.stderr
    scorer = _job_argv(run.applied[0])
    assert scorer[0] == "score"
    assert scorer[scorer.index("--table-managed-by") + 1] == "engine"
    assert f"token={value}" in scorer
    for shard in (0, 1):
        producer = _job_argv(run.applied[1], shard)
        assert producer[0] == "produce"
        assert producer[producer.index("--corpus") + 1] == f"s3://a-bucket/{value}"
        assert producer[producer.index("--shard") + 1] == str(shard)
        assert producer[producer.index("--publish-log") + 1] == f"/work/publish_log-{shard}.jsonl"
        assert producer[producer.index("--value-encoding") + 1] == "confluent"
        assert producer[producer.index("--schema-id") + 1] == "42"
        if security:
            assert f"sasl.password={value}" in producer
        else:
            assert "--kafka-prop" not in producer


@needs_bash
def test_port_forward_counts_probe_time_and_caps_request_timeout(tmp_path: Path) -> None:
    calls = tmp_path / "curl.log"
    program = f"""
        set -euo pipefail
        source "{K8S_LIB}"
        KUBE_CONTEXT=test SITE_NAMESPACE=test K8S_PORT_FORWARD_WAIT_S=1
        log() {{ :; }}
        die() {{ printf '%s\\n' "$*" >&2; exit 3; }}
        kubectl() {{ printf 'Forwarding from 127.0.0.1:1234 -> 80\\n'; exec /bin/sleep 10; }}
        curl() {{ printf '%s\\n' "$*" >>"{calls}"; SECONDS=$((SECONDS + 2)); return 1; }}
        sleep() {{ :; }}
        trap k8s_port_forward_stop EXIT
        k8s_port_forward svc/test 1234:80
    """
    out = subprocess.run(["bash", "-c", program], capture_output=True, text=True, timeout=5)
    assert out.returncode == 3, out.stderr
    assert "within 1s" in out.stderr
    assert "--max-time 1" in calls.read_text()


@needs_bash
def test_catalog_tunnel_uses_the_ephemeral_port_assigned_by_kubectl(tmp_path: Path) -> None:
    calls = tmp_path / "calls.log"
    program = f"""
        set -euo pipefail
        unset CATALOG_FORWARD_PORT
        source "{K8S_LIB}"
        KUBE_CONTEXT=test SITE_NAMESPACE=test K8S_PORT_FORWARD_WAIT_S=2
        log() {{ :; }}
        die() {{ printf '%s\\n' "$*" >&2; exit 3; }}
        kubectl() {{
            printf '%s\\n' "$*" >>"{calls}"
            printf 'Forwarding from 127.0.0.1:43210 -> 8181\\n' >&2
            exec /bin/sleep 10
        }}
        curl() {{ printf '%s\\n' "$*" >>"{calls}"; return 0; }}
        trap k8s_port_forward_stop EXIT
        k8s_reach_catalog http://lakekeeper.test.svc:8181/catalog
        printf '%s\\n' "$CATALOG_URI"
    """
    out = subprocess.run(["bash", "-c", program], capture_output=True, text=True, timeout=5)
    assert out.returncode == 0, out.stderr
    assert out.stdout.strip() == "http://localhost:43210/catalog"
    logged = calls.read_text()
    assert "port-forward svc/lakekeeper :8181" in logged
    assert "http://localhost:43210/health" in logged


@needs_bash
def test_port_forward_rejects_a_probe_answered_after_the_tunnel_exits(tmp_path: Path) -> None:
    program = f"""
        set -euo pipefail
        source "{K8S_LIB}"
        KUBE_CONTEXT=test SITE_NAMESPACE=test K8S_PORT_FORWARD_WAIT_S=2
        log() {{ :; }}
        die() {{ printf '%s\\n' "$*" >&2; exit 3; }}
        kubectl() {{ printf 'unable to listen on port 18181\\n' >&2; return 1; }}
        curl() {{ /bin/sleep 0.1; return 0; }}
        trap k8s_port_forward_stop EXIT
        k8s_port_forward svc/test 18181:8181
    """
    out = subprocess.run(["bash", "-c", program], capture_output=True, text=True, timeout=5)
    assert out.returncode == 3, out.stderr
    assert "unable to listen on port 18181" in out.stderr
    assert "port-forward svc/test 18181:8181 exited" in out.stderr


@needs_bash
def test_port_forward_does_not_start_an_unbounded_probe_at_the_deadline(tmp_path: Path) -> None:
    calls = tmp_path / "curl.log"
    program = f"""
        set -euo pipefail
        source "{K8S_LIB}"
        KUBE_CONTEXT=test SITE_NAMESPACE=test K8S_PORT_FORWARD_WAIT_S=1
        log() {{ :; }}
        die() {{ printf '%s\\n' "$*" >&2; exit 3; }}
        kubectl() {{ printf 'Forwarding from 127.0.0.1:43210 -> 8181\\n'; exec /bin/sleep 10; }}
        awk() {{ /bin/sleep 2; printf '43210\\n'; }}
        curl() {{ printf '%s\\n' "$*" >>"{calls}"; return 0; }}
        trap k8s_port_forward_stop EXIT
        k8s_port_forward svc/test :8181
    """
    out = subprocess.run(["bash", "-c", program], capture_output=True, text=True, timeout=5)
    assert out.returncode == 3, out.stderr
    assert "within 1s" in out.stderr
    assert not calls.exists()


@needs_shell_tools
def test_run_deadline_includes_time_spent_in_gate(tmp_path: Path) -> None:
    run = _run_chained(tmp_path, [], ["running"], {"RUN_MAX_S": "3", "STUB_GATE_DELAY_S": "2"})
    assert run.result.returncode == 1, run.result.stderr
    assert run.drivers() == ["stage", "launch", "gate", "teardown", "finish"]
    assert "still running after 3s" in run.result.stderr


@needs_shell_tools
@pytest.mark.parametrize("machine_type", ["", MACHINE_TYPE_UNSPECIFIED, f"{PLACEHOLDER}MACHINE_TYPE"])
@pytest.mark.parametrize("publish_invalid", [False, True])
def test_finish_rejects_missing_machine_type_before_publishing(
    tmp_path: Path, machine_type: str, publish_invalid: bool
) -> None:
    run_dir = build_run_dir(tmp_path / "work", run_id=RUN_ID, run_valid=not publish_invalid)
    spec_path = run_dir / "spec.yaml"
    spec = yaml.safe_load(spec_path.read_text())
    spec["flink"]["machine_type"] = machine_type
    spec_path.write_text(yaml.safe_dump(spec))
    run = _run_driver(
        FINISH,
        [RUN_ID, "--publish", "results", *(["--publish-invalid"] if publish_invalid else [])],
        tmp_path,
        {
            **_finish_environment(tmp_path),
            "STUB_S3_CP_ABSENT": "corpus.json",
        },
        site=write_site(tmp_path).read_text(),
        programs={**FINISH_PROGRAMS, "collect": REAL_COLLECT_STUB},
    )
    assert run.result.returncode != 0
    assert "machine_type" in run.result.stderr
    assert not (tmp_path / "work" / "results").exists()
    assert len((tmp_path / "collect.log").read_text().splitlines()) == 1


@needs_shell_tools
def test_finish_keeps_unpublishable_smoke_results_locally(tmp_path: Path) -> None:
    _torn_down_run(tmp_path)
    run = _run_driver(
        FINISH,
        [RUN_ID],
        tmp_path,
        {
            **_finish_environment(tmp_path),
            "STUB_COLLECT_FLEET": '[{"role":"taskmanager","machine_type":"unspecified"}]',
        },
        programs=FINISH_PROGRAMS,
    )
    assert run.result.returncode == 0, run.result.stderr
    assert (tmp_path / "work" / "runs" / RUN_ID / "run.json").exists()


# ---------------------------------------------------------------------------
# The compaction sidecar
# ---------------------------------------------------------------------------


def test_the_shell_and_the_registry_name_the_sidecars_files_the_same_way() -> None:
    text = K8S_LIB.read_text()
    assert f"COMPACTOR_DOCUMENT_FILE={compactors.DOCUMENT_FILE}" in text
    assert f"COMPACTOR_CONFIGMAP_FILE={compactors.CONFIGMAP_FILE}" in text


def _declare_compactor(run_dir: Path) -> None:
    spec_path = run_dir / "spec.yaml"
    spec = _mapping(yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text()))
    spec["compaction"] = {
        "driver": "spark-rewrite",
        "interval_s": 15,
        "spark-rewrite": {"executors": 1, "executor_cores": 2, "executor_mem_mb": 2048},
    }
    spec_path.write_text(yaml.safe_dump(spec))
    (run_dir / compactors.CONFIGMAP_FILE).write_text("# compactor configmap\n")
    (run_dir / compactors.DOCUMENT_FILE).write_text(f"metadata:\n  name: {RUN_OBJECT}-compact\n")


@needs_shell_tools
@pytest.mark.parametrize("selector", [None, "compactor.example/instance=<name>"])
def test_stage_starts_the_sidecar_before_the_engine_it_runs_beside(tmp_path: Path, selector: str | None) -> None:
    """A rendered ConfigMap is the whole signal: no driver status is polled."""
    staged = _staged_engine_run(tmp_path)
    _declare_compactor(staged)
    programs = {"curl": CURL_STUB, "verify-flink": VERIFY_STUB}
    if selector is not None:
        programs["engine-k8s"] = (
            "if [[ $1 == --compaction ]]; then\n"
            "  [[ $2 == spark-rewrite && $3 == fleet_selector ]] || exit 2\n"
            f"  printf '%s\\n' {shlex.quote(selector)}\n"
            "else\n"
            f'  exec {shlex.quote(sys.executable)} -m ingest_bench.k8s.engine "$@"\n'
            "fi\n"
        )

    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {**_verify_environment(tmp_path), "STUB_STAGE_DIR": str(staged), "STUB_PODS": '{"items":[]}'},
        programs=programs,
    )
    assert run.result.returncode == 0, run.result.stderr

    applied = [line.rsplit("/", 1)[-1] for line in run.calls.splitlines() if " apply -f ./runs/" in line]
    # The sidecar's ConfigMap precedes the document that mounts it, and both
    # precede the engine's, so compaction is running before ingest starts.
    assert applied == [
        compactors.CONFIGMAP_FILE,
        compactors.DOCUMENT_FILE,
        "flink-job-configmap.yaml",
        "flinkdeployment.yaml",
    ]
    label = "sparkoperator.k8s.io/app-name" if selector is None else "compactor.example/instance"
    assert f"get pods -l {label}={RUN_OBJECT}-compact" in run.calls
    assert (tmp_path / "work" / "runs" / RUN_ID / "compactor-pods.json").exists()
    assert f"s3://a-bucket/runs/{RUN_ID}/stage/compactor-pods.json" in run.aws_calls


@needs_shell_tools
def test_stage_refuses_to_launch_with_an_incomplete_compactor_fleet(tmp_path: Path) -> None:
    staged = _staged_engine_run(tmp_path)
    _declare_compactor(staged)
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {
            **_verify_environment(tmp_path),
            "STUB_STAGE_DIR": str(staged),
            "STUB_COMPACTOR_FLEET_STATUS": "2",
            "ENGINE_RUNNING_WAIT_S": "0",
        },
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode != 0
    assert "compactor" in run.result.stderr and "fleet" in run.result.stderr
    assert "run_id:" not in run.result.stdout


@needs_shell_tools
def test_stage_starts_no_sidecar_for_a_run_that_declared_none(tmp_path: Path) -> None:
    run = _run_driver(
        STAGE,
        [str(REPO_ROOT / "specs/smoke/smoke-flink.yaml"), "--image-tag", "abc1234"],
        tmp_path,
        {**_verify_environment(tmp_path), "STUB_STAGE_DIR": str(_staged_engine_run(tmp_path))},
        programs={"curl": CURL_STUB, "verify-flink": VERIFY_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr
    assert compactors.DOCUMENT_FILE not in run.calls


@needs_shell_tools
@pytest.mark.parametrize("fleet_status", [0, 2])
def test_teardown_stops_the_sidecar_before_its_configmap(tmp_path: Path, fleet_status: int) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    (run_dir / "spec.yaml").write_text((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    _declare_compactor(run_dir)

    run = _run_driver(
        TEARDOWN,
        [RUN_ID, "--image-tag", "abc1234"],
        tmp_path,
        {
            **_stub_logs(tmp_path, {"STUB_METADATA_LOG": "metadata.log", "STUB_COLLECT_LOG": "collect.log"}),
            "STUB_COMPACTOR_FLEET_STATUS": str(fleet_status),
        },
        programs={"table-metadata": TABLE_METADATA_STUB, "collect": COLLECT_STUB},
    )
    assert run.result.returncode == 0, run.result.stderr

    deleted = [line.rsplit("/", 1)[-1].split()[0] for line in run.calls.splitlines() if " delete -f ./runs/" in line]
    assert deleted == [compactors.DOCUMENT_FILE, compactors.CONFIGMAP_FILE]
    assert run.calls.index("get pods -l sparkoperator.k8s.io/app-name=") < run.calls.index("delete -f ./runs/")
    assert (run_dir / "compactor-pods.json").exists() is (fleet_status == 0)


@needs_shell_tools
@pytest.mark.parametrize("mode", [None, "saturation"])
def test_launch_forwards_saturation_configuration_only_when_selected(tmp_path: Path, mode: str | None) -> None:
    run_dir = tmp_path / "work" / "runs" / RUN_ID
    run_dir.mkdir(parents=True)
    (run_dir / "facts.json").write_text(json.dumps(FACTS))
    spec = yaml.safe_load((REPO_ROOT / "specs/smoke/smoke-flink.yaml").read_text())
    if mode is not None:
        spec["scoring"].update(mode=mode, measurement_s=600)
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    (run_dir / "timeline.log").write_text("2026-09-08T12:00:00Z staged\n")
    run = _run_driver(LAUNCH, [RUN_ID, "--image-tag", "abc1234"], tmp_path, {})
    assert run.result.returncode == 0, run.result.stderr
    command = _job_command(run.applied[0])
    if mode is None:
        assert "--mode" not in command and "--measurement-s" not in command
    else:
        assert "--mode saturation" in command and "--measurement-s 600" in command


@needs_shell_tools
@pytest.mark.parametrize("engine", ["flink", "spark"])
@pytest.mark.parametrize("encoding", ["avro", "json"])
def test_smoke_generates_the_corpus_selected_by_the_spec(tmp_path: Path, engine: str, encoding: str) -> None:
    binary = tmp_path / "bin"
    binary.mkdir()
    calls = tmp_path / "docker.log"
    docker = binary / "docker"
    docker.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$STUB_LOG"\ncase "$*" in *"gen-corpus "*) exit 73 ;; esac\n'
    )
    docker.chmod(0o755)
    suffix = "-json" if encoding == "json" else ""
    spec = REPO_ROOT / "specs" / "smoke" / f"smoke-{engine}{suffix}.yaml"
    result = subprocess.run(
        ["bash", str(SMOKE), "--engine", engine, "--spec", str(spec), "--set", "duration_s=2"],
        env={**os.environ, "PATH": f"{binary}:{os.environ['PATH']}", "STUB_LOG": str(calls)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    # Stop at generation so no broker, engine, scorer, or run directory is created.
    assert result.returncode == 73, result.stdout + result.stderr
    generated = [line for line in calls.read_text().splitlines() if "gen-corpus " in line]
    assert len(generated) == 1
    assert f"gen-corpus --preset smoke{suffix} --set duration_s=2 --out s3://corpus --seed 1" in generated[0]


@needs_shell_tools
def test_smoke_generates_the_corpus_of_the_track_a_spec_names(tmp_path: Path) -> None:
    binary = tmp_path / "bin"
    binary.mkdir()
    calls = tmp_path / "docker.log"
    docker = binary / "docker"
    docker.write_text(
        '#!/usr/bin/env bash\nprintf "%s\\n" "$*" >> "$STUB_LOG"\ncase "$*" in *"gen-corpus "*) exit 73 ;; esac\n'
    )
    docker.chmod(0o755)
    spec = REPO_ROOT / "specs" / "tracks" / "events-1mrps-skew" / "aws-1mrps-skew-flink-ref.yaml"
    result = subprocess.run(
        ["bash", str(SMOKE), "--engine", "flink", "--spec", str(spec)],
        env={**os.environ, "PATH": f"{binary}:{os.environ['PATH']}", "STUB_LOG": str(calls)},
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 73, result.stdout + result.stderr
    generated = [line for line in calls.read_text().splitlines() if "gen-corpus " in line]
    assert len(generated) == 1 and "gen-corpus --preset events-1mrps-skew " in generated[0]


@needs_shell_tools
@pytest.mark.parametrize(
    "metadata_state",
    ["valid", "room", "missing", "empty", "listing-error", "wrong-encoding", "ambiguous", "shard", "bad-size"],
)
def test_json_preflight_uses_generated_bytes_and_fails_closed(tmp_path: Path, metadata_state: str) -> None:
    name = "custom-json"
    repo = tmp_path / "repo"
    scripts = repo / "scripts"
    scripts.mkdir(parents=True)
    for filename in ("preflight-kafka.sh", "_lib.sh", "_k8s.sh"):
        shutil.copy2(SCRIPTS / filename, scripts / filename)
    presets = repo / "workloads" / "presets"
    presets.mkdir(parents=True)
    custom = yaml.safe_load((REPO_ROOT / "workloads/presets/events-1mrps-uniform.yaml").read_text())
    custom.update(value_encoding="json", sizing_encoding="avro")
    (presets / f"{name}.yaml").write_text(yaml.safe_dump(custom))
    published = tmp_path / "published"
    published.mkdir()
    meta = {
        "name": name,
        "value_encoding": "json",
        "shard_count": 1,
        "duration_s": 3600,
        "encoded_bytes": 850_000_000_000,
    }
    if metadata_state == "wrong-encoding":
        meta["value_encoding"] = "avro"
    if metadata_state == "shard":
        meta["shard_count"] = 2
    if metadata_state == "bad-size":
        meta["encoded_bytes"] = 0
    (published / "corpus.json").write_text(json.dumps(meta))
    listing = f"PRE {name}-12345678/"
    if metadata_state == "empty":
        listing = ""
    if metadata_state == "ambiguous":
        listing += f"\nPRE {name}-87654321/"
    run = _run_kafka_preflight(
        tmp_path,
        {},
        {
            "STUB_S3_LS": listing,
            "STUB_S3_CP_DIR": str(published),
            "STUB_S3_CP_ABSENT": "corpus.json" if metadata_state == "missing" else "",
            "STUB_KAFKA_AVAIL": str(1_500_000_000_000 if metadata_state == "room" else 500_000_000_000),
            "STUB_S3_LS_ABSENT": "corpus" if metadata_state == "listing-error" else "",
            "WAIT_BROKERS_S": "0",
        },
        corpus=name,
        script=scripts / "preflight-kafka.sh",
    )
    if metadata_state in {"valid", "room"}:
        assert run.result.returncode == (0 if metadata_state == "room" else 3), run.result.stderr
        assert "236111112 B/s" in run.result.stderr
        assert f"s3 cp {CORPUS_ROOT}/{name}-12345678/corpus.json" in run.aws_calls
    else:
        assert run.result.returncode == 1, run.result.stderr
        assert "get pods" not in run.calls
    assert ("brokers are clear" in run.result.stderr) == (metadata_state == "room")
    assert "s3 rm" not in run.aws_calls and "apply" not in run.calls


@needs_shell_tools
def test_json_generation_does_not_extrapolate_avro_timing(tmp_path: Path) -> None:
    name = "smoke-json"
    run = _run_driver(
        GEN_CORPUS,
        [name, "--image-tag", "abc1234"],
        tmp_path,
        {},
        job_log=f"wrote {CORPUS_ROOT}/{name}-12345678: 1000 rows, 600000 encoded bytes\n",
    )
    assert run.result.returncode == 0, run.result.stderr
    assert "JSON generation time has not been measured" in run.result.stderr
    assert "projected generation time" not in run.result.stderr


RENDER_SPARK_SCAN_STUB = """
out=""
while [[ $# -gt 0 ]]; do
    case "$1" in
    --out) out=$2; shift 2 ;;
    *) printf '%s\\n' "$1" >>"$STUB_SCAN_RENDER_LOG"; shift ;;
    esac
done
printf 'spark.sql.catalog.scan org.apache.iceberg.spark.SparkCatalog\\nspark.driver.memory 24576m\\n' >"$out"
"""

UPSERT_SCAN_PLAN_STUB = """
printf '11,22\\n'
"""

TABLE_HEALTH_STUB = """
printf '%s\\n' "$*" >>"$STUB_TABLE_HEALTH_LOG"
exit "${STUB_TABLE_HEALTH_STATUS:-0}"
"""


@needs_shell_tools
@pytest.mark.parametrize(("site_names_spark_account", "health_status"), [(True, 0), (False, 1)])
def test_finish_verifies_an_upsert_run_with_one_spark_scan_in_the_cluster(
    tmp_path: Path, site_names_spark_account: bool, health_status: int
) -> None:
    run_dir = _torn_down_run(tmp_path)
    spec = yaml.safe_load((run_dir / "spec.yaml").read_text())
    spec["scoring"] = {**spec.get("scoring", {}), "exactness": "LATEST_PER_KEY"}
    (run_dir / "spec.yaml").write_text(yaml.safe_dump(spec))
    facts = json.loads((run_dir / "facts.json").read_text())
    (run_dir / "facts.json").write_text(json.dumps({**facts, "upsert": {"key_columns": ["order_id", "partition_key"]}}))
    (run_dir / "writers-stopped").touch()
    # An earlier finish's diagnostics must not survive a measurement that fails this time.
    (run_dir / "scores/table-health.json").write_text('{"rows_per_live_row": 9.0}')
    fetched = tmp_path / "s3-objects"
    fetched.mkdir()
    (fetched / "terminal-upsert.json").write_text('{"mode":"LATEST_PER_KEY","verdict":true}')
    environment = {
        **_finish_environment(tmp_path),
        **_stub_logs(tmp_path, {"STUB_SCAN_RENDER_LOG": "scan-render.log", "STUB_TABLE_HEALTH_LOG": "health.log"}),
        "STUB_TABLE_HEALTH_STATUS": str(health_status),
        "STUB_S3_CP_DIR": str(fetched),
        "STUB_S3_UPLOAD_DIR": str(tmp_path / "uploaded"),
        "VERIFY_SPARK_CPU": "8",
        "VERIFY_SPARK_MEMORY": "32Gi",
        "VERIFY_WAIT_S": "900",
        "K8S_JOB_POLL_S": "0",
    }
    site = _filled_site()
    if not site_names_spark_account:
        site = site.replace("  spark_service_account: ingest-bench-spark\n", "")
    run = _run_driver(
        FINISH,
        [RUN_ID, "--verify-in-cluster", "--image-tag", "abc1234"],
        tmp_path,
        environment,
        programs={
            **FINISH_PROGRAMS,
            "render-spark-scan": RENDER_SPARK_SCAN_STUB,
            "upsert-scan-plan": UPSERT_SCAN_PLAN_STUB,
            "table-health": TABLE_HEALTH_STUB,
        },
        site=site,
    )
    # Table health is diagnostic: a failure to measure it is reported and does not stop the run's verdict.
    assert run.result.returncode == 0, run.result.stderr
    assert ("could not measure table health" in run.result.stderr) == (health_status != 0)
    health = (tmp_path / "health.log").read_text()
    assert f"--metadata {run_dir}/table-metadata.final.json --corpus-uri " in health
    assert f"--checkpoints s3://a-bucket/runs/{RUN_ID}/scores/checkpoints --out {run_dir}/scores" in health
    jobs = {cast(dict[str, str], job["metadata"])["name"]: job for job in run.applied if job["kind"] == "Job"}
    assert set(jobs) == {f"spark-scan-{RUN_OBJECT}", f"verify-upsert-{RUN_OBJECT}"}
    scan = jobs[f"spark-scan-{RUN_OBJECT}"]
    # The site model's default account applies when the site omits it.
    assert _pod_spec(scan)["serviceAccountName"] == "ingest-bench-spark"
    # A scan that outlives finish.sh's wait must not keep its CPUs: the Job stops at the same deadline.
    assert cast(dict[str, object], scan["spec"])["activeDeadlineSeconds"] == 900
    container = cast(dict[str, object], cast(list[object], _pod_spec(scan)["containers"])[0])
    assert "spark:abc1234" in str(container["image"])
    resources = cast(dict[str, dict[str, str]], container["resources"])
    assert resources["requests"] == {"cpu": "8", "memory": "32Gi"} == resources["limits"]
    command = _job_command(scan)
    final_uri = f"s3://a-bucket/runs/{RUN_ID}/table-metadata.final.json"
    assert "/opt/spark/bin/spark-submit --master local[8] --driver-memory 24576m" in command
    assert "--conf spark.sql.catalog.scan=org.apache.iceberg.spark.SparkCatalog" in command
    assert (
        f"/opt/bench/engines/spark/upsert_scan.py --metadata {final_uri} --snapshots 11,22 --key-column order_id --out"
        in command
    )
    assert not (run_dir / "scores/table-health.json").exists()
    # A verify Job that fails before uploading must not leave an earlier finish's verdict to fetch, and an
    # earlier verifier still running is stopped before the clear so it cannot upload after it.
    assert f"s3 rm s3://a-bucket/runs/{RUN_ID}/scores/terminal-upsert.json --only-show-errors" in run.aws_calls
    assert f"delete job verify-upsert-{RUN_OBJECT} --ignore-not-found --cascade=foreground --wait=true" in run.calls
    attempt = command.split("--out ", 1)[1].split()[0]
    assert attempt.startswith(f"s3://a-bucket/runs/{RUN_ID}/scores/live/")
    assert "--cores 8" in (tmp_path / "scan-render.log").read_text().replace("\n", " ")
    verify = _job_command(jobs[f"verify-upsert-{RUN_OBJECT}"])
    assert f"verify-upsert --metadata {final_uri}" in verify
    assert f"--live {attempt}" in verify and "--reader none" in verify
    assert (run_dir / "scores/terminal-upsert.json").exists()
    timing = json.loads((run_dir / "scores/terminal-validation.json").read_text())
    assert timing["mode"] == "cluster" and timing["scan"] == {"cpu": "8", "memory": "32Gi"}
