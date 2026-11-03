# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Exercise local launch control flow with Docker calls stubbed."""

import json
import os
import shlex
import shutil
import subprocess
import sys
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


@pytest.mark.skipif(
    any(shutil.which(tool) is None for tool in ("bash", "jq", "yq", "curl")),
    reason="local launch requires bash, jq, yq, and curl",
)
@pytest.mark.parametrize("engine", ["managed", "external"])
@pytest.mark.parametrize("epoch_write_fails", [False, True])
def test_local_launch_persists_the_epoch_passed_to_producer_and_scorer(
    tmp_path: Path, engine: str, epoch_write_fails: bool
) -> None:
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    for name in ("smoke.sh", "_lib.sh"):
        shutil.copy(REPO_ROOT / "scripts" / name, scripts / name)
    runs = tmp_path / "runs"
    runs.mkdir()
    spec = tmp_path / "specs" / "smoke" / f"smoke-{engine}.yaml"
    spec.parent.mkdir(parents=True)
    spec.write_text("corpus: smoke\nproducer:\n  shards: 1\n")
    managed = tmp_path / "engines" / "managed"
    managed.mkdir(parents=True)
    (managed / "compose.yaml").write_text("services:\n  test:\n    profiles: [managed]\n")
    (managed / "compose.sh").write_text(
        "engine_compose_build() { :; }\n"
        "engine_compose_start() { :; }\n"
        "engine_compose_ready() { :; }\n"
        "engine_compose_logs() { :; }\n"
    )
    ready = tmp_path / "ready"
    ready.touch()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    if epoch_write_fails:
        jq = bin_dir / "jq"
        jq.write_text(
            "#!/usr/bin/env bash\n"
            "if [[ $1 == --argjson && $2 == epoch ]]; then echo partial; exit 7; fi\n"
            f'exec {shlex.quote(str(shutil.which("jq")))} "$@"\n'
        )
        jq.chmod(0o755)
    docker = bin_dir / "docker"
    docker.write_text(
        f"#!{sys.executable}\n"
        + """import json
import os
import sys
from pathlib import Path

root = Path(os.environ["TEST_ROOT"])
run = root / "runs" / "test-run"
command = sys.argv[-1]
if command.startswith("stage "):
    run.mkdir()
    # Staging copies the spec mounted at /spec.yaml into the run directory.
    mount = next(arg for arg in sys.argv if arg.endswith(":/spec.yaml:ro"))
    (run / "spec.yaml").write_text(Path(mount.rsplit(":/spec.yaml", 1)[0]).read_text())
    (run / "facts.json").write_text(json.dumps({
        "epoch": None, "bootstrap": "kafka:9092", "corpus_uri": "s3://corpus/test",
        "table": "bench.test", "run_id": "test-run",
    }))
    (run / "scores").mkdir()
    (run / "scores" / "summary.json").write_text('{"run_valid": true}')
    print("run_id: test-run")
elif command.startswith(("score ", "produce ")):
    with (root / "launches.jsonl").open("a") as output:
        output.write(json.dumps({"command": command, "facts": json.loads((run / "facts.json").read_text())}) + "\\n")
elif sys.argv[1] == "wait":
    print("0")
"""
    )
    docker.chmod(0o755)
    args = ["bash", str(scripts / "smoke.sh"), "--engine", engine, "--keep"]
    if engine == "external":
        args += ["--external-ready-file", str(ready)]
    lead_s = 30
    before = int(time.time())
    result = subprocess.run(
        args,
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "TEST_ROOT": str(tmp_path),
            "EPOCH_LEAD_S": str(lead_s),
        },
        text=True,
        capture_output=True,
        timeout=30,
    )
    after = int(time.time())
    if epoch_write_fails:
        assert result.returncode == 7, result.stderr
        facts = json.loads((runs / "test-run" / "facts.json").read_text())
        assert facts["epoch"] is None
        assert facts["run_id"] == "test-run"
        assert not (tmp_path / "launches.jsonl").exists()
        assert not list((runs / "test-run").glob("facts.json.*"))
        return
    assert result.returncode == 0, result.stdout + result.stderr
    facts = json.loads((runs / "test-run" / "facts.json").read_text())
    assert before + lead_s <= facts["epoch"] <= after + lead_s
    assert facts["run_id"] == "test-run"
    launches = [json.loads(line) for line in (tmp_path / "launches.jsonl").read_text().splitlines()]
    assert [shlex.split(launch["command"])[0] for launch in launches] == ["score", "produce"]
    for launch in launches:
        command = shlex.split(launch["command"])
        assert int(command[command.index("--epoch") + 1]) == facts["epoch"]
        assert launch["facts"] == facts, "facts must be updated before either process starts"
    assert not list((runs / "test-run").glob("facts.json.*"))


@pytest.mark.skipif(shutil.which("jq") is None, reason="requires jq")
@pytest.mark.parametrize("failed_command", ["jq", "mv"])
def test_epoch_write_failure_preserves_facts_and_caller_cleanup(tmp_path: Path, failed_command: str) -> None:
    facts = tmp_path / "facts.json"
    original = '{"epoch": null, "run_id": "test-run"}\n'
    facts.write_text(original)
    cleanup = tmp_path / "caller-cleanup"
    result = subprocess.run(
        [
            "bash",
            "-c",
            f"""
            set -euo pipefail
            source {shlex.quote(str(REPO_ROOT / "scripts" / "_lib.sh"))}
            cleanup={shlex.quote(str(cleanup))}
            trap 'touch "$cleanup"' EXIT
            {failed_command}() {{ echo partial; return 7; }}
            write_launch_epoch {shlex.quote(str(facts))} 123
            """,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 7, result.stderr
    assert facts.read_text() == original
    assert cleanup.exists()
    assert not list(tmp_path.glob("facts.json.*"))
