# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Compare the running Flink job with the requested configuration.

RUNNING alone does not prove that the engine applied every setting. Read the
checkpoint cadence, exactly-once mode, operator parallelism, and TaskManager
count from the JobManager REST API. Injected JSON responses keep verification
testable without a cluster.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from engines.flink.knobs import SOURCE_TABLE, Knobs, read, uses_v2_sink
from ingest_bench.readings import (
    DRIFT_EXIT,
    UNVERIFIED_EXIT,
    document,
    documents,
    fetch_json,
    field,
    int_field,
    line,
    str_field,
)
from ingest_bench.specs.model import RunSpec, load_run_spec

# Job-independent endpoints; build job-specific paths where they are read.
JOBS_OVERVIEW = "/jobs/overview"
CLUSTER_OVERVIEW = "/overview"

RUNNING = "RUNNING"

# Use an explicit state for a job absent from the cluster's listing.
NOT_FOUND = "not found"

# The endpoint's spelling of the mode `render_conf` submits as `EXACTLY_ONCE`.
EXACTLY_ONCE = "exactly_once"

# Match graph operator names despite chaining and generated suffixes.
_SOURCE_PREFIX = f"Source: {SOURCE_TABLE}"
# The v1 FlinkSink names its operators; under the SinkV2 sink Flink names the
# framework's operators "<table>: Writer" and "<table>: Committer", and Iceberg its
# own single-subtask hop "<table>: Sink pre-commit aggregator: <table>".
_WRITER = "IcebergStreamWriter"
_COMMITTER = "IcebergFilesCommitter"
_V2_WRITER = ": Writer"
_V2_COMMITTER = ": Committer"
_V2_AGGREGATOR = "Sink pre-commit aggregator"
# Iceberg's table maintenance operators, named "<base>[<task index>]"; only the
# rewrite runner carries the configured parallelism.
_MAINTENANCE_RUNNER = "Rewrite["
_MAINTENANCE_SINGLE = ("Trigger manager", "RDF Planner[", "Rewrite commit[", "Lock remover")

# Iceberg commits are serialized through a single committer. Under the SinkV2
# sink the committer vertex runs at the writer parallelism and the single commit
# path is the pre-commit aggregator, which Iceberg forces to one subtask.
COMMITTER_PARALLELISM = 1

# Explicit configuration overrides take precedence over the corresponding knobs.
_INTERVAL_KEY = "execution.checkpointing.interval"
_MIN_PAUSE_KEY = "execution.checkpointing.min-pause"

# Match supported Flink duration units; a missing unit means milliseconds.
# Reject unsupported units instead of comparing values at the wrong scale.
_DURATION_RE = re.compile(r"^(\d+)\s*([a-z]*)$")
_UNIT_MS = {"": 1, "ms": 1, "s": 1_000, "m": 60_000, "h": 3_600_000}


def duration_ms(value: str, where: str) -> int:
    """Parse a Flink duration in milliseconds, naming ``where`` on failure."""
    match = _DURATION_RE.match(value.strip().lower())
    if match is None or match.group(2) not in _UNIT_MS:
        raise ValueError(
            f"{where} is {value!r}, which is not a duration this can read: write a whole number of "
            "milliseconds, or one suffixed ms, s, m or h"
        )
    return int(match.group(1)) * _UNIT_MS[match.group(2)]


# ---------------------------------------------------------------------------
# The readings
# ---------------------------------------------------------------------------


def _effective(knobs: Knobs, key: str, knob: str) -> str:
    """Return the submitted setting, including extra_flink_conf overrides."""
    return knobs.extra_flink_conf.get(key, knob)


def _job(run_id: str, overview: object) -> tuple[str, str]:
    """Return the run's state and the ID of its running attempt.

    Restarted jobs can share a name with archived attempts. Return an empty ID
    unless a RUNNING attempt exists.
    """
    jobs = documents(field(document(overview, JOBS_OVERVIEW), "jobs", JOBS_OVERVIEW), f"{JOBS_OVERVIEW}.jobs")
    named = [job for job in jobs if str_field(job, "name", JOBS_OVERVIEW) == run_id]
    if not named:
        return NOT_FOUND, ""
    for job in named:
        if str_field(job, "state", JOBS_OVERVIEW) == RUNNING:
            return RUNNING, str_field(job, "jid", JOBS_OVERVIEW)
    return str_field(named[0], "state", JOBS_OVERVIEW), ""


def _checkpoint_drift(knobs: Knobs, fetch: Callable[[str], object], jid: str) -> list[str]:
    where = f"/jobs/{jid}/checkpoints/config"
    config = document(fetch(where), where)
    durations: tuple[tuple[str, str, str, str], ...] = (
        ("checkpoint interval ms", _INTERVAL_KEY, knobs.checkpoint_interval, "interval"),
        ("checkpoint min pause ms", _MIN_PAUSE_KEY, knobs.min_pause, "min_pause"),
    )
    lines: list[str] = []
    for what, key, knob, reported in durations:
        expected = duration_ms(_effective(knobs, key, knob), key)
        actual = int_field(config, reported, where)
        if expected != actual:
            lines.append(line(what, expected, actual))
    # Always require exactly-once mode, including when extra_flink_conf overrides it.
    mode = str_field(config, "mode", where)
    if mode != EXACTLY_ONCE:
        lines.append(line("checkpoint mode", EXACTLY_ONCE, mode))
    return lines


def _contains(base: str) -> Callable[[str], bool]:
    return lambda name: base in name


def _vertex_drift(knobs: Knobs, v2_sink: bool, fetch: Callable[[str], object], jid: str) -> list[str]:
    """Report graph parallelism that differs from the requested reader/writer counts.

    Writer parallelism comes from the SQL hint derived from fleet knobs; the
    maintenance vertices come from the same hint when the spec asks for them.
    """
    where = f"/jobs/{jid}"
    vertices = documents(field(document(fetch(where), where), "vertices", where), f"{where}.vertices")
    writers = knobs.writer_parallelism()
    roles: list[tuple[str, Callable[[str], bool], int]] = [
        ("source", lambda name: name.startswith(_SOURCE_PREFIX), knobs.parallelism_default()),
    ]
    if v2_sink:
        roles += [
            ("writer", lambda name: _V2_WRITER in name, writers),
            ("committer", lambda name: _V2_COMMITTER in name, writers),
            ("pre-commit aggregator", lambda name: _V2_AGGREGATOR in name, COMMITTER_PARALLELISM),
        ]
    else:
        roles += [
            ("writer", lambda name: _WRITER in name, writers),
            ("committer", lambda name: _COMMITTER in name, COMMITTER_PARALLELISM),
        ]
    if knobs.maintenance is not None:
        # A hint that did not take effect leaves no maintenance vertices at all.
        roles.append(
            ("maintenance runner", lambda name: _MAINTENANCE_RUNNER in name, knobs.maintenance.rewrite_parallelism)
        )
        roles.extend((f"maintenance {base.rstrip('[').lower()}", _contains(base), 1) for base in _MAINTENANCE_SINGLE)
    lines: list[str] = []
    for label, matches, expected in roles:
        matched = [vertex for vertex in vertices if matches(str_field(vertex, "name", where))]
        if not matched:
            # Report a missing role before attempting to inspect its parallelism.
            lines.append(line(f"{label} vertices", "at least 1", 0))
            continue
        for vertex in matched:
            actual = int_field(vertex, "parallelism", where)
            if actual != expected:
                lines.append(line(f"{label} vertex parallelism", expected, actual))
    return lines


def _fleet_drift(knobs: Knobs, fetch: Callable[[str], object]) -> list[str]:
    """Compare the registered TaskManager count with the requested fleet size."""
    overview = document(fetch(CLUSTER_OVERVIEW), CLUSTER_OVERVIEW)
    taskmanagers = int_field(overview, "taskmanagers", CLUSTER_OVERVIEW)
    if taskmanagers == knobs.taskmanagers:
        return []
    return [line("taskmanagers", knobs.taskmanagers, taskmanagers)]


def verify(spec: RunSpec, run_id: str, fetch: Callable[[str], object]) -> list[str]:
    """Return one line per configuration mismatch, or an empty list.

    ``fetch`` maps REST paths to parsed JSON. Malformed responses raise instead
    of producing a successful verdict.
    """
    knobs = read(spec.engine_block)
    state, jid = _job(run_id, fetch(JOBS_OVERVIEW))
    if state != RUNNING:
        # A stopped job's graph describes a past attempt, not a runnable fleet.
        return [line("job state", RUNNING, state)]
    return [
        *_checkpoint_drift(knobs, fetch, jid),
        *_vertex_drift(knobs, uses_v2_sink(knobs, spec), fetch, jid),
        *_fleet_drift(knobs, fetch),
    ]


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-flink",
        description="Compare a running Flink job's effective settings with its run spec.",
    )
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec the job was staged from")
    parser.add_argument(
        "--run-id",
        required=True,
        metavar="ID",
        help="run ID used as the Flink job name to identify the job to verify",
    )
    parser.add_argument(
        "--rest", required=True, metavar="URL", help="the jobmanager's REST endpoint, e.g. http://localhost:8081"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    run_id = str(args.run_id)
    try:
        drift = verify(load_run_spec(Path(str(args.spec))), run_id, fetch_json(str(args.rest)))
    except ValueError as error:
        # An unreadable response is retryable, not evidence of configuration drift.
        print(error, file=sys.stderr)
        return UNVERIFIED_EXIT
    for drifted in drift:
        print(drifted)
    if drift:
        return DRIFT_EXIT
    # Keep diagnostics on stderr so stdout contains only drift findings.
    print(f"verified: {run_id} matches its run spec", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
