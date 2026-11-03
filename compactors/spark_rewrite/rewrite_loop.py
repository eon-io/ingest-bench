# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Call Iceberg's rewrite procedure on a fixed cadence inside the Spark image.

One long-lived application rather than a Job per pass: the compaction layer
then holds a fixed fleet for the whole run, which is the compute a comparison
has to charge it, and there is no scheduler latency between passes to mistake
for compaction time. The run ends when the sidecar is deleted.

The procedure's own report goes to the log and nowhere else. What a run
concludes about compaction is read from the table's metadata afterwards, so
this job never has to be believed. Import PySpark inside main so this module
remains testable outside the image.
"""

from __future__ import annotations

import json
import sys
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import cast

# Mount beside the job; /run is reserved for container runtime files.
RUN_DIR = Path("/opt/bench/run")
JOB_DOCUMENT = RUN_DIR / "rewrite.json"

# A rewrite races the ingest engine's appends, and Iceberg resolves that by
# retrying the commit. Tolerate the occasional loss of that race, but do not
# keep a sidecar alive that fails every time: a run whose compactor is broken
# should say so rather than read as a compactor that cannot keep up.
MAX_CONSECUTIVE_FAILURES = 3
FIRST_DATA_POLL_S = 5.0
UTC = timezone.utc  # noqa: UP017 - the Spark image runs Python 3.10, before datetime.UTC


@dataclass(frozen=True)
class Job:
    """The table to rewrite, how often, and the procedure's options."""

    catalog: str
    table: str
    interval_s: int
    start_delay_s: int
    options: dict[str, str]


def _str_at(document: dict[str, object], key: str, path: Path) -> str:
    value = document[key]
    if not isinstance(value, str):
        raise ValueError(f"{path} key {key!r} must be a string, got {value!r}")
    return value


def read_job(path: Path) -> Job:
    """Read and validate the job document at ``path``.

    Require every key so renderer omissions fail explicitly rather than
    running a rewrite with a silently defaulted cadence.
    """
    loaded = json.loads(path.read_text())
    if not isinstance(loaded, dict):
        raise ValueError(f"{path} must hold a mapping, got {type(loaded).__name__}")
    document = cast(dict[str, object], loaded)
    interval = document["interval_s"]
    if isinstance(interval, bool) or not isinstance(interval, int) or interval <= 0:
        raise ValueError(f"{path} key 'interval_s' must be a positive number of seconds, got {interval!r}")
    delay = document["start_delay_s"]
    if isinstance(delay, bool) or not isinstance(delay, int) or delay < 0:
        raise ValueError(f"{path} key 'start_delay_s' must be a non-negative number of seconds, got {delay!r}")
    options = document["options"]
    if not isinstance(options, dict):
        raise ValueError(f"{path} key 'options' must be a mapping, got {options!r}")
    return Job(
        catalog=_str_at(document, "catalog", path),
        table=_str_at(document, "table", path),
        interval_s=interval,
        start_delay_s=delay,
        options={str(key): str(value) for key, value in cast(dict[object, object], options).items()},
    )


def rewrite_call(job: Job) -> str:
    """Build the procedure call for one pass.

    Leave the target file size out: Iceberg reads it from the table's own
    ``write.target-file-size-bytes``, which is where the spec already declares
    it and where the measurement reads it back from.
    """
    arguments = [f"table => '{job.table}'"]
    if job.options:
        pairs = ", ".join(f"'{key}', '{value}'" for key, value in sorted(job.options.items()))
        arguments.append(f"options => map({pairs})")
    return f"CALL {job.catalog}.system.rewrite_data_files({', '.join(arguments)})"


def _timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _emit(event: dict[str, object], *, stderr: bool = False) -> None:
    print(
        json.dumps(event, sort_keys=True, separators=(",", ":")),
        file=sys.stderr if stderr else sys.stdout,
        flush=True,
    )


def _rewrite_counts(result: object) -> tuple[int, int, int]:
    if isinstance(result, (str, bytes)) or not isinstance(result, Sequence) or len(result) != 1:
        raise ValueError("rewrite_data_files must return exactly one result row")
    row = result[0]
    as_dict = getattr(row, "asDict", None)
    if not callable(as_dict):
        raise TypeError("rewrite_data_files result row must provide asDict")
    values = as_dict(recursive=True)
    if not isinstance(values, dict):
        raise TypeError("rewrite_data_files result row asDict must return a mapping")

    counts: list[int] = []
    for field in ("rewritten_data_files_count", "added_data_files_count", "rewritten_bytes_count"):
        value = values[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"rewrite_data_files field {field!r} must be a non-negative integer, got {value!r}")
        counts.append(value)
    return counts[0], counts[1], counts[2]


def first_data_query(job: Job) -> str:
    """Build the query for when the table first gained rows, in epoch microseconds.

    ``unix_micros`` keeps the answer independent of the session time zone. The
    result is null until a commit has added rows.
    """
    return (
        f"SELECT unix_micros(min(committed_at)) AS first_data_micros FROM {job.catalog}.{job.table}.snapshots "
        "WHERE CAST(summary['added-records'] AS BIGINT) > 0"
    )


def _first_data_micros(result: object) -> int | None:
    if isinstance(result, (str, bytes)) or not isinstance(result, Sequence) or len(result) != 1:
        raise ValueError("the first-data query must return exactly one row")
    as_dict = getattr(result[0], "asDict", None)
    if not callable(as_dict):
        raise TypeError("first-data query row must provide asDict")
    value = as_dict(recursive=True)["first_data_micros"]
    if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
        raise ValueError(f"first_data_micros must be an integer or null, got {value!r}")
    return value


def _exception_chain(error: Exception) -> list[dict[str, str]]:
    chain: list[dict[str, str]] = []
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        try:
            message = str(current)
        except BaseException:  # A broken exception formatter must not replace the pass failure.
            message = f"<unprintable {type(current).__name__}>"
        chain.append(
            {
                "type": type(current).__name__,
                "module": type(current).__module__,
                "message": message,
            }
        )
        if current.__cause__ is not None:
            current = current.__cause__
        elif not current.__suppress_context__:
            current = current.__context__
        else:
            current = None
    return chain


def _wait_for_backlog(
    job: Job,
    execute: Callable[[str], object],
    *,
    refresh_table: Callable[[str], None],
    sleep: Callable[[float], None],
    wall_now: Callable[[], datetime],
) -> None:
    """Hold the first pass until ``start_delay_s`` after the first commit that adds rows.

    Anchor on data rather than on this process's start: the sidecar is staged
    before the producers are released, so only the first commit marks when the
    backlog begins to build. A failed query is retried like a failed pass.
    """
    failures = 0
    while True:
        try:
            refresh_table(f"{job.catalog}.{job.table}")
            first_micros = _first_data_micros(execute(first_data_query(job)))
            failures = 0
        except Exception as error:  # noqa: BLE001 - a catalog hiccup must not end the run
            failures += 1
            _emit(
                {
                    "event": "spark_rewrite_delay_poll",
                    "status": "failure",
                    "consecutive_failures": failures,
                    "failure_type": type(error).__name__,
                    "error_chain": _exception_chain(error),
                },
                stderr=True,
            )
            if failures >= MAX_CONSECUTIVE_FAILURES:
                raise
            first_micros = None
        if first_micros is not None:
            break
        sleep(FIRST_DATA_POLL_S)
    first_data_at = datetime.fromtimestamp(first_micros / 1_000_000, UTC)
    start_at = first_data_at + timedelta(seconds=job.start_delay_s)
    _emit(
        {
            "event": "spark_rewrite_delay",
            "first_data_at": _timestamp(first_data_at),
            "start_at": _timestamp(start_at),
            "start_delay_s": job.start_delay_s,
        }
    )
    sleep(max(0.0, (start_at - wall_now()).total_seconds()))


def run(
    job: Job,
    execute: Callable[[str], object],
    *,
    refresh_table: Callable[[str], None],
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
    wall_now: Callable[[], datetime] = lambda: datetime.now(UTC),
    passes: int | None = None,
) -> int:
    """Rewrite every ``interval_s`` until the pass limit or a broken compactor.

    With a ``start_delay_s``, the first pass waits that long after the table's
    first commit that adds rows.

    Time the cadence from the start of each pass so a slow rewrite does not
    push the next one further out; a pass that overruns its interval starts
    the next immediately, which is what falling behind looks like.

    ``passes`` bounds the loop for tests. Unbounded is the deployed shape:
    teardown deletes the application.
    """
    if job.start_delay_s:
        _wait_for_backlog(job, execute, refresh_table=refresh_table, sleep=sleep, wall_now=wall_now)
    completed = 0
    failures = 0
    while passes is None or completed < passes:
        started = now()
        started_at = _timestamp(wall_now())
        pass_number = completed + 1
        _emit({"event": "spark_rewrite_start", "pass": pass_number, "started_at": started_at})
        try:
            # Short cadences keep Iceberg's access-expiring catalog cache alive;
            # refresh so this pass sees appends committed by the ingest engine.
            refresh_table(f"{job.catalog}.{job.table}")
            rewritten_files, added_files, rewritten_bytes = _rewrite_counts(execute(rewrite_call(job)))
            failures = 0
        except Exception as error:  # noqa: BLE001 - a lost commit race must not end the run
            failures += 1
            duration_s = now() - started
            _emit(
                {
                    "event": "spark_rewrite_pass",
                    "pass": pass_number,
                    "status": "failure",
                    "started_at": started_at,
                    "ended_at": _timestamp(wall_now()),
                    "duration_s": duration_s,
                    "rewritten_data_files_count": None,
                    "added_data_files_count": None,
                    "rewritten_bytes_count": None,
                    "consecutive_failures": failures,
                    "failure_type": type(error).__name__,
                    "error_chain": _exception_chain(error),
                },
                stderr=True,
            )
            if failures >= MAX_CONSECUTIVE_FAILURES:
                raise
        else:
            duration_s = now() - started
            _emit(
                {
                    "event": "spark_rewrite_pass",
                    "pass": pass_number,
                    "status": "no-op" if rewritten_files == added_files == rewritten_bytes == 0 else "success",
                    "started_at": started_at,
                    "ended_at": _timestamp(wall_now()),
                    "duration_s": duration_s,
                    "rewritten_data_files_count": rewritten_files,
                    "added_data_files_count": added_files,
                    "rewritten_bytes_count": rewritten_bytes,
                    "consecutive_failures": 0,
                    "failure_type": None,
                    "error_chain": [],
                }
            )
        completed += 1
        if passes is not None and completed >= passes:
            break
        sleep(max(0.0, job.interval_s - duration_s))
    return completed


def main(argv: Sequence[str] | None = None) -> int:
    from pyspark.sql import SparkSession  # noqa: PLC0415 - absent outside the engine image

    if argv:
        raise SystemExit(f"rewrite_loop takes no arguments; it reads {JOB_DOCUMENT}. Got {list(argv)}")
    job = read_job(JOB_DOCUMENT)
    spark = SparkSession.builder.getOrCreate()
    delay = f", from {job.start_delay_s}s after its first rows" if job.start_delay_s else ""
    print(f"rewriting {job.catalog}.{job.table} every {job.interval_s}s{delay}", flush=True)
    run(
        job,
        lambda statement: spark.sql(statement).collect(),
        refresh_table=spark.catalog.refreshTable,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
