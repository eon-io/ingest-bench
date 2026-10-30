# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Commands for live scoring, early-stop decisions, and file geometry.

The gate reads scorer artifacts to avoid a second table reader. Geometry can
be measured after the run, keeping manifest traversal out of the live loop.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
from typing import cast

from ingest_bench import uri
from ingest_bench.catalog import load_catalog_props, table_identifier
from ingest_bench.clock import SystemClock
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.scorer import compaction as compaction_report_module
from ingest_bench.scorer import geometry, seed_barrier, terminal, upsert_verify
from ingest_bench.scorer import score as score_loop
from ingest_bench.scorer import table_health as health
from ingest_bench.scorer.gate import PASS, UNDERSIZED, VOID, gate_verdict
from ingest_bench.scorer.snapshots import load_table, read_metadata, snapshots_in_order
from ingest_bench.specs.model import (
    AT_LEAST_ONCE,
    DEFAULT_GEOMETRY_OFFSETS_S,
    DEFAULT_WARMUP_S,
    ENGINE_OWNED,
    EXACTLY_ONCE,
    EXACTNESS_MODES,
    HARNESS,
    validate_scoring_mode,
)
from ingest_bench.table.cli import add_catalog_arguments

FSSPEC_FILE_IO = "pyiceberg.io.fsspec.FsspecFileIO"

# Distinct verdict exit codes let shell drivers branch without parsing output.
EXIT_CODES = {PASS: 0, UNDERSIZED: 3, VOID: 5}

# Distinguish an empty table from argparse's argument-error exit code.
NO_GEOMETRY = 4

DEFAULT_ADAPTATION_S = 120
DEFAULT_FLOOR_WINDOW_S = 60
DEFAULT_READ_WORKERS = 32

# Match the newest backlog window. This allows twelve normal poll intervals
# or twice the configured retry gap before declaring samples stale.
DEFAULT_STALE_AFTER_S = DEFAULT_FLOOR_WINDOW_S


def _positive_int(raw: str) -> int:
    """Parse an integer of at least one."""
    try:
        value = int(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"must be an integer, got {raw!r}") from error
    if value < 1:
        raise argparse.ArgumentTypeError(f"must be at least 1, got {value}")
    return value


def build_score_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="score",
        description="Measure freshness, exactness, and keep-up as table commits arrive.",
    )
    add_catalog_arguments(parser)
    parser.add_argument(
        "--corpus",
        required=True,
        metavar="URI",
        help="source corpus whose manifest defines the expected rows",
    )
    parser.add_argument(
        "--publish-logs",
        required=True,
        metavar="URI",
        help="URI prefix containing all producer shards' publish logs",
    )
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, the same one every producer shard was given",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the directory the artifacts are written to")
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help="upload artifacts to this URI prefix as they are written",
    )
    parser.add_argument(
        "--mode",
        choices=("sustainable", "saturation"),
        default="sustainable",
        help="sustainable requires freshness; saturation measures throughput under backlog",
    )
    parser.add_argument(
        "--measurement-s",
        type=_positive_int,
        help="fixed measurement duration after warmup; required only in saturation mode",
    )
    parser.add_argument("--poll-interval-s", type=float, default=5.0, help="table polling interval in seconds")
    parser.add_argument(
        "--idle-stop-s",
        type=float,
        default=300.0,
        help="stop after this many seconds without a new commit while rows are still missing",
    )
    parser.add_argument(
        "--warmup-s",
        type=int,
        default=120,
        help="seconds after the epoch before the freshness or saturation measurement window",
    )
    parser.add_argument(
        "--freshness-bound-s", type=float, default=180.0, help="maximum allowed p95 freshness lag in seconds"
    )
    parser.add_argument(
        "--freshness-max-bound-s",
        type=float,
        default=None,
        help="maximum allowed freshness lag in seconds (default: twice --freshness-bound-s)",
    )
    parser.add_argument(
        "--exactness",
        choices=sorted(EXACTNESS_MODES),
        default=EXACTLY_ONCE,
        help="exactness SLA; AT_LEAST_ONCE permits duplicate rows",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="the replay speed the producers were given, recorded with the score",
    )
    parser.add_argument(
        "--sample-offsets",
        type=_offsets,
        default=(),
        metavar="S,S,...",
        help="LATEST_PER_KEY: seconds after the epoch whose snapshot is checkpointed for live-state verification",
    )
    parser.add_argument(
        "--publish-shards",
        type=_positive_int,
        default=1,
        help="number of producer shards; publishing ends when all shard logs are marked done",
    )
    parser.add_argument(
        "--gate-window-s",
        type=_positive_int,
        default=DEFAULT_FLOOR_WINDOW_S,
        help="scheduled window for per-shard producer lateness measurements",
    )
    parser.add_argument(
        "--behind-max-ms",
        type=int,
        default=5000,
        help="maximum producer schedule delay in milliseconds before the run is void",
    )
    parser.add_argument(
        "--read-workers",
        type=_positive_int,
        default=DEFAULT_READ_WORKERS,
        help="maximum number of data files whose ID columns are read concurrently",
    )
    parser.add_argument(
        "--read-processes",
        type=_positive_int,
        default=1,
        help="maximum worker processes for ID-column reads, capped at --read-workers; 1 uses threads only",
    )
    parser.add_argument(
        "--table-managed-by",
        choices=sorted({HARNESS, ENGINE_OWNED}),
        default=HARNESS,
        help=f"table creator, matching table.managed_by in the run spec. With {ENGINE_OWNED!r}, treat a "
        "missing table as empty and keep polling until the engine creates it",
    )
    return parser


def build_gate_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="gate",
        description="Evaluate an active run from scorer artifacts: PASS, UNDERSIZED, or VOID.",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the scorer's artifact directory")
    parser.add_argument(
        "--adaptation-s",
        type=int,
        default=DEFAULT_ADAPTATION_S,
        help="seconds after the epoch before the lag check applies; commit spacing seen until then widens "
        "the backlog windows",
    )
    parser.add_argument(
        "--window-s",
        type=int,
        default=DEFAULT_FLOOR_WINDOW_S,
        help="backlog-floor window in seconds; a rising floor across three windows indicates falling behind",
    )
    parser.add_argument(
        "--stale-after-s",
        type=int,
        default=DEFAULT_STALE_AFTER_S,
        help="maximum age of the latest keep-up sample in seconds before the run is VOID",
    )
    return parser


def score(argv: Sequence[str] | None = None) -> int:
    parser = build_score_parser()
    args = parser.parse_args(argv)
    table = str(args.table)
    try:
        # Validate the identifier before contacting the catalog.
        table_identifier(table)
        validate_scoring_mode(str(args.mode), args.measurement_s)
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    return score_loop.run(
        score_loop.ScoreArgs(
            corpus_uri=str(args.corpus),
            table=table,
            catalog_props=props,
            publish_logs_uri=str(args.publish_logs),
            epoch_ms=round(float(args.epoch) * 1000),
            out_dir=Path(str(args.out)),
            poll_interval_s=float(args.poll_interval_s),
            idle_stop_s=float(args.idle_stop_s),
            warmup_s=int(args.warmup_s),
            scoring_mode=str(args.mode),
            measurement_s=args.measurement_s,
            freshness_bound_s=float(args.freshness_bound_s),
            freshness_max_bound_s=args.freshness_max_bound_s,
            exactness_mode=str(args.exactness),
            speed=float(args.speed),
            behind_max_ms=int(args.behind_max_ms),
            gate_window_s=int(args.gate_window_s),
            expected_publish_shards=int(args.publish_shards),
            upload_prefix=None if args.upload_prefix is None else str(args.upload_prefix),
            read_workers=int(args.read_workers),
            read_processes=int(args.read_processes),
            table_managed_by=str(args.table_managed_by),
            sample_offsets_s=tuple(args.sample_offsets),
        ),
        SystemClock(),
        sys.stdout,
    )


def _report(verdict: str, reason: str) -> int:
    print(f"{verdict} {reason}")
    return EXIT_CODES[verdict]


def gate(argv: Sequence[str] | None = None) -> int:
    args = build_gate_parser().parse_args(argv)
    out_dir = Path(str(args.out))
    summary_path = out_dir / score_loop.SUMMARY_FILE
    # Report missing measurements as VOID so the polling driver can handle them.
    if not summary_path.exists():
        return _report(VOID, f"{summary_path} does not exist, so the run has no measurement to judge")
    summary = cast(dict[str, object], json.loads(summary_path.read_text(encoding="utf-8")))
    # Preserve schema-failure verdicts before judging lag and backlog.
    if summary["state"] == score_loop.VOID:
        return _report(VOID, str(summary["reason"]))
    producer = cast(dict[str, object], summary["producer"])
    window = cast(dict[str, object] | None, producer.get("window"))
    if window is not None:
        print(
            f"producers closed_window n={window['shards']} behind "
            f"median={float(cast(float, window['behind_ms_median'])) / 1000:.1f}s "
            f"max={float(cast(float, window['behind_ms_max'])) / 1000:.1f}s "
            f"window={window['start_ms']}..{window['end_ms']}"
        )
    # A bound producer is a fact about the offer, so it cannot be reported
    # through the reader-aborted reason the verdict function would give it.
    if summary["producer_bound"]:
        return _report(
            VOID,
            f"the producer fell {producer['behind_ms']} ms behind its schedule with {producer['errors']} delivery "
            "errors, so the offer rather than the engine set the rate",
        )
    verdict, reason = gate_verdict(
        {"aborted": summary["aborted"], "lag_s": summary["lag_s"], "freshness_live": summary.get("freshness_live")},
        score_loop.read_keepup_samples(out_dir / score_loop.KEEPUP_SAMPLES_FILE),
        bound_s=float(cast(float, summary["freshness_bound_s"])),
        max_bound_s=cast(float | None, summary.get("freshness_max_bound_s")),
        adaptation_s=int(args.adaptation_s),
        window_s=int(args.window_s),
        now_ms=SystemClock().now_ms(),
        epoch_ms=int(cast(int, summary["epoch_ms"])),
        stale_after_s=int(args.stale_after_s),
        scoring_mode=str(summary.get("scoring_mode", "sustainable")),
    )
    return _report(verdict, reason)


def _offsets(raw: str) -> tuple[int, ...]:
    """The geometry ladder, as ``--offsets 600,1200,1800`` gives it."""
    try:
        offsets = tuple(int(part) for part in raw.split(","))
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"--offsets must be comma-separated seconds, got {raw!r}") from error
    if any(offset < 0 for offset in offsets):
        raise argparse.ArgumentTypeError(f"--offsets are seconds from the epoch, so none may be negative: {raw!r}")
    if any(later <= earlier for earlier, later in zip(offsets, offsets[1:], strict=False)):
        raise argparse.ArgumentTypeError(
            f"--offsets must ascend, since each rung reports the commits since the one before it: {raw!r}"
        )
    return offsets


def build_file_sizes_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="file-sizes",
        description="Report table file sizes and counts at selected times and at the end of the run.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument(
        "--metadata",
        metavar="URI_OR_PATH",
        help="saved table metadata to read instead of --table, allowing analysis after teardown. "
        "Catalog properties still supply object-store settings",
    )
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, the same one the scorer was given; the offsets are measured from it",
    )
    parser.add_argument(
        "--offsets",
        type=_offsets,
        default=DEFAULT_GEOMETRY_OFFSETS_S,
        metavar="SECONDS,…",
        help="comma-separated seconds after the epoch at which to measure the table",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the directory geometry.json is written to")
    return parser


def file_sizes(argv: Sequence[str] | None = None) -> int:
    parser = build_file_sizes_parser()
    args = parser.parse_args(argv)
    metadata_location = None if args.metadata is None else str(args.metadata)
    table = None if args.table is None else str(args.table)
    # Require one metadata source: copied and live documents may differ.
    if (metadata_location is None) == (table is None):
        parser.error("give exactly one of --metadata (a copied document) and --table (through a catalog)")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
        if table is not None:
            table_identifier(table)
    except ValueError as error:
        parser.error(str(error))
    # Prefer fsspec for operator-side profile support, including credential_process
    # and SSO. Honor an explicit IO implementation.
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    if metadata_location is not None:
        document, io = geometry.open_metadata_document(metadata_location, props)
    else:
        loaded = load_table(props, cast(str, table))
        document, io = read_metadata(loaded), loaded.io
    report = geometry.geometry_report(document, io, round(float(args.epoch) * 1000), tuple(args.offsets))
    out_dir = Path(str(args.out))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / geometry.GEOMETRY_FILE
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    final = cast(dict[str, object] | None, report["final"])
    # The empty document is still published: `collect` records that geometry
    # was measured, and a missing file would read as a step never run.
    if final is None:
        print(f"table has no commits; wrote empty file geometry to {path}", file=sys.stderr)
        return NO_GEOMETRY
    live = cast(dict[str, object], final["live"])
    quantiles = cast(dict[str, float | None], live["size_quantiles"])
    print(
        f"GEOMETRY out={path} files={live['files']} rows={live['rows']} bytes={live['bytes']} "
        f"p50_bytes={quantiles['p50']} small_file_share_32mib={live['small_file_share_32mib']}"
    )
    return 0


def build_compaction_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compaction",
        description="Report how close the table sits to the fewest files its partitioning permits, "
        "and how many bytes were rewritten to get there.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument(
        "--metadata",
        metavar="URI_OR_PATH",
        help="saved table metadata to read instead of --table, allowing analysis after teardown. "
        "Catalog properties still supply object-store settings",
    )
    parser.add_argument(
        "--target-file-bytes",
        type=int,
        metavar="BYTES",
        help="the file size compaction aims at. Declare the compactor's target, which an engine may "
        f"set differently from the size it writes at. Defaults to the table's "
        f"{compaction_report_module.TARGET_TABLE_PROPERTY}, then to the format default; the report "
        "names which was used",
    )
    parser.add_argument(
        "--epoch",
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin. With --offsets it samples the ratio over the run instead of only "
        "at the end, which a periodic compactor needs: one terminal reading lands wherever the run "
        "stopped in its cycle",
    )
    parser.add_argument(
        "--observed-at",
        type=float,
        metavar="UNIX_SECONDS",
        help="when the metadata was observed. Required to measure quiet time from a saved --metadata document; "
        "live catalog reads default to the current time",
    )
    parser.add_argument(
        "--offsets",
        type=_offsets,
        default=DEFAULT_GEOMETRY_OFFSETS_S,
        metavar="SECONDS,…",
        help="comma-separated seconds after the epoch at which to sample the ratio",
    )
    parser.add_argument("--out", required=True, metavar="DIR", help="the directory compaction.json is written to")
    return parser


def compaction(argv: Sequence[str] | None = None) -> int:
    parser = build_compaction_parser()
    args = parser.parse_args(argv)
    metadata_location = None if args.metadata is None else str(args.metadata)
    table = None if args.table is None else str(args.table)
    # Require one metadata source: copied and live documents may differ.
    if (metadata_location is None) == (table is None):
        parser.error("give exactly one of --metadata (a copied document) and --table (through a catalog)")
    if args.target_file_bytes is not None and int(args.target_file_bytes) <= 0:
        parser.error("--target-file-bytes must be positive")
    try:
        observed_at_ms = None if args.observed_at is None else round(float(args.observed_at) * 1000)
    except (OverflowError, ValueError) as error:
        parser.error(f"--observed-at must be a finite UNIX timestamp: {error}")
    automatic_observation = table is not None and observed_at_ms is None
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
        if table is not None:
            table_identifier(table)
    except ValueError as error:
        parser.error(str(error))
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    if metadata_location is not None:
        document, io = geometry.open_metadata_document(metadata_location, props)
    else:
        loaded = load_table(props, cast(str, table))
        document, io = read_metadata(loaded), loaded.io
    if automatic_observation:
        observed_at_ms = SystemClock().now_ms()
    ordered = snapshots_in_order(document)
    latest_commit_ms = max((info.timestamp_ms for info in ordered), default=None)
    if observed_at_ms is not None and latest_commit_ms is not None and observed_at_ms < latest_commit_ms:
        if automatic_observation:
            # A writer clock ahead of the observer cannot establish quiet time.
            observed_at_ms = None
        else:
            parser.error(f"--observed-at precedes the latest commit at {latest_commit_ms / 1000:g} UNIX seconds")
    declared = None if args.target_file_bytes is None else int(args.target_file_bytes)
    target_file_bytes, target_source = compaction_report_module.resolve_target(document, declared)
    report = compaction_report_module.compaction_report(
        document,
        io,
        target_file_bytes=target_file_bytes,
        target_source=target_source,
        epoch_ms=None if args.epoch is None else round(float(args.epoch) * 1000),
        offsets_s=tuple(args.offsets),
        observed_at_ms=observed_at_ms,
    )
    out_dir = Path(str(args.out))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / compaction_report_module.COMPACTION_FILE
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(
        f"COMPACTION out={path} files={report['files']} floor={report['floor']} "
        f"floor_ratio={report['floor_ratio']} rewrite_share={report['rewrite_share_of_writes']} "
        f"written_per_ingested={report['written_per_ingested_byte']} "
        f"records_dropped={report['records_dropped']} converged={report['converged']}"
    )
    return 0


def build_verify_table_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-table",
        description="Validate final row-ID coverage in an immutable Iceberg metadata document.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument("--metadata", required=True, metavar="URI_OR_PATH", help="immutable final table metadata")
    parser.add_argument("--corpus-uri", required=True, metavar="URI", help="corpus that defines expected row IDs")
    parser.add_argument(
        "--publish-logs", required=True, metavar="URI", help="URI prefix containing producer publish logs"
    )
    parser.add_argument("--out", required=True, metavar="DIR", help=f"directory for {terminal.TERMINAL_FILE}")
    parser.add_argument(
        "--exactness",
        choices=(EXACTLY_ONCE, AT_LEAST_ONCE),
        default=EXACTLY_ONCE,
        help="exactness SLA; AT_LEAST_ONCE permits duplicate rows",
    )
    parser.add_argument(
        "--publish-shards",
        type=_positive_int,
        default=1,
        help="number of producer shards whose completed logs make up the offer",
    )
    parser.add_argument(
        "--read-workers",
        type=_positive_int,
        default=DEFAULT_READ_WORKERS,
        help="maximum number of live data files whose ID columns are read concurrently",
    )
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help=f"also write {terminal.TERMINAL_FILE} under this prefix, for a validator running in the cluster",
    )
    return parser


def verify_table(argv: Sequence[str] | None = None) -> int:
    parser = build_verify_table_parser()
    args = parser.parse_args(argv)
    if args.table is not None:
        parser.error("--table is unsupported; terminal validation requires immutable --metadata")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    result = terminal.run(
        terminal.VerifyArgs(
            metadata_location=str(args.metadata),
            corpus_uri=str(args.corpus_uri),
            publish_logs_uri=str(args.publish_logs),
            catalog_props=props,
            out_dir=Path(str(args.out)),
            expected_publish_shards=int(args.publish_shards),
            read_workers=int(args.read_workers),
            exactness_mode=str(args.exactness),
        )
    )
    artifact = Path(str(args.out)) / terminal.TERMINAL_FILE
    # A failed verdict is evidence too: upload whatever was written, whatever the status.
    if args.upload_prefix is not None and artifact.exists():
        uri.write_bytes(uri.join(str(args.upload_prefix), terminal.TERMINAL_FILE), artifact.read_bytes())
    print(f"TERMINAL_EXACTNESS exit={result} out={artifact}")
    return result


def _instant_ms(raw: str) -> int:
    """Parse UNIX seconds or a timezone-aware ISO 8601 timestamp as milliseconds.

    Reject naive ISO timestamps to avoid shifting samples into the wrong phase.
    """
    try:
        return round(float(raw) * 1000)
    except ValueError:
        pass
    try:
        moment = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            f"must be UNIX seconds or an ISO 8601 instant, got {raw!r}: {error}"
        ) from error
    if moment.tzinfo is None:
        raise argparse.ArgumentTypeError(f"the ISO instant {raw!r} has no UTC offset, so it names no instant")
    return round(moment.timestamp() * 1000)


def _small_fraction(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as error:
        raise argparse.ArgumentTypeError(f"must be a number, got {raw!r}") from error
    if not 0 < value <= 1:
        raise argparse.ArgumentTypeError(f"must be within (0, 1], got {value}")
    return value


# One pass of the rewrite sidecar, and the fields worth carrying onto the grid.
REWRITE_PASS_EVENT = "spark_rewrite_pass"
REWRITE_PASS_FIELDS = (
    "pass",
    "status",
    "duration_s",
    "rewritten_data_files_count",
    "added_data_files_count",
    "rewritten_bytes_count",
    "failure_type",
)


def _json_objects(text: str) -> list[dict[str, object]]:
    """Every JSON object in a log, ignoring whatever a collector prefixed.

    Cluster logs carry a pod prefix and a timestamp ahead of the line the job
    printed; Compose logs do not. Reading from the first brace accepts both.
    """
    objects: list[dict[str, object]] = []
    for line in text.splitlines():
        start = line.find("{")
        if start < 0:
            continue
        try:
            loaded = json.loads(line[start:])
        except json.JSONDecodeError:
            continue
        if isinstance(loaded, dict):
            objects.append(cast(dict[str, object], loaded))
    return objects


def _rewrite_passes(path: Path) -> list[tuple[int, dict[str, object]]]:
    """Place rewrite-pass diagnostics at their completion time.

    A pass start does not establish that its work reached the table.
    """
    placed: list[tuple[int, dict[str, object]]] = []
    for event in _json_objects(path.read_text(encoding="utf-8")):
        if event.get("event") != REWRITE_PASS_EVENT or not isinstance(ended := event.get("ended_at"), str):
            continue
        placed.append((_instant_ms(ended), {field: event.get(field) for field in REWRITE_PASS_FIELDS}))
    return placed


def _committer_samples(path: Path) -> list[tuple[int, dict[str, object]]]:
    """Committer gauge samples, each carrying how many tasks completed since the last.

    The gauge is cumulative, so the change between samples is the work done;
    the raw total only says how much had been done by the end of the run.
    """
    loaded = json.loads(path.read_text(encoding="utf-8"))
    samples = loaded.get("samples") if isinstance(loaded, dict) else loaded
    if not isinstance(samples, list):
        raise ValueError(f"{path} must hold a list of committer samples, or a mapping with a 'samples' list")
    ordered = sorted(cast(list[dict[str, object]], samples), key=lambda sample: int(cast(int, sample["ts_ms"])))
    placed: list[tuple[int, dict[str, object]]] = []
    previous: int | None = None
    for sample in ordered:
        completed = int(cast(int, sample["completed_total"]))
        placed.append(
            (
                int(cast(int, sample["ts_ms"])),
                {
                    "in_flight_tasks": sample.get("in_flight_tasks"),
                    "completed_total": completed,
                    "completed_delta": None if previous is None else completed - previous,
                },
            )
        )
        previous = completed
    return placed


def build_campaign_compaction_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="compaction-report",
        description="Report compaction geometry and backlog over offered load, "
        "with engine diagnostics on the same time grid. "
        "The cycle-count check can fail a run but cannot certify keep-up.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument(
        "--metadata", required=True, metavar="URI_OR_PATH", help="saved table metadata covering the whole run"
    )
    parser.add_argument(
        "--epoch", required=True, type=_instant_ms, metavar="TIME", help="the run's time origin, the grid's offset zero"
    )
    parser.add_argument(
        "--offer-end",
        type=_instant_ms,
        metavar="TIME",
        help="when the offer ended. Without it the report uses all sampled instants, including drain",
    )
    parser.add_argument(
        "--writers-stopped",
        type=_instant_ms,
        metavar="TIME",
        help="when the last writer stopped, which separates drain from shutdown",
    )
    parser.add_argument(
        "--target-bytes",
        type=_positive_int,
        metavar="BYTES",
        help=f"the file size compaction aims at. Defaults to the table's "
        f"{compaction_report_module.TARGET_TABLE_PROPERTY}, then to the format default",
    )
    parser.add_argument(
        "--min-input-files",
        type=_positive_int,
        default=compaction_report_module.MIN_INPUT_FILES,
        metavar="FILES",
        help="minimum small-file count per partition in the reported eligible backlog. Match the engine's "
        "planner setting; other planner rules can also trigger rewrites",
    )
    parser.add_argument(
        "--grid-s",
        type=_positive_int,
        default=compaction_report_module.GRID_S,
        metavar="SECONDS",
        help="the spacing of the sampled time grid",
    )
    parser.add_argument(
        "--small-fraction",
        type=_small_fraction,
        default=compaction_report_module.SMALL_FRACTION,
        metavar="SHARE",
        help="the share of the target below which a file counts as small",
    )
    parser.add_argument(
        "--warmup-s",
        type=float,
        default=compaction_report_module.WARMUP_S,
        metavar="SECONDS",
        help="how much of the run to skip before the measured windows begin",
    )
    parser.add_argument(
        "--window-s",
        type=float,
        default=compaction_report_module.WINDOW_S,
        metavar="SECONDS",
        help="the length of each of the consecutive measured windows",
    )
    parser.add_argument(
        "--sidecar-log",
        metavar="FILE",
        help="a rewrite sidecar log whose per-pass JSON lines are joined onto the grid as diagnostics",
    )
    parser.add_argument(
        "--committer-metrics",
        metavar="FILE",
        help="committer gauge samples ({ts_ms, in_flight_tasks, completed_total}) joined onto the grid",
    )
    parser.add_argument(
        "--out",
        required=True,
        metavar="DIR",
        help=f"the directory {compaction_report_module.CAMPAIGN_FILE} is written to",
    )
    return parser


def campaign_compaction(argv: Sequence[str] | None = None) -> int:
    parser = build_campaign_compaction_parser()
    args = parser.parse_args(argv)
    if args.table is not None:
        parser.error("--table is unsupported; a campaign reading requires the run's saved --metadata")
    if args.writers_stopped is not None and args.offer_end is None:
        parser.error("--writers-stopped needs --offer-end; drain cannot be told from offered load without it")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    document, io = geometry.open_metadata_document(str(args.metadata), props)
    target_file_bytes, target_source = compaction_report_module.resolve_target(
        document, None if args.target_bytes is None else int(args.target_bytes)
    )
    try:
        report = compaction_report_module.campaign_report(
            document,
            io,
            target_file_bytes=target_file_bytes,
            target_source=target_source,
            epoch_ms=int(args.epoch),
            offer_end_ms=None if args.offer_end is None else int(args.offer_end),
            writers_stopped_ms=None if args.writers_stopped is None else int(args.writers_stopped),
            grid_s=int(args.grid_s),
            small_fraction=float(args.small_fraction),
            min_input_files=int(args.min_input_files),
            warmup_s=float(args.warmup_s),
            window_s=float(args.window_s),
        )
    except ValueError as error:
        parser.error(str(error))
    offsets = [int(cast(int, sample["offset_s"])) for sample in cast(list[dict[str, object]], report["grid"])]
    placed = {
        "rewrite_passes": None if args.sidecar_log is None else _rewrite_passes(Path(str(args.sidecar_log))),
        "committer_samples": None
        if args.committer_metrics is None
        else _committer_samples(Path(str(args.committer_metrics))),
    }
    report["engine_diagnostics"] = {
        name: None
        if events is None
        else compaction_report_module.place_on_grid(
            events, epoch_ms=int(args.epoch), grid_s=int(args.grid_s), offsets_s=offsets
        )
        for name, events in placed.items()
    }
    out_dir = Path(str(args.out))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / compaction_report_module.CAMPAIGN_FILE
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    windows = cast(dict[str, object], report["windows"])
    print(
        f"COMPACTION_REPORT out={path} campaign_pass={report['campaign_pass']} "
        f"samples={len(offsets)} floor_ratio_median={windows['floor_ratio_median']} "
        f"rewrite_cycles={windows['rewrite_cycles']} basis={windows['basis']}"
    )
    if args.offer_end is None:
        # Without an offer boundary, the report cannot exclude drain.
        print(
            "  no --offer-end: every instant was read, including any after the offer, where the backlog can only decay",
            file=sys.stderr,
        )
    for reason in cast(list[str], report["reasons"]):
        print(f"  {reason}", file=sys.stderr)
    return 0


def build_wait_seed_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="wait-seed",
        description=(
            "Wait until the engine has committed an upsert run's Kafka seed, before the scored offer, "
            "and hold the seed to the run's freshness bound."
        ),
    )
    add_catalog_arguments(parser, table_required=True)
    parser.add_argument("--corpus", required=True, metavar="URI", help="the upsert corpus whose seed was sent")
    parser.add_argument("--publish-logs", required=True, metavar="URI", help="the seed producer's publish logs")
    parser.add_argument(
        "--out", required=True, metavar="URI_OR_PATH", help=f"where to write {seed_barrier.SEED_FRESHNESS_FILE}"
    )
    parser.add_argument(
        "--freshness-bound-s", type=float, default=180.0, help="the run's freshness bound, applied to the seed"
    )
    parser.add_argument(
        "--freshness-max-bound-s",
        type=float,
        default=None,
        help="the run's freshness max bound, applied to the seed (default: twice --freshness-bound-s)",
    )
    parser.add_argument(
        "--warmup-s", type=int, default=DEFAULT_WARMUP_S, help="seconds after the first seed batch the bound starts"
    )
    parser.add_argument(
        "--behind-max-ms",
        type=int,
        default=5000,
        help="maximum seed producer schedule delay before the seed's freshness is void",
    )
    parser.add_argument("--timeout-s", type=float, default=3600, help="give up after this long (default: 3600)")
    parser.add_argument("--poll-s", type=float, default=5, help="seconds between table reads (default: 5)")
    return parser


def wait_seed(argv: Sequence[str] | None = None) -> int:
    """Return 0 once the seed is committed within the bound, 3 when it missed the bound, 5 when the seed
    producer fell behind its schedule so the freshness judges nothing, and 1 at the timeout."""
    parser = build_wait_seed_parser()
    args = parser.parse_args(argv)
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    try:
        loaded = seed_barrier.wait_for_seed(
            props,
            str(args.table),
            str(args.corpus),
            str(args.publish_logs),
            warmup_s=int(args.warmup_s),
            bound_s=float(args.freshness_bound_s),
            max_bound_s=args.freshness_max_bound_s,
            timeout_s=float(args.timeout_s),
            poll_s=float(args.poll_s),
            behind_max_ms=int(args.behind_max_ms),
        )
    except TimeoutError as error:
        print(f"wait-seed: {error}", file=sys.stderr)
        return 1
    result = loaded.freshness
    report = {
        "rows": loaded.rows,
        "snapshot_id": loaded.snapshot_id,
        "waited_s": loaded.waited_s,
        "producer_behind_ms": loaded.producer_behind_ms,
        "producer_bound": loaded.producer_bound,
        **asdict(result),
    }
    uri.write_text(str(args.out), json.dumps(report, indent=2))
    print(
        f"SEED_LOADED rows={loaded.rows} snapshot={loaded.snapshot_id} waited_s={loaded.waited_s:.0f} "
        f"p95_s={result.window['p95_s']} max_s={result.window['max_s']} verdict={result.verdict}"
    )
    if loaded.producer_bound:
        print(
            f"wait-seed: VOID: the seed producer fell {loaded.producer_behind_ms} ms behind its schedule, past "
            f"{args.behind_max_ms} ms, so the producer rather than the engine set the seed's pace; stage a fresh run "
            "with room for the producer pods",
            file=sys.stderr,
        )
        return 5
    if not result.verdict:
        print(
            f"wait-seed: the seed missed the freshness bound (p95 {result.window['p95_s']}s against "
            f"{result.bound_s}s, max {result.window['max_s']}s against {result.max_bound_s}s); see {args.out}",
            file=sys.stderr,
        )
        return 3
    return 0


def build_verify_upsert_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="verify-upsert",
        description="Check an upsert table's live rows at the scorer's sample snapshots and the final snapshot.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument("--metadata", required=True, metavar="URI_OR_PATH", help="immutable final table metadata")
    parser.add_argument("--corpus-uri", required=True, metavar="URI", help="the upsert corpus the run offered")
    parser.add_argument(
        "--checkpoints",
        required=True,
        metavar="URI",
        help="the scorer's checkpoints directory: checkpoints.json and stamps",
    )
    parser.add_argument(
        "--live", required=True, metavar="URI", help="directory holding each snapshot's live rows, written or read here"
    )
    parser.add_argument(
        "--out", required=True, metavar="DIR", help=f"directory for {upsert_verify.TERMINAL_UPSERT_FILE}"
    )
    parser.add_argument(
        "--reader",
        choices=upsert_verify.READERS,
        default=upsert_verify.PYICEBERG,
        help="pyiceberg scans the snapshots first; none checks rows the Spark reader already wrote",
    )
    parser.add_argument(
        "--upload-prefix", metavar="URI", help=f"also write {upsert_verify.TERMINAL_UPSERT_FILE} under this prefix"
    )
    return parser


def verify_upsert(argv: Sequence[str] | None = None) -> int:
    parser = build_verify_upsert_parser()
    args = parser.parse_args(argv)
    if args.table is not None:
        parser.error("--table is unsupported; verification reads immutable --metadata")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    out_dir = Path(str(args.out))
    try:
        result = upsert_verify.run(
            upsert_verify.UpsertVerifyArgs(
                metadata_location=str(args.metadata),
                corpus_uri=str(args.corpus_uri),
                checkpoints_uri=str(args.checkpoints),
                live_uri=str(args.live),
                out_dir=out_dir,
                catalog_props=props,
                reader=str(args.reader),
            )
        )
    except Exception as error:
        print(f"VERIFY_UPSERT_FAILED error={type(error).__name__}: {error}", file=sys.stderr)
        result = upsert_verify.ERROR
    artifact = out_dir / upsert_verify.TERMINAL_UPSERT_FILE
    # A failed verdict is evidence too: upload whatever was written, whatever the status.
    if args.upload_prefix is not None and artifact.exists():
        uri.write_bytes(uri.join(str(args.upload_prefix), upsert_verify.TERMINAL_UPSERT_FILE), artifact.read_bytes())
    print(f"TERMINAL_UPSERT exit={result} out={artifact}")
    return result


def build_table_health_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="table-health",
        description="Measure the merge-on-read work an upsert table leaves readers, from its metadata alone.",
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument("--metadata", required=True, metavar="URI_OR_PATH", help="immutable final table metadata")
    parser.add_argument("--corpus-uri", required=True, metavar="URI", help="the upsert corpus the run offered")
    parser.add_argument(
        "--checkpoints", required=True, metavar="URI", help="the scorer's checkpoints directory: the batches offered"
    )
    parser.add_argument("--out", required=True, metavar="DIR", help=f"directory for {health.TABLE_HEALTH_FILE}")
    return parser


def table_health(argv: Sequence[str] | None = None) -> int:
    parser = build_table_health_parser()
    args = parser.parse_args(argv)
    if args.table is not None:
        parser.error("--table is unsupported; table health reads immutable --metadata")
    try:
        props = load_catalog_props(
            [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
        )
    except ValueError as error:
        parser.error(str(error))
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    meta = corpus_metadata.read(str(args.corpus_uri))
    if meta.schedule is None:
        parser.error(f"{args.corpus_uri} is not an upsert corpus")
    checkpoints = cast(
        dict[str, object],
        json.loads(uri.read_text(uri.join(str(args.checkpoints), upsert_verify.CHECKPOINTS_FILE))),
    )
    truth = upsert_verify.expected_truth(meta, upsert_verify.offered_batches(checkpoints, meta.schedule))
    document, io = geometry.open_metadata_document(str(args.metadata), props)
    report = health.health_report(document, io, truth.rows)
    out_dir = Path(str(args.out))
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / health.TABLE_HEALTH_FILE
    path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    deletes = cast(dict[str, dict[str, int]], report["delete_files"])
    print(
        f"TABLE_HEALTH out={path} data_files={report['data_files']} rows_per_live_row={report['rows_per_live_row']} "
        + " ".join(f"{kind}_files={figures['files']}" for kind, figures in deletes.items())
    )
    return 0


def build_upsert_scan_plan_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="upsert-scan-plan", description="Print the snapshots an upsert run's live-state scan must read."
    )
    add_catalog_arguments(parser, table_required=False)
    parser.add_argument("--metadata", required=True, metavar="URI_OR_PATH", help="immutable final table metadata")
    parser.add_argument("--checkpoints", required=True, metavar="URI", help="the scorer's checkpoints directory")
    return parser


def upsert_scan_plan(argv: Sequence[str] | None = None) -> int:
    parser = build_upsert_scan_plan_parser()
    args = parser.parse_args(argv)
    props = load_catalog_props(
        [str(prop) for prop in args.catalog_prop], [str(name) for name in args.catalog_prop_file]
    )
    props.setdefault("py-io-impl", FSSPEC_FILE_IO)
    snapshots = upsert_verify.scan_snapshot_ids(str(args.metadata), str(args.checkpoints), props)
    print(",".join(str(snapshot) for snapshot in snapshots))
    return 0
