# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate final row-ID coverage in an immutable table metadata document."""

from __future__ import annotations

import json
import os
import sys
from collections.abc import Callable, Sequence
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO, cast

import numpy as np
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntryStatus
from pyiceberg.table.metadata import TableMetadata

from ingest_bench import uri
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.producer import publish_log
from ingest_bench.scorer import live_rows
from ingest_bench.scorer.exactness import exactness_result
from ingest_bench.scorer.geometry import open_metadata_document
from ingest_bench.scorer.snapshots import AddedFile, check_table_schema, snapshot_by_id
from ingest_bench.scorer.tally import BatchTally, read_id_column
from ingest_bench.specs.model import EXACTLY_ONCE

TERMINAL_FILE = "terminal-exactness.json"
PROGRESS_EVERY_FILES = 1_000

PASS = 0
DATA_FAILURE = 2
ERROR = 1


@dataclass(frozen=True)
class VerifyArgs:
    metadata_location: str
    corpus_uri: str
    publish_logs_uri: str
    catalog_props: dict[str, str]
    out_dir: Path
    expected_publish_shards: int = 1
    read_workers: int = 32
    exactness_mode: str = EXACTLY_ONCE


class VerdictFailure(ValueError):
    """Readable evidence that cannot support a passing verdict.

    Put storage URIs in ``details`` so collection can redact them. Root-based
    redaction does not recognize paths embedded inside reason text.
    """

    def __init__(self, reason: str, details: dict[str, object] | None = None) -> None:
        super().__init__(reason)
        self.details = {} if details is None else details


def _write(path: Path, document: dict[str, object]) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _metadata_identity(location: str, document: TableMetadata) -> dict[str, object]:
    return {
        "location": location,
        "table_location": document.location,
        "table_uuid": str(document.table_uuid),
    }


def _offer_report(expected_shards: int) -> dict[str, object]:
    return {
        "complete": False,
        "expected_shards": expected_shards,
        "completed_shards": [],
        "batches": 0,
        "rows": 0,
        "delivery_errors": 0,
    }


def _read_json_lines(text: str, source: str) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as error:
            raise VerdictFailure(f"{source}:{line_number} is not valid JSON: {error.msg}") from error
        if not isinstance(raw, dict):
            raise VerdictFailure(f"{source}:{line_number} must hold a JSON object")
        rows.append(cast(dict[str, object], raw))
    return rows


def _read_offer(
    location: str, records: Sequence[BatchRecord], expected_shards: int
) -> tuple[list[publish_log.PublishRecord], set[int], dict[str, object]]:
    """Read a completed producer offer and validate it against the corpus manifest."""
    report = _offer_report(expected_shards)
    names = publish_log.log_names(location) if uri.exists(location) else []
    by_shard: dict[int, str] = {}
    for name in names:
        try:
            shard = publish_log.shard_index(name)
        except ValueError as error:
            raise VerdictFailure(str(error), {"offer": report}) from error
        if shard in by_shard:
            raise VerdictFailure(
                f"publish logs name shard {shard} more than once: {by_shard[shard]!r} and {name!r}",
                {"offer": report},
            )
        by_shard[shard] = name

    expected = set(range(expected_shards))
    present = set(by_shard)
    issues: list[str] = []
    if missing := sorted(expected - present):
        issues.append(f"missing publish log(s) for shard(s) {missing}")
    if surplus := sorted(present - expected):
        issues.append(f"unexpected publish log(s) for shard(s) {surplus}")

    completed: list[int] = []
    for shard in sorted(expected & present):
        name = by_shard[shard]
        log_uri = uri.join(location, name)
        lines = _read_json_lines(uri.read_text(log_uri), log_uri)
        trailers = [index for index, raw in enumerate(lines) if "done" in raw]
        if len(trailers) != 1 or trailers[0] != len(lines) - 1 or lines[-1].get("done") is not True:
            issues.append(f"{name} has no single final completion trailer")
            continue
        trailer = lines[-1]
        batch_lines = len(lines) - 1
        try:
            trailer_shard = int(cast(int, trailer["shard"]))
            trailer_batches = int(cast(int, trailer["batches"]))
        except (KeyError, TypeError, ValueError):
            issues.append(f"{name} has a malformed completion trailer")
            continue
        if trailer_shard != shard:
            issues.append(f"{name} completion trailer names shard {trailer_shard}, expected {shard}")
        elif trailer_batches != batch_lines:
            issues.append(f"{name} completion trailer records {trailer_batches} batches, log holds {batch_lines}")
        else:
            completed.append(shard)
    report["completed_shards"] = completed
    if issues:
        raise VerdictFailure("; ".join(issues), {"offer": report})

    try:
        published = publish_log.read_all(location)
    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as error:
        raise VerdictFailure(f"invalid publish logs: {error}", {"offer": report}) from error
    offered = {record.batch for record in published}
    report.update(
        {
            "batches": len(published),
            "rows": sum(record.rows for record in published),
            "delivery_errors": sum(record.errors for record in published),
        }
    )
    faults: list[str] = []
    if not published:
        faults.append("the completed publish logs contain no offered batches")
    if offered and sorted(offered) != list(range(max(offered) + 1)):
        faults.append("the offered batch IDs are not a contiguous prefix beginning at 0")
    for record in published:
        if record.batch < 0 or record.batch >= len(records):
            faults.append(f"offered batch {record.batch} is outside the corpus manifest")
            continue
        expected_record = records[record.batch]
        if record.rows != expected_record.rows:
            faults.append(
                f"offered batch {record.batch} records {record.rows} rows, "
                f"corpus manifest expects {expected_record.rows}"
            )
    if report["delivery_errors"]:
        faults.append(f"the completed offer records {report['delivery_errors']} delivery errors")
    if faults:
        raise VerdictFailure("; ".join(faults), {"offer": report})
    report["complete"] = True
    return published, offered, report


def _live_files(document: TableMetadata, snapshot_id: int, io: FileIO) -> list[AddedFile]:
    """List live data files; position deletes are applied by the snapshot reader."""
    files: list[AddedFile] = []
    paths: set[str] = set()
    for manifest in snapshot_by_id(document, snapshot_id).manifests(io):
        for entry in manifest.fetch_manifest_entry(io, discard_deleted=True):
            if entry.status == ManifestEntryStatus.DELETED:
                continue
            data_file = entry.data_file
            if data_file.content == DataFileContent.EQUALITY_DELETES:
                raise ValueError(f"snapshot {snapshot_id} has unsupported equality deletes")
            if data_file.content != DataFileContent.DATA:
                continue
            if data_file.file_path in paths:
                raise VerdictFailure(
                    f"snapshot {snapshot_id} lists a live data file more than once; see `file_path`",
                    {"file_path": data_file.file_path},
                )
            paths.add(data_file.file_path)
            files.append(
                AddedFile(
                    path=data_file.file_path,
                    file_format=data_file.file_format.name.lower(),
                    record_count=data_file.record_count,
                    size_bytes=data_file.file_size_in_bytes,
                )
            )
    return files


def _scan(
    files: Sequence[AddedFile],
    tally: BatchTally,
    read_workers: int,
    *,
    snapshot_id: int,
    log: TextIO,
    read_ids: Callable[[AddedFile], np.ndarray] | None = None,
) -> tuple[int, int]:
    """Read bounded groups of ID columns and report periodic aggregate progress."""
    total_files = len(files)
    manifest_rows = sum(data_file.record_count for data_file in files)
    effective_workers = min(read_workers, total_files)
    print(
        f"TERMINAL_SCAN_START snapshot_id={snapshot_id} files={total_files} "
        f"manifest_rows={manifest_rows} read_workers={effective_workers}",
        file=log,
        flush=True,
    )
    if not files:
        print(
            f"TERMINAL_SCAN_COMPLETE snapshot_id={snapshot_id} files=0/0 rows=0/0",
            file=log,
            flush=True,
        )
        return 0, 0

    def read(file: AddedFile) -> np.ndarray:
        return read_id_column(file.path, file.file_format) if read_ids is None else read_ids(file)

    scanned_rows = 0
    files_scanned = 0
    pool = ThreadPoolExecutor(max_workers=effective_workers)
    try:
        reads = {pool.submit(read, data_file): data_file for data_file in files[:read_workers]}
        next_file = read_workers
        while reads:
            completed, _ = wait(reads, return_when=FIRST_COMPLETED)
            future = completed.pop()
            data_file = reads.pop(future)
            ids = future.result()
            if read_ids is None and ids.size != data_file.record_count:
                raise VerdictFailure(
                    f"{data_file.path} contains {ids.size} row IDs, manifest records {data_file.record_count} rows"
                )
            scanned_rows += ids.size
            tally.add_ids(ids)
            files_scanned += 1
            if files_scanned == total_files:
                label = "COMPLETE"
            elif files_scanned % PROGRESS_EVERY_FILES == 0:
                label = "PROGRESS"
            else:
                label = ""
            if label:
                print(
                    f"TERMINAL_SCAN_{label} snapshot_id={snapshot_id} "
                    f"files={files_scanned}/{total_files} rows={scanned_rows}/{manifest_rows}",
                    file=log,
                    flush=True,
                )
            del ids, future
            if next_file < len(files):
                pending = files[next_file]
                next_file += 1
                reads[pool.submit(read, pending)] = pending
    finally:
        pool.shutdown(wait=True, cancel_futures=True)
    return files_scanned, scanned_rows


def run(args: VerifyArgs, log: TextIO | None = None) -> int:
    """Write a fail-closed terminal exactness artifact and return its verdict code."""
    log = sys.stdout if log is None else log
    args.out_dir.mkdir(parents=True, exist_ok=True)
    artifact = args.out_dir / TERMINAL_FILE
    report: dict[str, object] = {
        "status": "running",
        "verdict": False,
        "reason": "terminal validation did not complete",
        "metadata": {"location": args.metadata_location},
        "snapshot_id": None,
        "validation_scope": "row_ids_only",
        "mode": args.exactness_mode,
        "offer": _offer_report(args.expected_publish_shards),
        "expected_rows": None,
        "rows": None,
        "loss_rows": None,
        "duplicate_rows": None,
        "unexpected_rows": None,
        "unoffered_rows": None,
        "invalid_rows": None,
        "files_scanned": 0,
        "scanned_rows": 0,
    }
    # Replace any result from an earlier invocation before reading external data.
    _write(artifact, report)
    try:
        corpus = corpus_metadata.read(args.corpus_uri)
        records = corpus_metadata.read_manifest(args.corpus_uri)
        document, io = open_metadata_document(args.metadata_location, args.catalog_props)
        report["metadata"] = _metadata_identity(args.metadata_location, document)
        snapshot_id = document.current_snapshot_id
        if snapshot_id is None:
            raise VerdictFailure("the metadata document has no current snapshot")
        snapshot = snapshot_by_id(document, snapshot_id)
        report.update(
            {
                "snapshot_id": snapshot_id,
                "snapshot_timestamp_ms": snapshot.timestamp_ms,
                "snapshot_sequence_number": snapshot.sequence_number,
            }
        )
        mismatches = check_table_schema(document.schema(), corpus)
        if mismatches:
            raise VerdictFailure("table schema mismatch: " + "; ".join(mismatches))

        _, offered, offer = _read_offer(args.publish_logs_uri, records, args.expected_publish_shards)
        report["offer"] = offer
        files = _live_files(document, snapshot_id, io)
        tally = BatchTally(records, corpus.p, track_unique=True)
        offered_mask = np.zeros(tally.counts.size, dtype=bool)
        offered_mask[list(offered)] = True
        tasks = live_rows.plan_files(document, snapshot_id, io)
        reader = live_rows.LiveIdReader(io, tasks.values(), args.read_workers)
        files_scanned, scanned_rows = _scan(
            files,
            tally,
            args.read_workers,
            snapshot_id=snapshot_id,
            log=log,
            read_ids=lambda file: reader.read(tasks[file.path]),
        )
        exactness = exactness_result(tally, offered_batches=offered, mode=args.exactness_mode)
        unexpected_rows = int(tally.unexpected_rows[offered_mask].sum())
        verdict = bool(exactness["verdict"])
        reason = None
        if not verdict:
            faults = [
                f"{name}={value}"
                for name, value in (
                    ("loss_rows", exactness["loss_rows"]),
                    ("duplicate_rows", exactness["duplicate_rows"]),
                    ("unexpected_rows", unexpected_rows),
                    ("unoffered_rows", exactness["unoffered_rows"]),
                    ("invalid_rows", exactness["invalid_rows"]),
                )
                if value
            ]
            reason = "terminal exactness: " + ", ".join(faults)
        report.update(exactness)
        report.update(
            {
                "status": "complete",
                "verdict": verdict,
                "reason": reason,
                "unexpected_rows": unexpected_rows,
                "files_scanned": files_scanned,
                "scanned_rows": scanned_rows,
                "manifest_rows": sum(data_file.record_count for data_file in files),
                "corpus_uri": corpus.uri,
                "corpus_hash": corpus.corpus_hash,
            }
        )
        _write(artifact, report)
        return PASS if verdict else DATA_FAILURE
    except VerdictFailure as error:
        report.update(error.details)
        report.update({"status": "failed", "verdict": False, "reason": str(error)})
        _write(artifact, report)
        return DATA_FAILURE
    except Exception as error:
        detail = f"{type(error).__name__}: {error}"
        if isinstance(error, FileNotFoundError):
            detail = f"No such file: {error}"
        report.update(
            {
                "status": "error",
                "verdict": False,
                "reason": detail,
            }
        )
        _write(artifact, report)
        return ERROR
