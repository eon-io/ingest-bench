# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Collect run artifacts and render the published results table.

Teardown collects partial results; finish collects again after geometry is
available. Both commands write the same document format.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from datetime import UTC, datetime
from importlib.metadata import version
from pathlib import Path
from typing import cast

from ingest_bench.collect.paths import result_relative_path
from ingest_bench.collect.run_json import RUN_JSON_FILE, build_run_json
from ingest_bench.collect.schema import ResultDocument, parse_result, readiness_failures
from ingest_bench.collect.table import (
    DEFAULT_VARIANT,
    readme_for,
    readme_prefix,
    render_readme_block,
    render_results_table,
    splice_readme,
)
from ingest_bench.collect.validate import document_failures
from ingest_bench.specs.model import load_site
from ingest_bench.workloads import workloads_dir

DISTRIBUTION = "lakehouse-ingest-bench"


def _result_path(root: Path, document: ResultDocument) -> Path:
    path = root / result_relative_path(document)
    if path.exists():
        existing_run_id = parse_result(json.loads(path.read_text(encoding="utf-8")))["run"]["run_id"]
        if existing_run_id != document["run"]["run_id"]:
            raise ValueError(f"{path}: already contains a different run ({existing_run_id}); cannot overwrite")
    return path


def _out_path(out: str | None, run_dir: Path, document: dict[str, object]) -> Path:
    """Resolve an explicit file or a trailing-slash canonical results root."""
    if out is None:
        return run_dir / RUN_JSON_FILE
    path = Path(out)
    if out.endswith("/"):
        try:
            return _result_path(path, parse_result(document))
        except ValueError as err:
            raise ValueError(f"{run_dir}: {err}") from err
    if path.is_dir():
        raise ValueError(f"--out {out}: is a directory; append '/' to derive the result path")
    return path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="collect",
        description="Assemble a publishable run result with private site details redacted.",
    )
    parser.add_argument("--run-dir", required=True, metavar="DIR", help="the run directory to collect")
    parser.add_argument(
        "--site",
        required=True,
        metavar="PATH",
        help="site configuration used for staging; its storage roots identify paths to redact",
    )
    output = parser.add_mutually_exclusive_group()
    output.add_argument(
        "--out",
        metavar="PATH",
        help="output file or results root ending in / (default: <run dir>/run.json)",
    )
    parser.add_argument(
        "--variant",
        default=DEFAULT_VARIANT,
        metavar="NAME",
        help=f"configuration label recorded in the result (default: '{DEFAULT_VARIANT}')",
    )
    output.add_argument("--publish", metavar="DIR", help="also publish the local run.json under this results root")
    parser.add_argument(
        "--publish-invalid",
        action="store_true",
        help="allow --publish for a run with run_valid false or missing compaction-readiness "
        "disclosures (terminal exactness, compactor health); it cannot waive a failed scoring check",
    )
    parser.add_argument("--workloads", metavar="DIR", help="preset and schema directory used to validate --publish")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.publish_invalid and args.publish is None:
        parser.error("--publish-invalid requires --publish")
    run_dir = Path(str(args.run_dir))
    variant = str(args.variant)
    collected_at = datetime.now(UTC)
    try:
        document = build_run_json(
            run_dir,
            load_site(Path(str(args.site))),
            harness_version=version(DISTRIBUTION),
            collected_at=collected_at,
            variant=variant,
        )
        serialized = json.dumps(document, indent=2, sort_keys=True) + "\n"
        missing = cast(list[str], document["missing"])
        out = None if args.out is None else str(args.out)
        if out is not None and out.endswith("/"):
            local_path = run_dir / RUN_JSON_FILE
            local_path.write_text(serialized, encoding="utf-8")
            print(f"COLLECTED out={local_path} schema_version={document['schema_version']} missing={len(missing)}")
        out_path = _out_path(out, run_dir, document)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(serialized, encoding="utf-8")
        # Name the saved report even when publication is refused.
        print(f"COLLECTED out={out_path} schema_version={document['schema_version']} missing={len(missing)}")
        # Name missing inputs so publication checks can distinguish missing scores
        # from optional measurements.
        for name in missing:
            print(f"  missing: {name}")
        if args.publish is not None:
            parsed = parse_result(document)
            failures = document_failures(parsed, workloads=workloads_dir(args.workloads))
            if failures:
                raise ValueError("cannot publish: " + "; ".join(failures))
            readiness = readiness_failures(parsed)
            if readiness and not args.publish_invalid:
                raise ValueError(
                    "cannot publish: " + "; ".join(readiness) + "; use --publish-invalid to record the failed run"
                )
            summary = parsed["data"]["summary"]
            if summary is not None and not summary["run_valid"] and not args.publish_invalid:
                raise ValueError("cannot publish: run_valid is false; use --publish-invalid to record the failed run")
            published_path = _result_path(Path(str(args.publish)), parsed)
            published_path.parent.mkdir(parents=True, exist_ok=True)
            published_path.write_text(serialized, encoding="utf-8")
            print(f"PUBLISHED out={published_path}")
    except (ValueError, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    return 0


# ---------------------------------------------------------------------------
# results-table
# ---------------------------------------------------------------------------


def load_results(results_dir: Path) -> list[tuple[Path, ResultDocument]]:
    """Load run documents under ``results_dir``; an empty directory yields no rows."""
    documents: list[tuple[Path, ResultDocument]] = []
    for path in sorted(results_dir.glob("**/*.json")):
        try:
            document = parse_result(json.loads(path.read_text(encoding="utf-8")))
        except ValueError as err:
            raise ValueError(f"{path}: {err}") from err
        documents.append((path, document))
    return documents


def build_results_table_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="results-table",
        description="Render every published result under a results directory into RESULTS.md.",
    )
    parser.add_argument("results_dir", metavar="DIR", help="the results directory to read, e.g. results/")
    parser.add_argument("--out", required=True, metavar="PATH", help="where to write the rendered table")
    parser.add_argument(
        "--workloads",
        metavar="DIR",
        help="where the shipped tracks live (default: the workloads/ directory shipped beside the package)",
    )
    return parser


def results_table_main(argv: Sequence[str] | None = None) -> int:
    args = build_results_table_parser().parse_args(argv)
    results_dir = Path(str(args.results_dir))
    if not results_dir.is_dir():
        print(f"error: {results_dir}: not an existing directory", file=sys.stderr)
        return 1
    workloads = workloads_dir(args.workloads)
    readme = readme_for(results_dir)
    try:
        documents = load_results(results_dir)
        text = render_results_table(documents, results_dir=results_dir, workloads=workloads)
        block = render_readme_block(
            documents, workloads=workloads, results_dir=results_dir, prefix=readme_prefix(results_dir)
        )
    except ValueError as err:
        print(f"error: {err}", file=sys.stderr)
        return 1
    out_path = Path(str(args.out))
    out_path.write_text(text, encoding="utf-8")
    print(f"RESULTS_TABLE out={out_path} rows={len(documents)}")
    # Refresh the README's summary only where the README opts in with both markers.
    spliced = splice_readme(readme.read_text(encoding="utf-8"), block) if readme.is_file() else None
    if spliced is not None:
        readme.write_text(spliced, encoding="utf-8")
        print(f"RESULTS_README out={readme}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
