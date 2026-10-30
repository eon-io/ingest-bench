# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Validate publication requirements for every result under ``results/``.

Check for leaked identifiers, unsupported corpora, missing disclosures,
reused resources, and a stale results table independently of collection.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import math
import re
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

import yaml

from ingest_bench.collect.leaderboard import load_tracks, track_mismatches
from ingest_bench.collect.paths import result_relative_path
from ingest_bench.collect.privacy import (
    ACCOUNT_DIGITS,
    ACCOUNT_KEY,
    CLOUD_ACCOUNT_ID,
    OBJECT_STORE_URI,
    PLACEHOLDER_ACCOUNT_ID,
    URI_AUTHORITY_PLACEHOLDER,
    captured,
)
from ingest_bench.collect.schema import (
    LEGACY_SCHEMA_VERSIONS,
    Cost,
    Exactness,
    FleetRole,
    ResultDocument,
    Run,
    ingest_only_failures,
    parse_result,
    scoring_failures,
)
from ingest_bench.collect.table import (
    readme_for,
    readme_prefix,
    render_readme_block,
    render_results_table,
    splice_readme,
)
from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.corpus.preset import corpus_hash, load_preset
from ingest_bench.corpus.values import corpus_sizing
from ingest_bench.pricing import resource_cost
from ingest_bench.specs.model import (
    EXACTLY_ONCE,
    MACHINE_TYPE_UNSPECIFIED,
    PLACEHOLDER,
    SUSTAINABLE,
    RunSpec,
    parse_run_spec,
    without_freshness_bounds,
)
from ingest_bench.workloads import canonical_workload_name, workloads_dir

RESULTS_MD = "RESULTS.md"

# A full commit from a clean tree. Staging marks a dirty one with a suffix.
_HARNESS_COMMIT = re.compile(r"[0-9a-f]{40}")
# Pod placement names the operator's nodes. Specs ship placeholders for it, so it is not part of what ran.
_PLACEMENT_CONF = re.compile(
    r"spark\.kubernetes\.(driver|executor)\.node\.selector\..+|kubernetes\.(taskmanager|jobmanager)\.node-selector"
)
_PLACEMENT_FIELDS = frozenset({"node_selector", "tolerations"})

# The specs directory in a checkout, beside the workloads.
_SPECS_DIR = Path(__file__).resolve().parents[2] / "specs"


def _uri_failures(text: str) -> list[str]:
    return [
        f"uri: an unredacted URI ({match.group()!r}) was not rewritten to a placeholder root"
        for match in OBJECT_STORE_URI.finditer(text)
        if URI_AUTHORITY_PLACEHOLDER.fullmatch(captured(match)) is None
    ]


def _account_id_failures(value: object) -> list[str]:
    """Find AWS account IDs in text or JSON string values and keys.

    A bare 12-digit value is unclassified unless its context names an account:
    the patterns in ``CLOUD_ACCOUNT_ID``, or a mapping key that labels it.
    """
    if isinstance(value, str):
        return [
            f"account_id: an AWS account ID ({captured(match)}) appears in a string"
            for match in CLOUD_ACCOUNT_ID.finditer(value)
            if captured(match) != PLACEHOLDER_ACCOUNT_ID
        ]
    if isinstance(value, dict):
        failures = [
            f"account_id: an AWS account ID ({captured(match)}) appears in a key"
            for key in value
            for match in CLOUD_ACCOUNT_ID.finditer(key)
            if captured(match) != PLACEHOLDER_ACCOUNT_ID
        ]
        for key, inner in value.items():
            # A key such as `account_id` or `glue.id` labels a bare value as an account.
            text = str(inner) if isinstance(inner, str | int) and not isinstance(inner, bool) else None
            if (
                text is not None
                and ACCOUNT_KEY.fullmatch(key)
                and ACCOUNT_DIGITS.fullmatch(text)
                and text != PLACEHOLDER_ACCOUNT_ID
            ):
                failures.append(f"account_id: an AWS account ID ({text}) is the value of {key!r}")
            failures.extend(_account_id_failures(inner))
        return failures
    if isinstance(value, list):
        return [failure for inner in value for failure in _account_id_failures(inner)]
    return []


def _fleet_failures(fleet: list[FleetRole]) -> list[str]:
    if not fleet:
        return ["fleet: run.fleet is empty"]
    failures = []
    for role in fleet:
        name = role["role"]
        # Reject empty, unspecified, and unreplaced example machine types.
        machine_type = role["machine_type"]
        if not machine_type or machine_type == MACHINE_TYPE_UNSPECIFIED or machine_type.startswith(PLACEHOLDER):
            failures.append(f"fleet: role {name!r} has no machine_type ({machine_type!r})")
        for field, value in (("count", role["count"]), ("vcpu", role["vcpu"]), ("gib", role["gib"])):
            if value <= 0:
                failures.append(f"fleet: role {name!r} has a non-positive {field} ({value!r})")
    return failures


def _cost_failures(run: Run, cost: Cost) -> list[str]:
    """Recompute the fleet's hourly and total cost from the record's own fleet and pricing."""
    usd_per_hour = cost["usd_per_hour"]
    if usd_per_hour is None:
        return ["derived.cost.usd_per_hour: is null — a published result must disclose resource cost"]
    site_pricing = run["site_pricing"]
    try:
        expected_hourly = sum(
            role["count"] * resource_cost(role["vcpu"], role["gib"], role["machine_type"], site_pricing)
            for role in run["fleet"]
        )
    except ValueError as err:
        return [str(err)]
    failures = []
    if not math.isclose(usd_per_hour, expected_hourly, rel_tol=1e-9):
        failures.append(
            "derived.cost.usd_per_hour: does not match the fleet cost recomputed from run.fleet and run.site_pricing"
        )
    usd, run_hours = cost.get("usd"), cost.get("run_hours")
    if (usd is None) != (run_hours is None):
        failures.append("derived.cost: usd and run_hours must both be null or both be numbers")
    elif usd is not None and run_hours is not None and not math.isclose(usd, usd_per_hour * run_hours, rel_tol=1e-9):
        failures.append("derived.cost.usd: does not match derived.cost.usd_per_hour times derived.cost.run_hours")
    return failures


def _corpus_failures(run: Run, workloads: Path) -> list[str]:
    spec = run["spec"]
    name = spec["corpus"]
    shipped_presets = {preset_path.stem for preset_path in (workloads / "presets").glob("*.yaml")}
    if canonical_workload_name(name) not in shipped_presets:
        return [f"corpus: {name!r} is not a shipped preset (workloads/presets/*.yaml)"]
    corpus_hash_value = run["corpus_hash"]
    if corpus_hash_value is None:
        return ["corpus_hash: run.corpus_hash is null — the result was collected before it was scored"]
    seed = run.get("corpus_seed")
    if seed is None:
        return ["corpus_seed: the result does not record its corpus seed"]
    version = run.get("corpus_generator_version")
    if version != GENERATOR_VERSION:
        return [f"corpus_generator_version: expected {GENERATOR_VERSION!r}, got {version!r}"]
    row_block = run.get("corpus_row_block")
    if row_block is None or row_block < 1:
        return ["corpus_row_block: the result must record a positive generator row block size"]
    preset = load_preset(name, workloads_dir=workloads)
    encoding = run["kafka"]["value_encoding"]
    stored_encoding = "avro" if encoding == "confluent" else encoding
    if stored_encoding != preset.value_encoding:
        return [f"value_encoding: {encoding!r} does not match the shipped preset's {preset.value_encoding!r}"]
    expected = corpus_hash(preset, seed, row_block=row_block)
    if corpus_hash_value != expected:
        return [
            f"corpus_hash: {corpus_hash_value!r} does not match the shipped preset's {expected!r} "
            "for the recorded generator inputs — the corpus identity or preset differs"
        ]
    corpus = run["corpus"]
    if corpus is None:
        return []
    sizing = corpus_sizing(preset, seed) if preset.sizing_encoding is not None else None
    return [
        f"run.corpus.{field}: {actual!r} does not match the shipped preset's {expected_value!r}"
        for field, actual, expected_value in (
            ("schema", corpus["schema"], preset.schema_name),
            (
                "offered_bytes_per_s",
                corpus["offered_bytes_per_s"],
                preset.offered_bytes_per_s if sizing is None else sizing.offered_bytes_per_s,
            ),
            ("duration_s", corpus["duration_s"], preset.duration_s),
            ("partition_count", corpus["partition_count"], preset.partition_count),
            ("alpha", corpus["alpha"], preset.alpha),
            (
                "target_row_bytes",
                corpus["target_row_bytes"],
                preset.target_row_bytes if sizing is None else sizing.target_row_bytes,
            ),
            ("batch_interval_ms", corpus["batch_interval_ms"], preset.batch_interval_ms),
        )
        if actual != expected_value
    ]


def _harness_commit_failures(document: ResultDocument) -> list[str]:
    if document["schema_version"] in LEGACY_SCHEMA_VERSIONS:
        return []
    commit = document["run"].get("harness_commit")
    if commit is None or _HARNESS_COMMIT.fullmatch(commit) is None:
        return [
            f"run.harness_commit: is {commit!r}, but publication requires the full commit of a clean harness "
            "(images built by scripts/push-images.sh without --allow-dirty)"
        ]
    return []


def _track_failures(document: ResultDocument, workloads: Path) -> list[str]:
    """Require a sustainable result to conform to a shipped track, the unit results compare within."""
    spec = cast(dict[str, object], document["run"]["spec"])
    if cast(dict[str, object], spec.get("scoring") or {}).get("mode", SUSTAINABLE) != SUSTAINABLE:
        return []
    tracks = load_tracks(workloads)
    named = spec.get("track")
    if named is not None:
        track = next((track for track in tracks if track.name == canonical_workload_name(cast(str, named))), None)
        if track is None:
            return [f"track: {named!r} is not a shipped track (workloads/tracks/*.yaml)"]
        return [f"track: {mismatch}" for mismatch in track_mismatches(document, track)]
    if any(not track_mismatches(document, track) for track in tracks):
        return []
    corpus = document["run"]["corpus"]
    same_corpus = [
        track for track in tracks if corpus is not None and track.corpus == canonical_workload_name(corpus["name"])
    ]
    detail = "; ".join(mismatch for track in same_corpus for mismatch in track_mismatches(document, track))
    return [f"track: conforms to no shipped track (workloads/tracks/*.yaml){': ' + detail if detail else ''}"]


def _exactness_failures(document: ResultDocument) -> list[str]:
    """Require every recorded SLA surface to agree with the configured contract."""
    run = document["run"]
    configured = run["spec"].get("scoring", {}).get("exactness", EXACTLY_ONCE)
    summary = document["data"]["summary"]
    failures: list[str] = []
    if summary is None:
        return failures
    if summary["exactness_mode"] != configured:
        failures.append(
            f"data.summary.exactness_mode: {summary['exactness_mode']!r} does not match "
            f"run.spec.scoring.exactness {configured!r}"
        )

    surfaces: list[tuple[str, Exactness | None]] = [
        ("data.summary.exactness", summary.get("exactness")),
        ("data.exactness", document["data"].get("exactness")),
        ("derived.exactness", document["derived"]["exactness"]),
    ]
    failed_surfaces = []
    for field, exactness in surfaces:
        if exactness is None:
            continue
        mode = exactness["mode"]
        if mode != configured:
            failures.append(f"{field}.mode: {mode!r} does not match run.spec.scoring.exactness {configured!r}")
        loss, duplicates, corrupt = exactness["loss_rows"], exactness["duplicate_rows"], exactness["corrupt_batches"]
        stray = (exactness.get("invalid_rows", 0), exactness.get("unoffered_rows", 0))
        counts = (loss, duplicates, corrupt, *stray)
        if any(count < 0 for count in counts):
            failures.append(f"{field}: loss, duplicate, corrupt, invalid, and unoffered counts must be non-negative")
            continue
        strict = not any(counts)
        if exactness["exact"] != strict:
            failures.append(f"{field}.exact: does not match the recorded fault counts")
        # Only duplicates are ever permitted; rows outside the offer fail both modes.
        expected_verdict = strict if mode == EXACTLY_ONCE else loss == 0 and corrupt == 0 and not any(stray)
        if exactness["verdict"] != expected_verdict:
            failures.append(f"{field}.verdict: does not match mode {mode!r} and the recorded counts")
        if summary["run_valid"] and not exactness["verdict"]:
            failed_surfaces.append(f"{field}.verdict")

    if failed_surfaces:
        failures.append("data.summary.run_valid: is true despite failed verdicts: " + ", ".join(failed_surfaces))

    if summary["run_valid"] and document["derived"]["exactness"] is None:
        failures.append("derived.exactness: is null for a run with data.summary.run_valid true")
    return failures


def _scorer_identity_failures(run: Run, summary: object) -> list[str]:
    """Require recorded scorer identity to agree with the collected corpus."""
    if not isinstance(summary, dict):
        return []
    values = cast(dict[str, object], summary)
    failures = []
    for field, run_field in (
        ("corpus_hash", "corpus_hash"),
        ("corpus_seed", "corpus_seed"),
        ("corpus_generator_version", "corpus_generator_version"),
        ("corpus_row_block", "corpus_row_block"),
    ):
        if field in values and values[field] is not None and values[field] != run.get(run_field):
            failures.append(f"data.summary.{field}: must equal run.{run_field}")
    return failures


def document_failures(document: ResultDocument, *, workloads: Path) -> list[str]:
    """Check publication rules for one parsed document, independent of its destination.

    Invalid runs may be published as failed probes. Notes, resource reuse and
    generated table checks belong to directory validation.
    """
    run = document["run"]
    spec = run["spec"]
    failures = _uri_failures(json.dumps(document)) + _account_id_failures(document)

    seconds = spec.get("producer", {}).get("seconds")
    corpus = run["corpus"]
    # An absent or null limit denotes a full corpus offer.
    if (
        spec.get("scoring", {}).get("mode", "sustainable") == "sustainable"
        and seconds is not None
        and (corpus is None or seconds < corpus["duration_s"])
    ):
        failures.append(
            f"producer.seconds: spec.producer.seconds ({seconds!r}) must be at least "
            "run.corpus.duration_s; corpus metadata is required for publication"
        )

    failures.extend(_corpus_failures(run, workloads))
    if corpus is None:
        failures.append("run.corpus: is null — a published result must include corpus metadata")
    else:
        if corpus["hash"] != run["corpus_hash"]:
            failures.append("run.corpus.hash: must equal run.corpus_hash")
        if canonical_workload_name(corpus["name"]) != canonical_workload_name(spec["corpus"]):
            failures.append("run.corpus.name: must equal run.spec.corpus")

    derived = document["derived"]
    failures.extend(_cost_failures(run, derived["cost"]))
    if derived["keepup"] is None:
        failures.append("derived.keepup: is null")
    if derived["producer"]["producer_bound"] is None:
        failures.append("derived.producer.producer_bound: is null")

    data = document["data"]
    if data["summary"] is None:
        failures.append("data.summary: is null — a published result must include the scorer's summary")
    else:
        failures.extend(_scorer_identity_failures(run, data["summary"]))
        if data["summary"]["run_valid"]:
            failures.extend(ingest_only_failures(document))
    failures.extend(_exactness_failures(document))
    failures.extend(scoring_failures(document))

    failures.extend(_fleet_failures(run["fleet"]))
    failures.extend(_harness_commit_failures(document))
    failures.extend(_track_failures(document, workloads))

    site_pricing = run["site_pricing"]
    # Example zero prices are placeholders, not a valid cost disclosure.
    for field, price in (
        ("vcpu_hour_usd", site_pricing.get("vcpu_hour_usd", 0.0)),
        ("gib_hour_usd", site_pricing.get("gib_hour_usd", 0.0)),
    ):
        if "machine_types" not in site_pricing and price <= 0:
            failures.append(f"site_pricing.{field}: is {price!r}, so the cost column has no price behind it")

    return failures


def _freshness_failures(documents: list[tuple[Path, ResultDocument]]) -> list[str]:
    """Find table and topic names reused across published results."""
    failures: list[str] = []
    for field in ("table", "topic"):
        seen: dict[str, Path] = {}
        for path, document in documents:
            run = document["run"]
            name = run.get("table") if field == "table" else run.get("topic")
            if name is None:
                failures.append(f"{path}: {field}: run.{field} is absent, so its freshness cannot be checked")
                continue
            if name in seen:
                failures.append(f"{path}: {field}: {name!r} is also the {field} of {seen[name]}")
                continue
            seen[name] = path
    return failures


def _without_placement(value: object) -> object:
    """Drop placement keys, and any mapping that held nothing else."""
    if not isinstance(value, dict):
        return value
    kept: dict[str, object] = {}
    for key, item in cast(dict[str, object], value).items():
        if key in _PLACEMENT_FIELDS or _PLACEMENT_CONF.fullmatch(key):
            continue
        stripped = _without_placement(item)
        if item and stripped == {}:
            continue
        kept[key] = stripped
    return kept


def _spec_identity(spec: dict[str, object], workloads: Path) -> RunSpec:
    """A spec as it ran, less its name, track label, pod placement and track freshness bounds.

    Compare parsed specs, with defaults applied, so a committed spec that names its
    track matches the expanded copy a result embeds. A committed spec keeps its
    placeholder node selectors when the run used the operator's own.
    """
    unbounded = without_freshness_bounds(cast(dict[str, object], _without_placement(spec)))
    parsed = parse_run_spec(unbounded, workloads=workloads)
    return dataclasses.replace(parsed, name="", track=None, corpus=canonical_workload_name(parsed.corpus))


def _committed_spec_failures(documents: list[tuple[Path, ResultDocument]], specs: Path, workloads: Path) -> list[str]:
    """Require the spec behind each result to be committed, so anyone can rerun it."""
    failures: list[str] = []
    committed = []
    for path in sorted(specs.glob("**/*.yaml")):
        try:
            committed.append(_spec_identity(yaml.safe_load(path.read_text(encoding="utf-8")), workloads))
        except ValueError as err:
            failures.append(f"{path}: spec: {err}")
    for path, document in documents:
        try:
            identity = _spec_identity(cast(dict[str, object], document["run"]["spec"]), workloads)
        except ValueError as err:
            failures.append(f"{path}: spec: {err}")
            continue
        if identity not in committed:
            failures.append(
                f"{path}: spec: no spec under {specs} matches run.spec (names and node placement aside). "
                "Commit the spec that ran"
            )
    return failures


def validate(results_dir: Path, *, workloads: Path, specs: Path) -> list[str]:
    """Return failures as ``<path>: <rule>: <detail>`` lines.

    An empty results directory is valid if its generated table is current. A
    missing directory is a failure, not an empty result set.
    """
    if not results_dir.is_dir():
        return [f"{results_dir}: not an existing directory"]
    failures: list[str] = []
    documents: list[tuple[Path, ResultDocument]] = []
    all_renderable = True

    for path in sorted(results_dir.glob("**/*.md")):
        text = path.read_text(encoding="utf-8")
        failures.extend(f"{path}: {failure}" for failure in _uri_failures(text) + _account_id_failures(text))

    for path in sorted(results_dir.glob("**/*.json")):
        notes_path = path.with_name("README.md")
        text = path.read_text(encoding="utf-8")
        # Files rejected before document_failures still need privacy checks.
        try:
            value = json.loads(text)
        except json.JSONDecodeError as err:
            failures.append(f"{path}: json: {err}")
            failures.extend(f"{path}: {failure}" for failure in _uri_failures(text))
            all_renderable = False
            continue
        try:
            document = parse_result(value)
        except ValueError as err:
            failures.append(f"{path}: {err}")
            failures.extend(f"{path}: {failure}" for failure in _uri_failures(text) + _account_id_failures(value))
            all_renderable = False
            continue
        if document["data"]["summary"] is None or document["run"]["corpus"] is None:
            all_renderable = False
        documents.append((path, document))
        failures.extend(f"{path}: {failure}" for failure in document_failures(document, workloads=workloads))
        if document["run"]["corpus"] is not None:
            try:
                expected_path = result_relative_path(document)
                if path.relative_to(results_dir) != expected_path:
                    failures.append(f"{path}: path: must be {expected_path.as_posix()} relative to {results_dir}")
            except ValueError as err:
                failures.append(f"{path}: path: {err}")
        if document["run"]["engine"] == "external" and not notes_path.is_file():
            failures.append(f"{path}: notes: an external engine requires {notes_path}")

    # Skip table comparison if a document cannot be rendered; its parse failure
    # is already reported above.
    failures.extend(_freshness_failures(documents))
    failures.extend(_committed_spec_failures(documents, specs, workloads))

    if all_renderable:
        results_md_path = results_dir / RESULTS_MD
        try:
            rendered = render_results_table(documents, results_dir=results_dir, workloads=workloads)
        except ValueError as err:
            failures.append(str(err))
            return failures
        current = results_md_path.read_text(encoding="utf-8") if results_md_path.exists() else None
        if current != rendered:
            failures.append(
                f"{results_md_path}: results_md: does not match a fresh render of {results_dir} — "
                "run results-table again"
            )
        readme = readme_for(results_dir)
        if readme.is_file():
            text = readme.read_text(encoding="utf-8")
            block = render_readme_block(
                documents, workloads=workloads, results_dir=results_dir, prefix=readme_prefix(results_dir)
            )
            spliced = splice_readme(text, block)
            if spliced is not None and spliced != text:
                failures.append(
                    f"{readme}: results_block: does not match a fresh render of {results_dir}. Run results-table again"
                )

    return failures


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="validate-results",
        description="Check every published result under results/ against the public-repo publication rules.",
    )
    parser.add_argument(
        "results_dir",
        nargs="?",
        default="results",
        metavar="DIR",
        help="the results directory to check (default: results/)",
    )
    parser.add_argument(
        "--workloads",
        metavar="DIR",
        help="where the shipped presets and tracks live (default: the workloads/ directory shipped beside the package)",
    )
    parser.add_argument(
        "--specs",
        metavar="DIR",
        default=str(_SPECS_DIR),
        help="where the run specs behind results are committed (default: the checkout's specs/ directory)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    results_dir = Path(str(args.results_dir))
    workloads = workloads_dir(None if args.workloads is None else str(args.workloads))
    failures = validate(results_dir, workloads=workloads, specs=Path(str(args.specs)))
    for line in failures:
        print(line)
    if failures:
        print(f"{len(failures)} failure(s)", file=sys.stderr)
        return 1
    print(f"OK: {results_dir} holds only publishable results")
    return 0
