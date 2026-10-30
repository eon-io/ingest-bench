# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Render deterministic workload comparisons from published run documents."""

from __future__ import annotations

import math
import os
import re
from collections import Counter, defaultdict
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, cast

from ingest_bench.collect.leaderboard import (
    CLASS_INGEST_ONLY,
    CLASSES,
    SIDECAR_CLASSES,
    comparison_class,
    freshness_mismatches,
    load_tracks,
    sidecar_roles,
    tier,
    track_for,
)
from ingest_bench.collect.paths import run_timestamp
from ingest_bench.collect.schema import (
    Corpus,
    FleetRole,
    Kafka,
    ResultDocument,
    Summary,
    parse_result,
    readiness_failures,
)
from ingest_bench.pricing import resource_cost
from ingest_bench.scorer.geometry import MIB
from ingest_bench.scorer.score import DRAINED, IDLE_STOP, PRODUCER_BOUND, VOID
from ingest_bench.specs.model import (
    DEFAULT_PARTITION,
    DEFAULT_WARMUP_S,
    EXACTLY_ONCE,
    LATEST_PER_KEY,
    SUSTAINABLE,
    TIER_REFERENCE,
    Track,
)
from ingest_bench.workloads import canonical_workload_name

# The label a result carries when its operator named no variant.
DEFAULT_VARIANT = "default"

_NBSP = "&nbsp;"
_ENGINE_LABELS = {"flink": "Flink", "spark": "Spark"}
_MODE_LABELS = {
    CLASS_INGEST_ONLY: "Ingest only",
    "integrated-compaction": "Integrated compaction",
    "spark-compaction": "Spark rewrite sidecar",
    "flink-maintenance": "Flink maintenance sidecar",
    "external-compaction": "External compaction",
}
_ROLE_LABELS = {
    "jobmanager": "JM",
    "taskmanager": "TM",
    "compactor-jobmanager": "maintenance JM",
    "compactor-taskmanager": "maintenance TM",
    "compactor-driver": "Spark driver",
    "compactor-executor": "Spark executor",
}
_ENCODING_LABELS = {"avro": "Avro", "json": "JSON", "confluent": "Confluent Avro"}
_TRACK_COLUMNS = (
    "Engine",
    "Mode",
    "Fleet",
    "Compute",
    "Freshness p50 / p95",
    "Median file",
    "Files < 32 MiB",
    "Details",
)
# Upsert tables leave readers merge-on-read work, shown beside the file geometry.
_UPSERT_TRACK_COLUMNS = (*_TRACK_COLUMNS[:-1], "Delete files", "Rows per live row", _TRACK_COLUMNS[-1])
_OTHER_COLUMNS = ("Engine", "Mode", "Version", "Fleet", "Compute", "Freshness p50 / p95", "Date", "Details")
_SUPERSEDED_COLUMNS = ("Engine", "Mode", "Fleet", "Compute", "Date", "Details")
_FAILED_COLUMNS = ("Engine", "Mode", "Fleet", "Compute", "State", "Reason", "Date", "Details")
# Measurements read best aligned on their digits.
_RIGHT_ALIGNED = frozenset(
    {"Compute", "Freshness p50 / p95", "Median file", "Files < 32 MiB", "Delete files", "Rows per live row"}
)
_DELETE_KINDS = (("equality", "eq"), ("position", "pos"), ("deletion_vector", "DV"))
_SATURATION_COLUMNS = (
    "Engine",
    "Class · tier",
    "Version",
    "Fleet",
    "vCPU / GiB",
    "Rows/s",
    "Source MB/s (est)",
    "Commit-to-commit rows/s",
    "Commit span (s)",
    "Start / end gap (s)",
    "First-seen difference (%)",
    "Appends",
    "Commit gap p95 (s)",
    "Max silence (s)",
    "p95 lag (s; diagnostic)",
    "File p50 (MiB)",
    "< 32 MiB",
    "$/h",
    "Date",
    "Result",
)
_SATURATION_INTRO = (
    "Saturation sections fix the offered workload, measurement window and compute budget. "
    "Rows show committed throughput under overload; freshness is diagnostic. MB/s estimates use average "
    "encoded corpus bytes per offered row. Commit-to-commit rates exclude edge intervals; compare their spans "
    "and start/end gaps with the full-window rate. First-seen difference is relative to the commit-clock rate. "
    "Validity establishes overload and exactness, not steady state or comparison precision. "
    "Compare commit settings and compaction as well as throughput; "
    "see [saturation methodology](../docs/saturation.md)."
)
_INVALID_COLUMNS = ("Engine", "Class · tier", "Version", "Fleet", "vCPU / GiB", "State", "Reason", "Date", "Result")
_INGEST_LEAD = "Requested compute for ingest without file compaction."
_COMPACTION_LEAD = "Requested compute for fleets that ingest data and rewrite small files into larger ones."
_COMPACTION_NOTE = (
    "Compaction is not yet included in the keep-up verdict. "
    "Review each run's notes for compaction backlog evidence before comparing."
)
_FALLBACK_NOTE = (
    "\\* No valid ingest-only run exists for this track. "
    "This figure reports only the ingest roles from a run that also used a compaction sidecar."
)
_NA = "n/a"


def _validity_cell(summary: Summary) -> str:
    state = summary["state"]
    if state in (PRODUCER_BOUND, VOID, DRAINED):
        return state
    if state == IDLE_STOP:
        # An undrained idle stop indicates insufficient ingest capacity.
        return "undersized"
    return "not drained"


def _fleet_cell(fleet: list[FleetRole]) -> str:
    if not fleet:
        return _NA
    if len({role["machine_type"] for role in fleet}) == 1:
        roles = " + ".join(f"{role['role']}×{role['count']}" for role in fleet)
        return f"{roles} ({fleet[0]['machine_type']})"
    return " + ".join(f"{role['role']}×{role['count']} ({role['machine_type']})" for role in fleet)


def _fmt(value: float | None, digits: int) -> str:
    return _NA if value is None else f"{value:.{digits}f}"


def _percent(value: float | None, digits: int) -> str:
    return _NA if value is None else f"{value * 100:.{digits}f}%"


def _version(document: ResultDocument) -> str:
    run = document["run"]
    versions = run["engine_versions"]
    if versions is None:
        return _NA
    if run["engine"] == "external":
        return f"{versions['name']} {versions['version']}"
    return versions["image"].rsplit("/", 1)[-1]


def _unit(value: str, unit: str) -> str:
    return f"{value}{_NBSP}{unit}"


def _whole(value: float) -> str:
    # Whole units read cleanly for real fleets. Halves round up, where round() would round to even.
    # A fraction of one keeps its decimals so it never reads as zero.
    return f"{value:g}" if value < 1 else str(math.floor(value + 0.5))


def _compute_cell(vcpu: float, gib: float) -> str:
    return f"{_unit(_whole(vcpu), 'vCPU')} · {_unit(_whole(gib), 'GiB')}"


def _role_label(role: str) -> str:
    # Keep each role on one line, so a cell breaks only between roles.
    return _ROLE_LABELS.get(role, role).replace(" ", _NBSP).replace("-", "\u2011")


def _fleet_list(fleet: list[FleetRole]) -> str:
    if not fleet:
        return _NA
    return "<br>".join(f"{role['count']}{_NBSP}×{_NBSP}{_role_label(role['role'])}" for role in fleet)


def _slug(title: str) -> str:
    """The anchor GitHub derives from a heading."""
    return re.sub(r"[^\w\- ]", "", title.lower()).replace(" ", "-")


def _duration(seconds: float) -> str:
    if seconds >= 3600:
        hours = seconds / 3600
        return f"{hours:g} hour" + ("" if hours == 1 else "s")
    minutes = seconds / 60
    return f"{minutes:g} minute" + ("" if minutes == 1 else "s")


@dataclass
class _Entry:
    path: Path
    document: ResultDocument
    corpus: Corpus
    summary: Summary
    staged_at: datetime
    track: Track | None
    tier: str
    comparison_class: str

    @property
    def engine_label(self) -> str:
        """The engine as ranked: an external engine by its declared product name."""
        run = self.document["run"]
        versions = run["engine_versions"]
        if run["engine"] == "external" and versions is not None:
            return versions["name"]
        return _ENGINE_LABELS.get(run["engine"], run["engine"])

    @property
    def class_cell(self) -> str:
        # Name a variant only where it tells rows apart beyond their class.
        variant = self.document["run"]["variant"]
        if variant in (DEFAULT_VARIANT, self.comparison_class):
            return self.comparison_class
        return f"{self.comparison_class} ({variant})"

    @property
    def mode_cell(self) -> str:
        variant = self.document["run"]["variant"]
        label = _MODE_LABELS.get(self.comparison_class, self.comparison_class)
        return label if variant in (DEFAULT_VARIANT, self.comparison_class) else f"{label} ({variant})"

    @property
    def gib(self) -> float:
        return sum(role["count"] * role["gib"] for role in self.document["run"]["fleet"])

    @property
    def order_key(self) -> tuple[bool, float, float, str]:
        """Cheapest first under the disclosed pricing, smallest compute first when a result has none."""
        cost = self.cost
        # One vCPU weighs as 8 GiB, the ratio of the published per-vCPU and per-GiB rates.
        compute = self.vcpu + self.gib / 8
        return (cost is None, 0.0 if cost is None else cost, compute, str(self.path))

    def details(self, results_dir: Path | None, prefix: str = "") -> str:
        """Link a run's notes, or its JSON when it has none.

        Without a results root the paths are relative to an unknown directory, so
        nothing is looked up on disk.
        """
        label, target = self.reading(results_dir)
        return f"[{label}]({prefix}{target})"

    def reading(self, results_dir: Path | None) -> tuple[str, str]:
        """What a reader should open for this run: its notes when published, otherwise its JSON."""
        if results_dir is not None:
            notes = self.path.with_name("README.md")
            if notes.is_file():
                return "notes", notes.relative_to(results_dir).as_posix()
        return "json", self.link_target(results_dir)

    def freshness_cell(self) -> str:
        freshness = self.document["derived"]["freshness"]
        if freshness is None:
            return _NA
        p50, p95 = freshness["window"]["p50_s"], freshness["window"]["p95_s"]
        if p50 is None or p95 is None:
            return _NA
        return f"{_unit(f'{p50:.0f}', 's')} / {_unit(f'{p95:.0f}', 's')}"

    @property
    def per_tb(self) -> float | None:
        cost = self.cost
        return None if cost is None else cost / (self.offered_rate * 3600 / 10**12)

    @property
    def mode(self) -> str:
        return self.document["run"]["spec"].get("scoring", {}).get("mode", SUSTAINABLE)

    @property
    def replay_s(self) -> float:
        limit = self.document["run"]["spec"].get("producer", {}).get("seconds")
        duration = self.corpus["duration_s"]
        return min(duration, limit if limit is not None else duration) / self.summary["speed"]

    @property
    def compute_budget(self) -> tuple[tuple[str, float, float], ...]:
        resources: dict[str, tuple[float, float]] = {}
        for role in self.document["run"]["fleet"]:
            cpu, memory = resources.get(role["machine_type"], (0.0, 0.0))
            resources[role["machine_type"]] = (cpu + role["count"] * role["vcpu"], memory + role["count"] * role["gib"])
        return tuple((machine, *resources[machine]) for machine in sorted(resources))

    @property
    def offered_rate(self) -> float:
        return self.corpus["offered_bytes_per_s"] * self.summary["speed"]

    @property
    def cost(self) -> float | None:
        pricing = self.document["run"]["site_pricing"]
        if "machine_types" not in pricing and pricing["vcpu_hour_usd"] == 0 and pricing["gib_hour_usd"] == 0:
            return None
        return self.document["derived"]["cost"]["usd_per_hour"]

    @property
    def p95(self) -> float | None:
        freshness = self.document["derived"]["freshness"]
        return None if freshness is None else freshness["window"]["p95_s"]

    @property
    def vcpu(self) -> float:
        return sum(role["count"] * role["vcpu"] for role in self.document["run"]["fleet"])

    @property
    def date(self) -> str:
        return self.staged_at.date().isoformat()

    @property
    def kafka(self) -> Kafka:
        return self.document["run"]["kafka"]

    @property
    def readiness_failures(self) -> list[str]:
        return readiness_failures(self.document)

    @property
    def freshness_failures(self) -> list[str]:
        return [] if self.track is None else freshness_mismatches(self.document, self.track)

    @property
    def valid(self) -> bool:
        return self.summary["run_valid"] and not self.readiness_failures and not self.freshness_failures

    def link_target(self, results_dir: Path | None) -> str:
        return (self.path if results_dir is None else self.path.relative_to(results_dir)).as_posix()

    def link(self, results_dir: Path | None, *, notes: bool = True) -> str:
        relative = self.path if results_dir is None else self.path.relative_to(results_dir)
        link = f"[json]({self.link_target(results_dir)})"
        if notes and self.path.with_name("README.md").is_file():
            link += f" · [notes]({relative.with_name('README.md').as_posix()})"
        return link

    def state_and_reason(self) -> tuple[str, str]:
        """Why an invalid row did not count: its state and the reasons recorded or found."""
        readiness = self.readiness_failures
        if readiness:
            if self.summary["run_valid"]:
                return "did-not-validate", "; ".join(readiness)
            recorded_reason = self.summary["reason"]
            reasons = [*([] if recorded_reason is None else [recorded_reason]), *readiness]
            return _validity_cell(self.summary), "; ".join(reasons)
        if self.summary["run_valid"] and self.freshness_failures:
            return "over-bound", "; ".join(self.freshness_failures)
        reason = self.summary["reason"]
        return _validity_cell(self.summary), _NA if reason is None else reason

    def cells(self, results_dir: Path | None) -> list[str]:
        """A saturation row: throughput under overload, or why the run did not count."""
        run = self.document["run"]
        fleet = run["fleet"]
        cells = [
            run["engine"],
            f"{self.class_cell} · {self.tier}",
            _version(self.document),
            _fleet_cell(fleet),
            f"{self.vcpu:g} / {self.gib:g}",
        ]
        if not self.valid:
            return [*cells, *self.state_and_reason(), self.date, self.link(results_dir)]
        geometry = self.document["geometry"]
        final = None if geometry is None else geometry["final"]
        file_p50_bytes = None if final is None else final["live"]["size_quantiles"]["p50"]
        small_file_share = None if final is None else final["live"]["small_file_share_32mib"]
        metrics = self.summary.get("saturation")
        assert metrics is not None  # Readiness requires a complete measurement.
        span = metrics.get("commit_to_commit")
        cells.extend(
            [
                _fmt(metrics["rows_per_s"], 1),
                _fmt(metrics["source_mb_per_s_estimate"], 2),
                _fmt(None if span is None else span["rows_per_s"], 1),
                _fmt(None if span is None else span["duration_s"], 1),
                f"{_fmt(metrics.get('start_edge_gap_s'), 1)} / {_fmt(metrics.get('end_edge_gap_s'), 1)}",
                _fmt(metrics.get("first_seen_rate_difference_pct"), 1),
                str(metrics["append_commits"]),
                _fmt(metrics["commit_gap_s"]["p95"], 1),
                _fmt(metrics["max_silence_s"], 1),
                _fmt(self.p95, 1),
                _fmt(None if file_p50_bytes is None else file_p50_bytes / MIB, 1),
                _percent(small_file_share, 0),
                _fmt(self.cost, 2),
            ]
        )
        return [*cells, self.date, self.link(results_dir)]


def _saturation_heading(entry: _Entry) -> str:
    corpus, summary, kafka = entry.corpus, entry.summary, entry.kafka
    key = "none" if kafka["key"] is None else kafka["key"]
    rate = f"{entry.offered_rate / 10**6:g} MB/s"
    if summary["speed"] != 1:
        rate += f" ({float(summary['speed'])}× {corpus['name']})"
    seconds = entry.replay_s
    duration = f"{seconds / 3600:g} h" if seconds >= 3600 else f"{seconds / 60:g} min"
    configured = entry.document["run"]["spec"].get("scoring", {})
    budget = "; ".join(f"{cpu:g} vCPU / {gib:g} GiB on {machine}" for machine, cpu, gib in entry.compute_budget)
    return (
        f"## {corpus['name']} ({corpus['hash']}) · saturation · offer {rate} for {duration} · "
        f"measure {configured.get('measurement_s', 0)} s after "
        f"{configured.get('warmup_s', DEFAULT_WARMUP_S)} s warmup · "
        f"{budget} · Kafka {kafka['partitions']} partitions, key={key}, "
        f"assignment={kafka['partition_assignment']}, {kafka['value_encoding']}/{kafka['compression']} · "
        f"exactness {summary['exactness_mode']}"
    )


def _upsert(entry: _Entry) -> bool:
    return entry.summary["exactness_mode"] == LATEST_PER_KEY


def _freshness_bounds(entry: _Entry) -> tuple[float, float]:
    """The p95 and max freshness bounds a row is held to: its track's, or those it was scored under."""
    if entry.track is not None:
        return entry.track.freshness_bound_s, entry.track.freshness_max_bound_s
    bound = entry.summary["freshness_bound_s"]
    return bound, 2 * bound


def _rate_and_keys(entry: _Entry) -> tuple[str, str]:
    # An upsert track's rows differ by what they replace, not by how its partition keys are skewed.
    if _upsert(entry):
        return f"{entry.offered_rate / 10**6:g} MB/s", "upsert"
    return f"{entry.offered_rate / 10**6:g} MB/s", "uniform" if float(entry.corpus["alpha"]) == 0 else "skewed"


def _title(entry: _Entry) -> str:
    rate, keys = _rate_and_keys(entry)
    return f"{rate} · {keys}" if _upsert(entry) else f"{rate} · {keys} keys"


def _column_label(entry: _Entry) -> str:
    rate, keys = _rate_and_keys(entry)
    return f"{rate} {keys}"


def _titles(sections: list[list[_Entry]]) -> dict[int, str]:
    """Unique titles for the sustainable sections, keyed by the ``id`` of each section's entries.

    Sections that share a title gain the first of track, corpus and exactness that
    varies among them, then the next, until they differ. Any that still collide, such
    as one untracked corpus under two Kafka setups, are numbered.
    """
    sustainable = [entries for entries in sections if entries[0].mode == SUSTAINABLE]
    shared: dict[str, list[list[_Entry]]] = defaultdict(list)
    for entries in sustainable:
        shared[_title(entries[0])].append(entries)
    titles: dict[int, str] = {}
    for title, group in shared.items():
        candidates = (
            ["" if entries[0].track is None else entries[0].track.name for entries in group],
            [f"{entries[0].corpus['name']} ({entries[0].corpus['hash']})" for entries in group],
            [entries[0].summary["exactness_mode"].lower().replace("_", " ") for entries in group],
        )
        parts: list[list[str]] = [[] for _ in group]
        for labels in candidates:
            if len(group) == len({" · ".join(part) for part in parts}):
                break
            if len(set(labels)) > 1:
                for part, label in zip(parts, labels, strict=True):
                    part.append(label)
        for entries, part in zip(group, parts, strict=True):
            titles[id(entries)] = " · ".join([title, *(label for label in part if label)])
    repeated = {title for title, count in Counter(titles.values()).items() if count > 1}
    seen: Counter[str] = Counter()
    for entries in sustainable:
        title = titles[id(entries)]
        if title in repeated:
            seen[title] += 1
            titles[id(entries)] = f"{title} · {seen[title]}"
    return titles


def _partition_label(partition: str) -> str:
    identity = re.fullmatch(r"identity\((\w+)\)", partition)
    return f"`{identity.group(1) if identity else partition}`"


def _workload_details(entry: _Entry) -> list[str]:
    corpus, summary, kafka = entry.corpus, entry.summary, entry.kafka
    rate = f"{entry.offered_rate / 10**6:g} MB/s"
    if summary["speed"] != 1:
        rate += f" ({float(summary['speed'])}× {corpus['name']})"
    key = "none" if kafka["key"] is None else f"`{kafka['key']}`"
    exactness = summary["exactness_mode"].lower().replace("_", "-").capitalize()
    bound, max_bound = _freshness_bounds(entry)
    partition = entry.document["run"]["spec"].get("table", {}).get("partition", DEFAULT_PARTITION)
    lines = ["<details><summary>Workload details</summary>", ""]
    if entry.track is not None:
        lines.append(f"- Track: `{entry.track.name}`")
    lines += [
        "- Replay",
        f"  - Rate: {rate}",
        f"  - Duration: {_duration(corpus['duration_s'] / summary['speed'])}",
        # An upsert corpus keys its messages by the row key, so its partition count is not a key count.
        f"  - `partition_key` values: {corpus['partition_count']}"
        if _upsert(entry)
        else f"  - Keys: {corpus['partition_count']}",
        f"  - Zipf alpha: {float(corpus['alpha'])}",
        f"  - Corpus hash: `{corpus['hash']}`",
        "- Kafka",
        f"  - Partitions: {kafka['partitions']}",
        f"  - Message key: {key}",
        f"  - Partition assignment: {kafka['partition_assignment'].replace('_', '-')}",
        f"  - Encoding: {_ENCODING_LABELS.get(kafka['value_encoding'], kafka['value_encoding'])}",
        f"  - Compression: {kafka['compression']}",
        "- Keep-up criteria",
        f"  - {exactness}",
        f"  - Freshness p95: ≤ {bound:g} s",
        f"  - Freshness max: ≤ {max_bound:g} s",
    ]
    if entry.track is not None:
        lines.append(f"  - Warm-up: ≤ {entry.track.max_warmup_s} s")
    return [*lines, f"- Iceberg partitioning: {_partition_label(partition)}", "", "</details>"]


def _row(cells: Sequence[str]) -> str:
    """One Markdown table row, with cell text that cannot break the row."""
    return "| " + " | ".join(cell.replace("|", "\\|").replace("\n", " ").replace("\r", " ") for cell in cells) + " |"


def _table(columns: Sequence[str], entries: Sequence[_Entry], results_dir: Path | None) -> list[str]:
    lines = ["| " + " | ".join(columns) + " |", "|" + "|".join("---" for _ in columns) + "|"]
    lines.extend(_row(entry.cells(results_dir)) for entry in entries)
    return lines


def _saturation_sort_key(entry: _Entry) -> tuple[float, str, str, str, datetime, str]:
    """Highest committed throughput first."""
    metrics = entry.summary.get("saturation")
    return (
        float("inf") if metrics is None else -metrics["rows_per_s"],
        entry.document["run"]["engine"],
        entry.class_cell,
        entry.tier,
        entry.staged_at,
        str(entry.path),
    )


def _latest(entries: list[_Entry]) -> tuple[list[_Entry], list[_Entry]]:
    """Split a section into each configuration's latest run and the runs it superseded, newest first.

    Select across both verdicts, so a newer failure supersedes an older pass.
    """
    latest: dict[tuple[str, str, str, str, str, tuple[tuple[str, int, float, float, str], ...]], _Entry] = {}
    superseded = []
    for entry in sorted(entries, key=lambda item: (item.staged_at, str(item.path)), reverse=True):
        run = entry.document["run"]
        signature = tuple(
            sorted(
                (role["role"], role["count"], role["vcpu"], role["gib"], role["machine_type"]) for role in run["fleet"]
            )
        )
        key = (run["engine"], entry.engine_label, entry.comparison_class, entry.tier, run["variant"], signature)
        if key in latest:
            superseded.append(entry)
        else:
            latest[key] = entry
    return list(latest.values()), superseded


def _saturation_section(entries: list[_Entry], results_dir: Path | None) -> list[str]:
    latest, superseded = _latest(entries)
    valid = sorted((entry for entry in latest if entry.valid), key=_saturation_sort_key)
    invalid = sorted(
        (entry for entry in latest if not entry.valid),
        key=lambda entry: (entry.vcpu, entry.staged_at, str(entry.path)),
    )
    lines = [_saturation_heading(entries[0]), "", *_table(_SATURATION_COLUMNS, valid, results_dir)]
    if invalid:
        lines.extend(["", "### Did not validate", "", *_table(_INVALID_COLUMNS, invalid, results_dir)])
    if superseded:
        history = []
        for entry in superseded:
            run = entry.document["run"]
            history.append(
                f"{run['engine']} {entry.class_cell} · {entry.tier}, {_fleet_cell(run['fleet'])}, "
                f"{entry.date} ({entry.link(results_dir, notes=False)})"
            )
        lines.extend(["", "Superseded: " + "; ".join(history)])
    return lines


def _with_tier(columns: Sequence[str], entries: Sequence[_Entry]) -> tuple[list[str], bool]:
    """Add a Tier column only when a row is at the reference tier, since every other row is tuned."""
    show = any(entry.tier == TIER_REFERENCE for entry in entries)
    return ([*columns[:2], "Tier", *columns[2:]] if show else list(columns)), show


def _tiered(entry: _Entry, cells: list[str], tier: bool) -> list[str]:
    return [*cells[:2], entry.tier, *cells[2:]] if tier else cells


def _track_cells(entry: _Entry, results_dir: Path | None) -> list[str]:
    geometry = entry.document["geometry"]
    final = None if geometry is None else geometry["final"]
    p50_bytes = None if final is None else final["live"]["size_quantiles"]["p50"]
    small = None if final is None else final["live"]["small_file_share_32mib"]
    return [
        entry.engine_label,
        entry.mode_cell,
        _fleet_list(entry.document["run"]["fleet"]),
        _compute_cell(entry.vcpu, entry.gib),
        entry.freshness_cell(),
        _NA if p50_bytes is None else _unit(f"{p50_bytes / MIB:.1f}", "MiB"),
        _percent(small, 0),
        entry.details(results_dir),
    ]


def _upsert_track_cells(entry: _Entry, results_dir: Path | None) -> list[str]:
    cells = _track_cells(entry, results_dir)
    health = cast(dict[str, Any] | None, entry.document.get("table_health"))
    if health is None:
        return [*cells[:-1], _NA, _NA, cells[-1]]
    counts = [(health["delete_files"][kind]["files"], label) for kind, label in _DELETE_KINDS]
    deletes = " + ".join(f"{files:,} {label}" for files, label in counts if files) or "0"
    per_live_row = health["rows_per_live_row"]
    return [*cells[:-1], deletes, _fmt(per_live_row, 2), cells[-1]]


def _other_cells(entry: _Entry, results_dir: Path | None) -> list[str]:
    return [
        entry.engine_label,
        entry.mode_cell,
        _version(entry.document),
        _fleet_list(entry.document["run"]["fleet"]),
        _compute_cell(entry.vcpu, entry.gib),
        entry.freshness_cell(),
        entry.date,
        entry.details(results_dir),
    ]


def _superseded_cells(entry: _Entry, results_dir: Path | None) -> list[str]:
    return [
        entry.engine_label,
        entry.mode_cell,
        _fleet_list(entry.document["run"]["fleet"]),
        _compute_cell(entry.vcpu, entry.gib),
        entry.date,
        entry.details(results_dir),
    ]


def _failed_cells(entry: _Entry, results_dir: Path | None) -> list[str]:
    return [
        entry.engine_label,
        entry.mode_cell,
        _fleet_list(entry.document["run"]["fleet"]),
        _compute_cell(entry.vcpu, entry.gib),
        *entry.state_and_reason(),
        entry.date,
        entry.details(results_dir),
    ]


def _markdown_table(columns: Sequence[str], rows: list[list[str]]) -> list[str]:
    rule = "|" + "|".join("---:" if column in _RIGHT_ALIGNED else "---" for column in columns) + "|"
    return ["| " + " | ".join(columns) + " |", rule, *map(_row, rows)]


def _rows_for(
    columns: Sequence[str],
    entries: list[_Entry],
    cells: Callable[[_Entry, Path | None], list[str]],
    results_dir: Path | None,
) -> list[str]:
    shown, tier = _with_tier(columns, entries)
    return _markdown_table(shown, [_tiered(entry, cells(entry, results_dir), tier) for entry in entries])


def _collapsed(summary: str, table: list[str], lead: str | None = None) -> list[str]:
    intro = [] if lead is None else [lead, ""]
    return ["", f"<details><summary>{summary}</summary>", "", *intro, *table, "", "</details>"]


def _shown_and_others(valid: list[_Entry]) -> tuple[list[_Entry], list[_Entry]]:
    """The cheapest valid run of each engine, class, tier and variant, and every other valid run."""
    shown: dict[tuple[str, str, str, str], _Entry] = {}
    others = []
    for entry in sorted(valid, key=lambda item: item.order_key):
        key = (entry.engine_label, entry.comparison_class, entry.tier, entry.document["run"]["variant"])
        if key in shown:
            others.append(entry)
        else:
            shown[key] = entry
    return list(shown.values()), others


def _track_section(entries: list[_Entry], results_dir: Path | None, title: str) -> list[str]:
    latest, superseded = _latest(entries)
    shown, others = _shown_and_others([entry for entry in latest if entry.valid])
    failed = sorted((entry for entry in latest if not entry.valid), key=lambda entry: entry.order_key)
    lines = [f"## {title}", "", *_workload_details(entries[0])]
    if shown:
        if _upsert(entries[0]):
            lines.extend(["", *_rows_for(_UPSERT_TRACK_COLUMNS, shown, _upsert_track_cells, results_dir)])
        else:
            lines.extend(["", *_rows_for(_TRACK_COLUMNS, shown, _track_cells, results_dir)])
    if others:
        table = _rows_for(_OTHER_COLUMNS, others, _other_cells, results_dir)
        lead = "Larger fleets and earlier engine versions."
        lines.extend(_collapsed(f"Other qualifying runs ({len(others)})", table, lead))
    if failed:
        lines.extend(["", "### Did not validate", "", *_rows_for(_FAILED_COLUMNS, failed, _failed_cells, results_dir)])
    if superseded:
        # History reads newest first, the order ``_latest`` returns.
        table = _rows_for(_SUPERSEDED_COLUMNS, superseded, _superseded_cells, results_dir)
        lines.extend(_collapsed(f"Superseded runs ({len(superseded)})", table))
    return lines


def _intro(
    sections: list[list[_Entry]], methodology: str, *, freshness_bounds: tuple[float, float] | None = None
) -> list[str]:
    """What a figure is and what keeping up means, with the bounds the sections share."""
    sustainable = [entries[0] for entries in sections if entries[0].mode == SUSTAINABLE]
    bounds = {_freshness_bounds(entry) for entry in sustainable}
    warmups = {None if entry.track is None else entry.track.max_warmup_s for entry in sustainable}
    shared_bounds = freshness_bounds
    if shared_bounds is None and len(bounds) == 1:
        (shared_bounds,) = bounds
    if shared_bounds is not None:
        p95, maximum = shared_bounds
        freshness = f"lag p95 ≤ {p95:g} s and max ≤ {maximum:g} s"
        (warmup,) = warmups if len(warmups) == 1 else (None,)
        if warmup is not None:
            freshness += f", after up to {warmup / 60:g} minutes of warm-up"
    else:
        freshness = "lag within its track's bounds"
    upsert = {_upsert(entry) for entry in sustainable}
    exactly_once = "every offered row is committed once, verified by row ID."
    latest = "each key's newest version is live, and no other, verified at sampled snapshots."
    if upsert == {True}:
        exactness = [f"- **Latest per key:** {latest}"]
    elif upsert == {True, False}:
        exactness = [
            f"- **Exactly-once** (append tracks): {exactly_once}",
            f"- **Latest per key** (upsert tracks): {latest}",
        ]
    else:
        exactness = [f"- **Exactly-once:** {exactly_once}"]
    return [
        "Each figure is the smallest fleet found so far that streamed an hour of Kafka data into Iceberg "
        "and kept up. Lower is better.",
        "",
        "Fleets shown here meet both criteria:"
        if freshness_bounds is not None
        else "A fleet keeps up when it meets both criteria:",
        "",
        *exactness,
        f"- **Freshness:** {freshness}.",
        "",
        f"See the [methodology]({methodology}) for details.",
    ]


@dataclass(frozen=True)
class _Cell:
    """One summary figure: a run's compute, or only its ingest roles when it stands in for an ingest-only run."""

    entry: _Entry
    vcpu: float
    gib: float
    cost: float
    fallback: bool

    def text(self, results_dir: Path | None, prefix: str) -> str:
        link = f"[{_compute_cell(self.vcpu, self.gib)}]({prefix}{self.entry.reading(results_dir)[1]})"
        return f"{link}{_NBSP}\\*" if self.fallback else link


def _ingest_cell(entry: _Entry) -> _Cell:
    """The roles of a sidecar run that are not the sidecar's, priced under the run's own rates."""
    roles = sidecar_roles(entry.document)
    pricing = entry.document["run"]["site_pricing"]
    fleet = [role for role in entry.document["run"]["fleet"] if role["role"] not in roles]
    vcpu = sum(role["count"] * role["vcpu"] for role in fleet)
    gib = sum(role["count"] * role["gib"] for role in fleet)
    cost = sum(
        role["count"] * resource_cost(role["vcpu"], role["gib"], role["machine_type"], pricing) for role in fleet
    )
    return _Cell(entry, vcpu, gib, cost, True)


def _summary(
    sections: list[list[_Entry]],
    titles: dict[int, str],
    results_dir: Path | None,
    *,
    prefix: str = "",
    page: str = "",
) -> list[str]:
    """Engines against tracks, the smallest valid fleet each: one table for ingest only, one with compaction.

    ``prefix`` rebases run links and ``page`` names the page the section anchors live on,
    so the README can link into ``RESULTS.md``.
    """
    tracked = [entries for entries in sections if entries[0].track is not None and entries[0].mode == SUSTAINABLE]
    best: dict[tuple[str, str, str], dict[str, _Entry]] = defaultdict(dict)
    # A track spans several sections when results used different corpus seeds. Its column links the first.
    columns: dict[str, tuple[str, str]] = {}
    for entries in tracked:
        name = cast(Track, entries[0].track).name
        columns.setdefault(name, (_column_label(entries[0]), f"{page}#{_slug(titles[id(entries)])}"))
        latest, _ = _latest(entries)
        for entry in latest:
            if not entry.valid:
                continue
            cells = best[(entry.comparison_class, entry.engine_label, entry.tier)]
            if name not in cells or entry.order_key < cells[name].order_key:
                cells[name] = entry
    # Tracks that share a rate and key distribution are told apart by name.
    shared_labels = Counter(label for label, _ in columns.values())
    columns = {
        name: (f"{label} ({name})" if shared_labels[label] > 1 else label, anchor)
        for name, (label, anchor) in columns.items()
    }
    table: dict[tuple[str, str, str], dict[str, _Cell]] = {
        key: {name: _Cell(entry, entry.vcpu, entry.gib, entry.cost or 0.0, False) for name, entry in cells.items()}
        for key, cells in best.items()
    }
    # An engine with no valid ingest-only run on a track, at any tier, shows the ingest
    # roles of its smallest sidecar run.
    ingest_tracks: dict[str, set[str]] = defaultdict(set)
    for (comparison, engine, _), cells in best.items():
        if comparison == CLASS_INGEST_ONLY:
            ingest_tracks[engine].update(cells)
    for (comparison, engine, tier_name), cells in list(best.items()):
        if comparison not in SIDECAR_CLASSES:
            continue
        for name, entry in cells.items():
            if name in ingest_tracks[engine]:
                continue
            ingest = table.setdefault((CLASS_INGEST_ONLY, engine, tier_name), {})
            candidate = _ingest_cell(entry)
            if name not in ingest or candidate.cost < ingest[name].cost:
                ingest[name] = candidate

    def rank(key: tuple[str, str, str]) -> tuple[bool, float, str]:
        # Mean cost per offered TB over the tracks a row has its own results on.
        costs = [cost for cell in table[key].values() if not cell.fallback and (cost := cell.entry.per_tb) is not None]
        return (not costs, sum(costs) / len(costs) if costs else 0.0, key[1])

    lines: list[str] = []
    groups = (
        ("Ingest only", (CLASS_INGEST_ONLY,), [_INGEST_LEAD]),
        (
            "With compaction",
            tuple(name for name in CLASSES.values() if name != CLASS_INGEST_ONLY),
            [_COMPACTION_LEAD, "", _COMPACTION_NOTE],
        ),
    )
    for heading, classes, lead in groups:
        rows = sorted((key for key in table if key[0] in classes and table[key]), key=rank)
        if not rows:
            continue
        names = [name for name in columns if any(name in table[key] for key in rows)]
        compaction = classes != (CLASS_INGEST_ONLY,)
        tier = any(key[2] == TIER_REFERENCE for key in rows)
        head = ["Engine", *(["Compaction method"] if compaction else []), *(["Tier"] if tier else [])]
        links = [f"[{columns[name][0]}]({columns[name][1]})" for name in names]
        lines.extend(["", f"## {heading}", "", *lead, ""])
        lines.append("| " + " | ".join([*head, *links]) + " |")
        lines.append("|" + "|".join(["---"] * len(head) + ["---:"] * len(names)) + "|")
        for key in rows:
            label = [key[1], *([_MODE_LABELS.get(key[0], key[0])] if compaction else []), *([key[2]] if tier else [])]
            values = [table[key][name].text(results_dir, prefix) if name in table[key] else _NA for name in names]
            lines.append(_row([*label, *values]))
        if any(cell.fallback for key in rows for cell in table[key].values()):
            lines.extend(["", _FALLBACK_NOTE])
    return lines


def _parse_sections(documents: Sequence[tuple[Path, object]], workloads: Path) -> tuple[list[list[_Entry]], set[str]]:
    """Group documents into ordered sections, reporting malformed ones by source path and field."""
    tracks = load_tracks(workloads)
    groups: dict[tuple[object, ...], list[_Entry]] = defaultdict(list)
    versions: set[str] = set()
    for path, document in sorted(documents, key=lambda item: str(item[0])):
        try:
            parsed = parse_result(document)
            corpus, summary = parsed["run"]["corpus"], parsed["data"]["summary"]
            if corpus is None:
                raise ValueError("run.corpus: is null; a results section requires corpus metadata")
            if summary is None:
                raise ValueError("data.summary: is null; a results row requires a scorer summary")
            entry = _Entry(
                path,
                parsed,
                corpus,
                summary,
                run_timestamp(parsed["run"]["run_id"]),
                track=track_for(parsed, tracks),
                tier=tier(parsed),
                comparison_class=comparison_class(parsed),
            )
            groups[
                (
                    None if entry.track is None else entry.track.name,
                    canonical_workload_name(corpus["name"]),
                    corpus["hash"],
                    summary["speed"],
                    _freshness_bounds(entry) if entry.mode == SUSTAINABLE else (0.0, 0.0),
                    summary["exactness_mode"],
                    entry.kafka["partitions"],
                    entry.kafka["key"],
                    entry.kafka["partition_assignment"],
                    entry.kafka["value_encoding"],
                    entry.kafka["compression"],
                    (SUSTAINABLE,)
                    if entry.mode == SUSTAINABLE
                    else (
                        "saturation",
                        parsed["run"]["spec"].get("scoring", {}).get("warmup_s", DEFAULT_WARMUP_S),
                        parsed["run"]["spec"].get("scoring", {}).get("measurement_s", 0),
                        entry.replay_s,
                        entry.compute_budget,
                    ),
                )
            ].append(entry)
            versions.add(parsed["harness_version"])
        except ValueError as err:
            raise ValueError(f"{path}: does not hold the fields a results row needs: {err}") from err

    ordered = sorted(
        groups,
        key=lambda key: (
            groups[key][0].offered_rate,
            key[1],
            "" if key[0] is None else key[0],
            key[4],
            key[5] != EXACTLY_ONCE,
            key[2],
            key[3],
            key[6],
            "" if key[7] is None else key[7],
            *key[8:],
        ),
    )
    return [groups[key] for key in ordered], versions


def render_results_table(
    documents: Sequence[tuple[Path, object]], *, workloads: Path, results_dir: Path | None = None
) -> str:
    """Render all workloads, reporting malformed documents by source path and field.

    Callers loading files pass the results root; otherwise paths are already
    relative to that root. Notes are checked beside the input JSON files.
    ``workloads`` supplies the tracks that label sections and the summary tables.
    """
    sections, versions = _parse_sections(documents, workloads)
    lines = ["# Results"]
    if any(entries[0].mode == SUSTAINABLE for entries in sections):
        lines.extend(["", *_intro(sections, "../docs/methodology.md")])
    if any(entries[0].mode == "saturation" for entries in sections):
        lines.extend(["", _SATURATION_INTRO])
    titles = _titles(sections)
    lines.extend(_summary(sections, titles, results_dir))
    for entries in sections:
        if entries[0].mode == SUSTAINABLE:
            lines.extend(["", *_track_section(entries, results_dir, titles[id(entries)])])
        else:
            lines.extend(["", *_saturation_section(entries, results_dir)])
    if not sections:
        lines.extend(["", "No results published yet."])
    harness = f" with harness {', '.join(sorted(versions))}" if versions else ""
    footer = f"_Generated by `results-table` from the results in this directory{harness}. Do not edit by hand._"
    return "\n".join([*lines, "", footer]) + "\n"


README_START = "<!-- results:start -->"
README_END = "<!-- results:end -->"
README_P95_S = 90.0
README_MAX_S = 120.0


def _meets_readme_freshness(entry: _Entry) -> bool:
    freshness = entry.document["derived"]["freshness"]
    if freshness is None:
        return False
    window = freshness["window"]
    p95, maximum = window["p95_s"], window["max_s"]
    return p95 is not None and maximum is not None and p95 <= README_P95_S and maximum <= README_MAX_S


def readme_for(results_dir: Path) -> Path:
    """The README whose marked block summarizes the results in ``results_dir``."""
    # Resolve first: the parent of "." or of a subdirectory is not the repository root.
    return results_dir.resolve().parent / "README.md"


def readme_prefix(results_dir: Path) -> str:
    """The path from the README's directory to the results, as a link prefix."""
    return Path(os.path.relpath(results_dir, readme_for(results_dir).parent)).as_posix() + "/"


def render_readme_block(
    documents: Sequence[tuple[Path, object]], *, workloads: Path, results_dir: Path | None, prefix: str
) -> str:
    """The README's results summary: what keeping up means and both summary tables.

    Links are rebased by ``prefix`` from the results directory to the README, and
    headings drop a level to sit under the README's own Results section.
    """
    sections, _ = _parse_sections(documents, workloads)
    titles = _titles(sections)
    # Keep full-results anchors and resolve supersession before filtering.
    for entries in sections:
        entries[:] = [entry for entry in _latest(entries)[0] if _meets_readme_freshness(entry)]
    sections = [entries for entries in sections if entries]
    lines = _intro(sections, "docs/methodology.md", freshness_bounds=(README_P95_S, README_MAX_S))
    lines.extend(_summary(sections, titles, results_dir, prefix=prefix, page=f"{prefix}RESULTS.md"))
    lines.extend(
        ["", f"Fleets, freshness and file sizes for every track are in the [full results]({prefix}RESULTS.md)."]
    )
    return "\n".join(f"#{line}" if line.startswith("## ") else line for line in lines) + "\n"


def splice_readme(text: str, block: str) -> str | None:
    """Replace the marked region of ``text`` with ``block``, or return None without both markers in order."""
    # Markers count only on lines of their own, so prose that quotes them is never rewritten.
    start = re.search(rf"^{re.escape(README_START)}$", text, re.MULTILINE)
    end = re.search(rf"^{re.escape(README_END)}$", text, re.MULTILINE)
    if start is None or end is None or end.start() < start.end():
        return None
    return f"{text[: start.end()]}\n{block}{text[end.start() :]}"
