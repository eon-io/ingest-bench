# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Place published results on the leaderboard: their track, tier and comparison class.

A result conforms to a track through the values it recorded while running, not
through the spec's claim, so results staged before tracks existed are placed by
the same rule as new ones. Everything here reads the embedded spec as published,
applying the loader's defaults to the settings it omits.
"""

from __future__ import annotations

from pathlib import Path
from typing import cast

from ingest_bench.collect.schema import ResultDocument
from ingest_bench.specs import compactors, engines
from ingest_bench.specs.model import (
    COMPACTION_INLINE,
    DEFAULT_BEHIND_MAX_MS,
    DEFAULT_PARTITION,
    DEFAULT_WARMUP_S,
    EXTERNAL,
    KEY_NONE,
    LATEST_PER_KEY,
    SUSTAINABLE,
    TIER_REFERENCE,
    TIER_TUNED,
    Track,
    load_track,
    track_names,
)
from ingest_bench.workloads import canonical_workload_name

# Comparison classes separate runs by the rewriting work they do, since a
# compacting fleet pays for output that an ingest-only fleet leaves undone.
CLASS_INGEST_ONLY = "ingest-only"
CLASSES: dict[str | None, str] = {
    None: CLASS_INGEST_ONLY,
    COMPACTION_INLINE: "integrated-compaction",
    **{driver: name for driver, name in compactors.COMPARISON_CLASSES.items()},
    EXTERNAL: "external-compaction",
}


def load_tracks(workloads: Path) -> list[Track]:
    return [load_track(name, workloads) for name in track_names(workloads)]


def _spec(document: ResultDocument) -> dict[str, object]:
    return cast(dict[str, object], document["run"]["spec"])


def _section(spec: dict[str, object], name: str) -> dict[str, object]:
    return cast(dict[str, object], spec.get(name) or {})


def comparison_class(document: ResultDocument) -> str:
    driver = cast(str | None, _section(_spec(document), "compaction").get("driver"))
    if driver not in CLASSES:
        raise ValueError(f"run.spec.compaction.driver: {driver!r} is not a driver this checkout compares")
    return CLASSES[driver]


# The classes whose compaction runs in its own pods beside the ingest fleet, so
# their ingest roles can be told apart from the compactor's.
SIDECAR_CLASSES = frozenset(compactors.COMPARISON_CLASSES.values())


def sidecar_roles(document: ResultDocument) -> frozenset[str]:
    """The fleet roles a managed compaction sidecar held, as its driver names them."""
    driver = cast(str | None, _section(_spec(document), "compaction").get("driver"))
    if driver not in compactors.MANAGED:
        return frozenset()
    return frozenset(role for _, role in compactors.kubernetes_for(driver).role_map)


def tier(document: ResultDocument) -> str:
    """The leaderboard tier, derived from the settings the run changed.

    Table properties and compaction layers are tuning in every engine. A managed
    engine is at the reference tier when its knobs, less the sizing knobs, equal
    the settings its knobs module ships. An external engine declares its tier.
    """
    spec = _spec(document)
    engine = document["run"]["engine"]
    upsert = _section(spec, "scoring").get("exactness") == LATEST_PER_KEY
    if spec.get("compaction") is not None:
        return TIER_TUNED
    if engine == EXTERNAL:
        if _section(spec, "table").get("properties"):
            return TIER_TUNED
        return cast(str, spec.get("tier", TIER_TUNED))
    if engine not in engines.MANAGED:
        # No shipped settings exist for an engine this checkout does not manage.
        return TIER_TUNED
    knobs = engines.knobs_for(engine)
    table = _section(spec, "table")
    if upsert:
        # Upsert reference tables differ by engine: each knobs module declares the settings it ships.
        declared = cast(
            dict[str, object], getattr(knobs, "REFERENCE_UPSERT_TABLE", {"format_version": 2, "properties": {}})
        )
        held = {"format_version": table.get("format_version", 2), "properties": table.get("properties") or {}}
        if held != declared:
            return TIER_TUNED
    elif table.get("properties"):
        return TIER_TUNED
    fixed = {key: value for key, value in _section(spec, engine).items() if key not in knobs.SIZING_KNOBS}
    return TIER_REFERENCE if fixed == knobs.REFERENCE_KNOBS else TIER_TUNED


def track_mismatches(document: ResultDocument, track: Track) -> list[str]:
    """Describe each recorded setting that differs from what ``track`` fixes.

    Measured freshness is not a setting: a valid result over the track's bounds still belongs to the track, and
    ``freshness_mismatches`` marks it as not passing.
    """
    run, summary = document["run"], document["data"]["summary"]
    corpus = run["corpus"]
    if summary is None or corpus is None:
        return ["the result has no scorer summary or corpus metadata to compare"]
    spec = _spec(document)
    producer, scoring, table = (_section(spec, name) for name in ("producer", "scoring", "table"))
    kafka = run["kafka"]
    recorded: dict[str, object] = {
        "corpus": canonical_workload_name(corpus["name"]),
        "kafka.partitions": kafka["partitions"],
        "kafka.key": KEY_NONE if kafka["key"] is None else kafka["key"],
        "kafka.partition_assignment": kafka["partition_assignment"],
        "kafka.value_encoding": kafka["value_encoding"],
        "producer.compression": kafka["compression"],
        "producer.behind_max_ms": producer.get("behind_max_ms", DEFAULT_BEHIND_MAX_MS),
        "producer.speed": summary["speed"],
        "scoring.mode": scoring.get("mode", SUSTAINABLE),
        "scoring.exactness": summary["exactness_mode"],
        "table.partition": table.get("partition", DEFAULT_PARTITION),
    }
    fixed: dict[str, object] = {
        "corpus": canonical_workload_name(track.corpus),
        "kafka.partitions": track.kafka.partitions,
        "kafka.key": track.kafka.key,
        "kafka.partition_assignment": track.kafka.partition_assignment,
        "kafka.value_encoding": track.kafka.value_encoding,
        "producer.compression": track.compression,
        "producer.behind_max_ms": track.behind_max_ms,
        "producer.speed": 1.0,
        "scoring.mode": SUSTAINABLE,
        "scoring.exactness": track.exactness,
        "table.partition": track.partition,
    }
    mismatches = [
        f"{field} is {recorded[field]!r} where track {track.name!r} fixes {value!r}"
        for field, value in fixed.items()
        if recorded[field] != value
    ]
    if track.geometry_offsets_s is not None:
        offsets = tuple(cast(list[int], scoring.get("geometry_offsets_s", [])))
        if offsets != track.geometry_offsets_s:
            mismatches.append(
                f"scoring.geometry_offsets_s is {list(offsets)} where track {track.name!r} fixes "
                f"{list(track.geometry_offsets_s)}"
            )
    warmup_s = summary.get("warmup_s", cast(float, scoring.get("warmup_s", DEFAULT_WARMUP_S)))
    if warmup_s > track.max_warmup_s:
        mismatches.append(f"scoring.warmup_s is {warmup_s:g}, above track {track.name!r}'s {track.max_warmup_s} s")
    return mismatches


def freshness_mismatches(document: ResultDocument, track: Track) -> list[str]:
    """Hold a valid result's measured freshness to the track's bounds.

    A track's bounds decide whether a result passes, whatever bounds it was scored under: a run scored
    under looser bounds passes when its measured window meets the track's. A run that did not validate
    claims no freshness.
    """
    summary = document["data"]["summary"]
    if summary is None or not summary["run_valid"]:
        return []
    freshness = document["derived"]["freshness"]
    p95_s, max_s = (None, None) if freshness is None else (freshness["window"]["p95_s"], freshness["window"]["max_s"])
    mismatches = []
    if p95_s is None or p95_s > track.freshness_bound_s:
        observed = None if p95_s is None else f"{p95_s:g}"
        mismatches.append(
            f"freshness p95 is {observed} s where track {track.name!r} bounds it at {track.freshness_bound_s:g} s"
        )
    if max_s is None or max_s > track.freshness_max_bound_s:
        observed = None if max_s is None else f"{max_s:g}"
        mismatches.append(
            f"freshness max is {observed} s where track {track.name!r} bounds it at {track.freshness_max_bound_s:g} s"
        )
    return mismatches


def track_for(document: ResultDocument, tracks: list[Track]) -> Track | None:
    """The track a result conforms to, or ``None``.

    A spec that names a track conforms only to that one. Shipped tracks differ
    in a fixed setting, so a result conforms to at most one.
    """
    named = _spec(document).get("track")
    candidates = [track for track in tracks if named is None or track.name == canonical_workload_name(cast(str, named))]
    return next((track for track in candidates if not track_mismatches(document, track)), None)
