# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Merge shard metadata and validate the complete corpus.

Apply corpus-wide gates before publishing ``corpus.json``. Keep batch files
in their shard directories and reference them from the merged manifest.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import cast

from ingest_bench import uri
from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.corpus import columns as c
from ingest_bench.corpus import values as v
from ingest_bench.corpus.generate import (
    BatchRecord,
    dump_column_stats,
    finalize_corpus_json,
    load_column_stats,
)
from ingest_bench.corpus.preset import Preset, corpus_dir_name, corpus_hash, preset_from_effective
from ingest_bench.corpus.stats import ColumnStats, new_column_stats

# Require identical workload, seed, and disk encoding across shards.
AGREED_KEYS = ("corpus_hash", "seed", "generator_version", "payload_width", "row_block", "zstd_level")

TRUTH_FIELDS = ("rows", "sum_mod", "encoded_bytes")


def _read_partition_truth(shard_uri: str) -> dict[str, dict[str, int]]:
    raw = cast(dict[str, object], json.loads(uri.read_text(uri.join(shard_uri, "partition_truth.json"))))
    return {
        str(label): {field: c.as_int_value(cast(dict[str, object], entry)[field]) for field in TRUTH_FIELDS}
        for label, entry in raw.items()
    }


def _sum_partition_truth(shard_uris: list[str], preset: Preset) -> dict[str, dict[str, int]]:
    """Sum partition truth over the preset's declared labels.

    A shard with a different key space must fail instead of contributing partial
    truth.
    """
    labels = [v.partition_label(key) for key in range(preset.partition_count)]
    truth = {label: {"rows": 0, "sum_mod": 0, "encoded_bytes": 0} for label in labels}
    for shard_uri in shard_uris:
        shard_truth = _read_partition_truth(shard_uri)
        if sorted(shard_truth) != sorted(labels):
            raise ValueError(f"{shard_uri} publishes {len(shard_truth)} partition keys, not {len(labels)}")
        for label in labels:
            total, entry = truth[label], shard_truth[label]
            total["rows"] += entry["rows"]
            total["encoded_bytes"] += entry["encoded_bytes"]
            # Checksums add modulo P across batches and shards.
            total["sum_mod"] = (total["sum_mod"] + entry["sum_mod"]) % c.P
    return truth


def _merge_column_stats(shard_uris: list[str], preset: Preset) -> dict[str, ColumnStats]:
    """Union shard samples selected on the shared corpus-wide row stride."""
    merged = new_column_stats(preset.columns)
    for shard_uri in shard_uris:
        shard_stats = load_column_stats(uri.read_text(uri.join(shard_uri, "column_stats.json")))
        for name, stats in merged.items():
            stats.merge(shard_stats[name])
    return merged


def _batch_uri(shard_uri: str, reference: str) -> str:
    """Keep manifest references valid outside their source directory."""
    if reference.startswith("/") or "://" in reference:
        return reference
    path = uri.join(shard_uri, reference)
    return path if "://" in path else str(Path(path).absolute())


def merge(shard_uris: list[str], out_uri: str) -> dict[str, object]:
    """Assemble the shards under ``out_uri`` and return the merged `corpus.json`."""
    if not shard_uris:
        raise ValueError("merging needs at least one shard corpus")
    shards = [cast(dict[str, object], json.loads(uri.read_text(uri.join(s, "corpus.json")))) for s in shard_uris]
    first = shards[0]
    for shard in shards:
        effective = cast(dict[str, object], shard["effective_preset"])
        if shard.get("value_encoding", "avro") != effective.get("value_encoding", "avro"):
            raise ValueError("shard corpus encoding disagrees with its effective preset")
    for shard in shards[1:]:
        for key in AGREED_KEYS:
            if shard[key] != first[key]:
                raise ValueError(f"shards disagree on {key}: {shard[key]!r} != {first[key]!r}")
    indices = sorted(c.as_int_value(shard["shard_index"]) for shard in shards)
    if indices != list(range(c.as_int_value(first["shard_count"]))):
        raise ValueError(f"expected shards 0..{c.as_int_value(first['shard_count']) - 1}, got {indices}")
    if first["generator_version"] != GENERATOR_VERSION:
        raise ValueError(f"shard generator version {first['generator_version']!r} does not match {GENERATOR_VERSION!r}")
    preset = preset_from_effective(cast(dict[str, object], first["effective_preset"]), str(first["schema_name"]))
    seed = c.as_int_value(first["seed"])
    row_block = c.as_int_value(first["row_block"])
    if corpus_hash(preset, seed, row_block=row_block) != str(first["corpus_hash"]):
        raise ValueError(
            f"shard corpus hash {first['corpus_hash']!r} does not match the recorded preset and generator inputs"
        )

    records: list[BatchRecord] = []
    for shard_uri in shard_uris:
        for line in uri.read_text(uri.join(shard_uri, "manifest.jsonl")).splitlines():
            record = BatchRecord.from_json(line)
            record.uri = _batch_uri(shard_uri, record.uri)
            record.key_uris = {name: _batch_uri(shard_uri, reference) for name, reference in record.key_uris.items()}
            records.append(record)
    records.sort(key=lambda record: record.batch)
    if [record.batch for record in records] != list(range(preset.batch_count)):
        raise ValueError(
            f"the shards hold {len(records)} batches covering {len({r.batch for r in records})} distinct indices, "
            f"and the preset asks for batches 0..{preset.batch_count - 1}"
        )

    truth = _sum_partition_truth(shard_uris, preset)
    column_stats = _merge_column_stats(shard_uris, preset)
    meta = finalize_corpus_json(
        preset,
        seed,
        records,
        truth,
        column_stats,
        c.as_int_value(first["column_stats_stride"]),
        c.as_int_value(first["payload_width"]),
        row_block,
        c.as_int_value(first["zstd_level"]),
        shard_index=0,
        shard_count=1,
    )
    corpus_uri = uri.join(out_uri, corpus_dir_name(preset, seed, row_block=row_block))
    uri.write_text(uri.join(corpus_uri, "manifest.jsonl"), "".join(record.to_json() + "\n" for record in records))
    uri.write_text(uri.join(corpus_uri, "schema.avsc"), uri.read_text(uri.join(shard_uris[0], "schema.avsc")))
    uri.write_text(uri.join(corpus_uri, "partition_truth.json"), json.dumps(truth, indent=2))
    uri.write_text(uri.join(corpus_uri, "column_stats.json"), dump_column_stats(column_stats))
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(meta, indent=2, sort_keys=True))
    return meta
