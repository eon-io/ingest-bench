# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Read published corpus metadata and batch manifests.

Consumers use the recorded corpus facts instead of re-deriving them from a
preset that may have changed since generation.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import cast

from ingest_bench import uri
from ingest_bench.corpus import GENERATOR_VERSION
from ingest_bench.corpus.columns import ColumnDistribution, as_float_value, as_int_value
from ingest_bench.corpus.generate import BatchRecord
from ingest_bench.corpus.preset import corpus_hash, preset_from_effective
from ingest_bench.corpus.upsert import KeySchedule

# An upsert corpus's seed lives under this directory and is published by this file.
SEED_DIR = "seed"
SEED_FILE = "seed.json"


def _as_string_map(value: object) -> dict[str, str]:
    return {str(key): str(entry) for key, entry in cast(dict[str, object], value).items()}


@dataclass(frozen=True)
class CorpusMetadata:
    """Published corpus facts and their source URI."""

    uri: str
    name: str
    corpus_hash: str
    seed: int
    row_block: int
    schema: dict[str, object]
    schema_name: str
    # The declarations rebuilt from the effective preset, from which every layer derives column types.
    columns: tuple[ColumnDistribution, ...]
    column_roles: dict[str, str]
    key_columns: tuple[str, ...]
    p: int
    id_block: int
    batch_count: int
    batch_interval_ms: int
    row_count: int
    offered_bytes_per_s: int
    duration_s: int
    rows_per_s: float
    generator_version: str
    corpus_epoch: str
    value_encoding: str = "avro"
    # Upsert corpora: the key schedule and the published final-table truth.
    schedule: KeySchedule | None = None
    upsert_truth: dict[str, int] | None = None

    def validate_value_encoding(self, encoding: str) -> None:
        """Reject incompatible wire formats before creating resources or sending rows."""
        expected = "avro" if encoding in {"avro", "confluent"} else encoding
        if expected not in {"avro", "json"} or expected != self.value_encoding:
            raise ValueError(
                f"value encoding {encoding!r} does not match corpus encoding {self.value_encoding!r}; "
                "generate a matching corpus and select its preset name in the run spec"
            )

    def field_names(self) -> list[str]:
        """The corpus columns in schema order, which is the order a row encodes in."""
        return [str(cast(dict[str, object], field)["name"]) for field in cast(list[object], self.schema["fields"])]

    def avro_schema_json(self) -> str:
        return json.dumps(self.schema)

    def role(self, name: str) -> str:
        """The column holding the ``name`` role, so a query names a role rather than a column."""
        return self.column_roles[name]


def read(corpus_uri: str) -> CorpusMetadata:
    """Read a complete corpus; reject shards until they have been merged."""
    raw = cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_uri, "corpus.json"))))
    try:
        version = str(raw["generator_version"])
        if version != GENERATOR_VERSION:
            raise ValueError(
                f"{corpus_uri} was written by generator version {version}; this build requires "
                f"version {GENERATOR_VERSION}. Regenerate it with `gen-corpus`."
            )
        shard_count = as_int_value(raw["shard_count"])
        if shard_count != 1:
            raise ValueError(
                f"{corpus_uri} is shard {as_int_value(raw['shard_index'])} of {shard_count}, not a whole corpus; "
                "merge the shards first"
            )
        effective = cast(dict[str, object], raw["effective_preset"])
        seed = as_int_value(raw["seed"])
        row_block = as_int_value(raw["row_block"])
        preset = preset_from_effective(effective, str(raw["schema_name"]))
        if str(raw["corpus_hash"]) != corpus_hash(preset, seed, row_block=row_block):
            raise ValueError(
                f"{corpus_uri}: corpus hash does not match its preset, seed, generator version, and row-block size. "
                "Regenerate it with `gen-corpus`."
            )
        encoding = str(raw.get("value_encoding", "avro"))
        if encoding != preset.value_encoding:
            raise ValueError(f"{corpus_uri}: corpus encoding disagrees with its effective preset")
        return CorpusMetadata(
            value_encoding=encoding,
            uri=corpus_uri,
            name=str(raw["name"]),
            corpus_hash=str(raw["corpus_hash"]),
            seed=seed,
            row_block=row_block,
            schema=cast(dict[str, object], raw["schema"]),
            schema_name=str(raw["schema_name"]),
            columns=preset.columns,
            column_roles=_as_string_map(raw["column_roles"]),
            key_columns=tuple(str(name) for name in cast(list[object], raw["key_columns"])),
            p=as_int_value(raw["p"]),
            id_block=as_int_value(raw["id_block"]),
            batch_count=as_int_value(raw["batch_count"]),
            batch_interval_ms=as_int_value(raw["batch_interval_ms"]),
            row_count=as_int_value(raw["row_count"]),
            offered_bytes_per_s=as_int_value(raw["offered_bytes_per_s"]),
            duration_s=as_int_value(raw["duration_s"]),
            rows_per_s=as_float_value(raw["rows_per_s"]),
            generator_version=version,
            corpus_epoch=str(effective["corpus_epoch"]),
            schedule=None if preset.upsert is None else KeySchedule.for_preset(preset, seed),
            upsert_truth=None
            if raw.get("upsert") is None
            else {
                str(key): as_int_value(value)
                for key, value in cast(dict[str, object], raw["upsert"]).items()
                if key != "schedule_version"
            },
        )
    except KeyError as err:
        raise ValueError(f"{corpus_uri}/corpus.json is missing key {err.args[0]!r}") from err


def _absolute(corpus_uri: str, reference: str) -> str:
    """Resolve relative manifest paths, preserving existing absolute shard URIs."""
    if reference.startswith("/") or "://" in reference:
        return reference
    return uri.join(corpus_uri, reference)


def read_manifest(corpus_uri: str) -> list[BatchRecord]:
    """Read batches in send order with absolute paths.

    Reject gaps and duplicates before they can affect engine scoring.
    """
    return _read_records(corpus_uri, read(corpus_uri).batch_count)


def read_seed_manifest(corpus_uri: str) -> list[BatchRecord]:
    """Read a Kafka seed's batches in send order with absolute paths."""
    seed_uri = uri.join(corpus_uri, SEED_FILE)
    if not uri.exists(seed_uri):
        raise ValueError(f"{corpus_uri} has published no seed for Kafka; build it with seed-corpus and commit-seed")
    facts = cast(dict[str, object], json.loads(uri.read_text(seed_uri)))
    if "batches" not in facts:
        raise ValueError(f"{corpus_uri} publishes a seed table, not seed batches for Kafka")
    return _read_records(uri.join(corpus_uri, SEED_DIR), int(cast(int, facts["batches"])))


def _read_records(directory: str, batch_count: int) -> list[BatchRecord]:
    records = [
        BatchRecord.from_json(line)
        for line in uri.read_text(uri.join(directory, "manifest.jsonl")).splitlines()
        if line.strip()
    ]
    records.sort(key=lambda record: record.batch)
    held = [record.batch for record in records]
    if held != list(range(batch_count)):
        raise ValueError(
            f"{directory} publishes batches 0..{batch_count - 1}, and its manifest holds {len(held)} "
            f"records covering {len(set(held))} distinct batches"
        )
    for record in records:
        record.uri = _absolute(directory, record.uri)
        record.key_uris = {name: _absolute(directory, reference) for name, reference in record.key_uris.items()}
    return records
