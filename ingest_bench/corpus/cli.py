# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Commands for generating corpora, merging generated shards and writing upsert seed tables."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ingest_bench import uri
from ingest_bench.corpus import DEFAULT_ROW_BLOCK, seed
from ingest_bench.corpus import merge as merge_corpora
from ingest_bench.corpus import values as v
from ingest_bench.corpus.generate import generate
from ingest_bench.corpus.preset import Preset, corpus_dir_name_for, corpus_hash, load_preset
from ingest_bench.workloads import WORKLOADS_ENV, workloads_dir

# Storage estimates assume this compression ratio; `corpus.json` records
# the actual stored size.
ASSUMED_COMPRESSION_RATIO = 4


def print_plan(preset: Preset, seed: int) -> None:
    """Print calibrated generation estimates without writing a corpus.

    Estimates ignore row-block rounding and variation in encoded widths.
    """
    sizing = v.corpus_sizing(preset, seed)
    mean_row = sizing.mean_encoded_row_bytes
    rows = max(1, int(preset.batch_bytes / sizing.mean_budget_row_bytes)) * preset.batch_count
    encoded = sizing.offered_bytes_per_s * preset.duration_s
    hash_value = corpus_hash(preset, seed, row_block=DEFAULT_ROW_BLOCK)
    print(f"preset: {preset.name} (hash {hash_value}, seed {seed})")
    print(
        f"batches: {preset.batch_count} x {preset.batch_interval_ms} ms, {preset.batch_bytes} "
        f"{preset.budget_encoding} budget bytes each"
    )
    if preset.sizing_encoding is not None:
        print(f"output encoding: {preset.value_encoding}; estimated encoded rate: {sizing.offered_bytes_per_s} bytes/s")
    print(f"estimated rows: {rows} ({rows // preset.duration_s}/s), mean row {mean_row:.1f} bytes")
    print(f"estimated encoded bytes: {encoded}")
    print(
        f"estimated stored bytes: ~{encoded // ASSUMED_COMPRESSION_RATIO} (assumes ~{ASSUMED_COMPRESSION_RATIO}:1 zstd)"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="corpus", description="Build a benchmark corpus, or merge the shards of one.")
    commands = parser.add_subparsers(dest="command", required=True)

    gen = commands.add_parser("gen", help="build a corpus, or one shard of one")
    gen.add_argument("--preset", required=True, help="a shipped preset name, or a path to a preset YAML file")
    gen.add_argument(
        "--set", dest="overrides", action="append", default=[], metavar="KEY=VALUE", help="override a preset key"
    )
    gen.add_argument("--out", required=True, metavar="URI", help="parent of the corpus directory to write")
    gen.add_argument("--seed", type=int, default=1)
    gen.add_argument("--shard-index", type=int, default=0)
    gen.add_argument("--shard-count", type=int, default=1)
    gen.add_argument("--zstd-level", type=int, default=3)
    gen.add_argument("--workloads", metavar="DIR", help=f"preset and schema directory (env {WORKLOADS_ENV})")
    gen.add_argument("--plan", action="store_true", help="print what would be generated and exit")

    merge = commands.add_parser("merge", help="merge shard corpora into one corpus")
    merge.add_argument("shard_uris", nargs="+", metavar="SHARD_URI")
    merge.add_argument("--out", required=True, metavar="URI", help="parent of the corpus directory to write")

    seed_shard = commands.add_parser("seed", help="write one shard of an upsert corpus's seed table")
    seed_shard.add_argument("--corpus", required=True, metavar="URI", help="the merged upsert corpus directory")
    seed_shard.add_argument("--shard-index", type=int, default=0)
    seed_shard.add_argument("--shard-count", type=int, default=1)
    seed_shard.add_argument("--target-file-bytes", type=int, default=seed.DEFAULT_TARGET_FILE_BYTES)

    commit = commands.add_parser("seed-commit", help="register the seed shards as one Iceberg snapshot")
    commit.add_argument("--corpus", required=True, metavar="URI", help="the merged upsert corpus directory")
    commit.add_argument("--target-file-bytes", type=int, default=seed.DEFAULT_TARGET_FILE_BYTES)
    return parser


def run_gen(args: argparse.Namespace) -> int:
    preset = load_preset(
        str(args.preset),
        workloads_dir=workloads_dir(None if args.workloads is None else str(args.workloads)),
        overrides=[str(assignment) for assignment in args.overrides],
    )
    seed = int(args.seed)
    if args.plan:
        print_plan(preset, seed)
        return 0
    shard_index, shard_count = int(args.shard_index), int(args.shard_count)
    meta = generate(
        preset,
        str(args.out),
        seed=seed,
        row_block=DEFAULT_ROW_BLOCK,
        shard_index=shard_index,
        shard_count=shard_count,
        zstd_level=int(args.zstd_level),
    )
    shard = "" if shard_count == 1 else f" shard {shard_index} of {shard_count}"
    corpus_uri = uri.join(str(args.out), corpus_dir_name_for(str(meta["name"]), str(meta["corpus_hash"])))
    print(
        f"wrote {corpus_uri}{shard}: "
        f"{meta['row_count']} rows, {meta['encoded_bytes']} encoded bytes, {meta['stored_bytes']} stored bytes"
    )
    return 0


def run_merge(args: argparse.Namespace) -> int:
    shard_uris = [str(shard_uri) for shard_uri in args.shard_uris]
    meta = merge_corpora.merge(shard_uris, str(args.out))
    corpus_uri = uri.join(str(args.out), corpus_dir_name_for(str(meta["name"]), str(meta["corpus_hash"])))
    print(
        f"wrote {corpus_uri} from {len(shard_uris)} shards: "
        f"{meta['row_count']} rows, {meta['encoded_bytes']} encoded bytes"
    )
    return 0


def run_seed(args: argparse.Namespace) -> int:
    files = seed.write_seed_shard(
        str(args.corpus), int(args.shard_index), int(args.shard_count), int(args.target_file_bytes)
    )
    print(f"wrote {len(files)} seed files for shard {args.shard_index} of {args.shard_count}")
    return 0


def run_seed_commit(args: argparse.Namespace) -> int:
    facts = seed.commit_seed(str(args.corpus), int(args.target_file_bytes))
    if "batches" in facts:
        print(f"published {facts['rows']} seed rows in {facts['batches']} batches for Kafka: {args.corpus}")
    else:
        print(f"committed {facts['rows']} seed rows in {facts['files']} files: {facts['metadata_location']}")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "gen":
        return run_gen(args)
    if args.command == "seed":
        return run_seed(args)
    if args.command == "seed-commit":
        return run_seed_commit(args)
    return run_merge(args)


def gen_corpus() -> int:
    return main(["gen", *sys.argv[1:]])


def merge_corpus() -> int:
    return main(["merge", *sys.argv[1:]])


def seed_corpus() -> int:
    return main(["seed", *sys.argv[1:]])


def commit_seed() -> int:
    return main(["seed-commit", *sys.argv[1:]])


if __name__ == "__main__":
    raise SystemExit(main())
