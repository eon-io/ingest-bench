# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import subprocess
import sys
from pathlib import Path

import pytest

from ingest_bench import uri
from ingest_bench.corpus import GENERATOR_VERSION, cli, frames, generate, merge, metadata, preset

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"
OVERRIDES = ["offered_bytes_per_s=1MB", "duration_s=4", "partition_count=8"]


def test_merge_equals_unsharded(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    whole = generate.generate(p, str(tmp_path / "whole"), seed=5, row_block=64)
    corpus_dir = preset.corpus_dir_name(p, seed=5, row_block=64)
    shards = []
    for i in range(2):
        generate.generate(p, str(tmp_path / f"s{i}"), seed=5, shard_index=i, shard_count=2, row_block=64)
        shards.append(uri.join(str(tmp_path / f"s{i}"), corpus_dir))
    merged = merge.merge(shards, str(tmp_path / "merged"))
    for key in (
        "row_count",
        "encoded_bytes",
        "partition_rows",
        "partition_sum_mod",
        "mean_encoded_row_size",
        "corpus_hash",
        # A shard's sample is a subset of the whole corpus's sample by
        # construction, so the merged statistic is the same statistic.
        "column_stats",
        "column_cardinality_max_relative_deviation",
    ):
        assert merged[key] == whole[key], key
    assert merged["shard_count"] == 1
    meta = metadata.read(uri.join(str(tmp_path / "merged"), corpus_dir))
    records = metadata.read_manifest(meta.uri)
    assert [r.batch for r in records] == [0, 1, 2, 3]
    assert records[1].uri.startswith(shards[1]) and uri.exists(records[1].uri)
    # A merged corpus is metadata beside the per-shard data, not a copy of it.
    assert "batches" not in uri.listdir(meta.uri)
    # An unsharded manifest holds relative references, and resolves against its own corpus.
    whole_meta = metadata.read(uri.join(str(tmp_path / "whole"), corpus_dir))
    whole_records = metadata.read_manifest(whole_meta.uri)
    assert whole_records[1].uri.startswith(whole_meta.uri) and uri.exists(whole_records[1].uri)
    assert all(uri.exists(key_uri) for key_uri in whole_records[1].key_uris.values())


def test_merge_refuses_a_missing_shard(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    generate.generate(p, str(tmp_path), seed=5, shard_index=0, shard_count=2, row_block=64)
    shard = uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=5, row_block=64))
    with pytest.raises(ValueError, match="expected shards 0..1"):
        merge.merge([shard], str(tmp_path / "merged"))


def test_merge_checks_generator_version_before_parsing_the_preset(tmp_path: Path) -> None:
    shard = tmp_path / "shard"
    uri.write_text(
        str(shard / "corpus.json"),
        json.dumps(
            {"generator_version": "5", "shard_index": 0, "shard_count": 1, "effective_preset": {}, "schema_name": "old"}
        ),
    )
    with pytest.raises(ValueError, match="shard generator version '5' does not match '6'"):
        merge.merge([str(shard)], str(tmp_path / "merged"))


def test_metadata_refuses_a_shard(tmp_path: Path) -> None:
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=OVERRIDES)
    generate.generate(p, str(tmp_path), seed=5, shard_index=0, shard_count=2, row_block=64)
    corpus_uri = uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=5, row_block=64))
    try:
        metadata.read(corpus_uri)
    except ValueError as err:
        assert "shard" in str(err)
    else:
        raise AssertionError("a shard corpus.json must be refused")
    published = json.loads(uri.read_text(uri.join(corpus_uri, "corpus.json")))
    published["generator_version"] = "0"
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(published))
    # The remedy as well as the disagreement: a corpus another build wrote is
    # regenerated, and the refusal is the only place that says so.
    with pytest.raises(ValueError, match="generator version 0.*gen-corpus"):
        metadata.read(corpus_uri)
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps({"generator_version": GENERATOR_VERSION}))
    with pytest.raises(ValueError, match="missing key 'shard_count'"):
        metadata.read(corpus_uri)


def test_cli_plan_and_generate(tmp_path: Path) -> None:
    env = {"INGEST_BENCH_WORKLOADS": str(WORKLOADS)}
    plan = subprocess.run(
        [
            sys.executable,
            "-m",
            "ingest_bench.corpus.cli",
            "gen",
            "--preset",
            "smoke",
            "--set",
            "duration_s=2",
            "--plan",
            "--out",
            str(tmp_path),
        ],
        capture_output=True,
        text=True,
        check=True,
        env={**env, "PATH": ""},
    )
    assert "batches: 2" in plan.stdout and not list(tmp_path.iterdir())
    subprocess.run(
        [
            sys.executable,
            "-m",
            "ingest_bench.corpus.cli",
            "gen",
            "--preset",
            "smoke",
            "--set",
            "duration_s=2",
            "--set",
            "offered_bytes_per_s=500KB",
            "--set",
            "partition_count=8",
            "--out",
            str(tmp_path),
            "--seed",
            "9",
        ],
        check=True,
        env={**env, "PATH": ""},
    )
    dirs = list(tmp_path.iterdir())
    assert len(dirs) == 1 and dirs[0].name.startswith("smoke-")
    assert json.loads((dirs[0] / "corpus.json").read_text())["seed"] == 9


def test_gen_reports_the_identity_returned_by_generation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    overrides = ["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"]
    p = preset.load_preset("smoke", workloads_dir=WORKLOADS, overrides=overrides)
    generated = generate.generate(p, str(tmp_path), seed=9, row_block=64)
    monkeypatch.setattr(cli, "generate", lambda *args, **kwargs: generated)
    args = ["gen", "--preset", "smoke", "--out", str(tmp_path), "--seed", "9"]
    for override in overrides:
        args.extend(["--set", override])
    assert cli.main(args) == 0
    reported = capsys.readouterr().out.split()[1].removesuffix(":")
    assert Path(reported, "corpus.json").is_file()
    assert Path(reported).name == f"{generated['name']}-{generated['corpus_hash']}"


def test_version_five_corpus_is_rejected_by_the_version_gate(tmp_path: Path) -> None:
    uri.write_text(uri.join(str(tmp_path), "corpus.json"), json.dumps({"generator_version": "5"}))
    with pytest.raises(ValueError, match="generator version 5.*version 6.*gen-corpus"):
        metadata.read(str(tmp_path))


def test_plan_and_generation_share_the_row_block_setting(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(cli, "DEFAULT_ROW_BLOCK", 64)
    args = ["gen", "--preset", "smoke", "--out", str(tmp_path), "--seed", "9"]
    for override in ["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"]:
        args.extend(["--set", override])
    assert cli.main([*args, "--plan"]) == 0
    plan = capsys.readouterr().out
    assert not list(tmp_path.iterdir())
    assert cli.main(args) == 0
    reported = capsys.readouterr().out.split()[1].removesuffix(":")
    generated = json.loads(Path(reported, "corpus.json").read_text())
    assert generated["row_block"] == 64
    assert f"hash {generated['corpus_hash']}," in plan


@pytest.mark.parametrize("field,value", [("seed", 2), ("row_block", 64), ("corpus_hash", "legacy")])
def test_metadata_refuses_mismatched_corpus_identity(tmp_path: Path, field: str, value: object) -> None:
    p = preset.load_preset(
        "smoke", workloads_dir=WORKLOADS, overrides=["offered_bytes_per_s=200KB", "duration_s=1", "partition_count=8"]
    )
    generated = generate.generate(p, str(tmp_path))
    corpus_uri = uri.join(str(tmp_path), preset.corpus_dir_name(p, seed=1, row_block=1024))
    assert metadata.read(corpus_uri).seed == 1
    generated[field] = value
    uri.write_text(uri.join(corpus_uri, "corpus.json"), json.dumps(generated))
    with pytest.raises(ValueError, match="corpus hash.*gen-corpus"):
        metadata.read(corpus_uri)


@pytest.mark.parametrize("relative", [True, False])
def test_merged_manifest_references_stay_readable_when_merged_again(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative: bool
) -> None:
    monkeypatch.chdir(tmp_path)
    p = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=100KB", "duration_s=2", "partition_count=2"],
    )
    corpus_dir = preset.corpus_dir_name(p, seed=5, row_block=64)
    shards = []
    for index in range(2):
        root = f"shard-{index}" if relative else str(tmp_path / f"shard-{index}")
        generate.generate(p, root, seed=5, row_block=64, shard_index=index, shard_count=2)
        shards.append(uri.join(root, corpus_dir))
    merge.merge(shards, "merged")
    merged_uri = str(tmp_path / "merged" / corpus_dir)
    records = metadata.read_manifest(merged_uri)
    for record in records:
        assert frames.sha256_hex(uri.read_bytes(record.uri)) == record.sha256
        for key_uri in record.key_uris.values():
            assert len(list(frames.iter_frames(frames.decompress(uri.read_bytes(key_uri))))) == record.rows
    merge.merge([merged_uri], "merged-again")
    again = metadata.read_manifest(str(tmp_path / "merged-again" / corpus_dir))
    assert [record.uri for record in again] == [record.uri for record in records]
    assert [record.key_uris for record in again] == [record.key_uris for record in records]
