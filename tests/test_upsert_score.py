# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import json
import time
from pathlib import Path

import numpy as np
import pytest

from ingest_bench.clock import now_ms
from ingest_bench.corpus import generate
from ingest_bench.corpus import metadata as corpus_metadata
from ingest_bench.producer import publish_log
from ingest_bench.scorer import score
from tests.test_score_loop import StepClock
from tests.upsert_fixtures import ToyEngine, build_upsert_corpus, register


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> corpus_metadata.CorpusMetadata:
    return build_upsert_corpus(tmp_path_factory.mktemp("corpora"))


def _offer(logs: Path, records: list[generate.BatchRecord], epoch: int) -> None:
    for record in records:
        due = epoch + record.offset_ms
        publish_log.append(
            logs / "publish_log-0.jsonl",
            publish_log.PublishRecord(record.batch, due, due + 20, due + 80, record.rows, record.encoded_bytes, 0),
        )
    publish_log.append_done(logs / "publish_log-0.jsonl", 0, len(records))


def test_upsert_scoring_drains_on_key_coverage_and_checkpoints(
    tmp_path: Path, corpus: corpus_metadata.CorpusMetadata
) -> None:
    props = {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}
    table = register(props, "bench.up", corpus, tmp_path / "wh")
    engine = ToyEngine(table, corpus)
    records = corpus_metadata.read_manifest(corpus.uri)
    # The run starts after the seed commit, which every sample before the first stream commit reads.
    time.sleep(0.01)
    epoch = now_ms()
    engine.upsert(engine.batch_versions(0, collapse=False))
    engine.upsert(engine.batch_versions(1, collapse=True))
    # Sample one second after the epoch lands between batch 1's commits and batch 2's.
    time.sleep(1.2)
    middle_ms = now_ms()
    engine.upsert(engine.batch_versions(2, collapse=True))
    engine.upsert(engine.batch_versions(3, collapse=False))
    logs = tmp_path / "logs"
    logs.mkdir()
    _offer(logs, records, epoch)
    sample_s = (middle_ms - epoch) // 1000
    args = score.ScoreArgs(
        corpus_uri=corpus.uri,
        table="bench.up",
        catalog_props=props,
        publish_logs_uri=str(logs),
        epoch_ms=epoch,
        out_dir=tmp_path / "out",
        poll_interval_s=1.0,
        idle_stop_s=30.0,
        warmup_s=0,
        exactness_mode="LATEST_PER_KEY",
        sample_offsets_s=(0, sample_s, 100_000),
    )
    assert score.run(args, StepClock(now_ms()), open(tmp_path / "score.log", "w")) == 0
    summary = json.loads((args.out_dir / "summary.json").read_text())
    assert summary["state"] == "drained" and summary["prefix"] == 3, summary["reason"]
    assert summary["exactness"]["mode"] == "LATEST_PER_KEY" and summary["exactness"]["verdict"] is True
    lines = [json.loads(line) for line in (args.out_dir / "snapshots.jsonl").read_text().splitlines()]
    assert {line["operation"] for line in lines} >= {"append", "overwrite"}
    checkpoints = json.loads((args.out_dir / score.CHECKPOINTS_DIR / score.CHECKPOINTS_FILE).read_text())
    by_name = {entry["name"]: entry for entry in checkpoints["checkpoints"]}
    assert set(by_name) == {"0", str(sample_s), "final"}
    assert checkpoints["absent"] == [{"offset_s": 100_000, "reason": "after the final commit"}]
    seed_snapshot = table.history()[0].snapshot_id
    assert by_name["0"]["snapshot_id"] == seed_snapshot
    zero = np.load(args.out_dir / score.CHECKPOINTS_DIR / "stamp-0.npy")
    assert not zero.any()
    middle = np.load(args.out_dir / score.CHECKPOINTS_DIR / f"stamp-{sample_s}.npy")
    final = np.load(args.out_dir / score.CHECKPOINTS_DIR / "stamp-final.npy")
    assert middle.any() and (final >= middle).all() and (final != middle).any()
    current = table.current_snapshot()
    assert current is not None and by_name["final"]["snapshot_id"] == current.snapshot_id
