# SPDX-License-Identifier: CC-BY-NC-SA-4.0
from __future__ import annotations

import json
from copy import copy
from io import StringIO
from pathlib import Path
from typing import cast
from urllib.parse import unquote, urlparse

import numpy as np
import pyarrow as pa
import pytest
from pyiceberg.io import FileIO
from pyiceberg.manifest import DataFileContent, ManifestEntry, ManifestFile
from pyiceberg.table import Table

from ingest_bench import uri
from ingest_bench.corpus import generate, metadata, preset
from ingest_bench.producer import publish_log
from ingest_bench.scorer import cli, geometry, terminal
from ingest_bench.scorer.exactness import exactness_result
from ingest_bench.scorer.snapshots import AddedFile
from ingest_bench.scorer.tally import BatchTally
from ingest_bench.specs.model import AT_LEAST_ONCE, EXACTLY_ONCE
from ingest_bench.table import create
from tests.test_tally import _rows_of

WORKLOADS = Path(__file__).resolve().parents[1] / "workloads"


@pytest.fixture(scope="module")
def corpus(tmp_path_factory: pytest.TempPathFactory) -> metadata.CorpusMetadata:
    configured = preset.load_preset(
        "smoke",
        workloads_dir=WORKLOADS,
        overrides=["offered_bytes_per_s=300KB", "duration_s=3", "partition_count=8"],
    )
    out = str(tmp_path_factory.mktemp("terminal-corpus"))
    generate.generate(configured, out, seed=17, row_block=64)
    return metadata.read(uri.join(out, preset.corpus_dir_name(configured, seed=17, row_block=64)))


def _props(tmp_path: Path) -> dict[str, str]:
    return {"type": "sql", "uri": f"sqlite:///{tmp_path}/cat.db", "warehouse": f"file://{tmp_path}/wh"}


def _catalog_file(tmp_path: Path, props: dict[str, str]) -> Path:
    path = tmp_path / "catalog.properties"
    path.write_text("\n".join(f"{key}={value}" for key, value in props.items()) + "\n", encoding="utf-8")
    return path


def _publish(logs: Path, records: list[generate.BatchRecord], *, done: bool = True, shards: int = 1) -> None:
    for shard in range(shards):
        selected = records[shard::shards]
        path = logs / f"publish_log-{shard}.jsonl"
        for record in selected:
            publish_log.append(
                path,
                publish_log.PublishRecord(
                    batch=record.batch,
                    scheduled_ms=record.offset_ms,
                    first_ack_ms=record.offset_ms + 1,
                    last_ack_ms=record.offset_ms + 2,
                    rows=record.rows,
                    bytes=record.encoded_bytes,
                    errors=0,
                ),
            )
        if done:
            publish_log.append_done(path, shard, len(selected))


def _all_rows(records: list[generate.BatchRecord], corpus: metadata.CorpusMetadata) -> pa.Table:
    return pa.concat_tables([_rows_of(record, corpus) for record in records])


def _table_with_history(
    tmp_path: Path, corpus: metadata.CorpusMetadata, name: str
) -> tuple[Table, list[generate.BatchRecord]]:
    records = metadata.read_manifest(corpus.uri)
    table = create.create_table(_props(tmp_path), f"bench.{name}", corpus, create.parse_partition("unpartitioned"), {})
    for record in records:
        table.append(_rows_of(record, corpus))
    return table, records


def _run(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    table: Table,
    records: list[generate.BatchRecord],
    *,
    done: bool = True,
    offered: list[generate.BatchRecord] | None = None,
    out: Path | None = None,
    publish_shards: int = 1,
    upload_prefix: Path | None = None,
    exactness_mode: str = EXACTLY_ONCE,
) -> tuple[int, dict[str, object]]:
    logs = tmp_path / "publish"
    logs.mkdir(exist_ok=True)
    for path in logs.iterdir():
        path.unlink()
    _publish(logs, records if offered is None else offered, done=done, shards=publish_shards)
    output = tmp_path / "scores" if out is None else out
    code = cli.verify_table(
        [
            *([] if upload_prefix is None else ["--upload-prefix", str(upload_prefix)]),
            "--metadata",
            str(table.metadata_location),
            "--corpus-uri",
            corpus.uri,
            "--publish-logs",
            str(logs),
            "--catalog-prop-file",
            str(_catalog_file(tmp_path, _props(tmp_path))),
            "--out",
            str(output),
            "--read-workers",
            "2",
            "--publish-shards",
            str(publish_shards),
            "--exactness",
            exactness_mode,
        ]
    )
    return code, json.loads((output / "terminal-exactness.json").read_text(encoding="utf-8"))


def _replace_first_id(rows: pa.Table, replacement: int) -> pa.Table:
    ids = np.asarray(rows.column("id").to_numpy()).copy()
    ids[0] = replacement
    return rows.set_column(rows.schema.get_field_index("id"), rows.schema.field("id"), pa.array(ids, pa.int64()))


def test_verify_table_read_workers_match_the_scorer_default(tmp_path: Path) -> None:
    args = terminal.VerifyArgs("metadata", "corpus", "logs", {}, tmp_path)

    assert args.read_workers == 32
    assert args.exactness_mode == EXACTLY_ONCE
    assert cli.build_verify_table_parser().get_default("read_workers") == 32
    assert cli.build_verify_table_parser().get_default("exactness") == EXACTLY_ONCE
    assert cli.build_score_parser().get_default("read_workers") == 32


def test_scan_reports_bounded_file_and_row_progress(
    corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = metadata.read_manifest(corpus.uri)
    files = [AddedFile(f"file-{index}", "parquet", 1, 1) for index in range(5)]
    output = StringIO()
    monkeypatch.setattr(terminal, "PROGRESS_EVERY_FILES", 2)
    monkeypatch.setattr(
        terminal,
        "read_id_column",
        lambda path, file_format: np.array([records[0].id_min], dtype=np.int64),
    )

    result = terminal._scan(
        files,
        BatchTally(records, corpus.p, track_unique=True),
        2,
        snapshot_id=123,
        log=output,
    )

    assert result == (5, 5)
    assert output.getvalue().splitlines() == [
        "TERMINAL_SCAN_START snapshot_id=123 files=5 manifest_rows=5 read_workers=2",
        "TERMINAL_SCAN_PROGRESS snapshot_id=123 files=2/5 rows=2/5",
        "TERMINAL_SCAN_PROGRESS snapshot_id=123 files=4/5 rows=4/5",
        "TERMINAL_SCAN_COMPLETE snapshot_id=123 files=5/5 rows=5/5",
    ]


def test_rows_outside_the_manifest_or_the_offer_fail_exactness(corpus: metadata.CorpusMetadata) -> None:
    records = metadata.read_manifest(corpus.uri)
    tally = BatchTally(records, corpus.p, track_unique=True)

    tally.add_ids(np.array([records[0].id_min, records[1].id_min, -1], dtype=np.int64))
    result = exactness_result(tally, offered_batches={0}, mode=EXACTLY_ONCE)

    assert (result["invalid_rows"], result["unoffered_rows"]) == (1, 1)
    assert result["exact"] is False and result["verdict"] is False


@pytest.mark.parametrize(
    ("fault", "loss", "duplicates", "unexpected"),
    [
        ("loss", 1, 0, 0),
        ("duplicate", 0, 1, 0),
        ("substitution", 1, 0, 1),
        ("equal_total_loss", 1, 1, 0),
    ],
)
def test_terminal_scan_catches_replacement_damage_hidden_by_correct_append_history(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    fault: str,
    loss: int,
    duplicates: int,
    unexpected: int,
) -> None:
    table, records = _table_with_history(tmp_path, corpus, fault)
    rows = _all_rows(records, corpus)
    if fault == "loss":
        replacement = rows.slice(1)
    elif fault == "duplicate":
        replacement = pa.concat_tables([rows, rows.slice(0, 1)])
    elif fault == "substitution":
        replacement = _replace_first_id(rows, records[0].id_max + 1)
    else:
        replacement = _replace_first_id(rows, records[0].id_min + 1)
    table.overwrite(replacement)

    code, result = _run(tmp_path, corpus, table, records)

    assert code == 2 and result["verdict"] is False
    assert result["loss_rows"] == loss
    assert result["duplicate_rows"] == duplicates
    assert result["unexpected_rows"] == unexpected
    if fault in {"substitution", "equal_total_loss"}:
        assert result["rows"] == result["expected_rows"]


def test_terminal_scan_accepts_a_healthy_rewrite_and_records_snapshot_identity(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    table, records = _table_with_history(tmp_path, corpus, "healthy")
    table.overwrite(_all_rows(records, corpus))

    code, result = _run(tmp_path, corpus, table, records)

    assert code == 0 and result["verdict"] is True
    current = table.current_snapshot()
    assert current is not None
    assert result["snapshot_id"] == current.snapshot_id
    assert result["metadata"] == {
        "location": table.metadata_location,
        "table_location": table.metadata.location,
        "table_uuid": str(table.metadata.table_uuid),
    }
    assert cast(dict[str, object], result["offer"])["complete"] is True
    assert result["validation_scope"] == "row_ids_only"


@pytest.mark.parametrize(
    ("mode", "expected_code", "expected_verdict"),
    [(EXACTLY_ONCE, 2, False), (AT_LEAST_ONCE, 0, True)],
)
def test_terminal_scan_scores_duplicate_rows_under_configured_exactness(
    tmp_path: Path,
    corpus: metadata.CorpusMetadata,
    mode: str,
    expected_code: int,
    expected_verdict: bool,
) -> None:
    table, records = _table_with_history(tmp_path, corpus, f"duplicate-{mode.lower()}")
    rows = _all_rows(records, corpus)
    table.overwrite(pa.concat_tables([rows, rows.slice(0, 1)]))

    code, result = _run(tmp_path, corpus, table, records, exactness_mode=mode)

    assert code == expected_code
    assert result["mode"] == mode
    assert result["duplicate_rows"] == 1
    assert result["loss_rows"] == 0
    assert result["exact"] is False
    assert result["verdict"] is expected_verdict


def test_terminal_scan_rejects_duplicate_that_masks_loss_under_at_least_once(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    table, records = _table_with_history(tmp_path, corpus, "masked_loss_alo")
    rows = _replace_first_id(_all_rows(records, corpus), records[0].id_min + 1)
    table.overwrite(rows)

    code, result = _run(tmp_path, corpus, table, records, exactness_mode=AT_LEAST_ONCE)

    assert code == 2 and result["verdict"] is False
    assert result["mode"] == AT_LEAST_ONCE
    assert result["rows"] == result["expected_rows"]
    assert result["loss_rows"] == 1 and result["duplicate_rows"] == 1


def test_verify_table_uploads_its_verdict_for_an_in_cluster_validator(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    """A Job's /work is gone with the pod; the verdict has to reach the run's prefix, pass or fail."""
    table, records = _table_with_history(tmp_path, corpus, "uploaded")
    table.overwrite(_replace_first_id(_all_rows(records, corpus), records[0].id_min + 1))
    prefix = tmp_path / "uploaded" / "scores"

    code, result = _run(tmp_path, corpus, table, records, upload_prefix=prefix)

    assert code == 2 and result["verdict"] is False
    assert json.loads((prefix / "terminal-exactness.json").read_text(encoding="utf-8")) == result


def test_terminal_scan_requires_every_configured_publish_shard(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    table, records = _table_with_history(tmp_path, corpus, "shards")

    code, result = _run(tmp_path, corpus, table, records, publish_shards=2)

    assert code == 0 and result["verdict"] is True
    offer = cast(dict[str, object], result["offer"])
    assert offer["complete"] is True and offer["completed_shards"] == [0, 1]


def test_terminal_scan_uses_current_snapshot_instead_of_sequence_last(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    table, records = _table_with_history(tmp_path, corpus, "current")
    current = table.current_snapshot()
    assert current is not None
    healthy_snapshot_id = current.snapshot_id
    table.overwrite(_all_rows(records, corpus).slice(1))
    broken = table.current_snapshot()
    assert broken is not None and broken.snapshot_id != healthy_snapshot_id
    table.manage_snapshots().set_current_snapshot(snapshot_id=healthy_snapshot_id).commit()

    code, result = _run(tmp_path, corpus, table, records)

    assert code == 0 and result["verdict"] is True
    assert result["snapshot_id"] == healthy_snapshot_id


def test_terminal_scan_rejects_rows_from_an_unoffered_batch(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    table, records = _table_with_history(tmp_path, corpus, "unoffered")

    code, result = _run(tmp_path, corpus, table, records, offered=records[:-1])

    assert code == 2 and result["verdict"] is False
    assert result["unoffered_rows"] == records[-1].rows
    assert cast(dict[str, object], result["offer"])["complete"] is True


def test_terminal_scan_rejects_an_id_outside_the_corpus_manifest(
    tmp_path: Path, corpus: metadata.CorpusMetadata
) -> None:
    table, records = _table_with_history(tmp_path, corpus, "invalid")
    rows = _replace_first_id(_all_rows(records, corpus), -1)
    table.overwrite(rows)

    code, result = _run(tmp_path, corpus, table, records)

    assert code == 2 and result["verdict"] is False
    assert result["invalid_rows"] == 1 and result["loss_rows"] == 1


def test_terminal_scan_requires_complete_publish_trailers(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    table, records = _table_with_history(tmp_path, corpus, "unfinished")

    code, result = _run(tmp_path, corpus, table, records, done=False)

    assert code == 2 and result["verdict"] is False
    assert cast(dict[str, object], result["offer"])["complete"] is False
    assert "completion trailer" in str(result["reason"])
    assert result["expected_rows"] is None and result["snapshot_id"] is not None


def test_scan_error_replaces_a_stale_passing_artifact(tmp_path: Path, corpus: metadata.CorpusMetadata) -> None:
    table, records = _table_with_history(tmp_path, corpus, "missing")
    out = tmp_path / "scores"
    code, passing = _run(tmp_path, corpus, table, records, out=out)
    assert code == 0 and passing["verdict"] is True

    document, io = geometry.open_metadata_document(table.metadata_location, _props(tmp_path))
    current = document.current_snapshot_id
    assert current is not None
    data_file = geometry.data_files_at(document, current, io)[0]
    parsed = urlparse(data_file.path)
    Path(unquote(parsed.path if parsed.scheme else data_file.path)).unlink()

    code, failed = _run(tmp_path, corpus, table, records, out=out)

    assert code == 1 and failed["verdict"] is False and failed["status"] == "error"
    assert "No such file" in str(failed["reason"])


def test_terminal_scan_rejects_unsupported_equality_deletes(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    table, records = _table_with_history(tmp_path, corpus, "deletes")
    original = ManifestFile.fetch_manifest_entry

    def with_delete(self: ManifestFile, io: FileIO, discard_deleted: bool = True) -> list[ManifestEntry]:
        entries = original(self, io, discard_deleted=discard_deleted)
        first = copy(entries[0])
        delete_file = copy(first.data_file)
        delete_file[0] = DataFileContent.EQUALITY_DELETES
        first.data_file = delete_file
        return [first, *entries[1:]]

    monkeypatch.setattr(ManifestFile, "fetch_manifest_entry", with_delete)

    code, result = _run(tmp_path, corpus, table, records)

    assert code == terminal.ERROR and result["verdict"] is False
    assert result["status"] == "error"
    assert "equality deletes" in str(result["reason"])
