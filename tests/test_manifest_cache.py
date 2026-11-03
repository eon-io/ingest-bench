# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Reuse planning bytes without retaining inherited manifest-entry state."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
from pyiceberg.io import FileIO, InputFile, InputStream
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import ManifestFile

from ingest_bench.corpus import metadata
from ingest_bench.scorer import live_rows
from ingest_bench.scorer.manifest_cache import ManifestCache
from ingest_bench.scorer.tally import BatchTally
from ingest_bench.table import create
from tests.test_terminal import _all_rows, _props
from tests.test_terminal import corpus as corpus


class _CountingInput(InputFile):
    def __init__(self, source: InputFile, reads: Counter[str]) -> None:
        super().__init__(source.location)
        self.source = source
        self.reads = reads

    def __len__(self) -> int:
        return len(self.source)

    def exists(self) -> bool:
        return self.source.exists()

    def open(self, seekable: bool = True) -> InputStream:
        self.reads[self.location] += 1
        return self.source.open(seekable)


def _count_reads(io: FileIO, monkeypatch: pytest.MonkeyPatch) -> Counter[str]:
    reads: Counter[str] = Counter()
    original = io.new_input
    monkeypatch.setattr(io, "new_input", lambda location: _CountingInput(original(location), reads))
    return reads


def _read(cache: ManifestCache, path: Path) -> bytes:
    with cache.new_input(str(path)).open() as stream:
        return stream.read()


def test_manifest_cache_evicts_least_recently_used_bytes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    io = PyArrowFileIO()
    reads = _count_reads(io, monkeypatch)
    paths = [tmp_path / name for name in ("a", "b", "c")]
    for path in paths:
        path.write_bytes(path.name.encode() * 3)
    cache = ManifestCache(io, max_bytes=6)
    for index in (0, 1, 0, 2, 0, 1):
        assert _read(cache, paths[index]) == paths[index].name.encode() * 3
    assert reads == {str(paths[0]): 1, str(paths[1]): 2, str(paths[2]): 1}
    assert cache._bytes == 6
    assert sum(map(len, cache._entries.values())) == cache._bytes


def test_manifest_cache_bounds_empty_input_entries(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    io = PyArrowFileIO()
    reads = _count_reads(io, monkeypatch)
    paths = [tmp_path / name for name in ("a", "b", "c")]
    cache = ManifestCache(io, max_entries=2)
    for path in paths:
        path.write_bytes(b"")
        assert _read(cache, path) == b""
    assert len(cache._entries) == 2
    assert _read(cache, paths[0]) == b""
    assert reads[str(paths[0])] == 2


def test_manifest_cache_bypasses_oversized_files(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "large"
    path.write_bytes(b"12345")
    io = PyArrowFileIO()
    reads = _count_reads(io, monkeypatch)
    cache = ManifestCache(io, max_bytes=4)
    assert _read(cache, path) == _read(cache, path) == b"12345"
    assert reads[str(path)] == 2
    assert cache._bytes == 0


def test_manifest_cache_retries_failed_reads(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "manifest"
    path.write_bytes(b"contents")
    io = PyArrowFileIO()
    reads = _count_reads(io, monkeypatch)
    original = _CountingInput.open

    def fail_once(source: _CountingInput, seekable: bool = True) -> InputStream:
        monkeypatch.setattr(_CountingInput, "open", original)
        raise OSError("temporary input failure")

    monkeypatch.setattr(_CountingInput, "open", fail_once)
    cache = ManifestCache(io)
    with pytest.raises(OSError, match="temporary input failure"):
        _read(cache, path)
    assert _read(cache, path) == _read(cache, path) == b"contents"
    assert reads[str(path)] == 1


def test_manifest_cache_delegates_input_metadata_and_properties(tmp_path: Path) -> None:
    path = tmp_path / "manifest"
    io = PyArrowFileIO({"custom": "value"})
    cache = ManifestCache(io)
    source = cache.new_input(str(path))
    assert source.location == str(path)
    assert not source.exists()
    assert cache.properties == io.properties
    with cache.new_output(str(path)).create() as stream:
        stream.write(b"123")
    assert source.exists() and len(source) == 3
    assert _read(cache, path) == b"123"
    cache.delete(source)
    assert not source.exists()
    with pytest.raises(FileNotFoundError):
        _read(cache, path)


@pytest.mark.parametrize("n", [8, 32])
def test_overwrite_planning_reuses_immutable_inputs(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch, n: int
) -> None:
    table = create.create_table(_props(tmp_path), "bench.cache", corpus, create.parse_partition("unpartitioned"), {})
    records = metadata.read_manifest(corpus.uri)
    rows = _all_rows(records[:1], corpus).slice(0, 1)
    for _ in range(n):
        table.append(rows)
    latest = table.current_snapshot()
    assert latest is not None
    reads = _count_reads(table.io, monkeypatch)
    tally = BatchTally(records, corpus.p)
    uncached = live_rows.incoming_files(table.metadata, latest.snapshot_id, table.io, tally, 4)
    assert uncached is not None and len(uncached) == 1
    manifests = {manifest.manifest_path for manifest in latest.manifests(table.io)}
    assert sum(reads[path] for path in manifests) == 2 * n - 1
    reads.clear()
    cache = ManifestCache(table.io)
    for _ in range(2):
        incoming = live_rows.incoming_files(
            table.metadata, latest.snapshot_id, table.io, tally, 4, manifest_cache=cache
        )
        assert incoming is not None and incoming.keys() == uncached.keys()
        assert sum(reads[path] for path in manifests) == n
    table.append(rows)
    added = table.current_snapshot()
    assert added is not None
    live_rows.incoming_files(table.metadata, added.snapshot_id, table.io, tally, 4, manifest_cache=cache)
    manifests.update(manifest.manifest_path for manifest in added.manifests(table.io))
    assert sum(reads[path] for path in manifests) == n + 1


def test_manifest_cache_preserves_manifest_list_inheritance(
    tmp_path: Path, corpus: metadata.CorpusMetadata, monkeypatch: pytest.MonkeyPatch
) -> None:
    table = create.create_table(_props(tmp_path), "bench.inherit", corpus, create.parse_partition("unpartitioned"), {})
    table.append(_all_rows(metadata.read_manifest(corpus.uri)[:1], corpus))
    snapshot = table.current_snapshot()
    assert snapshot is not None
    manifest = snapshot.manifests(table.io)[0]
    assert manifest.added_snapshot_id is not None
    changed = ManifestFile.from_args(
        manifest_path=manifest.manifest_path,
        manifest_length=manifest.manifest_length,
        sequence_number=manifest.sequence_number + 10,
        added_snapshot_id=manifest.added_snapshot_id + 10,
        partition_spec_id=manifest.partition_spec_id + 10,
    )
    reads = _count_reads(table.io, monkeypatch)
    cache = ManifestCache(table.io)
    first = manifest.fetch_manifest_entry(cache)[0]
    second = changed.fetch_manifest_entry(cache)[0]
    expected = changed.fetch_manifest_entry(table.io)[0]
    assert second.sequence_number == expected.sequence_number == changed.sequence_number
    assert second.snapshot_id == expected.snapshot_id
    assert second.data_file.spec_id == expected.data_file.spec_id == changed.partition_spec_id
    assert first.sequence_number == manifest.sequence_number
    assert first.data_file.spec_id == manifest.partition_spec_id
    assert reads[manifest.manifest_path] == 2


def test_manifest_cache_opens_without_a_separate_size_lookup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "manifest"
    path.write_bytes(b"contents")
    io = PyArrowFileIO()
    reads = _count_reads(io, monkeypatch)

    def unexpected_size(source: _CountingInput) -> int:
        raise AssertionError("size lookup adds a remote metadata request")

    monkeypatch.setattr(_CountingInput, "__len__", unexpected_size)
    cache = ManifestCache(io)
    assert _read(cache, path) == _read(cache, path) == b"contents"
    assert reads[str(path)] == 1
