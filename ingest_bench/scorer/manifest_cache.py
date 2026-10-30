# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Bound reuse of immutable manifest bytes within one scoring run."""

from __future__ import annotations

from collections import OrderedDict
from io import BytesIO
from threading import Lock

from pyiceberg.io import FileIO, InputFile, InputStream, OutputFile


class _CachedInput(InputFile):
    def __init__(self, cache: ManifestCache, location: str) -> None:
        super().__init__(location)
        self._cache = cache

    def __len__(self) -> int:
        return len(self._cache._io.new_input(self.location))

    def exists(self) -> bool:
        return self._cache._io.new_input(self.location).exists()

    def open(self, seekable: bool = True) -> InputStream:
        return self._cache._open(self.location, seekable)


class ManifestCache(FileIO):
    """Cache immutable planning inputs within payload-byte and entry-count limits.

    Cache bytes rather than parsed entries so PyIceberg still inherits sequence numbers,
    snapshot IDs and partition specs from each snapshot's manifest-list entry.
    """

    def __init__(self, io: FileIO, max_bytes: int = 64 * 1024 * 1024, max_entries: int = 4096) -> None:
        super().__init__(io.properties)
        if max_bytes < 1 or max_entries < 1:
            raise ValueError("manifest cache limits must be positive")
        self._io = io
        self._max_bytes = max_bytes
        self._max_entries = max_entries
        self._bytes = 0
        self._entries: OrderedDict[str, bytes] = OrderedDict()
        self._lock = Lock()

    def new_input(self, location: str) -> InputFile:
        return _CachedInput(self, location)

    def new_output(self, location: str) -> OutputFile:
        self._discard(location)
        return self._io.new_output(location)

    def delete(self, location: str | InputFile | OutputFile) -> None:
        self._discard(location if isinstance(location, str) else location.location)
        self._io.delete(location)

    def _discard(self, location: str) -> None:
        with self._lock:
            self._bytes -= len(self._entries.pop(location, b""))

    def _open(self, location: str, seekable: bool) -> InputStream:
        with self._lock:
            if (cached := self._entries.get(location)) is not None:
                self._entries.move_to_end(location)
                return BytesIO(cached)
        source = self._io.new_input(location)
        # Avro reads the whole payload anyway; a size lookup adds a remote request.
        with source.open(seekable) as stream:
            payload = stream.read()
        if len(payload) <= self._max_bytes:
            with self._lock:
                # Concurrent scans may finish reading the same immutable input together.
                self._bytes -= len(self._entries.pop(location, b""))
                while self._entries and (
                    self._bytes + len(payload) > self._max_bytes or len(self._entries) >= self._max_entries
                ):
                    _, evicted = self._entries.popitem(last=False)
                    self._bytes -= len(evicted)
                self._entries[location] = payload
                self._bytes += len(payload)
        return BytesIO(payload)
