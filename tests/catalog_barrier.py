# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""A test-only REST catalog proxy that holds one rewrite commit until an append lands.

The sidecar is pointed at this proxy instead of the catalog; every request is
forwarded unchanged except the first table commit carrying requirements, which
waits until the upstream table's current snapshot changes to a new APPEND before
it is forwarded. That makes the stale-metadata race deterministic: the rewrite
was planned against a snapshot that is no longer current when its commit reaches
the catalog, so the catalog must reject it and the client must retry against the
new metadata, or fail safely. Nothing here runs outside the integration test.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from datetime import UTC, datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

HOP_HEADERS = frozenset({"connection", "keep-alive", "transfer-encoding", "content-length", "host"})


def _iso(moment: float) -> str:
    return datetime.fromtimestamp(moment, UTC).isoformat().replace("+00:00", "Z")


@dataclass
class BarrierState:
    upstream: str
    max_hold_s: float
    poll_s: float = 0.5
    lock: threading.Lock = field(default_factory=threading.Lock)
    held: bool = False
    commits: list[dict[str, object]] = field(default_factory=list)
    hold: dict[str, object] | None = None

    def report(self) -> dict[str, object]:
        return {"hold": self.hold, "commits": list(self.commits)}


class _Handler(BaseHTTPRequestHandler):
    state: BarrierState
    protocol_version = "HTTP/1.1"

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - BaseHTTPRequestHandler's name
        return

    def _forward(self, body: bytes | None) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(
            self.state.upstream + self.path,
            data=body,
            method=self.command,
            headers={key: value for key, value in self.headers.items() if key.lower() not in HOP_HEADERS},
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:
                return response.status, dict(response.headers.items()), response.read()
        except urllib.error.HTTPError as error:
            return error.code, dict(error.headers.items()), error.read()

    def _current_snapshot(self, table_path: str) -> tuple[int | None, str | None]:
        status, _, payload = self._forward_path("GET", table_path)
        if status != 200:
            return None, None
        metadata = json.loads(payload).get("metadata", {})
        current = metadata.get("current-snapshot-id")
        operation = None
        for snapshot in metadata.get("snapshots", []):
            if snapshot.get("snapshot-id") == current:
                operation = snapshot.get("summary", {}).get("operation")
        return current, operation

    def _forward_path(self, method: str, path: str) -> tuple[int, dict[str, str], bytes]:
        request = urllib.request.Request(self.state.upstream + path, method=method)
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.status, dict(response.headers.items()), response.read()
        except urllib.error.HTTPError as error:
            return error.code, dict(error.headers.items()), error.read()

    def _is_commit(self, body: bytes | None) -> bool:
        if self.command != "POST" or "/tables/" not in self.path or self.path.endswith("/metrics"):
            return False
        if not body:
            return False
        try:
            document = json.loads(body)
        except json.JSONDecodeError:
            return False
        return isinstance(document, dict) and "requirements" in document and "updates" in document

    def _hold_until_append(self, body: bytes) -> None:
        table_path = self.path.split("?", 1)[0]
        base_id, base_operation = self._current_snapshot(table_path)
        started = time.time()
        hold: dict[str, object] = {
            "held_at": _iso(started),
            "table": table_path,
            "snapshot_at_hold": base_id,
            "operation_at_hold": base_operation,
            "requirements": json.loads(body).get("requirements"),
        }
        released_by: int | None = None
        while time.time() - started < self.state.max_hold_s:
            current, operation = self._current_snapshot(table_path)
            if current is not None and current != base_id and operation == "append":
                released_by = current
                break
            time.sleep(self.state.poll_s)
        hold.update(
            {
                "released_at": _iso(time.time()),
                "released_by_append_snapshot": released_by,
                "timed_out": released_by is None,
            }
        )
        self.state.hold = hold

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(length) if length else None
        commit = self._is_commit(body)
        held_here = False
        if commit and body is not None:
            with self.state.lock:
                if not self.state.held:
                    self.state.held = True
                    held_here = True
            if held_here:
                self._hold_until_append(body)
        status, headers, payload = self._forward(body)
        if commit:
            self.state.commits.append({"at": _iso(time.time()), "status": status, "held": held_here})
        self.send_response(status)
        for key, value in headers.items():
            if key.lower() not in HOP_HEADERS:
                self.send_header(key, value)
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    do_GET = do_POST = do_PUT = do_DELETE = do_HEAD = do_PATCH = _handle


class CatalogBarrier:
    """Serve on an ephemeral local port; `start()` returns the port."""

    def __init__(self, upstream: str, *, max_hold_s: float = 120.0) -> None:
        self.state = BarrierState(upstream=upstream.rstrip("/"), max_hold_s=max_hold_s)
        handler = type("BarrierHandler", (_Handler,), {"state": self.state})
        self.server = ThreadingHTTPServer(("0.0.0.0", 0), handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> int:
        self.thread.start()
        return int(self.server.server_address[1])

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()

    def report(self) -> dict[str, object]:
        return self.state.report()
