# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Test registration requests and error handling against a local HTTP server."""

from __future__ import annotations

import base64
import json
import threading
from collections.abc import Iterator
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import cast

import pytest

from ingest_bench import schema_registry

SCHEMA_TEXT = '{"type": "record", "name": "event", "fields": [{"name": "a", "type": "long"}]}'


@dataclass
class Recorded:
    """One request the fake registry served."""

    path: str
    headers: dict[str, str]
    body: dict[str, object]


@dataclass
class FakeRegistry:
    """A registry endpoint on a real socket, answering with ``status`` and ``body``."""

    status: int = 200
    body: str = '{"id": 7}'
    requests: list[Recorded] = field(default_factory=list)


def _serve(registry: FakeRegistry) -> tuple[HTTPServer, threading.Thread]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802  (the name is BaseHTTPRequestHandler's contract)
            length = int(self.headers["Content-Length"])
            raw = self.rfile.read(length).decode("utf-8")
            registry.requests.append(
                Recorded(
                    path=self.path,
                    headers={key: value for key, value in self.headers.items()},
                    body=cast(dict[str, object], json.loads(raw)),
                )
            )
            payload = registry.body.encode()
            self.send_response(registry.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            """Silence the handler's own stderr line per request."""

    server = HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


@pytest.fixture
def registry() -> FakeRegistry:
    return FakeRegistry()


@pytest.fixture
def registry_url(registry: FakeRegistry) -> Iterator[str]:
    server, thread = _serve(registry)
    # The path a Confluent-compatible endpoint is served under, which is not
    # always the server's root: the client has to append to whatever the site
    # named rather than to a host.
    yield f"http://127.0.0.1:{server.server_port}/apis/ccompat/v7"
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_the_header_is_the_magic_byte_and_four_big_endian_bytes() -> None:
    assert schema_registry.confluent_header(7) == b"\x00" + (7).to_bytes(4, "big")
    assert schema_registry.confluent_header(7) == b"\x00\x00\x00\x00\x07"
    assert len(schema_registry.confluent_header(16_777_216)) == 5
    assert schema_registry.confluent_header(0) == b"\x00\x00\x00\x00\x00"
    with pytest.raises(ValueError, match="non-negative"):
        schema_registry.confluent_header(-1)


def test_the_subject_is_the_topic_name_strategy() -> None:
    assert schema_registry.subject_for("smoke-flink-20260909T120000Z") == "smoke-flink-20260909T120000Z-value"


def test_register_posts_the_schema_and_returns_its_id(registry: FakeRegistry, registry_url: str) -> None:
    assert schema_registry.register_schema(registry_url, None, "a-topic-value", SCHEMA_TEXT) == 7
    request = registry.requests[0]
    assert request.path == "/apis/ccompat/v7/subjects/a-topic-value/versions"
    assert request.headers["Content-Type"] == "application/vnd.schemaregistry.v1+json"
    assert "Authorization" not in request.headers
    # The schema travels as a JSON string inside the body, not as the body.
    assert request.body == {"schema": SCHEMA_TEXT, "schemaType": "AVRO"}


def test_a_trailing_slash_on_the_url_does_not_double(registry: FakeRegistry, registry_url: str) -> None:
    assert schema_registry.register_schema(f"{registry_url}/", None, "a-value", SCHEMA_TEXT) == 7
    assert registry.requests[0].path == "/apis/ccompat/v7/subjects/a-value/versions"


def test_user_info_becomes_a_basic_authorization_header(registry: FakeRegistry, registry_url: str) -> None:
    assert schema_registry.register_schema(registry_url, "key:secret", "a-value", SCHEMA_TEXT) == 7
    expected = base64.b64encode(b"key:secret").decode()
    assert registry.requests[0].headers["Authorization"] == f"Basic {expected}"


@pytest.mark.parametrize(
    ("status", "body", "message"),
    (
        (409, '{"error_code": 409, "message": "Schema being registered is incompatible"}', "incompatible"),
        (500, "the registry fell over", "the registry fell over"),
    ),
)
def test_a_refusal_carries_the_status_and_the_registrys_own_reason(
    registry: FakeRegistry, registry_url: str, status: int, body: str, message: str
) -> None:
    registry.status = status
    registry.body = body
    with pytest.raises(ValueError, match=message) as raised:
        schema_registry.register_schema(registry_url, None, "a-value", SCHEMA_TEXT)
    assert str(status) in str(raised.value)


def test_an_answer_without_an_id_is_refused(registry: FakeRegistry, registry_url: str) -> None:
    registry.body = '{"version": 1}'
    with pytest.raises(ValueError, match="without a schema id"):
        schema_registry.register_schema(registry_url, None, "a-value", SCHEMA_TEXT)
    registry.body = '{"id": "seven"}'
    with pytest.raises(ValueError, match="not an integer"):
        schema_registry.register_schema(registry_url, None, "a-value", SCHEMA_TEXT)


def test_an_unreachable_registry_names_the_endpoint() -> None:
    # Port 1 on the loopback: nothing listens, and the refusal is immediate.
    with pytest.raises(ValueError, match="could not be reached"):
        schema_registry.register_schema("http://127.0.0.1:1", None, "a-value", SCHEMA_TEXT)
