# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Register Avro schemas and construct Confluent record headers.

Staging registers the corpus schema once with the configured registry. The
producer prepends a zero magic byte and four-byte big-endian schema ID to
each Avro value when Confluent framing is selected.
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.request
from typing import cast
from urllib.parse import quote

# The Confluent wire format: a zero byte, then the schema's registry id as a
# four-byte big-endian integer, then the Avro binary of the value.
CONFLUENT_MAGIC = b"\x00"
_SCHEMA_ID_BYTES = 4

# Use the registry API's versioned JSON media type.
_CONTENT_TYPE = "application/vnd.schemaregistry.v1+json"

# Bound registry requests so staging cannot hang indefinitely.
_TIMEOUT_S = 30.0


def subject_for(topic: str) -> str:
    """Return the value subject using TopicNameStrategy: ``<topic>-value``."""
    return f"{topic}-value"


def confluent_header(schema_id: int) -> bytes:
    if schema_id < 0:
        raise ValueError(f"a schema id is a non-negative integer, got {schema_id}")
    return CONFLUENT_MAGIC + schema_id.to_bytes(_SCHEMA_ID_BYTES, "big")


def register_schema(url: str, basic_auth_user_info: str | None, subject: str, schema_text: str) -> int:
    """Register an Avro schema under ``subject`` and return its ID.

    Repeated registration of the same schema is idempotent. Optional basic-auth
    credentials must already be resolved as ``user:password``.
    """
    endpoint = f"{url.rstrip('/')}/subjects/{quote(subject, safe='')}/versions"
    headers = {"Content-Type": _CONTENT_TYPE}
    if basic_auth_user_info is not None:
        headers["Authorization"] = f"Basic {base64.b64encode(basic_auth_user_info.encode()).decode()}"
    request = urllib.request.Request(
        endpoint,
        data=json.dumps({"schema": schema_text, "schemaType": "AVRO"}).encode(),
        headers=headers,
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=_TIMEOUT_S) as response:
            body = response.read().decode("utf-8")
    except urllib.error.HTTPError as error:
        # Include the registry response body for schema and subject errors.
        detail = error.read().decode("utf-8", errors="replace")
        raise ValueError(f"POST {endpoint} was refused with HTTP {error.code}: {detail}") from error
    except urllib.error.URLError as error:
        raise ValueError(f"POST {endpoint} could not be reached: {error.reason}") from error
    payload = cast(dict[str, object], json.loads(body))
    if "id" not in payload:
        raise ValueError(f"POST {endpoint} answered without a schema id: {body}")
    schema_id = payload["id"]
    if isinstance(schema_id, bool) or not isinstance(schema_id, int):
        raise ValueError(f"POST {endpoint} answered with a schema id that is not an integer: {body}")
    return schema_id
