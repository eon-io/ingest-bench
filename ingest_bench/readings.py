# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Read engine status and report configuration drift consistently.

Shared readers validate response shapes. Missing or malformed required data
is unverified, not a successful comparison. Drivers distinguish drift,
unreadable responses, and pending placement through separate exit codes.
"""

from __future__ import annotations

import json
import urllib.request
from collections.abc import Callable
from typing import cast

# Keep verification requests shorter than the driver's overall wait.
REST_TIMEOUT_S = 5.0

# Separate configuration drift from an unreadable response for driver retries.
DRIFT_EXIT = 3
UNVERIFIED_EXIT = 2
# Pending placement uses the engine startup timeout, not endpoint retries.
PENDING_EXIT = 4

# Explicit label for absent reported fields.
NOT_REPORTED = "not reported"


def document(value: object, where: str) -> dict[str, object]:
    if not isinstance(value, dict):
        raise ValueError(f"{where} returned {type(value).__name__}; expected a JSON object")
    return {str(key): entry for key, entry in cast(dict[object, object], value).items()}


def documents(value: object, where: str) -> list[dict[str, object]]:
    if not isinstance(value, list):
        raise ValueError(f"{where} returned {type(value).__name__}; expected a JSON array")
    return [document(entry, f"{where}[{index}]") for index, entry in enumerate(cast(list[object], value))]


def field(holder: dict[str, object], key: str, where: str) -> object:
    if key not in holder:
        raise ValueError(f"{where} is missing {key!r}; available fields: {sorted(holder)}")
    return holder[key]


def int_field(holder: dict[str, object], key: str, where: str) -> int:
    value = field(holder, key, where)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{where} returned {key}={value!r}; expected an integer")
    return value


def str_field(holder: dict[str, object], key: str, where: str) -> str:
    value = field(holder, key, where)
    if not isinstance(value, str):
        raise ValueError(f"{where} returned {key}={value!r}; expected a string")
    return value


def optional_str_field(holder: dict[str, object], key: str, where: str) -> str:
    """Read a string field, returning empty when absent.

    Use for status fields that may appear later, such as pod QoS. Reject a
    present field of the wrong type.
    """
    if key not in holder:
        return ""
    return str_field(holder, key, where)


def line(what: str, spec: object, engine: object) -> str:
    """Format a configuration mismatch for engine verification output."""
    return f"{what}: spec {spec}, engine {engine}"


def fetch_json(base_url: str) -> Callable[[str], object]:
    """Create an HTTP JSON reader that reports failures with the requested URL."""
    root = base_url.rstrip("/")

    def fetch(path: str) -> object:
        url = f"{root}{path}"
        try:
            with urllib.request.urlopen(url, timeout=REST_TIMEOUT_S) as answer:
                return cast(object, json.loads(answer.read()))
        except (OSError, ValueError) as error:
            raise ValueError(f"could not read {url}: {error}") from error

    return fetch
