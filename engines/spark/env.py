# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Resolve rendered environment references inside the Spark image.

Keep this module standard-library-only: the image lacks the harness package.
Tests keep its placeholder syntax aligned with engines.flink.script and
ingest_bench.specs.env.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

_ENV_PLACEHOLDER = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def substitute_env(text: str) -> str:
    """Replace environment references, rejecting unset variables by name."""

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"${{env:{name}}} is not set in the environment")
        return os.environ[name]

    return _ENV_PLACEHOLDER.sub(replace, text)


def substitute_env_values(values: Mapping[str, str]) -> dict[str, str]:
    """Return a copy of ``values`` with environment references resolved."""
    return {key: substitute_env(value) for key, value in values.items()}
