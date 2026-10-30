# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Join and split rendered SQL, and resolve environment references.

The Java SQL runner follows the same delimiter and environment-reference
conventions when submitting these scripts.
"""

from __future__ import annotations

import os
import re
from collections.abc import Iterable

# Split at line-ending semicolons so embedded SASL semicolons remain intact.
STATEMENT_TERMINATOR = ";\n"

# Separate statements with a blank line for readability.
_STATEMENT_GAP = "\n"


# Keep aligned with ingest_bench.specs.env and the Java SQL runner.
_ENV_PLACEHOLDER = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")


def substitute_env(text: str) -> str:
    """Replace ``${env:NAME}`` references using the container's environment.

    Reject unset variables with a named error instead of an opaque authentication
    failure. Substitution does not modify the rendered files.
    """

    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"${{env:{name}}} is not set in the environment")
        return os.environ[name]

    return _ENV_PLACEHOLDER.sub(replace, text)


def join_statements(statements: Iterable[str]) -> str:
    """Join statements into a script that split_statements can read."""
    return (STATEMENT_TERMINATOR + _STATEMENT_GAP).join(statements) + STATEMENT_TERMINATOR


def split_statements(script: str) -> list[str]:
    """Return script statements in submission order."""
    return [statement.strip() for statement in script.split(STATEMENT_TERMINATOR) if statement.strip()]
