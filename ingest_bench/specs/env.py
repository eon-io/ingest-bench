# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Resolve environment references without writing secrets back to configuration.

Files use ``${env:NAME}``; the process that needs the credential resolves it
before configuring a client.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping

# `engines/flink/script.py` carries the same pattern. The copy is deliberate:
# that module is copied into the engine's image, where this package does not
# exist, so it may import nothing but the standard library.
PLACEHOLDER_RE = re.compile(r"\$\{env:([A-Za-z_][A-Za-z0-9_]*)\}")

# Example syntax for placeholder validation errors.
PLACEHOLDER_FORM = "${env:NAME}"

# Identify credentials by property name; values alone cannot distinguish
# tokens from ordinary strings.
SECRET_HINTS = ("token", "credential", "secret", "password", "auth_user_info", "authorization")


def has_placeholder(value: str) -> bool:
    return PLACEHOLDER_RE.search(value) is not None


def names_a_secret(key: str) -> bool:
    return any(hint in key.lower() for hint in SECRET_HINTS)


def refuse_literal_secrets(values: Mapping[str, str], where: str) -> None:
    """Reject literal credentials in properties destined for rendered artifacts.

    These properties reach ConfigMaps and object storage. Require environment
    references so only the consuming process resolves their values.
    """
    literal = sorted(key for key, value in values.items() if names_a_secret(key) and not has_placeholder(value))
    if literal:
        raise ValueError(
            f"{where} contains literal credentials in {literal}, which would be stored in ConfigMaps and "
            f"uploaded run artifacts; use {PLACEHOLDER_FORM} and store the values in the Secret named by "
            "site.kubernetes.secret_name"
        )


def _substituted(key: str, value: str) -> str:
    def replace(match: re.Match[str]) -> str:
        name = match.group(1)
        if name not in os.environ:
            raise ValueError(f"${{env:{name}}} is not set in the environment, and {key!r} references it")
        return os.environ[name]

    return PLACEHOLDER_RE.sub(replace, value)


def resolve_env_placeholders(values: Mapping[str, str]) -> dict[str, str]:
    """Replace ``${env:NAME}`` references with environment values.

    Reject unset variables with a property-specific error.
    """
    return {key: _substituted(key, value) for key, value in values.items()}
