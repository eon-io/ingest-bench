# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Substitute ``__NAME__`` markers in Kubernetes templates.

Drivers choose values and apply the result. This renderer performs one-pass
substitution and rejects missing or unused variables.
"""

from __future__ import annotations

import argparse
import re
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path

from ingest_bench.catalog import parse_key_values

MARKER_RE = re.compile(r"__[A-Z_]+__")


def render_template(path: Path, variables: Mapping[str, str]) -> str:
    """Replace each template marker once.

    Marker-like text inside replacement values remains literal.
    """
    text = path.read_text(encoding="utf-8")
    markers = {match[2:-2] for match in MARKER_RE.findall(text)}
    missing = sorted(markers - set(variables))
    if missing:
        raise ValueError(f"{path} has no value for {missing}; pass one --set for each")
    unused = sorted(set(variables) - markers)
    if unused:
        raise ValueError(f"{path} holds no marker for {unused}; a value that reaches no manifest is a typo")
    return MARKER_RE.sub(lambda match: variables[match.group(0)[2:-2]], text)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="render-k8s", description="Render a Kubernetes manifest template.")
    parser.add_argument("template", metavar="PATH", help="the template to render")
    parser.add_argument(
        "--set",
        dest="values",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="value for a __NAME__ marker; repeat for each marker. Missing or unused values are errors",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    rendered = render_template(
        Path(str(parsed.template)), parse_key_values([str(value) for value in parsed.values], "--set")
    )
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
