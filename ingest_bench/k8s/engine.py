# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Print managed engine and compactor Kubernetes descriptors for shell drivers.

One requested field prints its value. Multiple fields, or no selection,
print one ``field=value`` line per field.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from ingest_bench.specs import compactors, engines
from ingest_bench.specs.kubernetes import COMPACTOR_FIELDS, FIELDS


def render(name: str, requested: Sequence[str], *, compaction: bool = False) -> str:
    """Render requested descriptor fields, rejecting unknown names."""
    if compaction:
        texts = compactors.kubernetes_for(name).texts()
        fields = COMPACTOR_FIELDS
        owner = "managed compactor"
    else:
        texts = engines.kubernetes_for(name).texts()
        fields = FIELDS
        owner = "managed engine"
    # Reject missing and unexpected descriptor fields.
    if set(texts) != set(fields):
        raise ValueError(f"{name}'s descriptor prints {sorted(texts)}, and its fields are {list(fields)}")
    unknown = [field for field in requested if field not in texts]
    if unknown:
        raise ValueError(f"no such field of a {owner}'s cluster shape: {unknown}; they are {list(fields)}")
    if len(requested) == 1:
        return texts[requested[0]] + "\n"
    selected = list(requested) if requested else list(fields)
    return "".join(f"{field}={texts[field]}\n" for field in selected)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="engine-k8s",
        description="Print a managed engine or compactor Kubernetes descriptor for shell drivers.",
    )
    parser.add_argument("--compaction", action="store_true", help="read a managed compactor descriptor")
    parser.add_argument(
        "engine",
        metavar="NAME",
        help=(
            f"a managed engine ({', '.join(sorted(engines.MANAGED))}) or, with --compaction, "
            f"a compactor ({', '.join(sorted(compactors.MANAGED))})"
        ),
    )
    parser.add_argument(
        "fields",
        metavar="FIELD",
        nargs="*",
        help=(
            "descriptor fields to print. One field prints only its value; "
            "multiple or no fields print field=value lines. "
            f"Engine fields are: {', '.join(FIELDS)}. "
            f"Compactor fields are: {', '.join(COMPACTOR_FIELDS)}"
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    try:
        sys.stdout.write(
            render(str(parsed.engine), [str(field) for field in parsed.fields], compaction=bool(parsed.compaction))
        )
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
