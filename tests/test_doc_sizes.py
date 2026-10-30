# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Keep documents within their scope and line budgets. Move unrelated material
before increasing a budget; update the budget when a document gains necessary scope.
"""

from __future__ import annotations

from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Budgets reflect each document's scope and expected reading style.
MAX_LINES: dict[str, int] = {
    "README.md": 160,
    "CONTRIBUTING.md": 150,
    # Upsert runs add key coverage, sampled live-state validation and diagnostics.
    "docs/methodology.md": 320,
    # Column kinds add nullability, nesting and per-engine type support; upsert corpora add keys, the seed
    # and its Kafka delivery; events200 adds the wide-row presets.
    "docs/corpus.md": 390,
    # Include JSON/Avro smokes, the upsert scan Job and local freshness rescoring.
    "docs/running.md": 390,
    # Tracks add a fourth run-spec section.
    "docs/run-spec.md": 260,
    # Upsert runs replace rule 3 and add connection facts.
    "docs/adding-an-engine.md": 260,
    # Campaign reporting adds phase, coverage, verdict and scorer-poll caveats.
    "docs/results-format.md": 275,
    "docs/pitfalls.md": 150,
    "docs/submitting.md": 130,
    # Broker sizing formulas and worked examples for both in-cluster and MSK brokers.
    "deploy/k8s/stack/README.md": 285,
    # Maintenance knobs, JSON decoding, the reference tier, column-kind limits and upsert runs extend the guide.
    "engines/flink/README.md": 207,
    # Upsert runs add the MERGE path and the reference table.
    "engines/spark/README.md": 167,
}


@pytest.mark.parametrize("name", sorted(MAX_LINES), ids=sorted(MAX_LINES))
def test_the_document_is_inside_its_budget(name: str) -> None:
    path = REPO_ROOT / name
    lines = len(path.read_text(encoding="utf-8").splitlines())
    assert lines <= MAX_LINES[name], (
        f"{name} is {lines} lines, over its {MAX_LINES[name]}-line budget. "
        "Move what belongs to another document, or raise the budget in this file."
    )


def test_every_budgeted_document_exists() -> None:
    missing = sorted(name for name in MAX_LINES if not (REPO_ROOT / name).is_file())
    assert not missing, f"budgeted but absent: {', '.join(missing)}"
