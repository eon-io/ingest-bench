# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Measure backlog and drain time.

These metrics distinguish sustained ingest from a fleet that catches up only
after the offer ends. Use rows consistently with the corpus and tally.
"""

from __future__ import annotations

from dataclasses import dataclass, field

import numpy as np

from ingest_bench.producer.publish_log import ProducerWindow


@dataclass(frozen=True)
class KeepupSample:
    """Row totals, backlog, and interval rates at one sampling instant."""

    at_ms: int
    offered_rows: int
    committed_rows: int
    backlog_rows: int
    offered_rate: float | None
    committed_rate: float | None
    # Diagnostic history only; the gate reads the current producer window from
    # summary.json. Late publish-log uploads can amend entries here.
    producer_windows: list[ProducerWindow] = field(default_factory=list)


def make_sample(at_ms: int, offered_rows: int, committed_rows: int, previous: KeepupSample | None) -> KeepupSample:
    """Calculate rates since the previous sample.

    The first sample, or a nonpositive interval, has unknown rates.
    """
    gap_ms = at_ms - previous.at_ms if previous is not None else 0
    if previous is not None and gap_ms > 0:
        seconds = gap_ms / 1000
        offered_rate: float | None = (offered_rows - previous.offered_rows) / seconds
        committed_rate: float | None = (committed_rows - previous.committed_rows) / seconds
    else:
        offered_rate, committed_rate = None, None
    return KeepupSample(
        at_ms=at_ms,
        offered_rows=offered_rows,
        committed_rows=committed_rows,
        # Clamp backlog at zero; exactness reports duplicate rows separately.
        backlog_rows=max(offered_rows - committed_rows, 0),
        offered_rate=offered_rate,
        committed_rate=committed_rate,
    )


def keepup_summary(
    samples: list[KeepupSample],
    offer_end_ms: int | None,
    drained_ms: int | None,
) -> dict[str, object]:
    """Summarize drain time and backlog from scorer observations."""
    backlogs = [sample.backlog_rows for sample in samples]
    return {
        "drain_s": (drained_ms - offer_end_ms) / 1000 if drained_ms is not None and offer_end_ms is not None else None,
        "backlog_rows_max": max(backlogs, default=None),
        "backlog_rows_p50": float(np.percentile(np.array(backlogs, dtype=np.float64), 50)) if backlogs else None,
    }
