#!/usr/bin/env python3
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Report campaign compaction, implemented in ingest_bench.scorer.cli."""

from __future__ import annotations

import sys

from ingest_bench.scorer.cli import campaign_compaction

if __name__ == "__main__":
    raise SystemExit(campaign_compaction(sys.argv[1:]))
