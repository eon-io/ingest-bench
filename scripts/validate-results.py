#!/usr/bin/env python3
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Run publication checks implemented in ingest_bench.collect.validate."""

from __future__ import annotations

import sys

from ingest_bench.collect.validate import main

if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
