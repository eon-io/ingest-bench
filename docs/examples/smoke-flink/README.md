# Recorded local Flink smoke

This smoke ran on 2026-09-09 using `scripts/smoke.sh`: the full 300-second,
5 MB/s corpus against stock Flink, on an Apple M5 Pro with OrbStack emulating
amd64 for the engine image.

The verdict was `run_valid: true`:

- Drained at batch 299; the count and checksum checks passed for all
  5,840,896 offered rows.
- Freshness p95 was 17.6 s, within the spec's 60 s bound.
- The table drained 4.87 s after the offer ended.

`summary.json` and `freshness.json` are the recorded scorer artifacts. This
local smoke verifies operation; it is not a publishable capacity result. See the
[repository README](../../../README.md).
