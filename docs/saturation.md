# Saturation mode

Use saturation mode to compare committed Kafka-to-Iceberg throughput at a fixed
compute budget. Offer more than the fleet is expected to process, measure a
fixed interval, then let the full offer drain and verify exactness.
The existing benchmark remains the default `sustainable` mode.

**On this page:** [Run configuration](#configuration-and-execution) · [Comparison setup](#fixed-compute-and-workload) · [Metrics](#throughput-and-cadence) · [Validity](#what-a-valid-result-establishes)

## Configuration and execution

Keep your engine, fleet, corpus, Kafka and table settings. Add:

```yaml
producer:
  seconds: 30
  speed: 0.025
  shards: 2
scoring:
  mode: saturation
  warmup_s: 120
  measurement_s: 900
  exactness: EXACTLY_ONCE
  freshness_bound_s: 180
```

This measures seconds 120 through 1020 after the epoch. `producer.seconds` is
corpus time; divide it by `producer.speed` to get replay time. The selected
replay must extend beyond the measurement window. Staging refuses a window
that reaches the last scheduled batch, before creating topics or tables. The
producer still finishes its finite offer; the scorer then waits for drain.

Use the same stage, launch, gate, teardown and finish commands as other runs.
[`specs/saturation/saturation-flink.yaml`](../specs/saturation/saturation-flink.yaml) is an example
configuration, not a measured capacity claim. It replays 30 corpus seconds over
1200 wall seconds, offering about 34.4 MB/s from the standard corpus with two
producer shards. Probe the fleet before choosing the excess offer. With a prepared site and corpus:

```bash
RUN_MAX_S=7200 IDLE_STOP_S=600 scripts/run.sh specs/saturation/saturation-flink.yaml \
  --site site.yaml --image-tag YOUR_IMAGE_TAG
```

Build a harness image containing this mode before launching. For local
integration checks, `scripts/smoke.sh --spec` also forwards the mode and window;
use a spec naming the `smoke` corpus and a window that fits its duration.
A shared local machine does not establish engine performance.

## Fixed compute and workload

Fix total engine CPU and memory, hardware class, replica counts and resource
limits across arms. Include coordinators as well as workers; disable
autoscaling. The harness reuses the declared fleet and, for managed cluster
engines, admitted pod resource requests. This is a resource snapshot, not
continuous verification that the fleet remained fixed.

Keep corpus, Kafka partitions, compression, partitioning and exactness mode
comparable. Disclose commit/checkpoint settings, file targets and compaction.
A throughput improvement bought by longer commit pauses is visible in cadence
and freshness, but is not an improvement at the same freshness requirement.

- **Offer and storage:** offer slightly above expected capacity without overwhelming
  Kafka or the producer. The example offers about 41.3 GB before compression
  (1376 MB/s × 30 corpus seconds). Check retention and disk capacity for the offer plus drain.
- **Idle timeout:** set `IDLE_STOP_S` above the longest expected commit pause.
- **Run timeout:** set `RUN_MAX_S` above the remaining epoch lead plus replay and
  drain, allowing for startup and final scoring. The example's 7200 s limit is
  a starting budget, not a drain guarantee. Use a capacity probe to reduce the
  offer or adjust the budget. A timeout cannot produce a valid result.

Estimate drain time when the offer exceeds output, using rates in the same units:

```text
drain_s ≈ replay_s × (offer_rate / committed_rate − 1)
```

For example, **if a probe measures 25 MB/s**, this offer needs about 451 s of
drain: `1200 × (34.4/25 − 1)`. This is a sizing illustration, not a measured
rate for the example fleet. Recalculate after the probe and check that the
measurement window ends before the last scheduled batch.

> **Cloud cost:** Kafka replication, Iceberg output, scorer reads, network transfer
> and idle infrastructure add to source storage costs. Cost depends on the site,
> not engine CPU alone. A smaller excess offer limits drain and storage costs;
> longer timeouts increase cost.

## Throughput and cadence

`summary.json` records `scoring_mode`, `measurement_s` and a final `saturation`
object. Raw `snapshots.jsonl` records cumulative `committed_rows` after each
snapshot. Only appends contribute new work; compaction rewrites do not.

The headline is the cumulative useful-row delta over `(start, end]`, divided
by the **entire declared duration**. Commits at the start are baseline;
commits at the end are included. Startup and subsequent drain are excluded.
Pauses inside the window remain in the denominator. Under `AT_LEAST_ONCE`,
useful rows are distinct expected IDs. Under `EXACTLY_ONCE`, counts are accepted
only if the final count/checksum exactness checks pass.

### Output rate

- `rows_per_s` and `committed_rows` on Iceberg commit timestamps.
- `source_mb_per_s_estimate`: rows/s times average encoded corpus bytes per
  offered row, in decimal MB/s. This excludes message framing and compression
  and is an estimate for the measured rows, not Parquet or network throughput.

### Commit boundaries

- `commit_to_commit`: first through last nonempty append inside the window,
  with `start_ms`, `end_ms`, `duration_s`, `committed_rows`, `rows_per_s` and
  `append_commits`. The first timestamp supplies the cumulative baseline;
  its rows, including all appends tied to it, are excluded. The count includes
  baseline appends. Fewer than two distinct timestamps yields `null`.
- `start_edge_gap_s` and `end_edge_gap_s`: time from the window start to the
  first nonempty append, and from the last nonempty append to the window end.
  Both are `null` when there are no nonempty appends.

### Half-window consistency

- `first_half_rows_per_s` and `second_half_rows_per_s`: fixed-window rates for
  each half. Commit phase can produce a difference even at constant processing
  speed; these figures alone do not establish drift.
- `first_half_commit_to_commit` and `second_half_commit_to_commit`: the same
  commit-aligned measurement within each half, with their own spans. The
  midpoint belongs to the first half. A half with fewer than two distinct
  nonempty commit timestamps yields `null`.

### Observation clock

- `first_seen_rows_per_s` on the scorer's visibility clock. Slow polling can
  group commits or shift them across window boundaries; compare both clocks.
- `first_seen_rate_difference_pct`: `100 × (first_seen_rows_per_s / rows_per_s
  − 1)`, or `null` when `rows_per_s` is zero. Negative values mean the scorer
  observed fewer rows within the window. This is an observation-boundary
  diagnostic, not a throughput correction or validity threshold.

### Cadence and backlog

- `append_commits`, `rows_per_commit`, and `commit_gap_s` p50/p95/max.
  Gaps use consecutive nonempty appends inside the window.
- `max_silence_s`, including pauses from the start to the first append and from
  the last append to the end. These edge intervals are censored observations.
- `backlog_min_rows` and `backlog_growth_rows_per_s` over the same window.

### Interpreting the metrics

Commit batching makes short fixed-window rates sensitive to boundary placement.
The commit-to-commit rate removes partial edge intervals but also excludes any
unfinished stall at those edges. Pauses between its selected commits remain in
the denominator. Compare both rates and their spans; a shorter retained span
can change the ranking of runs. Neither rate reconstructs when individual rows
were processed, and the harness does not interpolate between commits.

Freshness curves, exactness, drain time and geometry remain available as before.
Freshness bounds are diagnostic in saturation mode. The existing freshness
window includes drain; consult the lag series for the throughput interval.
Results tables separate modes and compute budgets and show throughput and
cadence for saturation, alongside the commit-to-commit rate and span, edge gaps,
and clock difference. Older results without these diagnostics show `n/a`.
Tables omit offered-rate `$/TB` for saturation runs.

## What a valid result establishes

A result requires normal drain, schema validity, the configured exactness SLA,
an on-schedule producer, and a complete saturation measurement. The measurement
requires at least three nonempty appends and a strictly positive output backlog
at the start and after every event through the window end. Net backlog growth
must exceed the largest measured append, so a constant pipeline backlog or a
single commit batching swing does not establish overload. Offered totals use
fully acknowledged batches, so an incompletely acknowledged batch cannot
supply backlog. At a commit, acknowledgements tied to its millisecond timestamp
are excluded because their ordering is unknown. Nonmonotonic commit clocks,
commits carrying rows before the epoch, or timestamps later than first
observation invalidate the measurement. Empty startup commits are allowed.

The live gate still rejects producer failures and missing, stale or aborted
measurements. It ignores excessive lag and growing backlog in saturation mode.
Its `PASS` means the measurement remains live, not that the final result is valid.
Insufficient backlog or too few commits produces an invalid final measurement
with diagnostic figures retained; it does not silently switch modes.

Positive output backlog can reside inside the engine. It does not prove CPU
saturation or that every Kafka reader always had work. Use independent CPU,
Kafka-lag and engine metrics to identify the bottleneck. Three commits are a
minimum evidence check, not proof of steady state or comparison precision.
Choose windows covering many commit cycles and inspect both edge gaps and
commit-aligned spans. Longer windows reduce relative boundary sensitivity but
do not guarantee steady state; a fixed commit count cannot certify a few-percent
A/B effect. Inspect both half-window estimates with their spans, and repeat
matched runs with alternating arm order to assess variation. Clock differences,
short spans and large edge gaps are diagnostics, not additional validity gates.

> **Report as measured throughput under overload.** It does not establish a
> proven maximum or a sustainable rate meeting a freshness requirement.
