# Methodology

This document defines the benchmark's measurements, validity rules and cost model.

**On this page:** [Terms](#glossary) · [Freshness](#freshness) · [Exactness](#exactness) · [Verdict](#the-verdict) · [Gate](#the-gate) · [Upsert](#upsert-runs) · [Cost](#cost)

## Glossary

- **Batch:** one frozen corpus interval, with width `batch_interval_ms`. Its
  manifest record gives the row count, row-id range and checksum.
- **The offer:** the producer replaying the corpus on its original timeline.
  The **offered rate** is `offered_bytes_per_s` scaled by `producer.speed`.
- **Epoch:** the run's time origin, selected before launching the offer. At normal
  speed, batch *k* is due at `epoch_ms + k * batch_interval_ms`, in milliseconds.
- **Emit time:** the batch's final Kafka acknowledgement (`last_ack_ms`), as
  recorded by the producer's delivery callback; distinct from its due time.
- **Contiguous prefix:** the largest *k* for which every batch `0..k` is covered.
  This is the watermark used to measure freshness.
- **Window vs full:** the freshness window excludes `scoring.warmup_s` after
  the epoch; the full series covers the whole run. The verdict uses the window.
- **PASS / UNDERSIZED / VOID:** in-flight gate verdicts used to stop a failing fleet before the full run ends.
- **Track, class, tier:** a track fixes the workload and scoring a results section
  compares on. A class names the rewriting work a fleet did. A tier says whether a
  run changed only sizing knobs (fleet size, resources and parallelism) from the
  shipped engine settings (`reference`) or tuned more (`tuned`). See [tracks, classes and tiers](../results/README.md#tracks-classes-and-tiers).

## The corpus is the ground truth

A corpus is generated once and reused unchanged. Its manifest records each batch's
row count, ID range and sum of IDs modulo a prime. `EXACTLY_ONCE` compares counts
and checksums; `AT_LEAST_ONCE` tracks distinct IDs within the expected range. The
generator verifies the manifest against encoded bytes before publishing the corpus.

Row IDs encode their batch in their high bits, so the scorer can attribute rows
without a join. It reads only IDs from each data file and checks column
declarations, but does not validate timestamps, keys, payloads or other values.

## Coverage and delivery mode

`scoring.exactness` selects the delivery SLA and how the tally measures coverage:

- **`EXACTLY_ONCE`** (default) uses observed row counts to advance the freshness
  watermark. A batch is complete only when its count and checksum match.
- **`AT_LEAST_ONCE`** tracks every expected row ID in a bitset. Distinct valid IDs
  drive the watermark, committed-row count and backlog, so duplicates cannot mask
  loss. An ID outside its batch's expected range is corruption.

In the default mode, a late duplicate can fail exactness without changing an
earlier freshness observation. Counts only grow, so the prefix advances
incrementally. Equal loss and duplication can leave a count unchanged; the
checksum detects many such substitutions, but does not prove row-set identity.

## Freshness

The scorer samples freshness on a one-second grid, giving stalls the same
weight as periods with frequent commits. At time *t*, lag is *t* minus the
final Kafka acknowledgement of the contiguous-prefix batch, or minus the epoch
before any batch is covered. These are watermark quantiles, not per-row latency.
Commit delay is included; producer delay before acknowledgement is checked separately.

The **window** starts `scoring.warmup_s` after the epoch; the full series retains startup.
Each clock ends at its first observation covering the final offered batch, including that endpoint.
Undrained runs end at scorer shutdown. If warmup exceeds the run, the window uses the final point.
Snapshots within one transaction receive the timestamp of the snapshot made visible by that commit.

Draining is also required: passing lag quantiles alone does not establish that
all offered batches arrived. The freshness verdict is true only when:

1. the prefix reaches the last offered batch;
2. the lag series has no coverage gap;
3. window p95 is at most `scoring.freshness_bound_s`;
4. window max is at most `scoring.freshness_max_bound_s`, which defaults to twice the p95 bound.

A **coverage gap** is a sampled prefix whose batch has no emit time in the publish
logs. Any gap invalidates the affected series' quantiles and fails freshness.
Dropping the sample would hide missing evidence.

### Two clocks, and clock sanity

Lag is published on two time bases: the table's commit timestamps (the default)
and the scorer's wall time when it first observed each commit. The second avoids
dependence on the writer's clock. All snapshots discovered in one metadata read
share an observation timestamp, captured before scanning their data files. The
scorer-clock series includes polling delay and still assumes the producer and
scorer clocks are aligned.

`min_lag_s` is the smallest per-commit lag on the table's clock. A negative value
means a commit timestamp precedes the corresponding producer acknowledgement and
sets `clock_skew_suspected` to true. Treat measurements from those timestamps as
suspect and inspect the scorer-clock series. Backward commit timestamps also set this flag
and invalidate the table-clock verdict; a negative lag alone remains a diagnostic.

## Exactness

Loss, duplication and corruption are measured for batches recorded as sent in
the publish logs. Unsent batches are not counted as lost, but committed rows
outside the corpus or recorded offer fail exactness.

| Fault | `EXACTLY_ONCE` | `AT_LEAST_ONCE` |
|---|---|---|
| `loss_rows` | row-count deficits | missing distinct expected IDs |
| `duplicate_rows` | row-count excesses | repeated expected IDs; unexpected IDs are excluded and treated as corruption |
| `corrupt_batches` | batches with the expected count but a different checksum | batches with unexpected IDs |
| `invalid_rows` | rows whose ID names no corpus batch | the same |
| `unoffered_rows` | rows of batches the publish logs never recorded | the same |

`mode` records the selected SLA. `verdict` says whether the run met it:
`EXACTLY_ONCE` rejects loss, duplication and corruption; `AT_LEAST_ONCE` permits
duplicates but rejects loss and corruption. Both reject invalid and unoffered
rows. `exact` remains strict and is false for any fault, including a permitted
duplicate. Only the count-based mode can let equal loss and duplication cancel
before the checksum check. The violation list is capped; aggregate counts cover
every scored batch.

Scoring stops when the completed offer drains. Later commits are outside exactness. Keep full snapshot ancestry.
The scorer counts appends and new live files in ingestion overwrites, applying position deletes and Puffin vectors.
It checks retained files with changed masks before tallying incoming rows.

`replace` compaction adds no delivery. Overwrites removing files compare per-batch counts/checksums of removed and
added live IDs. Matching tallies add no delivery, freshness or saturation updates, but do not prove row-set identity.
Changed retained rows, mismatched tallies, equality deletes, table replacement, rollback and missing ancestry abort scoring.

Compaction requires distinct-ID verification of the pinned final snapshot with masks applied.
Publication rejects undeclared rewrites. Declare compaction before runs with non-append snapshots, removed rows or replaced files.

## Keep-up

Freshness measures lag throughout the run; catching up does not erase earlier
stalls. The headline table shows window p50, p95 and max lag. `derived.keepup`
retains queued-row and post-offer drain diagnostics.

| Figure | Definition |
|---|---|
| `drain_s` | seconds from the final producer acknowledgement until the scorer first observed the prefix covering the last batch |
| `backlog_rows_max`, `backlog_rows_p50` | backlog from samples at or after the run epoch |

Drain time uses producer acknowledgement times and scorer observation times,
so it assumes those clocks are aligned. Drain includes polling delay but is
independent of writer-clock skew. Use the same `score --poll-interval-s` across
compared runs (default `5` seconds). Negative drain values are retained; inspect
producer/scorer clock alignment if they occur. Live backlog and rate samples still
reflect the publish logs available at each poll.

## Geometry

Geometry describes live files, rows and bytes independently of the verdict:
file-size p50/p90/p99, min and max; shares under 32 MiB and 8 MiB; a log2 size
histogram; and per-commit quantiles of file counts and sizes. It is measured at
each `scoring.geometry_offsets_s` offset and at the final snapshot.

Offsets use table commit timestamps; a point before the first or after the final
commit is `absent`, even if the scorer ran longer. Per-commit statistics cover the
commits since the preceding measured point. Small-file shares count files, not bytes.
Cluster runs read geometry from saved metadata and manifests after teardown;
local smoke tests read the live table before cleanup. Neither uses the scoring loop.

## The verdict

`run_valid` determines whether the scorer considers the run valid. In the default
`sustainable` mode it is true only when:

1. the scoring loop finishes normally;
2. the table has every corpus column with the required type and nullability;
3. freshness passes: drained, no gap, p95 within the bound, max within the max bound;
4. exactness meets the selected SLA: no loss, corruption, invalid or unoffered rows,
   and no duplication when `scoring.exactness` is `EXACTLY_ONCE`;
5. the producer meets its schedule.

Running and abandoned runs are invalid even if their current measurements pass.
Publication also requires the [results checks](../results/README.md).
Keep-up and geometry have no final pass threshold in sustainable mode.
[Saturation mode](saturation.md) replaces freshness limits with throughput-window checks.

### Failure reasons and states

`reason` is null for valid or running results. Schema violations name the
mismatched columns; abandoned runs report `idle_stop_before_drain`; exceptions
report `scorer_failed:` and the exception type. Other failures use the first
applicable category: `producer_bound:`, `exactness:`, then `freshness:`, with
fault totals or the violated bound.

`state` records the scoring loop's status:

| `state` | Meaning |
|---|---|
| `running` | measurements are provisional |
| `drained` | the prefix reached the last offered batch; exactness may still fail |
| `idle_stop` | committed rows stopped advancing while rows remained outstanding: the engine stopped, never consumed, or exceeded the scorer's idle timeout |
| `producer_bound` | the producer failed to deliver the scheduled offer |
| `void` | table columns differ from the corpus schema. `reason` names missing columns, wrong types or nullability that differs from the corpus |
| `failed` | the scorer stopped on an error; `reason` names the exception type |

In sustainable mode, a drained, exact run that exceeds the freshness bound
indicates insufficient capacity for the offer rather than a correctness failure.

Schema violations take precedence over producer failures.

### Producer schedule

`producer_bound` is true when an offered window's maximum first-ack delay
exceeds `producer.behind_max_ms`, or an offered delivery fails. The scorer groups
per-shard median and maximum `behind_ms` in `keepup_samples.jsonl` by scheduled
time into `scoring.gate_window_s` windows. Delayed logs amend earlier windows; use the
latest entry per window and shard. The gate prints the latest closed window's
median of shard medians, maximum and reporting-shard count. A late batch remains
a run-level fault after the gate advances.

Each shard writes a completion marker after its final acknowledged batch.
Errored producers omit it. A shard killed mid-batch remains unfinished at its
last completed batch; this alone does not prove a producer bottleneck.
Completion delay within a batch has no separate threshold. Teardown waits for
producers before deleting the topic to avoid artificial delivery errors.

A producer-bound run cannot establish engine capacity. Check producer CPU and
shard count; lowering `producer.speed` reduces replay rate, while corpus
`--set` overrides change the workload.

## The gate

The gate reuses scorer artifacts and returns `PASS`, `UNDERSIZED` or `VOID`
(exit codes 0, 3 and 5). Stale, missing or aborted measurements cannot establish
capacity. In sustainable mode it checks:

- **Lag:** after `scoring.gate_adaptation_s`, lag must not exceed the max bound.
- **Window p95:** the scorer counts the window's seconds with lag over the bound. Once
  that count alone puts the planned window's p95 over the bound (166 s for an hour with a
  300 s warm-up), the run cannot pass. The planned window ends with the corpus offer, so
  a long drain allows slightly more over-bound seconds than the gate assumes.
- **Backlog floor:** at least three populated windows must have strictly increasing,
  positive minimum backlogs, with growth exceeding a typical observed commit size.

During adaptation, observed backlog decreases widen the effective floor window
to cover commit gaps, capped at three times `scoring.gate_window_s`. The window then
freezes. The allowance is the continually updated median increase in `committed_rows`
between samples whose backlog decreases, excluding the first decrease to avoid
startup drain. The check looks farther back through consecutive increasing floors
to detect smaller deficits; plateaus, decreases or missing windows break that evidence.

Set `gate_window_s` to cover the expected commit period. If commit gaps outgrow
the frozen window, floors can zig-zag despite a deficit; only the independent
lag limit may catch it. Commit jitter can also cause isolated `UNDERSIZED`
ticks. Inspect the series: detection timing depends on polling, cadence and
window settings, and is not a latency guarantee. Short probes can miss small deficits.

Three consecutive non-`PASS` ticks trigger `gate.sh --teardown` by default;
`--breaches` overrides this. `UNDERSIZED` means a capacity check failed; `VOID`
means measurements cannot support judgment. A live `PASS` does not establish
final `run_valid`. Saturation mode keeps liveness and producer checks but treats
lag and backlog as diagnostics.

## Upsert runs

An [upsert corpus](corpus.md#upsert-corpora) is scored with `scoring.exactness: LATEST_PER_KEY`.
After each commit, the table must hold exactly the newest version of every key.
A run starts from the corpus's seed: a registered seed table, or, for a Kafka seed, rows the
engine writes before the epoch. A Kafka seed is outside the scored offer but held to the same
freshness rule: `wait-seed` covers each seed batch once all its keys are in committed data files,
and its lag must meet the bound after warmup, or launch stops. This keeps an engine from buffering
the seed into a layout it could not build at the bound. A seed producer that falls behind its
schedule past `producer.behind_max_ms` voids the check instead. The result is `seed_freshness.json`.

- **Coverage.** The scorer reads row IDs from `append` and `overwrite` commits and keeps,
  for each key, the newest version any committed file held. A batch is covered once every
  key in it has a version at least that new, so engines may skip versions a newer one
  replaces. Freshness, drain, keep-up and the gate use this prefix.
- **Exactness.** Loss is the rows of offered batches never covered. Re-added rows are not
  duplicates, because rewrites legitimately copy them. Invalid and unoffered IDs still fail.
- **Live state.** The scorer saves each key's version at every `geometry_offsets_s` offset
  and at drain. After teardown, `verify-upsert` reads those snapshots' live rows. Each
  created key must have one row, at the saved version, with the key columns its ID derives.
  The final snapshot must also hold the corpus's live row count and checksum, computed
  over the offered batches when `producer.seconds` shortens the offer.
- **Limit.** This is sampled validation: a table wrong between samples and right at each
  one passes. Upsert tracks pin non-round offsets, so a periodic engine's cleanup cannot
  line up with every sample by chance.
- **Diagnostics.** `terminal-upsert.json` reports delete files by type, write amplification
  and scan time per sample. `table-health.json` reads the final table's manifests before any
  scan, so a table too costly to read still gets figures: delete files by type, data-file rows
  per live row, and equality-delete fan-out (delete files and rows applying to each data file).
  Results tables show delete files and rows per live row. No diagnostic changes the verdict.

## Cost

Resource-based cost applies only to Kubernetes runs. Local runs (sites without
`kubernetes`) report hourly and total dollar costs as `null`, and results tables
order them after priced runs by their compute. Run duration remains available when timestamps are recorded.

Hourly cost is `Σ count × (vcpu × vcpu_hour_usd + gib × gib_hour_usd)` across the
fleet. Multiply by run hours for total cost. Managed fleets use admitted container
requests captured in `engine-pods.json` after staging verifies the running fleet;
external engines use their declared fleet. Missing pod evidence leaves managed
cost unavailable. This accounts for Spark memory overhead and configuration
overrides, independent of how the cluster packs pods onto nodes. The snapshot
assumes fixed replica counts and resource requests throughout the run. Capture
supports ordinary containers; init containers and pod-level resource settings
require additional accounting and are rejected. Run hours extend from the epoch
to the later of the producer's last acknowledgement and the table's
last commit, including drain time.

> **Cost scope:** this estimates engine resources. It excludes the producer,
> scorer, Kafka, catalog, storage, network charges, unused node capacity and setup time.

Results pages show requested compute, not dollars. Cost decides which fleet is the
smallest and orders the rows. Summary rows rank by resource cost per offered decimal
terabyte (10¹² bytes):

```text
$/TB = usd_per_hour / (offered_bytes_per_s × speed × 3600 / 10¹²)
```

The offered rate is the corpus rate times replay speed; drain time and stored file
sizes do not enter the denominator. Compare `$/TB` only within sustainable
sections, which fix corpus, speed and freshness bound.

Use one pricing rule and price basis (on-demand or spot) across compared runs.
For each role, split its machine's hourly price `P` equally between CPU and memory:
`count × P/2 × (requested_vcpu/node_vcpu + requested_gib/node_gib)`.
Requesting the whole node costs `P` per hour, and half of both resources costs `P/2`.
Configure [machine prices](run-spec.md#site-config) offline. The map must cover every fleet type.
Legacy flat rates remain supported: `vcpu_hour_usd = P/vcpu/2`, `gib_hour_usd = P/gib/2`.
