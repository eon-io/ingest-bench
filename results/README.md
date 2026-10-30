# Publishing a result

**On this page:** [Tracks, classes and tiers](#tracks-classes-and-tiers) · [Publish](#from-run-spec-to-pull-request) · [Rules](#publication-rules) · [Read results](#how-to-read-the-table) · [Review](#reviewer-checklist)

## Tracks, classes and tiers

Each sustainable section of `RESULTS.md` is a **track**, defined under
`workloads/tracks/`. A track fixes the corpus, the Kafka workload and codec, the
producer-bound threshold, the freshness bound, the exactness SLA, the table
partitioning and a warmup ceiling. Rows compare only within a track. A valid row passes only
when its measured freshness meets its track's bounds, whatever bounds it was scored under, so
tightening a track's freshness bounds keeps every row that measured within them and lists the
rest as not passing. Any other
change to a shipped track would change what its rows mean, so add a new track instead.
The upsert track differs from the append tracks in key reuse, Kafka placement, starting
table and allowed collapse, so its rows are not an upsert overhead on the append rows.

A row's **class** names the rewriting work its fleet did, derived from `compaction.driver`:

| Class | Work performed |
|---|---|
| `ingest-only` | Kafka-to-Iceberg ingest with no compaction layer |
| `integrated-compaction` | an engine that compacts as it writes (`inline`) |
| `spark-compaction` | ingest with the harness's Spark rewrite sidecar |
| `flink-maintenance` | ingest with the harness's Flink table-maintenance sidecar |
| `external-compaction` | ingest with a compactor the operator starts |

A row's **tier** says how far its configuration moved from the shipped settings.
A `reference` run sets its managed engine's knobs to the engine's shipped
`REFERENCE_KNOBS` and changes only the sizing knobs listed in `SIZING_KNOBS`
(see the [Flink](../engines/flink/README.md) and [Spark](../engines/spark/README.md)
guides). It sets no table properties and no compaction layer. Every other run is
`tuned`. An external engine declares `tier: reference` in its spec when it ran
its defaults and changed only resources. Reviewers check that claim.

On an upsert track, a reference run's table must match its engine's `REFERENCE_UPSERT_TABLE`:

| Engine | Format version | Table properties |
|---|---|---|
| Flink | 3 | zstd level 1 only (the job converts its upsert sink's equality deletes into deletion vectors) |
| Spark | 3 | zstd level 1, and `write.merge.mode`, `write.update.mode` and `write.delete.mode` set to `merge-on-read` (deletion vectors) |

zstd level 1 is `write.parquet.compression-codec: zstd` with `write.parquet.compression-level: "1"`.
A harness-created table names no codec, so without them Iceberg's Java writers fall back to gzip.

The summary tables show the smallest valid fleet per track, engine, class and tier.
"Smallest" means cheapest under the common pricing rule, and the tables show its
compute rather than its cost. That fleet is the smallest found so far, not a proven minimum. Anyone can publish
a smaller passing fleet for the same engine, class and tier. It replaces the larger
result, which leaves this directory and stays in Git history. Notes record the
smaller fleets that failed. `--variant <name>` labels configurations that share
an engine, class and tier.

The README selects fleets whose measured freshness is p95 ≤ 90 s and max ≤ 120 s.
The full results retain each run's recorded track bounds.

### Resource and cost interpretation

`RESULTS.md` and `result.json` use the launched fleet's resource requests and
charge every declared role under the common pricing rule. These are the
published comparison figures, not whole-node cloud bills. When a role
needs a node to itself, for example for its network bandwidth, the notes must
say so, because requested resources do not charge the rest of that node.

The fleet and its memory are what the run requested. Notes report measured
usage against those requests and do not restate the fleet at other sizes.

### Compaction evidence

`run_valid` establishes the ingest result, including exactness and the selected mode's requirements;
it does not establish compaction keep-up. The retained compaction results have
`campaign.campaign_pass: null`: their recorded checks do not produce an aggregate
keep-up verdict. Inspect the eligible-backlog series in `campaign.grid` within
the offered-load interval identified by `campaign.windows`. Post-offer draining
cannot demonstrate capacity under continuing load.

Compaction documents distinguish `rewrite_share_of_writes` (the fraction of
written bytes due to rewrites) from `written_per_ingested_byte` (total bytes
written divided by ingested bytes). Notes retain file geometry, backlog behavior
and the limits of each comparison rather than treating one summary as a verdict.

## From run spec to pull request

[Submitting a result](../docs/submitting.md) walks through the whole path. In short:

1. Commit a spec under `specs/tracks/<track>/` that names the track, with real
   machine types and the [common pricing rule](../docs/methodology.md#cost).
2. Run it from a clean checkout's images (see [Running](../docs/running.md)), then
   `scripts/finish.sh <run_id> --publish results/`.
3. Add adjacent `README.md` notes (required for external engines), including
   reproducible setup, the broker shape and smaller failed probes.
4. Regenerate with `results-table results/ --out results/RESULTS.md`, run
   `uv run python scripts/validate-results.py`, and open a pull request with the
   spec, JSON, notes and table.

## Publication rules

1. **Only valid runs are headline results.** Publication requires `run_valid: true`.
   Compaction runs also require `terminal_exactness.verdict: true`; managed
   compactors require `compactor_status.healthy: true`. Missing or failed
   readiness evidence blocks normal publication even when ingestion passed.
   Use `--publish-invalid` to retain such a result under **Did not validate**,
   with its failure state and reason. See [compaction readiness](../docs/results-format.md#compaction-and-readiness).

2. **Conform to a track.** A sustainable result must match a shipped track in every
   setting the track fixes, and score at most the track's warmup. A spec that names
   a track conforms by construction.

3. **Publish the smallest fleet that passed.** Grow the fleet while the gate
   reports `UNDERSIZED` and publish the smallest full-duration fleet that passed.
   Document smaller failed fleets in the run notes, distinguishing short screens
   from full-duration runs. `--publish-invalid` does not bypass the corpus-duration
   requirement.

4. **State the tier honestly.** Managed tiers are derived. An external engine
   claims `reference` only when it ran its own defaults.

5. **Record a clean harness commit.** Results record `run.harness_commit`, the full
   commit the harness images were built from. Build them with
   `scripts/push-images.sh` from a clean checkout. Images built with
   `--allow-dirty` cannot publish. Results collected before this rule (schema
   version 3) have no commit.

6. **Use one pricing rule across compared runs.** Follow the split in
   [Methodology, Cost](../docs/methodology.md#cost). Undisclosed prices render
   `n/a`, never zero. Price each machine type from its hourly price and capacity;
   publication requires positive prices. Explicit legacy flat rates remain supported.

7. **Disclose real machine types and positive fleet counts.** The validator
   rejects empty types, `unspecified`, and `YOUR_` placeholders.

8. **Commit the spec.** The spec that produced the result belongs under `specs/`
   in the same PR, and the document embeds a copy. The validator requires a
   committed spec that matches the embedded copy, apart from names and node placement.
   Node selectors and tolerations are placeholders for each cluster's nodes.

9. **Kafka must not be the bottleneck.** Brokers need not match across rows.
   A `producer_bound` run cannot establish engine capacity, and notes state the
   broker count, shape and storage. See [sizing brokers](../deploy/k8s/stack/README.md#sizing-brokers).

10. **External engines ship notes.** A `README.md` beside the JSON must describe
    setup and observations in normal prose. The validator requires this file
    because the harness did not drive the engine.

11. **The path matches the document.** The validator derives the engine, preset,
    date and time directories from the JSON and requires the filename `result.json`.

12. **Corrections are explicit.** JSON and `RESULTS.md` are generated; never hand
    edit them. Rerun `collect` to correct a result, then regenerate the table.
    Delete a withdrawn result in its own commit with a message explaining why.

## File layout

```text
results/<engine>/<preset>/<YYYY-MM-DD>/<HH-MM-SSZ>/result.json
results/<engine>/<preset>/<YYYY-MM-DD>/<HH-MM-SSZ>/README.md
```

The standard corpus uses `events-1mrps-uniform` and `events-1mrps-skew` paths.
Retained runs record their original `events200-1mrps-*` names and hashes in JSON;
publication resolves those labels to the renamed tracks without altering measurements.
The rename preserves corpus hashes. Reuse the existing frozen corpus without regenerating it.

Paths use the UTC staging timestamp from `run.run_id`. Recollection overwrites
the same run's JSON and variant label, preserves notes, and regenerates the table.
A different run at the same engine/preset/timestamp is a collision: use a
separate results root or stage a new run. Do not edit IDs or timestamps.

`collect --run-dir runs/<run_id> --site site.yaml --publish results/` saves
local `run.json` before publication checks. A failed check or collision retains
that local report. `--out results/` derives the same path but skips publication
checks; without either option, only local `run.json` is written.
See the [document format](../docs/results-format.md).

JSON and `RESULTS.md` are generated; operators write notes. Include engine
configuration and the resolved Kafka tuple from `run.kafka` (partitions, key,
assignment, encoding and compression).

## How to read the table

`RESULTS.md` opens with what a figure is and what keeping up means, then two summary tables:

- **Ingest only** and **With compaction** put engines against tracks. Each cell is the
  smallest valid fleet's requested compute (vCPU and GiB), linked to its notes.
- Rows sort by mean cost per offered TB over the tracks they have results on, cheapest first.
- A cell marked `*` has no valid ingest-only run on that track, at any tier. It shows the
  ingest roles of the engine's smallest run with a compaction sidecar, without the sidecar's
  own roles.
- Compaction has no keep-up verdict yet, so read each run's
  [compaction evidence](#compaction-evidence) before comparing those rows.

Each track then has its own section:

- The heading names the offered rate and key distribution. Sections that would share a
  heading also name their corpus or exactness SLA.
- **Workload details** (collapsed) lists the track, replay, Kafka settings, keep-up criteria
  and Iceberg partitioning the section's rows share.
- The table shows the cheapest valid run per engine, mode, tier and variant, cheapest first.
  Columns are mode, fleet (one role per line), requested compute, freshness p50/p95, median
  file size and the share of files below 32 MiB. A Tier column appears when a row is at the
  `reference` tier. A variant name appears beside the mode when it tells rows apart.
- **Other qualifying runs** (collapsed) holds every other valid run, with its version and
  staging date.
- **Did not validate** lists runs that failed, cheapest first. States include
  `producer_bound`, `void`, `undersized` (idle stop), `drained` (another check failed),
  `not drained` (other unfinished runs) and `over-bound` (valid, but measured freshness
  exceeds the track's bounds). Reasons come from the scorer. Producer-bound runs
  cannot establish engine capacity.
- **Superseded runs** (collapsed) lists older runs of the same engine, mode, tier, variant
  and fleet, newest first. Fleet identity includes role, count, vCPU, GiB and machine type,
  independent of role order. Version changes do not prevent supersession.

Missing values are `n/a`. Managed versions are harness-build image tags, and external
versions are operator declarations. Dates use staging time, not `collected_at`. Saturation
uses its [own grouping and diagnostics](../docs/saturation.md). Apply the
[methodology caveats](../docs/methodology.md) to every comparison.

Exactness faults remain in `data.exactness` and `derived.exactness`:
`duplicate_rows`, `duplicate_ppm` and strict `exact`. SLA `verdict` can pass with
`exact: false` in `AT_LEAST_ONCE` mode when duplicates are the only fault.
Drain and backlog remain in `derived.keepup`. Cost per hour stays in `derived.cost`.

The README's Results section repeats the intro and both summary tables between
`<!-- results:start -->` and `<!-- results:end -->`. `results-table` refreshes that block
and `validate-results.py` checks it.

`collect --publish` and CI share the document checks below. After publishing,
run `uv run python scripts/validate-results.py` to also check notes, committed
specs, resource reuse and the generated table.

## Automated checks

CI runs `scripts/validate-results.py` over this directory. It checks:

- Schema version 4 (or legacy 3) and the fields needed to render each document.
- A clean, full `run.harness_commit` on schema version 4.
- Conformance of every sustainable result to a shipped track.
- A committed spec under `specs/` that matches the embedded spec, apart from names and node placement.
- No private buckets in JSON strings or Markdown, in `s3://`, `gs://` or S3 and
  GCS HTTPS URLs. A bucket may be `<bucket>`, `$BUCKET`, `${BUCKET}`,
  `${env:BUCKET}` or `YOUR_BUCKET`, and a placeholder with a literal suffix is rejected.
- No AWS account IDs where the text identifies an account: an ARN, an AWS
  endpoint host, an `AWSLogs/` path, a phrase such as "account", or a field
  such as `account_id` or `glue.id`. A bare 12-digit number is unclassified and
  passes. The check shares the repository scanner's rule and allows the
  documentation ID `123456789012`.
- A shipped preset, matching corpus hashes and non-null `run.corpus` metadata
  whose schema, rates and duration match that preset, plus the current corpus
  generator version, seed and positive row-block size.
- Sustainable mode requires a full offer (`spec.producer.seconds` must be absent,
  null, or at least `run.corpus.duration_s`). Saturation permits a shorter offer
  when the declared measurement window fits, with matching scorer mode and timing.
- A nonempty fleet with real machine types and positive counts, vCPU and GiB values.
- Nonempty external engine names and versions, and a collection timestamp with a UTC offset.
- Positive prices for both resources and an available hourly cost.
- The scorer summary, derived keep-up measures and producer-bound verdict.
- The configured exactness mode and every recorded mode, verdict and fault count agree.
- Unique table and topic names across results.
- Required external-engine notes and the canonical result path.
- `RESULTS.md` matching a fresh, deterministic render, including notes links.

## Reviewer checklist

- The spec is committed under `specs/tracks/<track>/` and matches the embedded configuration.
- The headline fleet is the smallest published fleet that passed. Notes explain
  evidence from smaller failed fleets and identify untested configurations.
- A `reference` claim by an external engine is backed by notes showing default settings.
- Compared runs use one pricing rule, and notes disclose roles that need a node to themselves.
- Notes state the broker shape, and the run was not `producer_bound`.
- External disclosures include engine name, version and reproducible setup notes.
- Clock-skew flags, missing artifacts and `SLOW_POLL` messages were investigated;
  `run_valid` alone does not establish a reliable writer clock.
- Result documents and notes contain no private company, person, cluster,
  node-pool or taint names; automated scans cannot identify every private name.
- Corrections were regenerated and withdrawals explain their reason in a commit.
