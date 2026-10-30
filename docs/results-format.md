# Result document reference

**File:** `run.json` · **Schema version:** `3`

`collect` combines a run's measurements and configuration into redacted JSON.
`results-table` renders published documents from `results/`.

```bash
collect --run-dir runs/<run_id> --site site.yaml
collect --run-dir runs/<run_id> --site site.yaml --out results/
```

By default, `collect` writes `run.json` in the run directory. `--out` takes an
exact path, or a directory ending in `/` for the [canonical result path](../results/README.md#file-layout).
Recollection updates the same run's file; handwritten `README.md` notes live beside it.

| Option | Behavior |
|---|---|
| `--out PATH` | **Skips publication checks**, even under `results/`. A directory path ending in `/` also saves local `run.json` first |
| `--publish DIR` | Saves the local report and publishes an identical copy after document checks pass |
| `--publish-invalid` | Allows `--publish` to retain failed runs |

A collision or refused publication retains the local report; a refusal prints its path.
Installed collectors can use `--workloads DIR` for presets. Validate notes and
the generated table with `uv run python scripts/validate-results.py`.

**On this page:** [Redaction](#redaction) · [Fields](#top-level) · [Derived metrics](#derived) · [Compaction](#compaction-and-readiness)

## Redaction

The site's private values are replaced wherever they appear in a string, including mapping keys and notes:

| Site value | Replacement |
|---|---|
| `corpus_root`, `runs_root`, `warehouse`, `kubernetes.registry` | `<corpus_root>`, `<runs_root>`, `<warehouse>`, `<registry>` |
| `catalog.props.warehouse`, when distinct from those roots | `<catalog_warehouse>`. A REST catalog's warehouse name, which is neither a location nor an account ID, is replaced only in `run.catalog_props` |
| any 12-digit account ID in the site's settings, such as `glue.id` or a registry host | `<account_id>` |
| credentials in a URI, as in `https://user:secret@host` | `https://<redacted>@host` |

The longest root wins. Name boundaries keep `corpus-archive` from being treated as a child of `corpus`.

Catalog properties with `token`, `credential`, `secret`, `password`,
`auth_user_info` or `authorization` in their names become `"<redacted>"`.
Environment references such as `${env:NAME}` remain intact so readers know
which variables to supply. `site.kafka.security` is omitted entirely. The
catalog `uri` and prices remain available for comparison.

Publication scans allow only complete authority placeholders: `<bucket>`,
`$BUCKET`, `${BUCKET}`, `${env:BUCKET}` or `YOUR_BUCKET`. A placeholder with a literal suffix is rejected.

## Top level

| Field | Description |
|---|---|
| `schema_version` | `4`, which adds `run.harness_commit`. `3` still parses. A missing field and an explicit `null` have different meanings |
| `collected_at` | when `collect` ran, `YYYY-MM-DDTHH:MM:SSZ` |
| `harness_version` | the installed `lakehouse-ingest-bench` version |
| `run` | run identity and configuration |
| `artifacts` | input paths, snapshot records and publish-log summaries |
| `data` | the scorer's documents, with redaction |
| `derived` | comparison metrics derived from recorded artifacts |
| `geometry` | `geometry.json` as `file-sizes` wrote it, or `null` |
| `compaction` | `compaction.json` as `compaction` wrote it, or `null`. Absent from results collected before the compaction layer existed |
| `campaign` | the selected campaign compaction report, or `null`; present only when the spec declares compaction |
| `campaign_source`, `campaign_available` | the selected campaign directory and every available campaign directory; present only when the spec declares compaction |
| `terminal_exactness` | final live-snapshot row-ID validation, including `verdict`, snapshot ID, offer completeness and loss/duplicate counts; `null` when absent |
| `terminal_upsert` | upsert runs: live-state checks at each sample snapshot and the final one (`faults`, `files`, `scan_s`), the `live_checksum` comparison and `write_amplification`. Replaces `terminal_exactness`. `null` when absent |
| `table_health` | upsert runs: `table-health.json`, the final table's delete files by type, `live_rows`, `dead_rows`, `rows_per_live_row` and `equality_fanout`, all from metadata. `null` when absent |
| `compactor_status` | managed sidecar status captured before shutdown, including `healthy`, pod/container states and restarts; `null` when absent |
| `missing` | absent optional inputs |

Schema version describes document compatibility, not publication eligibility.
Publication also requires the identity and workload fields checked by the validator.

## `run`

| Field | Source |
|---|---|
| `run_id`, `table`, `topic`, `harness_commit` | `facts.json`. The commit carries a `-dirty` suffix when the images or checkout differed from it, and is `null` when staging could not read one |
| `variant` | Configuration label from `--variant`; `default` when omitted, for every engine. This label does not change engine settings. |
| `spec` | verbatim `spec.yaml`, with a named track's settings filled in. Omitted defaults remain omitted |
| `engine` | `spec.engine` |
| `engine_versions` | `{image, digest}` from `engine-image.json` for a managed engine, or `null` if absent; external engines require nonempty `name` and `version` from the spec's `external` block |
| `fleet` | `[{role, count, vcpu, gib, machine_type}]`: managed Kubernetes requests from `engine-pods.json` plus managed compactor requests from `compactor-pods.json`, with a missing file contributing no entries; external fleets from `spec.fleet` and `spec.compaction.fleet`; local managed sizing from knobs, without costs. Machine types remain spec declarations; missing types prevent publication. See [Cost](methodology.md#cost) |
| `site_pricing` | Supplied site pricing: `{machine_types: {<type>: {vcpu, gib, usd_per_hour}}}` or legacy `{vcpu_hour_usd, gib_hour_usd}`. The machine map is preserved in full |
| `catalog_props` | `facts.catalog_props`, redacted |
| `epoch_ms` | the run's time origin; `null` for a run that was staged but never launched |
| `corpus` | metadata copied from run-local `corpus.json`; `null` when absent, with `corpus.json` listed in `missing` |
| `corpus_hash` | equals `corpus.hash` when metadata is present; otherwise retains the scorer summary hash when available |
| `corpus_seed`, `corpus_generator_version`, `corpus_row_block` | generator identity from `corpus.json`, falling back to `summary.json`; all are required for publication and must agree with recorded scorer inputs |
| `kafka` | resolved `{partitions, key, partition_assignment, value_encoding, compression}`; the first four prefer staged facts with a loaded-spec fallback, while `compression` is the resolved `spec.producer.compression`; `key` is a column name or `null` for explicitly unkeyed records |

`run.corpus` copies workload identity and scale from `corpus.json`, renaming
`corpus_hash` to `hash` and `schema_name` to `schema`; distribution fields come
from `effective_preset`. `finish.sh` fetches `<corpus_uri>/corpus.json`, while
`collect` reads only the run directory. `run.spec` remains the verbatim source
configuration, including every Kafka and producer knob.

## `artifacts`

`artifacts` maps relative paths for `spec`, `facts`, `timeline`, `corpus`,
`engine_image`, `engine_pods` and `compactor_pods` (managed Kubernetes),
`summary`, `freshness`, `exactness`, `geometry`, `compaction`, `terminal_exactness`,
`terminal_upsert`, `table_health`, `compactor_status` and `keepup_samples`. Missing inputs appear in `missing`.

`snapshots` embeds `snapshots.jsonl`, one record per commit observed by scorer
polling. The scorer stops after the offer is absorbed and before teardown, so
this is a prefix: it includes every append but can omit rewrites that land after
the final poll. Ingest metrics derived from it are complete; rewrite totals are
not. `compaction.file_counts` comes from final metadata, includes `added_bytes`
per commit and has no such gap; use it for rewrite metrics.

`publish_logs` contains a **summary per shard** instead of per-batch records. There
is one entry per `producer/publish_log-<i>.jsonl`, falling back to root-level logs.
When both locations contain logs, collection uses only `producer/`:

| Field | Description |
|---|---|
| `shard` | the index in the file's name |
| `batches` | how many records the log holds |
| `first_scheduled_ms` | when the shard's earliest batch was due |
| `first_ack_ms`, `last_ack_ms` | first and last acknowledgement times |
| `bytes`, `rows` | total published bytes and rows |
| `behind_ms_max` | maximum delay from a batch's due time to its first acknowledgement |
| `errors` | delivery errors across the shard |
| `done` | whether the log has a completion trailer; every shard must be done to establish a complete offer |

Full per-batch logs remain under `<runs_root>/<run_id>/producer/` in object
storage for detailed inspection.

## `data`

`data` holds redacted `summary`, `freshness` and `exactness`. Summary records
`exactness_mode`; exactness records `mode`, SLA `verdict`, strict `exact`, fault
counts and capped violations. Collection omits `absorbed_at_offer_end` and never
infers `AT_LEAST_ONCE` from the spec. Freshness includes both clocks' `lag_series`.

`data.summary` optionally includes `scoring_mode`, `measurement_s` and
`saturation`; an absent `scoring_mode` means `sustainable`. [Saturation diagnostics](saturation.md)
remain optional for older results and do not change schema version 3.

## `derived`

| Field | Description |
|---|---|
| `freshness` | `window` and `full` lag quantiles side by side, plus `clock_skew_suspected` and `min_lag_s` |
| `exactness` | `mode`, `verdict`, strict `exact`, `expected_rows`, `rows`, `scored_batches`, `loss_rows`, `duplicate_rows`, `duplicate_ppm`, `corrupt_batches`, plus optional `invalid_rows` and `unoffered_rows`; violations stay in `data` |
| `keepup` | `drain_s`, `backlog_rows_max`, `backlog_rows_p50`, as the scorer computed them |
| `producer` | `behind_ms_max`, `errors`, `effective_offered_rate_bytes_per_s`, `producer_bound` |
| `cost` | `usd_per_hour`, `run_hours`, `usd` |

`collect` derives these fields from recorded artifacts; it does not run new
measurements.

### `producer`

Effective rate is published value bytes divided by the recorded
acknowledgement interval. Bytes count encoded values, including any Confluent
header, before Kafka compression; keys and protocol overhead are excluded.
On failed deliveries, these totals include attempted rows. Without publish logs,
`behind_ms_max` and `errors` come from the scorer and rate is `null`.
`producer_bound` always comes from the scorer to remain consistent with `run_valid`.

### `cost`

Records hourly cost, duration and total cost. Managed Kubernetes
runs use the actual engine and compactor pod requests. Sites without
`kubernetes`, or managed runs missing either required fleet capture, have
`null` dollar costs. Duration and total cost are `null` without an epoch and end
time. See [Cost](methodology.md#cost) for the formula.

## `missing`

Absent optional inputs are listed by relative path, such as `scores/geometry.json`
or `producer/publish_log-*.jsonl`. Missing `spec.yaml` or `facts.json` fails collection.

## Compaction and readiness

`compaction` reports what the run's compaction layer did to the table.
`floor_ratio` compares its file count with the fewest files its partitioning
permits. `written_bytes` counts all bytes written over the table's lifetime;
`rewrite_share_of_writes` is rewritten bytes divided by that total and is
bounded by 1. `written_per_ingested_byte` is the write amplification, including
drain work, and is `null` when nothing was appended. `converged` says whether
rewrites finished. `observed_at_ms` records when metadata was observed; without
it, `quiet_for_s` and `converged` are `null`. `records_dropped` is a lower bound
on proven row loss: positive for a deficit, `0` when none was found or `null`
when the final row total is missing. Zero cannot prove preservation because new
rows can hide deleted rows.
The byte figures count every removed file, including deletions; use the
independent exactness results to assess data correctness.

During teardown, the observation time for `table-metadata.final.json` is saved
beside it as `table-metadata.observed-at`. `finish.sh` passes that value to
`compaction --observed-at`; analyzing legacy saved metadata without it leaves
quietness and convergence unknown.

Publication permits missing geometry but requires scorer results. Upsert runs
require `terminal_upsert.verdict: true`, with or without compaction. Other compaction
runs additionally require `terminal_exactness.verdict: true`; managed sidecars
require `compactor_status.healthy: true`. `--publish-invalid` preserves failures;
readiness failures on otherwise valid runs appear as `did-not-validate`, and valid runs whose
measured freshness exceeds their track's bounds appear as `over-bound`.
Ingestion failure states retain their original reason alongside readiness errors.
See [publication rules](../results/README.md).

### Campaign compaction (`compaction-report.json`)

`scripts/compaction-report.py` records evidence about whether the compactor kept
up while load was offered, rather than only where the table ended. Write each
report under `scores/campaign*/compaction-report.json`; `collect` embeds the
report with the widest configured window span as `campaign` and lists every
candidate in `campaign_available`. Its `campaign_pass` never redefines `run_valid`.

The report records `epoch_ms`, `offer_end_ms`, `writers_stopped_ms`,
`target_file_bytes` with `target_source`, `grid_s`, `small_fraction`, the derived
`small_file_bytes`, and `min_input_files`. Match `min_input_files` to the engine
planner: partitions below it are ineligible and excluded from eligible backlog.

#### `grid`

One entry every `grid_s` seconds from the epoch through the last
commit. Each carries `offset_s`, `at_ms`, `snapshot_id` and `phase`, which is
`offered` through `offer_end_ms`, `drain` through `writers_stopped_ms`,
`shutdown` after it, and `unknown` where the boundaries were not supplied.
Boundary instants belong to the phase that produced them. Before the first
commit, `snapshot_id` and `live` are `null`: an absent reading, not zero files,
which never satisfies a bound. Gaps carry the previous snapshot forward,
preserving the table geometry between commits.

`live` reads the snapshot's live files: `files` is `N`, `floor` is
`F = sum ceil(live_bytes / target)` over partitions, `floor_ratio` is `N/F`,
`excess` is `E = N - F`, beside `bytes`, `partitions`, `small_files`,
`small_bytes`, `oldest_small_age_s` and `size_quantiles` (`p50`, `p90`,
`max`). `hot` counts partitions holding more than one small file and `cold`
those holding exactly one: a lone small file has nothing to merge with, so it
is counted apart rather than as a backlog. `eligible` is the mergeable
backlog, the live files below `small_file_bytes` in a partition holding at
least `min_input_files` of them, with `files`, `bytes`, `partitions` and
`p95_age_s`. Ages come from each file's arrival in the manifests, so a
carried-forward snapshot still ages between commits.

#### `cycles`

Commits removing data files, with `snapshot_id`, `timestamp_ms`,
`files_removed` and `bytes_removed`. Removals come from file lifecycles, not
operation labels. When a summary declares deletions but manifests lack `DELETED`
tombstones, the reader compares previous and current live sets, walking both snapshots.
Commits declaring no deletion skip that walk. `windows.rewrite_input_bytes` is
`null` only when neither method observes a removal.

#### `windows`

`warmup_s`, `window_s`, `windows`, `measured_from_ms` and
`measured_to_ms` bound a closed measurement span. `offered_beyond_span_s`
records load after that span. `basis` is `offered` when filtering to offered-load
instants, or `all_instants` when phase boundaries are unavailable.
`floor_ratio_median` summarizes `N/F`; `rewrite_cycles` counts removal commits
within the span through the last offered instant, excluding drain work.

`campaign_pass` is `false` when `rewrite_cycles < min_rewrite_cycles`, otherwise
`null`, never `true`. `reasons` explains the outcome.
`max_floor_ratio_median` is recorded but not enforced: bin-packing can leave up
to `min_input_files - 1` fresh files beside merged files, so that bound measures
rewrite policy rather than capacity. No aggregate grid statistic establishes
keep-up; inspect the offered-load `grid` with its cycle and window boundaries.

#### `engine_diagnostics`

Optional records assigned to their grid cells:
`rewrite_passes` from sidecar logs, and `committer_samples` from cumulative gauges
with `completed_delta` since the previous sample. Missing diagnostics are `null`;
catalog metadata confirms completed replacements.
