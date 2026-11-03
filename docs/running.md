# Running a benchmark

Use the Docker Compose smoke test to check changes end to end on one machine.
Publishable measurements require a cluster; see [On a cloud](#on-a-cloud).
See [`methodology.md`](methodology.md) for measurement definitions and
[`run-spec.md`](run-spec.md) for configuration keys.

**On this page:** [Local smoke](#the-smoke) · [Cluster runs](#on-a-cloud) · [Artifacts](#the-run-directory) · [Verdicts](#reading-the-verdict)

## Prerequisites

- **Docker** with Compose v2 (`docker compose version`).
- **`jq`**, **`yq`** (mikefarah, v4) and **`curl`** on the host: the scripts read
  the run's facts, the spec's scoring keys and the engine's readiness with them.
- **16 GB RAM** available to Docker, and several GB of free disk space. The default
  smoke corpus is about 1.5 GB of encoded values (300 s at 5 MB/s), plus key
  sidecars. Compressed storage size is recorded in `corpus.json`; allow additional
  space for engine images, Kafka and table files until teardown.
- **Free ports:** 9000 / 9001 (object store), 8181 (catalog), 9092 / 29092 (broker),
  and 8081 (Flink REST) or 4040 (Spark driver UI).
  Only one engine runs at a time.

`uv sync` is only needed to run the tests and the tools outside a container; the
smoke builds its own image from the checkout. For host Python CLI examples,
use `uv run <command>` or activate `.venv` first.

## The smoke

Run these commands from the repository root:

```bash
scripts/smoke.sh                          # the full 300 s corpus, stock Flink
scripts/smoke.sh --engine spark           # the same run on Spark
scripts/smoke.sh --set duration_s=30      # a 30 s corpus, for a quick check
scripts/smoke.sh --engine external        # stage and score; you start the engine
```

### Smoke options

| Flag | Effect |
|---|---|
| `--engine flink\|spark\|external` | which spec under `specs/smoke/` to stage. `flink` and `spark` also start the engine; `external` prints the facts and waits |
| `--spec PATH` | a spec to stage instead. Match `--engine` to its engine. The script generates the preset named by `corpus` and supports one producer shard |
| `--set KEY=VALUE` | override a corpus preset key, repeatable |
| `--keep` | skip teardown, so the table and the stack can be inspected |
| `--external-ready-file PATH` | with `--engine external`, wait for `PATH` to appear instead of reading a newline from stdin |

`EPOCH_LEAD_S`, `IDLE_STOP_S` and `EXTERNAL_READY_WAIT_S` override the waits;
`FLINK_REST`, `FLINK_SLOT_WAIT_S`, `FLINK_JOB_WAIT_S` and `SPARK_UI`,
`SPARK_APP_WAIT_S`, `SPARK_QUERY_WAIT_S` override where an engine is polled and
for how long.

### Execution and verification

After scoring, the script measures the live table's file geometry and prints it
with the verdict. It exits 0 only if the scorer reports `run_valid: true`. On
failure, it prints the end of the scorer and engine logs before teardown.

For Flink, the smoke waits for slots, submits the job and runs `verify-flink`
before starting the producer. Local Spark uses `--master local[N]`, so the
driver runs the whole workload. The smoke waits for the named application and
one active streaming query; it cannot use the pod-based `verify-spark` check.
`executor_mem_mb` does not allocate executor memory in this mode.

### Encoding variants

The local stack includes the registry needed for [Confluent framing](adding-an-engine.md#confluent-values).
The JSON specs select `smoke-json`, with JSON generation and byte accounting.

```bash
scripts/smoke.sh --engine flink --spec specs/smoke/smoke-flink-confluent.yaml
scripts/smoke.sh --engine flink --spec specs/smoke/smoke-flink-json.yaml
scripts/smoke.sh --engine spark --spec specs/smoke/smoke-spark-json.yaml
```

The `kinds` specs, such as `specs/smoke/smoke-spark-kinds.yaml` and
`specs/smoke/smoke-flink-kinds-json.yaml`, check every [column kind](corpus.md#schemas)
the engine supports end to end, including nullable and nested columns.

### Compaction checks

A spec with a managed compaction driver starts the sidecar before the engine,
with `--engine flink` or `--engine spark` and the matching
`specs/smoke/smoke-<engine>-compact.yaml`. `compaction.json` measures its effect,
`terminal-exactness.json` checks the live row IDs in a pinned snapshot, and
`compactor.log` retains full rewrite diagnostics. The smoke checks integration,
not whether compaction sustains the standard workload. For concurrent append/rewrite and
restart qualification, run:

```bash
INGEST_BENCH_RUN_COMPACTION_CONCURRENCY=1 uv run pytest -q tests/test_compaction_concurrency.py
```

This creates an isolated Compose project and checks every retained snapshot;
expect at least fifteen minutes per case. Artifacts remain under `runs/`.

### Cleanup and interpretation

Teardown clears object storage, so subsequent smokes regenerate the corpus.
With `--keep`, changing `--set` can leave multiple corpora with the same preset
name; staging rejects that ambiguity. Host run directories under `runs/` survive
teardown.

A 30-second corpus is useful for checking drain and exactness, but gives little
evidence about steady-state performance. The smoke specs exclude 60 seconds of
warmup; if the run ends before then, all window quantiles use the final sample.
Read `freshness.full` for the full lag curve, including startup and commit delays.

`scoring.warmup_s` is 60 in smoke specs and 120 in hour-long specs. Choose a
warmup that covers expected startup while leaving enough time to measure steady
state; the full series still reports the excluded lag.

## CI

See [Contributing](../CONTRIBUTING.md#required-checks) for required checks.
`ci.yml` runs on pull requests and pushes to `main`; `smoke.yml` is manual only.

## On a cloud

Measured runs place the engine, producer and scorer in separate pods. AWS runs
use EKS and S3 with either Amazon MSK and Glue or in-cluster Kafka and
Lakekeeper. Complete the prerequisites and corpus setup in the
[AWS guide](../deploy/aws/README.md), then follow this sequence for each run.

```bash
scripts/run.sh specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-ref.yaml --publish results/
scripts/purge.sh <the run id it printed> --artifacts   # once you are done with the table
```

### Automatic runs

`run.sh` prints `run_id: <id>`, gates every `--gate-interval-s` (default 60),
tears down when scoring finishes or the gate trips, and runs `finish.sh`.
It returns the finish status, or 6 if teardown fails and resources may remain.
`RUN_MAX_S` defaults to two hours after launch; timeout tears down, collects
artifacts and fails. Launch failure leaves the fleet for inspection and prints
the cleanup command.

The gate always uses `--teardown`: `--breaches` consecutive non-passing checks
(default 3) stop the fleet. Before the epoch, a healthy check reports `PASS`;
a stale scorer still reports `VOID`.

In-cluster Kafka staging waits up to 20 minutes for quiet brokers;
`--wait-brokers-s 0` skips only that wait, not topic or capacity checks.
`--publish` and `--variant` pass through to `finish.sh`.

External runs wait for `--external-ready-file <path>` or a newline before launch.
For teardown, stop the reader and create `--external-detached-file <path>` when
requested. After `--external-detach-wait-s` (default 300), teardown drops the
topic even without that signal and warns that the reader may restart.

### Individual steps

To run or recover individual steps, use the run id printed by staging:

```bash
RUN_ID=$(scripts/stage.sh specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-ref.yaml | awk -F': ' '/^run_id: /{print $2}')
scripts/launch.sh "$RUN_ID"
scripts/gate.sh "$RUN_ID" --teardown       # every minute or so, while the run goes
scripts/teardown.sh "$RUN_ID"              # once the offer has drained
scripts/finish.sh "$RUN_ID" --publish results/   # geometry, the verdict, the result
scripts/purge.sh "$RUN_ID" --artifacts     # once you are done with the table
```

The Flink spec above and its Spark sibling are hour-long starting points, not
proven fleet sizes. Check `machine_type` against the actual node type and
fill in site pricing before publishing. With `pricing.machine_types`, price every
fleet type, including coordinator overrides (`driver_machine_type` or `jm_machine_type`).
These fields record hardware and pricing metadata. Site placement settings select nodes.
Follow the [fleet selection rules](../results/README.md#publication-rules) for capacity probes.

Managed engines share these drivers; staging uses `verify-<engine>` to check
their effective configuration against the copied spec before production.

| Driver | What it does |
|---|---|
| `run.sh <spec>` | Runs the sequence below, excluding purge. |
| `stage.sh <spec>` | For in-cluster Kafka, checks capacity and waits for quiet brokers; then stages the run as a Job and downloads its artifacts. For managed engines, applies the rendered manifests, waits for the fleet, verifies the running configuration and records the image. Prints the run ID. |
| `launch.sh <run_id>` | Checks available node capacity, starts the scorer and waits for its first reading, then starts producer shards. Records the run epoch. See [placement pitfalls](pitfalls.md). |
| `gate.sh <run_id>` | Reports `PASS`, `UNDERSIZED` or `VOID` from scorer artifacts, with exit codes 0, 3 or 5. With `--teardown`, stops the fleet after three consecutive non-passing checks, configurable with `--breaches N`. |
| `teardown.sh <run_id>` | Stops producer pods in the foreground, deletes engine and scorer resources, waits for an external detach signal when configured, drops the topic, copies the final table metadata and collects run artifacts. |
| `finish.sh <run_id>` | Measures file geometry, collects artifacts and prints the verdict and geometry. `--publish <dir>` also writes a result. Exits 0 only for `run_valid: true`. |
| `purge.sh <run_id>` | Drops the table and deletes its files. `--artifacts` also deletes the run's storage prefix. Lists deletion targets and asks for confirmation; `--yes` skips the prompt. |
| `preflight-kafka.sh <spec>` | Read-only in-cluster broker check, called by staging. Exits 3 for another run's topic or insufficient space at the topic's replication factor. Uses generated corpus metadata for presets with `sizing_encoding`. Estimates uncompressed bytes, so it may reject data that would fit compressed. See [broker quiet checks](../deploy/k8s/stack/README.md#broker-storage-between-runs). |

Each driver accepts `--site` (default `./site.yaml`); `--help` lists wait and pod-size settings.

### Scorer resources

`SCORER_READ_WORKERS` on `launch.sh` limits concurrent reads; `SCORER_READ_PROCESSES` uses separate
interpreters, capped at `SCORER_READ_WORKERS`. `SCORER_CPU` and `SCORER_MEMORY` size the scorer pod
(default 2 and 4Gi; the memory value is also the pod's limit). Raise them with the read width: a poll
holds one ID column per concurrent reader and every read process is another interpreter. The launch
capacity check counts the scorer as one producer-sized pod, so a larger scorer needs a node with that
much room of its own.

`PRODUCER_NODE_SELECTOR` and `SCORER_NODE_SELECTOR` on `launch.sh` take a one-line JSON object, the form of
the site's `kubernetes.node_selector`, and place the producer shards and the scorer. They default to the
site's selector. Set them to keep these pods off the engine's nodes, since the site's selector applies to
every harness pod.

### Teardown and validation

> **Cleanup boundary:** teardown releases compute before collection. Data remains
> until `purge.sh` deletes it.

Teardown waits for producer exit before topic deletion. A producer deletion timeout
retains the topic but continues resource cleanup and artifact capture. A scorer
timeout still allows topic deletion, final metadata capture and collection. Either timeout fails without `teardown-complete`.
Managed engine and compactor pods must disappear before final metadata capture.

Compaction runs also require:

- A `writers-stopped` marker, with external writers acknowledging detachment.
- Compactor status and logs captured before deletion. Health requires the full
  declared fleet with every container ready and running; a restarting executor
  blocks normal publication even if terminal exactness passes.
- Final-snapshot verification against `scoring.exactness` for missing or unexpected
  IDs, forbidden duplicates and incomplete producer logs. Failure blocks normal publication;
  `--publish-invalid` retains the failed result.

`COMPACTION_CATCHUP_S` (default 0) permits sidecar rewriting after ingest stops.
`compaction-catchup.json` records this time for separate costing; use 0 on both
arms for the comparison's shutdown policy.

`finish.sh` runs `verify-table` with the run's exactness mode and 32 concurrent
readers on the host. `AT_LEAST_ONCE` permits duplicates. For large scans, use `--verify-in-cluster` (also accepted by `run.sh`) to avoid pulling
all IDs to the host. It uploads the saved metadata to pin the same snapshot,
then fetches the verdict and `scores/verify-table.log`. Size the Job with
`VERIFY_CPU` (4), `VERIFY_MEMORY` (8Gi), `VERIFY_READ_WORKERS` and `VERIFY_WAIT_S`.
Eight billion IDs need about 1 GiB of bitmap memory, plus an ID column per reader.
A missing verdict fails closed. Both modes write timings and resources to
`scores/terminal-validation.json`, separate from run duration.

Upsert runs always run `verify-upsert` instead. It needs the scorer's stamp checkpoints,
gigabytes each, so the host mode fetches them and `--verify-in-cluster` reads them in place.

- **Cluster mode** first runs a Spark scan Job (`engines/spark/upsert_scan.py`, Spark 4.1
  and Iceberg 1.12). It registers the frozen metadata in a throwaway catalog and writes the
  live rows of every sampled snapshot and the final snapshot under `scores/live/`. The
  verifier then checks those rows with `--reader none`. Size the Job with
  `VERIFY_SPARK_CPU` (8) and `VERIFY_SPARK_MEMORY` (32Gi in whole Gi, 75% of it heap).
  Its log is `scores/spark-scan.log`.
- **Host mode** reads live rows with pyiceberg, which applies position deletes and
  deletion vectors but not equality deletes. Verify equality-delete tables, such as
  Flink upsert tables on format version 2, with `--verify-in-cluster`.

`scripts/gen-corpus.sh` builds an upsert corpus's seed once, after the corpus. Deny deletes under
the corpus's `seed/` prefix in the bucket policy: every run's table references those files,
and an engine's snapshot expiry or orphan cleanup could otherwise remove them. The
[AWS guide](../deploy/aws/README.md#upsert-runs) has the policy, the cluster smoke and pod sizes. Each upsert
run is offered once. A restarted producer shard refuses to replay, so stage a fresh run.
The guard is a marker each shard writes before sending, not an atomic claim: never start two
copies of one shard at once.

`purge.sh` deletes measured data only after `--yes` or an interactive `y`.
It refuses to run while the scorer is still in the namespace. It reads the
table location from the metadata saved during teardown; if that document is
missing, it queries the catalog. An unreachable catalog causes an error,
not an assumption that the table is absent.

### Resource capture and scorer polling

`engine-fleet --spec <spec.yaml> --pods <pods.json> --out <engine-pods.json>`
captures admitted CPU and memory requests from a `kubectl get pods -o json`
response for the engine fleet. Staging runs it automatically and archives its
output for cost calculation. It ignores terminal and terminating pods, then
requires the declared number of running pods. Invalid or incomplete fleets
exit with status 2. The output omits pod environment values and annotations.

For direct scorer invocations, `score --poll-interval-s <seconds>` sets the
polling interval (default `5`). The launch scripts use that default. Keep the
interval equal across compared runs: scorer-clock lag and drain include polling
delay; see [Keep-up](methodology.md#keep-up).

### Publishing a result

Run `finish.sh <run_id> --publish results/`; use `--variant <name>` to label
tuning. `--publish-invalid` retains failed runs but does not waive metadata
requirements such as real machine types. See [publishing](../results/README.md)
for paths, overwrites, collision handling and validation.

### In-cluster stack

The [in-cluster stack](../deploy/k8s/stack/README.md) uses the same run sequence.
For a plain-HTTP catalog at `<service>.<namespace>.svc[.cluster.local]`, local
drivers open a `kubectl port-forward` tunnel and rewrite only its URI. They choose
a free local port unless `CATALOG_FORWARD_PORT` fixes one;
`CATALOG_FORWARD_PROBE` (default `/health`) sets the readiness path. `teardown.sh`,
`finish.sh` and `purge.sh` share this property reader, including for copied metadata.

### Which steps run in the cluster, and why

`stage`, `drop-topic`, producer shards, the scorer, `gen-corpus` and
`merge-corpus` run as Jobs for broker access, data throughput and pod identity.
The host renders manifests, waits, fetches artifacts and evaluates verdicts.
Use the [AWS host prerequisites](../deploy/aws/README.md#prerequisites).

Install `uv sync --extra aws` or `pip install '.[aws]'` for host metadata reads,
geometry and table deletion. Glue request signing requires the extra's `boto3`.

### What the site declares about a cluster

- **Identity:** EKS Pod Identity supplies credentials. Set
  `site.kubernetes.aws_region` to inject `AWS_REGION` and `AWS_DEFAULT_REGION`;
  omit it outside AWS.
- **Placement:** site node selectors and tolerations apply to all Jobs and
  engine pods. Build matching images with `push-images.sh --platform`
  (default `linux/amd64`).
- **Artifacts:** host files go to `./runs/<run_id>/`, overridden by `RUNS_DIR`.
  Pods upload staging files, publish logs and per-poll scorer artifacts to
  `site.runs_root`, where gate and finish read them after pod deletion. The scorer
  reads the offer from uploaded publish logs, not host files.

## The run directory

Staging writes `runs/<run_id>/`, and everything downstream reads it:

| Path | Written by | What it is |
|---|---|---|
| `spec.yaml` | stage | the run spec, copied verbatim |
| `facts.json` | stage, launch | engine contract; local and cluster launch record the shared producer/scorer epoch — see [`adding-an-engine.md`](adding-an-engine.md) |
| `timeline.log` | stage | one line per phase transition |
| `job.sql`, `flink-conf.yaml`, `flink.env` | stage | a Flink run's script, the settings it is submitted with, and the cluster shape the local stack sizes containers from |
| `spark-defaults.conf` | stage | a Spark run's settings, and `job.json`, `reader-schema.avsc`, `job.env` beside it |
| `flinkdeployment.yaml` / `sparkapplication.yaml` | stage | the engine resource applied to its Kubernetes operator |
| `flink-job-configmap.yaml` / `spark-job-configmap.yaml` | stage | rendered files packaged as a ConfigMap for engine pods |
| `engine-image.json` | stage | the image the engine ran and the digest the node pulled |
| `engine-pods.json` | cluster stage | verified fleet's admitted CPU/memory requests, uploaded under `stage/`; finish retrieves missing local copies for cost calculation |
| `publish_log-<i>.jsonl` | producer | one record per batch: rows, bytes, when it was due, when it was acked. Beside the spec locally, under `producer/` on a cluster |
| `scores/summary.json` | scorer | the verdict, rewritten on every poll |
| `scores/freshness.json` | scorer | table-clock lag quantiles, plus the whole lag curve on both clocks |
| `scores/exactness.json` | scorer | delivery mode and verdict, strict exactness, fault totals and the first violations |
| `scores/keepup.json` | scorer | the keep-up scalars |
| `scores/geometry.json` | `file-sizes` | file geometry over time and at the end of the run |
| `scores/table-health.json` | `table-health` | upsert runs: the merge-on-read work the final table leaves readers, from its metadata |
| `scores/snapshots.jsonl` | scorer | one record per table commit |
| `scores/keepup_samples.jsonl` | scorer | offered and committed counts, once per poll |
| `scores/scorer.log` | teardown | scorer stdout, including `POLL`, `SLOW_POLL` and `POLL_FAILED` diagnostics |
| `scores/drop-topic.log` | teardown | output from the topic-deletion Job, including when deletion completed |
| `table-metadata.final.json` | teardown | the table's last metadata document, copied |
| `run.json` | `collect` | the whole result, redacted — see [`results-format.md`](results-format.md) |

## Reading the verdict

`runs/<run_id>/scores/summary.json` contains the verdict. Scripts print its key
fields; see definitions of `run_valid`, `reason` and `state` in
[Methodology, The verdict](methodology.md#the-verdict).

To inspect artifacts directly, run:

```bash
gate --out runs/<run_id>/scores                  # PASS / UNDERSIZED / VOID, mid-run
file-sizes --metadata runs/<run_id>/table-metadata.final.json \
           --epoch <facts.epoch> --out runs/<run_id>/scores
```

`finish.sh` runs the second and prints the file-size p50 and small-file share as
the last line of the verdict block. Run it before anything expires the run's
snapshots — see [`pitfalls.md`](pitfalls.md).

`finish.sh` also runs `compaction --observed-at`, which reads the table's end
state. Whether a compactor kept up *during* the offer is a separate reading over the
whole commit history, so it is a separate script and is not part of the verdict:

```bash
uv run python scripts/compaction-report.py \
  --metadata runs/<run_id>/table-metadata.final.json \
  --epoch <facts.epoch> --offer-end <when the offer ended> \
  --writers-stopped <when teardown recorded the writers stopping> \
  --target-bytes 536870912 --min-input-files <the engine's planner bound> \
  --sidecar-log runs/<run_id>/scores/compactor.log \
  --out runs/<run_id>/scores/campaign
```

Times are UNIX seconds or ISO 8601 instants with an offset. It writes
`scores/campaign/compaction-report.json`, which `collect` can embed in
`run.json`. Its `campaign_pass` never enters `run_valid`; `--sidecar-log` and
`--committer-metrics` are optional diagnostics joined onto the same grid. See
[Campaign compaction](results-format.md#campaign-compaction-compaction-reportjson).

To recompute freshness from a finished run's local files, run `uv run rescore-freshness runs/<run_id>`.
It replaces `scores/freshness.json` and freshness-related summary fields, including the derived verdict.
Then rerun `collect` to rebuild `run.json`. Other score artifacts stay unchanged. Legacy append runs retain
recorded timestamps; legacy upserts and advancing overwrites require effective visibility timestamps.

See [recorded smokes](examples/) for local Flink and AWS Flink/Spark artifacts.
