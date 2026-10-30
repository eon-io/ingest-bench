# lakehouse-ingest-bench

A benchmark for comparing streaming ingest from Apache Kafka into Apache Iceberg.
It replays the same frozen workload across engines and independently reads the
resulting tables to measure throughput, freshness, exactness and output file sizes.

The repository includes integrations for Apache Flink and Apache Spark, plus an
[external-engine contract](docs/adding-an-engine.md) for benchmarking other
Kafka-to-Iceberg writers.

We are building Eon Data Engine, a streaming engine for Kafka-to-Iceberg ingest.
It runs through the external-engine contract, and its results sit beside
Flink and Spark under the same rules.

| Mode | Question it answers |
|---|---|
| **Sustainable** (default) | What fleet keeps up within a freshness bound? |
| **[Saturation](docs/saturation.md)** | What throughput can fixed compute commit under excess load? |

## Why this benchmark

We found no benchmark that measured streaming ingest the way a lakehouse runs it:
keeping a table fresh at high volume while accounting for small files and compaction
cost. This benchmark measures them together:

- **Freshness at volume:** the table must meet fixed p95 and maximum lag bounds and
  drain fully at the offered rate.
- **Small files:** the benchmark records the file sizes each engine leaves behind.
- **Compaction:** fleets that rewrite small files are costed with that compute
  included.

The three pull against each other. Committing often keeps a table fresh but leaves
many small files that slow reads, and compacting them costs compute of its own.

We also wanted to measure how engines handle skew, because real-world workloads
are often skewed. The standard 200-column events corpus runs on both uniform and
skewed keys at approximately 1 million rows per second.

## Quickstart

Install Docker with Compose v2, `jq`, `yq` (mikefarah v4), and `curl`.
Give Docker 16 GB of RAM. Use `uv` for host tools and tests.

```bash
git clone <this repository> && cd lakehouse-ingest-bench
uv sync
scripts/smoke.sh
scripts/smoke.sh --engine spark
```

The smoke stages a five-minute, 5 MB/s corpus, runs ingest, scores the table,
and prints its verdict and file geometry. Artifacts remain in `runs/<run_id>/`.
For a quick integration check, add `--set duration_s=30`; this can end before
warmup or the first commit, so follow the [smoke interpretation
guide](docs/running.md#the-smoke). Local smokes share one machine's resources
and are not performance results. [Recorded smokes](docs/examples/) include
the artifacts behind their verdicts.

Prefix Python CLI examples with `uv run`, or activate `.venv` first.

## Cluster runs

Provide a Kubernetes cluster, configure `site.yaml`, build images and generate
a corpus using the [AWS setup guide](deploy/aws/README.md) or
[in-cluster stack guide](deploy/k8s/stack/README.md). Keep brokers on dedicated
nodes for measurements and budget for producer and scorer resources.

The [run workflow](docs/running.md#on-a-cloud) uses shell drivers under `scripts/`:

```text
stage → launch → gate → teardown → finish → purge
```

`scripts/run.sh <spec>` runs all steps except purge. Tear down billable shared
services between campaigns.

## Results

<!-- results:start -->
Each figure is the smallest fleet found so far that streamed an hour of Kafka data into Iceberg and kept up. Lower is better.

Fleets shown here meet both criteria:

- **Exactly-once** (append tracks): every offered row is committed once, verified by row ID.
- **Latest per key** (upsert tracks): each key's newest version is live, and no other, verified at sampled snapshots.
- **Freshness:** lag p95 ≤ 90 s and max ≤ 120 s, after up to 5 minutes of warm-up.

See the [methodology](docs/methodology.md) for details.

### Ingest only

Requested compute for ingest without file compaction.

| Engine | Tier | [50 MB/s upsert](results/RESULTS.md#50-mbs--upsert) | [1376 MB/s skewed](results/RESULTS.md#1376-mbs--skewed-keys) | [1376 MB/s uniform](results/RESULTS.md#1376-mbs--uniform-keys) |
|---|---|---:|---:|---:|
| Eon Data Engine | tuned | [4&nbsp;vCPU · 10&nbsp;GiB](results/external/events-50mbs-upsert-orders/2026-10-19/19-55-44Z/README.md) | [35&nbsp;vCPU · 172&nbsp;GiB](results/external/events-1mrps-skew/2026-10-22/22-27-37Z/README.md) | [35&nbsp;vCPU · 172&nbsp;GiB](results/external/events-1mrps-uniform/2026-10-22/23-38-24Z/README.md) |
| Flink | tuned | n/a | [239&nbsp;vCPU · 956&nbsp;GiB](results/flink/events-1mrps-skew/2026-10-15/20-36-35Z/README.md) | [253&nbsp;vCPU · 652&nbsp;GiB](results/flink/events-1mrps-uniform/2026-10-15/18-20-01Z/README.md) |
| Spark | tuned | [22&nbsp;vCPU · 81&nbsp;GiB](results/spark/events-50mbs-upsert-orders/2026-10-22/00-33-53Z/README.md) | [135&nbsp;vCPU · 214&nbsp;GiB](results/spark/events-1mrps-skew/2026-10-21/22-45-14Z/README.md) | [135&nbsp;vCPU · 214&nbsp;GiB](results/spark/events-1mrps-uniform/2026-10-21/23-28-15Z/README.md) |
| Flink | reference | [13&nbsp;vCPU · 40&nbsp;GiB](results/flink/events-50mbs-upsert-orders/2026-10-09/18-09-51Z/README.md) | n/a | n/a |

### With compaction

Requested compute for fleets that ingest data and rewrite small files into larger ones.

Compaction is not yet included in the keep-up verdict. Review each run's notes for compaction backlog evidence before comparing.

| Engine | Compaction method | [1376 MB/s skewed](results/RESULTS.md#1376-mbs--skewed-keys) | [1376 MB/s uniform](results/RESULTS.md#1376-mbs--uniform-keys) |
|---|---|---:|---:|
| Eon Data Engine | Integrated compaction | [63&nbsp;vCPU · 228&nbsp;GiB](results/external/events-1mrps-skew/2026-10-22/20-03-43Z/README.md) | [63&nbsp;vCPU · 228&nbsp;GiB](results/external/events-1mrps-uniform/2026-10-22/21-15-57Z/README.md) |
| Spark | Spark rewrite sidecar | [270&nbsp;vCPU · 523&nbsp;GiB](results/spark/events-1mrps-skew/2026-10-22/01-03-29Z/README.md) | [270&nbsp;vCPU · 523&nbsp;GiB](results/spark/events-1mrps-uniform/2026-10-22/02-12-21Z/README.md) |
| Flink | Flink maintenance sidecar | n/a | [422&nbsp;vCPU · 1326&nbsp;GiB](results/flink/events-1mrps-uniform/2026-10-22/05-02-06Z/README.md) |

Fleets, freshness and file sizes for every track are in the [full results](results/RESULTS.md).
<!-- results:end -->

[How results are selected](results/README.md#tracks-classes-and-tiers) explains tracks, modes and tiers.
To add a result, follow [submitting a result](docs/submitting.md).

## Documentation

| Task | Guides |
|---|---|
| Run a benchmark | [Running](docs/running.md), [pitfalls](docs/pitfalls.md) |
| Configure a workload | [Run specs](docs/run-spec.md), [corpora](docs/corpus.md) |
| Tune an engine | [Flink](engines/flink/README.md), [Spark](engines/spark/README.md) |
| Interpret measurements | [Methodology](docs/methodology.md), [saturation](docs/saturation.md), [result format](docs/results-format.md) |
| Check engines that could not keep up | [Attempted engines](docs/attempted-engines.md) |
| Add a result | [Submitting a result](docs/submitting.md), [publication rules](results/README.md) |
| Extend or contribute | [Adding an engine](docs/adding-an-engine.md), [contributing](CONTRIBUTING.md) |

Implementation lives in `ingest_bench/`, integrations in `engines/`, drivers in
`scripts/`, and deployment templates in `deploy/`. Workload definitions and
tracks are in `workloads/`, run specs in `specs/`, and local artifacts in `runs/`.

## License

[CC BY-NC-SA 4.0](LICENSE). See [NOTICE](NOTICE) for Eon attribution and
third-party notices.
