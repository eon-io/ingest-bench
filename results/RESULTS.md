# Results

Each figure is the smallest fleet found so far that streamed an hour of Kafka data into Iceberg and kept up. Lower is better.

A fleet keeps up when it meets both criteria:

- **Exactly-once** (append tracks): every offered row is committed once, verified by row ID.
- **Latest per key** (upsert tracks): each key's newest version is live, and no other, verified at sampled snapshots.
- **Freshness:** lag p95 ≤ 90 s and max ≤ 120 s, after up to 5 minutes of warm-up.

See the [methodology](../docs/methodology.md) for details.

## Ingest only

Requested compute for ingest without file compaction.

| Engine | Tier | [50 MB/s upsert](#50-mbs--upsert) | [1376 MB/s skewed](#1376-mbs--skewed-keys) | [1376 MB/s uniform](#1376-mbs--uniform-keys) |
|---|---|---:|---:|---:|
| Eon Data Engine | tuned | [4&nbsp;vCPU · 10&nbsp;GiB](external/events-50mbs-upsert-orders/2026-10-19/19-55-44Z/README.md) | [35&nbsp;vCPU · 172&nbsp;GiB](external/events-1mrps-skew/2026-10-22/22-27-37Z/README.md) | [35&nbsp;vCPU · 172&nbsp;GiB](external/events-1mrps-uniform/2026-10-22/23-38-24Z/README.md) |
| Flink | tuned | n/a | [239&nbsp;vCPU · 956&nbsp;GiB](flink/events-1mrps-skew/2026-10-15/20-36-35Z/README.md) | [253&nbsp;vCPU · 652&nbsp;GiB](flink/events-1mrps-uniform/2026-10-15/18-20-01Z/README.md) |
| Spark | tuned | [22&nbsp;vCPU · 81&nbsp;GiB](spark/events-50mbs-upsert-orders/2026-10-22/00-33-53Z/README.md) | [135&nbsp;vCPU · 214&nbsp;GiB](spark/events-1mrps-skew/2026-10-21/22-45-14Z/README.md) | [135&nbsp;vCPU · 214&nbsp;GiB](spark/events-1mrps-uniform/2026-10-21/23-28-15Z/README.md) |
| Flink | reference | [13&nbsp;vCPU · 40&nbsp;GiB](flink/events-50mbs-upsert-orders/2026-10-09/18-09-51Z/README.md) | n/a | n/a |

## With compaction

Requested compute for fleets that ingest data and rewrite small files into larger ones.

Compaction is not yet included in the keep-up verdict. Review each run's notes for compaction backlog evidence before comparing.

| Engine | Compaction method | [1376 MB/s skewed](#1376-mbs--skewed-keys) | [1376 MB/s uniform](#1376-mbs--uniform-keys) |
|---|---|---:|---:|
| Eon Data Engine | Integrated compaction | [63&nbsp;vCPU · 228&nbsp;GiB](external/events-1mrps-skew/2026-10-22/20-03-43Z/README.md) | [63&nbsp;vCPU · 228&nbsp;GiB](external/events-1mrps-uniform/2026-10-22/21-15-57Z/README.md) |
| Spark | Spark rewrite sidecar | [270&nbsp;vCPU · 523&nbsp;GiB](spark/events-1mrps-skew/2026-10-22/01-03-29Z/README.md) | [270&nbsp;vCPU · 523&nbsp;GiB](spark/events-1mrps-uniform/2026-10-22/02-12-21Z/README.md) |
| Flink | Flink maintenance sidecar | n/a | [422&nbsp;vCPU · 1326&nbsp;GiB](flink/events-1mrps-uniform/2026-10-22/05-02-06Z/README.md) |

## 50 MB/s · upsert

<details><summary>Workload details</summary>

- Track: `events-50mbs-upsert-orders`
- Replay
  - Rate: 50 MB/s
  - Duration: 1 hour
  - `partition_key` values: 5
  - Zipf alpha: 0.0
  - Corpus hash: `739703d1`
- Kafka
  - Partitions: 48
  - Message key: `event_id`
  - Partition assignment: corpus
  - Encoding: Confluent Avro
  - Compression: lz4
- Keep-up criteria
  - Latest-per-key
  - Freshness p95: ≤ 90 s
  - Freshness max: ≤ 120 s
  - Warm-up: ≤ 300 s
- Iceberg partitioning: `partition_key`

</details>

| Engine | Mode | Tier | Fleet | Compute | Freshness p50 / p95 | Median file | Files < 32 MiB | Delete files | Rows per live row | Details |
|---|---|---|---|---:|---:|---:|---:|---:|---:|---|
| Eon Data Engine | Ingest only | tuned | 1&nbsp;×&nbsp;reader<br>1&nbsp;×&nbsp;writer<br>1&nbsp;×&nbsp;committer | 4&nbsp;vCPU · 10&nbsp;GiB | 63&nbsp;s / 81&nbsp;s | 507.0&nbsp;MiB | 16% | 23 DV | 5.21 | [notes](external/events-50mbs-upsert-orders/2026-10-19/19-55-44Z/README.md) |
| Flink | Ingest only | reference | 1&nbsp;×&nbsp;JM<br>3&nbsp;×&nbsp;TM | 13&nbsp;vCPU · 40&nbsp;GiB | 60&nbsp;s / 88&nbsp;s | 512.9&nbsp;MiB | 6% | 45 DV | 5.21 | [notes](flink/events-50mbs-upsert-orders/2026-10-09/18-09-51Z/README.md) |
| Spark | Ingest only | tuned | 1&nbsp;×&nbsp;driver<br>5&nbsp;×&nbsp;executor | 22&nbsp;vCPU · 81&nbsp;GiB | 52&nbsp;s / 81&nbsp;s | 297.7&nbsp;MiB | 7% | 38 DV | 3.86 | [notes](spark/events-50mbs-upsert-orders/2026-10-22/00-33-53Z/README.md) |

## 1376 MB/s · skewed keys

<details><summary>Workload details</summary>

- Track: `events-1mrps-skew`
- Replay
  - Rate: 1376 MB/s
  - Duration: 1 hour
  - Keys: 512
  - Zipf alpha: 1.0
  - Corpus hash: `59c2b4a1`
- Kafka
  - Partitions: 192
  - Message key: `partition_key`
  - Partition assignment: round-robin
  - Encoding: Confluent Avro
  - Compression: lz4
- Keep-up criteria
  - Exactly-once
  - Freshness p95: ≤ 90 s
  - Freshness max: ≤ 120 s
  - Warm-up: ≤ 300 s
- Iceberg partitioning: `partition_key`

</details>

| Engine | Mode | Fleet | Compute | Freshness p50 / p95 | Median file | Files < 32 MiB | Details |
|---|---|---|---:|---:|---:|---:|---|
| Eon Data Engine | Ingest only | 4&nbsp;×&nbsp;reader<br>5&nbsp;×&nbsp;writer<br>1&nbsp;×&nbsp;committer | 35&nbsp;vCPU · 172&nbsp;GiB | 61&nbsp;s / 74&nbsp;s | 7.4&nbsp;MiB | 87% | [notes](external/events-1mrps-skew/2026-10-22/22-27-37Z/README.md) |
| Eon Data Engine | Integrated compaction | 4&nbsp;×&nbsp;reader<br>5&nbsp;×&nbsp;writer<br>1&nbsp;×&nbsp;committer<br>7&nbsp;×&nbsp;compactor | 63&nbsp;vCPU · 228&nbsp;GiB | 60&nbsp;s / 74&nbsp;s | 262.5&nbsp;MiB | 42% | [notes](external/events-1mrps-skew/2026-10-22/20-03-43Z/README.md) |
| Spark | Ingest only | 1&nbsp;×&nbsp;driver<br>19&nbsp;×&nbsp;executor | 135&nbsp;vCPU · 214&nbsp;GiB | 56&nbsp;s / 74&nbsp;s | 10.0&nbsp;MiB | 100% | [notes](spark/events-1mrps-skew/2026-10-21/22-45-14Z/README.md) |
| Flink | Ingest only | 1&nbsp;×&nbsp;JM<br>34&nbsp;×&nbsp;TM | 239&nbsp;vCPU · 956&nbsp;GiB | 36&nbsp;s / 63&nbsp;s | 0.1&nbsp;MiB | 96% | [notes](flink/events-1mrps-skew/2026-10-15/20-36-35Z/README.md) |
| Spark | Spark rewrite sidecar | 1&nbsp;×&nbsp;driver<br>19&nbsp;×&nbsp;executor<br>1&nbsp;×&nbsp;Spark&nbsp;driver<br>19&nbsp;×&nbsp;Spark&nbsp;executor | 270&nbsp;vCPU · 523&nbsp;GiB | 56&nbsp;s / 74&nbsp;s | 7.2&nbsp;MiB | 91% | [notes](spark/events-1mrps-skew/2026-10-22/01-03-29Z/README.md) |

### Did not validate

| Engine | Mode | Fleet | Compute | State | Reason | Date | Details |
|---|---|---|---:|---|---|---|---|
| Flink | Flink maintenance sidecar | 1&nbsp;×&nbsp;JM<br>34&nbsp;×&nbsp;TM<br>1&nbsp;×&nbsp;maintenance&nbsp;JM<br>24&nbsp;×&nbsp;maintenance&nbsp;TM | 408&nbsp;vCPU · 1630&nbsp;GiB | over-bound | freshness p95 is 105.833 s where track 'events-1mrps-skew' bounds it at 90 s; freshness max is 158.34 s where track 'events-1mrps-skew' bounds it at 120 s | 2026-10-22 | [notes](flink/events-1mrps-skew/2026-10-22/03-52-24Z/README.md) |

## 1376 MB/s · uniform keys

<details><summary>Workload details</summary>

- Track: `events-1mrps-uniform`
- Replay
  - Rate: 1376 MB/s
  - Duration: 1 hour
  - Keys: 512
  - Zipf alpha: 0.0
  - Corpus hash: `291a5072`
- Kafka
  - Partitions: 192
  - Message key: `partition_key`
  - Partition assignment: round-robin
  - Encoding: Confluent Avro
  - Compression: lz4
- Keep-up criteria
  - Exactly-once
  - Freshness p95: ≤ 90 s
  - Freshness max: ≤ 120 s
  - Warm-up: ≤ 300 s
- Iceberg partitioning: `partition_key`

</details>

| Engine | Mode | Fleet | Compute | Freshness p50 / p95 | Median file | Files < 32 MiB | Details |
|---|---|---|---:|---:|---:|---:|---|
| Eon Data Engine | Ingest only | 4&nbsp;×&nbsp;reader<br>5&nbsp;×&nbsp;writer<br>1&nbsp;×&nbsp;committer | 35&nbsp;vCPU · 172&nbsp;GiB | 59&nbsp;s / 74&nbsp;s | 21.0&nbsp;MiB | 100% | [notes](external/events-1mrps-uniform/2026-10-22/23-38-24Z/README.md) |
| Eon Data Engine | Integrated compaction | 4&nbsp;×&nbsp;reader<br>5&nbsp;×&nbsp;writer<br>1&nbsp;×&nbsp;committer<br>7&nbsp;×&nbsp;compactor | 63&nbsp;vCPU · 228&nbsp;GiB | 59&nbsp;s / 74&nbsp;s | 275.5&nbsp;MiB | 39% | [notes](external/events-1mrps-uniform/2026-10-22/21-15-57Z/README.md) |
| Spark | Ingest only | 1&nbsp;×&nbsp;driver<br>19&nbsp;×&nbsp;executor | 135&nbsp;vCPU · 214&nbsp;GiB | 56&nbsp;s / 74&nbsp;s | 12.3&nbsp;MiB | 100% | [notes](spark/events-1mrps-uniform/2026-10-21/23-28-15Z/README.md) |
| Flink | Ingest only | 1&nbsp;×&nbsp;JM<br>18&nbsp;×&nbsp;TM | 253&nbsp;vCPU · 652&nbsp;GiB | 34&nbsp;s / 61&nbsp;s | 33.0&nbsp;MiB | 3% | [notes](flink/events-1mrps-uniform/2026-10-15/18-20-01Z/README.md) |
| Spark | Spark rewrite sidecar | 1&nbsp;×&nbsp;driver<br>19&nbsp;×&nbsp;executor<br>1&nbsp;×&nbsp;Spark&nbsp;driver<br>19&nbsp;×&nbsp;Spark&nbsp;executor | 270&nbsp;vCPU · 523&nbsp;GiB | 56&nbsp;s / 74&nbsp;s | 12.4&nbsp;MiB | 86% | [notes](spark/events-1mrps-uniform/2026-10-22/02-12-21Z/README.md) |
| Flink | Flink maintenance sidecar | 1&nbsp;×&nbsp;JM<br>18&nbsp;×&nbsp;TM<br>1&nbsp;×&nbsp;maintenance&nbsp;JM<br>24&nbsp;×&nbsp;maintenance&nbsp;TM | 422&nbsp;vCPU · 1326&nbsp;GiB | 35&nbsp;s / 61&nbsp;s | 33.0&nbsp;MiB | 5% | [notes](flink/events-1mrps-uniform/2026-10-22/05-02-06Z/README.md) |

_Generated by `results-table` from the results in this directory with harness 0.1.0. Do not edit by hand._
