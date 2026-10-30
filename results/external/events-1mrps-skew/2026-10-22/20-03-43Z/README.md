# Eon Data Engine: standard events skewed, with integrated compaction and source dedup

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact in the cluster.

- Freshness p50 / p95 / max: 60 / 74 / 78 s, within the track's 90 s p95 and 120 s max bounds.
- Commits landed every 30 s, and no two were more than 32 s apart. The table drained 101 s after the offer.
- 122 ingest commits and 133 compaction commits.

The fleet is **63 vCPU and 228 GiB**: the [ingest-only fleet with source dedup](../22-27-37Z/README.md) plus seven
compactors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).
The run is in the tuned tier.

## Configuration

Eon Data Engine version `4aae9978` ran through the harness's external-engine interface.
See the [run spec](../../../../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-eon-compact-dedup.yaml).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Reader | 4 | 2 | 5 | m8g.4xlarge |
| Writer | 5 | 5 | 28 | m8g.4xlarge |
| Committer | 1 | 2 | 12 | m8g.2xlarge |
| Compactor | 7 | 4 | 8 | m8g.4xlarge |

Ingest uses the ingest-only run's settings. Compaction runs inside the engine:

- Files under 256 MiB are candidates, and a group of at least 8 qualifies with no minimum saving, as in the Spark rewrite sidecar. Output files target 512 MiB.

Source dedup is on, so source data redelivered after a restart is not committed twice.
The committer has 2 vCPU and 12 GiB for the dedup index.

## Capacity

- Writers averaged 59% to 74% of their 5 cores, and readers 55% to 60% of their 2 cores. The committer averaged 14% of its 2 cores.
- Compactors averaged 62% to 65% of their 4 cores.

## Compaction

- Compaction tasks came in bursts every 4 to 5 minutes, and each burst finished within 3 minutes.
- The run wrote almost 3 bytes per ingested byte, and rewrites were 67% of all written bytes.
- Compared with the [Spark run with the rewrite sidecar](../../../../spark/events-1mrps-skew/2026-10-22/01-03-29Z/README.md)
  on the same corpus, the compaction fleet is 28 vCPU against 135. Spark's live files moved between about 21,000 and
  37,000. This table ended at 4,050.

## Memory

- Readers peaked at 66% of their 5 GiB.
- The committer peaked at 9 GiB, 78% of its 12 GiB.
- Writers peaked at 13 to 24 GiB of their 28 GiB.
- Compactors peaked at their 8 GiB limit.
- No reader, writer, committer or compactor restarted, and no source data was redelivered.

## Table

- 4,050 live files, 993 GB. Median file 263 MiB, and 42% of files are under 32 MiB: the newest few minutes of ingest
  are always uncompacted.

## Setup

- Kafka: 192 partitions keyed by `partition_key` (512 keys, Zipf alpha 1), round-robin partition assignment, Confluent-framed Avro, lz4.
- Six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 volumes), replication factor 3.
- 69 producer shards. The producer was never the bottleneck.
