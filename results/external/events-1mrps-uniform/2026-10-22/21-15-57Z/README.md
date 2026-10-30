# Eon Data Engine: standard events uniform, with integrated compaction and source dedup

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact in the cluster.

- Freshness p50 / p95 / max: 59 / 74 / 79 s, within the track's 90 s p95 and 120 s max bounds.
- Commits landed every 30 s, and no two were more than 32 s apart. The table drained 50 s after the offer.
- 120 ingest commits and 108 compaction commits.

The fleet is **63 vCPU and 228 GiB**, the same as the [skewed run](../../../events-1mrps-skew/2026-10-22/20-03-43Z/README.md):
the [ingest-only fleet with source dedup](../23-38-24Z/README.md) plus seven compactors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).
The run is in the tuned tier.

## Configuration

Eon Data Engine version `4aae9978` ran through the harness's external-engine interface.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-eon-compact-dedup.yaml).

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

- Writers averaged 69% to 78% of their 5 cores, and readers 56% to 61% of their 2 cores. The committer averaged 14% of its 2 cores.
- Compactors averaged 48% to 50% of their 4 cores.

## Compaction

- Compaction tasks came in bursts every 4 to 5 minutes, and each burst finished within 5 minutes.
- Rewrites were 62% of all bytes the run wrote.
- Compared with the [Spark run with the rewrite sidecar](../../../../spark/events-1mrps-uniform/2026-10-22/02-12-21Z/README.md)
  on the same corpus, the compaction fleet is 28 vCPU against 135. Spark's live files moved between about 13,000 and
  24,000. This table ended at 5,047.

## Memory

- Readers peaked at 72% of their 5 GiB.
- The committer peaked at 10 GiB, 86% of its 12 GiB.
- Writers peaked at 24 to 26 GiB of their 28 GiB.
- Compactors peaked at 8 GiB, at or near their limit.
- No reader, writer, committer or compactor restarted, and no source data was redelivered.

## Table

- 5,047 live files, 995 GB. Median file 276 MiB, and 39% of files are under 32 MiB: the newest few minutes of ingest
  are always uncompacted.

## Setup

- Kafka: 192 partitions keyed by `partition_key` (512 keys, uniform), round-robin partition assignment, Confluent-framed Avro, lz4.
- Six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 volumes), replication factor 3.
- 69 producer shards. The producer was never the bottleneck.
