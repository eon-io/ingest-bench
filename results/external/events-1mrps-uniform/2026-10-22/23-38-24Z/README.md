# Eon Data Engine: standard events uniform, ingest only

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, verified in the cluster.

- Freshness p50 / p95 / max: 59 / 74 / 78 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed. The table drained 47 s after the offer.
- Commits landed every 30 s, and no two were more than 32 s apart.

The fleet is **35 vCPU and 172 GiB**, the same as the [skewed run](../../../events-1mrps-skew/2026-10-22/22-27-37Z/README.md).
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).
The run is in the tuned tier, and compaction was parked.

## Configuration

Eon Data Engine version `4aae9978` ran through the harness's external-engine interface.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-eon-dedup.yaml).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Reader | 4 | 2 | 5 | m8g.4xlarge |
| Writer | 5 | 5 | 28 | m8g.4xlarge |
| Committer | 1 | 2 | 12 | m8g.2xlarge |

Readers and writers ran one per node.
Main engine settings:

- Commits every 30 s.
- Writers flush a batch once its oldest row reached Kafka 45 s earlier, and keep each batch open at least 20 s.
- 64 MiB row groups and zstd level 1 Parquet.
- No dictionary encoding on 23 high-cardinality columns.
- Compaction parked.

Source dedup is on, so source data redelivered after a restart is not committed twice.
The committer has 2 vCPU and 12 GiB for the dedup index.

## Capacity

- Writers averaged 72% to 76% of their 5 cores, and readers 56% to 63% of their 2 cores. The committer averaged 8% of its 2 cores.

## Memory

- Readers peaked at 74% of their 5 GiB.
- The committer peaked at 11 GiB, 88% of its 12 GiB.
- Writers peaked at 24 to 26 GiB of their 28 GiB.
- No reader, writer or committer restarted, and no source data was redelivered.

## Table

- 50,341 live files, 1,094 GB. Median file 21 MiB, and all files are under 32 MiB, because compaction was parked.

## Setup

- Kafka: 192 partitions keyed by `partition_key` (512 keys, uniform), round-robin partition assignment, Confluent-framed Avro, lz4.
- Six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 volumes), replication factor 3.
- 69 producer shards. The producer was never the bottleneck.
