# Flink: 50 MB/s upsert orders, ingest only

The one-hour run kept the newest version of every order, verified against the corpus at every sampled snapshot.

- Freshness p50 / p95 / max: 60 / 88 / 99 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed.
- The table drained 62 s after the offer.

The fleet is **13 vCPU and 40 GiB**: one JobManager and three TaskManagers.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Flink 2.2.1 with the Iceberg 1.12.0 runtime and its SinkV2 `IcebergSink`, image `d5a26ba`.
See the [run spec](../../../../../specs/tracks/events-50mbs-upsert-orders/aws-50mbs-upsert-orders-flink-ref.yaml).
The run is in the reference tier.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| JobManager | 1 | 1 | 4 | m8g.4xlarge |
| TaskManager | 3 | 4 | 12 | m8g.4xlarge |

- The upsert sink writes equality deletes, and the job converts them into deletion vectors before readers see them.
- 60 s checkpoints, hash distribution, zstd level 1 Parquet.
- 12 sources, one per slot. 64 key groups.

## Capacity

- Hash distribution keys rows by partition, so one writer takes the current hour's partition, about 91% of the rows.
- That writer leveled off at 500-527 ms busy per second. Checkpoints took 5-7 s.
- Larger fleets also ran the full hour and verified:

| Fleet | vCPU / GiB | Freshness p95 |
|---|---|---:|
| 3 × 4 / 16 GiB | 13 / 52 | 91 s |
| 4 × 4 / 12 GiB | 17 / 52 | 80 s |
| 4 × 4 / 16 GiB | 17 / 68 | 80 s |

- Two smaller fleets, each one step down in a single dimension, were rejected. They stopped after two consecutive gate checks found the backlog rising, so they have no final freshness or verification.

| Fleet | vCPU / GiB | Stopped at | Gate checks passed |
|---|---|---:|---:|
| 2 × 4 / 12 GiB | 9 / 28 | minute 42 | 38 |
| 3 × 4 / 8 GiB | 13 / 28 | minute 9 | 7 |

- Both failed in the key index that converts equality deletes into deletion vectors. The writer stayed at its usual load in both.
- With two TaskManagers, the backlog rose from 4.8 million to 7.5 million rows. The key index ran 780-920 ms busy per second, against about 300 with three, and TaskManagers reached their 4 vCPU limit. Checkpoints grew from 7 s to 85 s, past the 60 s interval. Sources dropped from 12 to 8 to fit eight slots.
- With 8 GiB TaskManagers, the backlog rose from 1.7 million to 6.7 million rows. The key index reached 1000 ms busy per second by minute 6, and a checkpoint took 78 s. The smaller budget leaves RocksDB too little cache for the key index. No TaskManager ran out of memory or restarted.

## Memory

- All 76 checkpoints completed, and the job never restored from one.
- TaskManager memory held flat near 11 GiB. Flink sizes its heap and RocksDB memory from the container size, so usage fills the budget at any size.
- The last checkpoints held 4-5 GB of state across the three TaskManagers, mostly the key index of about 38 million live keys.

## Table

- 397 live files, 175 GB. Median file 513 MiB, and 6% of files are under 32 MiB.
- 45 deletion-vector files. The table holds about five rows for each live row.

## Setup

- Kafka: 48 partitions, `event_id` keys, corpus partition assignment, Confluent-framed Avro, lz4.
- Three brokers requested 6 vCPU and 24 GiB each on m8g.2xlarge nodes, with 1000 GiB gp3 volumes at 400 MiB/s.
- Four producer shards. The producer was never the bottleneck.
