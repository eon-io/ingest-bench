# Eon Data Engine: 50 MB/s upsert orders, ingest only

The one-hour run kept the newest version of every order, verified against the corpus at every sampled snapshot.

- Freshness p50 / p95 / max: 63 / 81 / 93 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed.
- The table drained 71 s after the offer.

The fleet is **4 vCPU and 10 GiB**.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).
The run is in the tuned tier, and compaction was parked.

## Configuration

Eon Data Engine version `d073b147` ran through the harness's external-engine interface.
See the [run spec](../../../../../specs/tracks/events-50mbs-upsert-orders/aws-50mbs-orders-eon.yaml).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Reader | 1 | 1 | 3 | m8g.4xlarge |
| Writer | 1 | 2 | 4 | m8g.4xlarge |
| Committer | 1 | 1 | 3 | m8g.4xlarge |

Main engine settings:

- Upsert on the primary key (`event_id`, `partition_key`). The row with the higher `id` is the newer version and replaces the older one. Deletes are deletion vectors on a v3 table.
- Commits every 30 s.
- Writers flush a batch at 512 MiB or after 30 s.
- zstd level 1 Parquet.
- Compaction parked and source dedup off.

## Capacity

- The writer averaged 20% of its 2 cores, the committer 28% of its core and the reader 20% of its core.
- In the hour's last ten minutes the committer took 26 s to apply each 30 s batch of changes. No commit was deferred.

## Memory

The writer peaked at 66% of its 4 GiB, the committer at 78% of its 3 GiB and the reader at 57% of its 3 GiB.

## Table

- 431 live files, 176 GB. Median file 507 MiB, and 16% of files are under 32 MiB.
- 23 deletion-vector files. The table holds about five rows for each live row.

## Setup

- Kafka: 48 partitions, `event_id` keys, corpus partition assignment, Confluent-framed Avro, lz4.
- Three brokers requested 6 vCPU and 24 GiB each on m8g.2xlarge nodes, with 1000 GiB gp3 volumes at 400 MiB/s.
- Four producer shards. The producer was never the bottleneck.
