# Spark: 50 MB/s upsert orders, ingest only

The one-hour run kept the newest version of every order, verified against the corpus at every sampled snapshot.

- Freshness p50 / p95 / max: 52 / 81 / 93 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed.
- The table drained 82 s after the offer.

The fleet is **22 vCPU and 81 GiB**: one driver and five executors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Spark 4.1.3 with the Iceberg 1.12.0 runtime, image `d5a26ba`.
See the [run spec](../../../../../specs/tracks/events-50mbs-upsert-orders/aws-50mbs-upsert-orders-spark.yaml).
The run is in the tuned tier: its trigger is 50 s, not the reference 60 s, and it sets four Spark options.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Driver | 1 | 2 | 4 heap, about 6 with Spark's overhead | m8g.4xlarge |
| Executor | 5 | 4 | 15 (12 heap + 3 overhead) | m8g.4xlarge |

- Each micro-batch keeps every key's newest row, then runs a guarded `MERGE` into a merge-on-read v3 table, so deletes are deletion vectors.
- 50 s trigger, hash distribution, zstd level 1 Parquet.
- `spark.locality.wait` is 0 s, so tasks never wait for a preferred executor.
- `spark.sql.shuffle.partitions` is 40, twice the cores.
- Adaptive skew-join splitting starts at 64 MiB partitions that are twice the median, against defaults of 256 MiB and five times.
- Engine nodes had 300 GiB gp3 volumes at 500 MB/s and 6000 IOPS for shuffle.

## Capacity

- Freshness is about the wait for the next trigger plus the batch. `MERGE` batches grow through the hour as the current hour's partition grows, and fewer rows per batch barely shorten them. So the trigger sets the wait, and the executors set how long the batch takes.
- The last batches took 40-43 s of the 50 s trigger. Mid-hour batches ran 29-38% faster than the same fleet without the four options.
- The 40-task join stage had no straggler: its tasks took 0.4-0.8 s. All five executors stayed evenly busy.
- Smaller or plainer configurations ran the full hour:

| Fleet and settings | Freshness p50 / p95 / max | Outcome |
|---|---|---|
| 4 × 4, 60 s trigger (reference) | 62 / 94 / 106 s | p95 over the 90 s bound |
| 4 × 4, 50 s trigger | 60 / 100 / 182 s | over both bounds. Batches neared 50 s by minute 62, and one took 118 s |
| 5 × 4, 50 s trigger | 61 / 88 / 100 s | within the bounds, but the last batches took 46-49.6 s |

- Without the four options, two of the five executors stayed nearly idle. One task of a 20-task join stage took the stage's whole 19 s, so a fifth executor shortened only the table scan.
- A shorter trigger costs little in `MERGE` work: at 50 s, batches ran 2-10% longer than at 60 s at the same minute.
- The four options have not been tried on 4 × 4 at a 50 s trigger.

## Table

- 481 live files, 130 GB. Median file 298 MiB, and 7% of files are under 32 MiB.
- The table holds about four rows for each live row.

## Setup

- Kafka: 48 partitions, `event_id` keys, corpus partition assignment, Confluent-framed Avro, lz4.
- Three brokers requested 6 vCPU and 24 GiB each on m8g.2xlarge nodes, with 1000 GiB gp3 volumes at 400 MiB/s.
- Four producer shards. The producer was never the bottleneck.
