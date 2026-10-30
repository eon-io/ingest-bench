# Spark: standard events uniform, ingest only

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates.

- Freshness p50 / p95 / max: 55 / 74 / 77 s, within the track's 90 s p95 and 120 s max bounds.
- All 59 gate checks during the offer passed. The table drained 36 s after the offer.
- 91 commits, one per 40 s micro-batch.

The fleet is **135 vCPU and 214 GiB**: one driver and 19 executors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Spark 4.1.3 Structured Streaming with the Iceberg 1.12.0 runtime, image `ec46574`.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-spark.yaml).
The run is in the tuned tier, with the same settings and fleet as the [skew run](../../../events-1mrps-skew/2026-10-21/22-45-14Z/README.md).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Driver | 1 | 2 | 5 | m8g.2xlarge, shared with harness pods |
| Executor | 19 | 7 | 11 | c8gn.4xlarge, two per node |

Executor memory is an 8 GiB heap plus 3 GiB of overhead.

Settings that differ from Spark and Iceberg defaults:

- A 40 s processing-time trigger with `maxOffsetsPerTrigger` 60 M rows, 1.5 times one trigger of offer. Freshness tracks the trigger plus the batch, so a shorter trigger is the main lever for p95.
- Hash distribution, with Iceberg's default fanout writer and no per-task sort.
- 2000 shuffle partitions, with adaptive execution splitting hot keys into write tasks of about 64 MiB.
- Kafka `minPartitions` 532, four full waves of map tasks on the 133 executor cores.
- Kafka polls of up to 20,000 records, with an 8 MiB partition fetch and a 1 MiB receive buffer.
- The Kafka source's fetched-data cache expires after 30 s, swept every 10 s.
- Driver periodic GC every 2 min, so each batch's shuffle files are freed.
- Explicit memory overhead (3 GiB per executor) instead of the 40 % PySpark default.
- zstd level 1 Parquet and a 512 MiB target file size.

Kafka ran on six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 at 1000 MiB/s), replication factor 3.
The run was not producer-bound: producers fell at most 215 ms behind.

## Capacity

The fleet is the skew run's, sized by a 40-minute skew probe.
In the hour, batches took a median 36 s of the 40 s trigger, at most 38 s, 1.11 times the offer.
By the batch model, 18 executors would take about 38 s, 1.05 times the offer, at the edge of the bar.
Each commit carried about 40 M rows, and commits landed a median 40 s apart, at most 61 s.
Executors used a median 5 of their seven cores.

## Memory and geometry

No pod restarted or was OOM-killed.
Executors peaked just under their 11 GiB limit because shuffle page cache is charged to the pod.
The driver peaked under 2 GiB.

The final table held 112,010 files in 1,236 GB, with a median of 12 MiB and all under 32 MiB.
Each commit wrote about 1,240 files, fewer than on the skewed corpus because no hot key splits across write tasks.
