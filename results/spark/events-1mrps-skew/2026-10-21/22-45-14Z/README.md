# Spark: standard events skewed, ingest only

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates.

- Freshness p50 / p95 / max: 56 / 74 / 79 s, within the track's 90 s p95 and 120 s max bounds.
- All 59 gate checks passed. The table drained 44 s after the offer.
- 91 commits, one per 40 s micro-batch.

The fleet is **135 vCPU and 214 GiB**: one driver and 19 executors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Spark 4.1.3 Structured Streaming with the Iceberg 1.12.0 runtime, image `ec46574`.
See the [run spec](../../../../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-spark.yaml).
The run is in the tuned tier.

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
The run was not producer-bound: producers fell at most 211 ms behind.

## Capacity

A 40-minute probe on the same corpus and fleet set the size:

- 19 executors finished 40 M-row batches in 36 to 37 s, 1.09 times the offer, with p95 freshness of 74 s.
- 18 executors were not run on this corpus: the batch model puts them at 1.04 times the offer, under the 1.05 bar.

In the hour, batches took a median 37 s of the 40 s trigger, at most 38 s.
Each commit carried about 40 M rows, and commits landed a median 40 s apart, at most 61 s.
Executors used a median 5 of their seven cores.

## Memory and geometry

No pod restarted or was OOM-killed.
Executors peaked just under their 11 GiB limit because shuffle page cache is charged to the pod.
The driver peaked under 2 GiB.

The final table held 147,677 files in 1,247 GB, with a median of 10 MiB and all under 32 MiB.
Each commit wrote about 1,640 files, because hot keys split into several write tasks.
