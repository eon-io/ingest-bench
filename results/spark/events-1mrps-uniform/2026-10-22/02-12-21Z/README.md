# Spark: standard events uniform, with the Spark rewrite sidecar

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact as well.

- Freshness p50 / p95 / max: 56 / 74 / 77 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed. The table drained 55 s after the offer.
- 92 ingest commits, one per 40 s micro-batch, and 33 rewrite commits.

The fleet is **270 vCPU and 523 GiB**: the ingest job's driver and 19 executors, plus the sidecar's driver and 19 executors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Spark 4.1.3 Structured Streaming and Spark rewrite jobs with the Iceberg 1.12.0 runtime, image `ec46574`.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-spark-spark.yaml).
The run is in the tuned tier. Ingest uses the settings of the [ingest-only run](../../2026-10-21/23-28-15Z/README.md), and the
sidecar uses the settings of the [skewed run](../../../events-1mrps-skew/2026-10-22/01-03-29Z/README.md).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Ingest driver | 1 | 2 | 5 | m8g.2xlarge, shared with harness pods |
| Ingest executor | 19 | 7 | 11 | c8gn.4xlarge, two per node |
| Sidecar driver | 1 | 2 | 5 | m8g.2xlarge, shared with harness pods |
| Sidecar executor | 19 | 7 | 16 | m8g.4xlarge, two per node |

The sidecar runs Iceberg's `rewrite_data_files` (bin-pack) every 600 s. Candidates are files under 256 MiB, in groups
of at least 8, written to 512 MiB targets.

Kafka ran on six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 at 1000 MiB/s), replication factor 3.
The run was not producer-bound: producers fell at most 226 ms behind.

## Compaction

Ingest wrote about 282 MB/s of small files (median 12 MiB). Every pass with data rewrote them:

| Pass | Duration | Rewritten | Files in | Files out |
|---|---:|---:|---:|---:|
| 2 (first data) | 249 s | 123 GB | 11,160 | 512 |
| 3 | 571 s | 256 GB | 16,570 | 532 |
| 4 | 441 s | 275 GB | 22,484 | 594 |
| 5 | 399 s | 206 GB | 18,600 | 512 |
| 6 | 392 s | 206 GB | 18,600 | 512 |

- Pass 3 took 571 s, and pass 4 started 32 s late. Passes 4 to 6 then finished 160 to 210 s before the next one started.
- Live files moved in a sawtooth between about 13,000 and 24,000, and the low points fell from 19,000 to 13,000 over the last 40 minutes.
- Sidecar executors used a median 6 of their seven cores.
- Across the hour the run wrote about 2 bytes per ingested byte, and rewrites were 54% of all written bytes.
- The pass in progress at the end committed its first 106 GB. The harness then stopped the sidecar with ingest, so the rest of that pass and the files written since stayed uncompacted.

Ingest was unaffected by the rewrites. Batches took a median 36 s of the 40 s trigger, at most 39 s, and commits landed at most 41 s apart.

## Memory and geometry

No pod restarted or was OOM-killed.
Ingest executors peaked just under their 11 GiB limit because shuffle page cache is charged to the pod.
Sidecar executors peaked at 15 GiB of 16 GiB, and the sidecar driver at 4 GiB.

The final table held 16,638 files in 1,015 GB. By count, 86% were under 32 MiB, because the newest ten minutes of
ingest are always uncompacted. 2,306 files of 128 to 512 MiB came from rewrites.
