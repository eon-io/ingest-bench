# Spark: standard events skewed, with the Spark rewrite sidecar

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact as well.

- Freshness p50 / p95 / max: 56 / 74 / 77 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed. The table drained 61 s after the offer.
- 92 ingest commits, one per 40 s micro-batch, and 31 rewrite commits.

The fleet is **270 vCPU and 523 GiB**: the ingest job's driver and 19 executors, plus the sidecar's driver and 19 executors.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Spark 4.1.3 Structured Streaming and Spark rewrite jobs with the Iceberg 1.12.0 runtime, image `ec46574`.
See the [run spec](../../../../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-spark-spark.yaml).
The run is in the tuned tier. Ingest uses the settings of the [ingest-only run](../../2026-10-21/22-45-14Z/README.md).

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| Ingest driver | 1 | 2 | 5 | m8g.2xlarge, shared with harness pods |
| Ingest executor | 19 | 7 | 11 | c8gn.4xlarge, two per node |
| Sidecar driver | 1 | 2 | 5 | m8g.2xlarge, shared with harness pods |
| Sidecar executor | 19 | 7 | 16 | m8g.4xlarge, two per node |

Sidecar executor memory is a 14 GiB heap plus 2 GiB of overhead.

The sidecar runs Iceberg's `rewrite_data_files` (bin-pack) every 600 s:

- Files under 256 MiB are candidates, and a group needs at least 8 of them. Output files target 512 MiB.
- File groups of up to 2 GiB, 133 at once so every executor slot has work, largest first.
- Partial progress with up to 6 commits per pass and no failed commits allowed.

Kafka ran on six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 at 1000 MiB/s), replication factor 3.
The run was not producer-bound: producers fell at most 212 ms behind.

## Compaction

Ingest wrote about 282 MB/s of small files (median 10 MiB). Every pass with data rewrote them:

| Pass | Duration | Rewritten | Files in | Files out |
|---|---:|---:|---:|---:|
| 2 (first data) | 272 s | 123 GB | 14,550 | 645 |
| 3 | 520 s | 244 GB | 25,065 | 799 |
| 4 | 495 s | 265 GB | 25,020 | 786 |
| 5 | 533 s | 274 GB | 24,959 | 781 |
| 6 | 500 s | 266 GB | 24,914 | 778 |

- Passes 3 to 6 rewrote 470 to 535 MB/s, and their size leveled off at 244 to 274 GB.
- Each pass finished 67 to 105 s before the next one started, so the sidecar kept up with room to spare.
- Live files moved in a sawtooth between about 21,000 and 37,000, with no upward trend.
- Sidecar executors used a median 6 of their seven cores.
- Across the hour the run wrote about 2 bytes per ingested byte, and rewrites were 56% of all written bytes.
- The pass in progress at the end committed its first 132 GB. The harness then stopped the sidecar with ingest, so the rest of that pass and the files written since stayed uncompacted.

Ingest was unaffected by the rewrites. Batches took a median 36 s of the 40 s trigger, at most 37 s, and commits landed at most 41 s apart.

## Memory and geometry

No pod restarted or was OOM-killed.
Ingest executors peaked just under their 11 GiB limit because shuffle page cache is charged to the pod. Sidecar executors peaked at 15 GiB of 16 GiB, and the sidecar driver at 4 GiB.

The final table held 26,593 files in 1,015 GB. By count, 91% were under 32 MiB, because the newest ten minutes of
ingest are always uncompacted. 2,192 files of 128 to 512 MiB came from rewrites.
