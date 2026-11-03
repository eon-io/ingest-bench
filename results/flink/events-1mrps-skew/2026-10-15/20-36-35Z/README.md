# Flink: standard events skewed, ingest only

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates.

- Freshness p50 / p95 / max: 35.9 / 63.0 / 113.1 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed. The table drained 8.9 s after the offer.
- 61 commits, one per 60 s checkpoint.

The fleet is **239 vCPU and 956 GiB**: one JobManager and 34 TaskManagers.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Flink 2.2.1 with the Iceberg 1.12.0 runtime and its v1 `FlinkSink`, image `3a54bbb`.
See the [run spec](../../../../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink.yaml).
The run is in the tuned tier.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| JobManager | 1 | 1 | 4 | m8g.2xlarge, shared with harness pods |
| TaskManager | 34 | 7 | 28 | m8g.4xlarge, two per node |

Settings that differ from Flink and Iceberg defaults:

- The v1 `FlinkSink` (`table.exec.iceberg.use-v2-sink: false`).
- 60 s checkpoints with a 2 s minimum pause.
- Range distribution over 170 writer subtasks (5 slots per TaskManager).
- 96 Kafka sources.
- 3 GiB of JVM overhead and 512 MiB of network memory.
- No managed memory: the job keeps no keyed state.
- zstd level 1 Parquet and a 512 MiB target file size.

Kafka ran on six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 at 1000 MiB/s), replication factor 3.
The run was not producer-bound: producers fell at most 234 ms behind.

## Capacity

Until range distribution has statistics, each writer keeps a file open for every key. The first two commits
therefore added 86,777 and 87,040 files, and lag peaked near 130 s before the backlog drained.
After that, commits added about 681 files each.

Probes on the same corpus set the fleet shape:

- 28 TaskManagers with 5 writers each fell about 6 % short, with the busiest writer 100 % busy.
- 14 TaskManagers with 14 writers each ran out of heap during calibration, which holds all 512 files per writer.
- 34 TaskManagers with 5 writers each kept up in a 12-minute probe and drained the calibration backlog at 1.2 times the offer.

In this hour TaskManagers used a median 4.5 of their 7 cores, at most 6.9.
Checkpoints completed in a median 4.1 s end to end, at most 43 s during calibration.

## Memory and geometry

No pod restarted or was OOM-killed.
TaskManagers peaked at 26.2 GiB of their 28 GiB. The JobManager peaked at 1.4 GiB.

The final table held 213,996 files with a median of 0.1 MiB. 96 % were under 32 MiB, nearly all from the two calibration commits.
