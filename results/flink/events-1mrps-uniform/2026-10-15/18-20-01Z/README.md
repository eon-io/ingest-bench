# Flink: standard events uniform, ingest only

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates.

- Freshness p50 / p95 / max: 34.1 / 61.1 / 67.0 s, within the track's 90 s p95 and 120 s max bounds.
- All 60 gate checks passed. The table drained 58.6 s after the offer.
- 61 commits, one per 60 s checkpoint.

The fleet is **253 vCPU and 652 GiB**: one JobManager and 18 TaskManagers.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Flink 2.2.1 with the Iceberg 1.12.0 runtime and its v1 `FlinkSink`, image `3a54bbb`.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-flink.yaml).
The run is in the tuned tier.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| JobManager | 1 | 1 | 4 | m8g.2xlarge, shared with harness pods |
| TaskManager | 18 | 14 | 36 | m8g.4xlarge, one per node |

Settings that differ from Flink and Iceberg defaults:

- The v1 `FlinkSink` (`table.exec.iceberg.use-v2-sink: false`).
- 60 s checkpoints with a 2 s minimum pause.
- Hash distribution over 612 writer subtasks (34 slots per TaskManager), so the busiest writer holds 4 of the 512 keys.
- 96 Kafka sources.
- 1 GiB of network memory for about 6,500 shuffle channels per TaskManager, and 2 GiB of task off-heap memory for the Kafka fetchers' direct buffers.
- No managed memory: the job keeps no keyed state.
- zstd level 1 Parquet and a 512 MiB target file size.

Kafka ran on six brokers (c8gn.8xlarge, 15 vCPU and 57 GiB each, 3000 GiB gp3 at 1000 MiB/s), replication factor 3.
The run was not producer-bound: producers fell at most 213 ms behind.

## Capacity

Probes on the same corpus set the fleet shape:

- 28 TaskManagers of 13 vCPU fell behind. With hash distribution the busiest writer held 5 keys and stayed 100 % busy.
- 18 TaskManagers of 14 vCPU with 34 slots kept up in a 20-minute probe. The busiest writer was 87-95 % busy.
- Smaller fleets of this shape were not run.

In this hour TaskManagers used a median 8.2 of their 14 cores, at most 11.9.
Checkpoints completed in a median 3.1 s end to end.

## Memory and geometry

No pod restarted or was OOM-killed.
TaskManagers peaked at 33.7 GiB of their 36 GiB. The JobManager peaked at 1.4 GiB.

The final table held 31,232 files with a median of 33.0 MiB. 3.3 % were under 32 MiB.
Each commit wrote one file per key.
