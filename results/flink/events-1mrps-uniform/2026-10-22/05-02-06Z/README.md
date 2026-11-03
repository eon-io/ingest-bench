# Flink: standard events uniform, with Flink table maintenance

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact in the cluster.

- Freshness p50 / p95 / max: 34 / 61 / 67 s, within the track's 90 s p95 and 120 s max bounds, and the same as the [ingest-only run](../../2026-10-15/18-20-01Z/README.md).
- All 60 gate checks passed. The table drained 40 s after the offer.
- 61 ingest commits, one per 60 s checkpoint, and 5 rewrite commits.

The fleet is **422 vCPU and 1326 GiB**: the ingest-only fleet plus a maintenance job with one JobManager and 24
TaskManagers.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Flink 2.2.1 with the Iceberg 1.12.0 runtime, image `485b5fe`.
See the [run spec](../../../../../specs/tracks/events-1mrps-uniform/aws-1mrps-unif-flink-compact.yaml).
The run is in the tuned tier. Ingest uses the settings of the ingest-only run.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| JobManager | 1 | 1 | 4 | m8g.2xlarge, shared with harness pods |
| TaskManager | 18 | 14 | 36 | m8g.4xlarge, one per node |
| Maintenance JobManager | 1 | 1 | 2 | m8g.4xlarge |
| Maintenance TaskManager | 24 | 7 | 28 | m8g.4xlarge, two per node |

Compaction runs as a separate Flink job, Iceberg's Flink table maintenance (`RewriteDataFiles`), with its own
JobManager and TaskManagers:

- 168 rewrite slots, seven per TaskManager.
- Files under 256 MiB are candidates, and a group needs at least 8 of them. Output files target 512 MiB.
- File groups of up to 1 GiB, largest first.

## Compaction

| Pass | Interval | Fresh since last pass | Written | Files out |
|---|---:|---:|---:|---:|
| 2 | 826 s | 248 GB | 252 GB | 512 |
| 3 | 537 s | 159 GB | 196 GB | 512 |
| 4 | 390 s | 106 GB | 147 GB | 512 |
| 5 | 475 s | 142 GB | 131 GB | 512 |

- The first pass, at 14 minutes, compacted the backlog from the start of ingest.
- After the long second pass, intervals fell to 390 to 540 s, so compaction kept up.
- Across the hour the run wrote 2 bytes per ingested byte, and rewrites were 48% of all written bytes.
- Compaction did not slow ingest: commits landed every 60 s and lag matched the ingest-only run.

Uniform ingest uses hash distribution, so it has none of the range-calibration startup that sets the skewed
run's freshness.

## Memory and geometry

No pod restarted or was OOM-killed.

The final table held 10,240 files in 1,001 GB. Median file 33 MiB, and 5% of files are under 32 MiB.
2,048 files of 128 to 512 MiB came from rewrites.
