# Flink: standard events skewed, with Flink table maintenance

**This run does not pass the track's freshness bounds.** Its p95 is 106 s against the 90 s bound, and the miss
comes from range distribution's startup calibration. Once ingest was steady, from
minute 11 to the end, the worst lag in every minute was 64 to 68 s, within the bounds. The
[uniform run](../../../events-1mrps-uniform/2026-10-22/05-02-06Z/README.md), which uses hash distribution and has no
calibration, passes with compaction at a p95 of 61 s.

The one-hour run committed all 3,602,067,456 offered rows with no loss or duplicates, and the final table, after
compaction, verified exact in the cluster.

- Freshness p50 / p95 / max: 39 / 106 / 158 s.
- 59 of 60 gate checks passed. The table drained 40 s after the offer.
- 60 ingest commits, one per 60 s checkpoint, and 6 rewrite commits.

The fleet is **408 vCPU and 1630 GiB**: the [ingest-only fleet](../../2026-10-15/20-36-35Z/README.md) plus a
maintenance job with one JobManager and 24 TaskManagers.
See the [result JSON](result.json) and the [comparison table](../../../../RESULTS.md).

## Configuration

Flink 2.2.1 with the Iceberg 1.12.0 runtime, image `485b5fe`.
See the [run spec](../../../../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-compact.yaml).
The run is in the tuned tier. Ingest uses the settings of the ingest-only run.

| Role | Count | vCPU each | GiB each | Machine type |
|---|---:|---:|---:|---|
| JobManager | 1 | 1 | 4 | m8g.2xlarge, shared with harness pods |
| TaskManager | 34 | 7 | 28 | m8g.4xlarge, two per node |
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
| 2 | 443 s | 134 GB | 110 GB | 636 |
| 3 | 489 s | 141 GB | 155 GB | 653 |
| 4 | 553 s | 177 GB | 200 GB | 711 |
| 5 | 591 s | 177 GB | 211 GB | 709 |
| 6 | 588 s | 159 GB | 219 GB | 728 |

- The first pass, at 8 minutes, rewrote the small files from range calibration.
- Pass intervals rose toward about 590 s and then held, and the fresh bytes per pass stopped growing. Compaction kept up.
- Outputs under 256 MiB are compacted again in the next pass, so each pass writes more than the fresh bytes it consumes.
- Across the hour the run wrote 2 bytes per ingested byte, and rewrites were 55% of all written bytes.

## Why freshness missed the bound

Compaction did not slow ingest once ingest was steady:

- From minute 11 to the end, the worst lag in every minute was 64 to 68 s, the same as the ingest-only run.
- Ingest commits landed every 60 s through all six rewrite commits, at most 63 s apart.
- Checkpoints took about 4 s, as in the ingest-only run.

The miss comes from the first 11 minutes:

- Until range distribution has statistics, each writer keeps a file open for every key, so the first two data checkpoints wrote 87,040 and 57,693 files and took 42 s and 29 s.
- A periodic checkpoint completed 2 s before the producers started, so the first checkpoint with data came a full minute later. Ingest fell 136 million rows behind, and lag peaked at 160 s.
- The backlog drained only by minute 11. The measured window starts after 5 minutes, so it includes about 6 minutes of 100 to 160 s lag.
- The ingest-only run started its calibration checkpoints sooner, fell at most 50 million rows behind, and recovered by minute 7.

The maintenance job may also have slowed the catch-up. In minutes 3 to 6, ingest committed 51 to 68 million rows a
minute against 71 to 80 million in the ingest-only run. That is when the maintenance job planned and ran its first
pass over the calibration files, on the same table, the same S3 prefixes and the same catalog. The jobs share
nothing else.

A rerun tested both causes. It triggered the first ingest checkpoint 7 s after the producers started and held the
maintenance job back for 10 minutes. The early checkpoint did not shorten calibration: that checkpoint still wrote
86,658 files and took 107 s, and lag peaked at 261 s. Calibration costs about the same whenever it starts, so the
rerun was stopped. Range distribution's calibration cost is a startup property of the ingest job on the skewed
track, which the ingest-only run also pays inside its warm-up.

## Memory and geometry

No pod restarted or was OOM-killed.
Ingest TaskManagers peaked at 26 GiB of their 28 GiB and used a median 5 of their 7 cores.
Maintenance TaskManagers peaked at 26 GiB of 28 GiB and used a median 4 of their 7 cores.
Both JobManagers stayed under 2 GiB.

The final table held 8,329 files in 990 GB. By count, 59% were under 32 MiB, because the newest minutes of ingest are
always uncompacted. 2,006 files of 128 to 512 MiB came from rewrites.
