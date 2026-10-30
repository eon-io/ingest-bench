# Recorded AWS Spark smoke

This run used the setup, image-push, stage, launch, gate, teardown and finish
scripts on 2026-09-09. The gate ran once per minute.

- EKS with three amd64 `m6i.xlarge` nodes.
- Kubeflow spark-operator 2.5.2 with its webhook enabled.
- Amazon MSK: three `kafka.m5.2xlarge` brokers, Kafka 3.9.x and IAM authentication.
- Glue Iceberg REST catalog and EKS Pod Identity.
- Images tagged `e508270`; spec `specs/cluster-smoke/aws-smoke-spark.yaml`.

For cluster requirements, see [cluster sizing](../../../deploy/aws/README.md#sizing-the-cluster).

## Run and verdict

The run used two executors with two cores and 4 GiB each, a 2 GiB driver,
a 10 s trigger interval, hash distribution, four Kafka partitions, and one
producer shard. The final verdict was `run_valid: true`:

- The run drained with 5,840,896 rows across 300 scored batches; count and
  checksum checks passed.
- Post-warmup freshness was 9.682 s at p50, 22.943 s at p95, and 36.01 s
  maximum, within the 60 s p95 and 120 s maximum bounds.
- The table drained 8.418 s after the offer ended.
- Backlog peaked at 272,384 rows, and the table had 32 snapshots.

The verdict uses the `freshness.window` quantiles above. Including warmup,
`freshness.full` reports p50 9.695 s and p95 19.943 s.

## File geometry

`finish.sh` measured `geometry.json` from the final table metadata:
1,984 data files, 719,577,266 bytes (~686 MiB), p50 file
size 176,877.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold.

## Result artifacts

`run.json` contains the recorded measurements in result schema 3. Object-store
roots, the image registry and the Glue warehouse account id are replaced with
placeholders. Its `run.corpus` metadata comes from the shipped `smoke` preset
and agrees with the recorded corpus hash and scorer row count. `rows_per_s` is
that count divided by the preset duration. The result uses `EXACTLY_ONCE`; its
mode, verdict and strict exactness agree with the recorded zero fault counts.

`scripts/gate.sh` reported `PASS` on every tick. Lag stayed at or below 36 s,
within the 120 s maximum bound.
