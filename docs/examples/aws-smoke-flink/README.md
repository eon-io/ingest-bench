# Recorded AWS Flink smoke

This run used the setup, image-push, stage, launch, gate, teardown and finish
scripts on 2026-09-09. The gate ran once per minute.

- EKS with three amd64 `m6i.xlarge` nodes.
- Flink Kubernetes Operator 1.15.0.
- Amazon MSK: three `kafka.m5.2xlarge` brokers, Kafka 3.9.x and IAM authentication.
- Glue Iceberg REST catalog and EKS Pod Identity.
- Images tagged `6d24375`; spec `specs/cluster-smoke/aws-smoke-flink.yaml`.

For cluster requirements, see [cluster sizing](../../../deploy/aws/README.md#sizing-the-cluster).

## Run and verdict

The run used two TaskManagers with four slots and 4 GiB each, four Kafka
partitions, and one producer shard. The final verdict was `run_valid: true`:

- The run drained with 5,840,896 rows; count and checksum checks passed.
- Freshness was 6.745 s at p50, 18.12 s at p95, and 31.645 s maximum.
  The p95 bound was 60 s.
- The table drained 6.598 s after the offer ended.
- Backlog peaked at 214,016 rows.

## File geometry

`finish.sh` measured `geometry.json` from the final table metadata:
1,984 data files, 723,633,080 bytes (~690 MiB), p50 file
size 178,340.5 bytes (~0.17 MiB), 100% of files under the 32 MiB
small-file threshold.
All five offsets in its `at` map are `absent`: the first is 600 s, beyond
this 300-second run. Only the final snapshot has geometry measurements.

## Result artifacts

`run.json` contains the recorded measurements in result schema 3. Object-store
roots, the image registry and the Glue warehouse account id are replaced with
placeholders. Its `run.corpus` metadata comes from the shipped `smoke` preset
and agrees with the recorded corpus hash and scorer row count. `rows_per_s` is
that count divided by the preset duration. The result uses `EXACTLY_ONCE`; its
mode, verdict and strict exactness agree with the recorded zero fault counts.

`scripts/gate.sh` reported one `UNDERSIZED` tick during the offer, followed by
a passing tick and a valid final verdict. The transient gate result was attributed
to its 60-second sampling windows interacting with Flink's 10-second checkpoints.
