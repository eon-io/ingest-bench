# Pitfalls

These issues can invalidate a run, distort comparisons, or leave resources running
after a failure.

## Create comparable tables before comparing writers

Put persistent table settings in `table.properties`. With
`table.managed_by: harness`, staging applies them when it creates the table and
forces Iceberg format version 2. With `table.managed_by: engine`, staging leaves
creation to the engine and supplies the requested DDL in `facts.ddl`.

Writer options do not necessarily change an existing table's properties. Use the
same table creation path and properties across runs when comparing file geometry.

## Runtime verification covers only selected settings

Staging checks managed engines after they start, but a successful check does not
prove that every setting took effect. Flink verification covers checkpoint
cadence and mode, operator parallelism, and TaskManager count. Spark verification
covers selected driver-reported settings and the running pod fleet.

Two cadence settings deserve particular care:

- Flink's `min_pause` defaults to `0s`. A nonzero pause can lengthen the time
  between checkpoints even when `checkpoint_interval` is unchanged.
- Spark's `trigger_interval` configures the writer's
  `trigger(processingTime=…)` call. The verifier cannot read this interval back;
  it rejects `spark.sql.streaming.trigger*` settings, which do not configure the
  writer's trigger.

## Size the cluster for the entire run

The scorer and each producer shard request 2 CPU, in addition to the engine
fleet. Count schedulable capacity after system reservations and existing pod
requests; a node with 4 vCPU may have room for only one 2-CPU pod.

`launch.sh` warns when available CPU appears insufficient for the scorer and
producers, but the check is best-effort and does not block launch. Pods that
cannot be scheduled stay Pending and have no application logs. Check pod events
for `FailedScheduling` and `Insufficient cpu`. Without autoscaling, provision
capacity before launch; see [cluster sizing](../deploy/aws/README.md#sizing-the-cluster).

Spark memory settings also need care: `executor_mem_mb` sets the JVM heap, while
pods need additional memory for process overhead. Local Spark runs execute in
the driver's JVM, using `driver_mem_mb`; `executor_mem_mb` does not allocate a
separate executor heap. See the [Spark engine guide](../engines/spark/README.md).

## Small files can overwhelm the scorer

When many writers touch many identity partitions, each commit can create many
small files. A higher offer rate alone does not control file count; partitioning,
writer distribution, and commit cadence also matter.

The scorer reads the ID column from each newly added file in append snapshots.
`--read-workers` controls concurrent reads, so the scorer needs enough CPU and
memory to support that concurrency. A `SLOW_POLL` log line means a poll took
longer than its configured interval. Investigate reader capacity and file counts
before trusting the verdict: the gate returns `VOID` when its latest measurement
exceeds the staleness bound.

## Retain snapshots until geometry measurement finishes

The [engine contract](adding-an-engine.md) prohibits snapshot expiry during a
run. Keep expiry disabled until `finish.sh` completes as well. It reads manifests
referenced by the final metadata document saved during teardown; saving that
document does not preserve the manifests themselves. Finish measurement before
scheduled expiry or `purge.sh` removes the required files.

## Failed launches still need cleanup

Staging creates resources before launch, and launch failures leave pods available
for diagnosis. After inspecting logs and pod events:

- Run `scripts/teardown.sh <run_id>` to remove managed engine, producer, and scorer
  resources and drop the topic.
- Run `scripts/purge.sh <run_id> --artifacts` when the table, its files, and the
  stored run artifacts are no longer needed.

Stage a fresh run for the next attempt. Reusing a topic or table can carry over
consumer offsets, checkpoint state, and rows from the failed run.

## Keep credentials out of rendered configuration

Rendered catalog and Kafka properties are stored in ConfigMaps and uploaded to
the runs prefix. Cluster sites reject literal values for credential-named
properties. Use `${env:NAME}` and the Kubernetes Secret described in
[the run specification](run-spec.md#secrets); local examples with public default
credentials cannot be copied unchanged into a cluster site.
