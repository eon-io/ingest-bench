# Flink engine

The engine runs Apache Flink 2.2.1 SQL with released Kafka, Avro, and Iceberg
connectors. `knobs.py` renders SQL, settings, and deployment files. The
[Java runner](sql-runner/src/main/java/org/ingestbench/flink/SqlRunner.java)
submits the SQL and settings through the Flink Table API.

**On this page:** [Local runs](#local-runs) · [Kubernetes](#kubernetes-runs) · [Knobs](#knobs) · [Encoding](#catalog-and-wire-encoding)

## Runtime dependencies

| Piece | What it is |
|---|---|
| Image | `flink:2.2.1-scala_2.12-java17` |
| SQL runner | `sql-runner.jar`, built and tested by the engine Dockerfile |
| Maintenance runner | `maintenance-runner.jar`, the [`flink-rewrite`](../../docs/run-spec.md#flink-rewrite) compaction sidecar's job, built and tested by the same Dockerfile |
| Source | `flink-connector-kafka:5.0.0-2.2` + `kafka-clients:4.2.0` and its codecs (`zstd-jni:1.5.6-10`, `at.yawk.lz4:lz4-java:1.10.1`, `snappy-java:1.1.10.7`) |
| Avro | `flink-sql-avro-confluent-registry:2.2.1`; one shaded jar registers both `avro` and `avro-confluent` |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier includes AWS SDK v2) |
| Sink | `iceberg-flink-runtime-2.2:1.12.0` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Classpath | `hadoop-client-api:3.3.6` + `hadoop-client-runtime:3.3.6` — the shaded client pair, not `hadoop-common` and siblings |
| Checkpoints | `ENABLE_BUILT_IN_PLUGINS=flink-s3-fs-hadoop-2.2.1.jar` |

The shaded Hadoop client jars supply Flink's Hadoop dependencies. Kafka must
remain unshaded so MSK's `IAMClientCallbackHandler` can load the expected
interface. See `Dockerfile` for the classpath constraints.

## Local runs

The `flink` Compose profile starts the cluster; `flink-job` submits one run
detached. Set `RUN_DIR` to the staged directory so the submitter can mount it.

Catalog and Kafka properties appear in `job.sql`. Use `${env:NAME}` for
credentials so the submitter resolves them from the container environment
instead of writing values into the rendered file. See
[pitfalls](../../docs/pitfalls.md).

The runner requires `--sql PATH` and `--conf PATH`. It reads both files at
submission time, so configuration changes do not require rebuilding the image.
Scripts use a semicolon followed by a newline to separate statements and end
with the ingestion `INSERT`. Compose submits detached (`flink run -d`); use
`--wait` with an attached submission to wait for the final statement to finish.

See the [recorded local smoke](../../docs/examples/smoke-flink/) for integration
evidence; it does not establish engine performance.

## Kubernetes runs

Sites with a `kubernetes` block also render `flinkdeployment.yaml` and
`flink-job-configmap.yaml`, using the tag of the pushed image. The ConfigMap
mounts `job.sql` and `flink-conf.yaml` at `/opt/bench/run`.

The deployment uses `mode: standalone` to honor `taskmanagers` instead of deriving it from parallelism.
Engine pods preserve the site's node selector, including any architecture selection.

## Knobs

| Knob | Default | Effect |
|---|---|---|
| `taskmanagers` | required | TaskManager count |
| `slots` | required | slots per TaskManager |
| `tm_cpu` | required | vCPU per TaskManager; Kubernetes CPU request |
| `tm_mem_mb` | required | `taskmanager.memory.process.size` |
| `jm_cpu` | `1` | JobManager vCPU; Kubernetes CPU request |
| `jm_mem_mb` | `2048` | `jobmanager.memory.process.size` |
| `checkpoint_interval` | required | `execution.checkpointing.interval` |
| `min_pause` | `0s` | `execution.checkpointing.min-pause` |
| `unaligned_checkpoints` | `false` | `execution.checkpointing.unaligned.enabled` |
| `source_parallelism` | `1` | Kafka readers; must be `<=` topic partitions |
| `max_parallelism` | `4 * taskmanagers * slots` | `pipeline.max-parallelism` |
| `distribution_mode` | required | `none` / `hash` / `range`, as a sink hint |
| `write_parallelism` | `taskmanagers * slots` | the Iceberg writer parallelism, as the `write-parallelism` sink hint; set it when the fleet also holds slots for `maintenance` |
| `maintenance` | unset | Iceberg's `RewriteDataFiles` table maintenance inside the job, after each commit; see below. Requires `compaction: {driver: inline}`, and `inline` on a Flink run requires it |
| `machine_type` | unset | default fleet machine type for reporting and pricing. Site settings control placement |
| `jm_machine_type` | `machine_type` | JobManager machine type for reporting and pricing. Does not change placement |
| `extra_flink_conf` | `{}` | applied last, so it overrides anything above |

### `maintenance`

Iceberg 1.12 attaches a maintenance topology to the SinkV2 sink's commit stream
(`table.exec.iceberg.use-v2-sink` is set for you): trigger manager, bin-pack
planner, rewrite runner and committer, the classes Spark's `rewrite_data_files`
uses. The block renders write options on the INSERT hint, where Iceberg reads the
planner's options.

| Key | Default | Effect |
|---|---|---|
| `rewrite_parallelism` | required | file groups rewritten at once, one per runner subtask (`flink-maintenance.parallelism`) |
| `slot_sharing_group` | `maintenance` | the runner uses a separate slot-sharing group; the fleet needs `max(source_parallelism, write_parallelism) + rewrite_parallelism` slots |
| `schedule_interval_s` | `120` | the only trigger; Iceberg's commit-count, file-count and size schedules are pushed out of reach |
| `rate_limit_s` | `60` | minimum spacing of passes; must not exceed the interval, or it sets the cadence |
| `lock_check_delay_s` | `10` | how soon a trigger that found a pass running looks again |
| `rewrite_options` | `{}` | planner and committer options as strings, `min-input-files`, `min-file-size-bytes`, `max-file-group-size-bytes`, `rewrite-all`, `partial-progress.enabled`, ... rendered as `flink-maintenance.rewrite.<key>`. No target: Iceberg reads the table's `write.target-file-size-bytes` |

A file group is rewritten inside one operator call, so a checkpoint barrier
arriving mid-group waits for it, and so does the sink commit behind it.

The benchmark requires `EXACTLY_ONCE` checkpointing. Verification rejects a
weaker mode, including one set through `extra_flink_conf`. CPU knobs size
Kubernetes requests and contribute to cost; local Compose does not apply them.

## Upsert runs

With `scoring.exactness: LATEST_PER_KEY`, the INSERT adds the `upsert-enabled` sink hint. Each record
replaces its key's live row through an equality delete, and a version arriving in the same checkpoint
replaces the earlier one through a position delete. The equality fields are the table's identifier
fields, which staging sets when it creates or registers the table.

On `table.format_version: 3` the job also converts those equality deletes into deletion vectors with
Iceberg's in-job maintenance (`flink-maintenance.convert-equality-deletes.enabled`). The sink commits
to a `staging` branch, and the converter promotes each staging commit's data files, with deletion
vectors in place of its equality deletes, to `main`, which readers and the scorer use. So `main`
never holds equality deletes, and its freshness includes the conversion.
- Converting in place on the branch the sink keeps committing to fails every cycle on conflicting files.
- The converter handles one staging commit per run, so its rate limit drops from 60 s to 1 s
  (`flink-maintenance.rate-limit-seconds`) to keep up with a commit per checkpoint.
- It runs on the SinkV2 sink (`table.exec.iceberg.use-v2-sink`) and keeps a key index of the live rows
  in Flink state, held in RocksDB with incremental checkpoints. Its state, CPU and memory are part of
  the fleet the run measures. The deployment raises the key index's logger to WARN: at INFO it logs
  a line for every key it first sees.
- It rewrites no data files and compacts nothing.

- `distribution_mode` must be `hash`. An equality delete removes only rows with an older sequence
  number, so two writers holding versions of one key in the same checkpoint would both stay live.
  Hashing on `partition_key`, which derives from the key, gives every key one writer.
- The reference tier holds a format version 3 table whose only properties set zstd level 1 Parquet.
  On version 2 the table keeps its equality deletes, which makes the run `tuned`.

## Reference tier

A `reference` run sets `REFERENCE_KNOBS` from [`knobs.py`](knobs.py) (60 s checkpoints,
2 s minimum pause, hash distribution) and varies only `SIZING_KNOBS`, the fleet, parallelism
and machine knobs. Anything else makes it `tuned`. Start from [`aws-1mrps-skew-flink-ref.yaml`](../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-ref.yaml).

## Reader and writer parallelism

The pinned Kafka connector has no `scan.parallelism`. The renderer sets
`parallelism.default` to `source_parallelism` and a `write-parallelism` sink
hint for a different writer count. Writers default to `taskmanagers * slots`
unless `write_parallelism` is set. Dynamic table options are enabled in the
pinned Flink version. Verification checks the live graph, including the
single-parallelism commit path described below.

## Staging verification

`verify.py` reads the JobManager REST API through a port-forward on Kubernetes
or the Compose network locally. It checks:

- A RUNNING attempt named for the run.
- Checkpoint interval and minimum pause, including explicit configuration
  overrides, plus exactly-once mode.
- Sources at `source_parallelism`, each Iceberg writer at `write_parallelism`,
  and the committer at 1. Under the SinkV2 sink the committer vertex runs at the
  writer parallelism and the pre-commit aggregator, Iceberg's single commit path,
  at 1. With `maintenance`, the rewrite runner at `rewrite_parallelism` and the
  trigger manager, planner, rewrite committer and lock remover at 1.
- The requested number of registered TaskManagers.

Exit code 3 reports drift. Exit code 2 means the response could not be read;
cluster staging retries it three times.

## Runner tests

With JDK 17 and Maven installed, run the tests and build the JAR from the
repository root:

```bash
mvn -f engines/flink/sql-runner/pom.xml verify
mvn -f engines/flink/maintenance-runner/pom.xml verify
```

## Catalog and wire encoding

Only Iceberg REST catalogs are supported. Flink renders `'type' = 'iceberg'`
and `'catalog-type' = 'rest'`. Kafka security properties become `properties.*`
options, with MSK IAM translation. See the
[run specification](../../docs/run-spec.md) for translated keys.

The source DDL uses these format options for each wire encoding:

| `kafka.value_encoding` | `'format'` | Options rendered beside it |
|---|---|---|
| `avro` (the default) | `avro` | `'avro.timestamp_mapping.legacy' = 'false'` |
| `json` | `json` | `'json.timestamp-format.standard' = 'ISO-8601'`, missing fields fail, parse errors fail |
| `confluent` | `avro-confluent` | `'avro-confluent.url'` from `site.kafka.schema_registry.url`, plus `'avro-confluent.basic-auth.credentials-source' = 'USER_INFO'` and `'avro-confluent.basic-auth.user-info'` where the site names a credential |

Confluent decoding looks up writer schemas by the ID in each five-byte header,
so both staging and the renderer require a registry. The shipped
`specs/smoke/smoke-flink-confluent.yaml` differs from `smoke-flink.yaml` only in encoding.

JSON uses the bundled `flink-json` connector and the [corpus JSON
representation](../../docs/corpus.md#json-mode). Try the [JSON smoke](../../docs/running.md#encoding-variants).

The source DDL takes its types from the [column kind registry](../../docs/corpus.md#schemas).
It declares `NOT NULL` wherever the corpus column or nested field is required,
so the Avro reader schema Flink derives from the DDL matches the corpus unions.
Flink cannot write `uuid` columns, and it cannot read a required field inside a
nullable struct from plain Avro. See [engine support](../../docs/corpus.md#engine-support).

### Timestamp precision

The source uses `TIMESTAMP(3)`, the highest precision Confluent Avro can plan,
preserving milliseconds in Iceberg's microsecond timestamp. Plain Avro maps to
`local-timestamp-millis`; Confluent Avro maps to `timestamp-millis`. Both encode
the same `long` and resolve by field name. Confluent format rejects the plain
format's `avro.timestamp_mapping.legacy` option; supplying a schema does not
bypass DDL conversion.
