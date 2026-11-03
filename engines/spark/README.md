# Spark engine

The engine runs Apache Spark 4.1.3 Structured Streaming with released Kafka,
Avro, and Iceberg connectors. `knobs.py` renders the configuration, reader
schema, job document, and deployment files. `stream_to_iceberg.py` reads the
runtime files and starts one query.

**On this page:** [Local runs](#local-runs) · [Kubernetes](#kubernetes-runs) · [Knobs](#knobs) · [Encoding](#wire-encoding-and-timestamps)

## Runtime dependencies

| Piece | What it is |
|---|---|
| Image | `apache/spark:4.1.3-scala2.13-java17-python3-ubuntu`, multi-arch |
| Source | `spark-sql-kafka-0-10_2.13:4.1.3` + `spark-token-provider-kafka-0-10_2.13:4.1.3`, an unshaded `kafka-clients:3.9.1` and `commons-pool2:2.12.1` |
| Avro | `spark-avro_2.13:4.1.3`; provides `from_avro`, which the image's Avro core jar lacks |
| Kafka auth | `aws-msk-iam-auth:2.3.8` (`all` classifier includes AWS SDK v2) |
| Sink | `iceberg-spark-runtime-4.1_2.13:1.12.0` plus the `iceberg-aws-bundle` / `iceberg-gcp-bundle` cloud SDKs |
| Checkpoints | `hadoop-aws:3.4.2` + the AWS SDK v2 `bundle:2.54.17`, the SDK version Iceberg 1.12.0 is built against |

Spark already includes Kafka's zstd, lz4, and snappy codecs. The image adds
`commons-pool2` because the bundled `commons-pool` 1.x uses a different package.
The Kafka client must remain unshaded so MSK's `IAMClientCallbackHandler` can
load its `AuthenticateCallbackHandler` interface. See `Dockerfile` for the
classpath constraints.

## Local runs

The `spark` Compose profile runs one container with
`--master local[executors × executor_cores]`. All work runs in the driver's JVM,
using `driver_mem_mb`; `executor_mem_mb` does not allocate a separate heap.
Set `RUN_DIR` to the staged run directory. Compose reads `LOCAL_CORES` and
`DRIVER_MEM_MB` from its `job.env`.

Readiness requires an application named for the run and an active query in the
Structured Streaming UI. Spark's `/streaming` REST resource serves DStreams,
so the readiness check uses `/api/v1/applications` and the Structured Streaming
tab. Disabling `spark.sql.streaming.ui.enabled` prevents readiness.

Local runs check integration, not throughput: the engine shares a machine with
the harness, broker, and object store. No local Spark result is recorded here.
See the [recorded AWS smoke run](../../docs/examples/aws-smoke-spark/).

## Kubernetes runs

Each run creates a `SparkApplication` in cluster mode and a ConfigMap mounted
at `/opt/bench/run`. The driver uses `site.kubernetes.spark_service_account`,
which has the cloud identity and RBAC needed to create executors. Setup disables
the operator chart's separate Spark service account and RBAC.

Driver and executor requests match their limits for Guaranteed QoS. Both receive the AWS region for authentication and storage.
`verify-spark` checks the effective driver settings, one Running driver, and the
requested number of Running executors with Guaranteed QoS. It accepts the run ID
or its lowercase Kubernetes name as the application name. It cannot read back
the trigger interval during staging; see [pitfalls](../../docs/pitfalls.md).

## Knobs

| Knob | Default | Effect |
|---|---|---|
| `executors` | required | executor count; `local[N]` multiplies it into cores |
| `executor_cores` | required | `spark.executor.cores` |
| `executor_mem_mb` | required | `spark.executor.memory` |
| `executor_overhead_mb` | unset | `spark.executor.memoryOverhead`. Unset leaves Spark's default for a Python job, 40% of `executor_mem_mb`, although the executors run no Python |
| `driver_cores` | `1` | `spark.driver.cores` |
| `driver_mem_mb` | `2048` | `spark.driver.memory`, and the local stack's `--driver-memory` |
| `trigger_interval` | required | `trigger(processingTime=…)` |
| `max_offsets_per_trigger` | unset | `maxOffsetsPerTrigger`; the option is omitted when unset |
| `distribution_mode` | required | `none` / `hash` / `range`, as a write option |
| `fanout` | unset | `fanout-enabled` write option. Unset leaves Iceberg's choice, which is the fanout writer with no per-task sort for an unsorted table; `false` adds a sort on the partition columns |
| `source_options` | `{}` | Kafka source options such as `minPartitions` or `kafka.max.partition.fetch.bytes`. Site Kafka properties override them; the options the run derives (bootstrap, topic, starting offsets, group, `maxOffsetsPerTrigger`) are refused |
| `machine_type` | unset | default fleet machine type for reporting and pricing. Site settings control placement |
| `driver_machine_type` | `machine_type` | driver machine type for reporting and pricing. Does not change placement |
| `extra_spark_conf` | `{}` | applied last, so it overrides anything above |

Shuffle partitions default to `executors × executor_cores`, avoiding Spark's 200-way default shuffle and excess small files on smaller fleets.

Spark 4.1 re-plans the write shuffle each micro-batch. Partitions coalesce or split at Iceberg's advisory
size (`spark.sql.iceberg.advisory-partition-size`), so a hot key gets several writers.

### Upsert runs

With `scoring.exactness: LATEST_PER_KEY`, `job.json` carries the key and version columns and the job
uses `foreachBatch`. Each micro-batch keeps every key's newest row (`max_by` over `id`), because
`MERGE` refuses two source rows matching one target row, then runs
`MERGE ... WHEN MATCHED AND s.id > t.id THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`.
The guard makes a replayed micro-batch a no-op. The `ON` clause also lists the batch's
`partition_key` values as literals. Iceberg prunes the target scan only by literal predicates in
`ON`, so without them every batch reads every partition. The batch is cached so collecting its
partition values and the `MERGE` share one Kafka read. The session sets `spark.sql.iceberg.check-nullability=false`,
which a `MERGE` reads in place of the append path's write option. `distribution_mode` must be `hash`
and `fanout` unset. For a reference upsert run, the table is merge-on-read on format v3
(`write.merge.mode`, `write.update.mode` and `write.delete.mode` all `merge-on-read`), so deletes are
deletion vectors. It also names zstd level 1 Parquet: a harness-created table names no codec, and
Iceberg's Java writers then fall back to gzip.

### Reference tier

A `reference` run sets `REFERENCE_KNOBS` from [`knobs.py`](knobs.py) (a 60 s trigger,
hash distribution) and varies only `SIZING_KNOBS`, the executor, driver and machine knobs.
Anything else makes it `tuned`. Start from [`aws-1mrps-skew-spark-ref.yaml`](../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-spark-ref.yaml).

<a id="avro-and-timestamp-handling"></a>

## Wire encoding and timestamps

### Avro

The corpus encodes `event_time` as Avro `timestamp-millis`. Spark normally reads
that as a zoned instant, but the Iceberg table requires a zoneless `timestamp`.
The renderer changes the reader schema annotation to `local-timestamp-millis`,
which makes `from_avro` return `TimestampNTZ`. Both annotations encode the same
`long`, so the bytes and millisecond value are preserved when widened to the
table's microsecond precision.

Corpus dates and timestamps are proleptic Gregorian, as Avro specifies. Spark's
default refuses dates before 1582-10-15, so the renderer sets
`spark.sql.avro.datetimeRebaseModeInRead` and `...InWrite` to `CORRECTED`.

### Confluent framing

With `kafka.value_encoding: confluent`, the job removes the five-byte header
using `substring(value, 6, length(value) - 5)` before decoding the payload with
`from_avro`. It needs no registry client: each run registers one schema and
already has the matching reader schema. Staging still requires
`site.kafka.schema_registry` to obtain the schema ID. See [wire framing](../../docs/adding-an-engine.md).

### JSON

With `kafka.value_encoding: json`, `from_json` decodes UTF-8 values in `FAILFAST`
mode. The renderer writes its schema to `job.json` from the column kind
registry, for example `BINARY` for base64 values and `TIMESTAMP_NTZ` for
millisecond UTC wall-clock strings. See [JSON representations](../../docs/corpus.md#json-mode)
and try the [JSON smoke](../../docs/running.md#encoding-variants).

`from_json` reads every list element and map value as nullable, and Iceberg
refuses to write those into required ones. For a corpus with a required list
element or map value, `job.json` sets `json_via_avro`, and the job re-decodes
each record through `to_avro` and `from_avro` with the reader schema. The round
trip restores the corpus nullability at the cost of extra decode work.

## Configuration constraints

- **Nullability:** both decoders produce nullable fields regardless of the
  corpus schema. The job sets `check-nullability=false` to bypass Iceberg's static
  comparison with required table columns. Spark's runtime `AssertNotNull`
  checks still fail the run if a required value is null.
- **Trigger intervals:** use a count, a space, and a full unit word, such as
  `10 seconds`, `500 milliseconds`, `2 minutes`, or `1 hour`. Spark 4.1.3 rejects
  abbreviations such as `10s`, `500ms`, `1m`, `1h`, and `10 sec`, as well as
  `10seconds`. The renderer validates this before staging.
- **Two storage clients:** Iceberg FileIO writes table data using
  `spark.sql.catalog.ice.s3.*`; Hadoop S3A writes checkpoints using
  `spark.hadoop.fs.s3a.*`. Local S3A settings are derived from the catalog
  properties. Checkpoint URIs use `s3a://`, which stock Spark recognizes.
- **Stable checkpoints:** the job sets the writer's `checkpointLocation` option.
  The session setting `spark.sql.streaming.checkpointLocation` is a parent path
  that gives unnamed queries random subdirectories. Reusing that setting after
  a restart could replay already committed rows.
- **Environment references:** `spark-submit` does not resolve `${env:NAME}` in
  its properties file. The renderer rejects such references there. Kafka
  options in `job.json` can contain references resolved by the job. Kubernetes
  storage access uses pod identity; only local sites render static S3A keys.
- **Catalog:** only Iceberg REST catalogs are supported. Spark renders
  `spark.sql.catalog.ice.type = rest` alongside the `SparkCatalog` class.
  Kafka security properties become `kafka.*` options, with MSK IAM translation.
  See the [run specification](../../docs/run-spec.md) for translated keys.
