# The run spec and the site config

Two YAML files configure a run. A **run spec** under `specs/` defines the workload,
table, producer and engine settings; published results include a verbatim copy.
A **site config** (`./site.yaml`, or a path supplied with `--site`) defines the
operator's storage, broker, catalog, cluster and prices. It is not published.

Both loaders reject unknown keys; the run-spec loader also checks numeric ranges before staging creates resources.

See the [methodology glossary](methodology.md#glossary) for measurement terms.

**On this page:** [Run keys](#run-spec) · [Tracks](#tracks) · [Scoring](#scoring) · [Compaction](#compaction) · [Site keys](#site-config) · [Secrets](#secrets)

## Run spec

| Key | Default | Effect |
|---|---|---|
| `name` | *required* | base name for the topic, table and Kubernetes objects. Must match `^[a-z0-9][a-z0-9-]{2,34}$` (3–35 characters), leaving room for timestamps and prefixes within Kubernetes limits. Engine limits may be stricter: Flink allows at most **28** characters |
| `engine` | *required* | `external`, or a managed engine — `flink` or `spark` |
| `track` | omitted | a shipped track under `workloads/tracks/`, which supplies the workload and scoring settings below. See [Tracks](#tracks) |
| `corpus` | *required* unless `track` is set | a shipped preset name, or a path to a preset file. See [`corpus.md`](corpus.md) |
| `tier` | `tuned` | `reference` or `tuned`, for `engine: external` only. A managed engine's tier is derived from its knobs |
| `<engine>:` | *required* for a managed engine | that engine's knobs; see `engines/<name>/README.md`. Refused for `engine: external` |
| `external:` | *required* for `engine: external` | `{name, version, notes}`, all strings. Operator-supplied engine identity and tuning notes |
| `fleet:` | *required* for `engine: external` | a non-empty list of `{role, count, vcpu, gib, machine_type}` for cluster cost. Managed cluster costs use captured pod requests; local runs have no resource cost |
| `compaction:` | omitted | what rewrites the table's files, if anything does. A layer of its own: valid beside any engine |

Spark ingest accepts `spark.driver_machine_type`; Flink accepts `flink.jm_machine_type`. Both default to `machine_type` and affect hardware and pricing metadata only.

### `table`

| Key | Default | Effect |
|---|---|---|
| `managed_by` | `harness` | `harness` creates the table with the specified properties. `engine` supplies equivalent DDL in `facts.ddl` for your engine to execute; the scorer treats an absent table as empty while waiting |
| `partition` | `identity(partition_key)` | `identity(col)`, `bucket(N, col)` or `unpartitioned` |
| `properties` | `{}` | Iceberg table properties, set at creation. See [`pitfalls.md`](pitfalls.md) |
| `format_version` | `2` | Iceberg format version. `3` (deletion vectors) is accepted for upsert runs only |

### `kafka` (required)

| Key | Default | Effect |
|---|---|---|
| `partitions` | *required* | topic partitions. Replication is not a knob: staging asks for `min(3, brokers)` |
| `key` | *required* | corpus column used as the Kafka message key, or `none` for unkeyed records |
| `partition_assignment` | `key` | `key` uses the client partitioner; `round_robin` cycles topic partitions independently of the key. `corpus` (upsert corpora only) sends each row to the partition the corpus stored it under |
| `value_encoding` | `avro` | `avro` sends raw binary records; `json` sends a pre-generated JSON corpus; `confluent` adds a five-byte Avro header and requires `site.kafka.schema_registry` |

`round_robin` preserves key bytes and record values, including table-partition skew.
Flink and Spark derive Iceberg partitions from record values. Equal keys can span
Kafka partitions without per-key ordering. Match key and assignment across engines.
CLI: `--partition-assignment round_robin --partitions N`; N must match topic metadata at startup and remain fixed.
`corpus` gives each producer shard a contiguous block of partitions, so every key's versions arrive
in corpus order. The topic must have the corpus's `kafka_partitions`, and each run is offered once:
a restarted shard refuses to replay, so stage a fresh run instead.

### Tracks

A track fixes everything a results section compares on: `corpus`, the `kafka` block, `producer.compression`
and `behind_max_ms`, `scoring.freshness_bound_s` and `exactness`, and `table.partition`. A track may also fix
`scoring.freshness_max_bound_s`, and an upsert track pins `scoring.geometry_offsets_s`, the snapshots its live
state is verified at. A track caps `scoring.warmup_s`, and the cap is the default. A spec that names a track
may omit those settings. Any it sets must equal the track's, and `producer.speed`, `scoring.mode` and
`measurement_s` are refused. Staging writes the spec with the track's settings filled in, so a published copy
stays runnable. Sustainable results publish only when their recorded settings match a track. A valid result passes
only when its measured freshness meets the track's p95 and max bounds, whatever it was scored under, and is listed as not passing otherwise.
Specs behind published results live under `specs/tracks/<track>/`.

### `producer`

| Key | Default | Effect |
|---|---|---|
| `speed` | `1.0` | replay speed multiplier; `2.0` doubles the offered rate |
| `seconds` | whole corpus | limit replay to this many corpus seconds; sustainable publication requires a full corpus, while saturation requires the declared window to fit (see [saturation mode](saturation.md)) |
| `shards` | `1` | producer process count; see [sizing guidance](corpus.md#sizing-the-offer) |
| `behind_max_ms` | `5000` | maximum delay from a batch's due time to its first acknowledgement; exceeding it makes the run `producer_bound` |
| `compression` | `zstd` | the wire codec: `zstd`, `lz4`, `snappy`, `gzip` or `none`. Use the same codec for compared runs |

Replay speed must be finite and positive. Supplied `seconds` and `shards` must
be positive integers; `behind_max_ms` must be nonnegative.

### `scoring`

| Key | Default | Effect |
|---|---|---|
| `mode` | `sustainable` | `sustainable` requires bounded freshness; `saturation` measures throughput under backlog |
| `measurement_s` | absent | positive integer duration after warmup; required only in saturation mode |
| `freshness_bound_s` | `180` | maximum window p95 lag; diagnostic in saturation mode |
| `freshness_max_bound_s` | twice `freshness_bound_s` | maximum window lag, at least `freshness_bound_s`; diagnostic in saturation mode |
| `warmup_s` | `120` | seconds after the epoch before the freshness or saturation measurement window |
| `exactness` | `EXACTLY_ONCE` | delivery SLA. `EXACTLY_ONCE` rejects loss, duplication and corruption; `AT_LEAST_ONCE` permits duplicates but rejects loss and corruption. `LATEST_PER_KEY` scores [upsert runs](methodology.md#upsert-runs) |
| `geometry_offsets_s` | `[600, 1200, 1800, 2700, 3600]` | geometry snapshot offsets from the epoch; unreached offsets are `absent` |
| `gate_adaptation_s` | `120` (gate default) | seconds after the epoch before the live lag check begins; the backlog check requires three populated windows |
| `gate_window_s` | `60` (gate default) | the width of the three backlog-floor windows |

See [saturation mode](saturation.md) for window, validity and comparison rules. The live lag and backlog
thresholds apply only in sustainable mode. Omit the gate keys to use the gate's defaults. `exactness` must be
`EXACTLY_ONCE` or `AT_LEAST_ONCE`. `freshness_bound_s` must be finite and positive, and
`freshness_max_bound_s` finite and no smaller. `gate_window_s` must be positive. `warmup_s` and
`gate_adaptation_s` must be nonnegative. Geometry offsets must be nonnegative integers in strictly increasing
order. Quote values in `table.properties` and site property maps, including numbers and booleans: these maps
require strings.

### `compaction`

Omitting `compaction` means no compaction. Declare it independently of ingest.

| Key | Default | Effect |
|---|---|---|
| `driver` | *required* | `inline`, `external`, or a registered driver — `spark-rewrite` or `flink-rewrite` |
| `interval_s` | *required* for a registered driver | seconds between rewrite passes, timed from the start of each. Refused for `inline` and `external`, which keep their own cadence |
| `<driver>:` | *required* for a registered driver | that driver's knobs, below. Refused otherwise |
| `external:` | *required* for `driver: external` | `{name, version, notes}`, as the engine block |
| `fleet:` | *required* for `driver: external` | the compactor's own compute, charged on top of the engine's |

- `inline`: compaction uses the engine fleet; starts no resources and adds no
  separate fleet cost. Flink requires a matching `flink.maintenance` block.
- `external`: the operator starts the compactor; its separate fleet adds cost.
- Registered drivers: the harness starts a sidecar beside any ingest engine.

#### `spark-rewrite`

A fixed, long-lived Spark fleet calls `rewrite_data_files` every `interval_s`,
using the Spark engine image.

| Key | Default | Effect |
|---|---|---|
| `executors`, `executor_cores`, `executor_mem_mb` | *required* | executor count, CPU and JVM heap; each must be at least 1. See memory accounting below |
| `executor_slots` | `executor_cores` | task slots per executor (`spark.executor.cores`); more slots than vCPU let I/O-bound rewrite tasks overlap on a pod, which is still costed at `executor_cores` |
| `driver_cores` | `1` | driver CPU |
| `driver_mem_mb` | `2048` | driver heap |
| `rewrite_options` | `{}` | options passed to `rewrite_data_files`, as strings. `partial-progress.enabled` changes how many commits a pass lands in |
| `start_delay_s` | `0` | seconds to hold the first pass after the table's first commit that adds rows. The fleet is held and costed meanwhile. The table builds a backlog, so the first passes run saturated and a short probe reads the sidecar's capacity |
| `machine_type` | unset | the node instance type, for costing. Publication rejects a result that leaves it unset |
| `driver_machine_type` | `machine_type` | driver hardware and pricing metadata. Does not select nodes |
| `extra_spark_conf` | `{}` | Spark settings applied last |
| `node_selector`, `tolerations` | `{}`, `[]` | placement added to the site's for the sidecar only. Match isolation from ingest nodes across comparison arms |

**Memory accounting:** Spark adds `spark.kubernetes.memoryOverheadFactor` to
`executor_mem_mb`. It defaults to `0.4` for `type: Python`, so the pod requests
and is costed at 1.4× the heap. Set the factor through `extra_spark_conf`.
Flink's `tm_mem_mb` covers the whole container; the two knobs are not equivalent.

Both rewrite drivers read the target from the table
(`table.properties.write.target-file-size-bytes`) and reject any target in
`rewrite_options`. Set the same target across comparison arms.

Staging waits for the sidecar's full fleet and captures its admitted pod
requests in `compactor-pods.json` for costing, including non-heap memory.
Total cost is unknown if either managed fleet's pod evidence is missing.
What compaction achieved is read from the table's metadata afterwards by the
`compaction` command; see [`results-format.md`](results-format.md).
A sidecar that fails every pass stops.

#### `flink-rewrite`

Iceberg's own Flink table maintenance (`TableMaintenance` with one
`RewriteDataFiles` task on `interval_s`) as a second FlinkDeployment on the
Flink engine's image, beside whichever engine is ingesting. Like the Spark
sidecar it holds a fixed fleet, keeps no checkpoint and is never restarted: a
failed maintenance job stays failed and reads as an unhealthy compactor.

| Key | Default | Effect |
|---|---|---|
| `rewrite_parallelism`, `slots`, `tm_cpu`, `tm_mem_mb` | *required* | file groups rewritten at once (the Rewrite operator's parallelism, one group per subtask), and the TaskManager shape holding them; the fleet is `ceil(rewrite_parallelism / slots)` TaskManagers |
| `jm_cpu`, `jm_mem_mb` | `1`, `2048` | the sidecar's JobManager |
| `rate_limit_s` | `15` | the table monitor's poll period and the least time between passes; at most `interval_s` |
| `lock_check_delay_s` | `10` | how often a trigger blocked by a running pass re-checks |
| `rewrite_options` | `{}` | string-valued settings from the supported list below; unknown options are rejected |
| `machine_type` | unset | the node instance type, for costing |
| `jm_machine_type` | `machine_type` | JobManager hardware and pricing metadata. Does not select nodes |
| `extra_flink_conf` | `{}` | Flink settings of the maintenance deployment, applied last |
| `node_selector`, `tolerations` | `{}`, `[]` | placement added to the site's for the sidecar's pods only, to give the maintenance fleet nodes of its own |

Supported `rewrite_options` (unknown keys would otherwise be silently ignored):

- **File selection:** `min-input-files`, `min-file-size-bytes`, `max-file-size-bytes`, `rewrite-all`.
- **Work limits:** `max-file-group-size-bytes`, `max-file-group-input-files`, `max-files-to-rewrite`, `max-rewrite-bytes`.
- **Commits:** `partial-progress.enabled`, `partial-progress.max-commits`.
- **Order:** `rewrite-job-order` accepts `none`, `bytes-asc`, `bytes-desc`, `files-asc`, `files-desc`. Groups reach subtasks round-robin in this order; `bytes-desc` spreads the largest groups across subtasks.

The deployment is named `<run object name>-m`, so a run with this sidecar may
use at most 26 characters of name. There is no target-file-size knob, for the
same reason as above. What the sidecar did is read from table metadata; the
job's own counters (trigger, pass and commit metrics on its REST API) are
diagnostics only.

## Site config

| Key | Effect |
|---|---|
| `corpus_root`, `runs_root`, `warehouse` | *required*. Storage roots for corpora, run artifacts and table data; redacted in results. Shell drivers use the AWS CLI and require `s3://` URIs |
| `kafka.bootstrap_servers` | *required* |
| `kafka.deployment` | `managed`, `in-cluster`, or `external`. Required by AWS and in-cluster stack setup and teardown; optional for run drivers and operator-managed sites. On AWS, only `managed` provisions MSK |
| `kafka.security` | librdkafka `security.*` / `sasl.*` properties; managed engines apply the translations below. Omitted from results |
| `kafka.schema_registry` | `{url, basic_auth_user_info?}`; optional for raw Avro and JSON, required for Confluent framing |
| `catalog.props` | *required*. PyIceberg catalog properties. Managed engines require a REST catalog; external runs may use any catalog PyIceberg can open |
| `kubernetes` | empty for local runs. Cluster runs require `context`, `namespace`, `harness_service_account`, `flink_service_account` and `registry`, and may set `spark_service_account` (default `ingest-bench-spark`), `aws_region`, `secret_name` (§Secrets), `service_account_annotations`, `node_selector` and `tolerations` |
| `pricing` | *required*. `machine_types: {<exact machine type>: {vcpu, gib, usd_per_hour}}`, or legacy `{vcpu_hour_usd, gib_hour_usd}` flat rates, never both. Every fleet type needs an entry when using the map. Missing types fail costing |

Start from `site.example.yaml`, `site.aws.example.yaml`, or `site.k8s.example.yaml`; replace every `YOUR_` placeholder. See the [pricing example](submitting.md#write-the-spec).

### Engine configuration translation

`site.catalog.props` and `site.kafka.security` are copied into managed-engine
configuration, with these translations:

| From the site | What an engine gets |
|---|---|
| `type` (`rest`, or absent) | the engine's own REST catalog binding |
| `s3.region` | `client.region` |
| `warehouse` on `s3://` or `gs://` | the matching `io-impl`, selected from `site.warehouse`. The catalog warehouse may be an ID, as in Glue, rather than a storage URI |
| `sasl.mechanism: OAUTHBEARER` plus `aws.region`, without `sasl.oauthbearer.*` | the Java client's `AWS_MSK_IAM` login module and its callback handler |

`aws.region` configures MSK token signing and is removed before passing
properties to librdkafka. Install the harness with the `aws` extra to use it.
If the site supplies a `sasl.oauthbearer.*` property, the harness passes that
OAuth configuration through to its own clients. Managed Spark and Flink runs
reject these librdkafka-specific OAuth properties before staging creates resources;
they are not translated into Java OAuth settings. Use an external engine for
explicit OAuth, or omit these properties when using Amazon MSK IAM.

**Set compression only through `producer.compression`.** The site loader and
producer reject `compression.*` keys in `site.kafka.security` and `--kafka-prop`
to prevent the actual codec from differing from the published spec. Catalog
properties do not reach Kafka clients and are not subject to this check.

### Secrets

Use `${env:NAME}` for credentials in Kafka security, catalog properties,
schema-registry authentication, `--catalog-prop`, or `--kafka-prop`.
References resolve only in the process that uses them; an unset variable is
an error. Site configs, staged facts, rendered files and results retain the
reference so operators can identify which variables to supply.

On a cluster the variable comes from one Secret in the run's namespace, created
once and named as `site.kubernetes.secret_name`:

```bash
kubectl --namespace ingest-bench create secret generic bench-env \
  --from-literal=IB_KAFKA_PASSWORD=... --from-literal=IB_REGISTRY_AUTH=...
```

Every key in that Secret becomes an environment variable on every harness and
engine pod. The keys must match the names used in `${env:NAME}` references.

**Cluster sites reject literal credentials** because rendered properties are
stored in ConfigMaps and uploaded to the runs prefix. Use environment references
to keep secrets out of those artifacts. Local sites permit literals for the
stack's public default credentials; credential-named properties are redacted
from `facts.json` and results.

Flink resolves environment references throughout its rendered script and
settings. Spark resolves them only in Kafka source options and rejects them in
other settings at render time. Put referenced Spark credentials in
`site.kafka.security`. Both engines support MSK IAM authentication without a
Secret.

Object storage and catalogs otherwise use the cloud SDK's default credential
chain, such as pod identity, an instance role or an ambient profile. The local
stack uses public default credentials.
