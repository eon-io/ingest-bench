# The corpus

A corpus is an immutable workload reused across runs. It includes each batch's
row count and row-id checksum, plus verified partition and column statistics.
See the [methodology glossary](methodology.md#glossary) for definitions.

Two files define the workload: a **schema** under `workloads/schemas/` defines
columns, and a **preset** under `workloads/presets/` defines the stream. The
effective preset, including the schema, is hashed together with the seed,
generator version, and row-block size to name the corpus directory. Row-block
size affects batch rounding and therefore the rows generated. Changing these
inputs produces a new corpus identity, except for the standard preset rename
described below.

**On this page:** [Presets](#shipped-presets) · [JSON](#json-mode) · [Upsert](#upsert-corpora) · [Schemas](#schemas) · [Generation](#generating) · [Offer sizing](#sizing-the-offer)

## Shipped presets

| Preset | Rate | Duration | Keys | Skew |
|---|---|---|---|---|
| `smoke` | 5 MB/s | 300 s | 64 | `alpha: 1.0` |
| `smoke-json` | 5 MB/s | 300 s | 64 | `alpha: 1.0` |
| `smoke-upsert` | 5 MB/s | 300 s | 64 | `alpha: 1.0` |
| `smoke-upsert-orders` | 5 MB/s | 300 s | 5 | creation hour |
| `events-1mrps-uniform` | 1376 MB/s (about 1 M rows/s) | 3600 s | 512 | `alpha: 0.0` |
| `events-1mrps-skew` | 1376 MB/s (about 1 M rows/s) | 3600 s | 512 | `alpha: 1.0` |
| `events-50mbs-upsert-orders` | 50 MB/s | 3600 s | 5 | creation hour |
| `kinds`, `kinds-json`, `kinds-flink`, `kinds-flink-json` | 1 MB/s | 120 s | 16 | `alpha: 1.0` |

All presets use one-second batches. The `kinds` presets use the `kinds` schema,
which exercises every column kind, nullability and nesting. The standard
`events-1mrps` presets use `events200.json`: 200 columns in all, including 177
named ad, web and commerce columns that reuse the smaller `events` schema's
column profiles. They target 1375-byte Avro rows and offer 1,000,448 rows per
second after row-block rounding. Both standard append tracks use 192 Kafka partitions.
The upsert corpus, `events-50mbs-upsert-orders`, uses the `events` schema. It is a
food-delivery orders table at a dinner peak: 1 KB rows, about ten versions per order
within an hour, time-ordered IDs, hourly partitions and 20M seed orders.
The `events` schema also serves the smoke runs. Its other Avro presets target 256-byte
rows, the `kinds` Avro presets target 512-byte rows, and JSON presets target
1024-byte rows.
The `kinds-flink` variants replace the columns [Flink](#engine-support) cannot ingest.
`events-1mrps-uniform` and `events-1mrps-skew` replace the former
`events200-1mrps-*` preset names. Only these two name pairs are aliases for
corpus hashing: with otherwise identical inputs, each new name retains its
former name's hash. Existing frozen corpora can be reused without regeneration;
new generation writes a directory under the new name with that same hash.
Keep only one matching old or new directory under a corpus root, since staging
rejects an ambiguous pair. Historical result JSON keeps its recorded name.
MB means 1,000,000 bytes; MiB means 1,048,576 bytes. Rates above count encoded
values before storage or Kafka compression. They exclude keys, protocol overhead
and the optional Confluent header. Whole row blocks can make the actual
encoded rate slightly higher than the target.

## Preset keys

Every key except `value_encoding` and `sizing_encoding` is required.

| Key | What it sets |
|---|---|
| `schema` | the schema file under `workloads/schemas/`, by name |
| `value_encoding` | `avro` (default) or `json`; determines stored records and, by default, row calibration and byte budgets |
| `sizing_encoding` | optional `avro` for JSON; uses Avro row calibration and batch budgets to preserve the reference records |
| `offered_bytes_per_s` | the rate in the sizing encoding, as a byte quantity (`5MB`, `100MB`, `1GiB`) |
| `duration_s` | stream duration; must be a whole number of batches |
| `partition_count` | the key space: how many distinct `partition_key` values exist |
| `alpha` | the Zipf exponent over those keys. `0.0` is uniform; higher is more skewed |
| `target_row_bytes` | the row size in the sizing encoding that the payload column is calibrated to |
| `batch_interval_ms` | duration of one batch; sets the workload's time granularity |
| `corpus_epoch` | start of the first batch's event-time range; requires an explicit UTC offset, since the offset is not part of the hash |
| `kafka_key_columns` | which columns get a key sidecar. Must be required `categorical` columns. `partition_key` is added if left out, because the Iceberg partition column is written from its sidecar |
| `column_overrides` | per-column changes to the schema's declarations, keyed by column name |
| `upsert` | optional. Makes the corpus an [upsert workload](#upsert-corpora) |

Use repeatable `--set KEY=VALUE` flags to override preset keys, or
`--set column_overrides.<column>.<attribute>=VALUE` for a column attribute.
Overrides change the corpus hash. Runs using them are probes; publishable
results must match a shipped preset.

## JSON mode

Generate a JSON corpus:

```bash
gen-corpus --preset smoke-json --out ./corpus
```

Then stage `specs/smoke/smoke-flink-json.yaml` or `specs/smoke/smoke-spark-json.yaml`.
The [JSON smoke commands](running.md#encoding-variants) handle both steps automatically.

Each Kafka value is a UTF-8 JSON object, without a registry header.
`schema.avsc` still defines logical column types:

| Value | JSON representation |
|---|---|
| Timestamp | UTC wall-clock string with millisecond precision, such as `2026-01-01T00:00:00.123`, without a timezone suffix |
| Date | ISO date string, such as `2026-01-01` |
| Binary | Base64 string |
| `exact_decimal` | String with `scale` fraction digits, such as `12.30`, so no digit passes through a double |
| UUID | Canonical lowercase string |
| Number or boolean | Native JSON type; readers must preserve 64-bit integer precision, especially `id` |
| Null | `null` |
| Struct, map or list | Object, or array for a list, whose members follow these rules |

The producer replays stored bytes without conversion. Set `corpus` to the JSON
preset and `kafka.value_encoding: json`; keep exactly one generated version per preset
under `corpus_root`. Staging and producer startup reject incompatible encodings.
Avro corpora support `avro` and `confluent`.

**Custom presets:** set `value_encoding: json`, or generate with
`--set value_encoding=json`. For `events` with default JSON sizing, use
`--set target_row_bytes=1024`. With `sizing_encoding: avro`, keep the Avro row budget.

**Byte accounting:** JSON budgets include field names, quoting and base64 expansion.
To preserve an Avro preset's records and batch boundaries with the same seed
and row-block size, set `sizing_encoding: avro`. Budgets then refer to Avro;
metadata and results report calibrated JSON targets, while manifests and
partition truth count actual JSON bytes.

> **Comparison rule:** use the same corpus and encoding across engines.
> Changing format or row size changes the workload.

## Upsert corpora

An `upsert` block turns the stream into keyed changes. Each row creates a key or updates
one, and an engine must keep only the newest version of every key.

| Key | What it sets |
|---|---|
| `key_column` | the role `entity` column holding the key, which is also the Kafka key |
| `kafka_partitions` | the topic's partition count. Rows are stored grouped by partition |
| `seed_keys` | keys in the seed every run starts from |
| `new_key_fraction` | share of rows that create a key |
| `updates` | how the other rows pick a key: `recent` (exponential age), `uniform` (any existing key) or `hot` (Zipf over fixed seed keys) |
| `seed_delivery` | optional. `table` (default) registers a seed table the harness wrote; `kafka` sends the seed through Kafka for the engine to write before the scored offer |
| `seed_history_s` | optional. Seed keys' creation times span this many seconds before the stream. A `recent` update older than the stream then reaches the seed key created that long ago, not a random one |
| `key_layout` | optional. `random` (default) hashes the key into its token; `time_ordered` leads the token with the key's creation time in hex milliseconds, like a Snowflake ID. Needs `seed_history_s` |
| `partition_layout` | optional. `hashed` (default) draws each key's table partition from the Zipf weights; `creation_hour` partitions by the hour the key was created, so `partition_count` covers the seed history and the stream and `alpha` is 0 |

- **Keys come from row ids.** A seeded schedule names every row's key, so the scorer
  recomputes it without reading the corpus. `event_id` and `partition_key` derive from
  the key, so a key never changes table partition.
- **Rows per batch are fixed** at the batch budget divided by `target_row_bytes`, so the
  offered byte rate is approximate. The mean-row-size gate still applies.
- **Batches are stored per Kafka partition.** Each partition's rows form an independently
  compressed segment, listed in the manifest with its offset, length, rows and digest. One
  producer shard owns each partition, so a key's versions reach Kafka in corpus order.
- **The seed** holds one row per seed key, with id `-(key + 1)` below every stream
  id. Build it after the corpus with `seed-corpus --corpus <uri>` on each shard
  (`--shard-index`, `--shard-count`), then `commit-seed --corpus <uri>`. On a cluster,
  `scripts/gen-corpus.sh` runs both steps as Jobs; see
  [upsert runs on AWS](../deploy/aws/README.md#upsert-runs). A table seed is a Parquet table
  whose snapshot, in `seed.json`, every run's table is registered over. A Kafka seed is a set
  of producer batches under `seed/`: launch sends it (`produce --seed`) into an empty table and
  waits for the engine to commit it (`wait-seed`) before the epoch, within the run's freshness bound.
- `corpus.json` publishes `upsert.live_rows` and `upsert.live_checksum`, the final table
  every correct engine converges to.

## Shape axes

Vary rate (batch size and memory), skew (`alpha` over `partition_count`), row
size (per-row work at a fixed byte rate), or batch interval (freshness granularity).
Per-column `cardinality` and `alpha` also affect compressibility;
`cardinality: 0` generates a fresh value per row.

## Schemas

Every schema starts with two implicit reserved columns: `id` (long), used for
exactness, and `partition_key` (string), used for partition statistics and table
partitioning. Schemas cannot rename or retype them.

Declare all other columns with a **kind**, which determines their type, and an
optional **role**, which identifies their purpose independently of their name.

| Kind | Avro type | Iceberg type |
|---|---|---|
| `categorical` | `string` | `string` |
| `integer` | `long` | `long` |
| `int32` | `int` | `int` |
| `decimal` | `double` | `double` |
| `float32` | `float` | `float` |
| `exact_decimal` | `bytes` / `decimal` | `decimal(precision, scale)` |
| `boolean` | `boolean` | `boolean` |
| `timestamp` | `long` / `timestamp-millis` | `timestamp` (microsecond, zoneless) |
| `date` | `int` / `date` | `date` |
| `uuid` | `string` / `uuid` | `uuid` |
| `blob` | `bytes` | `binary` |
| `struct` | `record` | `struct` |
| `list` | `array` | `list` |
| `map` | `map` | `map` with `string` keys |

`decimal` is a binary double rounded to hundredths. Kind names are part of
corpus identity, so the fixed-precision decimal is `exact_decimal`. The
[kind registry](../CONTRIBUTING.md#add-a-column-kind) also maps each kind to
Flink, Spark and JSON.

| Role | Exactly one column, of kind | Purpose |
|---|---|---|
| `event_time` | `timestamp` | generated event times within each corpus batch; freshness uses Kafka acknowledgements instead |
| `entity` | `categorical` | entity dimension; available as a Kafka key when a sidecar is generated |
| `sum_measure` | `integer` or `decimal` | numeric measure for downstream aggregation |
| `payload` | `blob` | row-size calibration; payload fills the remaining byte budget |

Any other column is `generic`. The benchmark currently scores ingestion and file
geometry; it does not run point-lookup or aggregate-query workloads.

### Declaring a column

A declaration is `{"name": ..., "kind": ...}` with optional attributes:
`role`, `cardinality`, `alpha`, `vocabulary`, `minimum`, `maximum`, `width`,
`null_fraction`, `precision` and `scale`, plus the nested declarations below.
Omitted attributes use defaults. See `workloads/schemas/events.json` and
`workloads/schemas/kinds.json` for examples.

The loader rejects declarations whose value ranges cannot support the requested
distribution:

- `alpha` above zero requires bounded cardinality of at most 1,000,000. The
  generator samples skewed ranks from a cumulative distribution stored in memory.
  Unbounded columns instead derive values from the row identity.
- A **bounded** categorical needs a `vocabulary` and no `width`; an **unbounded**
  one needs `width` of at least 16 hex characters and no vocabulary, to
  make token collisions unlikely across the corpus.
- A numeric column needs a `[minimum, maximum]` range holding at least as many
  values as it has ranks, after rounding to a whole number (`integer`, `int32`,
  `date`), two decimals (`decimal`, `float32`) or `scale` decimals (`exact_decimal`).
- A `date` range counts days since 1970-01-01, within 0001-01-01 to 9999-12-31.
  An `int32` range fits in 32 bits. A `float32` range stays within ±131072, where
  single precision still tells hundredths apart. An `exact_decimal` declares
  `precision` (1 to 38) and `scale` (0 to `precision`), and its range must fit them.
- A boolean's cardinality is 1 or 2. A timestamp may only be the `event_time`
  column, since arrival order belongs to the stream and not to a column.
- The `payload` column omits `width` because calibration determines it.
  Every other blob column requires a positive width.

### Nullable and nested columns

- `null_fraction` is the share of null rows. The default 0 keeps a column required.
  A nullable column is an Avro union led by null, an optional Iceberg field,
  nullable DDL and JSON `null`. Only `generic` columns and nested members may be
  nullable, so reserved, role and Kafka key columns and map keys stay required.
- A `struct` declares `fields`, a list of declarations. A `list` declares an
  `element`, and a `map` a `key` and a `value`. These take their name from their
  position. Members may be any kind except `timestamp`, and hold no role.
- A list or map holds a uniform `minimum` to `maximum` entries per row. A map key
  is a bounded `categorical` whose cardinality is at least `maximum`: a row's keys
  are consecutive ranks from a skewed start, so they stay distinct.
- Nested columns cannot be Kafka keys or partition sources. Column statistics
  cover scalar columns and count non-null values. Calibration measures every column.

### Engine support

- Flink cannot write `uuid`: Iceberg's Flink sink takes it as `BINARY(16)`, and
  Flink SQL cannot convert the corpus's uuid strings. Flink staging rejects such a corpus.
- Flink reads every field of a nullable struct as nullable. Plain Avro therefore
  cannot carry a required field inside a nullable struct to Flink, and staging
  rejects it. Confluent framing and JSON can.
- With `table.managed_by: engine`, the rendered Spark SQL cannot declare `uuid` or a
  required list element or map value, so staging rejects them. Tables the harness
  creates take every kind.

Before publication, the generator requires mean encoded row size within 2% of
`target_row_bytes`, checked column cardinalities within 10% of their targets, and
unbounded blob entropy of at least 7.5 bits per byte.

For a skewed corpus (`alpha > 0`), each key's byte share must be within 5% of
its Zipf weight. This check uses bytes to reflect write volume and applies only
when the least frequent key expects at least 10,000 rows, limiting sampling
noise. For a uniform corpus, the check requires every key to receive rows.

**Ship the schema and the preset before publishing any result from them.**
`validate-results.py` checks the recorded generator version and recomputes
`corpus_hash` from a shipped preset or its supported legacy alias, the recorded
seed, and the row-block size.
Local-only workload definitions and results without these identity fields are
not publishable.

## What a corpus directory holds

`<corpus_root>/<preset>-<hash>/`:

| Path | What it is |
|---|---|
| `batches/NNNNNN.bin.zst` | one batch's encoded rows, length-framed and compressed |
| `batches/NNNNNN.key.<column>.zst` | that batch's key sidecar, one per `kafka_key_columns` entry |
| `manifest.jsonl` | one record per batch: rows, the id range, the checksum, the encoded and stored bytes, a SHA-256 of the value file, checked during append replay. The frozen scoring input |
| `schema.avsc` | column schema; Avro uses its binary encoding, JSON uses the logical types |
| `partition_truth.json` | rows, checksum and encoded bytes per partition key |
| `column_stats.json` | sampled per-column statistics used to recompute statistics during a merge |
| `corpus.json` | corpus metadata: effective preset, schema, column roles, Iceberg types and optional columns, row count, rates, seed, row-block size and generator version |

`corpus.json` is authoritative. The producer, table creator, scorer and external
engines read it through the corpus URI. Use its recorded values rather than
recomputing them from a preset.

As of generator version 6, the corpus identity includes the seed, row-block
size, and generator version. Version 5 corpora fail the current
generator-version check. Regenerate them under the new identity; generation
leaves the old directories intact. Staging requires exactly one matching corpus
per preset under the configured root, so keep different seeds or duplicate
old/new-named copies in separate roots.

## Generating

```bash
gen-corpus --preset smoke --out s3://<corpus_root> --seed 1
gen-corpus --preset events-1mrps-skew --out s3://<corpus_root> --plan   # writes nothing
```

`--plan` prints the preset's hash, the batch count and size, the estimated rows,
the mean row size and the estimated encoded and stored bytes. It writes no files.
Row estimates use the generator's calibration and are lower bounds: batches
contain whole row blocks and may slightly exceed their byte budget. Preview a
large corpus before generating it. The standard one-hour offer is about 4.95 TB
of encoded values before compression and replication.

The directory hash excludes compression level. Generation settings and
`manifest_sha256` are recorded in `corpus.json`. Reuse the same frozen corpus
for compared runs; use a separate output root when changing compression.

### Memory

Generation and replay both hold a whole batch in memory; sharding divides batch
assignments, not batch size. `scripts/gen-corpus.sh` requests `10Gi` for the
standard presets and `3Gi` for the others unless `GEN_MEMORY` overrides it. A
1.4 GB standard batch peaked at 7.8 GiB in a local generation test.

Once a batch is sent, separate threads decompress the next and download the one
after, so a due batch sends on time. `PRODUCER_MEMORY` defaults to `3Gi` for
batches and Kafka buffers. `PRODUCER_CPU` defaults to `2` and is a scheduling request,
not a limit. Pending shards reduce the offered load. Add node capacity or lower
the CPU request if measured producer usage permits; ensure every shard can be scheduled.

### Sharding and the merge

Each shard writes a directory containing its batches and their metadata.
`merge-corpus <shard uri>… --out <corpus_root>` combines the metadata into one
corpus but leaves the batch files in place. The harness rejects shard names as
corpus inputs because their metadata describes only part of the workload.

> **Keep shard directories after merging.** They remain part of the corpus.

On a cluster, `scripts/gen-corpus.sh <preset> --shards N` runs generation and
merging as Jobs. This keeps corpus data off
the operator's laptop and uses the pods' identity to write to the run's bucket.

Estimate generation time from a probe with a standard batch and the intended
hardware. Cap useful parallelism at available physical cores. Use the measured
time to choose the shard count and override the default four-hour `GEN_WAIT_S`
when needed. The driver does not estimate generation time. A smaller-schema
batch measurement does not size the 200-column corpus.

Interrupted Jobs can leave unlisted batch objects. Before retrying with a
different shard count, use a new corpus root or remove only failed shard
directories that no merged corpus references. Never delete referenced shards.

## Sizing the offer

`scripts/measure-producer.sh` times one producer shard against the local broker
with an epoch an hour in the past, so every batch is already due and the wall
clock measures the producer rather than the corpus's pacing.

Choose an initial shard count using the measured throughput:

```
shards = ceil(offered_bytes_per_s * producer.speed / measured_bytes_per_s * 1.5)
```

The script uses local Kafka, eight partitions, raw Avro, zstd and `user_id` keys,
and accepts no site or run-spec options. Validate the estimate with a cluster
probe using the intended nodes and Kafka settings.

Check publish time, acknowledgement lateness and CPU during the first cluster
probe, then add shards if acknowledgement lateness grows.
Recalibrate after changing producer nodes, partitions, encoding, compression,
keys or the corpus.
