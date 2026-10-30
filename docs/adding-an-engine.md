# Adding an engine

The harness can score any engine that consumes Kafka and appends to Iceberg.
Choose one of two integration modes.

| Mode | Harness responsibility | When to use it |
|---|---|---|
| **Tier 1: external** | Stage the run and print connection facts; you start the engine | Start here with an existing cluster, service or local process |
| **Tier 2: managed** | Size and start the engine through `engines/<name>/` | Repeated runs and capacity sweeps that justify integration code |

**On this page:** [Contract](#the-tier-1-contract) · [Upsert](#upsert-runs) · [Connection facts](#what-the-harness-hands-you) · [Local example](#walk-through-an-engine-the-harness-does-not-manage) · [Cluster](#on-a-cluster) · [Managed integration](#tier-2-a-managed-engine)

## The tier 1 contract

Comparable runs must satisfy these six rules.

1. **Read the full topic.** Consume from the earliest offset. Decode `facts.json`'s
   `value_encoding`: raw Avro, Confluent-framed Avro, or plain JSON (see below).
   Keys are UTF-8 strings or null.

2. **Match the schema.** Append every row, map columns one to one by name, and
   declare `NOT NULL` every corpus column that `corpus.json` omits from
   `optional_columns`. Extra columns are allowed and may be nullable. A corpus
   column that is dropped, renamed, retyped or declared with other nullability
   voids the run. `corpus.json` publishes the Iceberg type of every column,
   including the required status of nested fields, and the scorer checks column
   names, types and nullability against it when first loading the table.
   Convert the event-time column from Avro `timestamp-millis` or JSON UTC
   wall-clock strings to Iceberg `timestamp` (microsecond precision, without a
   time zone) without rounding. Nullability affects encoding and file geometry,
   so it must match even for engine-created tables.

3. **Append only.** No delete files, upserts or overwrites on the path that
   writes corpus rows. Rule 6 governs compaction and other rewrites.

4. **Use Parquet or ORC.** Write data files in either format.

5. **Use the supplied catalog.** Commit through the catalog the harness names.

6. **Retain snapshots.** Do not expire them until scoring and geometry collection finish. Compaction
   and other rewrites only when declared as a named variant in the result.
   Declare the file-size target the compactor aims at, so `compaction` measures
   the table against the size it was aiming for. Left undeclared, the command
   reads the table's `write.target-file-size-bytes`, then the format default,
   and records which source it used.

Start at the earliest offset even if the engine starts before the producer.
Partition assignment happens after consumer startup; `latest` can skip records
published before assignment or during a restart without committed offsets.

The scorer checks initial column declarations and IDs in append snapshots;
it does not verify other values or prove rewrites preserve the live table.
A rewrite committed as `delete` then `append` can be tallied as new ingest.
Retain history and use terminal exactness checks for compacted tables.

Declare integrated compaction with `compaction: {driver: inline}`, or separate
compactor compute with the [compaction block](run-spec.md#compaction).
The independent [compaction report](results-format.md#compaction-and-readiness)
measures file geometry and rewrite bytes from file lifecycles.

### Upsert runs

`LATEST_PER_KEY` runs replace rule 3 and add connection facts.

- **Latest per key.** After each commit, the table holds exactly the newest version of every
  key it has seen. Newest means Kafka order within the key's partition, which equals the
  largest `id`. Any delete mechanism is allowed: equality deletes, position deletes,
  deletion vectors or copy-on-write. So is skipping versions a newer one replaces.
- **The table already exists.** Staging registers it over the corpus's seed table, with
  identifier fields `(event_id, partition_key)` and the spec's `table.format_version`.
  Seed rows have negative IDs. An engine that can only write tables it creates cannot run
  an upsert run.
- **A pinned reader judges live state.** Verification reads every engine's table with
  Spark 4.1 and Iceberg 1.12. A table that uses a newer format feature fails verification.
  That is a harness limit, not an engine fault.
- **Copy-on-write costs scorer reads, not verdicts.** The scorer re-reads the `id` column
  of every file a commit adds, including rewritten ones. Verdicts use commit timestamps.
- **`facts.json` adds `upsert`:** `key_columns`, `version_column` (`id`), `seed_rows` and
  `seed_snapshot_id`. `partition_assignment` is `corpus`.
- **Rule 6 still applies.** Never expire snapshots or remove seed files: every run shares them.

## What the harness hands you

Staging writes the engine connection contract to `runs/<run_id>/facts.json`:

| Key | What to do with it |
|---|---|
| `run_id` | the run's name; also the topic's, and a good consumer-group id |
| `harness_commit` | the harness commit that staged the run, which the result records. Nothing to do |
| `track` | the results track the spec names, or `null`. Nothing to do |
| `bootstrap` | Kafka bootstrap servers |
| `topic` | the topic to consume, from its earliest offset |
| `corpus_uri` | the corpus directory. `<corpus_uri>/corpus.json` is where the column types and roles are published |
| `schema_avsc_uri` | column schema, including logical types used by JSON readers |
| `value_encoding` | `avro` for raw Avro binary, `confluent` for framed Avro, `json` for UTF-8 JSON objects |
| `schema_registry_url` | the registry the schema was registered with, or `null` |
| `schema_subject` | the subject it was registered under, or `null` |
| `schema_id` | the id every record's header names, or `null` |
| `compression` | the codec every batch is compressed with on the wire: `zstd`, `lz4`, `snappy`, `gzip` or `none` |
| `catalog_props` | Iceberg catalog properties, credentials redacted |
| `table` | the `namespace.table` to append to |
| `partition` | how the table is partitioned |
| `ddl` | the exact statement your table must match, or `null` when the harness already created it. Ask for the statement with `table.managed_by: engine` in the spec |
| `key_column` | the corpus column sent as the message key, or `null` for unkeyed records |
| `epoch` | `null` until launch; a shared future Unix timestamp in seconds for the producer and scorer |

Check that the consumer supports `compression`; an unsupported codec can
prevent it from reading any records. Supply catalog credentials separately;
`facts.json` redacts them.

### Confluent values

`value_encoding` defaults to `avro`: each value is one Avro binary record
encoded against `schema.avsc`, without a header. The three schema-registry fields
are then `null`.

With `confluent`, a five-byte header precedes that record: one zero byte, then
`schema_id` as a four-byte big-endian integer. Staging registers the schema under
`schema_subject` at `schema_registry_url`. The schema and id stay fixed for the
run. Consumers can either resolve the schema through the Confluent API or strip
the header and use `schema.avsc`. Supply any registry credentials separately.

Confluent framing leaves the rows, keys, table and scoring unchanged. Use
`specs/smoke/smoke-external-confluent.yaml` locally or
`specs/cluster-smoke/aws-smoke-external-confluent.yaml` on a cluster. Both managed engines also
support this framing.

### JSON values

With `value_encoding: json`, follow the [JSON representation rules](corpus.md#json-mode).
Preserve 64-bit integer precision, especially `id`. Registry fields are `null`.

## Walk-through: an engine the harness does not manage

This example stages a raw Avro external run and starts Flink manually. The files
in `docs/examples/external-flink/` define a Kafka source, catalog and `INSERT`,
with `@PLACEHOLDER@` tokens for run-specific names. For another engine, create
equivalent configuration using the same facts and corpus schema.

In the first shell, stage the run and wait for a file to appear:

```bash
rm -f /tmp/engine-ready                   # remove a marker from a previous run
scripts/smoke.sh --engine external --set duration_s=30 \
  --external-ready-file /tmp/engine-ready
```

It prints `facts.json` and then waits. In a second shell, fill the run's names
into the example config and start the engine:

```bash
cd <this checkout>
compose() { docker compose -f deploy/compose/local/docker-compose.yml "$@"; }
RUN_ID=<the run_id the first shell printed>
export RUN_DIR="$PWD/runs/$RUN_ID"

TABLE=$(jq -r .table "$RUN_DIR/facts.json")
sed -e "s|@RUN_ID@|$RUN_ID|g" \
    -e "s|@TOPIC@|$(jq -r .topic "$RUN_DIR/facts.json")|g" \
    -e "s|@NAMESPACE@|${TABLE%%.*}|g" \
    -e "s|@TABLE@|${TABLE#*.}|g" \
    docs/examples/external-flink/job.sql > "$RUN_DIR/job.sql"
sed -e "s|@RUN_ID@|$RUN_ID|g" \
    docs/examples/external-flink/flink-conf.yaml > "$RUN_DIR/flink-conf.yaml"

set -a; source docs/examples/external-flink/flink.env; set +a
compose --profile flink build flink-jobmanager
compose --profile flink up -d --scale "flink-taskmanager=$TASKMANAGERS"
compose --profile flink-job run --rm flink-job
touch /tmp/engine-ready
```

The first shell then replays the corpus, scores the table and prints the
verdict. Without `--external-ready-file`, it waits for a newline on stdin.
For cluster runs, `scripts/run.sh <spec> --external-ready-file <path>` provides
the same readiness check.

The scorer writes `runs/<run_id>/scores/summary.json`. See
[the verdict](methodology.md#the-verdict) for field definitions and
[publication rules](../results/README.md) before publishing a result.

Set `scoring.gate_window_s` to cover the engine's commit period. Longer windows
delay capacity decisions; isolated `UNDERSIZED` ticks may be commit jitter.
See [The gate](methodology.md#the-gate).

## On a cluster

Cluster runs use the same engine contract through the shell drivers. Copy
`specs/cluster-smoke/aws-smoke-external.yaml` or `specs/cluster-smoke/aws-smoke-external-confluent.yaml` and
replace the placeholder `fleet` entries with the resources your engine requests;
they determine the reported cost. The Confluent spec also requires
`site.kafka.schema_registry`. Staging rejects a missing registry before changing
cluster resources.

```bash
RUN_ID=$(scripts/stage.sh specs/cluster-smoke/aws-smoke-external-confluent.yaml | awk -F': ' '/^run_id: /{print $2}')
jq . "runs/$RUN_ID/facts.json"    # start your engine against these
scripts/launch.sh "$RUN_ID"       # once it is consuming: the scorer, then the offer
```

To automate this sequence, use
`scripts/run.sh <spec> --external-ready-file <ready-path>
--external-detached-file <detached-path>`. Remove both marker files before the
run, create the ready file when the reader is consuming, then stop the reader
and create the detached file when teardown asks. After stopping the offer and
scorer, teardown waits for this signal before dropping the topic. The wait is
bounded by `--external-detach-wait-s` (five minutes by default). On timeout,
teardown drops the topic and warns that an attached reader may restart. Without
a detached file, teardown gives the operator the same grace period. See
[running a benchmark](running.md) for the other drivers.
Always wait for `stage.sh` to return before starting an engine that resolves the
topic or table at startup: staging creates those resources.

## Tier 2: a managed engine

`engines/<name>/` owns each engine's integration. The shipped integrations provide
these files and generated artifacts:

| File | Purpose |
|---|---|
| `README.md` | runtime, supported settings and known limitations |
| `Dockerfile` | stock upstream image plus connector jars, pinned; built locally, and pushed to the operator's own registry by `scripts/push-images.sh` |
| `compose.yaml`, `compose.sh` | local services and the four `engine_compose_*` hooks used by `smoke.sh` |
| job source | the job the engine runs (SQL or Python) |
| `knobs.py` | run-spec keys the engine accepts, validation, rendering into the template and the DDL |
| `verify.py` | reads effective state from the running engine and fails staging on drift from the spec |
| `fleet.py` | declared role counts, machine types and sizing estimates; measured cluster resources come from pods |
| cluster manifests | custom resource and ConfigMap generated by `knobs.py` in the run directory |

Keep `knobs.py` independent of cluster access so operators can inspect and
compare rendered configuration before starting resources. Reject unknown keys
so misspelled settings cannot silently change a measured run.

Register the engine's knobs module in `MANAGED` in
`ingest_bench/specs/engines.py`. Run specs can then select it with
`engine: <name>` and a `<name>:` block of settings. The harness requires three
modules:

- `knobs.py` provides `validate(block, spec, meta)` to reject invalid settings
  and `render(spec, site, derived, meta, *, image_tag)` to return generated files
  keyed by filename. The keyword-only `image_tag` identifies the images for
  cluster runs and is `None` for a site with no cluster.
- `fleet.py` provides `fleet(spec)` to declare role counts, machine types and
  sizing estimates. Set `KUBERNETES.pod_role_label` and `KUBERNETES.fleet_selector`
  beside the engine renderer to capture actual cluster requests for cost.
- `verify.py` provides `verify(spec, run_id, fetch, ...)` to report one line per
  mismatch between the spec and the running engine, or an empty result when
  they agree.

For cluster runs, `knobs.py` also provides `KUBERNETES`, an `EngineKubernetes`
descriptor. It defines the resource kind, status and error paths, lifecycle and
running states, API Service, pod labels and maximum object-name length. Drivers
read it through `engine-k8s`, so adding an engine requires no engine-specific
branches in `scripts/`.

Compaction drivers register separately in `ingest_bench/specs/compactors.py`.
Their `knobs.py` declares a `CompactorKubernetes` descriptor with a
`fleet_selector`, `pod_role_label` and mapping from pod labels to fleet roles.
Shell drivers read its selector through `engine-k8s --compaction`; pod capture
uses the same descriptor to interpret roles.
