# Contributing

## Required checks

Install Java 17 and Maven for the Flink SQL runner checks, plus `jq` and `yq`
(mikefarah v4) on `PATH`. Cluster-driver tests skip when `jq` or `yq` is unavailable.
Then run the same checks as CI:

```bash
uv sync --frozen
mvn -B -f engines/flink/sql-runner/pom.xml verify
uv run ruff check .
uv run ruff format --check .
uv run mypy
uv run pytest -q -m "not integration"
uv run python scripts/validate-results.py
```

Integration tests require the local Compose stack and are excluded above.
Run `scripts/smoke.sh` for each affected engine when changing
`deploy/compose/`, `engines/*/compose.yaml`, `scripts/smoke.sh`, or an engine
image. The `smoke.yml` CI workflow runs only on manual dispatch.

Use Python 3.12+, strict mypy typing and ruff's 120-column limit. Do not
swallow exceptions or supply defaults for required values. Every `.py` and
`.sh` file needs an `SPDX-License-Identifier: CC-BY-NC-SA-4.0` header, after
the shebang if present. Preserve notices and licenses on third-party material.

## Keep private details out of the repository

`tests/test_public_surface.py` scans tracked files for account and project IDs
and non-placeholder storage buckets.
Use reproducible placeholders and fixtures instead of private infrastructure
details.

Comments should explain mechanisms, rationale and constraints. Keep incident
history, dates and cluster names out of prose. Put measurements in
`docs/examples/` beside their supporting artifacts, or in `results/`.

## Add a corpus shape

Add a schema JSON file under `workloads/schemas/` and a preset YAML file under
`workloads/presets/`. [`docs/corpus.md`](docs/corpus.md) defines column kinds,
roles, nullable and nested columns, preset keys, value-space constraints and
generation checks. `workloads/schemas/kinds.json` declares every kind.

The corpus hash identifies the effective preset, seed, generator version and
row-block size. Reuse the generated corpus across compared runs. Publish the
schema and preset in the same change as any result that uses them; see
[corpus identity](docs/corpus.md#generating) before regenerating stored data.

Preview sizing without writing a corpus:

```bash
uv run gen-corpus --preset <name> --out <uri> --plan
```

## Add a track

A track turns a preset into a results section. Add `workloads/tracks/<name>.yaml`
fixing the corpus, Kafka workload, codec, freshness bound, exactness and warmup
ceiling, as the shipped tracks do, and file its specs under `specs/tracks/<name>/`.
Never edit a track that has published results, since that changes what they
mean. Add a new track instead.

## Add a column kind

`KINDS` in `ingest_bench/corpus/columns.py` is the one registry of column kinds.
Each entry validates its declaration and types its values for Avro, Iceberg,
Flink, Spark and JSON. Schema files, table creation, engine DDL, readers and
the scorer derive their types from it. A new kind ships with:

- a registry entry, plus any new declaration attribute in `declaration()`,
  omitted at its default so existing corpus hashes stay unchanged
- a generator in `ingest_bench/corpus/values.py`, deterministic by row position
- tests for validation, generation, Avro and JSON encoding, and type mapping,
  following `tests/test_kinds.py`
- a column in `workloads/schemas/kinds.json` and a passing
  [smoke](docs/running.md#encoding-variants) on each managed engine, in Avro
  and JSON. Where an engine cannot write the kind, reject it in that engine's
  knob validation and list it under [engine support](docs/corpus.md#engine-support).

`tests/test_preset.py` pins the hashes of presets with published results.

## Add an engine

Follow the [external or managed integration contract](docs/adding-an-engine.md).
Keep engine-specific behavior in `engines/<name>/`.

## Add a preset key or engine knob

Loaders reject unknown keys. Add the loader entry, default, test and reference
entry in [`docs/run-spec.md`](docs/run-spec.md) or the engine README. Define
defaults in the loader and avoid duplicating them in callers. A knob that only
sizes the fleet belongs in the engine's `SIZING_KNOBS`. Any other knob a run sets
moves it to the `tuned` tier unless `REFERENCE_KNOBS` sets it.

## Add a result

Follow [submitting a result](docs/submitting.md) and the
[publication rules](results/README.md#publication-rules). Never hand-edit generated
JSON or `results/RESULTS.md`. CI rejects a stale table; regenerate it with
`results-table results/ --out results/RESULTS.md`.

## Write documentation

Keep each document focused on its audience and purpose.
`tests/test_doc_sizes.py` enforces line budgets. Before adding an explanation,
search for an existing one and link to it where possible. Define benchmark
terms in [`docs/methodology.md`](docs/methodology.md).

## Commit messages

Use `type: subject`, with an imperative subject and no scope, for example:
`fix: reject client properties that override the wire codec`.
Explain the change in the subject and the rationale in the body.
