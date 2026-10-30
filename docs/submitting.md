# Submitting a result

The path from a shipped track to a published row. It assumes a working
cluster: see the [AWS guide](../deploy/aws/README.md) or the [in-cluster stack
guide](../deploy/k8s/stack/README.md) first. For the rules a result must
satisfy, see [publishing a result](../results/README.md). This page walks
through the steps to get there.

**On this page:** [Track and tier](#pick-a-track-and-tier) · [Spec](#write-the-spec) · [Cluster and Kafka](#set-up-the-cluster-and-kafka) · [Corpus](#generate-the-corpus) · [Images](#build-images) · [Run](#run-it) · [Notes](#write-the-notes) · [Publish](#regenerate-validate-and-open-a-pr) · [Reproduce](#reproduce-a-published-result)

## Pick a track and tier

Tracks live under `workloads/tracks/`. List them with `ls workloads/tracks/`.
A track fixes the corpus, Kafka block, codec, freshness bound, exactness and a
warmup ceiling, so rows compare only within one. See
[Tracks](run-spec.md#tracks) and [classes and
tiers](../results/README.md#tracks-classes-and-tiers).

A managed engine's tier is derived. A run is `reference` when it keeps the
engine's shipped `REFERENCE_KNOBS` and changes only the sizing knobs
(`SIZING_KNOBS`). See the [Flink](../engines/flink/README.md) or
[Spark](../engines/spark/README.md) guide. Any other configuration is `tuned`.
An external engine declares `tier: reference` in its spec when it ran its own
defaults with only resources changed, and reviewers check that claim.

## Write the spec

Copy a starting point:

- A shipped **reference spec**, `specs/tracks/<track>/*-ref.yaml`, where one
  exists for your engine. It carries the engine's `REFERENCE_KNOBS` at an
  untested fleet size.
- A **measured spec** under `specs/tracks/<track>/`, to compare configurations.
  Its fleet size may come from a short probe and is not proof of sustainable
  capacity. Give a copy a new `name`.

Set `track: <track name>` and drop the settings it fixes. `name` must match
`^[a-z0-9][a-z0-9-]{2,34}$`, shorter still for some engines and sidecars (see
[run keys](run-spec.md#run-spec)). Fill in real `machine_type` values. The
result records cost from `site.pricing` and the fleet's requests, following
the [common pricing rule](methodology.md#cost). For mixed hardware, configure
prices for every fleet type, including driver or JobManager overrides:

```yaml
pricing:
  machine_types:
    m8g.2xlarge: {vcpu: 8, gib: 32, usd_per_hour: 0.35904}
    m8g.4xlarge: {vcpu: 16, gib: 64, usd_per_hour: 0.71808}
    c8gn.4xlarge: {vcpu: 16, gib: 32, usd_per_hour: 0.948}
```

These [AWS catalog prices](https://pricing.us-east-1.amazonaws.com/offers/v1.0/aws/AmazonEC2/20261015191316/us-east-2/index.csv)
are for us-east-2, Linux, On-Demand, shared tenancy, effective October 1, 2026.
Prices are supplied offline. The harness makes no pricing API calls. Use either
the machine map or legacy flat rates, never both. Missing map entries fail staging before resources are created.

## Set up the cluster and Kafka

Provision the cluster, images and corpus bucket with the [AWS
guide](../deploy/aws/README.md) or the [in-cluster stack
guide](../deploy/k8s/stack/README.md). Keep brokers off engine nodes and size
them so they cannot bottleneck the run: see [sizing
brokers](../deploy/k8s/stack/README.md#sizing-brokers).

## Generate the corpus

```bash
scripts/gen-corpus.sh <preset> --shards N
```

Reuse an existing corpus for the same preset when one is already in the
bucket. See [generating](corpus.md#generating) to plan a corpus before writing
it, and [sizing the offer](corpus.md#sizing-the-offer) for an initial shard
count.

## Build images

```bash
scripts/push-images.sh
```

From a clean, committed checkout, without `--allow-dirty`. Publication
requires the full commit these images were built from (see the
[publication rules](../results/README.md#publication-rules)).

## Run it

```bash
scripts/run.sh specs/tracks/<track>/<your-spec>.yaml --publish results/
```

If the gate reports `UNDERSIZED`, grow the fleet and try again. Once a size
runs the full duration and passes, try a smaller one. Publish the smallest
fleet that ran the full duration and passed (see the
[publication rules](../results/README.md#publication-rules)). Use `--variant <name>` when
publishing more than one configuration for the same engine, class and tier.

## Write the notes

Add `results/<engine>/<preset>/<YYYY-MM-DD>/<HH-MM-SSZ>/README.md` beside the
result (required for external engines). Include:

- Cluster and site setup, in enough detail to reproduce the row.
- The broker shape, and any role that needed a node to itself.
- Smaller fleets you tried and how they failed, distinguishing short screens
  from full-duration attempts.
- For an external engine, the configuration files needed to rerun it.

See the [reviewer checklist](../results/README.md#reviewer-checklist) for
what a complete set of notes covers.

## Regenerate, validate, and open a PR

```bash
results-table results/ --out results/RESULTS.md
uv run python scripts/validate-results.py
```

Commit the spec, `result.json`, notes and the regenerated table together, and
open a pull request.

## Reproduce a published result

Copy the row's committed spec from `specs/tracks/<track>/`, check out its
`run.harness_commit`, rebuild images with `scripts/push-images.sh`, and run
it. Results from before that field existed (schema version 3) record no commit.
For a managed engine, the image tag in the row's Version column names the
commit its images were built from.
