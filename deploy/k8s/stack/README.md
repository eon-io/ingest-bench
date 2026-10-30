# Run Kafka and the catalog inside Kubernetes

`setup.sh` installs Kafka through Strimzi and an Iceberg REST catalog through
Lakekeeper with Postgres. It prints the values needed for `site.yaml`;
`teardown.sh` removes the stack. Both check existing resources, so you can rerun
interrupted operations.

Use [site.k8s.example.yaml](../../../site.k8s.example.yaml) for this deployment.
The run drivers, engines, and scorer use the same interfaces as the managed
broker deployment. Manage the Kubernetes cluster, node groups, and add-ons
separately.

**On this page:** [Prerequisites](#prerequisites) · [Environment](#environment) · [Sizing brokers](#sizing-brokers) · [Setup](#set-up-the-stack) · [Cleanup](#tear-down-the-stack) · [Troubleshooting](#troubleshooting)

## Prerequisites

- **Kubernetes and object storage.** On AWS, use EKS with the Pod Identity agent
  and an accessible bucket. `deploy/aws/setup.sh --site site.k8s.example.yaml`
  can provision the bucket and image registry without creating MSK.
- **EBS CSI driver on AWS.** Broker and database volumes require
  `ebs.csi.aws.com`. Setup checks for it and suggests:

  ```bash
  eksctl create addon --cluster YOUR_CLUSTER_NAME --region YOUR_REGION \
    --name aws-ebs-csi-driver --auto-apply-pod-identity-associations
  ```

- **Dedicated broker nodes for measured runs.** Keep broker CPU and network
  activity off engine nodes. The [node group example](../../aws/eksctl-kafka-nodegroup.example.yaml)
  includes a broker taint, selector, and toleration. Dedicated nodes are optional
  for smoke checks. See [Sizing brokers](#sizing-brokers) for instance choice.
- **Engine operators.** Install the Flink or Spark operator needed by your run;
  `deploy/aws/setup.sh` installs both. The Spark operator must watch the stack
  namespace. Stack setup warns and prints an upgrade command if it does not,
  leaving shared operator configuration for you to change.
- **Host tools.** Use the tools listed in the [AWS guide](../../aws/README.md)
  plus `helmfile` 0.170 or later.

Setup and teardown read `--site PATH`, defaulting to `SITE_FILE` and then
`./site.yaml`, and require `kafka.deployment: in-cluster` before contacting AWS.
Use this site for shared AWS setup as well so it cannot provision MSK.

## Environment

| Variable | Default | What it is |
|---|---|---|
| `CLOUD` | *required* | `aws`; selects `deploy/aws/_stack_hooks.sh`. Other clouds are unsupported |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context |
| `NAMESPACE` | `ingest-bench` | Namespace for the stack and runs |
| `BUCKET` | *required* | The bucket with `corpus/`, `runs/` and `warehouse/` |
| `AWS_REGION`, `CLUSTER_NAME` | *required with `CLOUD=aws`* | AWS region and EKS cluster for Pod Identity associations |
| `STACK_ROLE_NAME` | `lakehouse-ingest-bench-stack` | IAM role for the catalog and unassociated run accounts. Give each cluster in a shared account its own name, and pass the same name to teardown |
| `KAFKA_BROKERS` | `3` | Dual-role broker/controller replicas. Replication is `min(3, brokers)`; minimum in-sync replicas is `max(1, replication − 1)` |
| `KAFKA_VOLUME_GI` | `500` | GiB per broker volume; rerunning setup with a larger value grows volumes, but a smaller value does not shrink them |
| `KAFKA_VOLUME_THROUGHPUT_MIBS` / `KAFKA_VOLUME_IOPS` | `250` / `6000` | Provisioned gp3 throughput and IOPS |
| `KAFKA_CPU` / `KAFKA_MEM_GI` | `4` / `16` | Broker requests. CPU is also the limit; memory has no limit, so the node can reclaim page cache under pressure. The heap is `KAFKA_JVM_HEAP` (`6g`) |
| `KAFKA_IO_THREADS` / `KAFKA_NETWORK_THREADS` | `8` / `3` | Broker request-handler and network threads (`num.io.threads`, `num.network.threads`). The defaults are Kafka's own and suit about four cores; raise them with `KAFKA_CPU` |
| `KAFKA_METRICS` | `false` | `true` exposes broker JMX metrics through Strimzi's Prometheus exporter and deploys its consumer-lag exporter |
| `KAFKA_NODE_SELECTOR` / `KAFKA_TOLERATIONS` | `{}` / `[]` | One-line JSON for broker placement |
| `CATALOG_NODE_SELECTOR` / `CATALOG_TOLERATIONS` | `{}` / `[]` | Placement for Lakekeeper, Postgres, and the schema registry |
| `STRIMZI_VERSION` | `1.2.0` | The operator chart, `oci://quay.io/strimzi-helm/strimzi-kafka-operator` |
| `LAKEKEEPER_CHART_VERSION` | `0.12.0` | From `https://lakekeeper.github.io/lakekeeper-charts/` |
| `WITH_SCHEMA_REGISTRY` | `false` | `true` also applies `deploy/k8s/schema-registry.yaml.tmpl`, for a run whose spec says `kafka.value_encoding: confluent` |
| `KAFKA_READY_WAIT_S` / `CATALOG_READY_WAIT_S` | `900` / `600` | Maximum readiness wait in seconds for Kafka and the catalog |

## Sizing brokers

Brokers need not match across compared runs. They only need to keep Kafka from
limiting the run. Retention is 48 hours, so the whole offer stays resident until the run
ends. [`preflight-kafka.sh`](../../../scripts/preflight-kafka.sh) enforces the
capacity margin below and waits out the previous run's topic deletion before
staging.

Only the engine reads the topic. The scorer reads the Iceberg table, not
Kafka. Offered rates count encoded bytes before compression, so treat the
offered rate `R` as an upper bound on wire bytes.

For an offered rate `R`, replication factor `RF = min(3, KAFKA_BROKERS)`, and
`B` brokers, each broker's share is:

| Resource | Formula | Why |
|---|---|---|
| Network in | `R × RF / B` | producer writes plus replica fetches |
| Network out | `R × RF / B` | replica fetch out (`R × (RF − 1) / B`) plus the engine's read (`R / B`) |
| Disk write | `R × RF / B` | each broker stores its share of every replica |
| Disk capacity | `R × duration × RF / B`, plus margin | the retained offer, with `preflight-kafka.sh`'s 20% margin |

Size against each instance's **baseline** (sustained) network and EBS
bandwidth, not the "up to" burst figures. Burst credits run out well before an
hour-long run ends. Stay under about 70% of baseline, leaving headroom for
replication catch-up and leftover deletion work. `KAFKA_VOLUME_THROUGHPUT_MIBS`
must also cover disk write, and helps only up to the instance's EBS baseline.

**Worked examples** (`RF = 3` throughout, with instance figures to check against
[current AWS specs](https://docs.aws.amazon.com/ec2/latest/instancetypes/gp.html)):

| Offered rate | Brokers | Per-broker net/disk write | Fits `m6i.2xlarge`? (baseline 3.1 Gbps net, 313 MB/s EBS) |
|---|---|---|---|
| 100 MB/s | 3 (default) | ~100 MB/s | yes, comfortably |
| 300 MB/s | 3 (default) | ~300 MB/s | no margin left |
| 600 MB/s | 5 | ~360 MB/s | no, exceeds the EBS baseline |
| 1 GB/s | 6 | ~500 MB/s | no, exceeds the EBS baseline |
| 1.38 GB/s (1 M rows/s) | 8 | ~520 MB/s | no, exceeds the EBS baseline |

`m6in.2xlarge` (baseline 12.5 Gbps net, 781 MB/s EBS) covers all five within
the 70% guideline. A reference shape that has carried 600 MB/s: 5 brokers on `m6in.2xlarge`
requesting 6 vCPU / 24 GiB, with 1600-GiB gp3 volumes at 500 MiB/s provisioned
throughput.

MSK users (`kafka.deployment: managed`) size `MSK_BROKER_TYPE`, `MSK_BROKERS`
and `MSK_VOLUME_GIB` with the same formulas. See the
[AWS guide](../../aws/README.md#sizing-the-cluster).

Configuration that avoids known failure modes:

- No broker memory limit. A limit equal to the request makes the kernel reclaim
  page cache under load, and produce latency grows.
- `num.replica.fetchers: 4` and 1 MiB socket buffers. A single fetcher with small
  buffers cannot keep replicas in sync at high rates, which delays every
  `acks=all` produce.
- gp3 throughput is fixed once the StorageClass is created. Changing
  `KAFKA_VOLUME_THROUGHPUT_MIBS` needs [teardown and setup](#tear-down-the-stack)
  to recreate the class.

To confirm Kafka was not the bottleneck, check that the run is not
`producer_bound` ([`behind_max_ms`](../../../docs/run-spec.md#producer), see
[Producer schedule](../../../docs/methodology.md#producer-schedule)). On the
consume path, check that broker network and disk stayed under baseline and
that consumer lag tracked engine capacity, not broker throughput. Set
`KAFKA_METRICS=true` on the first run at a new rate or broker shape to capture
this evidence.

## What setup creates

1. The namespace, three run service accounts, and engine RBAC, using the shared
   namespace manifest.
2. On AWS, the `STACK_ROLE_NAME` IAM role, scoped to the bucket's
   three prefixes. Pod Identity binds the catalog account and any unassociated
   run accounts to it. Existing run-account bindings remain unchanged. This
   role has no MSK permissions.
3. On AWS, the expandable gp3 StorageClass `ingest-bench-kafka`, with provisioned
   throughput, plus `ingest-bench-catalog` for Postgres at baseline 125 MiB/s
   and 3000 IOPS. Volumes are created in the scheduled pod's availability zone.
4. The `ingest-bench-catalog-keys` Secret: encryption key, database passwords,
   and warehouse credential external ID. Setup creates it once and preserves it
   on reruns so stored secrets remain readable.
5. Three Helm releases: Strimzi in `strimzi-operator`, Kafka 4.3.1 named
   `ingest-bench`, and Lakekeeper with Postgres. Strimzi is shared across stacks.
   Setup merges this namespace with the watches in Helm values and the live
   deployment, excluding deleted namespaces and the operator's namespace.
   This preserves peer RoleBindings and avoids upgrades failing on stale watches.
   Kafka uses an internal, unauthenticated listener on port 9092 and
   dual-role nodes; Lakekeeper also has authentication disabled. With
   `KAFKA_METRICS=true`, broker JMX metrics and consumer-group lag are exposed
   for a Prometheus installation to scrape.
6. The `ingest-bench` warehouse at `s3://$BUCKET/warehouse`. Credential vending
   and remote signing are disabled because pods access storage through their
   own identities. Hard deletes match `purge.sh`'s file cleanup.

## Set up the stack

```bash
export CLOUD=aws AWS_REGION=... CLUSTER_NAME=... BUCKET=...
export KAFKA_NODE_SELECTOR='{"lakehouse-ingest-bench/role":"kafka"}'
export KAFKA_TOLERATIONS='[{"key":"lakehouse-ingest-bench/kafka","operator":"Equal","value":"true","effect":"NoSchedule"}]'
deploy/aws/setup.sh --site site.k8s.example.yaml --write-site site.yaml
deploy/k8s/stack/setup.sh --site site.yaml
```

Build images and generate a corpus with `scripts/push-images.sh` and
`scripts/gen-corpus.sh`, as described in the [AWS guide](../../aws/README.md).
Existing images and corpus data can be reused. Then follow the
[per-run workflow](../../../docs/running.md).

Broker nodes and volumes continue billing while idle. See
[Sizing brokers](#sizing-brokers) for instance and volume choices. Teardown
removes the stack's volumes but leaves the node group.

## Broker storage between runs

For `kafka.deployment: in-cluster`, `stage.sh` runs `preflight-kafka.sh`
automatically. Besides checking topics and free space, it waits up to 20 minutes
for deletion from the previous run to finish. Brokers are quiet when the latest
sample finds no `*-delete` log directories and aggregate used space has changed
by less than 64 MiB since the previous sample 30 seconds earlier. Every sample
reports its pending-delete count and used KiB so a long deletion remains visible.

Set `--wait-brokers-s N` on `stage.sh` or `WAIT_BROKERS_S` to change the bound.
A value of `0` skips only the quiet wait; the topic and capacity checks still
run. A timeout refuses to stage the run rather than mixing topic deletion I/O
with the measured offer.

## Tear down the stack

```bash
deploy/k8s/stack/teardown.sh              # Kafka, catalog, namespace, and identities
deploy/k8s/stack/teardown.sh --all        # also Strimzi and both StorageClasses
deploy/k8s/stack/teardown.sh --all --yes  # same cleanup without confirmation
```

Teardown asks for confirmation before removing the listed resources. It deletes
Kafka while Strimzi is still running so the operator can remove broker claims,
then removes the catalog, namespace, and identities. Namespace deletion also
removes database claims and run objects.

The node group, CSI add-on, and bucket remain. Before deleting the shared AWS
IAM role, teardown checks Pod Identity associations on every cluster in every
enabled region and retains the role if any association uses it or it lacks
the `lakehouse-ingest-bench=true` ownership tag. Even with `--all`,
the Strimzi operator and both StorageClasses stay while another namespace runs
Kafka. Cleanup stops if it cannot check for these shared-resource users.
Strimzi's CRDs remain because Helm does not delete CRDs installed from `crds/`.

The dedicated catalog StorageClass changes Postgres's immutable StatefulSet
claim template. A normal Helm upgrade cannot apply it to a stack whose Postgres
claim uses the Kafka class. Recreate a disposable smoke stack before running the
new setup. To preserve a catalog, back up the database and migrate it to new
storage first; setup does not migrate catalog data. Teardown with `--all`
removes both StorageClasses.

## Troubleshooting

- **Pending persistent volumes:** check the CSI driver and node placement.
  `WaitForFirstConsumer` creates a volume in the scheduled pod's zone; the
  selector and tolerations must allow that pod onto a suitable node.
- **Catalog cannot access the bucket:** warehouse creation checks read/write
  access. A catalog pod started before its Pod Identity association may need
  a deployment restart, followed by another setup invocation. Also inspect the
  returned catalog error for permission or storage configuration problems.

### Broker metrics and request timing

Set `KAFKA_METRICS=true` when running setup to add Strimzi's standard Kafka JMX
exporter rules and the Kafka Exporter for consumer-group lag. The stack exposes
Prometheus endpoints but does not install Prometheus. Scrape the broker port
named `tcp-prometheus` and the `ingest-bench-kafka-exporter` Service.

When aggregate metrics cannot locate a produce-path stall, capture a short
request-logger burst on one broker. DEBUG logging is high volume, so restore the
default immediately after the 20-second sample. Repeat with the broker pod and
ID whose traffic you need:

```bash
(
set -e
POD=ingest-bench-brokers-0
BROKER_ID=0
KAFKA_CONFIGS=/opt/kafka/bin/kafka-configs.sh
restore_request_logger() {
  if ! kubectl -n ingest-bench exec "$POD" -c kafka -- "$KAFKA_CONFIGS" \
    --bootstrap-server localhost:9092 --entity-type broker-loggers \
    --entity-name "$BROKER_ID" --alter --delete-config kafka.request.logger \
    >/dev/null; then
    printf 'warning: kafka.request.logger may still be enabled on broker %s\n' \
      "$BROKER_ID" >&2
  fi
}
trap restore_request_logger EXIT

kubectl -n ingest-bench exec "$POD" -c kafka -- "$KAFKA_CONFIGS" \
  --bootstrap-server localhost:9092 --entity-type broker-loggers \
  --entity-name "$BROKER_ID" --alter --add-config kafka.request.logger=DEBUG
sleep 20
kubectl -n ingest-bench logs "$POD" -c kafka --since=30s \
  | sed -n 's/^.*Completed request:\({.*}\) (kafka.request.logger)$/\1/p' \
  >request-logger.jsonl
)
```

The JSON request records carry `requestApiKeyName`, `totalTimeMs`,
`requestQueueTimeMs`, `localTimeMs`, `remoteTimeMs` and `sendTimeMs`. On the
produce path, rising `remoteTimeMs` points to replication wait. Compare it with
replica-fetch `sendTimeMs` and ISR changes to distinguish slow follower fetches
from request-queue or local broker work.

## Cloud hooks

Cloud-specific behavior lives in `deploy/aws/_stack_hooks.sh`. It defines
preflight checks, identity binding and removal, StorageClass management, catalog
settings, and warehouse storage/credential JSON. Setup-only storage checks are
separate so teardown does not require a bucket or CSI driver that is already gone.

A future cloud implementation must supply the same nine hook functions and add
support for its storage URI scheme in the drivers. `CLOUD=gcp` is currently
unsupported.
