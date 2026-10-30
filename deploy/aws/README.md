# Running the benchmark on AWS

`setup.sh` provisions the shared AWS resources needed by benchmark runs.
`teardown.sh` removes them. Both check existing resources, so you can rerun an
interrupted operation. Pending Helm operations need
[recovery](#remove-shared-resources) before retrying. Manage the EKS cluster
separately; neither script creates, reconfigures, or deletes it.

**On this page:** [Prerequisites](#prerequisites) · [Sizing](#sizing-the-cluster) · [Setup](#run-setup) · [Cleanup](#remove-shared-resources) · [EKS](#create-an-eks-cluster)

## Prerequisites

- **EKS cluster.** Build the benchmark images for the cluster's node
  architecture. For a new cluster, use the [EKS example](#create-an-eks-cluster).
- **Host tools.** Scripts check required tools before proceeding. `run.sh`
  checks the tools needed by all five drivers it invokes.

  | Tool | Needed by |
  |---|---|
  | `aws` CLI v2 | setup, teardown, and all run drivers except `launch.sh` |
  | `kubectl` | setup, teardown, and run drivers that access Kubernetes; `finish.sh` also requires it for catalog tunnels |
  | `helm` | `setup.sh` / `teardown.sh`, for the two operators |
  | `envsubst` (GNU gettext) | `setup.sh`, for the IAM and namespace templates |
  | `yq` (mikefarah v4) | every driver, to read the site configuration and copied run spec |
  | `jq` | every driver that reads a run's facts or an object's status |
  | `git` | drivers that resolve image tags from the Git commit |
  | `curl` | `stage.sh` for engine verification; catalog tunnels also probe readiness with it |
  | `docker` | `push-images.sh` |
  | `gzip` | `teardown.sh` and `purge.sh`, to read compressed table metadata |
- **AWS credentials.** Use an identity in the cluster's account with permission
  to manage S3, ECR, MSK, security groups, IAM roles, EKS add-ons, and Pod Identity
  associations. Scripts use the AWS CLI's ambient credentials without setting
  a profile. Export `AWS_PROFILE` or sign in with
  `aws sso login --profile <name>` or `aws login`, then verify the identity with
  `aws sts get-caller-identity` in each shell. Setup and teardown check credentials
  immediately; other drivers report credential failures on their first AWS call.

## Sizing the cluster

Kubernetes schedules pods by resource requests. Budget for these requests:

| Pod | How many | CPU | Memory |
|---|---|---|---|
| Scorer | one per run | `2` | `SCORER_MEMORY` (`4Gi`) |
| Producer shard | `producer.shards` | `2` | `PRODUCER_MEMORY` (`3Gi`) |
| Corpus generator | `gen-corpus.sh --shards` | `1` | `GEN_MEMORY` (`10Gi` for standard presets, `3Gi` otherwise) |
| Staging and other harness Jobs | one at a time | `500m` | `1Gi` |
| Schema registry | one when `WITH_SCHEMA_REGISTRY=true` | `200m` | `512Mi` |
| Flink jobmanager | one | `jm_cpu` (`1`) | `jm_mem_mb` |
| Flink taskmanager | `taskmanagers` | `tm_cpu` | `tm_mem_mb` |
| Spark driver | one | `driver_cores` (`1`) | approximately `1.4 × driver_mem_mb` |
| Spark executor | `executors` | `executor_cores` | `executor_mem_mb + executor_overhead_mb`, or approximately `1.4 × executor_mem_mb` when the overhead is unset |

Harness requests come from `deploy/k8s/`; engine requests come from
[Flink](../../engines/flink/knobs.py) and [Spark](../../engines/spark/knobs.py)
knobs. Spark memory includes JVM heap plus overhead (40% by default, subject to
minimums and configuration overrides). Budget for full pod requests, which
also determine collected resource costs.

A node fits `floor((allocatable CPU − DaemonSet requests) / 2)` two-CPU pods.
A four-vCPU node has less than four allocatable cores, so it fits only one.
At the shipped sizes, CPU generally limits placement before memory. See
[pitfalls](../../docs/pitfalls.md) for the scheduling consequences.

Count the engine fleet, scorer, and producer shards together. Corpus generation
and the staging Job finish before that peak.

- `specs/cluster-smoke/aws-smoke-flink.yaml` needs **four** four-vCPU nodes:
  two TaskManagers, one producer, and one scorer, with the one-CPU JobManager
  sharing a node. `aws-smoke-spark.yaml` has the same requirement.
- The standard [`aws-1mrps-skew-flink-ref.yaml`](../../specs/tracks/events-1mrps-skew/aws-1mrps-skew-flink-ref.yaml)
  is a starting configuration, not a measured fleet size. Count its engine,
  producer and scorer requests against the allocatable capacity of the nodes
  you intend to use before scaling the node groups.

Raise the node group's maximum as well as its desired size. For example, for a
six-node group:

```bash
eksctl scale nodegroup --cluster <name> --name amd64 --nodes 6 --nodes-max 6
```

Give TaskManagers or executors too big for that node type their own dedicated
group at a bigger machine type instead, as the dedicated Kafka node group does.

`launch.sh` warns if too few nodes have two free CPUs for the scorer and
producers. The engine fleet is already running at this point. The warning does
not block launch because Pending pods can trigger a cluster autoscaler.

Kafka brokers are sized separately from these pod requests, on network and
disk bandwidth rather than CPU. See
[Sizing brokers](../k8s/stack/README.md#sizing-brokers). The same formulas set
`MSK_BROKER_TYPE`, `MSK_BROKERS`, and `MSK_VOLUME_GIB` below for
`kafka.deployment: managed`.

## Configure the environment

Pass site-specific values through environment variables:

| Variable | Default | What it is |
|---|---|---|
| `AWS_REGION` | *required* | The region the EKS cluster is in |
| `CLUSTER_NAME` | *required* | The EKS cluster's name |
| `KUBE_CONTEXT` | `$CLUSTER_NAME` | The kubeconfig context. Written with `aws eks update-kubeconfig --alias` if it is missing |
| `BUCKET` | `lakehouse-ingest-bench-<account id>` | Bucket for `corpus/`, `runs/`, and `warehouse/`; the account ID helps avoid global name collisions |
| `MANAGE_BUCKET` | `true` | `false` uses an existing bucket you own: setup only checks access and teardown never deletes it. See [Ownership tag](#ownership-tag) |
| `HARNESS_ROLE_NAME` | `lakehouse-ingest-bench-harness` | IAM role for harness and engine pods. Give each cluster in a shared account its own name, and pass the same name to teardown |
| `SITE_FILE` | `./site.yaml` | Site configuration read by setup and teardown; `--site PATH` overrides it |
| `MSK_NAME` | `lakehouse-ingest-bench` | MSK cluster name; its security group is `<name>-msk` |
| `MSK_BROKER_TYPE` | `kafka.m5.large` | Broker instance type |
| `MSK_BROKERS` | `2` | Broker count; requires this many availability zones with private subnets |
| `MSK_KAFKA_VERSION` | newest `ACTIVE` `3.x` | Kafka version; setup prints the selected version |
| `MSK_VOLUME_GIB` | `100` | EBS GiB per broker. Increase for longer offers or larger backlogs. Existing volumes can grow with `update-broker-storage`, but cannot shrink |
| `FLINK_OPERATOR_VERSION` | `1.15.0` | Flink operator chart reconciled from `archive.apache.org` |
| `SPARK_OPERATOR_VERSION` | `2.5.2` | Kubeflow operator chart reconciled on each setup; configured to watch `$NAMESPACE` and use the benchmark's Spark identity |
| `OPERATORS` | `flink spark` | Engine operators that setup installs and `teardown.sh --all` removes. Leave one out to keep an operator you already run; see [Existing operators](#existing-operators) |
| `NAMESPACE` | `ingest-bench` | Namespace shared by harness Jobs and engine runs |
| `WITH_SCHEMA_REGISTRY` | `false` | Create an in-memory Apicurio registry for Confluent runs at `http://schema-registry.<namespace>.svc:8080/apis/ccompat/v7`; removed with the namespace |
| `NODE_SELECTOR` / `TOLERATIONS` | `{}` / `[]` | One-line JSON for registry placement. Harness Jobs read placement from `site.yaml` |
| `MSK_ACTIVE_WAIT_S` | `3600` | How long `setup.sh` waits for MSK to reach `ACTIVE` |
| `MSK_DELETED_WAIT_S` | `1800` | How long `teardown.sh` waits for MSK to disappear before deleting its security group |

## Provision shared resources

Setup first checks credentials, cluster access, and node architectures. It
installs the Pod Identity agent if absent and waits for it to become active.
It also reconciles the Flink operator release, disabling its
validating webhook so cert-manager is not required.

### Existing operators

Setup installs or upgrades both engine operators by default. To keep one you
already run, leave it out of `OPERATORS`, for example `OPERATORS=spark` or
`OPERATORS=` for neither. Setup then leaves that operator unchanged and warns
if its CRD is missing. Pass the same `OPERATORS` to `teardown.sh` so `--all`
does not remove it.

A Flink Kubernetes Operator you provide must support `flinkVersion: v2_2` and
watch `$NAMESPACE`. A Kubeflow spark-operator must watch `$NAMESPACE` with its
webhook enabled, which mounts run ConfigMaps into driver and executor pods.

### Resources created

Setup then provisions:

- **VPC:** S3 gateway endpoint routing on every route table in the cluster VPC.
  Setup leaves existing endpoints' routes in place and covers the remaining
  route tables with its own tagged endpoint, extending it on reruns. Same-region S3 traffic bypasses
  NAT processing charges; gateway endpoints have no hourly or per-byte charge.
- **S3:** a bucket with public access blocked and the benchmark ownership tag.
  It suspends enabled versioning to avoid retaining billable deleted corpus
  data. An existing bucket must carry `lakehouse-ingest-bench=true`, unless
  `MANAGE_BUCKET=false` names a bucket you keep managing yourself.
- **ECR:** the `lakehouse-ingest-bench/harness`, `lakehouse-ingest-bench/flink`,
  and `lakehouse-ingest-bench/spark` repositories.
- **MSK (only for `kafka.deployment: managed`):** a provisioned cluster in the
  EKS VPC's private subnets, with one broker per availability zone, IAM
  authentication, and TLS. Its security group allows port 9098 from all VPC
  CIDRs, covering node and pod ranges; IAM controls client authorization.
- **IAM:** the `HARNESS_ROLE_NAME` role, trusted by
  `pods.eks.amazonaws.com` for `sts:AssumeRole` and `sts:TagSession`. Its inline
  policy covers the bucket's three prefixes, the `ingest_bench` Glue database
  except for in-cluster sites, and, for managed Kafka, the MSK cluster's topics
  and consumer groups. Pod Identity binds unassociated benchmark service
  accounts to the role; existing bindings remain unchanged. Setup refuses an
  existing role without the ownership tag rather than replace its policy.
- **Kubernetes:** the namespace, harness/Flink/Spark service accounts, and
  engine RBAC. The Spark operator is installed after the namespace because its
  chart creates a Role there. It watches `spark.jobNamespaces={$NAMESPACE}`
  and uses the benchmark Spark identity. Setup enables its webhook to mount
  run ConfigMaps in driver and executor pods, including on existing releases.
- **Optional registry:** with `WITH_SCHEMA_REGISTRY=true`, an in-memory
  Apicurio Deployment and Service, followed by a rollout wait.

### Deployment mode and site file

Setup and teardown read `kafka.deployment` from `--site PATH`, defaulting to
`SITE_FILE` and then `./site.yaml`, before contacting AWS. The value is required
and must be `managed`, `in-cluster`, or `external`. Only `managed` permits MSK
and its security-group operations. The other modes omit MSK permissions and
readiness waits. Environment variables cannot override this choice.

### Ownership tag

Setup tags every bucket, role, ECR repository, MSK cluster, security group
and endpoint it creates with `lakehouse-ingest-bench=true`. It reconfigures,
and teardown deletes, only resources carrying that tag. Setup stops when a
bucket, role, MSK cluster or MSK security group with the configured name lacks
the tag, so a name collision never changes something unrelated. Teardown keeps
untagged resources and says so.

To use a bucket you already own without handing it to the benchmark, set
`MANAGE_BUCKET=false` for both setup and teardown. Setup checks access and
leaves the bucket's tags, public access block and versioning unchanged.
Teardown keeps the bucket and its data, so remove the benchmark prefixes
yourself when you are done. With versioning enabled, deleted corpus and table
files stay billable as noncurrent versions.

### Run setup

`--write-site PATH` refuses to overwrite an existing file. Use the matching
example as input: managed generates MSK/Glue addresses, in-cluster generates
Kafka/Lakekeeper addresses, and external preserves the input Kafka/catalog
blocks. Replace external `YOUR_` placeholders first. Generated prices are zero;
fill them in before publication.

```bash
export AWS_REGION=... CLUSTER_NAME=...
deploy/aws/setup.sh --site site.aws.example.yaml --write-site site.yaml
scripts/push-images.sh                    # harness and engine images, tagged with this commit
scripts/gen-corpus.sh smoke --shards 4     # generate a corpus in the bucket
```

Without `--write-site`, update your site from the printed values.

`push-images.sh` builds all images for `linux/amd64` by default. Use
`--platform linux/arm64` or a comma-separated platform list to change this.
Image tags identify commits, so the script rejects uncommitted changes unless
`--allow-dirty` is set.

Choose corpus presets and shard counts using the [corpus guide](../../docs/corpus.md),
then follow the [per-run workflow](../../docs/running.md).

> **Cloud cost:** MSK bills while idle and can take 15–30 minutes to become active.
> Check regional pricing and tear it down between campaigns. Rerunning setup
> reuses the cluster and can grow broker storage when requested.

### Upsert runs

An upsert run starts from its corpus's seed, so build both before staging.
`gen-corpus.sh` writes the seed after the corpus: sharded seed Jobs, then one
commit Job that publishes `seed.json`. A table seed is a Parquet table every
run's table is registered over. A Kafka seed is sent by `launch.sh` and must
commit before the epoch, within `SEED_WAIT_S` (3600 s).

```bash
scripts/gen-corpus.sh smoke-upsert                # cluster smoke corpus and table seed
scripts/gen-corpus.sh smoke-upsert-orders         # orders smoke corpus and Kafka seed
scripts/gen-corpus.sh events-50mbs-upsert-orders --shards 8   # upsert track corpus and Kafka seed
scripts/gen-corpus.sh --seed-only s3://<corpus_root>/<corpus> --seed-shards N   # retry only the seed
```

- **Kafka seed time.** `events-50mbs-upsert-orders` sends 20M 1 KB seed rows (20 GB) at the
  track's 50 MB/s: about 7 minutes per run before the epoch.
- **Keep the seed.** Every run's table references a table seed's files, and a published seed
  is never rewritten. Deny deletes under the seed prefixes in the bucket policy. Merge this
  statement into the existing policy (`aws s3api get-bucket-policy`), since
  `put-bucket-policy` replaces it. Remove the statement before deleting a corpus or the bucket.

  ```json
  {
    "Sid": "KeepUpsertSeeds",
    "Effect": "Deny",
    "Principal": "*",
    "Action": ["s3:DeleteObject", "s3:DeleteObjectVersion"],
    "Resource": "arn:aws:s3:::<bucket>/<corpus prefix>/*/seed/*"
  }
  ```

- **Spark verifies every engine.** `finish.sh` reads live state with a Spark scan Job, so
  even a Flink run needs the pushed Spark image and the `ingest-bench-spark` service account
  that setup creates. A Flink table can still hold equality deletes the job has not yet
  converted, which only that Job reads: pass `--verify-in-cluster` to `run.sh` or `finish.sh`.
- **One offer per run.** A restarted producer shard refuses to replay, so stage a fresh run
  for every attempt.
- **Short probes.** Set `producer.seconds` below the corpus duration to try a fleet quickly.
  Verification then expects the offered batches only. Sample offsets after the
  short offer are absent, so the scorer checks the final snapshot. A short run
  can rule out a fleet, but cannot establish sustained capacity because merge
  and delete-file costs can grow with run duration.

Start with the cluster smoke. It needs the four four-vCPU nodes of `aws-smoke-flink`. The
scan Job starts after teardown, on a freed node, so shrink it from its 8 CPU and 32 GiB default:

```bash
VERIFY_SPARK_CPU=2 VERIFY_SPARK_MEMORY=4Gi \
  scripts/run.sh specs/cluster-smoke/aws-smoke-upsert-flink.yaml --verify-in-cluster
```

Size scorer, verifier and scan Jobs for the corpus's key count and live-state
checkpoints. The `VERIFY_SPARK_CPU` and `VERIFY_SPARK_MEMORY` defaults are 8
and 32Gi; check them against the chosen corpus before running.

## Remove shared resources

```bash
deploy/aws/teardown.sh              # namespace and identity; MSK when managed
deploy/aws/teardown.sh --all        # also ECR, both operators, the S3 endpoint and the bucket
deploy/aws/teardown.sh --all --yes  # same cleanup without the bucket prompt
```

Teardown stops before contacting AWS if the site is missing or lacks a valid
`kafka.deployment`. If the original file is unavailable, select the example
matching the deployment you are removing:

```bash
deploy/aws/teardown.sh --site site.aws.example.yaml  # managed Kafka: includes MSK
deploy/aws/teardown.sh --site site.k8s.example.yaml  # in-cluster Kafka: skips MSK
```

Run only the matching command. Teardown reads only the deployment mode from the
site; keep `AWS_REGION`, `CLUSTER_NAME`, and any resource-name overrides set to
their original values. For external Kafka, use a file containing
`kafka: {deployment: external}`. No completed site configuration is required.

### Decommission a cluster

To remove everything, run these steps in order with the nodes still running:

```bash
CLOUD=aws deploy/k8s/stack/teardown.sh --all     # in-cluster Kafka only
deploy/aws/teardown.sh --all
eksctl delete cluster -f <your eksctl cluster config>
```

The stack teardown needs Strimzi and the EBS CSI driver running to delete the
broker and catalog volumes. If the node groups are already scaled to zero,
those volumes outlive the cluster and keep billing. Find them as `available`
volumes tagged `kubernetes.io/created-for/pvc/name` and delete them by hand.
Shared teardown removes the tagged S3 gateway endpoint and, for managed Kafka,
the MSK security group. Otherwise either one blocks VPC deletion and leaves
the eksctl stack in `DELETE_FAILED`.

### Ownership and deletion order

Cleanup first stops workloads and removes their Pod Identity associations.
Cluster and namespace lookup failures stop cleanup; only confirmed absence skips them.
For managed Kafka, delete MSK, then remove its security group after network
interfaces release it. Security-group lookup requires the original EKS VPC,
group name, and benchmark ownership tag. If EKS is already gone, supply its
original `VPC_ID` when rerunning teardown; cleanup refuses to select a group
without that scope, and skips the S3 gateway endpoint without it.

The shared IAM role is checked after broker cleanup. Deletion requires the benchmark
ownership tag and no remaining Pod Identity associations in any enabled region.
This check needs read access to regions, clusters and associations. A failed role
lookup preserves the role and stops the remaining cleanup with a nonzero exit status.

Without `--all`, the bucket, images, operators and the S3 gateway endpoint
remain available for another campaign or other workloads.

With `--all`, teardown removes the tagged S3 gateway endpoint after the operators;
untagged endpoints stay. Remove any remaining endpoints before asking
`eksctl delete cluster` to remove the VPC. Bucket deletion happens last and
requires the benchmark ownership tag plus confirmation. It removes every corpus,
run artifact, and warehouse object. `--yes` supplies confirmation for unattended use. By the time the prompt
appears, the images and operators have already been removed. Declining keeps
the bucket but does not restore those resources.
With `MANAGE_BUCKET=false`, teardown skips the bucket entirely.

### Operator recovery

Helm retains both engine operators' CRDs after uninstall. Setup uses
`helm upgrade --install --wait` to restore the controller releases and wait for
their workloads, even when those CRDs remain. Do not delete CRDs to repair a
missing operator: doing so also deletes custom resources using them. Failed
releases can be reconciled on rerun. If setup reports a pending Helm operation,
inspect `helm status` and `helm history` in the operator namespace and finish or
recover that operation before rerunning setup.

The EKS cluster and `eks-pod-identity-agent` add-on remain installed.

## Template conventions

AWS IAM documents and `k8s/namespace.yaml.tmpl` use `${NAME}` placeholders,
rendered by Bash with `envsubst`. Harness Kubernetes templates use `__NAME__`
markers, rendered in Python. Separate delimiters keep embedded shell or Helm
syntax intact.

## Create an EKS cluster

`eksctl-cluster.example.yaml` creates four `m6i.xlarge` amd64 nodes in the
private subnet of one availability zone, the Pod Identity agent, and a VPC across
three availability zones with one NAT gateway. This fits the smoke workload
described above. Replace the three placeholders first, including `YOUR_ZONE`.

Setup logs the current nodes' zones and warns if they span multiple zones;
it does not change existing node-group placement.

Use the same `YOUR_ZONE` for all benchmark node groups to avoid cross-AZ charges
between in-cluster brokers, engines and scorers. These templates cover the
smoke-sized amd64 group and dedicated Kafka group; configure other groups
separately. Managed MSK brokers still span multiple zones.

The S3 gateway endpoint bypasses NAT for same-region S3 traffic. Other traffic,
including image registry requests and AWS APIs without VPC endpoints, still
uses NAT; its cost depends on usage.

```bash
eksctl create cluster -f deploy/aws/eksctl-cluster.example.yaml   # about 20 minutes
eksctl delete cluster -f deploy/aws/eksctl-cluster.example.yaml
```

Run these commands yourself; setup and teardown do not invoke `eksctl`.

## In-cluster AWS resources

The [in-cluster stack](../k8s/stack/README.md) uses Kafka and Lakekeeper in
place of MSK and Glue. Its AWS hooks create a bucket-scoped
`STACK_ROLE_NAME` role (default `lakehouse-ingest-bench-stack`) for the catalog and any run service accounts
that are not already associated. They also create separate gp3 StorageClasses
for Kafka and catalog Postgres and require the EBS CSI driver.

Use `eksctl-kafka-nodegroup.example.yaml` for dedicated broker nodes. The AWS
setup supplies the bucket, image registry, and engine operators without MSK:

```bash
deploy/aws/setup.sh --site site.k8s.example.yaml --write-site site.yaml
CLOUD=aws deploy/k8s/stack/setup.sh
```

The generated Kafka and Lakekeeper addresses match the stack defaults. Use the
same namespace in both steps so the Spark operator watches the run namespace.
Both teardown scripts read the same site; stack commands require
`kafka.deployment: in-cluster`.

An existing MSK cluster is not removed when the site selects `in-cluster`. If an
earlier setup created one that no run uses, delete the cluster named by
`MSK_NAME`, wait for its network interfaces to disappear, then delete its tagged
`<MSK_NAME>-msk` security group. Rerunning shared setup with the in-cluster site
removes the `MskCluster`, `MskTopics`, `MskGroups`, and `GlueIcebergCatalog`
statements from the harness role. Do not use shared teardown for selective MSK
cleanup: it also removes the run namespace and identities.
