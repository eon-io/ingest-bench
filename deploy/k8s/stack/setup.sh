#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Provision an in-cluster Kafka and Lakekeeper stack, its identities, storage,
# and warehouse; then print site.yaml values. Check existing resources so
# interrupted setup can resume. CLOUD selects deploy/<cloud>/_stack_hooks.sh.
# Manage the cluster, node groups, and add-ons separately.
set -euo pipefail
PREREQ_DOC="deploy/k8s/stack/README.md"
STACK_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$STACK_DIR/../../../scripts/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$REPO_ROOT/scripts/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: deploy/k8s/stack/setup.sh [--site PATH]

  --site PATH  read kafka.deployment (default: SITE_FILE or ./site.yaml)

  The site must set kafka.deployment: in-cluster. Configure resources through
  environment variables:
  CLOUD=aws selects AWS; KUBE_CONTEXT selects the cluster; NAMESPACE selects
  the stack and run namespace; BUCKET selects the storage bucket.

  Set broker capacity with KAFKA_BROKERS, KAFKA_VOLUME_GI, KAFKA_CPU, and
  KAFKA_MEM_GI, and request-handler pools with KAFKA_IO_THREADS and
  KAFKA_NETWORK_THREADS. Set KAFKA_METRICS=true to expose JMX and consumer-lag metrics.
  Set placement with KAFKA_NODE_SELECTOR, KAFKA_TOLERATIONS,
  CATALOG_NODE_SELECTOR, and CATALOG_TOLERATIONS.
  See deploy/k8s/stack/README.md for all variables and defaults.
USAGE
}

SITE_FILE="${SITE_FILE:-./site.yaml}"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		printf 'unknown argument %s\n\n' "$1" >&2
		usage >&2
		exit 2
		;;
	esac
done

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------

# Reject unsupported clouds before checking unrelated host tools.
CLOUD="${CLOUD:-}"
case "$CLOUD" in
aws) ;;
gcp) die "CLOUD=gcp is not supported yet; use CLOUD=aws. See $PREREQ_DOC" ;;
"") die "CLOUD must be set; only aws is supported. See $PREREQ_DOC" ;;
*) die "CLOUD is '$CLOUD'; only aws is supported. See $PREREQ_DOC" ;;
esac

KAFKA_BROKERS="${KAFKA_BROKERS:-3}"
KAFKA_VOLUME_GI="${KAFKA_VOLUME_GI:-500}"
KAFKA_CPU="${KAFKA_CPU:-4}"
KAFKA_MEM_GI="${KAFKA_MEM_GI:-16}"
KAFKA_JVM_HEAP="${KAFKA_JVM_HEAP:-6g}"
KAFKA_IO_THREADS="${KAFKA_IO_THREADS:-8}"
KAFKA_NETWORK_THREADS="${KAFKA_NETWORK_THREADS:-3}"
KAFKA_METRICS="${KAFKA_METRICS:-false}"
# Use explicit empty JSON objects to avoid brace parsing in ${VAR:-...}.
KAFKA_NODE_SELECTOR="${KAFKA_NODE_SELECTOR:-}"
[[ -n $KAFKA_NODE_SELECTOR ]] || KAFKA_NODE_SELECTOR='{}'
KAFKA_TOLERATIONS="${KAFKA_TOLERATIONS:-[]}"
CATALOG_NODE_SELECTOR="${CATALOG_NODE_SELECTOR:-}"
[[ -n $CATALOG_NODE_SELECTOR ]] || CATALOG_NODE_SELECTOR='{}'
CATALOG_TOLERATIONS="${CATALOG_TOLERATIONS:-[]}"
# Pin chart versions so rendered CRD fields remain compatible across a campaign.
STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
LAKEKEEPER_CHART_VERSION="${LAKEKEEPER_CHART_VERSION:-0.12.0}"
WITH_SCHEMA_REGISTRY="${WITH_SCHEMA_REGISTRY:-false}"
KAFKA_READY_WAIT_S="${KAFKA_READY_WAIT_S:-900}"
CATALOG_READY_WAIT_S="${CATALOG_READY_WAIT_S:-600}"

for knob in KAFKA_BROKERS KAFKA_VOLUME_GI KAFKA_CPU KAFKA_MEM_GI KAFKA_IO_THREADS KAFKA_NETWORK_THREADS KAFKA_READY_WAIT_S CATALOG_READY_WAIT_S; do
	[[ ${!knob} =~ ^[1-9][0-9]*$ ]] || die "$knob must be a positive integer, got '${!knob}'"
done
[[ $KAFKA_JVM_HEAP =~ ^[0-9]+[gG]$ ]] || die "KAFKA_JVM_HEAP must be an integer number of gigabytes with a g suffix (e.g. 6g), got '$KAFKA_JVM_HEAP'"
[[ ${KAFKA_JVM_HEAP%[gG]} -lt $KAFKA_MEM_GI ]] ||
	die "KAFKA_JVM_HEAP $KAFKA_JVM_HEAP must be below KAFKA_MEM_GI ${KAFKA_MEM_GI}Gi to leave memory for page cache and JVM overhead"
[[ $KAFKA_METRICS == true || $KAFKA_METRICS == false ]] ||
	die "KAFKA_METRICS must be true or false, got '$KAFKA_METRICS'"

NAMESPACE="${NAMESPACE:-ingest-bench}"
KUBE_CONTEXT="${KUBE_CONTEXT:-${CLUSTER_NAME:-}}"
[[ -n $KUBE_CONTEXT ]] || die "KUBE_CONTEXT must name the kubeconfig context of the cluster (it defaults to CLUSTER_NAME)"

# Share stable names with teardown. Service account names also identify
# Pod Identity bindings; the Kafka name determines its bootstrap address.
KAFKA_NAME=ingest-bench
WAREHOUSE_NAME=ingest-bench
CATALOG_SECRET=ingest-bench-catalog-keys
CATALOG_SERVICE_ACCOUNT=ingest-bench-catalog
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark
STRIMZI_NAMESPACE=strimzi-operator
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator

# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------

require_host_tools kubectl helm helmfile yq jq curl envsubst
# shellcheck source=deploy/aws/_stack_hooks.sh
source "$REPO_ROOT/deploy/$CLOUD/_stack_hooks.sh"
read_deployment_site
[[ $KAFKA_DEPLOYMENT == in-cluster ]] || die "$SITE_FILE must set kafka.deployment: in-cluster for stack setup.sh"

if ! NODES="$(kubectl --context "$KUBE_CONTEXT" get nodes -o name 2>&1)"; then
	die "kubectl --context $KUBE_CONTEXT cannot reach the cluster: $NODES"
fi
log "context $KUBE_CONTEXT, namespace $NAMESPACE, $(wc -l <<<"$NODES" | tr -d ' ') nodes"
stack_preflight
stack_preflight_storage

# Warn about missing engine operators without modifying shared installations.
# Only the operator for the chosen engine is required.
if kubectl --context "$KUBE_CONTEXT" get crd flinkdeployments.flink.apache.org >/dev/null 2>&1; then
	log "the flinkdeployments CRD is present"
else
	log "warning: no flinkdeployments CRD on $KUBE_CONTEXT; a Flink run needs the operator, which deploy/aws/setup.sh installs"
fi
if SPARK_NAMESPACES="$(helm --kube-context "$KUBE_CONTEXT" get values "$SPARK_OPERATOR_RELEASE" \
	--namespace "$SPARK_OPERATOR_NAMESPACE" -o json 2>/dev/null | jq -r '.spark.jobNamespaces // [] | .[]')"; then
	if grep -qxF "$NAMESPACE" <<<"$SPARK_NAMESPACES"; then
		log "the spark-operator watches $NAMESPACE"
	else
		WATCHED="${SPARK_NAMESPACES//$'\n'/,}"
		log "warning: the spark-operator does not watch $NAMESPACE (it watches: ${WATCHED:-nothing}); a Spark run here needs:
     helm --kube-context $KUBE_CONTEXT upgrade $SPARK_OPERATOR_RELEASE spark-operator/spark-operator --namespace $SPARK_OPERATOR_NAMESPACE --reuse-values --set 'spark.jobNamespaces={${WATCHED:+$WATCHED,}$NAMESPACE}'"
	fi
else
	log "warning: no $SPARK_OPERATOR_RELEASE release on $KUBE_CONTEXT; a Spark run needs one, which deploy/aws/setup.sh installs"
fi

# ---------------------------------------------------------------------------
# The namespace and the identities
# ---------------------------------------------------------------------------

# Reuse the cloud-neutral namespace and RBAC manifest from AWS setup.
export NAMESPACE
log "applying the namespace, the three run identities and the engine RBAC"
envsubst '${NAMESPACE}' <"$REPO_ROOT/deploy/aws/k8s/namespace.yaml.tmpl" |
	kubectl --context "$KUBE_CONTEXT" apply -f -

stack_bind_identity "$NAMESPACE" "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT" "$CATALOG_SERVICE_ACCOUNT"

stack_storage_class

# ---------------------------------------------------------------------------
# The catalog's secrets
# ---------------------------------------------------------------------------

# Bound the urandom read to avoid SIGPIPE from an unbounded stream under pipefail.
random_token() {
	head -c 64 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 32
}

# Reuse the Secret on reruns: replacing its encryption key would make stored
# catalog secrets unreadable, and the warehouse uses its external ID.
if kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get secret "$CATALOG_SECRET" >/dev/null 2>&1; then
	log "secret $CATALOG_SECRET exists"
else
	log "creating secret $CATALOG_SECRET"
	kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" create secret generic "$CATALOG_SECRET" \
		--from-literal=encryptionKey="$(random_token)" \
		--from-literal=externalId="$(random_token)" \
		--from-literal=postgresUser=postgres \
		--from-literal=postgresPassword="$(random_token)" \
		--from-literal=pgDatabase=lakekeeper \
		--from-literal=pgUser=lakekeeper \
		--from-literal=pgPassword="$(random_token)"
	kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" label secret "$CATALOG_SECRET" lakehouse-ingest-bench=true
fi

# ---------------------------------------------------------------------------
# The releases
# ---------------------------------------------------------------------------

# Preserve other stacks' RoleBindings, including watches patched into the live
# deployment. Deleted namespaces must be pruned or the chart upgrade fails.
if ! WATCH_VALUES="$(helm --kube-context "$KUBE_CONTEXT" get values strimzi-kafka-operator \
	--namespace "$STRIMZI_NAMESPACE" -o json 2>&1)"; then
	[[ $WATCH_VALUES == *"release: not found"* ]] || die "could not read Strimzi release values: $WATCH_VALUES"
	WATCH_VALUES='{}'
fi
WATCHED_NOW="$(jq -r '.watchNamespaces // [] | .[]' <<<"$WATCH_VALUES")"
if ! DEPLOYED_NOW="$(kubectl --context "$KUBE_CONTEXT" --namespace "$STRIMZI_NAMESPACE" \
	get deployment strimzi-cluster-operator --ignore-not-found \
	-o jsonpath='{.spec.template.spec.containers[0].env[?(@.name=="STRIMZI_NAMESPACE")].value}' 2>&1)"; then
	die "could not read the Strimzi deployment's watched namespaces: $DEPLOYED_NOW"
fi
DEPLOYED_NOW="${DEPLOYED_NOW//,/$'\n'}"
STRIMZI_WATCH_NAMESPACES="$(jq -cn --arg ns "$NAMESPACE" '[$ns]')"
for watched in $WATCHED_NOW $DEPLOYED_NOW; do
	[[ $watched != "$NAMESPACE" && $watched != "$STRIMZI_NAMESPACE" ]] || continue
	if ! namespace_state="$(kubectl --context "$KUBE_CONTEXT" get namespace "$watched" --ignore-not-found -o name 2>&1)"; then
		die "could not check watched namespace $watched: $namespace_state"
	fi
	if [[ -n $namespace_state ]]; then
		STRIMZI_WATCH_NAMESPACES="$(jq -c --arg ns "$watched" '. + [$ns] | unique' <<<"$STRIMZI_WATCH_NAMESPACES")"
	else
		log "dropping deleted namespace $watched from the Strimzi watch list"
	fi
done
log "Strimzi watch namespaces: $STRIMZI_WATCH_NAMESPACES"
export STRIMZI_WATCH_NAMESPACES

stack_catalog_settings
export CATALOG_STORAGE_CLASS STRIMZI_VERSION LAKEKEEPER_CHART_VERSION KAFKA_BROKERS KAFKA_VOLUME_GI KAFKA_STORAGE_CLASS KAFKA_CPU KAFKA_MEM_GI KAFKA_JVM_HEAP KAFKA_IO_THREADS KAFKA_NETWORK_THREADS KAFKA_METRICS KAFKA_NODE_SELECTOR KAFKA_TOLERATIONS CATALOG_NODE_SELECTOR CATALOG_TOLERATIONS CATALOG_SECRET STACK_CATALOG_CONFIG_JSON STACK_CATALOG_ENV_JSON
log "helmfile sync: strimzi $STRIMZI_VERSION, kafka/$KAFKA_NAME ($KAFKA_BROKERS x ${KAFKA_VOLUME_GI}Gi on $KAFKA_STORAGE_CLASS), lakekeeper chart $LAKEKEEPER_CHART_VERSION"
# Use sync for install-or-upgrade without requiring the Helm diff plugin.
helmfile --file "$STACK_DIR/helmfile.yaml.gotmpl" --kube-context "$KUBE_CONTEXT" sync

# Wait separately for Kafka's operator-managed Ready condition.
log "waiting up to ${KAFKA_READY_WAIT_S}s for kafka/$KAFKA_NAME to be Ready"
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" wait "kafka/$KAFKA_NAME" \
	--for=condition=Ready --timeout="${KAFKA_READY_WAIT_S}s" ||
	die "kafka/$KAFKA_NAME is not Ready after ${KAFKA_READY_WAIT_S}s. For Pending broker pods, check volume provisioning (StorageClass and CSI driver) and scheduling (KAFKA_NODE_SELECTOR and KAFKA_TOLERATIONS). Inspect the cluster with: kubectl --context $KUBE_CONTEXT -n $NAMESPACE describe kafka $KAFKA_NAME"
kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" rollout status deployment/lakekeeper --timeout "${CATALOG_READY_WAIT_S}s"

# ---------------------------------------------------------------------------
# The catalog's bootstrap and warehouse
# ---------------------------------------------------------------------------

# mgmt_post <path> <json>
# Retain error response bodies; curl -f would hide the catalog's explanation.
mgmt_post() {
	local status body
	body="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-mgmt.XXXXXX")" || die "could not create a temporary file for the catalog response"
	status="$(curl -s -o "$body" -w '%{http_code}' -X POST "$MGMT$1" -H 'content-type: application/json' -d "$2")" ||
		die "could not reach the catalog through the tunnel for POST $1"
	if [[ $status != 2* ]]; then
		log "POST $1 returned HTTP $status: $(cat "$body")"
		rm -f "$body"
		return 1
	fi
	rm -f "$body"
}

trap k8s_port_forward_stop EXIT
k8s_port_forward svc/lakekeeper "$CATALOG_FORWARD_PORT:8181" "$NAMESPACE" /health
MGMT="http://localhost:$K8S_PORT_FORWARD_LOCAL_PORT/management/v1"

if [[ "$(curl -sf "$MGMT/info" | jq -r .bootstrapped)" == true ]]; then
	log "the catalog is bootstrapped"
else
	log "bootstrapping the catalog"
	mgmt_post /bootstrap '{"accept-terms-of-use": true}' || die "the catalog bootstrap failed; see the response above"
fi

if curl -sf "$MGMT/warehouse" | jq -e --arg name "$WAREHOUSE_NAME" '.warehouses[] | select(.name == $name)' >/dev/null; then
	log "warehouse $WAREHOUSE_NAME exists"
else
	EXTERNAL_ID="$(kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" get secret "$CATALOG_SECRET" \
		-o jsonpath='{.data.externalId}' | base64 -d)"
	REQUEST="$(jq -nc --arg name "$WAREHOUSE_NAME" \
		--argjson profile "$(stack_storage_profile_json)" \
		--argjson credential "$(stack_storage_credential_json "$EXTERNAL_ID")" '{
		"warehouse-name": $name,
		"storage-profile": $profile,
		"storage-credential": $credential,
		"delete-profile": {type: "hard"}
	}')"
	log "creating warehouse $WAREHOUSE_NAME at s3://$BUCKET/warehouse"
	mgmt_post /warehouse "$REQUEST" ||
		die "the catalog rejected warehouse creation; see the response above. If bucket access was denied, check the catalog Pod Identity association. If it was bound after the pod started, run kubectl --context $KUBE_CONTEXT -n $NAMESPACE rollout restart deployment/lakekeeper, then rerun setup"
fi
k8s_port_forward_stop

# ---------------------------------------------------------------------------
# The schema registry, when asked for
# ---------------------------------------------------------------------------

if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	log "applying the schema registry"
	sed -e "s|__NAMESPACE__|$NAMESPACE|g" \
		-e "s|__NODE_SELECTOR__|$CATALOG_NODE_SELECTOR|g" \
		-e "s|__TOLERATIONS__|$CATALOG_TOLERATIONS|g" \
		<"$REPO_ROOT/deploy/k8s/schema-registry.yaml.tmpl" |
		kubectl --context "$KUBE_CONTEXT" apply --namespace "$NAMESPACE" -f -
	kubectl --context "$KUBE_CONTEXT" rollout status deployment/schema-registry --namespace "$NAMESPACE" --timeout 300s
else
	log "skipping schema registry setup (WITH_SCHEMA_REGISTRY is '$WITH_SCHEMA_REGISTRY')"
fi

# ---------------------------------------------------------------------------
# What to put in site.yaml
# ---------------------------------------------------------------------------

log "setup complete. Update $SITE_FILE with these stack settings:"
# Printed roots and region are AWS-specific, matching the supported cloud hook.
cat <<SITE
  kafka.bootstrap_servers:        $KAFKA_NAME-kafka-bootstrap.$NAMESPACE.svc:9092
  kafka.security:                 {}
  corpus_root / runs_root / warehouse: s3://$BUCKET/{corpus,runs,warehouse}
  catalog.props.uri:              http://lakekeeper.$NAMESPACE.svc:8181/catalog
  catalog.props.warehouse:        $WAREHOUSE_NAME
  catalog.props.s3.region:        $AWS_REGION
  kubernetes.context:             $KUBE_CONTEXT
  kubernetes.namespace:           $NAMESPACE
  kubernetes.aws_region:          $AWS_REGION
SITE
if [[ $WITH_SCHEMA_REGISTRY == true ]]; then
	cat <<SITE
  kafka.schema_registry.url:      http://schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7
SITE
fi
log "broker nodes and volumes incur charges while idle. Run deploy/k8s/stack/teardown.sh when finished; delete the node group separately"
