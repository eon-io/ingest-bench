#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Delete Kafka while Strimzi can still clean up its claims, then the catalog,
# namespace, and identities. --all also removes Strimzi and the StorageClass.
# Keep the node group, CSI add-on, and bucket, including corpus and result data.
set -euo pipefail
PREREQ_DOC="deploy/k8s/stack/README.md"
STACK_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$STACK_DIR/../../../scripts/_lib.sh"

usage() {
	cat <<'USAGE'
usage: deploy/k8s/stack/teardown.sh [--site PATH] [--all] [--yes]

  --site PATH  read kafka.deployment: in-cluster (default: SITE_FILE or ./site.yaml)
  --all        also remove the Strimzi operator and both StorageClasses
  --yes        skip confirmation before deleting the stack

Environment: CLOUD, KUBE_CONTEXT, NAMESPACE, AWS_REGION, and CLUSTER_NAME
must match the values used by setup.sh.
USAGE
}

ALL=0
ASSUME_YES=no
SITE_FILE="${SITE_FILE:-./site.yaml}"
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--all)
		ALL=1
		shift
		;;
	--yes)
		ASSUME_YES=yes
		shift
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

CLOUD="${CLOUD:-}"
case "$CLOUD" in
aws) ;;
"") die "CLOUD must be set; only aws is supported. See $PREREQ_DOC" ;;
*) die "CLOUD is '$CLOUD'; only aws is supported. See $PREREQ_DOC" ;;
esac
NAMESPACE="${NAMESPACE:-ingest-bench}"
KUBE_CONTEXT="${KUBE_CONTEXT:-${CLUSTER_NAME:-}}"
[[ -n $KUBE_CONTEXT ]] || die "KUBE_CONTEXT must name the kubeconfig context of the cluster (it defaults to CLUSTER_NAME)"
# Supply values required to parse the helmfile even during destruction.
KAFKA_BROKERS="${KAFKA_BROKERS:-3}"
KAFKA_VOLUME_GI="${KAFKA_VOLUME_GI:-500}"
KAFKA_CPU="${KAFKA_CPU:-4}"
KAFKA_MEM_GI="${KAFKA_MEM_GI:-16}"
KAFKA_JVM_HEAP="${KAFKA_JVM_HEAP:-6g}"
KAFKA_IO_THREADS="${KAFKA_IO_THREADS:-8}"
KAFKA_NETWORK_THREADS="${KAFKA_NETWORK_THREADS:-3}"
KAFKA_NODE_SELECTOR="${KAFKA_NODE_SELECTOR:-}"
[[ -n $KAFKA_NODE_SELECTOR ]] || KAFKA_NODE_SELECTOR='{}'
KAFKA_TOLERATIONS="${KAFKA_TOLERATIONS:-[]}"
CATALOG_NODE_SELECTOR="${CATALOG_NODE_SELECTOR:-}"
[[ -n $CATALOG_NODE_SELECTOR ]] || CATALOG_NODE_SELECTOR='{}'
CATALOG_TOLERATIONS="${CATALOG_TOLERATIONS:-[]}"
STRIMZI_VERSION="${STRIMZI_VERSION:-1.2.0}"
LAKEKEEPER_CHART_VERSION="${LAKEKEEPER_CHART_VERSION:-0.12.0}"

KAFKA_NAME=ingest-bench
CATALOG_SECRET=ingest-bench-catalog-keys
CATALOG_SERVICE_ACCOUNT=ingest-bench-catalog
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark
STRIMZI_NAMESPACE=strimzi-operator

require_host_tools kubectl helm helmfile jq
# shellcheck source=deploy/aws/_stack_hooks.sh
source "$REPO_ROOT/deploy/$CLOUD/_stack_hooks.sh"
read_deployment_site
[[ $KAFKA_DEPLOYMENT == in-cluster ]] || die "$SITE_FILE must set kafka.deployment: in-cluster for stack teardown.sh"
stack_preflight
stack_catalog_settings
# Supply the required template variable during teardown.
STRIMZI_WATCH_NAMESPACES="$(jq -cn --arg ns "$NAMESPACE" '[$ns]')"
export STRIMZI_WATCH_NAMESPACES CATALOG_STORAGE_CLASS NAMESPACE STRIMZI_VERSION LAKEKEEPER_CHART_VERSION KAFKA_BROKERS KAFKA_VOLUME_GI KAFKA_STORAGE_CLASS KAFKA_CPU KAFKA_MEM_GI KAFKA_JVM_HEAP KAFKA_IO_THREADS KAFKA_NETWORK_THREADS KAFKA_NODE_SELECTOR KAFKA_TOLERATIONS CATALOG_NODE_SELECTOR CATALOG_TOLERATIONS CATALOG_SECRET STACK_CATALOG_CONFIG_JSON STACK_CATALOG_ENV_JSON

helmfile_here() {
	helmfile --file "$STACK_DIR/helmfile.yaml.gotmpl" --kube-context "$KUBE_CONTEXT" "$@"
}

# ---------------------------------------------------------------------------
# Name it, then ask
# ---------------------------------------------------------------------------

printf 'teardown of the in-cluster stack on %s removes:\n' "$KUBE_CONTEXT"
printf '  kafka/%s and its brokers'"'"' volumes, in %s\n' "$KAFKA_NAME" "$NAMESPACE"
printf '  the lakekeeper release, its Postgres and its volume, in %s\n' "$NAMESPACE"
printf '  namespace %s, with every run object still in it\n' "$NAMESPACE"
printf '  the pod identity associations of its four ServiceAccounts, and role %s if unused\n' "$STACK_ROLE_NAME"
if ((ALL == 1)); then
	printf '  the Strimzi operator in %s (its CRDs stay), and StorageClasses %s and %s, unless another namespace still runs a Kafka cluster\n' "$STRIMZI_NAMESPACE" "$KAFKA_STORAGE_CLASS" "$CATALOG_STORAGE_CLASS"
fi
printf 'The bucket, the node group and the CSI add-on stay.\n'
if [[ $ASSUME_YES != yes ]]; then
	confirm "delete the resources listed above?"
fi

# ---------------------------------------------------------------------------
# Remove it
# ---------------------------------------------------------------------------

if ! NAMESPACE_FOUND="$(kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" --ignore-not-found -o name)"; then
	die "could not determine whether namespace $NAMESPACE exists; keeping its workloads and identities"
fi
if [[ -n $NAMESPACE_FOUND ]]; then
	# Keep Strimzi running while it reconciles Kafka deletion and removes claims.
	log "destroying the kafka release"
	helmfile_here destroy --selector name=kafka
	log "waiting for broker pods to be deleted"
	kubectl --context "$KUBE_CONTEXT" --namespace "$NAMESPACE" wait pod \
		-l "strimzi.io/cluster=$KAFKA_NAME" --for=delete --timeout=300s 2>/dev/null || true
	log "destroying the lakekeeper release"
	helmfile_here destroy --selector name=lakekeeper
	log "deleting namespace $NAMESPACE"
	kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait=true --timeout=600s
else
	log "namespace $NAMESPACE is already gone"
fi

stack_unbind_identity "$NAMESPACE" "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT" "$CATALOG_SERVICE_ACCOUNT"

if ((ALL == 1)); then
	# Keep shared resources until every peer's Kafka cluster is gone.
	if ! OTHER_KAFKAS="$(kubectl --context "$KUBE_CONTEXT" get kafka --all-namespaces \
		-o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' 2>&1)"; then
		die "could not list Kafka clusters; keeping the Strimzi operator and StorageClasses: $OTHER_KAFKAS"
	fi
	OTHER_KAFKAS="$(awk -v ns="$NAMESPACE" 'NF && $0 != ns' <<<"$OTHER_KAFKAS" | sort -u | tr '\n' ' ')"
	if [[ -n $OTHER_KAFKAS ]]; then
		log "keeping the Strimzi operator and StorageClasses $KAFKA_STORAGE_CLASS and $CATALOG_STORAGE_CLASS: Kafka still runs in ${OTHER_KAFKAS% }"
	else
		log "destroying the strimzi operator release"
		helmfile_here destroy --selector name=strimzi-kafka-operator
		kubectl --context "$KUBE_CONTEXT" delete namespace "$STRIMZI_NAMESPACE" --ignore-not-found --wait=true --timeout=600s
		stack_delete_storage_class
	fi
else
	log "the Strimzi operator and StorageClasses $KAFKA_STORAGE_CLASS and $CATALOG_STORAGE_CLASS stay; --all removes them"
fi
log "teardown complete"
