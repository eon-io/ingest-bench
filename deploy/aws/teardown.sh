#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Stop workloads and remove their Pod Identity associations, then clean up managed
# Kafka before checking the shared IAM role. Reruns can finish partial teardown.
#
# Keep S3, ECR, both operators and the S3 gateway endpoint unless --all is
# set. Bucket deletion requires confirmation because it removes corpus data
# and measured results.
# Leave the EKS cluster and Pod Identity agent installed.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
AWS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=scripts/_lib.sh
source "$AWS_DIR/../../scripts/_lib.sh"
# shellcheck source=deploy/aws/_resources.sh
source "$AWS_DIR/_resources.sh"

usage() {
	cat <<'USAGE'
usage: deploy/aws/teardown.sh [--site PATH] [--all] [--yes]

  --site PATH  read kafka.deployment from PATH (default: SITE_FILE or ./site.yaml)
  --all        also delete the bucket and its contents, the three ECR
               repositories, both engine operators' releases and the S3
               gateway endpoint setup added to the cluster VPC
  --yes        skip confirmation before emptying the bucket

Environment: AWS_REGION and CLUSTER_NAME are required. BUCKET, MSK_NAME,
NAMESPACE, KUBE_CONTEXT, OPERATORS, MANAGE_BUCKET and HARNESS_ROLE_NAME
must match the values used by setup.sh.
MSK and its security group are removed only for kafka.deployment: managed.
MSK_DELETED_WAIT_S sets the timeout for MSK deletion.
If EKS is already gone, set VPC_ID to its original VPC for security-group and
S3 endpoint cleanup.
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

read_deployment_site

export AWS_REGION="${AWS_REGION:?AWS_REGION must name the region the EKS cluster is in}"
CLUSTER_NAME="${CLUSTER_NAME:?CLUSTER_NAME must name the EKS cluster setup.sh was run against}"
KUBE_CONTEXT="${KUBE_CONTEXT:-$CLUSTER_NAME}"
MSK_NAME="${MSK_NAME:-lakehouse-ingest-bench}"
NAMESPACE="${NAMESPACE:-ingest-bench}"
# Wait for MSK deletion before removing its security group; a timeout leaves
# the group for the next teardown attempt.
MSK_DELETED_WAIT_S="${MSK_DELETED_WAIT_S:-1800}"

TAG_KEY=lakehouse-ingest-bench
ROLE_NAME="${HARNESS_ROLE_NAME:-lakehouse-ingest-bench-harness}"
POLICY_NAME=lakehouse-ingest-bench-harness
HARNESS_SERVICE_ACCOUNT=ingest-bench-harness
FLINK_SERVICE_ACCOUNT=ingest-bench-flink
SPARK_SERVICE_ACCOUNT=ingest-bench-spark
FLINK_OPERATOR_RELEASE=flink-kubernetes-operator
FLINK_OPERATOR_NAMESPACE=flink-operator
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator
ECR_REPOSITORIES="lakehouse-ingest-bench/harness lakehouse-ingest-bench/flink lakehouse-ingest-bench/spark"
validate_operators
validate_manage_bucket

require_host_tools aws kubectl jq
if ((ALL == 1)); then
	require_host_tools helm
fi

if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
	die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
fi
BUCKET="${BUCKET:-lakehouse-ingest-bench-$ACCOUNT}"

# Skip Kubernetes cleanup if the cluster is gone. Check once so later errors
# are not mistaken for absent resources.
VPC_ID="${VPC_ID:-}"
if CLUSTER_JSON="$(aws_lookup eks describe-cluster --name "$CLUSTER_NAME" --output json)"; then
	CLUSTER_PRESENT=1
	VPC_ID="$(jq -r '.cluster.resourcesVpcConfig.vpcId' <<<"$CLUSTER_JSON")"
	if ! kubectl config get-contexts -o name 2>/dev/null | grep -qxF "$KUBE_CONTEXT"; then
		log "no kubeconfig context $KUBE_CONTEXT; writing one"
		aws eks update-kubeconfig --name "$CLUSTER_NAME" --alias "$KUBE_CONTEXT" >/dev/null
	fi
else
	[[ $CLUSTER_JSON == *"(ResourceNotFoundException)"* ]] ||
		die "could not determine whether EKS cluster $CLUSTER_NAME exists: $CLUSTER_JSON"
	CLUSTER_PRESENT=0
	log "EKS cluster $CLUSTER_NAME is not in $AWS_REGION; skipping everything that lives inside it"
fi

# ---------------------------------------------------------------------------
# The workloads
# ---------------------------------------------------------------------------

# Delete the namespace first and wait until no pods can use MSK or the IAM role.
if ((CLUSTER_PRESENT == 1)); then
	if ! NAMESPACE_FOUND="$(kubectl --context "$KUBE_CONTEXT" get namespace "$NAMESPACE" --ignore-not-found -o name)"; then
		die "could not determine whether namespace $NAMESPACE exists"
	fi
	if [[ -n $NAMESPACE_FOUND ]]; then
		log "deleting namespace $NAMESPACE and everything in it"
		kubectl --context "$KUBE_CONTEXT" delete namespace "$NAMESPACE" --wait --timeout=600s
	else
		log "namespace $NAMESPACE is already gone"
	fi
fi

# ---------------------------------------------------------------------------
# The workload's Pod Identity associations
# ---------------------------------------------------------------------------

if ((CLUSTER_PRESENT == 1)); then
	for service_account in "$HARNESS_SERVICE_ACCOUNT" "$FLINK_SERVICE_ACCOUNT" "$SPARK_SERVICE_ACCOUNT"; do
		association_ids="$(aws eks list-pod-identity-associations --cluster-name "$CLUSTER_NAME" \
			--namespace "$NAMESPACE" --service-account "$service_account" \
			--query 'associations[].associationId' --output text)"
		if [[ -z $association_ids ]]; then
			log "pod identity: $NAMESPACE/$service_account has no association"
			continue
		fi
		# shellcheck disable=SC2086  # a deliberate expansion: --output text tab-separates the ids
		for association_id in $association_ids; do
			log "deleting pod identity association $association_id ($NAMESPACE/$service_account)"
			aws eks delete-pod-identity-association --cluster-name "$CLUSTER_NAME" \
				--association-id "$association_id" >/dev/null
		done
	done
fi

# ---------------------------------------------------------------------------
# The broker and its security group
# ---------------------------------------------------------------------------

if [[ $KAFKA_DEPLOYMENT == managed ]]; then
	MSK_ARN="$(aws kafka list-clusters --cluster-name-filter "$MSK_NAME" \
		--query "ClusterInfoList[?ClusterName=='$MSK_NAME'].ClusterArn | [0]" --output text)"
	MSK_OWNED=1
	if [[ -n $MSK_ARN && $MSK_ARN != None ]] && ! msk_is_ours "$MSK_ARN"; then
		log "keeping MSK cluster $MSK_NAME: its $TAG_KEY=true ownership tag is absent"
		MSK_OWNED=0
	elif [[ -n $MSK_ARN && $MSK_ARN != None ]]; then
		MSK_STATE="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" --query ClusterInfo.State --output text)"
		if [[ $MSK_STATE != DELETING ]]; then
			log "deleting MSK cluster $MSK_NAME"
			aws kafka delete-cluster --cluster-arn "$MSK_ARN" >/dev/null
		fi
		# Wait for broker network interfaces to release the security group.
		log "waiting for $MSK_NAME to disappear (several minutes)"
		waited=0
		while aws kafka describe-cluster --cluster-arn "$MSK_ARN" >/dev/null 2>&1; do
			if ((waited >= MSK_DELETED_WAIT_S)); then
				die "$MSK_NAME still exists after ${waited}s; rerun this script after deletion completes"
			fi
			if ((waited % 300 == 0)); then
				log "  ... still deleting (${waited}s)"
			fi
			sleep 30
			waited=$((waited + 30))
		done
	else
		log "MSK cluster $MSK_NAME is already gone"
	fi

	# A kept cluster still holds its security group.
	if ((MSK_OWNED == 1)); then
		remove_msk_security_group
	fi
else
	log "skipping MSK and its security group (kafka.deployment=$KAFKA_DEPLOYMENT)"
fi

# Cross-region role checks must not leave a managed broker running on failure.
remove_harness_role

# ---------------------------------------------------------------------------
# What --all also removes
# ---------------------------------------------------------------------------

if ((ALL == 0)); then
	log "kept: s3://$BUCKET, the ECR repositories, both engine operators and the S3 gateway endpoint. --all removes them too."
	exit 0
fi

# shellcheck disable=SC2086  # a deliberate expansion: the names are space separated
for repository in $ECR_REPOSITORIES; do
	if ! REPOSITORY_ARN="$(aws_lookup ecr describe-repositories --repository-names "$repository" \
		--query 'repositories[0].repositoryArn' --output text)"; then
		[[ $REPOSITORY_ARN == *"(RepositoryNotFoundException)"* ]] ||
			die "could not inspect ecr repository $repository: $REPOSITORY_ARN"
		log "ecr repository $repository is already gone"
	elif [[ "$(aws ecr list-tags-for-resource --resource-arn "$REPOSITORY_ARN" \
		--query "tags[?Key=='$TAG_KEY'].Value | [0]" --output text)" != true ]]; then
		log "keeping ecr repository $repository: its $TAG_KEY=true ownership tag is absent"
	else
		log "deleting ecr repository $repository and its images"
		aws ecr delete-repository --repository-name "$repository" --force >/dev/null
	fi
done

if ((CLUSTER_PRESENT == 1)); then
	if ! manages_operator flink; then
		log "keeping the Flink Kubernetes Operator: OPERATORS ('$OPERATORS') leaves it out"
	elif helm --kube-context "$KUBE_CONTEXT" status "$FLINK_OPERATOR_RELEASE" \
		--namespace "$FLINK_OPERATOR_NAMESPACE" >/dev/null 2>&1; then
		log "uninstalling the Flink Kubernetes Operator"
		helm --kube-context "$KUBE_CONTEXT" uninstall "$FLINK_OPERATOR_RELEASE" \
			--namespace "$FLINK_OPERATOR_NAMESPACE" --wait
		kubectl --context "$KUBE_CONTEXT" delete namespace "$FLINK_OPERATOR_NAMESPACE" --ignore-not-found --wait --timeout=600s
	else
		log "the Flink Kubernetes Operator is not a helm release here; leaving it alone"
	fi
	if ! manages_operator spark; then
		log "keeping the Kubeflow spark-operator: OPERATORS ('$OPERATORS') leaves it out"
	elif helm --kube-context "$KUBE_CONTEXT" status "$SPARK_OPERATOR_RELEASE" \
		--namespace "$SPARK_OPERATOR_NAMESPACE" >/dev/null 2>&1; then
		log "uninstalling the Kubeflow spark-operator"
		helm --kube-context "$KUBE_CONTEXT" uninstall "$SPARK_OPERATOR_RELEASE" \
			--namespace "$SPARK_OPERATOR_NAMESPACE" --wait
		kubectl --context "$KUBE_CONTEXT" delete namespace "$SPARK_OPERATOR_NAMESPACE" --ignore-not-found --wait --timeout=600s
	else
		log "the Kubeflow spark-operator is not a helm release here; leaving it alone"
	fi
fi

# ---------------------------------------------------------------------------
# The VPC's path to S3
# ---------------------------------------------------------------------------

remove_s3_gateway_endpoint

# ---------------------------------------------------------------------------
# The bucket, last and asked about
# ---------------------------------------------------------------------------

# Delete the bucket last; declining leaves ECR and operators already removed.
if [[ $MANAGE_BUCKET == true ]]; then
	remove_bucket
else
	log "keeping s3://$BUCKET: MANAGE_BUCKET=false"
fi
