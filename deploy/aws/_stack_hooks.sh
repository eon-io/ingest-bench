# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# AWS hooks for deploy/k8s/stack setup and teardown when CLOUD=aws.
# Cloud-specific checks, identities, storage classes, and catalog credentials
# are isolated here. Other clouds must implement the same function interface.
# Sourced after scripts/_lib.sh, which provides log, die, and require_host_tools.

_STACK_HOOKS_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=deploy/aws/_resources.sh
source "$_STACK_HOOKS_DIR/_resources.sh"

# Give the stack's four service accounts a role independent of the MSK policy.
# Clusters that share an account need separate names, as for the harness role.
STACK_ROLE_NAME="${STACK_ROLE_NAME:-lakehouse-ingest-bench-stack}"
STACK_POLICY_NAME=lakehouse-ingest-bench-stack
STACK_TAG_KEY=lakehouse-ingest-bench

KAFKA_STORAGE_CLASS="${KAFKA_STORAGE_CLASS:-ingest-bench-kafka}"
CATALOG_STORAGE_CLASS=ingest-bench-catalog
KAFKA_VOLUME_THROUGHPUT_MIBS="${KAFKA_VOLUME_THROUGHPUT_MIBS:-250}"
KAFKA_VOLUME_IOPS="${KAFKA_VOLUME_IOPS:-6000}"

# Check shared AWS prerequisites and set ACCOUNT. Keep storage checks
# separate so teardown works after the bucket or CSI add-on is removed.
stack_preflight() {
	require_host_tools aws envsubst
	[[ -n ${AWS_REGION:-} ]] || die "AWS_REGION must specify the region containing the cluster and bucket"
	export AWS_REGION
	[[ -n ${CLUSTER_NAME:-} ]] || die "CLUSTER_NAME must specify the EKS cluster used for Pod Identity associations"

	if ! ACCOUNT="$(aws sts get-caller-identity --query Account --output text 2>&1)"; then
		die "aws sts get-caller-identity failed: $ACCOUNT — sign in first (aws configure, or aws sso login --profile ...)"
	fi
	log "account $ACCOUNT, region $AWS_REGION"
}

# Validate setup-only storage prerequisites: bucket access, volume settings,
# and the EBS CSI driver.
stack_preflight_storage() {
	[[ $KAFKA_STORAGE_CLASS != "$CATALOG_STORAGE_CLASS" ]] || die "KAFKA_STORAGE_CLASS must differ from the catalog class $CATALOG_STORAGE_CLASS"
	[[ -n ${BUCKET:-} ]] || die "BUCKET must specify the bucket containing the corpus/, runs/, and warehouse/ prefixes"
	[[ $KAFKA_VOLUME_THROUGHPUT_MIBS =~ ^[1-9][0-9]*$ ]] ||
		die "KAFKA_VOLUME_THROUGHPUT_MIBS must be a positive integer, got '$KAFKA_VOLUME_THROUGHPUT_MIBS'"
	[[ $KAFKA_VOLUME_IOPS =~ ^[1-9][0-9]*$ ]] || die "KAFKA_VOLUME_IOPS must be a positive integer, got '$KAFKA_VOLUME_IOPS'"

	log "bucket s3://$BUCKET"
	aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 ||
		die "s3://$BUCKET is not reachable from this account; deploy/aws/setup.sh creates the bucket, or name one of your own with BUCKET"

	# Fail before broker and database claims become stuck without a CSI driver.
	if ! kubectl --context "$KUBE_CONTEXT" get csidriver ebs.csi.aws.com >/dev/null 2>&1; then
		die "$CLUSTER_NAME has no ebs.csi.aws.com CSI driver, so no broker or database volume can be provisioned. Install the add-on with its identity, then re-run:
     eksctl create addon --cluster $CLUSTER_NAME --region $AWS_REGION --name aws-ebs-csi-driver --auto-apply-pod-identity-associations"
	fi
	log "ebs.csi.aws.com is present"
}

# stack_bind_identity <namespace> <service-account>...
# Bind identities before creating pods so the catalog receives credentials
# for its warehouse access check.
stack_bind_identity() {
	local namespace=$1
	shift
	local trust policy role_arn service_account associate_error
	trust="$(cat "$_STACK_HOOKS_DIR/iam/trust.json")"
	export BUCKET
	policy="$(envsubst '${BUCKET}' <"$_STACK_HOOKS_DIR/iam/stack-policy.json")"
	ensure_owned_role "$STACK_ROLE_NAME" "$STACK_TAG_KEY" "$trust" "$STACK_POLICY_NAME" "$policy"
	role_arn="arn:aws:iam::$ACCOUNT:role/$STACK_ROLE_NAME"
	for service_account in "$@"; do
		if associate_error="$(aws eks create-pod-identity-association --cluster-name "$CLUSTER_NAME" \
			--namespace "$namespace" --service-account "$service_account" --role-arn "$role_arn" \
			--tags "$STACK_TAG_KEY=true" 2>&1)"; then
			log "pod identity: $namespace/$service_account now assumes $STACK_ROLE_NAME"
		else
			case "$associate_error" in
			*ResourceInUseException*) log "pod identity: $namespace/$service_account is already associated" ;;
			*) die "could not associate $namespace/$service_account with $STACK_ROLE_NAME: $associate_error" ;;
			esac
		fi
	done
}

# stack_unbind_identity <namespace> <service-account>...
# Delete existing associations before the role so partial teardown can resume.
# Namespaces and clusters share the role; preserve it while any association uses it.
stack_unbind_identity() {
	local namespace=$1
	shift
	local service_account association_id detach_error role role_arn
	for service_account in "$@"; do
		if ! association_id="$(aws eks list-pod-identity-associations --cluster-name "$CLUSTER_NAME" \
			--namespace "$namespace" --service-account "$service_account" \
			--query 'associations[0].associationId' --output text 2>&1)"; then
			die "could not list pod identity associations for $namespace/$service_account: $association_id"
		fi
		if [[ -n $association_id && $association_id != None ]]; then
			log "deleting pod identity association for $namespace/$service_account"
			aws eks delete-pod-identity-association --cluster-name "$CLUSTER_NAME" --association-id "$association_id" >/dev/null
		else
			log "no pod identity association for $namespace/$service_account"
		fi
	done
	if ! role="$(aws_lookup iam get-role --role-name "$STACK_ROLE_NAME" --output json)"; then
		[[ $role == *"(NoSuchEntity)"* ]] || die "could not inspect iam role $STACK_ROLE_NAME: $role"
		log "iam role $STACK_ROLE_NAME is already gone"
		return
	fi
	role_arn="$(jq -er '.Role.Arn | select(type == "string" and length > 0)' <<<"$role")" ||
		die "iam role $STACK_ROLE_NAME returned no ARN"
	if ! jq -e --arg tag "$STACK_TAG_KEY" 'any(.Role.Tags[]?; .Key == $tag and .Value == "true")' <<<"$role" >/dev/null; then
		log "keeping iam role $STACK_ROLE_NAME: its $STACK_TAG_KEY=true ownership tag is absent"
		return
	fi
	if role_in_use "$role_arn"; then
		log "keeping iam role $STACK_ROLE_NAME: pod identity association $ROLE_USER still assumes it"
		return
	fi
	log "deleting iam role $STACK_ROLE_NAME"
	if ! detach_error="$(aws iam delete-role-policy --role-name "$STACK_ROLE_NAME" --policy-name "$STACK_POLICY_NAME" 2>&1)"; then
		[[ $detach_error == *NoSuchEntity* ]] || die "could not remove policy $STACK_POLICY_NAME from $STACK_ROLE_NAME: $detach_error"
	fi
	aws iam delete-role --role-name "$STACK_ROLE_NAME"
}

# Apply separate classes so broker throughput tuning does not affect Postgres.
stack_storage_class() {
	export KAFKA_STORAGE_CLASS KAFKA_VOLUME_THROUGHPUT_MIBS KAFKA_VOLUME_IOPS
	log "applying StorageClass $KAFKA_STORAGE_CLASS (gp3, ${KAFKA_VOLUME_THROUGHPUT_MIBS} MiB/s, ${KAFKA_VOLUME_IOPS} iops)"
	envsubst '${KAFKA_STORAGE_CLASS} ${KAFKA_VOLUME_THROUGHPUT_MIBS} ${KAFKA_VOLUME_IOPS}' \
		<"$_STACK_HOOKS_DIR/k8s/kafka-storageclass.yaml.tmpl" |
		kubectl --context "$KUBE_CONTEXT" apply -f -
	log "applying StorageClass $CATALOG_STORAGE_CLASS (baseline gp3)"
	kubectl --context "$KUBE_CONTEXT" apply -f "$_STACK_HOOKS_DIR/k8s/catalog-storageclass.yaml"
}

stack_delete_storage_class() {
	log "deleting StorageClasses $KAFKA_STORAGE_CLASS and $CATALOG_STORAGE_CLASS"
	kubectl --context "$KUBE_CONTEXT" delete storageclass "$KAFKA_STORAGE_CLASS" "$CATALOG_STORAGE_CLASS" --ignore-not-found
}

# Enable direct pod-identity access to storage and supply both AWS region names.
stack_catalog_settings() {
	STACK_CATALOG_CONFIG_JSON="$(jq -nc '{
		LAKEKEEPER__ENABLE_AWS_SYSTEM_CREDENTIALS: "true",
		LAKEKEEPER__S3_ENABLE_DIRECT_SYSTEM_CREDENTIALS: "true"
	}')"
	STACK_CATALOG_ENV_JSON="$(jq -nc --arg region "$AWS_REGION" \
		'[{name: "AWS_REGION", value: $region}, {name: "AWS_DEFAULT_REGION", value: $region}]')"
}

# Write the warehouse storage profile to stdout. Disable vending and remote
# signing because each client already accesses storage through pod identity.
stack_storage_profile_json() {
	jq -nc --arg bucket "$BUCKET" --arg region "$AWS_REGION" '{
		type: "s3",
		bucket: $bucket,
		"key-prefix": "warehouse",
		region: $region,
		flavor: "aws",
		"sts-enabled": false,
		"remote-signing-enabled": false
	}'
}

# stack_storage_credential_json <external-id>
# Describe the catalog's pod identity credentials for the warehouse.
stack_storage_credential_json() {
	jq -nc --arg external_id "$1" '{type: "s3", "credential-type": "aws-system-identity", "external-id": $external_id}'
}
