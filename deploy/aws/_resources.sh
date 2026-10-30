# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# AWS resource operations. Source after scripts/_lib.sh for log, die, and confirm.
# Callers supply the deployment settings used by each operation.

# Pin a CRD version compatible with the rendered SparkApplication fields.
# spark.jobNamespaces controls which namespaces the operator watches.
SPARK_OPERATOR_VERSION="${SPARK_OPERATOR_VERSION:-2.5.2}"
SPARK_OPERATOR_RELEASE=spark-operator
SPARK_OPERATOR_NAMESPACE=spark-operator
SPARK_OPERATOR_REPO=https://kubeflow.github.io/spark-operator

# Engine operators that setup installs and teardown --all removes. Leaving one
# out keeps an operator managed outside the benchmark untouched.
OPERATORS="${OPERATORS-flink spark}"
MANAGE_BUCKET="${MANAGE_BUCKET-true}"

# Both functions split OPERATORS on whitespace with pathname expansion off, in
# a subshell so set -f does not leak; a value like 'fl*' must not match files.
validate_operators() (
	set -f
	for operator in $OPERATORS; do
		case "$operator" in
		flink | spark) ;;
		*) die "OPERATORS may list flink and spark, got '$operator'" ;;
		esac
	done
)

manages_operator() (
	set -f
	for operator in $OPERATORS; do
		[[ $operator == "$1" ]] && exit 0
	done
	exit 1
)

# Keep successful stdout data separate from stderr warnings. On failure, include
# diagnostics in stdout so callers can distinguish absence from lookup errors.
# A subshell isolates temporary-file cleanup from the caller's traps.
aws_lookup() (
	local diagnostics status
	diagnostics="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-aws.XXXXXX")" || return
	trap 'rm -f "$diagnostics"' EXIT
	if aws "$@" 2>"$diagnostics"; then
		cat "$diagnostics" >&2
	else
		status=$?
		cat "$diagnostics"
		return "$status"
	fi
)

# role_in_use <role arn>
# IAM roles are account-wide: check every cluster in every enabled region.
# Return 0 with the matching association in ROLE_USER, or 1 if none uses the role.
# Exit on lookup failure, since an unreadable association may still use it.
# Call directly, not through command substitution, which would confine die to a
# subshell and let the caller mistake a lookup failure for an unused role.
role_in_use() {
	local role_arn=$1
	local regions region clusters cluster associations association used_roles used_role
	regions="$(aws_lookup ec2 describe-regions --query 'Regions[].RegionName' --output text)" ||
		die "could not list regions before deleting iam role $role_arn: $regions"
	[[ -n $regions && $regions != None ]] || die "no regions returned while checking iam role $role_arn"
	# shellcheck disable=SC2086  # AWS text output separates region names with whitespace.
	for region in $regions; do
		clusters="$(aws_lookup eks list-clusters --region "$region" --query 'clusters[]' --output text)" ||
			die "could not list EKS clusters in $region before deleting iam role $role_arn: $clusters"
		# shellcheck disable=SC2086
		for cluster in $clusters; do
			[[ $cluster != None ]] || continue
			associations="$(aws_lookup eks list-pod-identity-associations --region "$region" --cluster-name "$cluster" \
				--query 'associations[].associationId' --output text)" ||
				die "could not list pod identity associations on $region/$cluster: $associations"
			# shellcheck disable=SC2086
			for association in $associations; do
				[[ $association != None ]] || continue
				used_roles="$(aws_lookup eks describe-pod-identity-association --region "$region" --cluster-name "$cluster" \
					--association-id "$association" --query 'association.[roleArn,targetRoleArn]' --output text)" ||
					die "could not inspect pod identity association $region/$cluster/$association: $used_roles"
				[[ -n $used_roles && $used_roles != None ]] ||
					die "no role returned for pod identity association $region/$cluster/$association"
				# shellcheck disable=SC2086
				for used_role in $used_roles; do
					if [[ $used_role == "$role_arn" ]]; then
						ROLE_USER="$region/$cluster/$association"
						return 0
					fi
				done
			done
		done
	done
	return 1
}

# The MSK API returns tags as a map rather than a list.
msk_is_ours() {
	local tags
	tags="$(aws_lookup kafka list-tags-for-resource --resource-arn "$1" --query "Tags.\"$TAG_KEY\"" --output text)" ||
		die "could not read the tags of MSK cluster $1: $tags"
	[[ $tags == true ]]
}

# ensure_owned_role <role> <ownership tag key> <trust policy> <policy name> <policy>
# Create the role, or refresh one carrying the ownership tag. A role without the
# tag belongs to someone else, so refuse to replace its trust or policy.
ensure_owned_role() {
	local name=$1 tag=$2 trust=$3 policy_name=$4 policy=$5 role
	if role="$(aws_lookup iam get-role --role-name "$name" --output json)"; then
		jq -e --arg tag "$tag" 'any(.Role.Tags[]?; .Key == $tag and .Value == "true")' <<<"$role" >/dev/null ||
			die "iam role $name exists without the $tag=true tag; refusing to change its trust or policy.
     Choose an unused name with HARNESS_ROLE_NAME or STACK_ROLE_NAME."
		log "iam role $name exists; refreshing its trust policy"
		aws iam update-assume-role-policy --role-name "$name" --policy-document "$trust"
	else
		[[ $role == *"(NoSuchEntity)"* ]] || die "could not inspect iam role $name: $role"
		log "creating iam role $name"
		aws iam create-role --role-name "$name" --assume-role-policy-document "$trust" \
			--tags "Key=$tag,Value=true" >/dev/null
	fi
	# Replace the inline policy so reruns apply the checked-in permissions.
	aws iam put-role-policy --role-name "$name" --policy-name "$policy_name" --policy-document "$policy"
	log "iam policy $policy_name applied to $name"
}

remove_harness_role() {
	local role role_arn policy
	if ! role="$(aws_lookup iam get-role --role-name "$ROLE_NAME" --output json)"; then
		[[ $role == *"(NoSuchEntity)"* ]] || die "could not inspect iam role $ROLE_NAME: $role"
		log "iam role $ROLE_NAME is already gone"
		return
	fi
	role_arn="$(jq -er '.Role.Arn | select(type == "string" and length > 0)' <<<"$role")" ||
		die "iam role $ROLE_NAME returned no ARN"
	if ! jq -e --arg tag "$TAG_KEY" 'any(.Role.Tags[]?; .Key == $tag and .Value == "true")' <<<"$role" >/dev/null; then
		log "keeping iam role $ROLE_NAME: its $TAG_KEY=true ownership tag is absent"
		return
	fi
	if role_in_use "$role_arn"; then
		log "keeping iam role $ROLE_NAME: pod identity association $ROLE_USER still uses it"
		return
	fi
	if policy="$(aws_lookup iam get-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME")"; then
		log "deleting inline policy $POLICY_NAME from $ROLE_NAME"
		aws iam delete-role-policy --role-name "$ROLE_NAME" --policy-name "$POLICY_NAME"
	else
		[[ $policy == *"(NoSuchEntity)"* ]] || die "could not inspect policy $POLICY_NAME: $policy"
	fi
	log "deleting iam role $ROLE_NAME"
	aws iam delete-role --role-name "$ROLE_NAME"
}

# Missing tags may be an API error or an empty list; both mean unowned here.
bucket_is_ours() {
	local tags
	tags="$(aws s3api get-bucket-tagging --bucket "$1" \
		--query "TagSet[?Key=='$TAG_KEY'].Value" --output text 2>/dev/null)" || return 1
	[[ $tags == true ]]
}

# Only configure existing buckets carrying the benchmark tag. Tag replacement
# and versioning changes could otherwise alter an unrelated bucket.
create_bucket() {
	if aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1; then
		bucket_is_ours "$BUCKET" ||
			die "s3://$BUCKET already exists, but its $TAG_KEY=true tag could not be verified.
     Refusing to change its tags or versioning. Set MANAGE_BUCKET=false to use it
     as is, set BUCKET to a new bucket name, or tag this bucket $TAG_KEY=true if
     it is dedicated to the benchmark."
		log "s3://$BUCKET exists"
	else
		log "creating s3://$BUCKET"
		# CreateBucket in us-east-1 rejects an explicit LocationConstraint.
		if [[ $AWS_REGION == us-east-1 ]]; then
			aws s3api create-bucket --bucket "$BUCKET" >/dev/null
		else
			aws s3api create-bucket --bucket "$BUCKET" \
				--create-bucket-configuration "LocationConstraint=$AWS_REGION" >/dev/null
		fi
	fi
	aws s3api put-public-access-block --bucket "$BUCKET" \
		--public-access-block-configuration \
		BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
	aws s3api put-bucket-tagging --bucket "$BUCKET" --tagging "TagSet=[{Key=$TAG_KEY,Value=true}]"
	# Suspend enabled versioning to avoid retaining billable deleted corpus data.
	if [[ "$(aws s3api get-bucket-versioning --bucket "$BUCKET" --query 'Status' --output text)" == Enabled ]]; then
		log "suspending versioning on s3://$BUCKET"
		aws s3api put-bucket-versioning --bucket "$BUCKET" --versioning-configuration Status=Suspended
	fi
}

# With MANAGE_BUCKET=false the bucket belongs to its owner: setup only checks
# access, and teardown never empties or deletes it.
validate_manage_bucket() {
	case "$MANAGE_BUCKET" in
	true | false) ;;
	*) die "MANAGE_BUCKET must be true or false, got '$MANAGE_BUCKET'" ;;
	esac
}

use_unmanaged_bucket() {
	aws s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 ||
		die "s3://$BUCKET is not reachable; MANAGE_BUCKET=false needs an existing bucket this identity can access"
	log "using s3://$BUCKET unmanaged: setup leaves its settings unchanged and teardown keeps it"
	if [[ "$(aws s3api get-bucket-versioning --bucket "$BUCKET" --query 'Status' --output text)" == Enabled ]]; then
		log "WARNING: s3://$BUCKET has versioning enabled; deleted corpus and table data stays billable as noncurrent versions"
	fi
}

# Grow broker storage to MSK_VOLUME_GIB when needed; MSK cannot shrink it.
# Enough storage prevents broker capacity from limiting the offered rate.
grow_broker_volume() {
	local reported state version current update_error
	# Read size, state, and update version from one response.
	reported="$(aws kafka describe-cluster --cluster-arn "$MSK_ARN" \
		--query 'ClusterInfo.[State,CurrentVersion,BrokerNodeGroupInfo.StorageInfo.EbsStorageInfo.VolumeSize]' \
		--output text)" ||
		die "could not read $MSK_NAME's broker storage; try: aws kafka describe-cluster --cluster-arn $MSK_ARN"
	IFS=$'\t' read -r state version current <<<"$reported"
	# Reject missing numeric fields instead of interpreting AWS CLI's None as zero.
	if [[ -z $current || $current == None ]]; then
		die "$MSK_NAME reports no broker volume size; cannot verify the requested ${MSK_VOLUME_GIB} GiB capacity"
	fi
	if ((current >= MSK_VOLUME_GIB)); then
		log "MSK broker volumes are ${current} GiB, meeting the requested ${MSK_VOLUME_GIB} GiB minimum"
		return 0
	fi
	# An in-progress update may still report the old size; do not request it twice.
	if [[ $state != ACTIVE ]]; then
		log "MSK broker volumes are ${current} GiB and $MSK_NAME is $state; rerun setup after it becomes ACTIVE to verify the requested ${MSK_VOLUME_GIB} GiB capacity"
		return 0
	fi
	log "growing the msk broker volumes from ${current} to ${MSK_VOLUME_GIB} GiB"
	# Use the reported version for the update. The final ACTIVE wait covers it.
	if update_error="$(aws kafka update-broker-storage --cluster-arn "$MSK_ARN" --current-version "$version" \
		--target-broker-ebs-volume-info "KafkaBrokerNodeId=All,VolumeSizeGB=$MSK_VOLUME_GIB" 2>&1)"; then
		return 0
	fi
	case "$update_error" in
	# Cooldowns and concurrent updates are retryable on a later setup invocation.
	*ACTIVE* | *UPDATING* | *ooldown* | *"6 hour"* | *"6-hour"*)
		log "$MSK_NAME cannot increase broker storage to ${MSK_VOLUME_GIB} GiB yet: $update_error"
		;;
	*)
		die "could not grow $MSK_NAME's broker volumes to ${MSK_VOLUME_GIB} GiB: $update_error"
		;;
	esac
}

# Write the selected deployment's site settings. Quote scalars, including the
# numeric-looking Glue warehouse account ID. Leave prices for the operator to fill in.
write_site() {
	local path=$1 parsed read_back=""
	[[ ! -e $path ]] ||
		die "$path already exists; --write-site cannot overwrite it. Choose another path or move the existing file"
	{
		cat <<-SITE
			# Generated by setup.sh --write-site; see docs/run-spec.md for field details.
			corpus_root: "s3://$BUCKET/corpus"
			runs_root: "s3://$BUCKET/runs"
			warehouse: "s3://$BUCKET/warehouse"
		SITE
		if [[ $KAFKA_DEPLOYMENT == external ]]; then
			printf 'kafka: %s\n' "$(yq -o=json -I=0 '.kafka' "$SITE_FILE")"
		else
			cat <<-SITE
				kafka:
				  deployment: $KAFKA_DEPLOYMENT
				  bootstrap_servers: "$BOOTSTRAP"
			SITE
		fi
		if [[ $KAFKA_DEPLOYMENT == managed ]]; then
			cat <<-SITE
				  security:
				    security.protocol: SASL_SSL
				    sasl.mechanism: OAUTHBEARER
				    aws.region: "$AWS_REGION"
			SITE
		elif [[ $KAFKA_DEPLOYMENT == in-cluster ]]; then
			cat <<-SITE
				  security: {}
			SITE
		fi
		if [[ $WITH_SCHEMA_REGISTRY == true && $KAFKA_DEPLOYMENT != external ]]; then
			cat <<-SITE
				  schema_registry:
				    url: "http://schema-registry.$NAMESPACE.svc:8080/apis/ccompat/v7"
			SITE
		fi
		if [[ $KAFKA_DEPLOYMENT == external ]]; then
			printf 'catalog: %s\n' "$(yq -o=json -I=0 '.catalog' "$SITE_FILE")"
		elif [[ $KAFKA_DEPLOYMENT == in-cluster ]]; then
			cat <<-SITE
				catalog:
				  props:
				    uri: "http://lakekeeper.$NAMESPACE.svc:8181/catalog"
				    warehouse: ingest-bench
				    s3.region: "$AWS_REGION"
			SITE
		else
			cat <<-SITE
				catalog:
				  props:
				    uri: "https://glue.$AWS_REGION.amazonaws.com/iceberg"
				    warehouse: "$ACCOUNT"
				    rest.sigv4-enabled: "true"
				    rest.signing-name: glue
				    rest.signing-region: "$AWS_REGION"
				    s3.region: "$AWS_REGION"
			SITE
		fi
		cat <<-SITE
			kubernetes:
			  context: "$KUBE_CONTEXT"
			  namespace: "$NAMESPACE"
			  harness_service_account: "$HARNESS_SERVICE_ACCOUNT"
			  flink_service_account: "$FLINK_SERVICE_ACCOUNT"
			  spark_service_account: "$SPARK_SERVICE_ACCOUNT"
			  service_account_annotations: {}
			  registry: "$ACCOUNT.dkr.ecr.$AWS_REGION.amazonaws.com"
			  aws_region: "$AWS_REGION"
			  # Optional Secret supplying environment variables for \${env:NAME} references.
			  # secret_name: bench-env
			  node_selector: $NODE_SELECTOR
			  tolerations: $TOLERATIONS
			# Set hourly vCPU and GiB prices using docs/methodology.md's Cost section.
			# Published results cannot use these zero defaults.
			pricing: {vcpu_hour_usd: 0.0, gib_hour_usd: 0.0}
		SITE
	} >"$path"
	# Validate the generated YAML. Remove invalid output so the overwrite guard
	# does not prevent a corrected retry.
	if ! parsed="$(yq -e '.kafka.bootstrap_servers' "$path" 2>&1)"; then
		read_back="yq could not read a site config out of it: $parsed"
	elif [[ $parsed != "$BOOTSTRAP" ]]; then
		read_back="its kafka.bootstrap_servers reads back as '$parsed' rather than '$BOOTSTRAP'"
	fi
	if [[ -n $read_back ]]; then
		rm -f "$path"
		die "removed $path because the generated site configuration failed validation: $read_back"
	fi
	log "wrote $path"
}

# Require the benchmark tag before deleting corpus, run artifacts, and warehouse data.
remove_bucket() {
	local head_error
	if ! head_error="$(aws_lookup s3api head-bucket --bucket "$BUCKET")"; then
		case "$head_error" in
		*"(404)"* | *"(NoSuchBucket)"*)
			log "s3://$BUCKET is already gone"
			return 0
			;;
		*) die "could not inspect s3://$BUCKET: $head_error" ;;
		esac
	fi
	bucket_is_ours "$BUCKET" ||
		die "cannot verify the $TAG_KEY=true tag on s3://$BUCKET: refusing to empty it.
     Check bucket access and BUCKET, or delete the bucket manually"
	printf 'deleting %s permanently removes:\n' "s3://$BUCKET"
	printf '  every corpus generated into it\n'
	printf '  every run%s artifacts, scores and publish logs\n' "'s"
	printf '  the warehouse, and every table any run has written\n'
	if [[ $ASSUME_YES == no ]]; then
		confirm "delete the resources listed above?"
	fi
	log "emptying and deleting s3://$BUCKET"
	aws s3 rm "s3://$BUCKET" --recursive >/dev/null
	local delete_error
	if ! delete_error="$(aws s3api delete-bucket --bucket "$BUCKET" 2>&1)"; then
		case "$delete_error" in
		*BucketNotEmpty*)
			die "s3://$BUCKET still holds objects: $delete_error
     A bucket that was versioned before keeps its noncurrent versions, which \`aws s3 rm\` does not remove.
     Delete them (aws s3api list-object-versions / delete-objects) and re-run with --all."
			;;
		*) die "could not delete s3://$BUCKET: $delete_error" ;;
		esac
	fi
}

choose_kafka_version() {
	local KAFKA_VERSIONS=$1
	# Choose the newest plain ACTIVE 3.x release, excluding tiered variants.
	# Sort a trailing x after numeric patches using a temporary sentinel.
	# Allow no-match through pipefail so the next check can explain the failure.
	MSK_KAFKA_VERSION="$(tr '\t' '\n' <<<"$KAFKA_VERSIONS" |
		grep -E '^3\.[0-9]+\.([0-9]+|x)$' |
		while IFS=. read -r major minor patch; do
			sort_patch=$patch
			[[ $patch == x ]] && sort_patch=999999
			printf '%s %s %s\t%s.%s.%s\n' "$major" "$minor" "$sort_patch" "$major" "$minor" "$patch"
		done |
		sort -k1,1n -k2,2n -k3,3n | tail -1 | cut -f2 || true)"
	[[ -n $MSK_KAFKA_VERSION ]] ||
		die "no ACTIVE 3.x Kafka version among ${KAFKA_VERSIONS//$'\t'/ }; set MSK_KAFKA_VERSION yourself"
}

# A pending Helm operation needs explicit recovery; starting another cannot fix it.
check_operator_release() (
	local release=$1 namespace=$2 status_json status diagnostics
	diagnostics="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-helm.XXXXXX")" || return
	trap 'rm -f "$diagnostics"' EXIT
	if status_json="$(helm --kube-context "$KUBE_CONTEXT" status "$release" \
		--namespace "$namespace" --output json 2>"$diagnostics")"; then
		cat "$diagnostics" >&2
		status="$(jq -er '.info.status | select(type == "string")' <<<"$status_json")" ||
			die "invalid Helm status for operator release $namespace/$release"
		case "$status" in
		pending-* | uninstalling)
			die "operator release $namespace/$release is $status. Inspect helm status and helm history, then finish or recover that operation before rerunning setup."
			;;
		esac
	else
		cat "$diagnostics" >&2
		if ! grep -q 'release: not found' "$diagnostics"; then
			die "could not inspect operator release $namespace/$release"
		fi
	fi
)

# An engine operator left out of OPERATORS must already be installed. Warn
# rather than fail, because a site may never run that engine.
check_external_operator() {
	local name=$1 crd=$2
	log "keeping the existing $name: OPERATORS ('$OPERATORS') leaves it out"
	if ! kubectl --context "$KUBE_CONTEXT" get crd "$crd" >/dev/null 2>&1; then
		log "WARNING: no $crd CRD on $KUBE_CONTEXT; runs on this engine need a $name watching $NAMESPACE"
	fi
}

install_spark_operator() {
	check_operator_release "$SPARK_OPERATOR_RELEASE" "$SPARK_OPERATOR_NAMESPACE"
	log "reconciling the Kubeflow spark-operator $SPARK_OPERATOR_VERSION"
	helm repo add "$SPARK_OPERATOR_RELEASE" "$SPARK_OPERATOR_REPO" --force-update
	# Use the benchmark service account bound to Pod Identity. The webhook adds
	# the ConfigMap volume mounts needed at /opt/bench/run.
	helm --kube-context "$KUBE_CONTEXT" upgrade --install "$SPARK_OPERATOR_RELEASE" \
		"$SPARK_OPERATOR_RELEASE/spark-operator" \
		--namespace "$SPARK_OPERATOR_NAMESPACE" --create-namespace \
		--version "$SPARK_OPERATOR_VERSION" \
		--set "spark.jobNamespaces={$NAMESPACE}" \
		--set spark.serviceAccount.create=false \
		--set spark.rbac.create=false \
		--set webhook.enable=true \
		--wait

	# Record the reconciled chart version.
	if ! SPARK_OPERATOR_RELEASES="$(helm --kube-context "$KUBE_CONTEXT" list --namespace "$SPARK_OPERATOR_NAMESPACE" \
		--filter "^$SPARK_OPERATOR_RELEASE\$" --output json 2>&1)"; then
		die "helm could not list the releases on $KUBE_CONTEXT: $SPARK_OPERATOR_RELEASES"
	fi
	log "spark operator: $(jq -r '.[0].chart // "not a helm release on this cluster"' <<<"$SPARK_OPERATOR_RELEASES")"
}

remove_msk_security_group() {
	local MSK_SG_NAME MSK_SG_ID DELETE_ERROR
	MSK_SG_NAME="$MSK_NAME-msk"
	[[ -n $VPC_ID && $VPC_ID != null ]] ||
		die "cannot identify the VPC for $CLUSTER_NAME; set VPC_ID to its original VPC and rerun to remove the MSK security group safely"
	# Names and benchmark tags can repeat across VPCs.
	MSK_SG_ID="$(aws ec2 describe-security-groups \
		--filters "Name=vpc-id,Values=$VPC_ID" "Name=group-name,Values=$MSK_SG_NAME" "Name=tag:$TAG_KEY,Values=true" \
		--query 'SecurityGroups[0].GroupId' --output text)"
	if [[ -n $MSK_SG_ID && $MSK_SG_ID != None ]]; then
		log "deleting security group $MSK_SG_NAME ($MSK_SG_ID)"
		if ! DELETE_ERROR="$(aws ec2 delete-security-group --group-id "$MSK_SG_ID" 2>&1)"; then
			case "$DELETE_ERROR" in
			*DependencyViolation*)
				die "$MSK_SG_ID is still in use: $DELETE_ERROR
	     MSK may retain network interfaces for a few minutes after cluster deletion. Wait, then rerun this script."
				;;
			*) die "could not delete $MSK_SG_ID: $DELETE_ERROR" ;;
			esac
		fi
	else
		log "security group $MSK_SG_NAME is already gone"
	fi
}

# Bypass NAT processing charges for same-region S3 traffic. Cover every route
# table so node groups in any subnet use a gateway endpoint.
ensure_s3_gateway_endpoint() {
	local service route_tables reported endpoint owned covered missing table
	service="com.amazonaws.$AWS_REGION.s3"
	route_tables="$(aws ec2 describe-route-tables --filters "Name=vpc-id,Values=$VPC_ID" \
		--query 'RouteTables[].RouteTableId' --output text | tr '\t' ' ')" ||
		die "could not list the route tables of $VPC_ID"
	[[ -n $route_tables ]] || die "$VPC_ID has no route tables to attach an S3 gateway endpoint to"
	reported="$(aws ec2 describe-vpc-endpoints \
		--filters "Name=vpc-id,Values=$VPC_ID" "Name=service-name,Values=$service" "Name=vpc-endpoint-type,Values=Gateway" \
		--query 'VpcEndpoints' --output json)" ||
		die "could not list the VPC endpoints of $VPC_ID"
	endpoint="$(jq -r '.[0].VpcEndpointId // empty' <<<"$reported")"
	# Different endpoints can cover different tables; preserve every existing route.
	covered="$(jq -r '[.[].RouteTableIds[]] | unique | join(" ")' <<<"$reported")"
	if [[ -z $endpoint ]]; then
		log "creating an S3 gateway endpoint in $VPC_ID on route tables $route_tables"
		# shellcheck disable=SC2086  # a deliberate expansion: one id per argument
		aws ec2 create-vpc-endpoint --vpc-id "$VPC_ID" --service-name "$service" --vpc-endpoint-type Gateway \
			--route-table-ids $route_tables \
			--tag-specifications "ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=$TAG_KEY-s3},{Key=$TAG_KEY,Value=true}]" >/dev/null
		return
	fi
	missing=""
	for table in $route_tables; do
		[[ " $covered " == *" $table "* ]] || missing="${missing:+$missing }$table"
	done
	# Extend only the benchmark's own endpoint. Another owner's endpoint may carry
	# a restrictive policy, so uncovered tables get a new tagged endpoint instead.
	owned="$(jq -r --arg tag "$TAG_KEY" \
		'map(select(any(.Tags[]?; .Key == $tag and .Value == "true"))) | .[0].VpcEndpointId // empty' <<<"$reported")"
	if [[ -n $missing && -n $owned ]]; then
		log "attaching route tables $missing to the S3 gateway endpoint $owned"
		# shellcheck disable=SC2086  # a deliberate expansion: one id per argument
		aws ec2 modify-vpc-endpoint --vpc-endpoint-id "$owned" --add-route-table-ids $missing >/dev/null
	elif [[ -n $missing ]]; then
		log "creating an S3 gateway endpoint in $VPC_ID on route tables $missing; endpoint $endpoint is not tagged $TAG_KEY=true"
		# shellcheck disable=SC2086  # a deliberate expansion: one id per argument
		aws ec2 create-vpc-endpoint --vpc-id "$VPC_ID" --service-name "$service" --vpc-endpoint-type Gateway \
			--route-table-ids $missing \
			--tag-specifications "ResourceType=vpc-endpoint,Tags=[{Key=Name,Value=$TAG_KEY-s3},{Key=$TAG_KEY,Value=true}]" >/dev/null
	else
		log "S3 gateway endpoint routing covers every route table in $VPC_ID"
	fi
}

# Endpoints block VPC deletion. Remove only a benchmark-owned gateway endpoint;
# leave untagged endpoints to their owners.
remove_s3_gateway_endpoint() {
	local endpoint reported
	if [[ -z $VPC_ID || $VPC_ID == null ]]; then
		log "skipping the S3 gateway endpoint: set VPC_ID to remove it from a VPC that outlives $CLUSTER_NAME"
		return
	fi
	endpoint="$(aws ec2 describe-vpc-endpoints \
		--filters "Name=vpc-id,Values=$VPC_ID" "Name=service-name,Values=com.amazonaws.$AWS_REGION.s3" \
		"Name=vpc-endpoint-type,Values=Gateway" "Name=tag:$TAG_KEY,Values=true" \
		--query 'VpcEndpoints[0].VpcEndpointId' --output text)"
	if [[ $endpoint == vpce-* ]]; then
		log "deleting the S3 gateway endpoint $endpoint"
		reported="$(aws ec2 delete-vpc-endpoints --vpc-endpoint-ids "$endpoint" --output json)"
		# EC2 can return per-endpoint failures with a successful CLI exit status.
		jq -e '(.Unsuccessful // []) == []' <<<"$reported" >/dev/null ||
			die "could not delete S3 gateway endpoint $endpoint: $reported"
	else
		log "no S3 gateway endpoint tagged $TAG_KEY=true in $VPC_ID"
	fi
}
