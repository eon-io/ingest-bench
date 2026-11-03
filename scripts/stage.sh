#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Stage a run in a cluster Job, then start and verify its managed engine. The Job can
# reach the broker's private network and uploads the run artifacts for this driver to
# fetch.
# Only `run_id: <id>` is printed on stdout; diagnostics go to stderr.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Allow for corpus metadata reads, topic/table creation and a cold image pull.
STAGE_WAIT_S="${STAGE_WAIT_S:-600}"
# Allow time to provision nodes, pull images and schedule the engine fleet.
ENGINE_RUNNING_WAIT_S="${ENGINE_RUNNING_WAIT_S:-600}"
ENGINE_POLL_S="${ENGINE_POLL_S:-10}"
WAIT_BROKERS_S="${WAIT_BROKERS_S:-1200}"

usage() {
	cat <<'USAGE'
usage: scripts/stage.sh <spec> [options]

  <spec>             a run spec on this machine, copied into the Job's ConfigMap
  --site PATH        site config for the cluster, the broker and the catalog (default: ./site.yaml)
  --image-tag TAG    the harness and engine image tag to run (default: this checkout's commit)
  --wait-brokers-s N before staging onto in-cluster Kafka, wait up to N seconds for
                     broker deletion and disk activity to settle (default: 1200; 0 skips)

Environment: STAGE_WAIT_S, ENGINE_RUNNING_WAIT_S, ENGINE_POLL_S, WAIT_BROKERS_S, RUNS_DIR.

Prints `run_id: <id>` as its final stdout line. Pass this ID to launch.sh.
USAGE
}

SPEC=""
IMAGE_TAG=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--image-tag)
		IMAGE_TAG="${2:?--image-tag needs a tag}"
		shift 2
		;;
	--wait-brokers-s)
		WAIT_BROKERS_S="${2:?--wait-brokers-s needs a number of seconds}"
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	-*)
		printf 'unknown argument %s\n\n' "$1" >&2
		usage >&2
		exit 2
		;;
	*)
		[[ -z $SPEC ]] || die "expected one run spec; got '$SPEC' and '$1'"
		SPEC="$1"
		shift
		;;
	esac
done

[[ -n $SPEC ]] || {
	printf 'a run spec is required\n\n' >&2
	usage >&2
	exit 2
}
[[ $WAIT_BROKERS_S =~ ^[0-9]+$ ]] || die "--wait-brokers-s requires a non-negative integer; got '$WAIT_BROKERS_S'"

require_host_tools kubectl aws yq jq git curl
[[ -f $SPEC ]] || die "no run spec at $SPEC"

k8s_read_site
KAFKA_DEPLOYMENT="$(site_value '.kafka.deployment')"
if [[ $KAFKA_DEPLOYMENT == in-cluster ]]; then
	log "checking in-cluster Kafka capacity and waiting for quiet brokers"
	"$REPO_ROOT/scripts/preflight-kafka.sh" "$SPEC" --site "$SITE_FILE" --wait-brokers-s "$WAIT_BROKERS_S"
fi
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

# Use the spec name to address the staging Job before a run ID exists.
SPEC_NAME="$(yq '.name' "$SPEC")"
[[ -n $SPEC_NAME && $SPEC_NAME != null ]] || die "$SPEC has no run name"
ENGINE="$(yq '.engine' "$SPEC")"

STAGE_JOB="stage-$SPEC_NAME"
SPEC_CONFIGMAP="$STAGE_JOB-spec"
SITE_CONFIGMAP="$STAGE_JOB-site"

# ---------------------------------------------------------------------------
# Stage
# ---------------------------------------------------------------------------

# Remove both temporary ConfigMaps on every exit, including the one holding site settings.
# Cleanup failures must not replace the original exit status.
delete_stage_configmaps() {
	k8s_delete configmap "$SPEC_CONFIGMAP" || true
	k8s_delete configmap "$SITE_CONFIGMAP" || true
}

log "staging $SPEC as job/$STAGE_JOB with $IMAGE"
# A Job's spec is immutable, so the previous Job of this name goes first.
k8s_delete job "$STAGE_JOB"
trap delete_stage_configmaps EXIT
k8s_configmap_from_file "$SPEC_CONFIGMAP" "$(basename -- "$SPEC")=$SPEC"
k8s_configmap_from_file "$SITE_CONFIGMAP" "site.yaml=$SITE_FILE"

STAGE_COMMAND=(stage --spec "/runs/$(basename -- "$SPEC")" --site /site/site.yaml --runs-dir /work/runs
	--image-tag "$TAG" --upload-prefix "$RUNS_ROOT")
k8s_render_apply deploy/k8s/stage-job.yaml.tmpl \
	"NAME=$STAGE_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${STAGE_COMMAND[@]}")" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS" \
	"SPEC_CONFIGMAP=$SPEC_CONFIGMAP" \
	"SITE_CONFIGMAP=$SITE_CONFIGMAP"
k8s_wait_job "$STAGE_JOB" "$STAGE_WAIT_S"

# Read the run ID from staging's log. Capture the log before parsing: awk may close a
# pipeline early and cause kubectl to fail with SIGPIPE under pipefail.
LOGS="$(k8s_job_logs "$STAGE_JOB")" || die "could not read job/$STAGE_JOB's log; try: kubectl logs job/$STAGE_JOB"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' <<<"$LOGS")"
[[ -n $RUN_ID ]] || die "job/$STAGE_JOB printed no run_id line; read its log with: kubectl logs job/$STAGE_JOB"

RUN_DIR="$RUNS_DIR/$RUN_ID"
# Use the same object name as the engine manifests.
RUN_OBJECT="$(k8s_object_name "$RUN_ID")"
mkdir -p "$RUN_DIR"
log "fetching the run directory into $RUN_DIR"
aws s3 sync "$RUNS_ROOT/$RUN_ID/stage/" "$RUN_DIR/" --only-show-errors >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/stage/; check your local storage credentials"
[[ -f $RUN_DIR/facts.json ]] || die "$RUNS_ROOT/$RUN_ID/stage/ holds no facts.json"

k8s_delete job "$STAGE_JOB"
delete_stage_configmaps
trap - EXIT

# ---------------------------------------------------------------------------
# The compaction sidecar
# ---------------------------------------------------------------------------

# A rendered ConfigMap means the spec declared a driver the harness starts. The
# sidecar is a layer over whichever engine is ingesting, so it starts the same
# way whether that engine is managed here or not.
if [[ -f $RUN_DIR/$COMPACTOR_CONFIGMAP_FILE ]]; then
	log "starting the compaction sidecar"
	k8s_apply_file "$RUN_DIR/$COMPACTOR_CONFIGMAP_FILE"
	k8s_apply_file "$RUN_DIR/$COMPACTOR_DOCUMENT_FILE"
fi

# ---------------------------------------------------------------------------
# The engine
# ---------------------------------------------------------------------------

if [[ $ENGINE == external ]]; then
	# Keep facts on stderr so stdout contains only the run ID line.
	log "start your engine against these facts, in $RUN_DIR/facts.json:"
	cat "$RUN_DIR/facts.json" >&2
	log "then: scripts/launch.sh $RUN_ID"
else
	# Read object names and status paths from the engine descriptor.
	k8s_read_engine "$ENGINE" "$RUN_OBJECT"

	# Apply the ConfigMap before pods that mount it.
	log "starting the engine"
	k8s_apply_file "$RUN_DIR/$ENGINE_CONFIGMAP_FILE"
	k8s_apply_file "$RUN_DIR/$ENGINE_DOCUMENT_FILE"


	log "waiting up to ${ENGINE_RUNNING_WAIT_S}s for $ENGINE_KIND/$RUN_OBJECT to reach $ENGINE_RUNNING_STATE"
	waited=0
	# Log each new reconciliation error once.
	reported_error=""
	while :; do
		state="$(k8s_engine_field "$ENGINE_KIND" "$RUN_OBJECT" "$ENGINE_STATE_JSONPATH")"
		if [[ $state == "$ENGINE_RUNNING_STATE" ]]; then
			log "$ENGINE_KIND/$RUN_OBJECT is $state"
			break
		fi

		error="$(k8s_engine_field "$ENGINE_KIND" "$RUN_OBJECT" "$ENGINE_ERROR_JSONPATH")"
		# Match whole comma-delimited states, not prefixes.
		if [[ -n $state && ",$ENGINE_FAILED_STATES," == *",$state,"* ]]; then
			k8s_engine_tail "$ENGINE_LOG_TARGET"
			die "$ENGINE_KIND/$RUN_OBJECT went to $state before it ran${error:+: $error}; the lines above are the engine's own log"
		fi
		# An error can be transient during reconciliation. Fail only when the lifecycle also
		# reports a terminal failure, including rejection before a job was created.
		if [[ -n $error ]]; then
			lifecycle="$(k8s_engine_field "$ENGINE_KIND" "$RUN_OBJECT" "$ENGINE_LIFECYCLE_JSONPATH")"
			if [[ -n $lifecycle && ",$ENGINE_FAILED_STATES," == *",$lifecycle,"* ]]; then
				# Tail logs only if pods exist; rejected submissions may have none.
				[[ -z "$(k8s_pods_present "$ENGINE_PROVENANCE_SELECTOR")" ]] ||
					k8s_engine_tail "$ENGINE_LOG_TARGET"
				die "the operator failed to start $ENGINE_KIND/$RUN_OBJECT ($lifecycle): $error"
			fi
			if [[ $error != "$reported_error" ]]; then
				log "$ENGINE_KIND/$RUN_OBJECT reports an error; the operator has not reported a terminal failure: $error"
				reported_error="$error"
			fi
		fi
		if ((waited >= ENGINE_RUNNING_WAIT_S)); then
			k8s_engine_tail "$ENGINE_LOG_TARGET"
			die "$ENGINE_KIND/$RUN_OBJECT did not reach $ENGINE_RUNNING_STATE within ${ENGINE_RUNNING_WAIT_S}s (last state: ${state:-none reported}${reported_error:+, last error: $reported_error})"
		fi
		sleep "$ENGINE_POLL_S"
		waited=$((waited + ENGINE_POLL_S))
	done

	# Read back engine settings before producing: RUNNING alone does not prove the submitted
	# configuration took effect. Use a high local port to reduce conflicts with local
	# services; overridable because two sessions staging on one machine otherwise collide.
	VERIFY_PORT="${VERIFY_PORT:-18081}"
	# Configuration drift is fatal; unavailable endpoints and pending fleet placement have
	# separate retry policies.
	VERIFY_DRIFT_STATUS=3
	VERIFY_PENDING_STATUS=4
	VERIFY_TRIES=3
	# Resolve the path before harness_local can change directory.
	VERIFY_SPEC="$(cd -- "$RUN_DIR" && pwd)/spec.yaml" || die "could not resolve $RUN_DIR to check the engine against"
	[[ -f $VERIFY_SPEC ]] ||
		die "$RUNS_ROOT/$RUN_ID/stage/ holds no spec.yaml, so the engine has nothing to be checked against"
	# Keep raw pod data temporary; the saved resource artifact omits environment values.
	VERIFY_PODS="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-pods.XXXXXX")" ||
		die "could not create a temporary file for the run's pod details"

	# Install cleanup before opening the tunnel so failures leave no process behind.
	trap 'k8s_port_forward_stop; [[ -z $VERIFY_PODS ]] || rm -f "$VERIFY_PODS"' EXIT
	k8s_port_forward "svc/$RUN_OBJECT$ENGINE_REST_SERVICE_SUFFIX" "$VERIFY_PORT:$ENGINE_REST_PORT"

	log "checking $ENGINE_KIND/$RUN_OBJECT against $VERIFY_SPEC"
	verify_tries=0
	placement_waited=0
	while :; do
		verify_args=(--spec "$VERIFY_SPEC" --run-id "$RUN_ID" --rest "http://localhost:$VERIFY_PORT")
		# Refresh pod state on each retry to observe pending executors.
		if [[ -n $ENGINE_PODS_SELECTOR ]]; then
			k8s_write_pods "$VERIFY_PODS" "$ENGINE_PODS_SELECTOR"
			verify_args+=(--pods "$VERIFY_PODS")
		fi
		# Keep verifier output on stderr, separate from the run ID.
		verify_status=0
		harness_local "verify-$ENGINE" "${verify_args[@]}" >&2 || verify_status=$?
		if ((verify_status == 0)); then
			break
		fi
		if ((verify_status == VERIFY_DRIFT_STATUS)); then
			die "$ENGINE_KIND/$RUN_OBJECT does not match $SPEC; see the setting mismatches above. It is left running for inspection"
		fi
		# Pending pods get the full placement timeout; endpoint read failures get a limited
		# retry count.
		if ((verify_status == VERIFY_PENDING_STATUS)); then
			placement_waited=$((placement_waited + ENGINE_POLL_S))
			if ((placement_waited > ENGINE_RUNNING_WAIT_S)); then
				die "$ENGINE_KIND/$RUN_OBJECT's resources were not fully scheduled within ${ENGINE_RUNNING_WAIT_S}s; see the missing resources above. It is left running for inspection"
			fi
			sleep "$ENGINE_POLL_S"
			continue
		fi
		verify_tries=$((verify_tries + 1))
		if ((verify_tries >= VERIFY_TRIES)); then
			die "could not verify $ENGINE_KIND/$RUN_OBJECT through its endpoint after $VERIFY_TRIES attempts (last exit $verify_status); cannot confirm that the engine matches the staged spec"
		fi
		sleep "$ENGINE_POLL_S"
	done
	k8s_port_forward_stop

	# Capture admitted requests while the complete fleet exists. Keep the artifact
	# with staged files so collection can run after teardown or from another host.
	k8s_write_pods "$VERIFY_PODS" "$ENGINE_FLEET_SELECTOR"
	harness_local engine-fleet --spec "$VERIFY_SPEC" --pods "$VERIFY_PODS" --out "${VERIFY_SPEC%/*}/engine-pods.json"
	aws s3 cp "$RUN_DIR/engine-pods.json" "$RUNS_ROOT/$RUN_ID/stage/engine-pods.json" --only-show-errors >&2

	# Capture image provenance while the fleet still exists; teardown provides a fallback.
	k8s_write_engine_image "$RUN_DIR/$ENGINE_IMAGE_FILE" "$ENGINE_PROVENANCE_SELECTOR"
fi

if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]]; then
	log "capturing the compactor fleet's admitted requests"
	deadline=$((SECONDS + ENGINE_RUNNING_WAIT_S))
	until k8s_capture_compactor_fleet "$RUN_DIR"; do
		((SECONDS < deadline)) ||
			die "could not capture the complete compactor fleet within ${ENGINE_RUNNING_WAIT_S}s; it is left running for inspection"
		remaining=$((deadline - SECONDS))
		sleep "$((remaining < ENGINE_POLL_S ? remaining : ENGINE_POLL_S))"
	done
fi

printf 'run_id: %s\n' "$RUN_ID"
