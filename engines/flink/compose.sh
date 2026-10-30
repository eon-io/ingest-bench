# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Local Flink lifecycle hooks, sourced by scripts/smoke.sh. Kubernetes
# addressing is defined separately in specs/kubernetes.py.
#
# The four engine_compose_* hooks run in declaration order. scripts/_lib.sh
# provides compose, harness, log, and die; the caller sets RUN_ID, RUN_DIR,
# SPEC_FILE, and REPO_ROOT before invoking them.

# JobManager REST endpoint published by Compose.
FLINK_REST="${FLINK_REST:-http://localhost:8081}"

# Allow time for a cold local cluster to register its slots and job.
FLINK_SLOT_WAIT_S="${FLINK_SLOT_WAIT_S:-180}"
FLINK_JOB_WAIT_S="${FLINK_JOB_WAIT_S:-180}"

# Treat unavailable or non-numeric responses as zero while the endpoint starts.
_rest_number() {
	local value
	value="$(curl -sf --max-time 5 "$1" 2>/dev/null | jq -r "$2" 2>/dev/null || true)"
	case "$value" in '' | *[!0-9]*) value=0 ;; esac
	printf '%s' "$value"
}

# Wait for registered slots; running containers alone cannot accept a job.
wait_for_flink_slots() {
	local wanted=$1 waited=0 total=0
	while ((waited < FLINK_SLOT_WAIT_S)); do
		total="$(_rest_number "$FLINK_REST/overview" '."slots-total" // 0')"
		if ((total >= wanted)); then
			log "Flink slots registered: $total"
			return 0
		fi
		sleep 2
		waited=$((waited + 2))
	done
	die "Flink registered $total of $wanted required slots after ${FLINK_SLOT_WAIT_S}s; check: compose logs flink-taskmanager"
}

# Match pipeline.name to the run ID so an unrelated job cannot satisfy readiness.
wait_for_flink_job_running() {
	local name=$1 waited=0 state=""
	while ((waited < FLINK_JOB_WAIT_S)); do
		state="$(curl -sf --max-time 5 "$FLINK_REST/jobs/overview" 2>/dev/null |
			jq -r --arg name "$name" '.jobs[]? | select(.name == $name) | .state' 2>/dev/null || true)"
		case "$state" in
		RUNNING)
			log "Flink job $name is RUNNING"
			return 0
			;;
		FAILED | CANCELED | FINISHED)
			die "Flink job $name reached $state before RUNNING was observed; check: compose logs flink-jobmanager"
			;;
		esac
		sleep 2
		waited=$((waited + 2))
	done
	die "Flink job $name did not reach RUNNING within ${FLINK_JOB_WAIT_S}s (last state: ${state:-none reported})"
}

engine_compose_build() {
	log "building the engine image"
	compose build flink-jobmanager
}

engine_compose_start() {
	# Export rendered sizing for Compose's slot and memory interpolation.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/flink.env"
	set +a
	log "starting Flink: $TASKMANAGERS TaskManagers, $SLOTS slots each"
	compose up -d --scale "flink-taskmanager=$TASKMANAGERS" flink-jobmanager flink-taskmanager
	wait_for_flink_slots "$((TASKMANAGERS * SLOTS))"
	log "submitting the job"
	compose run --rm -T flink-job
}

engine_compose_ready() {
	wait_for_flink_job_running "$RUN_ID"
	# Verify effective settings after RUNNING. Use the service name because the
	# check runs inside the Compose network.
	log "checking the running job against its staged spec"
	harness "verify-flink --spec /runs/$RUN_ID/spec.yaml --run-id $RUN_ID --rest http://flink-jobmanager:8081" ||
		die "Flink settings do not match $(basename "$SPEC_FILE"); see the verification errors above"
}

engine_compose_logs() {
	log "--- last 40 lines of flink-jobmanager ---"
	compose logs --tail 40 --no-log-prefix flink-jobmanager >&2 || true
}
