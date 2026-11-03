# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Local maintenance-sidecar lifecycle hooks, sourced by scripts/smoke.sh.
# Kubernetes uses the two rendered manifests named in specs/compactors.py.
#
# The three compactor_compose_* hooks run in declaration order. scripts/_lib.sh
# provides compose, log and die; the caller sets RUN_ID, RUN_DIR, SPEC_FILE and
# REPO_ROOT before invoking them.
#
# There is no start readiness hook. The start hook itself waits for the
# maintenance job to run, because a job that never starts would read as a
# sidecar that compacted nothing; the final diagnostics hook records the job's
# state, while table metadata records what it accomplished.

# The configuration and sizing the renderer wrote beside the run's other files.
COMPACTOR_CONF_FILE=compactor-flink-conf.yaml
COMPACTOR_ENV_FILE=compactor-flink.env

# The sidecar's own JobManager REST endpoint published by Compose.
FLINK_COMPACTOR_REST="${FLINK_COMPACTOR_REST:-http://localhost:8082}"
FLINK_COMPACTOR_WAIT_S="${FLINK_COMPACTOR_WAIT_S:-180}"

_compactor_rest_number() {
	local value
	value="$(curl -sf --max-time 5 "$1" 2>/dev/null | jq -r "$2" 2>/dev/null || true)"
	case "$value" in '' | *[!0-9]*) value=0 ;; esac
	printf '%s' "$value"
}

# The runner names its job after the run, so an unrelated job cannot satisfy this.
_compactor_job_state() {
	curl -sf --max-time 5 "$FLINK_COMPACTOR_REST/jobs/overview" 2>/dev/null |
		jq -r --arg name "maintenance $RUN_ID" '.jobs[]? | select(.name == $name) | .state' 2>/dev/null || true
}

compactor_compose_build() {
	log "building the compaction sidecar image"
	compose build flink-compactor-jobmanager
}

compactor_compose_start() {
	local waited=0 total=0 state=""
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/$COMPACTOR_ENV_FILE"
	set +a
	# The Flink image appends FLINK_PROPERTIES to its stock configuration. Export
	# the same rendered settings for the JobManager, TaskManagers and submitter.
	COMPACTOR_FLINK_PROPERTIES="$(<"$RUN_DIR/$COMPACTOR_CONF_FILE")"
	export COMPACTOR_FLINK_PROPERTIES
	log "starting the maintenance sidecar: $COMPACTOR_TASKMANAGERS TaskManagers, $COMPACTOR_SLOTS slots each, parallelism $COMPACTOR_PARALLELISM"
	compose up -d --scale "flink-compactor-taskmanager=$COMPACTOR_TASKMANAGERS" flink-compactor-jobmanager flink-compactor-taskmanager
	while ((waited < FLINK_COMPACTOR_WAIT_S)); do
		total="$(_compactor_rest_number "$FLINK_COMPACTOR_REST/overview" '."slots-total" // 0')"
		((total >= COMPACTOR_TASKMANAGERS * COMPACTOR_SLOTS)) && break
		sleep 2
		waited=$((waited + 2))
	done
	((total >= COMPACTOR_TASKMANAGERS * COMPACTOR_SLOTS)) ||
		die "the maintenance sidecar registered $total of $((COMPACTOR_TASKMANAGERS * COMPACTOR_SLOTS)) slots after ${FLINK_COMPACTOR_WAIT_S}s; check: compose logs flink-compactor-taskmanager"
	log "submitting the maintenance job"
	compose run --rm -T flink-compactor-job
	waited=0
	while ((waited < FLINK_COMPACTOR_WAIT_S)); do
		state="$(_compactor_job_state)"
		case "$state" in
		RUNNING)
			log "maintenance job for $RUN_ID is RUNNING"
			return 0
			;;
		FAILED | CANCELED | FINISHED)
			die "the maintenance job reached $state before RUNNING was observed; check: compose logs flink-compactor-jobmanager"
			;;
		esac
		sleep 2
		waited=$((waited + 2))
	done
	die "the maintenance job did not reach RUNNING within ${FLINK_COMPACTOR_WAIT_S}s (last state: ${state:-none reported})"
}

compactor_compose_logs() {
	local container_id inspect_error inspect_json state status status_tmp
	local container_ids=()
	log "--- last 30 lines of flink-compactor-jobmanager ---"
	if [[ -n ${RUN_DIR:-} && -d $RUN_DIR ]]; then
		mkdir -p "$RUN_DIR/scores" || return 0
		status="$RUN_DIR/scores/compactor-status.json"
		status_tmp="$status.tmp"
		inspect_json="$RUN_DIR/scores/compactor-status.inspect.json"
		inspect_error="$RUN_DIR/scores/compactor-status.inspect-error.txt"
		# Replace stale passing evidence before asking Docker for current state.
		jq -n '{captured_at: now, healthy: false,
			capture_error: "container status capture did not complete", containers: []}' >"$status_tmp" &&
			mv "$status_tmp" "$status" || true
		compose logs --no-log-prefix flink-compactor-jobmanager >"$RUN_DIR/scores/compactor.log" 2>&1 || true
		compose logs --no-log-prefix flink-compactor-taskmanager >"$RUN_DIR/scores/compactor-taskmanager.log" 2>&1 || true
		tail -n 30 "$RUN_DIR/scores/compactor.log" >&2 || true
		# The job's state is read before the containers stop: a failed maintenance
		# job leaves its cluster running, so container state alone cannot say it worked.
		state="$(_compactor_job_state)"
		while IFS= read -r container_id; do
			[[ -z $container_id ]] || container_ids+=("$container_id")
		done < <(compose ps --all --quiet flink-compactor-jobmanager flink-compactor-taskmanager 2>/dev/null || true)
		if ((${#container_ids[@]} == 0)); then
			jq -n '{captured_at: now, healthy: false,
				capture_error: "no maintenance sidecar containers found", containers: []}' >"$status_tmp" &&
				mv "$status_tmp" "$status" || true
		elif docker container inspect "${container_ids[@]}" >"$inspect_json" 2>"$inspect_error"; then
			jq --arg state "${state:-unknown}" '{captured_at: now,
				healthy: ($state == "RUNNING" and length > 0 and all(.[]; .State.Status == "running")),
				capture_error: null,
				job_state: $state,
				containers: [.[] | {
					id: .Id,
					name: (.Name | ltrimstr("/")),
					image: .Config.Image,
					image_id: .Image,
					state: .State.Status,
					exit_code: .State.ExitCode,
					error: .State.Error,
					started_at: .State.StartedAt,
					finished_at: .State.FinishedAt
				}]}' "$inspect_json" >"$status_tmp" && mv "$status_tmp" "$status" || true
		else
			jq -n --rawfile capture_error "$inspect_error" '{captured_at: now, healthy: false,
				capture_error: $capture_error, containers: []}' >"$status_tmp" &&
				mv "$status_tmp" "$status" || true
		fi
		rm -f "$status_tmp" "$inspect_json" "$inspect_error" || true
	else
		compose logs --tail 30 --no-log-prefix flink-compactor-jobmanager >&2 || true
	fi
	# Diagnostics are best effort so cleanup always reaches Compose teardown.
	return 0
}
