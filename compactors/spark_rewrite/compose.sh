# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Local rewrite-sidecar lifecycle hooks, sourced by scripts/smoke.sh.
# Kubernetes uses the two rendered manifests named in specs/compactors.py.
#
# The three compactor_compose_* hooks run in declaration order. scripts/_lib.sh
# provides compose, log and die; the caller sets RUN_ID, RUN_DIR, SPEC_FILE and
# REPO_ROOT before invoking them.
#
# There is no start readiness hook. The final diagnostics hook records current
# container state, while table metadata records what the sidecar accomplished.

# The submission environment the renderer wrote beside the run's other files.
COMPACTOR_ENV_FILE=compactor-job.env

compactor_compose_build() {
	log "building the compaction sidecar image"
	compose build spark-compactor
}

compactor_compose_start() {
	# Export cores and heap for Compose. local[N] is an argument to
	# spark-submit, not a setting it reads.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/$COMPACTOR_ENV_FILE"
	set +a
	log "starting the compaction sidecar: local[$COMPACTOR_LOCAL_CORES], ${COMPACTOR_DRIVER_MEM_MB}m driver"
	compose up -d spark-compactor
}

compactor_compose_logs() {
	local container_id inspect_error inspect_json status status_tmp
	local container_ids=()
	log "--- last 30 lines of spark-compactor ---"
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
		compose logs --no-log-prefix spark-compactor >"$RUN_DIR/scores/compactor.log" 2>&1 || true
		tail -n 30 "$RUN_DIR/scores/compactor.log" >&2 || true
		while IFS= read -r container_id; do
			[[ -z $container_id ]] || container_ids+=("$container_id")
		done < <(compose ps --all --quiet spark-compactor 2>/dev/null || true)
		if ((${#container_ids[@]} == 0)); then
			jq -n '{captured_at: now, healthy: false,
				capture_error: "no spark-compactor containers found", containers: []}' >"$status_tmp" &&
				mv "$status_tmp" "$status" || true
		elif docker container inspect "${container_ids[@]}" >"$inspect_json" 2>"$inspect_error"; then
			jq '{captured_at: now,
				healthy: (length > 0 and all(.[];
					.State.Status == "running" and
					((.State.Health // null) == null or .State.Health.Status == "healthy"))),
				capture_error: null,
				containers: [.[] | {
					id: .Id,
					name: (.Name | ltrimstr("/")),
					image: .Config.Image,
					image_id: .Image,
					state: .State.Status,
					health: (.State.Health.Status // null),
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
		compose logs --tail 30 --no-log-prefix spark-compactor >&2 || true
	fi
	# Diagnostics are best effort so cleanup always reaches Compose teardown.
	return 0
}
