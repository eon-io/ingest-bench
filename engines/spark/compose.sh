# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Local Spark lifecycle hooks, sourced by scripts/smoke.sh. Kubernetes
# addressing is defined separately in specs/kubernetes.py.
#
# The four engine_compose_* hooks run in declaration order. scripts/_lib.sh
# provides compose, harness, log, and die; the caller sets RUN_ID, RUN_DIR,
# SPEC_FILE, and REPO_ROOT before invoking them.

# Read readiness from the driver's UI; local mode has no separate submitter.
SPARK_UI="${SPARK_UI:-http://localhost:4040}"
SPARK_APP_WAIT_S="${SPARK_APP_WAIT_S:-180}"
SPARK_QUERY_WAIT_S="${SPARK_QUERY_WAIT_S:-180}"

# Spark's streaming REST endpoint serves DStreams, not Structured
# Streaming. Poll the active-query count in the Structured Streaming UI.
_spark_active_queries() {
	local count
	count="$(curl -sf --max-time 5 "$SPARK_UI/StreamingQuery/" 2>/dev/null | tr -d '\n' |
		sed -n 's/.*Active Streaming Queries (\([0-9]*\)).*/\1/p' | head -n 1)"
	case "$count" in '' | *[!0-9]*) count=0 ;; esac
	printf '%s' "$count"
}

# Report an exited driver immediately rather than waiting for a UI timeout.
_die_if_the_spark_driver_exited() {
	if compose ps --status exited --services 2>/dev/null | grep -qx spark-job; then
		die "the spark driver exited before its query started; check: compose logs spark-job"
	fi
}

# Match the run ID to avoid accepting an application left by an earlier --keep.
wait_for_spark_query() {
	local name=$1 waited=0 running="" queries=0
	while ((waited < SPARK_APP_WAIT_S)); do
		running="$(curl -sf --max-time 5 "$SPARK_UI/api/v1/applications" 2>/dev/null |
			jq -r --arg name "$name" 'map(select(.name == $name)) | length' 2>/dev/null || true)"
		if [[ $running == 1 ]]; then break; fi
		_die_if_the_spark_driver_exited
		sleep 2
		waited=$((waited + 2))
	done
	[[ $running == 1 ]] ||
		die "no Spark application named $name on $SPARK_UI after ${SPARK_APP_WAIT_S}s; check: compose logs spark-job"
	log "Spark application $name is available"
	waited=0
	while ((waited < SPARK_QUERY_WAIT_S)); do
		queries="$(_spark_active_queries)"
		if ((queries >= 1)); then
			log "Spark active streaming queries: $queries"
			return 0
		fi
		_die_if_the_spark_driver_exited
		sleep 2
		waited=$((waited + 2))
	done
	die "Spark did not start a streaming query within ${SPARK_QUERY_WAIT_S}s; check: compose logs spark-job"
}

engine_compose_build() {
	log "building the engine image"
	compose build spark-job
}

engine_compose_start() {
	# Export cores and heap for Compose. local[N] runs the whole fleet inside
	# this driver's JVM.
	set -a
	# shellcheck source=/dev/null
	source "$RUN_DIR/job.env"
	set +a
	log "starting Spark: local[$LOCAL_CORES], ${DRIVER_MEM_MB}m driver"
	compose up -d spark-job
}

engine_compose_ready() {
	wait_for_spark_query "$RUN_ID"
}

engine_compose_logs() {
	log "--- last 40 lines of spark-job ---"
	compose logs --tail 40 --no-log-prefix spark-job >&2 || true
}
