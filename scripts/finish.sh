#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Collect a completed run's verdict and optionally publish it. Fetch artifacts from object
# storage after the pods stop. Measure geometry here to keep manifest walks out of the
# scoring poll loop.
# Exit 0 only when run_valid is true.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Must match ingest_bench.scorer.cli.NO_GEOMETRY. A table without commits has no geometry;
# other nonzero statuses are read failures.
NO_GEOMETRY=4

# The in-cluster validator: a campaign table's scan reads a hundred thousand files, and
# the one-bit-per-ID set for eight billion IDs is about 1 GiB before read buffers.
VERIFY_WAIT_S="${VERIFY_WAIT_S:-7200}"
VERIFY_CPU="${VERIFY_CPU:-4}"
VERIFY_MEMORY="${VERIFY_MEMORY:-8Gi}"
VERIFY_READ_WORKERS="${VERIFY_READ_WORKERS:-32}"
# The upsert scan Job: local[N] with N cores requested and limited, and the JVM heap at 75% of the pod.
VERIFY_SPARK_CPU="${VERIFY_SPARK_CPU:-8}"
VERIFY_SPARK_MEMORY="${VERIFY_SPARK_MEMORY:-32Gi}"

usage() {
	cat <<'USAGE'
usage: scripts/finish.sh <run_id> [options]

  <run_id>            run with scorer output under the runs prefix
  --site PATH         site config for the runs prefix and the catalog (default: ./site.yaml)
  --variant NAME      configuration label recorded in the report (default: 'default')
  --publish DIR       publish the report under DIR/ and refresh DIR/RESULTS.md
  --publish-invalid   publish even though `run_valid` is false, as a result labelled by its state
  --verify-in-cluster run terminal validation as a Job in the cluster,
                      next to the object store, instead of on this host
  --image-tag TAG     the harness image tag that Job runs (default: this checkout's commit)

Environment: RUNS_DIR; VERIFY_WAIT_S, VERIFY_CPU, VERIFY_MEMORY, VERIFY_READ_WORKERS for the Job.

Prints the verdict and exits 0 only when `run_valid: true`.
USAGE
}

RUN_ID=""
VARIANT=""
PUBLISH_DIR=""
PUBLISH_INVALID=no
VERIFY_IN_CLUSTER=no
IMAGE_TAG=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--verify-in-cluster)
		VERIFY_IN_CLUSTER=yes
		shift
		;;
	--image-tag)
		IMAGE_TAG="${2:?--image-tag needs a tag}"
		shift 2
		;;
	--variant)
		VARIANT="${2:?--variant needs a name}"
		shift 2
		;;
	--publish)
		PUBLISH_DIR="${2:?--publish needs a directory}"
		shift 2
		;;
	--publish-invalid)
		PUBLISH_INVALID=yes
		shift
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
		[[ -z $RUN_ID ]] || die "expected one run ID; got '$RUN_ID' and '$1'"
		RUN_ID="$1"
		shift
		;;
	esac
done

[[ -n $RUN_ID ]] || {
	printf 'a run ID is required\n\n' >&2
	usage >&2
	exit 2
}
[[ $PUBLISH_INVALID == no || -n $PUBLISH_DIR ]] ||
	die "--publish-invalid requires --publish <dir>"
[[ $VERIFY_WAIT_S =~ ^[0-9]+$ ]] || die "VERIFY_WAIT_S must be a non-negative integer; got '$VERIFY_WAIT_S'"

require_host_tools aws jq yq kubectl
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

# Validate the pinned final snapshot from a Job in the cluster. The Job reads the same
# metadata document this host saved, uploaded first so both sides name one snapshot; it
# writes its verdict under the run's scores prefix, which is fetched back here. A failed
# Job is a failed validation, never a reason to stop collecting the run.
# Arguments: the validator command (verify-table or verify-upsert) and the verdict file it writes.
verify_in_cluster() {
	local validator=$1 artifact=$2 job image final_uri status=0
	k8s_read_site
	image="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$(k8s_image_tag "$IMAGE_TAG")"
	job="$validator-$(k8s_object_name "$RUN_ID")"
	final_uri="$RUNS_ROOT/$RUN_ID/$METADATA_FINAL_FILE"
	aws s3 cp "$METADATA_FINAL" "$final_uri" --only-show-errors >&2 ||
		die "could not upload $METADATA_FINAL to $final_uri for the in-cluster validator"
	read_site_prop_flags '.catalog.props' --catalog-prop
	local command=("$validator" --metadata "$final_uri" --corpus-uri "$CORPUS_URI" --out /work/scores
		--upload-prefix "$RUNS_ROOT/$RUN_ID/scores")
	if [[ $validator == verify-upsert ]]; then
		command+=(--checkpoints "$RUNS_ROOT/$RUN_ID/scores/checkpoints" --live "$UPSERT_LIVE" --reader none)
	else
		command+=(--publish-logs "$RUNS_ROOT/$RUN_ID/producer" --publish-shards "$PUBLISH_SHARDS"
			--read-workers "$VERIFY_READ_WORKERS" --exactness "$EXACTNESS")
	fi
	command+=(${SITE_PROP_FLAGS[@]+"${SITE_PROP_FLAGS[@]}"})
	log "verifying the final live rows for $RUN_ID as job/$job ($VERIFY_CPU CPU, $VERIFY_MEMORY, $VERIFY_READ_WORKERS readers)"
	# A Job that fails before uploading must leave no verdict behind, or the fetch below would return an
	# earlier finish's. Stop any earlier verifier first, so it cannot upload after the clear.
	k8s_delete_job_and_wait "$job"
	aws s3 rm "$RUNS_ROOT/$RUN_ID/scores/$artifact" --only-show-errors >&2 ||
		die "could not clear the previous $artifact under $RUNS_ROOT/$RUN_ID/scores/"
	k8s_render_apply deploy/k8s/verify-job.yaml.tmpl \
		"NAME=$job" \
		"NAMESPACE=$SITE_NAMESPACE" \
		"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
		"IMAGE=$image" \
		"COMMAND=$(job_command_json "${command[@]}")" \
		"ENV=$JOB_ENV" \
		"ENV_FROM=$JOB_ENV_FROM" \
		"NODE_SELECTOR=$NODE_SELECTOR" \
		"TOLERATIONS=$TOLERATIONS" \
		"CPU=$VERIFY_CPU" \
		"MEMORY=$VERIFY_MEMORY"
	(k8s_wait_job "$job" "$VERIFY_WAIT_S") || status=$?
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$job" --timestamps >"$SCORES/$validator.log" 2>/dev/null ||
		log "could not save job/$job's log"
	# The verdict document is the evidence; a Job that wrote none failed closed.
	aws s3 cp "$RUNS_ROOT/$RUN_ID/scores/$artifact" "$SCORES/$artifact" --only-show-errors >&2 ||
		{ log "job/$job left no $artifact under $RUNS_ROOT/$RUN_ID/scores/"; status=1; }
	if ((status == 0)); then
		k8s_delete job "$job"
	else
		log "job/$job is kept for inspection; it expires after an hour"
	fi
	return "$status"
}
# Validate the Kubernetes context before a background catalog tunnel may need it.
KUBE_CONTEXT="$(site_required '.kubernetes.context')"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SCORES="$RUN_DIR/scores"
SUMMARY="$SCORES/summary.json"
GEOMETRY="$SCORES/geometry.json"
VALIDATION_STATUS=0
COMPACTION_DRIVER="$(yq -r '.compaction.driver // ""' "$RUN_DIR/spec.yaml")"
EXACTNESS="$(yq -r '.scoring.exactness // "EXACTLY_ONCE"' "$RUN_DIR/spec.yaml")"
# Every verdict and diagnostic is rebuilt below; a stale one from an earlier finish must not stand in for it.
rm -f "$SCORES/terminal-exactness.json" "$SCORES/terminal-upsert.json" "$SCORES/table-health.json"

# ---------------------------------------------------------------------------
# 1. The artifacts the pods wrote
# ---------------------------------------------------------------------------

mkdir -p "$SCORES"
# Stage captures engine pod requests for managed engines only; an external engine's
# fleet is costed from its spec, so there is nothing to fetch and no error to print.
if [[ ! -f $RUN_DIR/engine-pods.json ]] && ! grep -q '^engine: external' "$RUN_DIR/spec.yaml" 2>/dev/null; then
	aws s3 cp "$RUNS_ROOT/$RUN_ID/stage/engine-pods.json" "$RUN_DIR/engine-pods.json" --only-show-errors >&2 ||
		log "no captured engine pod requests; managed engine cost will be unavailable"
fi
if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE && ! -f $RUN_DIR/$COMPACTOR_PODS_FILE ]]; then
	aws s3 cp "$RUNS_ROOT/$RUN_ID/stage/$COMPACTOR_PODS_FILE" "$RUN_DIR/$COMPACTOR_PODS_FILE" --only-show-errors >&2 ||
		log "no captured compactor pod requests; total resource cost will be unavailable"
fi
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
# Upsert stamp checkpoints are gigabytes; only a host-side validation fetches them.
SCORE_SYNC_FLAGS=(--exclude "checkpoints/*" --exclude "live/*")
[[ ! -f $SCORES/$COMPACTOR_STATUS_FILE ]] || SCORE_SYNC_FLAGS+=(--exclude "$COMPACTOR_STATUS_FILE")
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" --only-show-errors ${SCORE_SYNC_FLAGS[@]+"${SCORE_SYNC_FLAGS[@]}"} >&2 ||
	die "could not fetch $RUNS_ROOT/$RUN_ID/scores/; check that the run was launched and that you can read the bucket"
rm -f "$SCORES/terminal-exactness.json" "$SCORES/terminal-upsert.json" "$SCORES/table-health.json"

# Fetch per-shard publish logs to derive producer pacing and producer_bound. Missing logs
# are recorded in the result.
PRODUCER="$RUN_DIR/producer"
mkdir -p "$PRODUCER"
log "fetching $RUNS_ROOT/$RUN_ID/producer/ into $PRODUCER"
aws s3 sync "$RUNS_ROOT/$RUN_ID/producer/" "$PRODUCER/" --only-show-errors >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/producer/; the report will list publish logs as missing"

CORPUS_URI="$(jq -r '.corpus_uri // empty' "$RUN_DIR/facts.json")" ||
	die "could not read corpus_uri out of $RUN_DIR/facts.json"
if [[ -n $CORPUS_URI ]]; then
	aws s3 cp "$CORPUS_URI/corpus.json" "$RUN_DIR/corpus.json" --only-show-errors >&2 || true
fi
if [[ ! -f $RUN_DIR/corpus.json ]]; then
	log "could not fetch corpus.json; the local report will list it as missing"
fi

# ---------------------------------------------------------------------------
# 2. The geometry
# ---------------------------------------------------------------------------

# Use saved metadata so geometry does not require a live catalog entry. Pass catalog
# properties for the object-store settings needed to read manifests.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
if [[ -f $METADATA_FINAL ]]; then
	# Install cleanup before catalog properties can open a tunnel.
	trap k8s_port_forward_stop EXIT
	read_catalog_prop_flags
	# Omit an unspecified ladder to preserve file-sizes defaults.
	OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$RUN_DIR/spec.yaml")" ||
		die "could not read scoring.geometry_offsets_s out of $RUN_DIR/spec.yaml"
	OFFSET_FLAGS=()
	[[ -z $OFFSETS ]] || OFFSET_FLAGS=(--offsets "$OFFSETS")
	# Geometry offsets are relative to the launch epoch.
	EPOCH="$(jq -r '.epoch // empty' "$RUN_DIR/facts.json")"
	[[ -n $EPOCH ]] || die "$RUN_DIR/facts.json has no epoch for geometry offsets; was this run launched?"

	log "measuring file geometry for $RUN_ID"
	GEOMETRY_STATUS=0
	harness_local --extra aws file-sizes \
		--metadata "$(abs_path "$METADATA_FINAL")" \
		--epoch "$EPOCH" \
		--out "$(abs_path "$SCORES")" \
		${OFFSET_FLAGS[@]+"${OFFSET_FLAGS[@]}"} \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} || GEOMETRY_STATUS=$?
	if ((GEOMETRY_STATUS == NO_GEOMETRY)); then
		log "no geometry: the table never committed"
	elif ((GEOMETRY_STATUS != 0)); then
		die "could not measure the geometry: file-sizes exited $GEOMETRY_STATUS; see the error above"
	fi

	# Quiet time ends when this metadata was observed, even if finish is rerun later.
	OBSERVED_FLAGS=()
	if [[ -f $RUN_DIR/$METADATA_OBSERVED_FILE ]]; then
		OBSERVED_FLAGS=(--observed-at "$(cat "$RUN_DIR/$METADATA_OBSERVED_FILE")")
	fi
	log "measuring compaction for $RUN_ID"
	COMPACTION_STATUS=0
	harness_local --extra aws compaction \
		--metadata "$(abs_path "$METADATA_FINAL")" \
		--epoch "$EPOCH" \
		--out "$(abs_path "$SCORES")" \
		${OBSERVED_FLAGS[@]+"${OBSERVED_FLAGS[@]}"} \
		${OFFSET_FLAGS[@]+"${OFFSET_FLAGS[@]}"} \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} || COMPACTION_STATUS=$?
	((COMPACTION_STATUS == 0)) ||
		die "could not measure compaction: compaction exited $COMPACTION_STATUS; see the error above"

	# Measured from metadata before verification, so a table too costly to scan still gets figures.
	# Diagnostic only: the result records it as missing rather than the run failing.
	if [[ $EXACTNESS == LATEST_PER_KEY && -n $CORPUS_URI ]]; then
		log "measuring table health for $RUN_ID"
		harness_local --extra aws table-health \
			--metadata "$(abs_path "$METADATA_FINAL")" --corpus-uri "$CORPUS_URI" \
			--checkpoints "$RUNS_ROOT/$RUN_ID/scores/checkpoints" --out "$(abs_path "$SCORES")" \
			${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} ||
			log "could not measure table health: table-health exited $?; see the error above"
	fi
else
	log "$METADATA_FINAL is missing; run scripts/teardown.sh to save metadata before measuring geometry"
fi

# Dump the sampled snapshots' live rows with the Spark scan Job, into a fresh attempt directory
# that UPSERT_LIVE names for the verify Job. The Job reads the same uploaded metadata document.
scan_in_cluster() {
	local job image final_uri snapshots conf heap_mb status=0 key value key_column
	k8s_read_site
	[[ $VERIFY_SPARK_CPU =~ ^[1-9][0-9]*$ ]] || die "VERIFY_SPARK_CPU must be a positive integer; got '$VERIFY_SPARK_CPU'"
	[[ $VERIFY_SPARK_MEMORY =~ ^([1-9][0-9]*)Gi$ ]] || die "VERIFY_SPARK_MEMORY must be whole Gi, like 32Gi; got '$VERIFY_SPARK_MEMORY'"
	heap_mb=$((BASH_REMATCH[1] * 1024 * 3 / 4))
	image="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/spark:$(k8s_image_tag "$IMAGE_TAG")"
	job="spark-scan-$(k8s_object_name "$RUN_ID")"
	final_uri="$RUNS_ROOT/$RUN_ID/$METADATA_FINAL_FILE"
	aws s3 cp "$METADATA_FINAL" "$final_uri" --only-show-errors >&2 ||
		die "could not upload $METADATA_FINAL to $final_uri for the scan Job"
	snapshots="$(harness_local --extra aws upsert-scan-plan --metadata "$(abs_path "$METADATA_FINAL")" \
		--checkpoints "$RUNS_ROOT/$RUN_ID/scores/checkpoints" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" ||
		die "could not list the snapshots to scan"
	UPSERT_LIVE="$RUNS_ROOT/$RUN_ID/scores/live/$(date -u +%Y%m%dT%H%M%SZ)"
	conf="$(mktemp "${TMPDIR:-/tmp}/spark-scan.XXXXXX")"
	harness_local --extra aws render-spark-scan --site "$(abs_path "$SITE_FILE")" --cores "$VERIFY_SPARK_CPU" \
		--memory-mb "$heap_mb" --out "$conf" || die "could not render the scan's Spark settings"
	local command=(/opt/spark/bin/spark-submit --master "local[$VERIFY_SPARK_CPU]" --driver-memory "${heap_mb}m")
	while read -r key value; do
		[[ -z $key ]] || command+=(--conf "$key=$value")
	done <"$conf"
	rm -f "$conf"
	key_column="$(jq -r '.upsert.key_columns[0] // empty' "$RUN_DIR/facts.json")"
	[[ -n $key_column ]] || die "$RUN_DIR/facts.json names no upsert key column for the scan"
	command+=(/opt/bench/engines/spark/upsert_scan.py --metadata "$final_uri" --snapshots "$snapshots"
		--key-column "$key_column" --out "$UPSERT_LIVE")
	log "scanning live rows at $snapshots for $RUN_ID as job/$job ($VERIFY_SPARK_CPU CPU, $VERIFY_SPARK_MEMORY)"
	k8s_delete job "$job"
	k8s_render_apply deploy/k8s/spark-scan-job.yaml.tmpl \
		"NAME=$job" \
		"NAMESPACE=$SITE_NAMESPACE" \
		"SERVICE_ACCOUNT=$(site_value '.kubernetes.spark_service_account // "ingest-bench-spark"')" \
		"IMAGE=$image" \
		"COMMAND=$(job_command_json "${command[@]}")" \
		"ENV=$JOB_ENV" \
		"ENV_FROM=$JOB_ENV_FROM" \
		"NODE_SELECTOR=$NODE_SELECTOR" \
		"TOLERATIONS=$TOLERATIONS" \
		"CPU=$VERIFY_SPARK_CPU" \
		"MEMORY=$VERIFY_SPARK_MEMORY" \
		"DEADLINE_S=$((VERIFY_WAIT_S > 0 ? VERIFY_WAIT_S : 1))"
	(k8s_wait_job "$job" "$VERIFY_WAIT_S") || status=$?
	kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$job" --timestamps >"$SCORES/spark-scan.log" 2>/dev/null ||
		log "could not save job/$job's log"
	if ((status == 0)); then
		k8s_delete job "$job"
	else
		log "job/$job is kept for inspection; it expires after an hour"
	fi
	return "$status"
}

record_validation() {
	jq -n --arg mode "$([[ $VERIFY_IN_CLUSTER == yes ]] && echo cluster || echo host)" \
		--arg started "$VERIFY_STARTED_AT" --arg ended "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		--arg cpu "$VERIFY_CPU" --arg memory "$VERIFY_MEMORY" --argjson workers "$VERIFY_READ_WORKERS" \
		--argjson status "$VALIDATION_STATUS" \
		--arg scan_cpu "$VERIFY_SPARK_CPU" --arg scan_memory "$VERIFY_SPARK_MEMORY" \
		--argjson scanned "$([[ $EXACTNESS == LATEST_PER_KEY && $VERIFY_IN_CLUSTER == yes ]] && echo true || echo false)" \
		'{mode: $mode, started_at: $started, ended_at: $ended, exit_status: $status,
		  resources: (if $mode == "cluster" then {cpu: $cpu, memory: $memory} else null end),
		  scan: (if $scanned then {cpu: $scan_cpu, memory: $scan_memory} else null end),
		  read_workers: $workers}' >"$SCORES/terminal-validation.json"
}

if [[ $EXACTNESS == LATEST_PER_KEY ]]; then
	# Upsert runs are always verified: live state at the sample snapshots is part of their verdict.
	[[ -f $RUN_DIR/$WRITERS_STOPPED_FILE ]] ||
		die "no $WRITERS_STOPPED_FILE evidence; finish teardown (including external detach) before verifying live state"
	[[ -f $METADATA_FINAL && -n $CORPUS_URI ]] || die "upsert verification requires final metadata and corpus_uri"
	VERIFY_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	if [[ $VERIFY_IN_CLUSTER == yes ]]; then
		if scan_in_cluster; then
			verify_in_cluster verify-upsert terminal-upsert.json || VALIDATION_STATUS=$?
		else
			VALIDATION_STATUS=1
		fi
	else
		log "fetching the scorer's stamp checkpoints for $RUN_ID"
		aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/checkpoints/" "$SCORES/checkpoints/" --only-show-errors >&2 ||
			die "could not fetch $RUNS_ROOT/$RUN_ID/scores/checkpoints/"
		log "verifying live state at the sample snapshots for $RUN_ID"
		harness_local --extra aws verify-upsert \
			--metadata "$(abs_path "$METADATA_FINAL")" --corpus-uri "$CORPUS_URI" \
			--checkpoints "$(abs_path "$SCORES/checkpoints")" --live "$(abs_path "$SCORES")/live" --out "$(abs_path "$SCORES")" \
			${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} || VALIDATION_STATUS=$?
	fi
	record_validation
	if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]]; then
		jq -e '.healthy == true' "$SCORES/$COMPACTOR_STATUS_FILE" >/dev/null 2>&1 || VALIDATION_STATUS=2
	fi
	if ((VALIDATION_STATUS != 0)); then
		log "live-state validation failed; preserving the result locally"
		[[ $PUBLISH_INVALID == yes ]] || PUBLISH_DIR=""
	fi
elif [[ -n $COMPACTION_DRIVER || $VERIFY_IN_CLUSTER == yes ]]; then
	[[ -f $RUN_DIR/$WRITERS_STOPPED_FILE ]] ||
		die "no $WRITERS_STOPPED_FILE evidence; finish teardown (including external detach) before verifying final live rows"
	[[ -f $METADATA_FINAL && -n $CORPUS_URI ]] || die "terminal verification requires final metadata and corpus_uri"
	PUBLISH_SHARDS="$(yq -r '.producer.shards // 1' "$RUN_DIR/spec.yaml")"
	VERIFY_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	if [[ $VERIFY_IN_CLUSTER == yes ]]; then
		verify_in_cluster verify-table terminal-exactness.json || VALIDATION_STATUS=$?
	else
		log "verifying the final live rows for $RUN_ID"
		harness_local --extra aws verify-table \
			--metadata "$(abs_path "$METADATA_FINAL")" --corpus-uri "$CORPUS_URI" \
			--publish-logs "$(abs_path "$PRODUCER")" --out "$(abs_path "$SCORES")" \
			--publish-shards "$PUBLISH_SHARDS" --exactness "$EXACTNESS" \
			${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"} || VALIDATION_STATUS=$?
	fi
	# Validation time and resources are the harness's, not the measured run's; record them
	# beside the verdict so a report can show them separately.
	record_validation
	if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]]; then
		jq -e '.healthy == true' "$SCORES/$COMPACTOR_STATUS_FILE" >/dev/null 2>&1 || VALIDATION_STATUS=2
	fi
	if ((VALIDATION_STATUS != 0)); then
		log "terminal validation failed; preserving the result locally"
		[[ $PUBLISH_INVALID == yes ]] || PUBLISH_DIR=""
	fi
fi

# ---------------------------------------------------------------------------
# 3. The run's document
# ---------------------------------------------------------------------------

VARIANT_FLAGS=()
[[ -z $VARIANT ]] || VARIANT_FLAGS=(--variant "$VARIANT")
RUN_DIR_ABS="$(abs_path "$RUN_DIR")"
SITE_ABS="$(abs_path "$SITE_FILE")"
PUBLISH_FLAGS=()
if [[ -n $PUBLISH_DIR ]]; then
	# collect creates nested results directories; abs_path requires an existing parent.
	PUBLISH_ABS=$PUBLISH_DIR
	[[ $PUBLISH_ABS == /* ]] || PUBLISH_ABS="$PWD/$PUBLISH_ABS"
	# Installed commands need an explicit path to the checkout's preset files.
	WORKLOADS_ABS="$(abs_path "${INGEST_BENCH_WORKLOADS:-$REPO_ROOT/workloads}")"
	PUBLISH_FLAGS=(--publish "$PUBLISH_ABS" --workloads "$WORKLOADS_ABS")
	[[ $PUBLISH_INVALID == no ]] || PUBLISH_FLAGS+=(--publish-invalid)
fi

log "collecting $RUN_ID"
harness_local --extra aws collect --run-dir "$RUN_DIR_ABS" --site "$SITE_ABS" \
	${VARIANT_FLAGS[@]+"${VARIANT_FLAGS[@]}"} ${PUBLISH_FLAGS[@]+"${PUBLISH_FLAGS[@]}"}

# ---------------------------------------------------------------------------
# 4. The results table
# ---------------------------------------------------------------------------

# Refresh the table before print_verdict can exit for a published invalid run.
if [[ -n $PUBLISH_DIR ]]; then
	# Keep the same aws extra to avoid uv resyncing between dependency sets.
	harness_local --extra aws results-table "$PUBLISH_ABS" --out "$PUBLISH_ABS/RESULTS.md"
fi

# ---------------------------------------------------------------------------
# 5. The verdict
# ---------------------------------------------------------------------------

((VALIDATION_STATUS == 0)) || die "terminal validation failed; see $SCORES/terminal-exactness.json or terminal-upsert.json, and $SCORES/$COMPACTOR_STATUS_FILE"
print_verdict "$SUMMARY" "$GEOMETRY"
log "run_valid: true — $RUN_ID, artifacts in $RUN_DIR"
