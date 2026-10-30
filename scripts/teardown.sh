#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Stop the managed engine, producer and scorer, then drop the Kafka topic from a cluster
# Job. Keep the table, warehouse data and result artifacts. Use purge.sh to delete
# measured data after reviewing the run.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Allow scheduling, image pull and broker confirmation for topic deletion.
DROP_WAIT_S="${DROP_WAIT_S:-600}"
EXTERNAL_DETACH_WAIT_S="${EXTERNAL_DETACH_WAIT_S:-300}"
# An optional post-offer period in which the compaction sidecar keeps rewriting after the
# engine has stopped. Timed on its own, so it can be costed apart from the offer and can
# never be read as the sidecar keeping up during it. 0 is the comparison's shutdown policy.
COMPACTION_CATCHUP_S="${COMPACTION_CATCHUP_S:-0}"

usage() {
	cat <<'USAGE'
usage: scripts/teardown.sh <run_id> [options]

  <run_id>           a run whose directory is under $RUNS_DIR
  --site PATH        site config for the cluster and the broker (default: ./site.yaml)
  --image-tag TAG    the harness image tag the drop-topic Job runs (default: this checkout's commit)
  --external-detached-file PATH
                     for an external engine, wait for PATH before dropping the topic
  --external-detach-wait-s N
                     maximum external detach wait (default: 300; then drops the topic)
  --keep-table       accepted for compatibility; teardown always preserves table data.
                     Use `purge.sh <run_id>` to delete it

Environment: DROP_WAIT_S, EXTERNAL_DETACH_WAIT_S, COMPACTION_CATCHUP_S, K8S_JOB_DELETE_WAIT_S,
RUNS_DIR.
USAGE
}

RUN_ID=""
IMAGE_TAG=""
EXTERNAL_DETACHED_FILE=""
EXTERNAL_DETACH_OPTION=""
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
	--external-detached-file)
		EXTERNAL_DETACHED_FILE="${2:?--external-detached-file needs a path}"
		shift 2
		;;
	--external-detach-wait-s)
		EXTERNAL_DETACH_WAIT_S="${2:?--external-detach-wait-s needs a number of seconds}"
		EXTERNAL_DETACH_OPTION=yes
		shift 2
		;;
	--keep-table)
		# Accepted for clarity; this script always preserves the table.
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
[[ $EXTERNAL_DETACH_WAIT_S =~ ^[0-9]+$ ]] ||
	die "--external-detach-wait-s requires a non-negative integer; got '$EXTERNAL_DETACH_WAIT_S'"
[[ $COMPACTION_CATCHUP_S =~ ^[0-9]+$ ]] ||
	die "COMPACTION_CATCHUP_S requires a non-negative integer; got '$COMPACTION_CATCHUP_S'"

require_host_tools kubectl aws yq jq git gzip

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; use the checkout where stage.sh saved it, or set RUNS_DIR"

k8s_read_site
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

BOOTSTRAP="$(jq -r .bootstrap "$FACTS")"
TABLE="$(jq -r .table "$FACTS")"
# Use the staged topic name instead of deriving it from the run ID.
TOPIC="$(jq -r .topic "$FACTS")"
[[ -n $TOPIC && $TOPIC != null ]] || die "$FACTS has no topic; cannot determine which topic to delete for $RUN_ID"

# Read the staged spec to identify the engine to stop.
SPEC="$RUN_DIR/spec.yaml"
[[ -f $SPEC ]] || die "no spec at $SPEC; cannot determine which engine to stop for $RUN_ID"
ENGINE="$(yq '.engine' "$SPEC")"
[[ -n $ENGINE && $ENGINE != null ]] || die "$SPEC has no engine; cannot determine what to stop"
if [[ $ENGINE != external && (-n $EXTERNAL_DETACHED_FILE || -n $EXTERNAL_DETACH_OPTION) ]]; then
	die "external detach options require engine: external; $SPEC specifies '$ENGINE'"
fi

SCORES="$RUN_DIR/scores"
mkdir -p "$SCORES"
rm -f "$RUN_DIR/$TEARDOWN_COMPLETE_FILE" "$RUN_DIR/$WRITERS_STOPPED_FILE"
# A verdict from an earlier finish must not outlive this teardown's newer metadata.
rm -f "$SCORES/terminal-exactness.json" "$SCORES/terminal-upsert.json"
WRITER_STATUS=0
EXTERNAL_STOPPED=no

# Preserve the previous copy when an idempotent teardown finds that the Job is already
# gone. A partial kubectl response is not a complete log.
save_job_log() {
	local job=$1 path=$2 temporary="$2.tmp.$$"
	log "saving job/$job log to $path"
	if kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" logs "job/$job" --timestamps >"$temporary"; then
		mv "$temporary" "$path"
	else
		rm -f "$temporary"
		log "could not save job/$job log; it may already be gone"
	fi
}

# ---------------------------------------------------------------------------
# 1. Stop the offer
# ---------------------------------------------------------------------------

PRODUCER_JOB="$(producer_job "$RUN_ID")"
log "deleting job/$PRODUCER_JOB in the foreground"
PRODUCER_STATUS=0
(k8s_delete_job_and_wait "$PRODUCER_JOB") || PRODUCER_STATUS=$?
# An upsert run that seeds through Kafka ran a seed producer and its barrier first. A launch that
# stopped mid-seed can leave the seed producer publishing, so wait for it as for the offer.
(k8s_delete_job_and_wait "$(seed_job "$RUN_ID")") || PRODUCER_STATUS=$?
k8s_delete job "$(seed_wait_job "$RUN_ID")"

# ---------------------------------------------------------------------------
# 2. The engine
# ---------------------------------------------------------------------------

if [[ $ENGINE == external ]]; then
	log "external engine remains attached; stop it manually"
else
	k8s_read_engine "$ENGINE" "$(k8s_object_name "$RUN_ID")"

	# Capture missing provenance before deleting the engine. Do not wait for a digest here;
	# staging normally records it once the fleet is running.
	if [[ ! -f $RUN_DIR/$ENGINE_IMAGE_FILE ]]; then
		k8s_write_engine_image "$RUN_DIR/$ENGINE_IMAGE_FILE" "$ENGINE_PROVENANCE_SELECTOR" 0
	fi

	# Delete engine resources before the ConfigMap their pods mount.
	if [[ -f $RUN_DIR/$ENGINE_DOCUMENT_FILE ]]; then
		log "stopping writers declared in $ENGINE_DOCUMENT_FILE"
		k8s_delete_writer_file "$RUN_DIR/$ENGINE_DOCUMENT_FILE" "$ENGINE_FLEET_SELECTOR" || WRITER_STATUS=1
	fi
	if [[ -f $RUN_DIR/$ENGINE_CONFIGMAP_FILE ]]; then
		k8s_delete_file "$RUN_DIR/$ENGINE_CONFIGMAP_FILE" || WRITER_STATUS=1
	fi
fi

# ---------------------------------------------------------------------------
# 3. The external engine hand-off
# ---------------------------------------------------------------------------

if ((PRODUCER_STATUS == 0)) && [[ $ENGINE == external ]]; then
	if [[ -n $EXTERNAL_DETACHED_FILE ]]; then
		log "stop the external engine, then create $EXTERNAL_DETACHED_FILE; waiting up to ${EXTERNAL_DETACH_WAIT_S}s"
		waited=0
		while [[ ! -e $EXTERNAL_DETACHED_FILE && $waited -lt $EXTERNAL_DETACH_WAIT_S ]]; do
			remaining=$((EXTERNAL_DETACH_WAIT_S - waited))
			pause=$((remaining < 2 ? remaining : 2))
			sleep "$pause"
			waited=$((waited + pause))
		done
		if [[ -e $EXTERNAL_DETACHED_FILE ]]; then
			log "external engine detached: $EXTERNAL_DETACHED_FILE"
			EXTERNAL_STOPPED=yes
		else
			log "detach signal did not appear within ${EXTERNAL_DETACH_WAIT_S}s; dropping the topic, so reader restarts are expected"
		fi
	else
		log "stop the external engine; waiting ${EXTERNAL_DETACH_WAIT_S}s before dropping the topic"
		sleep "$EXTERNAL_DETACH_WAIT_S"
		log "external detach wait elapsed; dropping the topic, so reader restarts are expected if it is still attached"
	fi
fi

# ---------------------------------------------------------------------------
# 4. The compaction sidecar
# ---------------------------------------------------------------------------

if [[ $ENGINE == external && $EXTERNAL_STOPPED != yes && -n $(yq -r '.compaction.driver // ""' "$SPEC") ]]; then
	log "compaction requires an external detach acknowledgement before final metadata can be captured"
	WRITER_STATUS=1
fi

rm -f "$RUN_DIR/compaction-catchup.json"
if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]] && ((COMPACTION_CATCHUP_S > 0)); then
	CATCHUP_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	log "letting the compaction sidecar catch up for ${COMPACTION_CATCHUP_S}s before stopping it"
	sleep "$COMPACTION_CATCHUP_S"
	jq -n --argjson n "$COMPACTION_CATCHUP_S" --arg s "$CATCHUP_STARTED_AT" --arg e "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		'{requested_s: $n, started_at: $s, ended_at: $e}' >"$RUN_DIR/compaction-catchup.json"
fi

# Preserve evidence before stopping the sidecar. A killed pass is not a completed
# rewrite; Iceberg's committed snapshot remains the source of table contents.
if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE && ! -f $RUN_DIR/$COMPACTOR_PODS_FILE ]]; then
	k8s_capture_compactor_fleet "$RUN_DIR" ||
		log "could not capture compactor pod requests; total resource cost will be unavailable"
fi
if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]]; then
	if COMPACTOR_SELECTOR="$(k8s_compactor_selector "$RUN_DIR")"; then
		k8s_capture_compactor_diagnostics "$RUN_DIR" "$COMPACTOR_SELECTOR" ||
			log "could not capture compactor diagnostics"
		log "stopping the compaction sidecar"
		k8s_delete_writer_file "$RUN_DIR/$COMPACTOR_DOCUMENT_FILE" "$COMPACTOR_SELECTOR" || WRITER_STATUS=1
	else
		WRITER_STATUS=1
		log "could not resolve the compactor pod selector; continuing cleanup without stopping the sidecar"
	fi
fi
if [[ -f $RUN_DIR/$COMPACTOR_CONFIGMAP_FILE ]]; then
	k8s_delete_file "$RUN_DIR/$COMPACTOR_CONFIGMAP_FILE" || WRITER_STATUS=1
fi

# ---------------------------------------------------------------------------
# 5. The scorer
# ---------------------------------------------------------------------------

SCORER_JOB="$(scorer_job "$RUN_ID")"
save_job_log "$SCORER_JOB" "$SCORES/scorer.log"
log "deleting job/$SCORER_JOB in the foreground"
SCORER_STATUS=0
(k8s_delete_job_and_wait "$SCORER_JOB") || SCORER_STATUS=$?

# ---------------------------------------------------------------------------
# 6. The topic
# ---------------------------------------------------------------------------

DROP_JOB="drop-topic-$(k8s_object_name "$RUN_ID")"
if ((PRODUCER_STATUS == 0)); then
	read_site_prop_flags '.kafka.security' --kafka-prop
	DROP_COMMAND=(drop-topic --bootstrap "$BOOTSTRAP" --topic "$TOPIC" ${SITE_PROP_FLAGS[@]+"${SITE_PROP_FLAGS[@]}"})
	log "dropping topic $TOPIC as job/$DROP_JOB"
	k8s_delete job "$DROP_JOB"
	k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
		"NAME=$DROP_JOB" \
		"NAMESPACE=$SITE_NAMESPACE" \
		"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
		"IMAGE=$IMAGE" \
		"COMMAND=$(job_command_json "${DROP_COMMAND[@]}")" \
		"ENV=$JOB_ENV" \
		"ENV_FROM=$JOB_ENV_FROM" \
		"NODE_SELECTOR=$NODE_SELECTOR" \
		"TOLERATIONS=$TOLERATIONS"
	DROP_STATUS=0
	(k8s_wait_job "$DROP_JOB" "$DROP_WAIT_S") || DROP_STATUS=$?
	save_job_log "$DROP_JOB" "$SCORES/drop-topic.log"
	((DROP_STATUS == 0)) || exit "$DROP_STATUS"
	k8s_delete job "$DROP_JOB"
else
	log "keeping topic $TOPIC because job/$PRODUCER_JOB may still be publishing"
fi

# ---------------------------------------------------------------------------
# 7. The scorer's artifacts
# ---------------------------------------------------------------------------

# Fetch scores for collection below. finish.sh fetches again to include any final uploads.
log "fetching $RUNS_ROOT/$RUN_ID/scores/ into $SCORES"
# Upsert stamp checkpoints are gigabytes; only a host-side validation fetches them.
SCORE_SYNC_FLAGS=(--exclude "checkpoints/*" --exclude "live/*")
[[ ! -f $SCORES/$COMPACTOR_STATUS_FILE ]] || SCORE_SYNC_FLAGS+=(--exclude "$COMPACTOR_STATUS_FILE")
aws s3 sync "$RUNS_ROOT/$RUN_ID/scores/" "$SCORES/" --only-show-errors ${SCORE_SYNC_FLAGS[@]+"${SCORE_SYNC_FLAGS[@]}"} >&2 ||
	log "could not fetch $RUNS_ROOT/$RUN_ID/scores/; the report below will list missing artifacts"
rm -f "$SCORES/terminal-exactness.json" "$SCORES/terminal-upsert.json"

# ---------------------------------------------------------------------------
# 8. The table's last metadata document
# ---------------------------------------------------------------------------

# Save the final metadata document for later geometry and location reads, even if the
# catalog entry is removed. It references manifests; it does not copy them.
# Install tunnel cleanup before reading catalog properties.
trap k8s_port_forward_stop EXIT

# An absent table needs no metadata copy. Report other errors because they do not
# establish absence.
METADATA_STATUS=0
if ((WRITER_STATUS != 0)); then
	log "writers may still commit; refusing to replace final table metadata"
	METADATA_STATUS=1
else
	read_catalog_prop_flags
	METADATA="$(harness_local --extra aws table-metadata --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" ||
		METADATA_STATUS=$?
fi
if ((METADATA_STATUS == 0)); then
	METADATA_OBSERVED_AT="$(jq -n now)"
	LOCAL_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
	FINAL="$RUNS_ROOT/$RUN_ID/$METADATA_FINAL_FILE"
	# Save locally for finish and purge, then upload the same document for durable storage.
	log "copying $TABLE's metadata document to $LOCAL_FINAL and $FINAL"
	# A failed replacement must not leave an old observation time beside new metadata.
	rm -f "$RUN_DIR/$METADATA_OBSERVED_FILE"
	if k8s_fetch_metadata_document "$METADATA" "$LOCAL_FINAL"; then
		printf '%s\n' "$METADATA_OBSERVED_AT" >"$RUN_DIR/$METADATA_OBSERVED_FILE"
		# The marker certifies this saved snapshot, never an older copy left by
		# a failed read during a repeated teardown.
		if ((WRITER_STATUS == 0)) && [[ $ENGINE != external || $EXTERNAL_STOPPED == yes ]]; then
			touch "$RUN_DIR/$WRITERS_STOPPED_FILE"
		fi
		if aws s3 cp "$LOCAL_FINAL" "$FINAL" --only-show-errors >&2; then
			aws s3 cp "$RUN_DIR/$METADATA_OBSERVED_FILE" "$RUNS_ROOT/$RUN_ID/$METADATA_OBSERVED_FILE" --only-show-errors >&2 ||
				log "could not upload the metadata observation time; the local copy is available"
		else
			log "could not copy $LOCAL_FINAL to $FINAL; the local copy is available"
		fi
	else
		log "could not copy $METADATA to $LOCAL_FINAL; the source metadata is unchanged"
	fi
elif ((METADATA_STATUS == TABLE_ABSENT)); then
	log "no table $TABLE in the catalog, so there is no metadata document to copy"
elif ((WRITER_STATUS == 0)); then
	die "could not read $TABLE's metadata document: table-metadata exited $METADATA_STATUS; see the error above"
fi

# ---------------------------------------------------------------------------
# 9. The run's document
# ---------------------------------------------------------------------------

# Collect a result even if finish is never called. Geometry and producer logs may still be
# missing. Report collection failures without treating completed resource teardown as a
# failure; collect can be retried separately.
log "collect will report scores/geometry.json and publish logs as missing until finish.sh fetches the logs and measures geometry"
log "collecting $RUN_ID"
harness_local --extra aws collect --run-dir "$(abs_path "$RUN_DIR")" --site "$(abs_path "$SITE_FILE")" ||
	log "could not collect $RUN_ID; rerun: collect --run-dir $RUN_DIR --site $SITE_FILE"

if ((WRITER_STATUS != 0)); then
	log "partially torn down $RUN_ID: engine or compactor pods may still be writing"
	exit "$WRITER_STATUS"
fi
if ((PRODUCER_STATUS != 0)); then
	log "partially torn down $RUN_ID: job/$PRODUCER_JOB may remain; topic $TOPIC was retained; see cleanup errors above"
	exit "$PRODUCER_STATUS"
fi
if ((SCORER_STATUS != 0)); then
	log "partially torn down $RUN_ID: job/$SCORER_JOB may remain; topic $TOPIC was dropped; see cleanup errors above"
	exit "$SCORER_STATUS"
fi
touch "$RUN_DIR/$TEARDOWN_COMPLETE_FILE"
log "torn down $RUN_ID; its table and the warehouse data are untouched"
