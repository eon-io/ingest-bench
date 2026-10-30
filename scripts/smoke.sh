#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Run the full benchmark locally: generate, stage, start the engine, produce and score.
# Exit 0 only when run_valid is true.
# This checks integration, not engine performance: the stack shares one machine.
set -euo pipefail
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"

# Set a shared future epoch so producer and scorer can start before the first batch.
EPOCH_LEAD_S="${EPOCH_LEAD_S:-30}"
# Allow a cold first commit, then stop if outstanding rows make no progress.
IDLE_STOP_S="${IDLE_STOP_S:-120}"
# Timeout for --external-ready-file.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-900}"
# How long the engine may take to commit a Kafka seed before the scored offer.
SEED_WAIT_S="${SEED_WAIT_S:-900}"
# Must match ingest_bench.scorer.cli.NO_GEOMETRY. A table without commits has no geometry;
# other nonzero statuses are read failures.
NO_GEOMETRY=4

usage() {
	cat <<'USAGE'
usage: scripts/smoke.sh [options]

  --engine NAME|external      which run spec to stage. NAME is a directory under
                              engines/ whose compose.sh starts the engine here;
                              `external` waits for you to start yours
                              (default: flink)
  --spec PATH                 a run spec to stage instead of
                              specs/smoke/smoke-<engine>.yaml
  --set KEY=VALUE             override a corpus preset key, repeatable
                              (e.g. --set duration_s=30 for a 30 s corpus)
  --keep                      leave the stack up afterwards
  --external-ready-file PATH  with --engine external, wait for PATH to appear
                              instead of reading a newline from stdin

Environment: EPOCH_LEAD_S, IDLE_STOP_S, EXTERNAL_READY_WAIT_S, SEED_WAIT_S, plus readiness
timeouts and endpoints defined in engines/<engine>/compose.sh.
USAGE
}

# Load engine operations from engines/<name>/compose.sh.
ENGINE=flink
KEEP=0
READY_FILE=""
SPEC_FILE=""
# These options enter the image's shell as part of one command string.
SETS=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--engine)
		ENGINE="${2:?--engine needs an engines/ directory name, or external}"
		shift 2
		;;
	--spec)
		SPEC_FILE="${2:?--spec needs a path}"
		shift 2
		;;
	--set)
		SETS="$SETS --set ${2:?--set needs KEY=VALUE}"
		shift 2
		;;
	--keep)
		KEEP=1
		shift
		;;
	--external-ready-file)
		READY_FILE="${2:?--external-ready-file needs a path}"
		shift 2
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

require_host_tools docker jq yq curl

[[ -n $SPEC_FILE ]] || SPEC_FILE="$REPO_ROOT/specs/smoke/smoke-$ENGINE.yaml"
[[ -f $SPEC_FILE ]] ||
	die "no run spec at $SPEC_FILE; --engine takes external or a directory under $REPO_ROOT/engines, or name a spec with --spec"
# Staging mounts the spec file into the harness container, so it may live anywhere.
SPEC_FILE="$(cd -- "$(dirname -- "$SPEC_FILE")" && pwd)/$(basename -- "$SPEC_FILE")"
[[ $ENGINE == external || -z $READY_FILE ]] || die "--external-ready-file only applies to --engine external"

# The harness takes a shell command string, so escape the preset name for that shell.
# A spec that names a track takes its corpus from the track.
CORPUS_SOURCE="$SPEC_FILE"
TRACK="$(yq -r '.track // ""' "$SPEC_FILE")"
if [[ -n $TRACK && "$(yq -r '.corpus // ""' "$SPEC_FILE")" == "" ]]; then
	CORPUS_SOURCE="$REPO_ROOT/workloads/tracks/$TRACK.yaml"
	[[ -f $CORPUS_SOURCE ]] || die "spec track '$TRACK' is not a shipped track under workloads/tracks"
fi
CORPUS_PRESET="$(yq -er '.corpus | select(tag == "!!str" and . != "") | @sh' "$CORPUS_SOURCE")" ||
	die "could not read corpus preset from $CORPUS_SOURCE"
# An upsert preset needs a seed table before staging, and its runs are verified on live state.
UPSERT_PRESET="$REPO_ROOT/workloads/presets/$(yq -r '.corpus' "$CORPUS_SOURCE").yaml"
UPSERT=no
if [[ -f $UPSERT_PRESET && "$(yq -r '.upsert // ""' "$UPSERT_PRESET")" != "" ]]; then
	UPSERT=yes
fi

# Managed engines supply their own Compose hooks; external engines are started by the
# operator.
if [[ $ENGINE != external ]]; then
	ENGINE_COMPOSE="$REPO_ROOT/engines/$ENGINE/compose.sh"
	[[ -f $ENGINE_COMPOSE ]] ||
		die "no $ENGINE_COMPOSE, so this stack does not know how to start '$ENGINE'; --engine takes external or a directory under $REPO_ROOT/engines"
	# shellcheck source=/dev/null
	source "$ENGINE_COMPOSE"
	for hook in engine_compose_build engine_compose_start engine_compose_ready engine_compose_logs; do
		declare -F "$hook" >/dev/null ||
			die "$ENGINE_COMPOSE declares no $hook, and this script calls all four; see docs/adding-an-engine.md"
	done
fi

# The compaction layer supplies its own Compose hooks, as engines do, so this
# script names none of its services. The registry maps the spec's driver name
# to a Python package, and the directory follows that spelling.
COMPACTOR="$(yq -r '.compaction.driver // ""' "$SPEC_FILE")" ||
	die "could not read compaction.driver from $SPEC_FILE"
COMPACTION_DRIVER="$COMPACTOR"
case "$COMPACTOR" in
"" | inline | external)
	# Nothing here starts these; a run may legitimately have no sidecar.
	COMPACTOR=""
	;;
*)
	COMPACTOR_COMPOSE="$REPO_ROOT/compactors/${COMPACTOR//-/_}/compose.sh"
	[[ -f $COMPACTOR_COMPOSE ]] ||
		die "no $COMPACTOR_COMPOSE, so this stack does not know how to start compaction driver '$COMPACTOR'"
	# shellcheck source=/dev/null
	source "$COMPACTOR_COMPOSE"
	for hook in compactor_compose_build compactor_compose_start compactor_compose_logs; do
		declare -F "$hook" >/dev/null ||
			die "$COMPACTOR_COMPOSE declares no $hook, and this script calls all three; see docs/run-spec.md"
	done
	;;
esac

STAGE_OUT=""
RUN_ID=""
cleanup() {
	local status=$?
	if ((status != 0)); then
		# Print engine and scorer logs before teardown removes their containers.
		if [[ -n $RUN_ID ]] && docker container inspect "scorer-$RUN_ID" >/dev/null 2>&1; then
			log "--- last 30 lines of scorer-$RUN_ID ---"
			docker logs --tail 30 "scorer-$RUN_ID" >&2 || true
		fi
		# Delegate log selection to the engine's Compose hooks.
		if declare -F engine_compose_logs >/dev/null; then
			engine_compose_logs
		fi
		# The sidecar keeps its own log; from the table alone, a run it never
		# compacted looks the same as one it compacted badly.
		if declare -F compactor_compose_logs >/dev/null; then
			compactor_compose_logs
		fi
	fi
	[[ -z $STAGE_OUT ]] || rm -f "$STAGE_OUT"
	if ((KEEP == 1)); then
		log "--keep: the stack is still up. Tear it down with:"
		log "  RUN_DIR=/tmp docker compose -f $COMPOSE_FILE --profile '*' down -v"
	else
		log "tearing the stack down"
		compose down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# The site
# ---------------------------------------------------------------------------

log "building the harness image"
# Record the checkout's commit in the image, as push-images.sh does for cluster images.
HARNESS_COMMIT="$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null)" || HARNESS_COMMIT=""
if [[ -n $HARNESS_COMMIT && -n "$(git -C "$REPO_ROOT" status --porcelain)" ]]; then
	HARNESS_COMMIT="$HARNESS_COMMIT-dirty"
fi
export HARNESS_COMMIT
compose build harness
if [[ $ENGINE != external ]]; then
	engine_compose_build
fi
if [[ -n $COMPACTOR ]]; then
	compactor_compose_build
fi
if [[ $UPSERT == yes ]]; then
	# Every engine's upsert table is verified with the Spark reader.
	compose build upsert-scan
fi

log "starting the broker, the object store and the catalog"
compose up -d kafka minio minio-init iceberg-rest

log "generating the corpus"
GEN_OUT="$(harness "gen-corpus --preset $CORPUS_PRESET$SETS --out s3://corpus --seed 1")"
printf '%s\n' "$GEN_OUT"
if [[ $UPSERT == yes ]]; then
	SEED_CORPUS="$(sed -n 's/^wrote \(s3:[^:]*\): .*/\1/p' <<<"$GEN_OUT" | head -n 1)"
	[[ -n $SEED_CORPUS ]] || die "gen-corpus printed no corpus location to build the seed table for"
	log "writing the seed table for $SEED_CORPUS"
	harness "seed-corpus --corpus $SEED_CORPUS"
	harness "commit-seed --corpus $SEED_CORPUS"
fi

# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

# Use an explicit XXXXXX template for both BSD and GNU mktemp.
STAGE_OUT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-stage.XXXXXX")"
log "staging $(basename "$SPEC_FILE")"
compose run --rm -T -v "$SPEC_FILE:/spec.yaml:ro" harness "stage --spec /spec.yaml --site /site.yaml --runs-dir /runs" |
	tee "$STAGE_OUT"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' "$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "stage printed no run_id line; see the output above"
# Compose reads the submitter's mount path from the environment.
export RUN_DIR="$REPO_ROOT/runs/$RUN_ID"
[[ -d $RUN_DIR ]] || die "stage reported run_id $RUN_ID but wrote no $RUN_DIR"
log "run $RUN_ID staged in $RUN_DIR"
# Read run settings from the staged spec, which carries a named track's settings.
STAGED_SPEC="$RUN_DIR/spec.yaml"

# Compaction begins before ingest, as it does on a cluster. There is no
# readiness check: what the sidecar did is read from the table afterwards.
if [[ -n $COMPACTOR ]]; then
	compactor_compose_start
fi

if [[ $ENGINE != external ]]; then
	# Start the engine, then verify its effective settings against the staged spec.
	engine_compose_start
	engine_compose_ready
else
	log "start your engine now against these facts:"
	cat "$RUN_DIR/facts.json"
	if [[ -n $READY_FILE ]]; then
		log "waiting up to ${EXTERNAL_READY_WAIT_S}s for $READY_FILE to appear"
		waited=0
		while [[ ! -e $READY_FILE ]]; do
			((waited < EXTERNAL_READY_WAIT_S)) || die "$READY_FILE did not appear within ${EXTERNAL_READY_WAIT_S}s"
			sleep 2
			waited=$((waited + 2))
		done
	else
		log "press enter once it is consuming $RUN_ID"
		read -r
	fi
fi

# ---------------------------------------------------------------------------
# The offer, and the score of it
# ---------------------------------------------------------------------------

BOOTSTRAP="$(jq -r .bootstrap "$RUN_DIR/facts.json")"
CORPUS_URI="$(jq -r .corpus_uri "$RUN_DIR/facts.json")"
TABLE="$(jq -r .table "$RUN_DIR/facts.json")"
# Omit --key-column for unkeyed records.
KEY_COLUMN="$(jq -r '.key_column // empty' "$RUN_DIR/facts.json")"
PARTITION_ASSIGNMENT="$(jq -r '.partition_assignment // empty' "$RUN_DIR/facts.json")"
KAFKA_PARTITIONS="$(jq -r '.kafka_partitions // empty' "$RUN_DIR/facts.json")"
# Use the staged encoding and schema ID. Raw Avro has no schema ID.
VALUE_ENCODING="$(jq -r '.value_encoding // empty' "$RUN_DIR/facts.json")"
SCHEMA_ID="$(jq -r '.schema_id // empty' "$RUN_DIR/facts.json")"

SHARDS="$(yq '.producer.shards' "$STAGED_SPEC")"
[[ $SHARDS == null || $SHARDS == 1 ]] ||
	die "this script supports one producer shard; $(basename "$SPEC_FILE") requests $SHARDS"

# Pass configured options and preserve command defaults for omitted ones.
SPEED="$(yq '.producer.speed' "$STAGED_SPEC")"
REPLAY_SECONDS="$(yq '.producer.seconds' "$STAGED_SPEC")"
BEHIND_MAX_MS="$(yq '.producer.behind_max_ms' "$STAGED_SPEC")"
COMPRESSION="$(yq '.producer.compression' "$STAGED_SPEC")"

# produce_args <epoch> <publish log> <upload prefix>: the producer command shared by the
# seed and the offer.
produce_args() {
	local args="produce --corpus $CORPUS_URI --bootstrap $BOOTSTRAP --topic $RUN_ID --epoch $1"
	args="$args --publish-log $2 --upload-prefix $3"
	[[ -z $KEY_COLUMN ]] || args="$args --key-column $KEY_COLUMN"
	[[ -z $PARTITION_ASSIGNMENT ]] || args="$args --partition-assignment $PARTITION_ASSIGNMENT"
	[[ -z $KAFKA_PARTITIONS ]] || args="$args --partitions $KAFKA_PARTITIONS"
	[[ -z $VALUE_ENCODING ]] || args="$args --value-encoding $VALUE_ENCODING"
	[[ -z $SCHEMA_ID ]] || args="$args --schema-id $SCHEMA_ID"
	[[ $BEHIND_MAX_MS == null ]] || args="$args --behind-max-ms $BEHIND_MAX_MS"
	[[ $COMPRESSION == null ]] || args="$args --compression $COMPRESSION"
	printf '%s' "$args"
}

# An upsert engine that writes its own seed reads it from Kafka first. The epoch is set once the
# table holds it, and the seed must meet the run's freshness bound.
if [[ "$(jq -r '.upsert.seed_delivery // empty' "$RUN_DIR/facts.json")" == kafka ]]; then
	SEED_ROWS="$(jq -r '.upsert.seed_rows' "$RUN_DIR/facts.json")"
	log "sending the $SEED_ROWS-row seed"
	harness "$(produce_args $(($(date +%s) + 5)) "/runs/$RUN_ID/seed/publish_log-0.jsonl" "s3://runs/$RUN_ID/seed") --seed"
	log "waiting up to ${SEED_WAIT_S}s for the engine to commit the seed"
	WAIT_SEED="wait-seed --table $TABLE --catalog-prop-file /catalog.props --corpus $CORPUS_URI"
	WAIT_SEED="$WAIT_SEED --publish-logs s3://runs/$RUN_ID/seed/producer --timeout-s $SEED_WAIT_S"
	WAIT_SEED="$WAIT_SEED --out /runs/$RUN_ID/scores/seed_freshness.json"
	for key in scoring.freshness_bound_s scoring.freshness_max_bound_s scoring.warmup_s producer.behind_max_ms; do
		value="$(yq ".$key" "$STAGED_SPEC")"
		[[ $value == null ]] || WAIT_SEED="$WAIT_SEED --$(tr _ - <<<"${key#*.}") $value"
	done
	harness "$WAIT_SEED"
fi

EPOCH=$(($(date +%s) + EPOCH_LEAD_S))
write_launch_epoch "$RUN_DIR/facts.json" "$EPOCH"
SCORE="score --corpus $CORPUS_URI --table $TABLE --catalog-prop-file /catalog.props"
SCORE="$SCORE --publish-logs s3://runs/$RUN_ID/producer --epoch $EPOCH --out /runs/$RUN_ID/scores"
SCORE="$SCORE --idle-stop-s $IDLE_STOP_S"
# An engine-managed table may not exist until the first record.
MANAGED_BY="$(yq '.table.managed_by' "$STAGED_SPEC")"
[[ $MANAGED_BY == null ]] || SCORE="$SCORE --table-managed-by $MANAGED_BY"
# Preserve scorer defaults for omitted options.
for key in warmup_s freshness_bound_s freshness_max_bound_s gate_window_s mode measurement_s; do
	value="$(yq ".scoring.$key" "$STAGED_SPEC")"
	[[ $value == null ]] || SCORE="$SCORE --${key//_/-} $value"
done
EXACTNESS="$(yq '.scoring.exactness // "EXACTLY_ONCE"' "$STAGED_SPEC")"
SCORE="$SCORE --exactness $EXACTNESS"
if [[ $EXACTNESS == LATEST_PER_KEY ]]; then
	SAMPLE_OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$STAGED_SPEC")"
	[[ -z $SAMPLE_OFFSETS ]] || SCORE="$SCORE --sample-offsets $SAMPLE_OFFSETS"
fi
[[ $SPEED == null ]] || SCORE="$SCORE --speed $SPEED"
# Pass lateness tolerance to the scorer so it can determine producer_bound.
[[ $BEHIND_MAX_MS == null ]] || SCORE="$SCORE --behind-max-ms $BEHIND_MAX_MS"

log "starting the scorer (epoch $EPOCH, idle stop ${IDLE_STOP_S}s)"
compose run -d --name "scorer-$RUN_ID" harness "$SCORE" >/dev/null

PRODUCE="$(produce_args "$EPOCH" "/runs/$RUN_ID/publish_log-0.jsonl" "s3://runs/$RUN_ID")"
[[ $SPEED == null ]] || PRODUCE="$PRODUCE --speed $SPEED"
[[ $REPLAY_SECONDS == null ]] || PRODUCE="$PRODUCE --seconds $REPLAY_SECONDS"
log "offering the corpus"
harness "$PRODUCE"

log "waiting for the scorer to see the table drain"
SCORER_STATUS="$(docker wait "scorer-$RUN_ID")"
docker logs "scorer-$RUN_ID" 2>&1 | tail -n 20 >&2
docker rm "scorer-$RUN_ID" >/dev/null
log "the scorer exited $SCORER_STATUS (0 means drained, 2 means it stopped idle)"

# Measure before cleanup removes the catalog. Preserve file-sizes' default offsets
# when the spec does not supply them.
OFFSETS="$(yq '[.scoring.geometry_offsets_s // [] | .[] | tostring] | join(",")' "$STAGED_SPEC")" ||
	die "could not read scoring.geometry_offsets_s out of $STAGED_SPEC"
OFFSET_FLAGS=""
[[ -z $OFFSETS ]] || OFFSET_FLAGS="--offsets $OFFSETS"

log "measuring file geometry for $RUN_ID"
GEOMETRY_STATUS=0
harness "file-sizes --table $TABLE --catalog-prop-file /catalog.props --epoch $EPOCH --out /runs/$RUN_ID/scores $OFFSET_FLAGS" ||
	GEOMETRY_STATUS=$?
if ((GEOMETRY_STATUS == NO_GEOMETRY)); then
	log "no geometry: the table never committed"
elif ((GEOMETRY_STATUS != 0)); then
	die "could not measure the geometry: file-sizes exited $GEOMETRY_STATUS; see the error above"
fi

# Measure compaction against the same table, before cleanup takes the catalog.
# The target is left to the command, which reads the table property the spec
# declared, or the format default, and names the source it used.
log "measuring compaction for $RUN_ID"
COMPACTION_STATUS=0
harness "compaction --table $TABLE --catalog-prop-file /catalog.props --epoch $EPOCH --out /runs/$RUN_ID/scores $OFFSET_FLAGS" ||
	COMPACTION_STATUS=$?
((COMPACTION_STATUS == 0)) ||
	die "could not measure compaction: compaction exited $COMPACTION_STATUS; see the error above"

if [[ -n $COMPACTION_DRIVER && $EXACTNESS != LATEST_PER_KEY ]]; then
	# Pin one immutable metadata document even when --keep leaves writers running.
	# An upsert run's superseded rows would fail this check; its live state is verified below.
	METADATA_URI="$(harness "table-metadata --table $TABLE --catalog-prop-file /catalog.props")"
	[[ -n $METADATA_URI ]] || die "table-metadata returned no metadata location"
	log "validating live rows in $METADATA_URI"
	harness "verify-table --metadata $METADATA_URI --corpus-uri $CORPUS_URI --publish-logs /runs/$RUN_ID --out /runs/$RUN_ID/scores --exactness $EXACTNESS --catalog-prop-file /catalog.props"
fi
if [[ $EXACTNESS == LATEST_PER_KEY ]]; then
	# Pin one metadata document, dump its sampled snapshots with the Spark reader every
	# engine's run is verified with, then check them against the scorer's stamps.
	METADATA_URI="$(harness "table-metadata --table $TABLE --catalog-prop-file /catalog.props")"
	[[ -n $METADATA_URI ]] || die "table-metadata returned no metadata location"
	log "measuring table health in $METADATA_URI"
	harness "table-health --metadata $METADATA_URI --corpus-uri $CORPUS_URI --checkpoints /runs/$RUN_ID/scores/checkpoints --out /runs/$RUN_ID/scores --catalog-prop-file /catalog.props" ||
		die "could not measure table health; see the error above"
	SNAPSHOTS="$(harness "upsert-scan-plan --metadata $METADATA_URI --checkpoints /runs/$RUN_ID/scores/checkpoints --catalog-prop-file /catalog.props")"
	LIVE="s3://runs/$RUN_ID/live/$(date -u +%Y%m%dT%H%M%SZ)"
	harness "render-spark-scan --site /site.yaml --cores 2 --memory-mb 2048 --out /runs/$RUN_ID/spark-scan.conf"
	log "scanning live rows at snapshots $SNAPSHOTS"
	SCAN_DIR="$RUN_DIR" compose run --rm -T upsert-scan /opt/spark/bin/spark-submit --master 'local[2]' \
		--properties-file /opt/bench/scan/spark-scan.conf /opt/bench/engines/spark/upsert_scan.py \
		--metadata "$METADATA_URI" --snapshots "$SNAPSHOTS" \
		--key-column "$(jq -r '.upsert.key_columns[0]' "$RUN_DIR/facts.json")" --out "$LIVE" >&2 ||
		die "the Spark scan of $METADATA_URI failed"
	log "verifying live state at every sample"
	harness "verify-upsert --metadata $METADATA_URI --corpus-uri $CORPUS_URI --checkpoints /runs/$RUN_ID/scores/checkpoints --live $LIVE --reader none --out /runs/$RUN_ID/scores --catalog-prop-file /catalog.props" ||
		die "live-state verification failed; see $RUN_DIR/scores/terminal-upsert.json"
fi
if [[ -n $COMPACTOR ]]; then
	compactor_compose_logs
	jq -e '.healthy == true' "$RUN_DIR/scores/compactor-status.json" >/dev/null 2>&1 ||
		die "managed compactor is not healthy; see $RUN_DIR/scores/compactor-status.json and compactor.log"
fi

print_verdict "$RUN_DIR/scores/summary.json" "$RUN_DIR/scores/geometry.json"
log "run_valid: true — $RUN_ID, artifacts in $RUN_DIR"
