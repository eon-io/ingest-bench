#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Measure one producer process against local Kafka in encoded MB/s and rows/s. A past
# epoch and speed 1000 make every batch immediately due, removing pacing delays.
# The result includes contention from the shared local stack. Measure on the target
# hardware before sizing a cluster run; see docs/corpus.md.
set -euo pipefail
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"

usage() {
	cat <<'USAGE'
usage: scripts/measure-producer.sh

Measures producer throughput using a fixed corpus, topic, and speed.
Prints elapsed seconds, encoded MB, MB/s, rows, and rows/s.
Requires Docker and this checkout's local stack. Takes no arguments.
USAGE
}

while [[ $# -gt 0 ]]; do
	case "$1" in
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

require_host_tools docker

cleanup() {
	local status=$?
	log "tearing the stack down"
	compose down -v --remove-orphans >/dev/null 2>&1 || true
	exit "$status"
}
trap cleanup EXIT

log "building the harness image"
compose build harness

log "starting the broker and the object store"
compose up -d kafka minio minio-init

log "generating the corpus"
GEN_OUT="$(harness "gen-corpus --preset smoke --set duration_s=60 --set offered_bytes_per_s=50MB --set partition_count=64 --out s3://corpus --seed 1")"
log "$GEN_OUT"
# Use the generator's reported URI instead of listing the bucket.
WROTE_LINE="$(printf '%s\n' "$GEN_OUT" | grep '^wrote ')"
[[ -n $WROTE_LINE ]] || die "gen-corpus printed no 'wrote' line; see the output above"
CORPUS_URI="${WROTE_LINE#wrote }"
CORPUS_URI="${CORPUS_URI%%: *}"

# Read measured row and byte counts from corpus.json; whole-row batching can exceed the
# preset's byte budget. Quote the Python command for the image's shell entrypoint.
META_LINE="$(harness "python -c 'from ingest_bench import uri; import json; meta = json.loads(uri.read_text(uri.join(\"$CORPUS_URI\", \"corpus.json\"))); print(\"ROW_COUNT=%d ENCODED_BYTES=%d\" % (meta[\"row_count\"], meta[\"encoded_bytes\"]))'")"
ROW_COUNT="$(printf '%s\n' "$META_LINE" | sed -n 's/.*ROW_COUNT=\([0-9]*\).*/\1/p')"
ENCODED_BYTES="$(printf '%s\n' "$META_LINE" | sed -n 's/.*ENCODED_BYTES=\([0-9]*\).*/\1/p')"
[[ $ROW_COUNT =~ ^[0-9]+$ ]] || die "could not read row_count from corpus.json; got: $META_LINE"
[[ $ENCODED_BYTES =~ ^[0-9]+$ ]] || die "could not read encoded_bytes from corpus.json; got: $META_LINE"
log "corpus $CORPUS_URI: $ROW_COUNT rows, $ENCODED_BYTES encoded bytes"

# Recreate the topic to exclude records from earlier measurements.
log "creating the measure topic (8 partitions)"
harness "python -c 'from ingest_bench import kafka_admin as k; k.delete_topic(\"kafka:9092\", \"measure\", client={}); k.create_topic(\"kafka:9092\", \"measure\", 8, 1, topic_config={}, client={})'"

EPOCH=$(($(date +%s) - 3600))
log "producing $CORPUS_URI into measure (epoch $EPOCH, speed 1000)"
START=$(date +%s)
harness "produce --corpus $CORPUS_URI --bootstrap kafka:9092 --topic measure --epoch $EPOCH --speed 1000 --key-column user_id --publish-log /tmp/measure.jsonl" | tail -n 3
END=$(date +%s)

WALL=$((END - START))
((WALL > 0)) || die "wall time measured as ${WALL}s at one-second resolution; rerun with a longer corpus"

awk -v bytes="$ENCODED_BYTES" -v rows="$ROW_COUNT" -v wall="$WALL" 'BEGIN {
	mb = bytes / 1000000
	printf "wall seconds: %d\n", wall
	printf "encoded MB: %.1f\n", mb
	printf "MB/s: %.2f\n", mb / wall
	printf "rows: %d\n", rows
	printf "rows/s: %.0f\n", rows / wall
}'
