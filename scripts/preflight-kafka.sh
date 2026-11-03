#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Refuse to stage a run onto in-cluster brokers that still hold another run's topic or
# lack the space its offer needs. Topics are created with 48-hour retention, so the whole
# offer is resident at the end of a run; a topic that survived a failed launch or teardown
# keeps its space until teardown.sh drops it. Read-only: this changes nothing.
set -euo pipefail
PREREQ_DOC="deploy/k8s/stack/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Exit code for a failed check, distinct from usage (2) and other failures (1).
UNSAFE=3
# Deletion of a large topic can keep broker disks and replication busy after Kafka no
# longer lists it. Require stable disk use and no pending deletion directories.
WAIT_BROKERS_S="${WAIT_BROKERS_S:-1200}"
BROKER_QUIET_SAMPLE_S="${BROKER_QUIET_SAMPLE_S:-30}"
BROKER_QUIET_DELTA_KIB=65536

usage() {
	cat <<'USAGE'
usage: scripts/preflight-kafka.sh <spec> [options]

  <spec>             the run spec about to be staged; its corpus preset sizes the offer
  --site PATH        site config naming the in-cluster brokers (default: ./site.yaml)
  --margin PCT       free space required beyond the projected peak, in percent (default: 20)
  --wait-brokers-s N wait up to N seconds for broker deletion and disk activity to settle
                     (default: 1200; 0 skips the quiet check)

Checks that no topic other than Kafka's own exists, and that every broker's free space
covers its share of the offer at the topic's replication factor plus the margin. It
estimates uncompressed bytes, so it can reject a compressed run whose data would fit.

Exit codes: 0 safe to stage, 3 unsafe (a leftover topic or too little space), 2 for
invalid arguments, 1 for other failures.
USAGE
}

SPEC=""
MARGIN_PCT=20
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--margin)
		MARGIN_PCT="${2:?--margin needs a percentage}"
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
[[ $MARGIN_PCT =~ ^[0-9]+$ ]] || die "--margin requires a whole percentage; got '$MARGIN_PCT'"
[[ $WAIT_BROKERS_S =~ ^[0-9]+$ ]] || die "--wait-brokers-s requires a non-negative integer; got '$WAIT_BROKERS_S'"
[[ $BROKER_QUIET_SAMPLE_S =~ ^[1-9][0-9]*$ ]] || die "BROKER_QUIET_SAMPLE_S must be a positive integer"

require_host_tools kubectl yq jq
[[ -f $SPEC ]] || die "no run spec at $SPEC"
require_site_file
k8s_read_site

# Bytes offered over the run: the preset's rate over its corpus duration, cut to a
# probe's producer.seconds when set. Replay speed changes wall time, not retained bytes.
# This estimates uncompressed corpus bytes. With producer.compression, the check can
# reject brokers that would hold the compressed run.
CORPUS="$(yq -r '.corpus // ""' "$SPEC")"
# A spec that names a track takes its corpus from the track.
TRACK="$(yq -r '.track // ""' "$SPEC")"
if [[ -z $CORPUS && -n $TRACK ]]; then
	TRACK_FILE="$REPO_ROOT/workloads/tracks/$TRACK.yaml"
	[[ -f $TRACK_FILE ]] || die "spec track '$TRACK' is not a shipped track under workloads/tracks"
	CORPUS="$(yq -r '.corpus' "$TRACK_FILE")"
fi
PRESET="$REPO_ROOT/workloads/presets/$CORPUS.yaml"
[[ -f $PRESET ]] || die "spec corpus '$CORPUS' is not a shipped preset under workloads/presets"
RATE="$(yq -r '.offered_bytes_per_s' "$PRESET")"
DURATION_S="$(yq -r '.duration_s' "$PRESET")"
SECONDS_OFFERED="$(yq -r ".producer.seconds // $DURATION_S" "$SPEC")"
# Decimal SI suffixes, as the presets write them.
case "$RATE" in
*GB) RATE_BPS=$((${RATE%GB} * 1000000000)) ;;
*MB) RATE_BPS=$((${RATE%MB} * 1000000)) ;;
*KB) RATE_BPS=$((${RATE%KB} * 1000)) ;;
*) RATE_BPS="$RATE" ;;
esac
[[ $RATE_BPS =~ ^[0-9]+$ ]] || die "could not read offered_bytes_per_s '$RATE' from $PRESET"

# A reference encoding sizes rows, not Kafka values. Use the completed corpus's
# encoded bytes; never fall back to its smaller reference budget on a read failure.
if [[ $(yq -r '.sizing_encoding // ""' "$PRESET") != "" ]]; then
	require_host_tools aws
	CORPUS_ROOT="$(site_root '.corpus_root')"
	LISTING="$(aws s3 ls "${CORPUS_ROOT%/}/")" || die "could not list corpora under $CORPUS_ROOT"
	CORPUS_DIRS=()
	IFS=$'\n' read -r -d '' -a CORPUS_DIRS < <(awk -v prefix="$CORPUS-" '$1 == "PRE" && index($2, prefix) == 1 {print $2}' <<<"$LISTING") || true
	((${#CORPUS_DIRS[@]} == 1)) || die "expected exactly one generated $CORPUS under $CORPUS_ROOT"
	CORPUS_META="$(mktemp)"
	trap 'rm -f "$CORPUS_META"' EXIT
	aws s3 cp "${CORPUS_ROOT%/}/${CORPUS_DIRS[0]}corpus.json" "$CORPUS_META" --only-show-errors ||
		die "could not read generated corpus metadata for $CORPUS"
	RATE_BPS="$(jq -er --arg name "$CORPUS" '
		select(.name == $name and .value_encoding == "json" and .shard_count == 1)
		| select((.duration_s | type) == "number" and .duration_s > 0)
		| select((.encoded_bytes | type) == "number" and .encoded_bytes > 0)
		| (.encoded_bytes / .duration_s | ceil)' "$CORPUS_META")" ||
		die "invalid completed JSON corpus metadata for $CORPUS"
	DURATION_S="$(jq -r '.duration_s' "$CORPUS_META")"
	SECONDS_OFFERED="$(yq -r ".producer.seconds // $DURATION_S" "$SPEC")"
	SECONDS_OFFERED="$(jq -n --argjson seconds "$SECONDS_OFFERED" --argjson duration "$DURATION_S" '[$seconds, $duration] | min')"
fi

# The brokers, from the bootstrap address `<kafka name>-kafka-bootstrap.<namespace>.svc`.
BOOTSTRAP="$(site_required '.kafka.bootstrap_servers')"
KAFKA_NAME="${BOOTSTRAP%%-kafka-bootstrap*}"
[[ $KAFKA_NAME != "$BOOTSTRAP" ]] ||
	die "kafka.bootstrap_servers '$BOOTSTRAP' is not an in-cluster Strimzi listener; this check reads the brokers' pods"
KUBECTL=(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE")
BROKER_LIST="$("${KUBECTL[@]}" get pods -l "strimzi.io/cluster=$KAFKA_NAME,strimzi.io/broker-role=true" \
	-o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}')" ||
	die "could not list broker pods for kafka/$KAFKA_NAME in $SITE_NAMESPACE"
BROKERS=()
IFS=$'\n' read -r -d '' -a BROKERS <<<"$BROKER_LIST" || true
((${#BROKERS[@]} > 0)) || die "no broker pods for kafka/$KAFKA_NAME in $SITE_NAMESPACE"
REPLICATION=$((${#BROKERS[@]} < 3 ? ${#BROKERS[@]} : 3))

# Strimzi mounts the first volume at data-0.
DATA_DIR=/var/lib/kafka/data-0
PEAK_TOTAL="$(jq -n --argjson rate "$RATE_BPS" --argjson seconds "$SECONDS_OFFERED" \
	--argjson replication "$REPLICATION" '($rate * $seconds * $replication) | floor')"
PER_BROKER="$(jq -n --argjson total "$PEAK_TOTAL" --argjson n "${#BROKERS[@]}" --argjson margin "$MARGIN_PCT" \
	'($total / $n * (1 + $margin / 100)) | floor')"
log "corpus $RATE_BPS B/s for ${SECONDS_OFFERED}s at replication $REPLICATION across ${#BROKERS[@]} brokers: $PEAK_TOTAL B of log, $PER_BROKER B per broker with ${MARGIN_PCT}% margin"

UNSAFE_REASONS=()

# Kafka's own topics start with a double underscore.
TOPIC_LIST=""
if ! TOPIC_LIST="$("${KUBECTL[@]}" exec "${BROKERS[0]}" -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)"; then
	die "could not list Kafka topics through ${BROKERS[0]}"
fi
TOPICS="$(grep -v '^__' <<<"$TOPIC_LIST" || true)"
if [[ -n $TOPICS ]]; then
	UNSAFE_REASONS+=("leftover topic(s): $(tr '\n' ' ' <<<"$TOPICS")— run scripts/teardown.sh for their run, or drop them, before staging")
fi

# Sum pending deletion directories and used KiB across the brokers. Topic deletion only
# counts as settled once no directory remains and aggregate use moves by less than 64 MiB
# over one 30-second interval.
if ((WAIT_BROKERS_S > 0)) && ((${#UNSAFE_REASONS[@]} == 0)); then
	previous_used=""
	deadline=$((SECONDS + WAIT_BROKERS_S))
	sample=0
	while :; do
		pending_total=0
		used_total=0
		for broker in "${BROKERS[@]}"; do
			state="$("${KUBECTL[@]}" exec "$broker" -c kafka -- sh -c \
				'set -- /var/lib/kafka/data*/kafka-log*
				[ -d "$1" ] || exit 1
				d=$1
				deletes=$(find "$d" -mindepth 1 -maxdepth 1 -type d -name "*-delete" -print) || exit 1
				if [ -z "$deletes" ]; then
					pending=0
				else
					pending=$(printf "%s\n" "$deletes" | wc -l)
				fi
				used=$(df -k "$d" | awk "END {print \$3}") || exit 1
				printf "%s %s\n" "$pending" "$used"')" ||
				die "could not read pending deletion directories and disk use from $broker"
			read -r pending used extra <<<"$state"
			[[ $pending =~ ^[0-9]+$ && $used =~ ^[0-9]+$ && -z ${extra:-} ]] ||
				die "could not parse broker quiet sample from $broker: '$state'"
			pending_total=$((pending_total + pending))
			used_total=$((used_total + used))
		done
		sample=$((sample + 1))
		log "broker quiet sample $sample: pending-delete dirs $pending_total, used $used_total KiB"
		if [[ -n $previous_used ]]; then
			delta=$((used_total - previous_used))
			((delta < 0)) && delta=$((-delta))
			if ((pending_total == 0 && delta < BROKER_QUIET_DELTA_KIB)); then
				log "brokers are quiet: no pending-delete dirs and disk use changed by $delta KiB"
				break
			fi
		fi
		((SECONDS < deadline)) ||
			die "brokers did not become quiet within ${WAIT_BROKERS_S}s; pending-delete dirs $pending_total, used $used_total KiB"
		remaining=$((deadline - SECONDS))
		pause=$((remaining < BROKER_QUIET_SAMPLE_S ? remaining : BROKER_QUIET_SAMPLE_S))
		sleep "$pause"
		previous_used=$used_total
	done
elif ((WAIT_BROKERS_S == 0)); then
	log "skipping the broker quiet wait (--wait-brokers-s 0)"
fi

for broker in "${BROKERS[@]}"; do
	AVAIL="$("${KUBECTL[@]}" exec "$broker" -- df -B1 --output=avail "$DATA_DIR" | tail -n 1 | tr -d ' ')"
	[[ $AVAIL =~ ^[0-9]+$ ]] || die "could not read free space on $broker:$DATA_DIR"
	if ((AVAIL < PER_BROKER)); then
		UNSAFE_REASONS+=("$broker has $AVAIL B free at $DATA_DIR, below the $PER_BROKER B its share needs")
	else
		log "$broker: $AVAIL B free"
	fi
done

if ((${#UNSAFE_REASONS[@]} > 0)); then
	for reason in "${UNSAFE_REASONS[@]}"; do
		log "UNSAFE: $reason"
	done
	exit "$UNSAFE"
fi
log "brokers are clear for $SPEC"
