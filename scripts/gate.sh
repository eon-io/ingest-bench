#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Judge a running benchmark from the scorer's artifacts, keeping one source of commit
# visibility measurements. Exit codes: 0 PASS, 3 UNDERSIZED, 5 VOID.
# With --teardown, require consecutive non-PASS verdicts to avoid stopping on a transient
# breach.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/gate.sh <run_id> [options]

  <run_id>           launched run with scorer output under the runs prefix
  --site PATH        site config for the runs prefix (default: ./site.yaml)
  --image-tag TAG    passed to teardown.sh, which runs one harness Job (default: this checkout's commit)
  --external-detached-file PATH  passed to teardown.sh before an external topic is dropped
  --external-detach-wait-s N     passed to teardown.sh (default: 300)
  --teardown         stop the run after --breaches consecutive non-PASS verdicts
  --breaches N       how many consecutive non-PASS verdicts --teardown waits for (default 3; 1 acts at once)

Environment: RUNS_DIR.

Exit codes: 0 PASS, 3 UNDERSIZED, 5 VOID.
USAGE
}

RUN_ID=""
TEARDOWN=0
IMAGE_TAG=""
DETACH_ARGS=()
# Require repeated breaches before teardown; the usual polling interval is one minute.
BREACHES_REQUIRED=3
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
	--external-detached-file | --external-detach-wait-s)
		DETACH_ARGS+=("$1" "${2:?external detach option needs a value}")
		shift 2
		;;
	--teardown)
		TEARDOWN=1
		shift
		;;
	--breaches)
		BREACHES_REQUIRED="${2:?--breaches needs a count}"
		[[ $BREACHES_REQUIRED =~ ^[1-9][0-9]*$ ]] ||
			die "--breaches requires a positive integer; got '$BREACHES_REQUIRED'"
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

require_host_tools aws yq
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-gate.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

# Fetch only the two artifacts the gate needs.
for name in summary.json keepup_samples.jsonl; do
	aws s3 cp "$RUNS_ROOT/$RUN_ID/scores/$name" "$SCRATCH/$name" --only-show-errors >&2 ||
		die "the scorer has published no $name under $RUNS_ROOT/$RUN_ID/scores/; it may not have read the table yet"
done

GATE=(gate --out "$SCRATCH")
# Read scoring overrides from the uploaded spec so every operator uses the same windows.
# An unreadable spec is an error, not permission to substitute defaults.
SPEC="$SCRATCH/spec.yaml"
aws s3 cp "$RUNS_ROOT/$RUN_ID/stage/spec.yaml" "$SPEC" --only-show-errors >&2 ||
	die "could not read gate windows from $RUNS_ROOT/$RUN_ID/stage/spec.yaml"
ADAPTATION_S="$(yq '.scoring.gate_adaptation_s' "$SPEC")"
[[ $ADAPTATION_S == null ]] || GATE+=(--adaptation-s "$ADAPTATION_S")
WINDOW_S="$(yq '.scoring.gate_window_s' "$SPEC")"
[[ $WINDOW_S == null ]] || GATE+=(--window-s "$WINDOW_S")

VERDICT_STATUS=0
harness_local "${GATE[@]}" || VERDICT_STATUS=$?

# Persist consecutive non-PASS counts because each gate invocation is a separate process.
BREACH_FILE="$RUNS_DIR/$RUN_ID/gate-breaches"
BREACHES=0
if ((VERDICT_STATUS == 0)); then
	# Reset on PASS so separated breaches do not accumulate.
	rm -f "$BREACH_FILE"
else
	if [[ -f $BREACH_FILE ]]; then
		BREACHES="$(cat "$BREACH_FILE")"
		[[ $BREACHES =~ ^[0-9]+$ ]] ||
			die "$BREACH_FILE holds '$BREACHES' rather than a count of consecutive verdicts; remove it"
	fi
	BREACHES=$((BREACHES + 1))
	mkdir -p "$RUNS_DIR/$RUN_ID"
	printf '%s\n' "$BREACHES" >"$BREACH_FILE"
fi

if ((VERDICT_STATUS != 0)) && ((TEARDOWN == 1)) && ((BREACHES < BREACHES_REQUIRED)); then
	log "the verdict is not PASS (breach $BREACHES of $BREACHES_REQUIRED), so $RUN_ID is left running"
fi

if ((VERDICT_STATUS != 0)) && ((TEARDOWN == 1)) && ((BREACHES >= BREACHES_REQUIRED)); then
	# Count all nonzero statuses, including unavailable measurements.
	log "the verdict has not been PASS for $BREACHES consecutive ticks, so tearing $RUN_ID down"
	# Pass the image tag through to teardown's drop-topic Job.
	TEARDOWN_ARGS=("$RUN_ID" --site "$SITE_FILE" ${DETACH_ARGS[@]+"${DETACH_ARGS[@]}"})
	[[ -z $IMAGE_TAG ]] || TEARDOWN_ARGS+=(--image-tag "$IMAGE_TAG")
	# Report teardown failures without replacing the gate's exit code. The operator must
	# check any fleet left running.
	TEARDOWN_STATUS=0
	"$REPO_ROOT/scripts/teardown.sh" "${TEARDOWN_ARGS[@]}" || TEARDOWN_STATUS=$?
	((TEARDOWN_STATUS == 0)) ||
		log "tearing $RUN_ID down exited $TEARDOWN_STATUS, so its fleet may still be running: scripts/teardown.sh $RUN_ID"
fi
exit "$VERDICT_STATUS"
