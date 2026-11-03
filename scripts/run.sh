#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Run stage, launch, gate, teardown and finish in order. Print the run ID immediately
# after staging so an interrupted session can resume with the individual drivers in
# docs/running.md.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Limit the polling wait after launch. The default allows an hour-long offer plus drain
# time.
RUN_MAX_S="${RUN_MAX_S:-7200}"
# Allow time to start an external engine before its ready file appears.
EXTERNAL_READY_WAIT_S="${EXTERNAL_READY_WAIT_S:-1800}"
EXTERNAL_DETACH_WAIT_S="${EXTERNAL_DETACH_WAIT_S:-300}"
WAIT_BROKERS_S="${WAIT_BROKERS_S:-1200}"

# Use a distinct exit code when teardown fails and the fleet may still be running.
TEARDOWN_FAILED=6

usage() {
	cat <<'USAGE'
usage: scripts/run.sh <spec> [options]

  <spec>                      a run spec on this machine, staged as it stands
  --site PATH                 site config shared by all drivers (default: ./site.yaml)
  --image-tag TAG             the harness and engine image tag to run (default: this checkout's commit)
  --publish DIR               passed to finish.sh: also write the result under DIR/<engine>/
  --variant NAME              passed to finish.sh: tuning variant for this run
  --verify-in-cluster         passed to finish.sh: run terminal live-row validation as a
                              Job in the cluster instead of on this host
  --external-ready-file PATH  for `engine: external`, wait for PATH to appear once the run is staged
                              instead of reading a newline from stdin
  --external-detached-file PATH
                              for `engine: external`, wait for PATH before dropping the topic
  --external-detach-wait-s N  maximum detach wait (default: 300; then drops the topic)
  --wait-brokers-s N          passed to stage.sh (default: 1200; 0 skips broker quiet wait)
  --gate-interval-s N         seconds between gate checks (default: 60)
  --breaches N                passed to gate.sh: consecutive non-PASS verdicts before it tears down

Environment: RUN_MAX_S, EXTERNAL_READY_WAIT_S, EXTERNAL_DETACH_WAIT_S,
WAIT_BROKERS_S, RUNS_DIR, and the timeout
settings listed in each driver's --help.

It prints `run_id: <id>` as soon as staging returns.

Exit codes: finish.sh's status (0 only when `run_valid: true`);
6 for incomplete teardown (resources may still be running);
2 for invalid arguments; 1 for other failures, including timeouts.
USAGE
}

SPEC=""
IMAGE_TAG=""
PUBLISH_DIR=""
VARIANT=""
READY_FILE=""
DETACHED_FILE=""
EXTERNAL_DETACH_OPTION=""
BREACHES=""
VERIFY_IN_CLUSTER=no
# Poll about once a minute to avoid repeatedly reading the same scorer sample.
GATE_INTERVAL_S=60
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
	--publish)
		PUBLISH_DIR="${2:?--publish needs a directory}"
		shift 2
		;;
	--variant)
		VARIANT="${2:?--variant needs a name}"
		shift 2
		;;
	--external-ready-file)
		READY_FILE="${2:?--external-ready-file needs a path}"
		shift 2
		;;
	--external-detached-file)
		DETACHED_FILE="${2:?--external-detached-file needs a path}"
		shift 2
		;;
	--external-detach-wait-s)
		EXTERNAL_DETACH_WAIT_S="${2:?--external-detach-wait-s needs a number of seconds}"
		EXTERNAL_DETACH_OPTION=yes
		shift 2
		;;
	--wait-brokers-s)
		WAIT_BROKERS_S="${2:?--wait-brokers-s needs a number of seconds}"
		shift 2
		;;
	--gate-interval-s)
		GATE_INTERVAL_S="${2:?--gate-interval-s needs a number of seconds}"
		shift 2
		;;
	--breaches)
		BREACHES="${2:?--breaches needs a count}"
		shift 2
		;;
	--verify-in-cluster)
		VERIFY_IN_CLUSTER=yes
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

# Validate polling arguments before starting a fleet.
[[ $GATE_INTERVAL_S =~ ^[1-9][0-9]*$ ]] ||
	die "--gate-interval-s requires a positive integer; got '$GATE_INTERVAL_S'"
[[ -z $BREACHES || $BREACHES =~ ^[1-9][0-9]*$ ]] ||
	die "--breaches requires a positive integer; got '$BREACHES'"
[[ $EXTERNAL_DETACH_WAIT_S =~ ^[0-9]+$ ]] ||
	die "--external-detach-wait-s requires a non-negative integer; got '$EXTERNAL_DETACH_WAIT_S'"
[[ $WAIT_BROKERS_S =~ ^[0-9]+$ ]] ||
	die "--wait-brokers-s requires a non-negative integer; got '$WAIT_BROKERS_S'"
# Pass one breach threshold to the gate and use it below. Tests check it matches gate.sh's
# default.
BREACHES="${BREACHES:-3}"

# Check all drivers' prerequisites before staging.
require_host_tools kubectl aws yq jq git curl gzip
[[ -f $SPEC ]] || die "no run spec at $SPEC"
require_site_file
RUNS_ROOT="$(site_root '.runs_root')"

# Validate external-only options before staging can start a managed engine.
ENGINE="$(yq '.engine' "$SPEC")"
if [[ $ENGINE != external && (-n $READY_FILE || -n $DETACHED_FILE || -n $EXTERNAL_DETACH_OPTION) ]]; then
	die "an external lifecycle option requires engine: external; $SPEC specifies '$ENGINE'"
fi

SCRATCH="$(mktemp -d "${TMPDIR:-/tmp}/ingest-bench-run.XXXXXX")"
trap 'rm -rf "$SCRATCH"' EXIT

# Pass the selected site explicitly to every driver.
COMMON=(--site "$SITE_FILE")
[[ -z $IMAGE_TAG ]] || COMMON+=(--image-tag "$IMAGE_TAG")
STAGE_ARGS=("${COMMON[@]}" --wait-brokers-s "$WAIT_BROKERS_S")
EXTERNAL_TEARDOWN_ARGS=()
if [[ $ENGINE == external ]]; then
	EXTERNAL_TEARDOWN_ARGS=(--external-detach-wait-s "$EXTERNAL_DETACH_WAIT_S")
	[[ -z $DETACHED_FILE ]] || EXTERNAL_TEARDOWN_ARGS+=(--external-detached-file "$DETACHED_FILE")
fi

# ---------------------------------------------------------------------------
# 1. Stage
# ---------------------------------------------------------------------------

log "staging $SPEC"
# Capture stage's result while leaving its stderr diagnostics visible.
STAGE_OUT="$("$REPO_ROOT/scripts/stage.sh" "$SPEC" "${STAGE_ARGS[@]}")"
RUN_ID="$(awk -F': ' '/^run_id: /{print $2; exit}' <<<"$STAGE_OUT")"
[[ -n $RUN_ID ]] || die "scripts/stage.sh printed no run_id line, so there is no run to launch"
# Print the run ID before later steps can fail, allowing manual recovery.
printf 'run_id: %s\n' "$RUN_ID"

RUN_DIR="$RUNS_DIR/$RUN_ID"
SUMMARY_URI="$RUNS_ROOT/$RUN_ID/scores/summary.json"

if [[ $ENGINE == external ]]; then

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
		read -r ||
			die "no input received; $RUN_ID is staged but not launched. Use --external-ready-file for unattended runs"
	fi
fi

# ---------------------------------------------------------------------------
# 2. Launch
# ---------------------------------------------------------------------------

LAUNCH_STATUS=0
"$REPO_ROOT/scripts/launch.sh" "$RUN_ID" "${COMMON[@]}" || LAUNCH_STATUS=$?
if ((LAUNCH_STATUS != 0)); then
	# Leave pods available for inspecting launch failures; print the teardown command.
	log "$RUN_ID is staged and its resources are still running. Review the output above, then stop it with scripts/teardown.sh $RUN_ID"
	exit "$LAUNCH_STATUS"
fi

# ---------------------------------------------------------------------------
# 3. Judge it while it goes
# ---------------------------------------------------------------------------

GATE_ARGS=("$RUN_ID" "${COMMON[@]}" --teardown --breaches "$BREACHES"
	${EXTERNAL_TEARDOWN_ARGS[@]+"${EXTERNAL_TEARDOWN_ARGS[@]}"})
# Read the gate's persisted count of consecutive non-PASS verdicts.
BREACH_FILE="$RUN_DIR/gate-breaches"

TORN_DOWN=0
OVERRAN=0
deadline=$((SECONDS + RUN_MAX_S))
# Report an unreadable breach file only once.
BREACH_FILE_REPORTED=""
while :; do
	# Enforce the timeout even when no summary can be read.
	if ((SECONDS >= deadline)); then
		log "$RUN_ID was still running after ${RUN_MAX_S}s, so it is being torn down unfinished"
		OVERRAN=1
		break
	fi
	remaining=$((deadline - SECONDS))
	sleep "$((remaining < GATE_INTERVAL_S ? remaining : GATE_INTERVAL_S))"
	((SECONDS < deadline)) || continue

	# Let gate --teardown apply the consecutive-breach rule; do not stop on one verdict.
	GATE_STATUS=0
	"$REPO_ROOT/scripts/gate.sh" "${GATE_ARGS[@]}" || GATE_STATUS=$?
	case "$GATE_STATUS" in
	0) log "gate: PASS (exit 0)" ;;
	3) log "gate: UNDERSIZED (exit 3)" ;;
	5) log "gate: VOID (exit 5)" ;;
	*) log "gate: could not judge the run (exit $GATE_STATUS); see the error above" ;;
	esac

	# The marker is written only after teardown completes, including when no table exists.
	if [[ -f $RUN_DIR/$TEARDOWN_COMPLETE_FILE ]]; then
		log "the gate tore $RUN_ID down"
		TORN_DOWN=1
		break
	fi

	# A failed teardown or an older driver may leave no completion marker. Stop polling
	# at the breach threshold and retry the idempotent teardown below.
	if ((GATE_STATUS != 0)) && [[ -f $BREACH_FILE ]]; then
		# Validate as text before arithmetic: Bash treats barewords as variable names, which can
		# abort under set -u before cleanup.
		BREACH_COUNT="$(cat "$BREACH_FILE")"
		if [[ ! $BREACH_COUNT =~ ^[0-9]+$ ]]; then
			[[ -n $BREACH_FILE_REPORTED ]] ||
				log "$BREACH_FILE holds '$BREACH_COUNT' rather than a count of verdicts, so whether the gate tore $RUN_ID down cannot be read from it; remove it"
			BREACH_FILE_REPORTED=yes
		elif ((BREACH_COUNT >= BREACHES)); then
			log "the gate has not passed $RUN_ID for $BREACHES ticks, so it has torn it down"
			break
		fi
	fi

	if ! aws s3 cp "$SUMMARY_URI" "$SCRATCH/summary.json" --only-show-errors; then
		log "the scorer has published nothing at $SUMMARY_URI yet"
		continue
	fi
	# Retry an unreadable summary; it may be an incomplete update.
	STATE="$(jq -r '.state // empty' "$SCRATCH/summary.json")" || STATE=""
	if [[ -z $STATE ]]; then
		log "$SUMMARY_URI names no state yet"
		continue
	fi
	if [[ $STATE != running ]]; then
		log "$RUN_ID is $STATE"
		break
	fi
done

# ---------------------------------------------------------------------------
# 4. Tear down, and read it
# ---------------------------------------------------------------------------

# Initialize even when the gate already completed teardown.
TEARDOWN_STATUS=0
if ((TORN_DOWN == 0)); then
	# Read and publish the verdict even if teardown failed.
	"$REPO_ROOT/scripts/teardown.sh" "$RUN_ID" "${COMMON[@]}" \
		${EXTERNAL_TEARDOWN_ARGS[@]+"${EXTERNAL_TEARDOWN_ARGS[@]}"} || TEARDOWN_STATUS=$?
	((TEARDOWN_STATUS == 0)) ||
		log "tearing $RUN_ID down exited $TEARDOWN_STATUS, so its fleet may still be running: scripts/teardown.sh $RUN_ID"
fi

FINISH_ARGS=("$RUN_ID" --site "$SITE_FILE")
[[ -z $VARIANT ]] || FINISH_ARGS+=(--variant "$VARIANT")
[[ -z $PUBLISH_DIR ]] || FINISH_ARGS+=(--publish "$PUBLISH_DIR")
if [[ $VERIFY_IN_CLUSTER == yes ]]; then
	FINISH_ARGS+=(--verify-in-cluster)
	[[ -z $IMAGE_TAG ]] || FINISH_ARGS+=(--image-tag "$IMAGE_TAG")
fi
FINISH_STATUS=0
"$REPO_ROOT/scripts/finish.sh" "${FINISH_ARGS[@]}" || FINISH_STATUS=$?

# Report failures after collecting the verdict. Prioritize failed teardown over an overrun
# because the fleet may still incur charges.
if ((TEARDOWN_STATUS != 0)); then
	log "exiting $TEARDOWN_FAILED: the teardown above did not converge, so check the cluster"
	exit "$TEARDOWN_FAILED"
fi
((OVERRAN == 0)) ||
	die "$RUN_ID was still running after ${RUN_MAX_S}s, so it was torn down unfinished; raise RUN_MAX_S for a longer offer"
exit "$FINISH_STATUS"
