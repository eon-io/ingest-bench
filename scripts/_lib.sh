# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Shared run-script helpers. Engine-specific Compose operations live in
# engines/<name>/compose.sh. Source this file; it leaves shell options to the caller.

# Resolve paths from this file so commands work outside the repository root.
_LIB_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$_LIB_DIR/.." && pwd)"
COMPOSE_FILE="$REPO_ROOT/deploy/compose/local/docker-compose.yml"

# Callers with different prerequisites can set this before sourcing the file.
PREREQ_DOC="${PREREQ_DOC:-docs/running.md}"

# Keep diagnostics on stderr so stdout remains parseable.
log() {
	printf '%s  %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*" >&2
}

die() {
	log "$*"
	exit 1
}

# Replace facts atomically; a subshell keeps temporary cleanup separate from
# the caller's traps (including local container cleanup).
write_launch_epoch() (
	local facts=$1 epoch=$2 temporary
	temporary="$(mktemp "$facts.XXXXXX")" || return
	trap 'rm -f "$temporary"' EXIT
	jq --argjson epoch "$epoch" '.epoch = $epoch' "$facts" >"$temporary" || return
	mv "$temporary" "$facts"
)

# Require an explicit deployment before provisioning or deleting resources.
read_deployment_site() {
	[[ -f $SITE_FILE ]] ||
		die "site configuration $SITE_FILE does not exist; pass --site with a configuration that sets kafka.deployment (managed, in-cluster, or external)"
	require_host_tools yq
	if ! KAFKA_DEPLOYMENT="$(yq -r '.kafka.deployment // ""' "$SITE_FILE" 2>&1)"; then
		die "could not read kafka.deployment from $SITE_FILE: $KAFKA_DEPLOYMENT"
	fi
	case "$KAFKA_DEPLOYMENT" in
	managed | in-cluster | external) ;;
	*) die "$SITE_FILE must set kafka.deployment to managed, in-cluster, or external; use --site to select another site" ;;
	esac
}

# Enable all profiles so every service is addressable; callers explicitly name what to
# start. Discover profiles from the engines' and the compactors' Compose files so adding
# either needs no change here.
compose() {
	local profiles=() name
	while IFS= read -r name; do
		[[ -n $name ]] || continue
		profiles+=(--profile "$name")
	done < <(yq -N '.services.*.profiles[]' "$REPO_ROOT"/engines/*/compose.yaml \
		"$REPO_ROOT"/compactors/*/compose.yaml | sort -u)
	docker compose -f "$COMPOSE_FILE" --profile tools ${profiles[@]+"${profiles[@]}"} "$@"
}

# Run one command string through the image's shell entrypoint. Disable the TTY to keep
# output parseable.
harness() {
	compose run --rm -T harness "$1"
}

# Print a consistent verdict for local and cluster runs, then fail if run_valid is false.
# Show optional geometry before failing; geometry does not determine validity.
print_verdict() {
	local summary=$1 geometry=${2:-}
	[[ -f $summary ]] || die "the scorer published no $summary"
	jq '{
  run_valid, state, reason, producer_bound,
  prefix, last_batch, committed_rows, offered_rows,
  freshness: .freshness.window,
  exactness: (.exactness | {mode, verdict, exact, loss_rows, duplicate_rows, corrupt_batches, invalid_rows, unoffered_rows}),
  keepup
} + (if .scoring_mode == "saturation" then {scoring_mode, saturation} else {} end)' "$summary"
	# Omit geometry when there is no measured p50.
	if [[ -n $geometry && -f $geometry ]]; then
		jq -r '(.final.live // empty) | select(.size_quantiles.p50 != null)
  | "geometry: p50 \((.size_quantiles.p50 / 1048576 * 10 | round) / 10) MiB, "
    + "small (<32 MiB) \((.small_file_share_32mib * 1000 | round) / 10)%, \(.files) files"' "$geometry"
	fi
	[[ "$(jq -r .run_valid "$summary")" == true ]] ||
		die "run_valid is false; see the verdict above and details in $summary"
}

# Ask for confirmation before deleting measured data. Require an interactive stdin;
# unattended callers must use their --yes option.
confirm() {
	[[ -t 0 ]] || die "confirmation requires an interactive terminal; pass --yes to run unattended"
	printf '%s [y/N] ' "$1"
	local answer=""
	read -r answer || true
	case "$answer" in
	y | Y) return 0 ;;
	*) die "operation cancelled (response: '${answer:-none}')" ;;
	esac
}

# Check prerequisites before starting work.
require_host_tools() {
	local missing="" tool
	for tool in "$@"; do
		command -v "$tool" >/dev/null 2>&1 || missing="$missing $tool"
	done
	[[ -z $missing ]] || die "missing host tool(s):$missing — see $PREREQ_DOC for prerequisites"
}
