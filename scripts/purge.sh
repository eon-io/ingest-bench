#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Delete a collected run's table and data, and optionally its stored artifacts. Keep this
# separate from teardown so stopping a fleet preserves evidence. List deletions and
# require confirmation unless --yes is supplied.
# Read the table location from metadata; never infer a deletion prefix from its name.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

usage() {
	cat <<'USAGE'
usage: scripts/purge.sh <run_id> [options]

  <run_id>       stopped run under $RUNS_DIR whose table and data will be deleted
  --site PATH    site config for the cluster, the catalog and the runs prefix (default: ./site.yaml)
  --artifacts    also delete stored run artifacts: scores, publish logs, and saved metadata.
                 Required to remove artifacts when the table is already absent
  --yes          skip confirmation for unattended use

Environment: RUNS_DIR.

Lists all planned deletions and asks for confirmation before deleting anything.
USAGE
}

RUN_ID=""
ARTIFACTS=no
ASSUME_YES=no
while [[ $# -gt 0 ]]; do
	case "$1" in
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--artifacts)
		ARTIFACTS=yes
		shift
		;;
	--yes)
		ASSUME_YES=yes
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

require_host_tools kubectl aws yq jq gzip

RUN_DIR="$RUNS_DIR/$RUN_ID"
FACTS="$RUN_DIR/facts.json"
[[ -f $FACTS ]] || die "no staged run at $RUN_DIR; use the checkout where stage.sh saved it, or set RUNS_DIR"

k8s_read_site

TABLE="$(jq -r .table "$FACTS")"
[[ -n $TABLE && $TABLE != null ]] || die "$FACTS names no table"

# Reuse catalog properties for metadata lookup and table deletion.
# Install tunnel cleanup before reading catalog properties.
trap k8s_port_forward_stop EXIT
read_catalog_prop_flags

# Use saved metadata when available; otherwise ask the catalog. Missing local metadata
# does not mean the table is absent. Distinguish TABLE_ABSENT from connection or
# credential failures before allowing deletion.
METADATA_FINAL="$RUN_DIR/$METADATA_FINAL_FILE"
# Read saved metadata or a temporary copy fetched from the catalog.
METADATA_DOCUMENT="$METADATA_FINAL"
HAVE_TABLE=yes
if [[ ! -f $METADATA_FINAL ]]; then
	log "$METADATA_FINAL is missing; checking the catalog for $TABLE"
	CURRENT_STATUS=0
	CURRENT="$(harness_local --extra aws table-metadata --table "$TABLE" \
		${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"})" || CURRENT_STATUS=$?
	if ((CURRENT_STATUS == 0)); then
		log "catalog metadata for $TABLE: $CURRENT"
		# Use a temporary file so declining confirmation leaves the run directory unchanged.
		METADATA_DOCUMENT="$(mktemp "${TMPDIR:-/tmp}/ingest-bench-metadata.XXXXXX")" ||
			die "could not create a temporary file for $TABLE's metadata"
		trap 'k8s_port_forward_stop; rm -f "$METADATA_DOCUMENT"' EXIT
		# Normalize compressed metadata through the same reader as teardown.
		k8s_fetch_metadata_document "$CURRENT" "$METADATA_DOCUMENT" ||
			die "could not fetch table metadata from $CURRENT; nothing is removed"
	elif ((CURRENT_STATUS == TABLE_ABSENT)); then
		HAVE_TABLE=no
	else
		die "could not check the catalog for $TABLE: table-metadata exited $CURRENT_STATUS; nothing is removed. See the error above. Restore catalog access and run scripts/teardown.sh, or correct the catalog properties in $SITE_FILE"
	fi
fi

LOCATION=""
if [[ $HAVE_TABLE == yes ]]; then
	# Delete the location declared by the table's metadata.
	LOCATION="$(jq -r '.location // empty' "$METADATA_DOCUMENT")"
	[[ -n $LOCATION ]] || die "$METADATA_DOCUMENT has no location; cannot determine the file prefix for $TABLE"

	# Require a location strictly below the warehouse root. Recursive deletion of the root or
	# a parent prefix would remove other tables or run data.
	WAREHOUSE="$(site_root '.warehouse')"
	WAREHOUSE="${WAREHOUSE%/}"
	[[ $LOCATION == "$WAREHOUSE"/* ]] ||
		die "$METADATA_DOCUMENT locates $TABLE outside the site warehouse $WAREHOUSE: '$LOCATION'; nothing is removed"
	# Strip all trailing separators before comparing roots.
	[[ ${LOCATION#"$WAREHOUSE"/} == *[!/]* ]] ||
		die "$METADATA_DOCUMENT locates $TABLE at the shared warehouse root $WAREHOUSE; nothing is removed"
fi

# ---------------------------------------------------------------------------
# Refuse while run resources can still read or write
# ---------------------------------------------------------------------------

# Refuse while the scorer can still read the table or write artifacts; deleting either
# would invalidate its measurements.
SCORER="$(scorer_job "$RUN_ID")"
STILL_RUNNING="$(k8s_object_present job "$SCORER")" ||
	die "cannot determine whether $RUN_ID is still being scored; nothing is removed. See the error above"
[[ -z $STILL_RUNNING ]] ||
	die "job/$SCORER is still in $SITE_NAMESPACE, so $RUN_ID is still being scored; run scripts/teardown.sh first"

# Return without prompting when there is nothing to delete.
if [[ $HAVE_TABLE == no && $ARTIFACTS == no ]]; then
	printf 'nothing to purge for %s: this catalog holds no %s. Use --artifacts to remove the run artifacts.\n' \
		"$RUN_ID" "$TABLE"
	exit 0
fi

# A failed teardown can remove the scorer while leaving writers behind. Check the
# actual resources, including orphaned pods, so manual teardown remains supported.
refuse_live_writer() {
	local kind=$1 name=$2 selector=$3 present pods
	present="$(k8s_object_present "$kind" "$name")" ||
		die "cannot determine whether $kind/$name is stopped; nothing is removed"
	[[ -z $present ]] ||
		die "$kind/$name is still in $SITE_NAMESPACE; run scripts/teardown.sh first"
	pods="$(kubectl --context "$KUBE_CONTEXT" --namespace "$SITE_NAMESPACE" get pod -l "$selector" -o name)" ||
		die "cannot check pods matching '$selector'; nothing is removed"
	[[ -z $pods ]] ||
		die "pods matching '$selector' are still in $SITE_NAMESPACE; run scripts/teardown.sh first"
}

for JOB in "$SCORER" "$(producer_job "$RUN_ID")" "$(seed_job "$RUN_ID")" "$(seed_wait_job "$RUN_ID")"; do
	refuse_live_writer job "$JOB" "job-name=$JOB"
done

SPEC="$RUN_DIR/spec.yaml"
[[ -f $SPEC ]] || die "no spec at $SPEC; cannot identify $RUN_ID's writers; nothing is removed"
ENGINE="$(yq -er '.engine' "$SPEC")" || die "$SPEC names no engine; nothing is removed"
if [[ $ENGINE != external ]]; then
	k8s_read_engine "$ENGINE" "$(k8s_object_name "$RUN_ID")"
	refuse_live_writer "$ENGINE_KIND" "$(k8s_object_name "$RUN_ID")" "$ENGINE_FLEET_SELECTOR"
fi
COMPACTION_DRIVER="$(yq -er '.compaction.driver // ""' "$SPEC")" ||
	die "cannot read the compaction driver from $SPEC; nothing is removed"
if [[ -n $COMPACTION_DRIVER && $COMPACTION_DRIVER != inline && $COMPACTION_DRIVER != external ]]; then
	[[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]] ||
		die "no compactor manifest at $RUN_DIR/$COMPACTOR_DOCUMENT_FILE; cannot identify its writers; nothing is removed"
fi
if [[ -f $RUN_DIR/$COMPACTOR_DOCUMENT_FILE ]]; then
	COMPACTOR_KIND="$(yq -er '.kind' "$RUN_DIR/$COMPACTOR_DOCUMENT_FILE")" ||
		die "cannot identify the compactor kind; nothing is removed"
	COMPACTOR_NAME="$(yq -er '.metadata.name' "$RUN_DIR/$COMPACTOR_DOCUMENT_FILE")" ||
		die "cannot identify the compactor name; nothing is removed"
	COMPACTOR_SELECTOR="$(k8s_compactor_selector "$RUN_DIR")" ||
		die "cannot identify the compactor pods; nothing is removed"
	refuse_live_writer "$COMPACTOR_KIND" "$COMPACTOR_NAME" "$COMPACTOR_SELECTOR"
fi

# ---------------------------------------------------------------------------
# Say what goes, then ask
# ---------------------------------------------------------------------------

printf 'purging %s permanently removes:\n' "$RUN_ID"
if [[ $HAVE_TABLE == yes ]]; then
	printf '  the table    %s\n' "$TABLE"
	printf '  its files    %s\n' "$LOCATION"
else
	printf '  no table:    this catalog holds no %s; only the run artifacts below will be removed\n' "$TABLE"
fi
if [[ $ARTIFACTS == yes ]]; then
	printf '  its run      %s/%s/\n' "$RUNS_ROOT" "$RUN_ID"
fi

if [[ $ASSUME_YES == no ]]; then
	confirm "remove all of the above?"
fi

# ---------------------------------------------------------------------------
# Remove it
# ---------------------------------------------------------------------------

if [[ $HAVE_TABLE == yes ]]; then
	# Drop the catalog entry before its files. drop-table tolerates absence so a partial
	# purge can be retried.
	log "dropping $TABLE"
	harness_local --extra aws drop-table --table "$TABLE" ${CATALOG_PROP_FLAGS[@]+"${CATALOG_PROP_FLAGS[@]}"}

	log "removing $LOCATION"
	aws s3 rm --recursive "$LOCATION" >&2 || die "could not remove $LOCATION; $TABLE is already out of the catalog"
fi

if [[ $ARTIFACTS == yes ]]; then
	log "removing $RUNS_ROOT/$RUN_ID/"
	aws s3 rm --recursive "$RUNS_ROOT/$RUN_ID/" >&2 || die "could not remove $RUNS_ROOT/$RUN_ID/"
fi

log "purged $RUN_ID; $RUN_DIR on this machine is untouched"
