#!/usr/bin/env bash
# SPDX-License-Identifier: CC-BY-NC-SA-4.0
# Generate a corpus in cluster Jobs, then merge shard metadata when needed. An upsert corpus
# then gets its seed table: sharded seed writers, then one commit. Pods write directly to the
# run's object store using their configured identity.
set -euo pipefail
PREREQ_DOC="deploy/aws/README.md"
# shellcheck source=scripts/_lib.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_lib.sh"
# shellcheck source=scripts/_k8s.sh
source "$(dirname -- "${BASH_SOURCE[0]}")/_k8s.sh"

# Generation, merge and seed wait timeouts; these do not limit the Jobs themselves. The
# seed commit only registers files, so it shares the merge's wait.
GEN_WAIT_S="${GEN_WAIT_S:-14400}"
MERGE_WAIT_S="${MERGE_WAIT_S:-1800}"
SEED_WAIT_S="${SEED_WAIT_S:-3600}"

usage() {
	cat <<'USAGE'
usage: scripts/gen-corpus.sh <preset> [options]

  <preset>           a shipped preset name, or a path to one inside the image
  --shards N         generate in N pods, then merge them (default: 1)
  --seed S           generator seed, included in the corpus name (default: 1)
  --site PATH        site config for the cluster and the corpus root (default: ./site.yaml)
  --image-tag TAG    the harness image tag to run (default: this checkout's commit)
  --seed-shards N    write an upsert corpus's seed in N pods (default: --shards)
  --seed-only URI    build the seed of an existing upsert corpus, without generating

Environment: GEN_WAIT_S, MERGE_WAIT_S, SEED_WAIT_S, GEN_MEMORY (also sizes seed pods).

Prints only the corpus URI to stdout.
USAGE
}

PRESET=""
SHARDS=1
SEED=1
IMAGE_TAG=""
SEED_SHARDS=""
SEED_ONLY=""
while [[ $# -gt 0 ]]; do
	case "$1" in
	--shards)
		SHARDS="${2:?--shards needs a count}"
		shift 2
		;;
	--seed)
		SEED="${2:?--seed needs an integer}"
		shift 2
		;;
	--site)
		SITE_FILE="${2:?--site needs a path}"
		shift 2
		;;
	--image-tag)
		IMAGE_TAG="${2:?--image-tag needs a tag}"
		shift 2
		;;
	--seed-shards)
		SEED_SHARDS="${2:?--seed-shards needs a count}"
		shift 2
		;;
	--seed-only)
		SEED_ONLY="${2:?--seed-only needs a corpus URI}"
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
		[[ -z $PRESET ]] || die "expected one preset; got '$PRESET' and '$1'"
		PRESET="$1"
		shift
		;;
	esac
done

if [[ -n $SEED_ONLY ]]; then
	[[ -z $PRESET ]] || die "--seed-only seeds an existing corpus; drop the preset '$PRESET'"
else
	[[ -n $PRESET ]] || {
		printf 'a preset is required\n\n' >&2
		usage >&2
		exit 2
	}
fi
[[ $SHARDS =~ ^[1-9][0-9]*$ ]] || die "--shards must be a positive integer, got '$SHARDS'"
[[ $SEED =~ ^[0-9]+$ ]] || die "--seed must be a non-negative integer, got '$SEED'"
SEED_SHARDS="${SEED_SHARDS:-$SHARDS}"
[[ $SEED_SHARDS =~ ^[1-9][0-9]*$ ]] || die "--seed-shards must be a positive integer, got '$SEED_SHARDS'"

# Presets in this checkout say whether they are upsert corpora; one only in the image cannot be read here.
PRESET_FILE="$REPO_ROOT/workloads/presets/$PRESET.yaml"
[[ -f $PRESET_FILE ]] || PRESET_FILE="$REPO_ROOT/$PRESET"

# Each shard generates whole batches, so sharding does not reduce per-pod memory.
# The standard corpus peaked at 7.8 GiB per batch in local generation.
case "$(basename -- "$PRESET_FILE" .yaml)" in
	events-1mrps-uniform | events-1mrps-skew | events200-1mrps-uniform | events200-1mrps-skew) GEN_MEMORY="${GEN_MEMORY:-10Gi}" ;;
	*) GEN_MEMORY="${GEN_MEMORY:-3Gi}" ;;
esac
UPSERT=no
if [[ -n $SEED_ONLY ]] || [[ -n $PRESET && -f $PRESET_FILE && $(yq '.upsert != null' "$PRESET_FILE") == true ]]; then
	UPSERT=yes
fi

require_host_tools kubectl yq git
# Sharded generation checks uploaded metadata before starting the merge, and seeding checks
# whether the corpus already published its seed.
[[ $SHARDS == 1 && $UPSERT == no ]] || require_host_tools aws

k8s_read_site
CORPUS_ROOT="$(site_root '.corpus_root')"
TAG="$(k8s_image_tag "$IMAGE_TAG")"
IMAGE="$REGISTRY/$IMAGE_REPOSITORY_PREFIX/harness:$TAG"

# Write the seed table of the upsert corpus at $1 in SEED_SHARDS pods, then register it as one
# snapshot. Seed files are immutable once seed.json exists, so a published seed is left alone.
build_seed() {
	local corpus=$1 name seed_job commit_job
	if aws s3 ls "$corpus/seed.json" >/dev/null 2>&1; then
		log "$corpus already published its seed table; leaving it unchanged"
		return 0
	fi
	name="$(k8s_object_name "$(basename -- "${PRESET:-$corpus}")")"
	seed_job="corpus-seed-$name"
	commit_job="corpus-seed-commit-$name"
	log "writing the seed table of $corpus in $SEED_SHARDS shard(s)"
	k8s_delete job "$seed_job"
	k8s_render_apply deploy/k8s/corpus-gen-job.yaml.tmpl \
		"NAME=$seed_job" \
		"NAMESPACE=$SITE_NAMESPACE" \
		"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
		"IMAGE=$IMAGE" \
		"COMMAND=$(job_command_json /bin/sh -c 'exec seed-corpus --corpus "$1" --shard-index "$JOB_COMPLETION_INDEX" --shard-count "$2"' -- "$corpus" "$SEED_SHARDS")" \
		"COUNT=$SEED_SHARDS" \
		"MEMORY=$GEN_MEMORY" \
		"ENV=$JOB_ENV" \
		"ENV_FROM=$JOB_ENV_FROM" \
		"NODE_SELECTOR=$NODE_SELECTOR" \
		"TOLERATIONS=$TOLERATIONS"
	k8s_wait_job "$seed_job" "$SEED_WAIT_S"
	log "registering the seed table"
	k8s_delete job "$commit_job"
	k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
		"NAME=$commit_job" \
		"NAMESPACE=$SITE_NAMESPACE" \
		"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
		"IMAGE=$IMAGE" \
		"COMMAND=$(job_command_json commit-seed --corpus "$corpus")" \
		"ENV=$JOB_ENV" \
		"ENV_FROM=$JOB_ENV_FROM" \
		"NODE_SELECTOR=$NODE_SELECTOR" \
		"TOLERATIONS=$TOLERATIONS"
	k8s_wait_job "$commit_job" "$MERGE_WAIT_S"
	log "seed table committed"
}

if [[ -n $SEED_ONLY ]]; then
	build_seed "${SEED_ONLY%/}"
	printf '%s\n' "${SEED_ONLY%/}"
	exit 0
fi

# Generation cost depends on the schema and encoding, not just encoded bytes.
if [[ -f $PRESET_FILE && $(yq -r '.value_encoding // "avro"' "$PRESET_FILE") == json ]]; then
	log "JSON generation time has not been measured; inspect gen-corpus --plan and set GEN_WAIT_S for the chosen shard count"
else
	log "generation time is not estimated for this preset; use a generation probe to set GEN_WAIT_S and --shards"
fi

# Read the generated URI from the harness log. Capture before awk to avoid an early pipe
# close causing SIGPIPE under pipefail.
wrote_uri() {
	local logs reported
	logs="$(k8s_job_logs "$1")" || die "could not read job/$1's log; try: kubectl logs job/$1"
	reported="$(awk '/^wrote /{print $2; exit}' <<<"$logs")"
	[[ -n $reported ]] || die "job/$1 printed no 'wrote' line; read its log with: kubectl logs job/$1"
	# The report separates the URI from its figures with a colon.
	printf '%s' "${reported%:}"
}

# ---------------------------------------------------------------------------
# Generate
# ---------------------------------------------------------------------------

if ((SHARDS == 1)); then
	GEN_COMMAND=(gen-corpus --preset "$PRESET" --out "$CORPUS_ROOT" --seed "$SEED")
else
	# Expand only the shard index in the pod; pass configuration as arguments.
	GEN_COMMAND=(/bin/sh -c 'root=$1; shift; exec gen-corpus --out "$root/shards/$JOB_COMPLETION_INDEX" --shard-index "$JOB_COMPLETION_INDEX" "$@"' --
		"$CORPUS_ROOT" --preset "$PRESET" --shard-count "$SHARDS" --seed "$SEED")
fi

# Include the preset in the Job name so different presets can generate concurrently.
GEN_JOB="corpus-gen-$(k8s_object_name "$(basename -- "$PRESET")")"
MERGE_JOB="corpus-merge-$(k8s_object_name "$(basename -- "$PRESET")")"

log "generating $PRESET in $SHARDS shard(s) with $IMAGE"
k8s_delete job "$GEN_JOB"
k8s_render_apply deploy/k8s/corpus-gen-job.yaml.tmpl \
	"NAME=$GEN_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${GEN_COMMAND[@]}")" \
	"COUNT=$SHARDS" \
	"MEMORY=$GEN_MEMORY" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$GEN_JOB" "$GEN_WAIT_S"

if ((SHARDS == 1)); then
	# Assign first so a failed command substitution reaches set -e instead of becoming a
	# successful printf of an empty string.
	CORPUS_URI="$(wrote_uri "$GEN_JOB")"
	log "corpus generated"
	[[ $UPSERT == no ]] || build_seed "$CORPUS_URI"
	printf '%s\n' "$CORPUS_URI"
	exit 0
fi

# ---------------------------------------------------------------------------
# Merge
# ---------------------------------------------------------------------------

# Read the corpus directory name from this generation's log; shard prefixes may also
# contain older corpora. Any indexed Job pod reports the same final directory name. Keep
# shard directories because merged metadata references their batches.
SHARD_DIR="$(wrote_uri "$GEN_JOB")"
SHARD_DIR="${SHARD_DIR##*/}"

# Check each shard's metadata before launching a merge pod. aws s3 ls fails when the path
# is absent.
MERGE_COMMAND=(merge-corpus)
shard=0
while ((shard < SHARDS)); do
	SHARD_CORPUS="$CORPUS_ROOT/shards/$shard/$SHARD_DIR"
	aws s3 ls "$SHARD_CORPUS/corpus.json" >/dev/null ||
		die "could not find corpus.json at $SHARD_CORPUS for shard $shard of $SHARDS; check storage access and read the job log: kubectl logs job/$GEN_JOB"
	MERGE_COMMAND+=("$SHARD_CORPUS")
	shard=$((shard + 1))
done
MERGE_COMMAND+=(--out "$CORPUS_ROOT")

log "merging $SHARDS shards of $SHARD_DIR"
k8s_delete job "$MERGE_JOB"
k8s_render_apply deploy/k8s/harness-job.yaml.tmpl \
	"NAME=$MERGE_JOB" \
	"NAMESPACE=$SITE_NAMESPACE" \
	"SERVICE_ACCOUNT=$SERVICE_ACCOUNT" \
	"IMAGE=$IMAGE" \
	"COMMAND=$(job_command_json "${MERGE_COMMAND[@]}")" \
	"ENV=$JOB_ENV" \
	"ENV_FROM=$JOB_ENV_FROM" \
	"NODE_SELECTOR=$NODE_SELECTOR" \
	"TOLERATIONS=$TOLERATIONS"
k8s_wait_job "$MERGE_JOB" "$MERGE_WAIT_S"

CORPUS_URI="$(wrote_uri "$MERGE_JOB")"
log "corpus merged"
[[ $UPSERT == no ]] || build_seed "$CORPUS_URI"
printf '%s\n' "$CORPUS_URI"
