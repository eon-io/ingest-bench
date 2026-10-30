# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Create a run's topic and table, then write its engine configuration.

Staging writes ``facts.json`` and either creates the table or supplies its DDL.
External engines can use these facts without a harness-specific integration.
Validate local inputs before creating the topic, and delete the topic if a
later step fails so staging can be retried.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol, cast

import yaml

from ingest_bench import kafka_admin, uri
from ingest_bench.catalog import table_identifier
from ingest_bench.collect.redact import redact_props
from ingest_bench.collect.run_json import compaction_fleet, run_fleet, usd_per_hour
from ingest_bench.corpus import metadata
from ingest_bench.corpus.upsert import SEED_KAFKA
from ingest_bench.kafka_auth import refuse_java_oauth
from ingest_bench.producer.pacing import scheduled_ms, select_batches
from ingest_bench.schema_registry import register_schema, subject_for
from ingest_bench.specs import derive as derive_module
from ingest_bench.specs import model
from ingest_bench.specs.compactors import knobs_for as compactor_knobs_for
from ingest_bench.specs.derive import Derived
from ingest_bench.specs.engines import knobs_for, kubernetes_for
from ingest_bench.specs.env import resolve_env_placeholders
from ingest_bench.specs.kubernetes import object_name
from ingest_bench.table.create import (
    DEFAULT_SEED_PARTITION,
    create_table,
    create_upsert_table,
    parse_partition,
    register_seed_table,
)
from ingest_bench.table.ddl import spark_sql_ddl
from ingest_bench.workloads import WORKLOAD_ALIASES, canonical_workload_name, workloads_dir

# Use up to three replicas, capped by the broker count.
MAX_REPLICATION_FACTOR = 3

STAGED = "staged"

# Use the full configuration path in credential errors.
_REGISTRY_AUTH_KEY = "site.kafka.schema_registry.basic_auth_user_info"

# Use sortable UTC timestamps with seconds precision.
_TIMELINE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# Images record the commit they were built from, since their build context has
# no Git history. Publication requires a clean commit, so a tree that differed
# from it is marked.
HARNESS_COMMIT_ENV = "INGEST_BENCH_COMMIT"
DIRTY_SUFFIX = "-dirty"


class KafkaAdmin(Protocol):
    """Kafka operations required by staging, injectable for tests.

    Methods receive client properties because staging loads the site and resolves
    credentials before calling the admin.
    """

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool: ...

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int: ...

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None: ...

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None: ...


class ClusterAdmin:
    """Kafka admin backed by the configured cluster."""

    def exists(self, bootstrap: str, name: str, client: dict[str, str]) -> bool:
        return kafka_admin.topic_exists(bootstrap, name, client)

    def broker_count(self, bootstrap: str, client: dict[str, str]) -> int:
        return kafka_admin.broker_count(bootstrap, client)

    def create(
        self,
        bootstrap: str,
        name: str,
        partitions: int,
        replication_factor: int,
        topic_config: dict[str, str],
        client: dict[str, str],
    ) -> None:
        kafka_admin.create_topic(bootstrap, name, partitions, replication_factor, topic_config, client)

    def delete(self, bootstrap: str, name: str, client: dict[str, str]) -> None:
        kafka_admin.delete_topic(bootstrap, name, client)


@dataclass(frozen=True)
class SchemaRegistration:
    """Registered schema used by every record in a Confluent-framed run.

    The producer writes this schema ID into each record header.
    """

    url: str
    subject: str
    schema_id: int


@dataclass(frozen=True)
class Staged:
    """Validated spec, derived names, run directory, and generated facts."""

    spec: model.RunSpec
    derived: Derived
    run_dir: Path
    facts: dict[str, object]


def resolve_corpus_dir(corpus_root: str, name: str) -> str:
    """Find the unique corpus named ``name`` under ``corpus_root``.

    Directories include the preset hash, so several versions may coexist.
    Reject ambiguous matches rather than silently choosing a scoring input.
    """
    canonical = canonical_workload_name(name)
    names = {canonical, *(legacy for legacy, current in WORKLOAD_ALIASES.items() if current == canonical)}
    candidates = [
        d
        for d in uri.listdir(corpus_root)
        if any(d.startswith(f"{label}-") for label in names) and uri.exists(uri.join(corpus_root, d, "corpus.json"))
    ]
    matches = [
        d
        for d in candidates
        if cast(dict[str, object], json.loads(uri.read_text(uri.join(corpus_root, d, "corpus.json"))))["name"] in names
    ]
    if len(matches) != 1:
        raise ValueError(f"expected exactly one corpus named {name!r} under {corpus_root}, found {matches}")
    return matches[0]


def redact(props: dict[str, str]) -> dict[str, str]:
    """Redact literal credentials while preserving configuration URIs.

    Engines read these properties from ``facts.json``. Publication also redacts
    site roots; see ``collect.redact``.
    """
    return redact_props(props)


def replication_factor(brokers: int) -> int:
    return min(MAX_REPLICATION_FACTOR, brokers)


def upsert_seed(spec: model.RunSpec, meta: metadata.CorpusMetadata) -> dict[str, object] | None:
    """Check an upsert run's settings against its corpus and return the seed facts it starts from.

    Upsert scoring needs the corpus layout end to end: Kafka partitions as the corpus stored them,
    the seed table's partitioning, and a full offer whose final table the corpus publishes.
    """
    upsert_run = spec.scoring.exactness == model.LATEST_PER_KEY
    upsert_corpus = meta.schedule is not None
    if upsert_run != upsert_corpus:
        raise ValueError(
            f"corpus {meta.name!r} is {'an upsert' if upsert_corpus else 'an append-only'} corpus, so "
            f"spec.scoring.exactness must {'be' if upsert_corpus else 'not be'} {model.LATEST_PER_KEY}"
        )
    if (spec.kafka.partition_assignment == model.PARTITION_ASSIGNMENT_CORPUS) != upsert_corpus:
        raise ValueError(
            f"spec.kafka.partition_assignment {model.PARTITION_ASSIGNMENT_CORPUS!r} goes with upsert corpora only"
        )
    if meta.schedule is None:
        if spec.table.format_version != 2:
            raise ValueError("spec.table.format_version 3 is supported for upsert runs only")
        return None
    expected = {
        "spec.table.managed_by": (spec.table.managed_by, model.HARNESS),
        "spec.table.partition": (spec.table.partition, DEFAULT_SEED_PARTITION),
        "spec.kafka.partitions": (spec.kafka.partitions, meta.schedule.config.kafka_partitions),
        "spec.kafka.key": (spec.kafka.key, meta.schedule.key_column),
    }
    wrong = [
        f"{name} is {found!r}, and an upsert run needs {needed!r}"
        for name, (found, needed) in expected.items()
        if found != needed
    ]
    if wrong:
        raise ValueError("; ".join(wrong))
    seed_uri = uri.join(meta.uri, "seed.json")
    if not uri.exists(seed_uri):
        raise ValueError(f"{seed_uri} is missing; build the seed with seed-corpus and commit-seed first")
    return cast(dict[str, object], json.loads(uri.read_text(seed_uri)))


def harness_table_properties(spec: model.RunSpec) -> dict[str, str]:
    """Return table properties with Iceberg format version 2 enforced.

    Equality and position deletes require version 2. Engine-owned tables receive
    the spec's properties through their DDL instead.
    """
    return {**spec.table.properties, "format-version": "2"}


def harness_commit() -> str | None:
    """The harness commit staging this run, or ``None`` when neither the image nor Git records one."""
    recorded = os.environ.get(HARNESS_COMMIT_ENV)
    if recorded:
        return recorded
    checkout = Path(__file__).resolve().parents[1]
    try:
        commit = subprocess.run(
            ["git", "-C", str(checkout), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
        ).stdout.strip()
        changes = subprocess.run(
            ["git", "-C", str(checkout), "status", "--porcelain"], capture_output=True, text=True, check=True
        ).stdout
    except (OSError, subprocess.CalledProcessError):
        # An installed package has no checkout to ask, and publication rejects the missing commit.
        return None
    return commit + (DIRTY_SUFFIX if changes.strip() else "")


def _facts(
    spec: model.RunSpec,
    site: model.SiteConfig,
    derived: Derived,
    ddl: str | None,
    registration: SchemaRegistration | None,
) -> dict[str, object]:
    """Build engine configuration facts in their printed order.

    Keep ``run_id`` first for shell readers. Leave ``epoch`` null until launch so
    staging delays do not count toward the run. Always include ``value_encoding``
    and ``compression``; raw Avro and JSON runs have null registry fields.
    """
    return {
        "run_id": derived.run_id,
        "harness_commit": harness_commit(),
        "track": spec.track,
        "bootstrap": site.kafka_bootstrap,
        "topic": derived.topic,
        "corpus_uri": derived.corpus_uri,
        "schema_avsc_uri": uri.join(derived.corpus_uri, "schema.avsc"),
        "value_encoding": spec.kafka.value_encoding,
        "schema_registry_url": None if registration is None else registration.url,
        "schema_subject": None if registration is None else registration.subject,
        "schema_id": None if registration is None else registration.schema_id,
        "compression": spec.producer.compression,
        "catalog_props": redact(site.catalog_props),
        "table": derived.table,
        "partition": spec.table.partition,
        "ddl": ddl,
        "key_column": None if spec.kafka.key == model.KEY_NONE else spec.kafka.key,
        "partition_assignment": spec.kafka.partition_assignment,
        "kafka_partitions": spec.kafka.partitions,
        "epoch": None,
    }


def _refuse_an_unaddressable_name(spec: model.RunSpec, derived: Derived) -> None:
    """Reject object names that exceed the engine operator's declared limit.

    Check before creating resources to avoid staging a run that cannot launch.
    """
    limit = kubernetes_for(spec.engine).max_object_name_length
    if limit is None:
        return
    name = object_name(derived.run_id)
    if len(name) <= limit:
        return
    raise ValueError(
        f"spec.name {spec.name!r} is {len(spec.name)} characters, and a {spec.engine!r} run's object name is that "
        f"plus the run's stamp — {len(name)} characters, past the {limit} its operator accepts. "
        f"Name this run in at most {limit - (len(name) - len(spec.name))} characters"
    )


def _registry_for(spec: model.RunSpec, site: model.SiteConfig) -> model.SchemaRegistryConfig | None:
    """Return the registry for Confluent framing, or ``None`` for raw Avro or JSON.

    Reject a missing required registry before creating cluster resources.
    """
    if spec.kafka.value_encoding != model.VALUE_ENCODING_CONFLUENT:
        return None
    if site.schema_registry is None:
        raise ValueError(
            f"spec.kafka.value_encoding is {model.VALUE_ENCODING_CONFLUENT!r}, which registers the corpus's "
            "schema, and the site declares no kafka.schema_registry.url to register it with"
        )
    return site.schema_registry


def _registry_auth(registry: model.SchemaRegistryConfig) -> str | None:
    if registry.basic_auth_user_info is None:
        return None
    return resolve_env_placeholders({_REGISTRY_AUTH_KEY: registry.basic_auth_user_info})[_REGISTRY_AUTH_KEY]


def timeline_line(event: str) -> str:
    return f"{datetime.now(UTC).strftime(_TIMELINE_TIME_FORMAT)} {event}"


def facts_lines(facts: dict[str, object]) -> list[str]:
    """Format facts as one ``key: value`` line each.

    Encode mappings, multiline strings, and other non-string values as JSON so
    shell readers can parse one fact per line.
    """
    lines: list[str] = []
    for key, value in facts.items():
        scalar = isinstance(value, str) and "\n" not in value
        lines.append(f"{key}: {value if scalar else json.dumps(value)}")
    return lines


def publish_run_dir(run_dir: Path, upload_prefix: str, run_id: str) -> None:
    """Copy the run directory to ``<upload_prefix>/<run_id>/stage/``.

    This preserves artifacts from staging Jobs after their pods are removed.
    """
    for path in sorted(run_dir.iterdir()):
        uri.write_bytes(uri.join(upload_prefix, run_id, "stage", path.name), path.read_bytes())


def stage(
    spec_path: Path,
    site_path: Path,
    runs_dir: Path,
    admin: KafkaAdmin,
    *,
    stamp: str | None = None,
    image_tag: str | None = None,
    upload_prefix: str | None = None,
) -> Staged:
    """Create the topic and table and write the run directory.

    Validate local inputs before modifying cluster resources.
    """
    site = model.load_site(site_path)
    spec = model.load_run_spec(spec_path)
    if site.kubernetes is not None and site.pricing_machine_types:
        # Reject incomplete price maps before creating billable cluster resources.
        usd_per_hour(run_fleet(spec) + compaction_fleet(spec), site)
    if not spec.is_external():
        refuse_java_oauth(site.kafka_security)
    derived = derive_module.derive(
        spec, site, stamp=stamp, corpus_dir=resolve_corpus_dir(site.corpus_root, spec.corpus)
    )
    meta = metadata.read(derived.corpus_uri)
    meta.validate_value_encoding(spec.kafka.value_encoding)
    if spec.scoring.mode == model.SATURATION:
        assert spec.scoring.measurement_s is not None
        duration_s = min(meta.duration_s, spec.producer.seconds or meta.duration_s) / spec.producer.speed
        if spec.scoring.warmup_s + spec.scoring.measurement_s >= duration_s:
            raise ValueError("saturation warmup_s + measurement_s must end before the selected replay duration")
        batches = select_batches(metadata.read_manifest(meta.uri), 0, 1, spec.producer.seconds)
        last_offer_ms = max((scheduled_ms(0, batch.offset_ms, spec.producer.speed) for batch in batches), default=0)
        if (spec.scoring.warmup_s + spec.scoring.measurement_s) * 1000 >= last_offer_ms:
            raise ValueError("saturation measurement must end before the last scheduled batch")
    if spec.kafka.key != model.KEY_NONE and spec.kafka.key not in meta.key_columns:
        raise ValueError(
            f"spec.kafka.key {spec.kafka.key!r} is not a key column of corpus {meta.name!r}; "
            f"it publishes {list(meta.key_columns)} and accepts {model.KEY_NONE!r}"
        )
    partition = parse_partition(spec.table.partition)
    seed = upsert_seed(spec, meta)
    registry = _registry_for(spec, site)
    knobs = None
    if not spec.is_external():
        knobs = knobs_for(spec.engine)
        knobs.validate(spec.engine_block, spec, meta)
        _refuse_an_unaddressable_name(spec, derived)
    # The compaction layer is separate from the engine: a run with an external
    # engine can still have a sidecar the harness renders and starts.
    compactor = None
    if spec.compaction is not None and spec.compaction.is_managed():
        compactor = compactor_knobs_for(spec.compaction.driver)
        compactor.validate(spec.compaction.driver_block, spec, meta)
    # Require the image tag before creating resources.
    if site.kubernetes is not None and image_tag is None and (knobs is not None or compactor is not None):
        raise ValueError("a run that starts an image on a cluster needs --image-tag: the tag push-images.sh pushed")

    # Resolve credentials only for client calls; keep placeholders in artifacts.
    # Resolve all references before creating resources so missing variables fail early.
    kafka_client = resolve_env_placeholders(site.kafka_security)
    catalog_props = resolve_env_placeholders(site.catalog_props)
    registry_auth = None if registry is None else _registry_auth(registry)
    if admin.exists(site.kafka_bootstrap, derived.topic, kafka_client):
        raise ValueError(f"topic {derived.topic!r} already exists on {site.kafka_bootstrap}; it holds another run")
    admin.create(
        site.kafka_bootstrap,
        derived.topic,
        spec.kafka.partitions,
        replication_factor(admin.broker_count(site.kafka_bootstrap, kafka_client)),
        dict(kafka_admin.DEFAULT_TOPIC_CONFIG),
        kafka_client,
    )
    try:
        ddl: str | None = None
        if spec.table.managed_by == model.HARNESS:
            # Use the storage warehouse for explicit locations. Glue REST catalogs use
            # the catalog `warehouse` property for an account ID, not a storage path.
            namespace, table_name = table_identifier(derived.table)
            if seed is not None and seed.get("delivery") == SEED_KAFKA:
                create_upsert_table(
                    catalog_props,
                    derived.table,
                    meta,
                    partition,
                    spec.table.properties,
                    location=uri.join(site.warehouse, namespace, table_name),
                    namespace_location=uri.join(site.warehouse, namespace),
                    format_version=spec.table.format_version,
                )
            elif seed is None:
                create_table(
                    catalog_props,
                    derived.table,
                    meta,
                    partition,
                    harness_table_properties(spec),
                    location=uri.join(site.warehouse, namespace, table_name),
                    namespace_location=uri.join(site.warehouse, namespace),
                )
            else:
                register_seed_table(
                    catalog_props,
                    derived.table,
                    meta,
                    str(seed["metadata_location"]),
                    spec.table.properties,
                    location=uri.join(site.warehouse, namespace, table_name),
                    namespace_location=uri.join(site.warehouse, namespace),
                    format_version=spec.table.format_version,
                )
        else:
            ddl = spark_sql_ddl(meta, derived.table, partition, spec.table.properties)
        registration = None
        if registry is not None:
            # Register the same schema file that `schema_avsc_uri` gives consumers.
            subject = subject_for(derived.topic)
            schema_id = register_schema(
                registry.url, registry_auth, subject, uri.read_text(uri.join(derived.corpus_uri, "schema.avsc"))
            )
            registration = SchemaRegistration(url=registry.url, subject=subject, schema_id=schema_id)
        facts = _facts(spec, site, derived, ddl, registration)
        if seed is not None and meta.schedule is not None:
            facts["upsert"] = {
                "key_columns": [meta.schedule.key_column, "partition_key"],
                "version_column": "id",
                "seed_rows": seed["rows"],
                # The engine writes a Kafka seed itself, before the scored offer; a table seed is registered.
                **(
                    {"seed_delivery": SEED_KAFKA}
                    if seed.get("delivery") == SEED_KAFKA
                    else {"seed_snapshot_id": seed["snapshot_id"]}
                ),
            }
        run_dir = runs_dir / derived.run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        # Preserve comments and omitted defaults in the staged spec. Write a track
        # spec with the track's settings filled in, so the staged copy and the
        # result that embeds it stay self-contained if the track later changes.
        spec_text = spec_path.read_text()
        if spec.track is not None:
            expanded = model.expand_track(cast(dict[str, object], yaml.safe_load(spec_text)), workloads_dir(None))
            spec_text = yaml.safe_dump(expanded, sort_keys=False)
        (run_dir / "spec.yaml").write_text(spec_text)
        (run_dir / "facts.json").write_text(json.dumps(facts, indent=2) + "\n")
        (run_dir / "timeline.log").write_text(f"{timeline_line(STAGED)}\n")
        for renderer in (knobs, compactor):
            if renderer is None:
                continue
            rendered = renderer.render(spec, site, derived, meta, image_tag=image_tag)
            for filename, content in cast(dict[str, str], rendered).items():
                (run_dir / filename).write_text(content)
        if upload_prefix is not None:
            publish_run_dir(run_dir, upload_prefix, derived.run_id)
    except BaseException:
        # Delete the topic on errors and interrupts so staging can be retried.
        # Leave any created table for explicit teardown to avoid unintended deletion.
        admin.delete(site.kafka_bootstrap, derived.topic, kafka_client)
        raise
    return Staged(spec=spec, derived=derived, run_dir=run_dir, facts=facts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="stage", description="Prepare a run's topic, table, and configuration files.")
    parser.add_argument("--spec", required=True, metavar="PATH", help="the run spec to stage")
    parser.add_argument(
        "--site", required=True, metavar="PATH", help="the site config naming storage, broker and catalog"
    )
    parser.add_argument("--runs-dir", default="./runs", metavar="DIR", help="where the run directory is written")
    parser.add_argument(
        "--stamp",
        metavar="YYYYmmddTHHMMSSZ",
        help="timestamp to reuse when staging a run with its original ID",
    )
    parser.add_argument(
        "--image-tag",
        metavar="TAG",
        help="image tag published by push-images.sh. Required when the site declares a Kubernetes cluster",
    )
    parser.add_argument(
        "--upload-prefix",
        metavar="URI",
        help="upload the run directory to <URI>/<run id>/stage/ so it survives the staging pod's deletion",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    staged = stage(
        Path(str(args.spec)),
        Path(str(args.site)),
        Path(str(args.runs_dir)),
        ClusterAdmin(),
        stamp=None if args.stamp is None else str(args.stamp),
        image_tag=None if args.image_tag is None else str(args.image_tag),
        upload_prefix=None if args.upload_prefix is None else str(args.upload_prefix),
    )
    for line in facts_lines(staged.facts):
        print(line)
    print(f"run_dir: {staged.run_dir}")
    if staged.spec.is_external():
        print("Start your engine now; run launch when it is consuming.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
