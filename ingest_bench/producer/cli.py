# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Command line for one producer shard.

All shards share the corpus, topic, and epoch; only ``--shard`` differs.
The explicit epoch keeps shard schedules aligned with the scorer.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from ingest_bench.catalog import parse_key_values
from ingest_bench.clock import SystemClock
from ingest_bench.producer.produce import FrameProducer, ProduceArgs, run
from ingest_bench.schema_registry import confluent_header
from ingest_bench.specs.env import resolve_env_placeholders
from ingest_bench.specs.model import (
    COMPRESSION_DEFAULT,
    COMPRESSIONS,
    PARTITION_ASSIGNMENT_KEY,
    PARTITION_ASSIGNMENTS,
    VALUE_ENCODING_AVRO,
    VALUE_ENCODING_CONFLUENT,
    VALUE_ENCODINGS,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="produce", description="Send a corpus into a Kafka topic on schedule.")
    parser.add_argument("--corpus", required=True, metavar="URI", help="the corpus directory to send")
    parser.add_argument("--bootstrap", required=True, metavar="HOST:PORT", help="Kafka bootstrap servers")
    parser.add_argument("--topic", required=True)
    parser.add_argument(
        "--epoch",
        required=True,
        type=float,
        metavar="UNIX_SECONDS",
        help="the run's time origin, shared by every shard and by the scorer",
    )
    parser.add_argument(
        "--speed", type=float, default=1.0, help="replay speed multiplier (1.0 preserves the corpus timeline)"
    )
    parser.add_argument("--shard", type=int, default=0)
    parser.add_argument("--shards", type=int, default=1)
    parser.add_argument("--seconds", type=int, help="send only the batches due in the first SECONDS of the corpus")
    parser.add_argument(
        "--seed",
        action="store_true",
        help="send the upsert corpus's Kafka seed, which the engine writes before the scored offer",
    )
    parser.add_argument("--key-column", metavar="NAME", help="a corpus key column to send as the Kafka message key")
    parser.add_argument(
        "--partition-assignment",
        choices=sorted(PARTITION_ASSIGNMENTS),
        default=PARTITION_ASSIGNMENT_KEY,
        help="key uses the client partitioner; round_robin cycles partitions independently of the message key",
    )
    parser.add_argument(
        "--partitions",
        type=int,
        metavar="N",
        help="topic partition count for round_robin: required, checked at startup, and fixed for the run. "
        "Ignored with key assignment",
    )
    parser.add_argument(
        "--value-encoding",
        choices=sorted(VALUE_ENCODINGS),
        default=VALUE_ENCODING_AVRO,
        help="value format: pre-generated JSON, raw Avro binary, or Confluent-framed Avro. "
        "Use the encoding recorded in the run's facts.json",
    )
    parser.add_argument(
        "--schema-id",
        type=int,
        metavar="N",
        help=f"schema registry ID from the run's facts.json. Required and allowed only with "
        f"--value-encoding {VALUE_ENCODING_CONFLUENT}",
    )
    parser.add_argument("--publish-log", required=True, metavar="PATH", help="where to write this shard's publish log")
    parser.add_argument(
        "--behind-max-ms", type=int, default=5000, help="report once the producer falls this far behind"
    )
    parser.add_argument(
        "--compression",
        choices=sorted(COMPRESSIONS),
        default=COMPRESSION_DEFAULT,
        help="batch compression codec (librdkafka compression.type). Use the codec in the run's facts.json; "
        "the consumer must support it",
    )
    parser.add_argument(
        "--upload-prefix", metavar="URI", help="copy the publish log under this prefix as it is written"
    )
    parser.add_argument(
        "--kafka-prop",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="librdkafka client property overriding producer defaults; repeat for multiple properties. "
        "Values of the form ${env:NAME} use environment variables. Set aws.region for Amazon MSK IAM "
        "authentication. Use --compression instead of compression.* properties",
    )
    return parser


def _value_prefix(encoding: str, schema_id: int | None) -> bytes:
    """Build the selected framing header, rejecting incompatible schema-ID flags."""
    if encoding == VALUE_ENCODING_CONFLUENT:
        if schema_id is None:
            raise ValueError(
                f"--value-encoding {VALUE_ENCODING_CONFLUENT} requires --schema-id; "
                "use schema_id from the run's facts.json"
            )
        return confluent_header(schema_id)
    if schema_id is not None:
        raise ValueError(f"--schema-id is only sent in the Confluent wire format, and --value-encoding is {encoding}")
    return b""


def main(argv: Sequence[str] | None = None) -> int:
    parsed = build_parser().parse_args(argv)
    args = ProduceArgs(
        corpus_uri=str(parsed.corpus),
        bootstrap=str(parsed.bootstrap),
        topic=str(parsed.topic),
        epoch_ms=round(float(parsed.epoch) * 1000),
        speed=float(parsed.speed),
        shard=int(parsed.shard),
        shards=int(parsed.shards),
        seconds=None if parsed.seconds is None else int(parsed.seconds),
        seed=bool(parsed.seed),
        key_column=None if parsed.key_column is None else str(parsed.key_column),
        partition_assignment=str(parsed.partition_assignment),
        partitions=None if parsed.partitions is None else int(parsed.partitions),
        value_encoding=str(parsed.value_encoding),
        value_prefix=_value_prefix(
            str(parsed.value_encoding), None if parsed.schema_id is None else int(parsed.schema_id)
        ),
        publish_log_path=Path(str(parsed.publish_log)),
        behind_max_ms=int(parsed.behind_max_ms),
        upload_prefix=None if parsed.upload_prefix is None else str(parsed.upload_prefix),
        compression=str(parsed.compression),
        kafka_props=resolve_env_placeholders(
            parse_key_values([str(prop) for prop in parsed.kafka_prop], "--kafka-prop")
        ),
    )
    return run(args, _confluent_producer, SystemClock(), sys.stdout)


def _confluent_producer(config: dict[str, object]) -> FrameProducer:
    # Imported here so the module can be loaded, and its arguments parsed, on a
    # machine without librdkafka.
    from confluent_kafka import Producer

    producer: FrameProducer = Producer(config)
    return producer


if __name__ == "__main__":
    raise SystemExit(main())
