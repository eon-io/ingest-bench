# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Encode corpus rows as plain JSON for Kafka JSON readers."""

from __future__ import annotations

import json

from ingest_bench.corpus.columns import ColumnDistribution, json_converter


def encode_records(rows: list[dict[str, object]], columns: tuple[ColumnDistribution, ...]) -> list[bytes]:
    """Represent each value as its column kind declares, without Avro union wrappers."""
    converters = {column.name: json_converter(column) for column in columns}
    records = []
    for row in rows:
        value = {}
        for name, cell in row.items():
            convert = converters.get(name)
            value[name] = cell if convert is None else convert(cell)
        records.append(json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8"))
    return records
