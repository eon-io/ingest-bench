# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Normalize harness catalog and Kafka properties for the Java engines."""

from ingest_bench.kafka_auth import MECHANISM_KEY, REGION_KEY, refuse_java_oauth

# A harness-created table names no Parquet codec, so Iceberg's Java writers fall back to gzip.
# An upsert reference table names zstd level 1 instead, so every engine's files share one codec.
ZSTD_LEVEL_1 = {"write.parquet.compression-codec": "zstd", "write.parquet.compression-level": "1"}

_CATALOG_PROP_RENAMES = {"s3.region": "client.region"}
_FILE_IO_BY_SCHEME = {
    "s3://": "org.apache.iceberg.aws.s3.S3FileIO",
    "gs://": "org.apache.iceberg.gcp.gcs.GCSFileIO",
}
# FileIO and AWS client settings, as opposed to settings for a particular catalog implementation.
_STORAGE_PREFIXES = ("s3.", "gcs.", "adls.", "client.")
_MSK_IAM_PROPS = {
    "security.protocol": "SASL_SSL",
    MECHANISM_KEY: "AWS_MSK_IAM",
    "sasl.jaas.config": "software.amazon.msk.auth.iam.IAMLoginModule required;",
    "sasl.client.callback.handler.class": "software.amazon.msk.auth.iam.IAMClientCallbackHandler",
}


def catalog_properties(props: dict[str, str], warehouse: str, *, engine: str) -> dict[str, str]:
    """Translate REST catalog properties, letting explicit values override defaults."""
    if "type" in props and props["type"] != "rest":
        raise ValueError(
            f"{engine} requires an Iceberg REST catalog; site.catalog.props specifies type {props['type']!r}"
        )
    for key in ("uri", "warehouse"):
        if key not in props:
            raise ValueError(f"site.catalog.props must set {key!r}: a {engine} run addresses its table through it")
    normalized = {"uri": props["uri"], "warehouse": props["warehouse"]}
    # Glue's catalog warehouse may be an account ID; FileIO follows the storage URI.
    for scheme, implementation in _FILE_IO_BY_SCHEME.items():
        if warehouse.startswith(scheme):
            normalized["io-impl"] = implementation
            break
    for key in sorted(props.keys() - {"type", "uri", "warehouse"}):
        normalized[_CATALOG_PROP_RENAMES.get(key, key)] = props[key]
    return normalized


def storage_properties(props: dict[str, str], warehouse: str) -> dict[str, str]:
    """FileIO settings for reading a table's files without the catalog that wrote them."""
    normalized = {"warehouse": warehouse}
    for scheme, implementation in _FILE_IO_BY_SCHEME.items():
        if warehouse.startswith(scheme):
            normalized["io-impl"] = implementation
            break
    for key in sorted(props):
        renamed = _CATALOG_PROP_RENAMES.get(key, key)
        if renamed.startswith(_STORAGE_PREFIXES):
            normalized[renamed] = props[key]
    return normalized


def kafka_properties(security: dict[str, str]) -> dict[str, str]:
    """Translate the harness's MSK IAM signal while preserving unrelated settings."""
    refuse_java_oauth(security)
    if security.get(MECHANISM_KEY) != "OAUTHBEARER" or REGION_KEY not in security:
        return dict(security)
    # Java's IAM module uses pod credentials and AWS_REGION, not the harness region key.
    return {
        **_MSK_IAM_PROPS,
        **{key: value for key, value in security.items() if key not in _MSK_IAM_PROPS and key != REGION_KEY},
    }
