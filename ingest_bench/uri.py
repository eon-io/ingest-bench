# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Access local paths and object stores through a shared fsspec interface."""

from __future__ import annotations

import os
import re

import fsspec
from fsspec.implementations.local import LocalFileSystem

_REMOTE_SCHEMES = ("s3://", "gs://")
_LOCAL_SCHEME = "file://"
_ANY_SCHEME = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*://")


def is_remote(uri: str) -> bool:
    return uri.startswith(_REMOTE_SCHEMES)


def filesystem_for(uri: str) -> tuple[fsspec.AbstractFileSystem, str]:
    """Return the filesystem and backend path for a supported URI.

    Use ``AWS_ENDPOINT_URL`` for S3-compatible stores. Reject unknown schemes
    instead of treating them as local paths.
    """
    if uri.startswith("s3://"):
        # Disable cached listings so the scorer sees new shard logs and completion.
        kwargs: dict[str, object] = {"use_listings_cache": False}
        endpoint = os.environ.get("AWS_ENDPOINT_URL")
        # Forward AWS_REGION for botocore, which otherwise reads AWS_DEFAULT_REGION.
        # This keeps signing regions consistent with the other clients.
        region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
        if endpoint or region:
            client_kwargs: dict[str, str] = {}
            if endpoint:
                client_kwargs["endpoint_url"] = endpoint
            if region:
                client_kwargs["region_name"] = region
            kwargs["client_kwargs"] = client_kwargs
        return fsspec.filesystem("s3", **kwargs), uri[len("s3://") :]
    if uri.startswith("gs://"):
        return fsspec.filesystem("gcs", use_listings_cache=False), uri[len("gs://") :]
    path = uri[len(_LOCAL_SCHEME) :] if uri.startswith(_LOCAL_SCHEME) else uri
    if _ANY_SCHEME.match(path):
        raise ValueError(f"unsupported URI scheme in {uri!r}")
    return LocalFileSystem(auto_mkdir=True), path


def join(uri: str, *parts: str) -> str:
    return "/".join([uri.rstrip("/"), *(part.strip("/") for part in parts)])


def write_bytes(uri: str, data: bytes) -> None:
    fs, path = filesystem_for(uri)
    # Create directories only for local filesystems; object stores need no mkdir.
    if not is_remote(uri):
        fs.makedirs(os.path.dirname(path), exist_ok=True)
    with fs.open(path, "wb") as handle:
        handle.write(data)


def read_bytes(uri: str) -> bytes:
    """Read the current object in one request.

    Avoid buffered range reads: concurrently replaced publish logs can invalidate
    their pinned ETags and cause ``FileExpired`` errors.
    """
    fs, path = filesystem_for(uri)
    return bytes(fs.cat_file(path))


def write_text(uri: str, text: str) -> None:
    write_bytes(uri, text.encode("utf-8"))


def read_text(uri: str) -> str:
    return read_bytes(uri).decode("utf-8")


def exists(uri: str) -> bool:
    fs, path = filesystem_for(uri)
    return bool(fs.exists(path))


def listdir(uri: str) -> list[str]:
    """The entry names directly under ``uri``, without their prefix."""
    fs, path = filesystem_for(uri)
    return sorted(str(entry).rsplit("/", 1)[-1] for entry in fs.ls(path, detail=False))
