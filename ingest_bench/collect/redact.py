# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Remove credentials and the site's private identifiers from published results.

Replace roots with named placeholders to preserve path relationships. Apply
redaction recursively to strings and mapping keys so newly added fields
receive the same treatment, and anywhere within a string, so a root or an
account ID quoted in notes is caught as well as a whole-value path.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import cast

from ingest_bench.collect.privacy import ACCOUNT_DIGITS, ACCOUNT_KEY, CLOUD_ACCOUNT_ID, OBJECT_STORE_URI, captured
from ingest_bench.specs.env import has_placeholder, names_a_secret
from ingest_bench.specs.model import SiteConfig

REDACTED = "<redacted>"
CATALOG_WAREHOUSE = "<catalog_warehouse>"
ACCOUNT_ID = "<account_id>"

# Userinfo in a URI (`scheme://user:secret@host`) carries credentials. It runs
# to the last `@` before the path, since a password may itself hold an `@`.
_URI_USERINFO = re.compile(r"(?<=://)[^/?#\s]*@")

# A name ends unless a word character, a hyphen or a dot that continues into
# another segment follows, so `images.invalid.` ends a sentence while
# `images.invalid.example` is a longer host.
_NAME_END = r"(?![\w-]|\.[\w-])"

# Twelve-digit runs inside a bucket name, the usual place a site's account ID appears.
_DIGIT_RUN = re.compile(r"(?<!\d)\d{12}(?!\d)")


def _warehouse_name(site: SiteConfig) -> str | None:
    """The catalog warehouse when it is a bare name, as a Lakekeeper REST catalog uses.

    Other catalog types take a storage location, even a one-segment relative path,
    and Glue identifies its warehouse by account ID. Both stay site roots. The
    catalog type follows PyIceberg: explicit in any case, or REST for an HTTP URI when empty.
    """
    props = site.catalog_props
    warehouse = props.get("warehouse")
    kind = props.get("type", "").lower()
    rest = kind == "rest" if kind else props.get("uri", "").startswith(("http://", "https://"))
    if warehouse is None or not rest or warehouse.isdigit() or "/" in warehouse or ":" in warehouse:
        return None
    return warehouse


def _roots(site: SiteConfig) -> list[tuple[str, str]]:
    """Pair site roots with placeholders, longest first to handle nested roots."""
    named = [("corpus_root", site.corpus_root), ("runs_root", site.runs_root), ("warehouse", site.warehouse)]
    # Image registries and Glue account IDs need explicit roots; neither is
    # necessarily under a configured storage path.
    if site.kubernetes is not None:
        named.append(("registry", site.kubernetes.registry))
    catalog_warehouse = site.catalog_props.get("warehouse")
    # A warehouse name is ordinary text that other values share, such as a label-key
    # prefix or a mapping key, so only the catalog properties redact it.
    if (
        catalog_warehouse is not None
        and catalog_warehouse != _warehouse_name(site)
        and catalog_warehouse.rstrip("/") not in {root.rstrip("/") for _, root in named}
    ):
        named.append(("catalog_warehouse", catalog_warehouse))
    return sorted(
        # An empty root would match every string.
        ((root.rstrip("/"), f"<{name}>") for name, root in named if root.strip("/")),
        key=lambda pair: len(pair[0]),
        reverse=True,
    )


def _account_ids(site: SiteConfig) -> frozenset[str]:
    """The AWS account IDs the site's settings identify as accounts.

    An ID counts where its context says so: an ARN or an AWS host, a digit run in
    a bucket name, or a catalog property labelled as an account or holding a
    Glue warehouse. Any other twelve-digit setting, such as a timestamp header,
    is not an ID and stays in place.
    """
    texts = list(site.catalog_props.values())
    if site.kubernetes is not None:
        texts.extend([site.kubernetes.registry, *site.kubernetes.service_account_annotations.values()])
    ids = {captured(match) for text in texts for match in CLOUD_ACCOUNT_ID.finditer(text)}
    ids.update(
        value
        for key, value in site.catalog_props.items()
        if (key == "warehouse" or ACCOUNT_KEY.fullmatch(key)) and ACCOUNT_DIGITS.fullmatch(value)
    )
    for root in (site.corpus_root, site.runs_root, site.warehouse, site.catalog_props.get("warehouse", "")):
        for match in OBJECT_STORE_URI.finditer(root):
            ids.update(_DIGIT_RUN.findall(captured(match)))
    return frozenset(ids)


@dataclass(frozen=True)
class _Patterns:
    """The site's private values, compiled once per document.

    Roots are matched in a single pass, longest first, so a shorter root never
    matches inside a longer root's placeholder. Account IDs follow, and no
    placeholder holds a digit for them to match.
    """

    roots: re.Pattern[str] | None
    placeholders: dict[str, str]
    ids: re.Pattern[str] | None


def _patterns(site: SiteConfig) -> _Patterns:
    roots = _roots(site)
    # A root ends where a path segment or a name ends, so `corpus` does not match
    # `corpus-archive`. A one-segment catalog warehouse, such as a relative
    # directory or an account ID, is private as the head of a path. As a bare
    # word it could be any key or value, so the catalog properties redact it instead.
    catalog_warehouse = site.catalog_props.get("warehouse", "").rstrip("/")
    alternatives = "|".join(
        re.escape(root) + ("(?=/)" if root == catalog_warehouse and "/" not in root else _NAME_END) for root, _ in roots
    )
    ids = sorted(_account_ids(site), key=len, reverse=True)
    return _Patterns(
        roots=re.compile(rf"(?<![\w.-])(?:{alternatives})") if roots else None,
        placeholders=dict(roots),
        ids=re.compile(rf"(?<!\d)(?:{'|'.join(map(re.escape, ids))})(?!\d)") if ids else None,
    )


def _redact_text(text: str, patterns: _Patterns) -> str:
    if patterns.roots is not None:
        text = patterns.roots.sub(lambda match: patterns.placeholders[match.group()], text)
    if patterns.ids is not None:
        text = patterns.ids.sub(ACCOUNT_ID, text)
    return _URI_USERINFO.sub(f"{REDACTED}@", text)


def redact_uri(text: str, site: SiteConfig) -> str:
    """Replace the site's roots and account IDs within ``text``, and credentials in any URI.

    Leave text outside the configured roots unchanged.
    """
    return _redact_text(text, _patterns(site))


# Share credential-name rules with configuration validation.
def redact_props(props: Mapping[str, str]) -> dict[str, str]:
    """Redact literal credentials, keeping environment references and URIs.

    Publication redacts site roots separately, through ``redact_document``.
    """
    return {
        key: REDACTED if names_a_secret(key) and not has_placeholder(value) else value for key, value in props.items()
    }


def redact_catalog_props(props: Mapping[str, str], site: SiteConfig) -> dict[str, str]:
    """Redact the catalog properties a result publishes.

    The catalog's own warehouse is redacted here when it is one segment, a
    name, an account ID or a relative directory, which the roots match only as
    the head of a path.
    """
    redacted = redact_props(props)
    warehouse = redacted.get("warehouse")
    if warehouse is not None and warehouse == site.catalog_props.get("warehouse") and "/" not in warehouse.rstrip("/"):
        redacted["warehouse"] = CATALOG_WAREHOUSE
    return redacted


def _redact_value(value: object, patterns: _Patterns) -> object:
    if isinstance(value, str):
        return _redact_text(value, patterns)
    if isinstance(value, dict):
        return {
            _redact_text(str(key), patterns): _redact_value(entry, patterns)
            for key, entry in cast(dict[object, object], value).items()
        }
    if isinstance(value, list):
        return [_redact_value(entry, patterns) for entry in cast(list[object], value)]
    return value


def redact_document(value: object, site: SiteConfig) -> object:
    """Recursively redact the site's private values in string values and mapping keys."""
    return _redact_value(value, _patterns(site))
