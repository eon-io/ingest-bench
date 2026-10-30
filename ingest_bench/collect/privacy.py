# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Shared patterns for detecting private storage locations and account IDs.

Each pattern names a value by the context that makes it private. A value's
shape alone, such as twelve digits, cannot tell an account ID from a byte
count, so collection redacts the site's own identifiers wherever they appear
and these patterns catch the recognizable forms of anyone else's.
"""

from __future__ import annotations

import re

# An AWS account ID in an ARN's account field, an AWS endpoint host, an
# AWSLogs path, or a field or phrase labelled as an account.
CLOUD_ACCOUNT_ID = re.compile(
    r"arn:aws[\w-]*:[\w-]*:[\w-]*:(?P<arn>\d{12}):"
    r"|(?<![0-9A-Za-z])(?P<host>\d{12})\.[\w.-]*amazonaws\.com"
    r"|\bAWSLogs/(?P<logs>\d{12})(?![0-9A-Za-z])"
    r"|(?i:(?<![A-Za-z])(?:aws[_-]?)?(?:account|acct)(?:[_ -]?ids?)?(?![A-Za-z])|\bglue\.id\b)"
    r"[^0-9A-Za-z\n]{0,8}(?P<labelled>\d{12})(?![0-9A-Za-z])"
)
PLACEHOLDER_ACCOUNT_ID = "123456789012"

# A mapping key that labels its value as an account, as `account_id` or PyIceberg's `glue.id` does.
ACCOUNT_KEY = re.compile(r"(?i)(?:aws[_-]?)?(?:account|acct)(?:[_ -]?ids?)?|glue\.id")
ACCOUNT_DIGITS = re.compile(r"\d{12}")

# A bucket name in an s3://, s3a:// or gs:// URI, or in the HTTPS forms of S3
# (virtual-hosted and path-style) and GCS. Capture the complete authority so a
# placeholder-looking prefix cannot hide a real bucket suffix. Bare schemes are
# documentation, not storage locations.
# A bucket segment: an environment reference counts as one unit, so a literal
# suffix after it still belongs to the captured authority.
_BUCKET = r"(?:\$\{env:[A-Za-z_]\w*\}|[^/\s\"'`,)\]:\\?#])+"
OBJECT_STORE_URI = re.compile(
    rf"(?:s3a?|gs)://(?P<scheme>{_BUCKET})"
    r"|https?://(?P<s3_host>[a-z0-9][a-z0-9.-]*?)\.s3(?:[.-][a-z0-9-]+)*\.amazonaws\.com"
    rf"|https?://s3(?:[.-][a-z0-9-]+)*\.amazonaws\.com(?::\d+)?/(?P<s3_path>{_BUCKET})"
    r"|https?://(?P<gcs_host>[a-z0-9][a-z0-9._-]*?)\.storage\.googleapis\.com"
    # The JSON API names the bucket after /storage/v1/b/ on any of its hosts, so try it before path style.
    r"|https?://(?:www|storage|content-storage)\.googleapis\.com(?::\d+)?/(?:(?:download|upload)/)?storage/v1/b/"
    rf"(?P<gcs_api>{_BUCKET})"
    rf"|https?://storage\.(?:googleapis|cloud\.google)\.com(?::\d+)?/(?P<gcs_path>{_BUCKET})"
)

URI_AUTHORITY_PLACEHOLDER = re.compile(
    r"(?:\$[A-Za-z_]\w*|\$\{[A-Za-z_]\w*\}|\$\{env:[A-Za-z_]\w*\}|<[^>]+>|[A-Z][A-Z0-9_]*)"
)


def captured(match: re.Match[str]) -> str:
    """The value a match captured, whichever of its pattern's named contexts matched."""
    return next(value for value in match.groupdict().values() if value is not None)
