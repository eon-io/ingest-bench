# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Build Kafka client properties and supply Amazon MSK IAM tokens.

Pass site security properties through, except ``aws.region``: the harness uses
that key to sign MSK tokens and removes it before configuring librdkafka.
For OAUTHBEARER, install a token callback unless the site supplies its own
``sasl.oauthbearer.*`` configuration.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping

# Harness-only region key, removed before configuring librdkafka.
REGION_KEY = "aws.region"

MECHANISM_KEY = "sasl.mechanism"

# Reject the plural alias: harness readers and Java renderers consistently
# use the singular key, even though librdkafka accepts both.
MECHANISM_ALIAS = "sasl.mechanisms"

_OAUTHBEARER = "OAUTHBEARER"
_OAUTHBEARER_PREFIX = "sasl.oauthbearer."

# Map a region to a token and its expiry in Unix milliseconds.
TokenProvider = Callable[[str], tuple[str, int]]

# librdkafka hands the callback the value of `sasl.oauthbearer.config`, which is
# None when no such property is set, and reads the expiry back in seconds.
OauthCallback = Callable[[str | None], tuple[str, float]]


def msk_token_provider() -> TokenProvider:
    """Load the optional Amazon MSK IAM signer when authentication needs it."""
    try:
        from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
    except ImportError as error:
        raise ValueError(
            f"{MECHANISM_KEY}={_OAUTHBEARER} requires the Amazon MSK IAM signer, which is not installed; "
            "install this harness with its 'aws' extra"
        ) from error

    def sign(region: str) -> tuple[str, int]:
        token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(region)
        return token, expiry_ms

    return sign


def _oauth_callback(region: str, token_provider: TokenProvider, on_token: Callable[[], None] | None) -> OauthCallback:
    def callback(_oauthbearer_config: str | None) -> tuple[str, float]:
        token, expiry_ms = token_provider(region)
        if on_token is not None:
            on_token()
        # Convert the signer's milliseconds to librdkafka's seconds.
        return token, expiry_ms / 1000

    return callback


def refuse_mechanism_alias(security: Mapping[str, object], where: str) -> None:
    """Reject ``sasl.mechanisms``; harness readers require ``sasl.mechanism``."""
    if MECHANISM_ALIAS in security:
        raise ValueError(f"{where} sets unsupported alias {MECHANISM_ALIAS!r}; write it as {MECHANISM_KEY!r}")


def has_explicit_oauth(security: Mapping[str, object]) -> bool:
    """Whether the site supplies a librdkafka OAuth token source."""
    return any(key.startswith(_OAUTHBEARER_PREFIX) for key in security)


def refuse_java_oauth(security: Mapping[str, object]) -> None:
    """Prevent librdkafka OAuth settings from silently selecting Java MSK IAM."""
    if has_explicit_oauth(security):
        raise ValueError(
            "managed Java engines do not support site.kafka.security sasl.oauthbearer.* properties; "
            "use an external engine for explicit OAuth, or omit these properties for Amazon MSK IAM"
        )


def librdkafka_config(
    security: Mapping[str, object],
    *,
    token_provider: TokenProvider | None = None,
    on_token: Callable[[], None] | None = None,
) -> dict[str, object]:
    """Build client properties, adding an MSK token callback when needed.

    Call ``on_token`` whenever the callback supplies a token, allowing admin
    clients to track whether polling has initialized authentication.
    """
    refuse_mechanism_alias(security, "the client properties")
    config = {key: value for key, value in security.items() if key != REGION_KEY}
    if MECHANISM_KEY not in security or security[MECHANISM_KEY] != _OAUTHBEARER:
        return config
    # Preserve an explicitly configured token source.
    if has_explicit_oauth(security):
        return config
    if REGION_KEY not in security:
        raise ValueError(
            f"{MECHANISM_KEY}={_OAUTHBEARER} requires {REGION_KEY!r} in the client properties "
            "to sign Amazon MSK IAM tokens"
        )
    region = security[REGION_KEY]
    if not isinstance(region, str):
        raise ValueError(f"{REGION_KEY!r} must be a region name, got {region!r}")
    provider = msk_token_provider() if token_provider is None else token_provider
    config["oauth_cb"] = _oauth_callback(region, provider, on_token)
    return config
