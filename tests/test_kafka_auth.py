# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Test MSK token signing and admin-client priming with injected providers."""

from __future__ import annotations

import sys
from types import ModuleType, SimpleNamespace
from typing import cast

import pytest

from ingest_bench import kafka_admin, kafka_auth

# A signed token is an opaque string, and its expiry is milliseconds since the
# epoch. The value is deliberately not a whole second, so a test of the
# conversion cannot pass on a truncated one.
TOKEN = "a-signed-url"
EXPIRY_MS = 1_700_000_000_123
REGION = "eu-west-1"


def msk_security() -> dict[str, str]:
    """What a site declares to reach a cluster with IAM authentication."""
    return {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": REGION}


def token_provider(regions: list[str]) -> kafka_auth.TokenProvider:
    """A provider that records the region it was asked to sign in."""

    def sign(region: str) -> tuple[str, int]:
        regions.append(region)
        return TOKEN, EXPIRY_MS

    return sign


def callback_of(config: dict[str, object]) -> kafka_auth.OauthCallback:
    value = config["oauth_cb"]
    assert callable(value)
    return cast(kafka_auth.OauthCallback, value)


def fake_signer_module(seen: list[str]) -> ModuleType:
    def generate_auth_token(region: str) -> tuple[str, int]:
        seen.append(region)
        return TOKEN, EXPIRY_MS

    provider = SimpleNamespace(generate_auth_token=generate_auth_token)
    return cast(ModuleType, SimpleNamespace(MSKAuthTokenProvider=provider))


def test_the_region_is_stripped_and_a_token_callback_attached() -> None:
    regions: list[str] = []
    config = kafka_auth.librdkafka_config(msk_security(), token_provider=token_provider(regions))
    # librdkafka refuses a property it does not know, so the harness's own key
    # must not survive into a client's configuration.
    assert "aws.region" not in config
    assert config["security.protocol"] == "SASL_SSL" and config["sasl.mechanism"] == "OAUTHBEARER"
    assert callback_of(config)(None)[0] == TOKEN
    assert regions == [REGION]


def test_the_expiry_is_reported_in_seconds() -> None:
    config = kafka_auth.librdkafka_config(msk_security(), token_provider=token_provider([]))
    assert callback_of(config)(None) == (TOKEN, 1_700_000_000.123)


def test_the_callback_announces_that_it_ran() -> None:
    runs: list[None] = []
    config = kafka_auth.librdkafka_config(
        msk_security(), token_provider=token_provider([]), on_token=lambda: runs.append(None)
    )
    assert runs == []
    callback_of(config)(None)
    callback_of(config)(None)
    assert len(runs) == 2


def test_oauthbearer_without_a_region_is_refused() -> None:
    with pytest.raises(ValueError, match="aws.region"):
        kafka_auth.librdkafka_config(
            {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER"}, token_provider=token_provider([])
        )


def test_a_region_that_is_not_a_string_is_refused() -> None:
    with pytest.raises(ValueError, match="aws.region"):
        kafka_auth.librdkafka_config({**msk_security(), "aws.region": 1}, token_provider=token_provider([]))


def test_a_site_that_configures_oauthbearer_itself_is_left_alone() -> None:
    security = {**msk_security(), "sasl.oauthbearer.method": "oidc", "sasl.oauthbearer.config": "region=eu-west-1"}
    config = kafka_auth.librdkafka_config(security)
    assert "oauth_cb" not in config and "aws.region" not in config
    assert config["sasl.oauthbearer.method"] == "oidc"


def test_another_mechanism_is_passed_through() -> None:
    security = {**msk_security(), "sasl.mechanism": "PLAIN", "sasl.username": "u"}
    assert kafka_auth.librdkafka_config(security) == {
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "u",
    }


def test_the_plural_spelling_of_the_mechanism_is_refused() -> None:
    plural = {"security.protocol": "SASL_SSL", "sasl.mechanisms": "OAUTHBEARER", "aws.region": REGION}
    with pytest.raises(ValueError, match="'sasl.mechanism'"):
        kafka_auth.librdkafka_config(plural, token_provider=token_provider([]))


def test_no_security_at_all_is_an_empty_configuration() -> None:
    assert kafka_auth.librdkafka_config({}) == {}


def test_non_string_properties_survive_the_pass_through() -> None:
    assert kafka_auth.librdkafka_config({"linger.ms": 5, "enable.idempotence": True}) == {
        "linger.ms": 5,
        "enable.idempotence": True,
    }


def test_the_default_provider_asks_the_signer_to_sign_in_the_region(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[str] = []
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", fake_signer_module(seen))
    config = kafka_auth.librdkafka_config(msk_security())
    assert callback_of(config)(None) == (TOKEN, 1_700_000_000.123)
    assert seen == [REGION]


def test_a_missing_signer_names_the_extra_that_carries_it(monkeypatch: pytest.MonkeyPatch) -> None:
    # A module bound to None in `sys.modules` is how an absent import is
    # provoked on a machine where the extra happens to be installed.
    monkeypatch.setitem(sys.modules, "aws_msk_iam_sasl_signer", cast(ModuleType, None))
    with pytest.raises(ValueError, match="'aws' extra"):
        kafka_auth.librdkafka_config(msk_security())


class FakeMetadata:
    def __init__(self) -> None:
        self.topics: dict[str, object] = {}
        self.brokers: dict[int, object] = {0: object(), 1: object()}


class FakeAdminClient:
    """An admin client that serves the token callback from `poll`, as librdkafka does."""

    def __init__(self, config: dict[str, object]) -> None:
        self.config = config
        self.polls = 0
        self.served = False

    def poll(self, timeout: float) -> int:
        self.polls += 1
        if self.polls < 2 or "oauth_cb" not in self.config:
            return 0
        callback_of(self.config)(None)
        self.served = True
        return 1

    def list_topics(self, timeout: float) -> FakeMetadata:
        if "oauth_cb" in self.config and not self.served:
            raise RuntimeError("SASL authentication error")
        return FakeMetadata()


def fake_admin_clients(monkeypatch: pytest.MonkeyPatch) -> list[FakeAdminClient]:
    """Every admin client the module builds while a test runs."""
    built: list[FakeAdminClient] = []

    def factory(config: dict[str, object]) -> FakeAdminClient:
        client = FakeAdminClient(config)
        built.append(client)
        return client

    monkeypatch.setattr(kafka_admin, "AdminClient", factory)
    return built


def test_an_admin_client_polls_until_the_token_is_served(monkeypatch: pytest.MonkeyPatch) -> None:
    built = fake_admin_clients(monkeypatch)

    def provider() -> kafka_auth.TokenProvider:
        return token_provider([])

    monkeypatch.setattr(kafka_auth, "msk_token_provider", provider)

    assert kafka_admin.broker_count("b:9098", msk_security()) == 2
    assert built[0].polls == 2 and built[0].served
    assert built[0].config["bootstrap.servers"] == "b:9098" and "aws.region" not in built[0].config


def test_an_admin_client_with_no_token_callback_is_not_polled(monkeypatch: pytest.MonkeyPatch) -> None:
    built = fake_admin_clients(monkeypatch)
    assert kafka_admin.broker_count("b:9092", {}) == 2
    assert built[0].polls == 0


@pytest.mark.parametrize("region", [None, REGION])
def test_explicit_oauth_cannot_be_reinterpreted_as_java_iam(region: str | None) -> None:
    from engines.java_properties import kafka_properties

    security = {"sasl.mechanism": "OAUTHBEARER", "sasl.oauthbearer.method": "oidc"}
    if region is not None:
        security["aws.region"] = region
    producer = kafka_auth.librdkafka_config(security)
    assert producer["sasl.mechanism"] == "OAUTHBEARER"
    assert "oauth_cb" not in producer
    with pytest.raises(ValueError, match="managed Java engines do not support"):
        kafka_properties(security)
