# SPDX-License-Identifier: CC-BY-NC-SA-4.0
import pytest

from ingest_bench.specs.env import (
    has_placeholder,
    names_a_secret,
    refuse_literal_secrets,
    resolve_env_placeholders,
)


def test_has_placeholder_matches_only_the_documented_form() -> None:
    assert has_placeholder("${env:TOKEN}")
    assert has_placeholder("Bearer ${env:TOKEN}")
    assert not has_placeholder("literal")
    # Neither shell nor Kubernetes syntax is honoured: the point of one spelling
    # is that a value carrying any other one is a value, not a reference.
    assert not has_placeholder("${TOKEN}")
    assert not has_placeholder("$env:TOKEN")
    assert not has_placeholder("${env:1TOKEN}")


def test_resolve_replaces_every_occurrence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IB_TEST_USER", "svc")
    monkeypatch.setenv("IB_TEST_PASSWORD", "s3cret")
    assert resolve_env_placeholders(
        {
            "sasl.username": "${env:IB_TEST_USER}",
            "sasl.password": "${env:IB_TEST_PASSWORD}",
            "sasl.jaas.config": "user=${env:IB_TEST_USER} password=${env:IB_TEST_PASSWORD};",
            "security.protocol": "SASL_SSL",
        }
    ) == {
        "sasl.username": "svc",
        "sasl.password": "s3cret",
        "sasl.jaas.config": "user=svc password=s3cret;",
        "security.protocol": "SASL_SSL",
    }
    assert resolve_env_placeholders({}) == {}


def test_an_unset_variable_names_itself_and_the_property(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("IB_TEST_ABSENT", raising=False)
    with pytest.raises(ValueError, match=r"\$\{env:IB_TEST_ABSENT\} is not set in the environment"):
        resolve_env_placeholders({"sasl.password": "${env:IB_TEST_ABSENT}"})
    with pytest.raises(ValueError, match="'sasl.password'"):
        resolve_env_placeholders({"sasl.password": "${env:IB_TEST_ABSENT}"})


def test_a_credential_named_key_is_recognised_by_its_name() -> None:
    for key in (
        "sasl.password",
        "s3.secret-access-key",
        "rest.token",
        "MY_CREDENTIALS",
        "basic_auth_user_info",
        "header.Authorization",
    ):
        assert names_a_secret(key), key
    for key in ("sasl.mechanism", "uri", "warehouse", "s3.region"):
        assert not names_a_secret(key), key


def test_a_literal_credential_is_refused_where_a_reference_is_required() -> None:
    with pytest.raises(ValueError, match=r"sasl\.password.*\$\{env:NAME\}"):
        refuse_literal_secrets({"sasl.password": "hunter2"}, "site.kafka.security")
    # A reference passes, and so does everything that is not a credential.
    refuse_literal_secrets({"sasl.password": "${env:IB_PASSWORD}"}, "site.kafka.security")
    refuse_literal_secrets({"sasl.mechanism": "PLAIN", "uri": "https://catalog.example"}, "site.catalog.props")
    refuse_literal_secrets({}, "site.kafka.security")
