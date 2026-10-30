# SPDX-License-Identifier: CC-BY-NC-SA-4.0
"""Test topic lifecycle against a broker and deletion behavior with a fake admin."""

from __future__ import annotations

import os
from types import SimpleNamespace
from typing import cast

import pytest
from confluent_kafka import KafkaError, KafkaException

from ingest_bench import kafka_admin, kafka_auth

BOOTSTRAP = os.environ.get("INGEST_BENCH_TEST_BOOTSTRAP")
MSK_IAM = os.environ.get("INGEST_BENCH_TEST_MSK_IAM") == "1"
MSK_IAM_REGION = os.environ.get("AWS_REGION")


@pytest.mark.integration
@pytest.mark.skipif(not BOOTSTRAP, reason="set INGEST_BENCH_TEST_BOOTSTRAP to a reachable broker")
def test_topic_lifecycle() -> None:
    assert BOOTSTRAP is not None
    name = "ingest-bench-test-topic"
    client: dict[str, str] = {}
    assert kafka_admin.broker_count(BOOTSTRAP, client) >= 1
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
    kafka_admin.create_topic(
        BOOTSTRAP, name, partitions=3, replication_factor=1, topic_config={"retention.ms": "60000"}, client=client
    )
    assert kafka_admin.topic_exists(BOOTSTRAP, name, client)
    assert kafka_admin.topic_partition_count(BOOTSTRAP, name, client) == 3
    with pytest.raises(ValueError):
        kafka_admin.create_topic(BOOTSTRAP, name, partitions=3, replication_factor=1, topic_config={}, client=client)
    assert kafka_admin.delete_topic(BOOTSTRAP, name, client) is True


@pytest.mark.integration
@pytest.mark.skipif(
    not (BOOTSTRAP and MSK_IAM and MSK_IAM_REGION),
    reason="set INGEST_BENCH_TEST_BOOTSTRAP, INGEST_BENCH_TEST_MSK_IAM=1 and AWS_REGION, and run it where the "
    "ambient credentials reach a cluster with IAM authentication",
)
def test_topic_lifecycle_with_iam_authentication() -> None:
    assert BOOTSTRAP is not None and MSK_IAM_REGION is not None
    client = {"security.protocol": "SASL_SSL", "sasl.mechanism": "OAUTHBEARER", "aws.region": MSK_IAM_REGION}
    name = "ingest-bench-test-topic-iam"
    brokers = kafka_admin.broker_count(BOOTSTRAP, client)
    assert brokers >= 1
    kafka_admin.delete_topic(BOOTSTRAP, name, client)
    kafka_admin.create_topic(
        BOOTSTRAP,
        name,
        partitions=3,
        replication_factor=min(3, brokers),
        topic_config={"retention.ms": "60000"},
        client=client,
    )
    assert kafka_admin.topic_exists(BOOTSTRAP, name, client)
    kafka_admin.delete_topic(BOOTSTRAP, name, client)


# ---------------------------------------------------------------------------
# drop-topic, against a fake admin client
# ---------------------------------------------------------------------------


def unknown_topic_error() -> KafkaException:
    """What librdkafka raises for a topic the cluster does not hold."""
    return KafkaException(SimpleNamespace(code=lambda: KafkaError.UNKNOWN_TOPIC_OR_PART))


class FakeFuture:
    def __init__(self, error: KafkaException | None) -> None:
        self.error = error

    def result(self) -> None:
        if self.error is not None:
            raise self.error


class FakeAdminClient:
    """An admin client over one mutable set of topic names."""

    def __init__(self, config: dict[str, object], topics: set[str], deleted: list[str]) -> None:
        self.config = config
        self.topics = topics
        self.deleted = deleted
        self.served = False

    def poll(self, timeout: float) -> int:
        """Serve the token callback, which librdkafka only does from `poll`."""
        if "oauth_cb" not in self.config or self.served:
            return 0
        cast(kafka_auth.OauthCallback, self.config["oauth_cb"])(None)
        self.served = True
        return 1

    def list_topics(self, timeout: float) -> SimpleNamespace:
        return SimpleNamespace(topics=dict.fromkeys(self.topics), brokers={0: object()})

    def delete_topics(self, names: list[str], request_timeout: float) -> dict[str, FakeFuture]:
        futures: dict[str, FakeFuture] = {}
        for name in names:
            self.deleted.append(name)
            present = name in self.topics
            self.topics.discard(name)
            futures[name] = FakeFuture(None if present else unknown_topic_error())
        return futures


def fake_admin(monkeypatch: pytest.MonkeyPatch, topics: set[str]) -> tuple[list[FakeAdminClient], list[str]]:
    """Every admin client the module builds while a test runs, and the names deleted."""
    built: list[FakeAdminClient] = []
    deleted: list[str] = []

    def factory(config: dict[str, object]) -> FakeAdminClient:
        client = FakeAdminClient(config, topics, deleted)
        built.append(client)
        return client

    monkeypatch.setattr(kafka_admin, "AdminClient", factory)
    return built, deleted


def test_topic_partition_count_reads_metadata_without_creating_a_topic(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_admin(monkeypatch, {"t"})

    def list_topics(self: FakeAdminClient, timeout: float) -> SimpleNamespace:
        return SimpleNamespace(topics={"t": SimpleNamespace(partitions={0: None, 1: None, 2: None}, error=None)})

    monkeypatch.setattr(FakeAdminClient, "list_topics", list_topics)
    assert kafka_admin.topic_partition_count("b:9092", "t", {}) == 3
    with pytest.raises(ValueError, match="not found"):
        kafka_admin.topic_partition_count("b:9092", "missing", {})


def test_topic_partition_count_reports_metadata_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    fake_admin(monkeypatch, {"t"})

    def list_topics(self: FakeAdminClient, timeout: float) -> SimpleNamespace:
        return SimpleNamespace(
            topics={
                "t": SimpleNamespace(
                    partitions={},
                    error=KafkaError(KafkaError.TOPIC_AUTHORIZATION_FAILED),
                )
            }
        )

    monkeypatch.setattr(FakeAdminClient, "list_topics", list_topics)
    with pytest.raises(KafkaException):
        kafka_admin.topic_partition_count("b:9092", "t", {})


def test_dropping_a_topic_that_exists(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    built, deleted = fake_admin(monkeypatch, {"a-run"})
    assert kafka_admin.main(["--bootstrap", "b:9092", "--topic", "a-run"]) == 0
    assert deleted == ["a-run"]
    assert built[0].config["bootstrap.servers"] == "b:9092"
    assert "dropped topic 'a-run'" in capsys.readouterr().out


def test_dropping_a_topic_that_does_not_exist_still_succeeds(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    _, deleted = fake_admin(monkeypatch, set())
    assert kafka_admin.main(["--bootstrap", "b:9092", "--topic", "a-run"]) == 0
    assert deleted == ["a-run"]
    assert "topic 'a-run' does not exist" in capsys.readouterr().out


def test_the_client_properties_reach_the_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("IB_TEST_DROP_PASSWORD", "s3cret")
    built, _ = fake_admin(monkeypatch, {"a-run"})

    def provider() -> kafka_auth.TokenProvider:
        return lambda region: ("a-token", 1_700_000_000_000)

    monkeypatch.setattr(kafka_auth, "msk_token_provider", provider)
    assert (
        kafka_admin.main(
            [
                "--bootstrap",
                "b:9098",
                "--topic",
                "a-run",
                "--kafka-prop",
                "security.protocol=SASL_SSL",
                "--kafka-prop",
                "sasl.mechanism=OAUTHBEARER",
                "--kafka-prop",
                "aws.region=eu-west-1",
                "--kafka-prop",
                "sasl.password=${env:IB_TEST_DROP_PASSWORD}",
            ]
        )
        == 0
    )
    config = built[0].config
    assert config["sasl.password"] == "s3cret"
    assert config["security.protocol"] == "SASL_SSL"
    assert "aws.region" not in config
    assert callable(config["oauth_cb"])
