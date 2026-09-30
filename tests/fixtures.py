import os
from pathlib import Path

import pytest
from confluent_kafka import ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from testcontainers.community.kafka import KafkaContainer


@pytest.fixture(scope="session")
def _docker(pytestconfig):
    if not pytestconfig.getoption("--integration"):
        pytest.skip("Integration tests are disabled. Use --integration to enable them.")
    rd_socket = Path.home() / ".rd/docker.sock"
    if rd_socket.exists():
        os.environ["DOCKER_HOST"] = f"unix://{rd_socket}"
        os.environ["TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE"] = "/var/run/docker.sock"


@pytest.fixture
def kafka(_docker):
    with KafkaContainer() as kafka:
        yield kafka


@pytest.fixture
def admin_client(kafka):
    return AdminClient({"bootstrap.servers": kafka.get_bootstrap_server()})


@pytest.fixture
def topic(admin_client):
    topic_name = "test-topic"
    for future in admin_client.create_topics(
        [NewTopic(topic_name, num_partitions=1, replication_factor=1)]
    ).values():
        future.result()
    yield topic_name
    for future in admin_client.delete_topics([topic_name]).values():
        future.result()


@pytest.fixture
def consumer_group():
    return "test-group"


@pytest.fixture
def _delete_consumer_group(admin_client, consumer_group):
    yield
    for future in admin_client.delete_consumer_groups([consumer_group]).values():
        future.result()


@pytest.fixture
def get_offset(admin_client, topic, consumer_group):
    def _get_offset() -> int:
        futures = admin_client.list_consumer_group_offsets(
            [ConsumerGroupTopicPartitions(consumer_group, [TopicPartition(topic, 0)])]
        )
        future = futures[consumer_group]
        cgtp = future.result()
        return cgtp.topic_partitions[0].offset

    return _get_offset


@pytest.fixture
def set_offset(admin_client, topic, consumer_group):
    def _set_offset(offset: int) -> None:
        for future in admin_client.alter_consumer_group_offsets(
            [
                ConsumerGroupTopicPartitions(
                    consumer_group, [TopicPartition(topic, partition=0, offset=offset)]
                )
            ]
        ).values():
            result = future.result()
            assert result.group_id == consumer_group
            assert result.topic_partitions[0].offset == offset

    return _set_offset
