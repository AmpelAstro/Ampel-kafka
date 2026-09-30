import contextlib
from threading import Event

import pytest

from ampel.kafka.KafkaConsumerBase import KafkaConsumerBase
from ampel.kafka.KafkaProducerBase import KafkaProducerBase


class KafkaBytesProducer(KafkaProducerBase[bytes]):
    def serialize(self, message: bytes) -> bytes:
        return message


@contextlib.contextmanager
def closing(consumer: KafkaConsumerBase):
    try:
        with consumer as c:
            yield c
    finally:
        consumer._consumer.commit()
        consumer._consumer.close()


@pytest.fixture
def _messages_on_topic(kafka, topic):
    producer = KafkaBytesProducer(bootstrap=kafka.get_bootstrap_server(), topic=topic)
    with producer:
        for i in range(5):
            producer.produce(f"{i}".encode(), delivery_callback=None)


@pytest.fixture
def _consumer_group_offset(set_offset):
    set_offset(1)


@pytest.fixture
def consumer(
    kafka,
    topic,
    consumer_group,
    _messages_on_topic,
    _consumer_group_offset,
    _delete_consumer_group,
):
    consumer = KafkaConsumerBase(
        bootstrap=kafka.get_bootstrap_server(),
        topics=[topic],
        group_name=consumer_group,
        timeout=10,
        kafka_consumer_properties={"broker.address.family": "v4"},
    )
    yield consumer
    consumer._consumer.close()  # Ensure consumer is closed after test


def test_start_at_offset(consumer, topic, get_offset):
    assert get_offset() == 1, "Initial offset should be 1"
    with closing(consumer):
        assert consumer._consumer is not None
        message = consumer._poll(Event())
        assert message is not None
        assert message.topic() == topic
        assert message.partition() == 0
        assert message.offset() == 1
        assert message.value() == b"1"
        consumer._consumer.store_offsets(message)
    assert get_offset() == 2, "Offset should be updated to 2 after storing offsets"
