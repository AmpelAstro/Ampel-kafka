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


def test_auto_reset_offset(consumer, get_offset):
    """Consumer starts from the low watermark if no offset is stored."""
    with closing(consumer):
        assert consumer._consumer is not None
        message = consumer._poll(Event())
        assert message is not None
        assert message.offset() == 0
        consumer._consumer.store_offsets(message)
    assert get_offset() == 1, "Offset should remain at 1 after consuming a message"


def test_start_at_offset(consumer, set_offset, get_offset):
    """Consumer starts from stored offset and updates offset after storing offsets."""
    set_offset(1)
    assert get_offset() == 1, "Initial offset should be 1"
    with closing(consumer):
        assert consumer._consumer is not None
        message = consumer._poll(Event())
        assert message is not None
        assert message.offset() == 1
        consumer._consumer.store_offsets(message)
    assert get_offset() == 2, "Offset should be updated to 2 after storing offsets"


def test_start_at_expired_offset(consumer, set_offset, get_offset, delete_messages):
    """Consuming from an expired offset should advance offset to the low watermark."""
    # store offset of 1
    set_offset(1)
    # delete messages to simulate expired offset
    delete_messages(5)
    assert get_offset() == 1, "Initial offset should be 1"
    with closing(consumer):
        assert consumer._consumer is not None
        message = consumer._consumer.poll(1)
        assert message is None, "No messages should be available after deletion"
    assert get_offset() == 5, "Offset should be updated to 5 after storing offsets"
