import os
import uuid
from threading import Event
from typing import Annotated, Any, Self

import confluent_kafka
from annotated_types import Gt, MinLen
from confluent_kafka import Consumer
from confluent_kafka.deserializing_consumer import DeserializingConsumer

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.AmpelUnit import AmpelUnit

from .KafkaAuthentication import KafkaAuthentication


class KafkaConsumerBase(AbsContextManager, AmpelUnit):
    #: Address of Kafka broker
    bootstrap: str
    #: Optional authentication
    auth: None | KafkaAuthentication = None
    #: Topics to subscribe to
    topics: Annotated[list[str], MinLen(1)]
    #: Consumer group name
    group_name: None | str = None
    #: time to wait for messages before giving up, in seconds
    timeout: Annotated[int, Gt(0)] = 1
    #: environment variable to use as group.instance.id. If None, disable static membership
    instance_id_env_var: None | str = "HOSTNAME"
    #: extra configuration to pass to confluent_kafka.Consumer
    kafka_consumer_properties: dict[str, Any] = {}

    def __init__(
        self, extra_consumer_properties: dict[str, Any] | None = None, **kwargs
    ):
        super().__init__(**kwargs)

        group_name = self.group_name if self.group_name else str(uuid.uuid1())

        config = (
            {
                "bootstrap.servers": self.bootstrap,
                "auto.offset.reset": "smallest",
                "enable.auto.commit": True,
                "enable.auto.offset.store": False,
                "auto.commit.interval.ms": 10000,
                "receive.message.max.bytes": 2**29,
                "enable.partition.eof": False,  # don't emit messages on EOF
                "error_cb": self._raise_errors,
                "group.id": group_name,
            }
            | (self.auth.librdkafka_config() if self.auth is not None else {})
            # allow process to restart without triggering a rebalance
            | (
                {"group.instance.id": os.getenv(self.instance_id_env_var)}
                if self.instance_id_env_var and os.getenv(self.instance_id_env_var)
                else {}
            )
            | (extra_consumer_properties or {})
            | self.kafka_consumer_properties
        )

        self._consumer = DeserializingConsumer(config)

        self._poll_interval = max((1, min((3, self.timeout))))
        self._poll_attempts = max((1, int(self.timeout / self._poll_interval)))

    def _raise_errors(self, exc: Exception) -> None:
        raise exc

    def _poll(self, stop: Event) -> confluent_kafka.Message | None:
        """
        Poll for a message, ignoring nonfatal errors
        """
        message = None
        # wake up occasionally to catch SIGINT
        for _ in range(self._poll_attempts):
            try:
                if stop.is_set() or (
                    message := self._consumer.poll(self._poll_interval)
                ):
                    break
            except confluent_kafka.KafkaException as exc:
                err = exc.args[0]
                if err.name() == "UNKNOWN_TOPIC_OR_PART":
                    # ignore unknown topic messages
                    continue
                if err.name() in (
                    "_TIMED_OUT",
                    "_MAX_POLL_EXCEEDED",
                ):
                    # bail on timeouts
                    return None
                raise
            except KeyboardInterrupt:
                stop.set()
        return message

    def advance_offset_to_low_watermark(
        self, consumer: Consumer, partitions: list[confluent_kafka.TopicPartition]
    ) -> None:
        """
        Callback for when partitions are assigned to this consumer. If stored
        offsets are smaller than the lower watermark, advance the offset to the
        lower watermark. This is useful for cases where messages have been
        deleted from the topic and the stored offset is no longer valid.
        """
        expired_offsets = []
        for p in consumer.committed(partitions, timeout=self.timeout):
            low, _ = consumer.get_watermark_offsets(p, timeout=self.timeout)
            if low > p.offset:
                p.offset = low
                expired_offsets.append(p)
        consumer.assign(partitions)
        if expired_offsets:
            consumer.store_offsets(offsets=expired_offsets)

    def __enter__(self) -> Self:
        self._consumer.subscribe(
            self.topics, on_assign=self.advance_offset_to_low_watermark
        )
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._consumer.commit()
        self._consumer.unsubscribe()
