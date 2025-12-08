import os
import uuid
from threading import Event
from typing import Annotated, Any, Self

import confluent_kafka
from annotated_types import Gt, MinLen
from confluent_kafka.deserializing_consumer import DeserializingConsumer

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.AmpelUnit import AmpelUnit

from .KafkaAuthentication import KafkaAuthentication, SASLAuthentication


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

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        group_name = self.group_name if self.group_name else str(uuid.uuid1())
        if isinstance(self.auth, SASLAuthentication):
            group_name = f"{self.auth.username.get()}-{group_name}"

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
                if self.instance_id_env_var
                and os.getenv(self.instance_id_env_var)
                else {}
            )
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
            except confluent_kafka.KafkaError as exc:
                if (
                    exc.code()
                    == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART
                ):
                    # ignore unknown topic messages
                    continue
                if exc.code() in (
                    confluent_kafka.KafkaError._TIMED_OUT,  # noqa: SLF001
                    confluent_kafka.KafkaError._MAX_POLL_EXCEEDED,  # noqa: SLF001
                ):
                    # bail on timeouts
                    return None
                raise
            except KeyboardInterrupt:
                stop.set()
        return message

    def __enter__(self) -> Self:
        self._consumer.subscribe(self.topics)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self._consumer.commit()
        self._consumer.unsubscribe()
