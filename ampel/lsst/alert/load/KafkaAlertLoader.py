#!/usr/bin/env python

import itertools
import uuid
from collections.abc import Iterable, Iterator
from typing import Annotated, Any, Literal

import confluent_kafka
from annotated_types import Gt, MinLen
from confluent_kafka.deserializing_consumer import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.secret.NamedSecret import NamedSecret

from .HttpSchemaRepository import DEFAULT_SCHEMA
from .PlainAvroDeserializer import Deserializer, PlainAvroDeserializer


class SchemaRegistryURL(AmpelBaseModel):
    registry: str


class StaticSchemaURL(AmpelBaseModel):
    root_url: str = DEFAULT_SCHEMA


class SASLAuthentication(AmpelBaseModel):
    protocol: Literal["SASL_PLAINTEXT", "SASL_SSL"] = "SASL_PLAINTEXT"
    mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = (
        "SCRAM-SHA-512"
    )
    username: NamedSecret[str]
    password: NamedSecret[str]


class KafkaAlertLoader(AbsAlertLoader[dict]):
    """
    Load alerts from one or more Kafka topics
    """

    #: Address of Kafka broker
    bootstrap: str
    #: Optional authentication
    auth: None | SASLAuthentication = None
    #: Topics to subscribe to
    topics: Annotated[list[str], MinLen(1)]
    #: Message schema (or url pointing to one)
    avro_schema: SchemaRegistryURL | StaticSchemaURL
    #: Consumer group name
    group_name: None | str = None
    #: time to wait for messages before giving up, in seconds
    timeout: Annotated[int, Gt(0)] = 1
    #: extra configuration to pass to confluent_kafka.Consumer
    kafka_consumer_properties: dict[str, Any] = {}

    def __init__(self, **kwargs):
        if isinstance(kwargs.get("avro_schema"), str):
            kwargs["avro_schema"] = {"root_url": kwargs["avro_schema"]}
        super().__init__(**kwargs)

        if isinstance(self.avro_schema, StaticSchemaURL):
            deserializer: Deserializer = PlainAvroDeserializer(
                self.avro_schema.root_url
            )
        else:
            deserializer = AvroDeserializer(
                SchemaRegistryClient({"url": self.avro_schema.registry})
            )

        config = (
            {
                "bootstrap.servers": self.bootstrap,
                "auto.offset.reset": "smallest",
                "enable.auto.commit": True,
                "enable.auto.offset.store": False,
                "auto.commit.interval.ms": 10000,
                "receive.message.max.bytes": 2**29,
                "enable.partition.eof": False,  # don't emit messages on EOF
                "value.deserializer": deserializer,
            }
            | (
                {
                    "group.id": self.group_name
                    if self.group_name
                    else str(uuid.uuid1())
                }
                if self.auth is None
                else {
                    "security.protocol": self.auth.protocol,
                    "sasl.mechanism": self.auth.mechanism,
                    "sasl.username": self.auth.username.get(),
                    "sasl.password": self.auth.password.get(),
                    "group.id": self.group_name
                    if self.group_name
                    else f"{self.auth.username.get()}-{uuid.uuid1()}",
                }
            )
            | self.kafka_consumer_properties
        )

        self._consumer = DeserializingConsumer(config)
        self._consumer.subscribe(self.topics)
        self._it = None

        self._poll_interval = max((1, min((3, self.timeout))))
        self._poll_attempts = max((1, int(self.timeout / self._poll_interval)))

    def set_logger(self, logger: AmpelLogger) -> None:
        super().set_logger(logger)

    @staticmethod
    def _add_message_metadata(alert: dict, message: confluent_kafka.Message):
        meta = {}
        timestamp_kind, timestamp = message.timestamp()
        meta["timestamp"] = {
            (
                "create"
                if timestamp_kind == confluent_kafka.TIMESTAMP_CREATE_TIME
                else "append"
                if timestamp_kind == confluent_kafka.TIMESTAMP_LOG_APPEND_TIME
                else "unavailable"
            ): timestamp
        }
        meta["topic"] = message.topic()
        meta["partition"] = message.partition()
        meta["offset"] = message.offset()
        meta["key"] = message.key()

        alert["__kafka"] = meta
        return alert

    def acknowledge(self, alert_dicts: Iterable[dict]) -> None:
        """
        Store offsets of fully-processed messages
        """
        offsets: dict[tuple[str, int], int] = dict()
        for alert in alert_dicts:
            meta = alert["__kafka"]
            key, value = (meta["topic"], meta["partition"]), meta["offset"]
            if key not in offsets or value > offsets[key]:
                offsets[key] = value
        try:
            self._consumer.store_offsets(
                offsets=[
                    confluent_kafka.TopicPartition(
                        topic, partition, offset + 1
                    )
                    for (topic, partition), offset in offsets.items()
                ]
            )
        except confluent_kafka.KafkaException as exc:
            # librdkafka will refuse to store offsets on a partition that is not
            # currently assigned. this can happen if the group is rebalanced
            # while a batch of messages is in flight. see also:
            # https://github.com/confluentinc/confluent-kafka-dotnet/issues/1861
            err = exc.args[0]
            if err.code() != confluent_kafka.KafkaError._STATE:  # noqa: SLF001
                raise

    def _poll(self) -> confluent_kafka.Message | None:
        """
        Poll for a message, ignoring nonfatal errors
        """
        message = None
        for _ in range(self._poll_attempts):
            # wake up occasionally to catch SIGINT
            message = self._consumer.poll(self._poll_interval)
            if message is not None:
                if err := message.error():
                    if (
                        err.code()
                        == confluent_kafka.KafkaError.UNKNOWN_TOPIC_OR_PART
                    ):
                        # ignore unknown topic messages
                        continue
                    if err.code() in (
                        confluent_kafka.KafkaError._TIMED_OUT,  # noqa: SLF001
                        confluent_kafka.KafkaError._MAX_POLL_EXCEEDED,  # noqa: SLF001
                    ):
                        # bail on timeouts
                        if self._logger:
                            self._logger.debug(f"Got {err}")
                        return None
                break
        if message is None:
            return message
        if message.error():
            raise message.error()
        return message

    def _consume(self) -> Iterator[dict]:
        while True:
            message = self._poll()
            if message is None:
                return
            else:
                yield self._add_message_metadata(message.value(), message)

    def alerts(self, limit: None | int = None) -> Iterator[dict]:
        """
        Generate alerts until timeout is reached
        :returns: dict instance of the alert content
        :raises StopIteration: when no alerts recieved within timeout
        """

        yield from itertools.islice(self._consume(), limit)

    def __next__(self) -> dict:
        if self._it is None:
            self._it = self.alerts()
        return next(self._it)
