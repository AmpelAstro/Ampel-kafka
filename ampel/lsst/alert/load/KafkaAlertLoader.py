#!/usr/bin/env python

import itertools
from collections.abc import Callable, Iterable, Iterator
from threading import Event
from typing import Any

import confluent_kafka
from pydantic import TypeAdapter

from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.lsst.kafka.AvroSchema import AvroSchema
from ampel.lsst.kafka.KafkaConsumerBase import KafkaConsumerBase

_get_schema: Callable[[Any], AvroSchema] = TypeAdapter(
    AvroSchema
).validate_python


class KafkaAlertLoader(KafkaConsumerBase, AbsAlertLoader[dict]):
    """
    Load alerts from one or more Kafka topics
    """

    #: Message schema (or url pointing to one)
    avro_schema: None | AvroSchema

    def __init__(self, **kwargs):
        if isinstance(kwargs.get("avro_schema"), str):
            kwargs["avro_schema"] = {"root_url": kwargs["avro_schema"]}

        if avro_schema := kwargs.get("avro_schema"):
            kwargs.setdefault("kafka_consumer_properties", {})[
                "value.deserializer"
            ] = _get_schema(avro_schema).deserializer()
        super().__init__(**kwargs)

        self._it: None | Iterator[dict] = None

    def set_logger(self, logger: AmpelLogger) -> None:
        super().set_logger(logger)

    @staticmethod
    def _add_message_metadata(alert: dict, message: confluent_kafka.Message):
        meta = {}
        timestamp_kind, timestamp = message.timestamp()
        meta["timestamp"] = {
            "kind": (
                "create"
                if timestamp_kind == confluent_kafka.TIMESTAMP_CREATE_TIME
                else "append"
                if timestamp_kind == confluent_kafka.TIMESTAMP_LOG_APPEND_TIME
                else "unavailable"
            ),
            "value": timestamp,
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

    def _consume(self) -> Iterator[dict]:
        stop = Event()
        while not stop.is_set():
            message = self._poll(stop)
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
