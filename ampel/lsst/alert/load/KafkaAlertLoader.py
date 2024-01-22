#!/usr/bin/env python

import io
import itertools
import logging
import uuid
from collections.abc import Iterable, Iterator
from typing import Any

import confluent_kafka
import fastavro
from pydantic import Field

from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.log.AmpelLogger import AmpelLogger
from ampel.ztf.t0.load.AllConsumingConsumer import AllConsumingConsumer

from .HttpSchemaRepository import DEFAULT_SCHEMA, parse_schema

log = logging.getLogger(__name__)


class KafkaAlertLoader(AbsAlertLoader[dict]):
    """
    Load alerts from one or more Kafka topics
    """

    #: Address of Kafka broker
    bootstrap: str = "public.alerts.ztf.uw.edu:9092"
    #: Topics to subscribe to
    topics: list[str] = Field(..., min_items=1)
    #: Message schema (or url pointing to one)
    avro_schema: dict | str = DEFAULT_SCHEMA
    #: Consumer group name
    group_name: str = str(uuid.uuid1())
    #: time to wait for messages before giving up, in seconds
    timeout: int = 1
    #: extra configuration to pass to confluent_kafka.Consumer
    kafka_consumer_properties: dict[str, Any] = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        config = {
            "group.id": self.group_name,
            "auto_commit": False,
        } | self.kafka_consumer_properties

        self._consumer = AllConsumingConsumer(
            self.bootstrap,
            timeout=self.timeout,
            topics=self.topics,
            logger=self.logger,
            **config,
        )
        self._it = None

    def set_logger(self, logger: AmpelLogger) -> None:
        super().set_logger(logger)
        self._consumer._logger = logger  # noqa: SLF001

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
        offsets: dict[tuple[str, int], int] = dict()
        for alert in alert_dicts:
            meta = alert["__kafka"]
            key, value = (meta["topic"], meta["partition"]), meta["offset"]
            if key not in offsets or value > offsets[key]:
                offsets[key] = value
        self._consumer.store_offsets(
            [
                confluent_kafka.TopicPartition(topic, partition, offset + 1)
                for (topic, partition), offset in offsets.items()
            ]
        )

    def alerts(self, limit: None | int = None) -> Iterator[dict]:
        """
        Generate alerts until timeout is reached
        :returns: dict instance of the alert content
        :raises StopIteration: when next(fastavro.reader) has dried out
        """

        schema = parse_schema(self.avro_schema)

        for message in itertools.islice(self._consumer, limit):
            alert = fastavro.schemaless_reader(
                io.BytesIO(message.value()),
                writer_schema=schema,
                reader_schema=None,
            )
            if isinstance(alert, list):
                for d in alert:
                    yield self._add_message_metadata(d, message)
            elif isinstance(alert, dict):
                yield self._add_message_metadata(alert, message)
            else:
                raise TypeError(
                    f"can't handle messages that deserialize to {type(message)}"
                )

    def __next__(self) -> dict:
        if self._it is None:
            self._it = self.alerts()
        return next(self._it)
