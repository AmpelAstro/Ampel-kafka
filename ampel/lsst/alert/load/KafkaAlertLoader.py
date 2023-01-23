#!/usr/bin/env python

import io
import itertools
import logging
import uuid
from typing import Iterator, Optional, Any

import fastavro
from pydantic import Field

from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.ztf.t0.load.AllConsumingConsumer import AllConsumingConsumer
import confluent_kafka

log = logging.getLogger(__name__)

# schema extracted from https://github.com/LSSTDESC/elasticc/blob/c47fbd301b87f915c77ac0046d7845c68c306444/alert_schema/elasticc.v0_9.alert.avsc
DEFAULT_SCHEMA = {
    "type": "record",
    "doc": "sample avro alert schema v4.1",
    "name": "elasticc.v0_9.alert",
    "fields": [
        {"doc": "unique alert identifer", "name": "alertId", "type": "long"},
        {
            "name": "diaSource",
            "type": {
                "type": "record",
                "name": "elasticc.v0_9.diaSource",
                "fields": [
                    {"name": "diaSourceId", "type": "long"},
                    {"name": "ccdVisitId", "type": "long"},
                    {
                        "default": None,
                        "name": "diaObjectId",
                        "type": ["null", "long"],
                    },
                    {
                        "default": None,
                        "name": "parentDiaSourceId",
                        "type": ["null", "long"],
                    },
                    {"name": "midPointTai", "type": "double"},
                    {"name": "filterName", "type": "string"},
                    {"name": "ra", "type": "double"},
                    {"name": "decl", "type": "double"},
                    {"name": "psFlux", "type": "float"},
                    {"name": "psFluxErr", "type": "float"},
                    {"name": "snr", "type": "float"},
                    {
                        "default": None,
                        "name": "nobs",
                        "type": ["null", "float"],
                    },
                ],
            },
        },
        {
            "default": None,
            "name": "prvDiaSources",
            "type": [
                "null",
                {"type": "array", "items": "elasticc.v0_9.diaSource"},
            ],
        },
        {
            "default": None,
            "name": "prvDiaForcedSources",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "elasticc.v0_9.diaForcedSource",
                        "fields": [
                            {"name": "diaForcedSourceId", "type": "long"},
                            {"name": "ccdVisitId", "type": "long"},
                            {"name": "diaObjectId", "type": "long"},
                            {"name": "midPointTai", "type": "double"},
                            {"name": "filterName", "type": "string"},
                            {"name": "psFlux", "type": "float"},
                            {"name": "psFluxErr", "type": "float"},
                            {"name": "totFlux", "type": "float"},
                            {"name": "totFluxErr", "type": "float"},
                        ],
                    },
                },
            ],
        },
        {
            "default": None,
            "name": "prvDiaNondetectionLimits",
            "type": [
                "null",
                {
                    "type": "array",
                    "items": {
                        "type": "record",
                        "name": "elasticc.v0_9.diaNondetectionLimit",
                        "fields": [
                            {"name": "ccdVisitId", "type": "long"},
                            {"name": "midPointTai", "type": "double"},
                            {"name": "filterName", "type": "string"},
                            {"name": "diaNoise", "type": "float"},
                        ],
                    },
                },
            ],
        },
        {
            "default": None,
            "name": "diaObject",
            "type": [
                "null",
                {
                    "type": "record",
                    "name": "elasticc.v0_9.diaObject",
                    "fields": [
                        {"name": "diaObjectId", "type": "long"},
                        {
                            "doc": "diaObject provenance",
                            "name": "simVersion",
                            "type": ["null", "string"],
                        },
                        {"name": "ra", "type": "double"},
                        {"name": "decl", "type": "double"},
                        {
                            "default": None,
                            "name": "mwebv",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "mwebv_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "z_final",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "z_final_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_ellipticity",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_sqradius",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zspec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zspec_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q000",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q010",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q020",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q030",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q040",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q050",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q060",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q070",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q080",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q090",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_q100",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_zphot_p50",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_mag_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_ra",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_dec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_snsep",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal_magerr_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_ellipticity",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_sqradius",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zspec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zspec_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_err",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q000",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q010",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q020",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q030",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q040",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q050",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q060",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q070",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q080",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q090",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_q100",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_zphot_p50",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_mag_Y",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_ra",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_dec",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_snsep",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_u",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_g",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_r",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_i",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_z",
                            "type": ["null", "float"],
                        },
                        {
                            "default": None,
                            "name": "hostgal2_magerr_Y",
                            "type": ["null", "float"],
                        },
                    ],
                },
            ],
        },
        {
            "default": None,
            "name": "cutoutDifference",
            "type": ["null", "bytes"],
        },
        {"default": None, "name": "cutoutTemplate", "type": ["null", "bytes"]},
    ],
}


class KafkaAlertLoader(AbsAlertLoader[dict]):
    """
    Load alerts from one or more Kafka topics
    """

    #: Address of Kafka broker
    bootstrap: str = "public.alerts.ztf.uw.edu:9092"
    #: Topics to subscribe to
    topics: list[str] = Field(..., min_items=1)
    #: Message schema
    avro_schema: dict = DEFAULT_SCHEMA
    #: Consumer group name
    group_name: str = str(uuid.uuid1())
    #: time to wait for messages before giving up, in seconds
    timeout: int = 1
    #: extra configuration to pass to confluent_kafka.Consumer
    kafka_consumer_properties: dict[str, Any] = {}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        config = {"group.id": self.group_name} | self.kafka_consumer_properties

        self._consumer = AllConsumingConsumer(
            self.bootstrap,
            timeout=self.timeout,
            topics=self.topics,
            auto_commit=True,
            logger=self.logger,
            **config,
        )
        self._it = None

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

    def alerts(self, limit: Optional[int] = None) -> Iterator[dict]:
        """
        Generate alerts until timeout is reached
        :returns: dict instance of the alert content
        :raises StopIteration: when next(fastavro.reader) has dried out
        """

        schema = fastavro.schema.parse_schema(self.avro_schema)

        for message in itertools.islice(self._consumer, limit):
            alert = fastavro.schemaless_reader(
                io.BytesIO(message.value()), schema
            )
            if isinstance(alert, list):
                for d in alert:
                    yield self._add_message_metadata(d, message)
            else:
                yield self._add_message_metadata(alert, message)

    def __next__(self) -> dict:
        if self._it is None:
            self._it = self.alerts()
        return next(self._it)
