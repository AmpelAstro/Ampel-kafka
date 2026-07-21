from functools import cached_property
from typing import Any

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import (
    AvroDeserializer,
    AvroSerializer,
)
from confluent_kafka.serialization import Deserializer, Serializer

from ampel.base.AmpelBaseModel import AmpelBaseModel

from .HttpSchemaRepository import DEFAULT_SCHEMA
from .PlainAvroDeserializer import PlainAvroDeserializer
from .PlainAvroSerializer import PlainAvroSerializer


class SchemaRegistryURL(AmpelBaseModel):
    registry: str
    subject: str | None = None

    @cached_property
    def _schema_registry_client(self) -> SchemaRegistryClient:
        return SchemaRegistryClient({"url": self.registry})

    def serializer(self) -> Serializer:
        # Require that schemas are pre-registered
        conf: dict[str, Any] = {
            "auto.register.schemas": False,
            "use.latest.version": True,
        }
        if self.subject is not None:
            conf["subject.name.strategy"] = lambda *args: self.subject
        return AvroSerializer(self._schema_registry_client, conf=conf)

    def deserializer(self) -> Deserializer:
        return AvroDeserializer(self._schema_registry_client)


class StaticSchemaURL(AmpelBaseModel):
    root_url: str = DEFAULT_SCHEMA

    def serializer(self) -> Serializer:
        return PlainAvroSerializer(self.root_url)

    def deserializer(self) -> Deserializer:
        return PlainAvroDeserializer(self.root_url)


AvroSchema = SchemaRegistryURL | StaticSchemaURL
