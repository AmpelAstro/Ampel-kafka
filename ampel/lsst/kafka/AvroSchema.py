from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from ampel.base.AmpelBaseModel import AmpelBaseModel

from .HttpSchemaRepository import DEFAULT_SCHEMA
from .PlainAvroDeserializer import Deserializer, PlainAvroDeserializer


class SchemaRegistryURL(AmpelBaseModel):
    registry: str

    def deserializer(self) -> Deserializer:
        return AvroDeserializer(SchemaRegistryClient({"url": self.registry}))


class StaticSchemaURL(AmpelBaseModel):
    root_url: str = DEFAULT_SCHEMA

    def deserializer(self) -> Deserializer:
        return PlainAvroDeserializer(self.root_url)


AvroSchema = SchemaRegistryURL | StaticSchemaURL
