import io

import fastavro
from confluent_kafka.serialization import Deserializer, SerializationContext

from .HttpSchemaRepository import parse_schema


class PlainAvroDeserializer(Deserializer):
    """
    Deserializer for static schemas
    """

    def __init__(self, avro_schema: dict | str):
        self._schema = parse_schema(avro_schema)

    def __call__(self, value: bytes, ctx: SerializationContext) -> dict:  # noqa: ARG002
        return fastavro.schemaless_reader(  # type: ignore[return-value]
            io.BytesIO(value),
            writer_schema=self._schema,
            reader_schema=None,
        )
