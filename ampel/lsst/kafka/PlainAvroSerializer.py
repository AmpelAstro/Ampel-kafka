import io
from typing import Any

import fastavro
from confluent_kafka.serialization import SerializationContext, Serializer

from .HttpSchemaRepository import parse_schema


class PlainAvroSerializer(Serializer):
    """
    Serializer for static schemas
    """

    def __init__(self, avro_schema: dict | str):
        self._schema = parse_schema(avro_schema)

    def __call__(
        self,
        value: Any,
        ctx: SerializationContext | None = None,  # noqa: ARG002
    ) -> bytes | None:
        if value is None:
            return None
        with io.BytesIO() as buf:
            fastavro.schemaless_writer(buf, self._schema, value)
            return buf.getvalue()
