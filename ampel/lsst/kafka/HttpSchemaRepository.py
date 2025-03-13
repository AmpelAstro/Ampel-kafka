from pathlib import PurePosixPath
from urllib.parse import urlsplit, urlunsplit

import fastavro.schema
import requests
from fastavro.repository.base import (
    AbstractSchemaRepository,
    SchemaRepositoryError,
)
from fastavro.types import Schema

# Schema used until ~summer 2023
# DEFAULT_SCHEMA = "https://raw.githubusercontent.com/LSSTDESC/elasticc/c47fbd301b87f915c77ac0046d7845c68c306444/alert_schema/elasticc.v0_9.alert.avsc"
# Current default
DEFAULT_SCHEMA = "https://raw.githubusercontent.com/LSSTDESC/elasticc/main/alert_schema/elasticc.v0_9_1.alert.avsc"


class HttpSchemaRepostory(AbstractSchemaRepository):
    @classmethod
    def get_parts(cls, url: str) -> tuple[str, str, str]:
        parts = urlsplit(url)
        path = PurePosixPath(parts.path)
        return (
            urlunsplit(
                (
                    parts.scheme,
                    parts.netloc,
                    f"{path.parent}/",
                    parts.query,
                    parts.fragment,
                )
            ),
            path.stem,
            path.suffix,
        )

    @classmethod
    def load_schema(cls, url: str) -> Schema:
        base, schema, ext = cls.get_parts(url)
        return fastavro.schema.load_schema(schema, repo=cls(base, ext))

    def __init__(self, base_url: str, file_ext: str = ".avsc"):
        self.base_url = base_url
        self.file_ext = file_ext
        self.session = requests.Session()

    def load(self, name: str):
        try:
            response = self.session.get(
                f"{self.base_url}{name}{self.file_ext}"
            )
            response.raise_for_status()
            return response.json()
        except Exception as error:
            raise SchemaRepositoryError(
                f"Failed to load '{name}' schema",
            ) from error


def parse_schema(schema_or_url: str | dict) -> Schema:
    if isinstance(schema_or_url, str):
        return HttpSchemaRepostory.load_schema(schema_or_url)
    return fastavro.schema.parse_schema(schema_or_url)
