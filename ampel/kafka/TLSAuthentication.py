from typing import Any

from pydantic import FilePath

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.secret.NamedSecret import NamedSecret


class TLSAuthentication(AmpelBaseModel):
    ca: FilePath
    certificate: FilePath
    key: FilePath
    password: NamedSecret[str]

    def librdkafka_config(self) -> dict[str, Any]:
        return {
            "security.protocol": "ssl",
            "ssl.ca.location": str(self.ca),
            "ssl.certificate.location": str(self.certificate),
            "ssl.key.location": str(self.key),
            "ssl.key.password": self.password.get(),
        }
