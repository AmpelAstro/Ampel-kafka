from typing import Any, Literal

from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.secret.NamedSecret import NamedSecret


class SASLAuthentication(AmpelBaseModel):
    protocol: Literal["SASL_PLAINTEXT", "SASL_SSL"] = "SASL_PLAINTEXT"
    mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] = (
        "SCRAM-SHA-512"
    )
    username: NamedSecret[str]
    password: NamedSecret[str]

    def librdkafka_config(self) -> dict[str, Any]:
        return {
            "security.protocol": self.protocol,
            "sasl.mechanism": self.mechanism,
            "sasl.username": self.username.get(),
            "sasl.password": self.password.get(),
        }
