from collections.abc import Callable
from functools import partial
from typing import Any, Generic, TypeVar

from confluent_kafka import KafkaException, Producer

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod

from .SASLAuthentication import SASLAuthentication

_T = TypeVar("_T")


class KafkaProducerBase(AmpelABC, Generic[_T], abstract=True):
    bootstrap: str
    topic: str
    auth: None | SASLAuthentication = None

    kafka_producer_properties: dict[str, Any] = {}
    delivery_timeout: float = 10.0

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)
        self._producer = Producer(
            **{
                "bootstrap.servers": self.bootstrap,
            }
            | (self.auth.librdkafka_config() if self.auth else {})
            | self.kafka_producer_properties
        )

    @abstractmethod
    def serialize(self, message: _T) -> bytes: ...  # type: ignore[empty-body]

    def _on_delivery(
        self,
        hook: None | Callable[[], None],
        err,
        msg,  # noqa: ARG002
    ):
        if err is not None:
            raise KafkaException(err)
        if hook is not None:
            hook()

    def produce(
        self, message: _T, delivery_callback: None | Callable[[], None]
    ) -> None:
        self._producer.produce(
            self.topic,
            self.serialize(message),
            on_delivery=partial(self._on_delivery, delivery_callback),
        )
        self._producer.poll(0)

    def flush(self):
        if (in_queue := self._producer.flush(self.delivery_timeout)) > 0:
            raise TimeoutError(
                f"{in_queue} messages still in queue after {self.delivery_timeout} s"
            )
        self._producer.poll(0)
