from collections.abc import Callable
from functools import partial
from threading import Event, Thread
from typing import Any, Generic, Self, TypeVar

from confluent_kafka import KafkaException, Producer

from ampel.abstract.AbsContextManager import AbsContextManager
from ampel.base.decorator import abstractmethod

from .KafkaAuthentication import KafkaAuthentication

_T = TypeVar("_T")


class KafkaProducerBase(AbsContextManager, Generic[_T], abstract=True):
    bootstrap: str
    topic: str
    auth: None | KafkaAuthentication = None

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
        self._stop_thread = Event()
        self._thread: None | Thread = None

    def _poll(self):
        # Poll producer from a thread to trigger delivery callbacks
        while not self._stop_thread.is_set():
            self._producer.poll(1)

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

    def __enter__(self) -> "Self":
        assert self._thread is None, (
            f"{self.__class__.__qualname__} is not reentrant"
        )
        # start the delivery callback thread
        self._stop_thread.clear()
        self._thread = Thread(target=self._poll)
        self._thread.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # stop the delivery callback thread
        assert self._thread is not None
        self._stop_thread.set()
        self._thread.join()
        self._thread = None
        # ensure enqueued messages are delivered
        if (in_queue := self._producer.flush(self.delivery_timeout)) > 0:
            raise TimeoutError(
                f"{in_queue} messages still in queue after {self.delivery_timeout} s"
            )
        # trigger callbacks for any remaining messages
        self._producer.poll(0)
