from collections.abc import Iterable

import bson
import confluent_kafka

from ampel.queue.AbsConsumer import AbsConsumer
from ampel.t2.T2QueueWorker import QueueItem

from .KafkaConsumerBase import KafkaConsumerBase


class KafkaConsumer(KafkaConsumerBase, AbsConsumer[QueueItem]):
    def consume(self) -> None | QueueItem:
        message = self._poll(self.stop)
        if message is None or (payload := message.value()) is None:
            return None
        item: QueueItem = bson.decode(payload)  # type: ignore[assignment]
        topic, partition, offset = (
            message.topic(),
            message.partition(),
            message.offset(),
        )
        assert isinstance(topic, str)
        assert isinstance(partition, int)
        assert isinstance(offset, int)
        item["_meta"] = confluent_kafka.TopicPartition(  # type: ignore[typeddict-unknown-key]
            topic, partition, offset + 1
        )
        return item

    def acknowledge(self, messages: Iterable[QueueItem]) -> None:
        """
        Store offsets of fully-processed messages
        """
        offsets: dict[tuple[str, int], confluent_kafka.TopicPartition] = dict()
        for message in messages:
            meta: confluent_kafka.TopicPartition = message["_meta"]  # type: ignore[typeddict-item]
            key = (meta.topic, meta.partition)
            if key not in offsets or meta.offset > offsets[key].offset:
                offsets[key] = meta
        try:
            self._consumer.store_offsets(offsets=list(offsets.values()))
        except confluent_kafka.KafkaException as exc:
            # librdkafka will refuse to store offsets on a partition that is not
            # currently assigned. this can happen if the group is rebalanced
            # while a batch of messages is in flight. see also:
            # https://github.com/confluentinc/confluent-kafka-dotnet/issues/1861
            err = exc.args[0]
            if err.name() != "_STATE":
                raise
