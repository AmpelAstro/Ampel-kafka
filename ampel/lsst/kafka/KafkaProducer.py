import bson

from ampel.queue.AbsProducer import AbsProducer

from .KafkaProducerBase import KafkaProducerBase


class KafkaProducer(KafkaProducerBase[AbsProducer.Item], AbsProducer):
    def serialize(self, item: AbsProducer.Item) -> bytes:
        return bson.encode(
            {"stock": item.stock, "t0": item.t0, "t1": item.t1, "t2": item.t2}
        )
