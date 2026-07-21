# Ampel-kafka

Apache Kafka support for the Ampel system.

## Units

* **KafkaAlertLoader**: load alerts from a Kafka topic
* **KafkaProducer**: produce bundles of stock/t0/t1/t2 docs to a topic
* **KafkaConsumer**: consume bundles of stock/t0/t1/t2 docs from a topic
* **KafkaAdapter**: produce UnitResult body to a Kafka topic

### Optional `hop`

* **HopskotchAdapter**: produce UnitResult body to a Kafka topic with [Hopskotch](https://scimma.org/hopskotch) metadata
