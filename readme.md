# A Kafka Consumer Library

This repo contains some helpers for Kafka Consumer.

Right now it only contains 1 additional feature:

## UpsertKafkaConsumer

This is a wrapper of regular Kafka Consumer, with 1 distinct feature.

On 1st poll, it will try to read everything from the assigned partitions and perform compaction in memory.

This pattern is quite common on compacted topics where you only care about the latest value per key in your topic.

It also handles partition rebalancing out of the box to maintain correctness.