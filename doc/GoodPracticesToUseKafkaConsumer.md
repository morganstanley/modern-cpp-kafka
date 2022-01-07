# Good Practices to Use a KafkaConsumer

If we want to achieve high performance/availability, here're some rules of thumb.

## How to distribute the messages (for the same topics) to different KafkaConsumers

* Use a consumer group for these KafkaConsumers, thus they will work together -- each one deals with different partitions.

* Besides `subscribe` (topics), users could also choose to explicitly `assign` certain partitions to a `KafkaConsumer`.

## How to enhance the throughput

* Try with a larger `QUEUED_MIN_MESSAGES`, especially for small messages.

* Use multiple KafkaConsumers to distribute the payload.

## How to avoid polling duplicated messages

* To commit the offsets more frequently (e.g, always do commit after finishing processing a message).

* Don't use quite a large `MAX_POLL_RECORDS` for a `KafkaConsumer` (with `enable.auto.commit=true`) -- you might fail to commit all these messages before crash, thus more duplications with the next `poll`.

