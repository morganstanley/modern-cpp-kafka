# Good Practices to Use a KafkaProducer

If we want to achieve high performance/availability, here're some rules of thumb.

## Avoid using `syncSend` for better throughput

You should never call `syncSend` if you want to get a high throughput. The `syncSend` is a synchronous operation, and would not go on until the `acks` are received.

## The `message.max.bytes` must be consistent with Kafka servers' setting

* Default value: 1000,000

* The default setting for brokers is `message.max.bytes = 1000012`, and do MAKE SURE the client side setting no larger than it. Otherwise, it might construct a MessageSet which would be rejected (error: INVALID_MESSAGE_SIZE) by brokers.

## Calculate `batch.num.messages` with the average message size

*   Default value: 10,000

*   It defines the maximum number of messages batched in one MessageSet.

    Normally, larger value, better performance. However, since the size of MessageSet is limited by `message.max.bytes`, a too large value would not help any more.

    E.g, with the default `message.max.bytes=1000000` and `batch.num.messages=10000` settings, you could get the best performance while the average message size is larger than 100 bytes.

    However, if the average message size is small, you have to enlarge it (to `message.max.bytes/average_message_size` at least).

## Choose `acks` wisely

* The acks parameter controls how many partition replicas must receive the record before the producer can consider the write successful.

    * `acks=0`, the producer will not wait for a reply from the broker before assuming the message was sent successfully.

    * `acks=1`, the producer will receive a success response from the broker the moment the leader replica received the message.

    * `acks=all`, the producer will receive a success response from the broker once all in-sync replicas received the message.

    * Note: if "ack=all", please make sure the topic's replication factor is larger than 1.

* The `acks=all` setting will highly impact the throughput & latency, and it would be obvious if the traffic latency between kafka brokers is high. But it's mandatory if we want to achieve high availability.

## How could a message miss after send?

* The message might even not have been received by the partition leader! (with `acks=0`)

* Once the message received by the partition leader, the leader crashed just after responding to the producer, but has no chance to synchronize the message to other replicas. (with `acks=1`)

* Once the message received by the partition leader, the leader crashed just after responding to the producer, but with no in-sync replica to synchronize for the message. (with `acks=all`, while brokers are with `min.insync.replicas=1`)

