# Good Practices to Use a KafkaProducer

If we want to achieve high performance/availability, here're some rules of thumb.

## Use a `KafkaAsyncProducer` for better throughput

You should not use a `KafkaSyncProducer` if you want to get a high throughput. The `Sync` means the `send` operation is a synchronous operation, and would not go on until the `acks` are received.

## The `message.max.bytes` must be consistent with Kafka servers' setting

* Default value: 1000,000

* The default setting for brokers is `message.max.bytes = 1000012`, and do MAKE SURE the client side setting no larger than it. Otherwise, it might construct a MessageSet which would be rejected (error: INVALID_MESSAGE_SIZE) by brokers.

## In most cases, the default value for `linger.ms` is good enough

* It means the delay in milliseconds to wait for messages in the producer queue to accumulate before constructing MessageSets to transmit to brokers.

    1.  For a `KafkaSyncProducer`, it sends the messages one by one (after `acks` response received), thus it could use `linger.ms=0` (as default) to eliminate the unnecessary waiting time.

    2.  For a `KafkaAsyncProduer`, a larger `linger.ms` could be used to accumulate messages for MessageSets to improve the performance -- it should be the result of balancing between throughput and latency.

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

## Determine the default sending buffer (according to the latency)

* Default queue.buffing setting,

    * `queue.buffering.max.messages=1000000`

    * `queue.buffering.max.kbytes=0x100000` (1GB)

* Normally, the default settings should be good enough.

## How could a message miss after send?

* The message might even not have been received by the partition leader! (with `acks=0`)

* Once the message received by the partition leader, the leader crashed just after responding to the producer, but has no chance to synchronize the message to other replicas. (with `acks=1`)

* Once the message received by the partition leader, the leader crashed just after responding to the producer, but with no in-sync replica to synchronize for the message. (with `acks=all`, while brokers are with `min.insync.replicas=1`)

## How does message disordering happen? How to avoid it?

* Take an example, -- a `KafkaAsyncProducer` just sent many messages, and a few of these messages (in the middle) failed to be delivered successfully. While the producer got the sending error, it might retry sending these messages again, thus causing the disordering.

* To avoid it. Two options,

    * Use a `KafkaSyncProducer`, but this would severely impact the throughput.

    * Embed some `sequence number` (e.g, record id, part of the `key`, etc) in the `ProducerRecord`, for de-duplication.

