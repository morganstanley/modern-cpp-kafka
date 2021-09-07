# How to Make KafkaProducer Reliable

While using message dispatching systems, we always suffer from message lost, duplication and disordering.

Since the application (using the `KafkaProducer`) might crash/restart, we might consider using certain mechanism to achieve `At most once`/`At least once`, and `Ordering`, -- such as locally persisting the messages until successful delivery, using embedded sequence number to de-duplicate, or responding data-source to acknowledgement the delivery result, etc. These are common topics, which are not quite specific to Kafka. 

Here we'd focus on `KafkaProducer`, together with the `idempotence` feature. Let's see, in which cases problems might happen, how to avoid them, and what's the best practise,-- to achieve `No Message Lost`, `Exactly Once` and `Ordering`.


## About `No Message Lost`

### When might a message actually be lost

* The producer gets a successful delivery response after sending the message, but the `partition leader` failed to sync it to other `replicas`.

### How could a message be lost even with successful delivery

* First, the `partition leader` doesn't sync-up the latest message to enough `in-sync replicas` before responding with the `ack`

    * The `partition leader` just don't need to wait for other `replica`s response

        - E.g, the producer is configured with `acks=1`

    * No available `in-sync replica` to wait for the response

        - E.g, all other replicas are not in-sync

* Then, the `partition leader` crashes, and one `in-sync replica` becomes new `partition leader`

    * The new `partition leader` has no acknowledgement with the latest messages. Later, while new messages arrive, it would use conflicting record offsets (same with those records which the `partition leader` knows only). Then, even if the previous `partition leader` comes up again, these records have no chance to be recovered (just internally overwritten to be consistent with other replicas).

### How to make sure `No Message Lost`

* Make sure the leader would wait for responses from all in-sync replicas before the response

    * Configuration `acks=all` is a MUST for producer

* Ensure enough `In-Sync partition replicas`

    * Configuration `min.insync.replicas >= 2` is a MUST for brokers

        - Take `min.insync.replicas = 2` for example, it means,

            1. At most `replication.factor - min.insync.replicas` replicas are out-of-sync, -- the producer would still be able to send messages, otherwise, it could fail with 'no enough replica' error, and keeps retrying.

            2. Occasionally no more than `min.insync.replicas` in-sync-replica failures. -- otherwise, messages might be missed. In this case, if just one in-sync replica crashes after sending back the ack to the producer, the message would not be lost; if two failed, it would! Since the new leader might be a replica which was not in-sync previously, and has no acknowledgement with these latest messages.

    * Please refer to [Kafka Broker Configuration](KafkaBrokerConfiguration.md) for more details.

    * Then, what would happen if replicas fail

        1. Fails to send (`not enough in-sync replica failure`), -- while number of `in-sync replicas` could not meet `min.insync.replication` 

        2. Lost messages (after sending messages), -- with no `in-sync replica` survived from multi-failures

        3. No message lost (while with all `in-sync replicas` acknowledged, and at least one `in-sync replica` available)


## About `Exactly Once`

### How duplications happen

* After brokers successfully persisted a message, it sent the `ack` to the producer. But for some abnormal reasons (such as network failure, etc), the producer might fail to receive the `ack`. The `librdkafka`'s internal queue would retry, thus another (duplicated) message would be persisted by brokers.

### How to guarantee `Exactly Once`

* The `enable.idempotence` configuration is RECOMMENDED.


## About `Ordering`

### No ordering between partitions

* Make sure these `ProducerRecord`s be with the same partition

    - Explicitly assigned with the same `topic-partition`

    - Use the same `key` for these records

### How disordering happens within one partition

* The `librdkafka` uses internal partition queues, and once a message fails to be sent successfully(e.g, brokers are down), it would be put back on the queue and retries again while `retry.backoff.ms` expires. However, before that (retry with the failed message), the brokers might recover and the messages behind (if with configuration `max.in.flight > 1`) happened to be sent successfully. In this case (with configuration `max.in.flight > 1` and `retries > 0`), disordering could happen, and the user would not even be aware of it.

* Furthermore, while the last retry still failed, delivery callback would eventually be triggered. The user has to determine what to do for that (might want to re-send the message, etc). But there might be a case, -- some later messages had already been saved successfully by the server, thus no way to revert the disordering.


## More About `Idempotent producer`

Please refer to the document from librdkafka, [Idempotent Producer](https://github.com/edenhill/librdkafka/blob/master/INTRODUCTION.md#idempotent-producer) for more details.

### Extra fields to maintain the message sequence

The `librdkafka` maintains the original produce() ordering per-partition for all messages produced, using an internal per-partition 64-bit counter called the `msgid` which starts at 1. This `msgid` allows messages to be re-inserted in the partition message queue in the original order in the case of retries.

The Idempotent Producer functionality in the Kafka protocol also has a per-message `sequence number`, which is a signed 32-bit wrapping counter that is reset each time the `Producer's ID (PID)` or `Epoch` changes.

The `msgid` is used, (along with a base `msgid` value stored at the time the `PID/Epoch` was bumped), to calculate the Kafka protocol's message `sequence number`.

### Configuration conflicts

* Since the following configuration properties are adjusted automatically (if not modified by the user). Producer instantiation will fail if user-supplied configuration is incompatible.

    - `acks = all`

    - `max.in.flight (i.e, `max.in.flight.requests.per.connection`) = 5`

    - `retries = INT32_MAX`

### Error handling

* Exception thrown during `send`

    * For these errors which could be detected locally (and could not be recovered with retrying), an exception would be thrown. E.g, invalid message, as RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE (conflicting with local configuration `message.max.bytes`).

* Permanent errors (respond from brokers)

    * Typical errors are:

        * Invalid message: RD_KAFKA_RESP_ERR_CORRUPT_MESSAGE, RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS, RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT, RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE.

        * Topic/Partition not exist: ERR_UNKNOWN_TOPIC_OR_PART, -- automatic topic creation is disabled on the broker or the application is specifying a partition that does not exist.

        * Authorization failure: ERR_TOPIC_AUTHORIZATION_FAILED, ERR_CLUSTER_AUTHORIZATION_FAILED

    * Normally, `Permanent error` means careless design, or wrong configuration, which should be avoided from the very beginning.

    * Unless with `enable.gapless.guarantee`(EXPERIMENTAL) configured, producer would keep going with the following messages; otherwise, it would purge all messages in-flight/in-queue (with RD_KAFKA_RESP_ERR__PURGE_INFLIGHT/RD_KAFKA_RESP_ERR__PURGE_QUEUE). 

* Temporary errors

    * Apart from those `permanent errors`, most of the left are temporary errors, which will be retried (if retry count permits); and while `message.timeout` expired, message delivery callback would be triggered with `RD_KAFKA_RESP_ERR__TIEMD_OUT`.

* Be careful with the `RD_KAFKA_RESP_ERR__TIEMD_OUT` failure

    * There's some corner cases, such as a message that has been persisted by brokers but `KafkaProducer` failed to get the response. If `message.timeout.ms` has not expired, the producer could retry and eventually get the response. Otherwise, (i.e, `message.timeout.ms` expired before the producer receives the successful `ack`), it would be considered as a delivery failure by the producer (while the brokers wouldn't). Users might re-transmit the message thus causing duplications.

    * To avoid this tricky situation, a longer `message.timeout.ms` is RECOMMENDED, to make sure there's enough time for transmission retries / on-flight responses.

### Performance impact

* The main impact comes from `max.in.flight=5` limitation. Currently, `max.in.flight` means `max.in.flight.per.connection`, -- that's 5 message batches (with size of ~1MB at the most) in flight (not get the `ack` response yet) at the most, towards per broker. Within low-latency networks, it would not be a problem; while in other cases, it might be! Good news is, there might be a plan (in `librdkafka`) to improve that `per.connection` limit to `per.partition`, thus boost the performance a lot.


## The best practice for `KafkaProducer`

* Enable `enable.idempotence` configuration

* Use a long `message.timeout.ms`, which would let `librdkafka` keep retrying, before triggering the delivery failure callback.


## Some examples

### `KafkaProducer` demo

```cpp
    std::atomic<bool> running = true;

    KafkaProducer producer(
        Properties({
            { ProducerConfig::BOOTSTRAP_SERVERS,  "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092" },
            { ProducerConfig::ENABLE_IDEMPOTENCE, "true" },
            { ProducerConfig::MESSAGE_TIMEOUT_MS, "86400000"}  // as long as 1 day
        })
    );

    while (running) {
        auto msg = fetchMsgFromUpstream();
        auto record = ProducerRecord(topic, msg.key, msg.value, msg.id);
        producer.send(record,
                      // Ack callback
                      [&msg](const Producer::RecordMetadata& metadata, std::error_code ec) {
                           // the message could be identified by `metadata.recordId()`
                           auto recordId = metadata.recordId();
                           if (ec)  {
                               std::cerr << "Cannot send out message with recordId: " << recordId << ", error:" << ec.message() << std::endl;
                           } else {
                               commitMsgToUpstream(recordId);
                           }
                       });
    }

    producer.close();
```

* With a long `message.timeout.ms`, we're not likely to catch an error with delivery callback, --it would retry for temporary errors anyway. But be aware with permanent errors, it might be caused by careless design.
