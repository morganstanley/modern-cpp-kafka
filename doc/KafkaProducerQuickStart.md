# KafkaProducer Quick Start

Generally speaking, The `modern-cpp-kafka API` is quite similar to the [Kafka Java's API](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

We'd recommend users to cross-reference them, --especially the examples.


## KafkaProducer

* The `send` is an unblock operation, and the result (including errors) could only be got from the delivery callback.

### [Example](https://github.com/morganstanley/modern-cpp-kafka/blob/main/examples/kafka_async_producer_not_copy_payload.cc)

```cpp
        using namespace kafka;
        using namespace kafka::clients::producer;

        // Create configuration object
        const Properties props ({
            {"bootstrap.servers",  {brokers}},
            {"enable.idempotence", {"true" }},
        });

        // Create a producer instance
        KafkaProducer producer(props);

        // Read messages from stdin and produce to the broker
        std::cout << "% Type message value and hit enter to produce message. (empty line to quit)" << std::endl;

        for (auto line = std::make_shared<std::string>();
             std::getline(std::cin, *line);
             line = std::make_shared<std::string>()) {
            // The ProducerRecord doesn't own `line`, it is just a thin wrapper
            auto record = ProducerRecord(topic,
                                         NullKey,
                                         Value(line->c_str(), line->size()));

            // Send the message
            producer.send(record,
                          // The delivery report handler
                          // Note: Here we capture the shared_pointer of `line`,
                          //       which holds the content for `record.value()`.
                          //       It makes sure the memory block is valid until the lambda finishes.
                          [line](const RecordMetadata& metadata, const Error& error) {
                              if (!error) {
                                  std::cout << "% Message delivered: " << metadata.toString() << std::endl;
                              } else {
                                  std::cerr << "% Message delivery failed: " << error.message() << std::endl;
                              }
                          });

            if (line->empty()) break;
        }

        // producer.close(); // No explicit close is needed, RAII will take care of it
```

* User must guarantee the memory block for `ProducerRecord`'s `key` is valid until being `send`.

* By default, the memory block for `ProducerRecord`'s `value` must be valid until the delivery callback is called; Otherwise, the `send` should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.

* It's guaranteed that the delivery callback would be triggered anyway after `send`, -- a producer would even be waiting for it before `close`. So, it's a good way to release these memory resources in the `Producer::Callback` function.


## About `enable.manual.events.poll`

While we construct a `KafkaProducer` with `enable.manual.events.poll=false` (the default option), an internal thread would be created for `MessageDelivery` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to send the messages and handle the `MessageDelivery` responses.

Here we have another choice, -- using `enable.manual.events.poll=true`, thus the `MessageDelivery` callbacks would be called within member function `pollEvents()`.

* Note: if you constructed the `KafkaProducer` with `enable.manual.events.poll=true`, the `send()` will be an `unblocked` operation even if the `message buffering queue` is full. In that case, the `send()` operation would throw an exception (or return an `error code` with the input reference parameter), -- instead of blocking there. And you might want to call `pollEvents()`, thus delivery-callback could be called for some messages (which could then be removed from the `message buffering queue`).

### Example

```cpp
    using namespace kafka;
    using namespace kafka::clients::producer;

    KafkaProducer producer(props.put("enable.manual.events.poll", "true"));

    // Prepare "msgsToBeSent"
    auto std::map<int, std::pair<Key, Value>> msgsToBeSent = ...;
    
    for (const auto& msg : msgsToBeSent) {
        auto record = ProducerRecord(topic, partition, msg.second.first, msg.second.second, msg.first);
        kafka::Error sendError;
        producer.send(sendError,
                      record,
                      // Ack callback
                      [&msg](const RecordMetadata& metadata, const Error& deliveryError) {
                           // the message could be identified by `metadata.recordId()`
                           if (deliveryError)  {
                               std::cerr << "% Message delivery failed: " << deliveryError.message() << std::endl;
                           } else {
                               msgsToBeSend.erase(metadata.recordId()); // Quite safe here
                           }
                       });
        if (sendError) break;
    }
    
    // Here we call the `MessageDelivery` callbacks
    // Note: we can only do this while the producer was constructed with `enable.manual.events.poll=true`.
    producer.pollEvents();
```

## `Headers` in `ProducerRecord`

* A `ProducerRecord` could take extra information with `headers`.

    * Note: the `header` within `headers` contains the pointer of the memory block for its `value`. The memory block __MUST__ be valid until the `ProducerRecord` is read by `producer.send()`.

### Example

```cpp
    using namespace kafka::clients;

    kafak::producer::KafkaProducer producer(props);

    auto record = producer::ProducerRecord(topic, partition, Key(), Value());

    for (const auto& msg : msgsToBeSent) {
        // Prepare record headers
        std::string session = msg.session;
        std::uint32_t seqno = msg.seqno;
        record.headers() = {
            { "session", { session.c_str(), session.size()} },
            { "seqno",   { &seqno, sizeof(seqno)} }
        };

        record.setKey(msg.key);
        record.setValue(msg.value);

        producer.send(record,
                      // Ack callback
                      [&msg](const kafka::producer::RecordMetadata& metadata, , const kafka::Error& error) {
                           if (error)  {
                               std::cerr << "% Message delivery failed: " << error.message() << std::endl;
                           }
                       });
    }
```

## To Make KafkaProducer Reliable

While using message dispatching systems, we always suffer from message lost, duplication and disordering.

Since the application (using the `KafkaProducer`) might crash/restart, we might consider using certain mechanism to achieve `At most once`/`At least once`, and `Ordering`, -- such as locally persisting the messages until successful delivery, using embedded sequence number to de-duplicate, or responding data-source to acknowledgement the delivery result, etc. These are common topics, which are not quite specific to Kafka. 

Here we'd focus on `KafkaProducer`, together with the `idempotence` feature. Let's see, in which cases problems might happen, how to avoid them, and what's the best practise,-- to achieve `No Message Lost`, `Exactly Once` and `Ordering`.

### No Message Lost

#### How could a message be lost even with successful delivery

* First, the `partition leader` doesn't sync-up the latest message to enough `in-sync replicas` before responding with the `acks`

    * The `partition leader` just don't need to wait for other `replica`s response

        - E.g, the producer is configured with `acks=1`

    * No available `in-sync replica` to wait for the response

        - E.g, all other replicas are not in-sync and brokers are configured with `min.insync.replicas=1`)

* Then, the `partition leader` crashes, and one `in-sync replica` becomes new `partition leader`

    * The new `partition leader` has no acknowledgement with the latest messages. Later, while new messages arrive, it would use conflicting record offsets (same with those records which the `partition leader` knows only). Then, even if the previous `partition leader` comes up again, these records have no chance to be recovered (just internally overwritten to be consistent with other replicas).

#### How to make sure __No Message Lost__

* Make sure the leader would wait for responses from all in-sync replicas before the response

    * Configuration `acks=all` is a __MUST__ for producer

* Ensure enough `In-Sync partition replicas`

    * Configuration `min.insync.replicas >= 2` is a __MUST__ for brokers

        - Take `min.insync.replicas = 2` for example, it means,

            1. At most `replication.factor - min.insync.replicas` replicas are out-of-sync, -- the producer would still be able to send messages, otherwise, it could fail with 'no enough replica' error, and keeps retrying.

            2. Occasionally no more than `min.insync.replicas` in-sync-replica failures. -- otherwise, messages might be missed. In this case, if just one in-sync replica crashes after sending back the ack to the producer, the message would not be lost; if two failed, it would! Since the new leader might be a replica which was not in-sync previously, and has no acknowledgement with these latest messages.

    * Please refer to [Kafka Broker Configuration](KafkaBrokerConfiguration.md) for more details.

    * Then, what would happen if replicas fail

        1. Fails to send (`not enough in-sync replica failure`), -- while number of `in-sync replicas` could not meet `min.insync.replication` 

        2. Lost messages (after sending messages), -- with no `in-sync replica` survived from multi-failures

        3. No message lost (while with all `in-sync replicas` acknowledged, and at least one `in-sync replica` available)

### Exactly Once & Ordering

#### How duplications happen

* After brokers successfully persisted a message, it sent the `ack` to the producer. But for some abnormal reasons (such as network failure, etc), the producer might fail to receive the `ack`. The `librdkafka`'s internal queue would retry, thus another (duplicated) message would be persisted by brokers.

#### How disordering happens within one partition

* The `librdkafka` uses internal partition queues, and once a message fails to be sent successfully(e.g, brokers are down), it would be put back on the queue and retries again while `retry.backoff.ms` expires. However, before that (retry with the failed message), the brokers might recover and the messages behind (if with configuration `max.in.flight > 1`) happened to be sent successfully. In this case (with configuration `max.in.flight > 1` and `retries > 0`), disordering could happen, and the user would not even be aware of it.

* Furthermore, while the last retry still failed, delivery callback would eventually be triggered. The user has to determine what to do for that (might want to re-send the message, etc). But there might be a case, -- some later messages had already been saved successfully by the server, thus no way to revert the disordering.

#### No ordering between partitions

* Make sure these `ProducerRecord`s be with the same partition

    - Explicitly assigned with the same `topic-partition`

    - Use the same `key` for these records

### Idempotent Producer

* The `enable.idempotence=true` configuration is highly **RECOMMENDED**.

* Please refer to the [document from librdkafkar](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#idempotent-producer) for more details.

* Note: the Kafka cluster should be configured with `min.insync.replicas=2` at least


## Error handling

`Error` might occur at different places while sending a message,

- A `KafkaException` would be triggered if `KafkaProducer` failed to trigger the `send` operation.

- Delivery `Error` would be passed through the delivery-callback.

There are 2 cases for the `Error` `value()`

* Local errors

    - `RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC`      -- The topic doesn't exist

    - `RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION`  -- The partition doesn't exist

    - `RD_KAFKA_RESP_ERR__INVALID_ARG`        -- Invalid topic (topic is null or the length is too long (>512))

    - `RD_KAFKA_RESP_ERR__MSG_TIMED_OUT`      -- No ack received within the time limit
    
    - `RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE`    -- The mesage size conflicts with local configuration `message.max.bytes`

* Broker errors

    - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
    
    - Typical errors are

        * Invalid message: `RD_KAFKA_RESP_ERR_CORRUPT_MESSAGE`, `RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE`, `RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS`, `RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT`, `RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE`.

        * Topic/Partition not exist: `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART`, -- automatic topic creation is disabled on the broker or the application is specifying a partition that does not exist.

        * Authorization failure: `RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED`, `RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED`


## Frequently Asked Questions

* About the automatic `topic creation`

    - If the cluster's configuration is with `auto.create.topics.enable=true`, the producer/consumer could trigger the brokers to create a new topic (with `send`, `subscribe`, etc)

    - Note: the default created topic may be not what you want (e.g, with `default.replication.factor=1` configuration as default, etc), thus causing other unexpected problems.

* How to enhance the sending performance?

    - Enlarging the default `batch.num.messages` and `linger.ms` might improve message batching, thus enhancing the throughput. (while, on the other hand, `linger.ms` would highly impact the latency)

    - The `queue.buffering.max.messages` and `queue.buffering.max.kbytes` would determine the `max in flight requests (some materials about Kafka would call it in this way)`. If the queue buffer is full, the `send` operation would be blocked. Larger `queue.buffering.max.messages`/`queue.buffering.max.kbytes` might help to improve throughput, while also means more messages locally buffering.

* How many threads would be created by a KafkaProducer?

    - Each broker (in the list of `bootstrap.servers`) would take a separate thread to transmit messages towards a kafka cluster server.

    - Another 2 threads would handle internal operations and kinds of timers, etc.

    - By default, `enable.manual.events.poll=false`, then one more background thread would be created, which keeps polling the delivery events and triggering the callbacks.
