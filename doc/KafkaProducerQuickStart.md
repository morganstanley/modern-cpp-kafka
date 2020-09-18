# KafkaProducer Quick Start

Generally speaking, The `Modern C++ based Kafka API` is quite similar to the [Kafka Java's API](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

We'd recommend users to cross-reference them, --especially the examples.

Unlike Java's KafkaProducer, here we introduce two derived classes, -- `KafkaSyncProducer` and `KafkaAsyncProducer` --depending on different `send` behaviors (synchronous/asynchronous).

## KafkaSyncProducer

* The "Sync" (in the name) means `send` is a blocking operation, and it will immediately get the RecordMetadata while the function returns. If anything wrong occurs, an exception would be thrown.

### Example
```cpp
    ProducerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345"));

    KafkaSyncProducer producer(props);

    // Prepare "msgsToBeSent", and an empty "msgsFailedToBeSent" as well

    for (const auto& msg : msgsToBeSent) {
        auto record = ProducerRecord(topic, partition, msg.key, msg.value);
        try {
            producer.send(record);
        } catch (const KafkaException& e) {
            LOG_ERROR("Cannot send out message with err={0}", e.what());
            msgsFailedToBeSent.emplace(msg); // Push it back to another list to handle with them later
        }
    }

    producer.close(); // Not mandatory (destructor of the producer would call it anyway)
```

* `ProducerConfig::BOOTSTRAP_SERVERS` is mandatory for ProducerConfig.

* `ProducerRecord` would not take any ownership for the `key` or `value`. Thus, the user must guarantee the memory block (pointed by `key` or `value`) is valid until being `send`.

* Since `send` is a blocking operation, the throughput will be highly impacted, but it is easier to make sure of the message delivery and logically it is simpler.

* At the end, the user could call `close` manually, or just leave it to the destructor (`close` would be called anyway).

## KafkaAsyncProducer

* The `Async` (in the name) means `send` is an unblocking operation, and the result (including errors) could only be got from the delivery callback.

### Example
```cpp
    ProducerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345");

    KafkaAsyncProducer producer(props);

    // Prepare "msgsToBeSent", and an empty "msgsFailedToBeSent" as well

    for (const auto& msg : msgsToBeSent) {
        auto record = ProducerRecord(topic, partition, msg.key, msg.value);
        producer.send(record,
                      // Ack callback
                      [&msg](const Producer::RecordMetadata& metadata, std::error_code ec) {
                           // the message could be identified by `metadata.recordId()`
                           if (ec)  {
                               LOG_ERROR("Cannot send out message with recordId={0}", metadata.recordId());
                               msgsFailedToBeSent.emplace(msg); // Push it back to another list to handle with them later
                           }
                       });
    }

    producer.close(); // Not mandatory (destructor of the producer would call it anyway)
```

* Same with KafkaSyncProducer, the user must guarantee the memory block for `ProducerRecord`'s `key` is valid until being `send`.

* By default, the memory block for `ProducerRecord`'s `value` must be valid until the delivery callback is called; Otherwise, the `send` should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.

* It's guaranteed that the delivery callback would be triggered anyway after `send`, -- a producer would even be waiting for it before `close`. So, it's a good way to release these memory resources in the `Producer::Callback` function.

## KafkaAsyncProducer with `KafkaClient::EventsPollingOption::MANUAL`

While we construct a `KafkaAsyncProducer` with option `KafkaClient::EventsPollingOption::AUTO` (default), an internal thread would be created for `MessageDelivery` callbacks handling. 

This might not be what you want, since then you have to use 2 different threads to send the messages and handle the `MessageDelivery` responses.

Here we have another choice, -- using `KafkaClient::EventsPollingOption::MANUAL`, thus the `MessageDelivery` callbacks would be called within member function `pollEvents()`.

* Note, if you constructed the `KafkaAsyncProducer` with `EventsPollingOption::MANUAL`, the `send()` would be an `unblocked` operation.
I.e, once the `message buffering queue` becomes full, the `send()` operation would throw an exception (or return an `error code` with the input reference parameter), -- instead of blocking there.
This makes sense, since you might want to call `pollEvents()` later, thus delivery-callback could be called for some messages (which could then be removed from the `message buffering queue`).

### Example
```cpp
    ProducerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345");

    KafkaAsyncProducer producer(props, KafkaClient::EventsPollingOption::MANUAL);

    // Prepare "msgsToBeSent"
    auto std::map<int, std::pair<Key, Value>> msgsToBeSent = ...;
    
    for (const auto& msg : msgsToBeSent) {
        auto record = ProducerRecord(topic, partition, msg.second.first, msg.second.second, msg.first);
        std::error_code error;
        producer.send(error,
                      record,
                      // Ack callback
                      [&msg](const Producer::RecordMetadata& metadata, std::error_code ec) {
                           // the message could be identified by `metadata.recordId()`
                           if (ec)  {
                               LOG_ERROR("Cannot send out message with recordId={0}", metadata.recordId());
                           } else {
                               msgsToBeSend.erase(metadata.recordId()); // Quite safe here
                           }
                       });
        if (error) break;
    }
    
    // Here we call the `MessageDelivery` callbacks
    // Note, we can only do this while the producer was constructed with `EventsPollingOption::MANUAL`.
    producer.pollEvents();

    producer.close();
```

## Idempotent Producer

The way to make a `KafkaProducer` be `Idempotent` is really simple, just adding one single line of configuration -- `{ProducerConfig::ENABLE_IDEMPOTENCE, "true"}` would be enough.

Note: The `ProducerConfig::ENABLE_IDEMPOTENCE` configuration would internally set some default values for related properties, such as `{ProducerConfig::ACKS, "all"}`, `{ProducerConfig::MAX_IN_FLIGHT, "5"}`, etc. Thus suggest not to set them explicitly to avoid configuration conflict. 

## Headers in ProducerRecord

* A `ProducerRecord` could take extra information with `headers`.

    * Note, the `header` within `headers` contains the pointer of the memory block for its `value`. The memory block MUST be valid until the `ProducerRecord` is read by `producer.send()`.

### Example
```cpp
    KafkaAsyncProducer producer(props);

    auto record = ProducerRecord(topic, partition, Key(), Value());

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
                      [&msg](const Producer::RecordMetadata& metadata, std::error_code ec) {
                           if (ec)  {
                               LOG_ERROR("Cannot send out message: {0}, err: {1}", metadata.toString(), ec);
                           }
                       });
    }
```

## Error handling

Once an error occurs during `send()`, `KafkaSyncProducer` and `KafkaAsyncProducer` behave differently.

1. `KafkaSyncProducer` gets `std::error_code` by catching exceptions (with `error()` member function).

2. `KafkaAsyncProducer` gets `std::error_code` with delivery-callback (with a parameter of the callback function).

There are 2 kinds of possible errors,

1. Local errors,

    - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC      -- The topic doesn't exist

    - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION  -- The partition doesn't exist

    - RD_KAFKA_RESP_ERR__INVALID_ARG        -- Invalid topic (topic is null or the length is too long (>512))

    - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT      -- No ack received within the time limit

2. Broker errors,

    - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)

## Frequently Asked Questions

### What are the available configurations?

- [KafkaProducerConfiguration](KafkaClientConfiguration.md#kafkaproducer-configuration)

- [Inline doxygen page](../doxygen/classKAFKA__CPP__APIS__NAMESPACE_1_1ProducerConfig.html)

### About the automatic `topic creation`

If the cluster's configuration is with `auto.create.topics.enable=true`, the producer/consumer could trigger the brokers to create a new topic (with `send`, `subscribe`, etc)

Note, the default created topic may be not what you want (e.g, with `default.replication.factor=1` configuration as default, etc), thus causing other unexpected problems.

### What will happen after `ack` timeout?

If an ack failed to be received within `MESSAGE_TIMEOUT_MS`, an exception would be thrown for a KafkaSyncSend, or, an error code would be received by the delivery callback for a KafkaAsyncProducer.

### How to enhance the sending performance?

Enlarging the default `BATCH_NUM_MESSAGES` and `LINGER_MS` might improve message batching, thus enhancing the throughput.

While, on the other hand, `LINGER_MS` would highly impact the latency.

The `QUEUE_BUFFERING_MAX_MESSAGES` and `QUEUE_BUFFERING_MAX_KBYTES` would determine the `max in flight requests (some materials about Kafka would call it in this way)`. If the queue buffer is full, the `send` operation would be blocked.

Larger `QUEUE_BUFFERING_MAX_MESSAGES`/`QUEUE_BUFFERING_MAX_KBYTES` might help to improve throughput as well, while also means more messages locally buffering.

### How to achieve reliable delivery

* Quick Answer,

    1. The Kafka cluster should be configured with `min.insync.replicas = 2` at least

    2. Use a `KafkaSyncProducer` (with configuration `{ProducerConfig::ACKS, "all"}`); or use a `KafkaAsyncProducer` (with configuration `{ProducerConfig::ENABLE_IDEMPOTENCE, "true"}`), together with proper error-handling within the delivery callbacks. 

* Complete Answer,

    * [How to Make KafkaProducer Reliable](HowToMakeKafkaProducerReliable.md)

### How many threads would be created by a KafkaProducer?

Excluding the user's main thread, KafkaSyncProducer would start another (N + 2) threads in the background, while `KafkaAsyncProducer` would start (N + 3) background threads. (N means the number of BOOTSTRAP_SERVERS)

Most of these background threads are started internally by librdkafka.

Here is a brief introduction what they're used for,

1. Each broker (in the list of BOOTSTRAP_SERVERS) would take a separate thread to transmit messages towards a kafka cluster server.

2. Another 2 background threads would handle internal operations and kinds of timers, etc.

3. `KafkaAsyncProducer` has one more background thread to keep polling the delivery callback event.

E.g, if a KafkaSyncProducer was created with property of `BOOTSTRAP_SERVERS=127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890`, it would take 6 threads in total (including the main thread).

### Which one of these threads will handle the callbacks

The `Producer::Callback` is only available for a `KafkaAsyncProducer`.

It will be handled by a background thread, not by the user's thread.

Note, should be careful if both the `KafkaAsyncProducer::send()` and the `Producer::Callback` might access the same container at the same time.

