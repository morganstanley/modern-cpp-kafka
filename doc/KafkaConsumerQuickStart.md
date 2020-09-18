# KafkaConsumer Quick Start

Generally speaking, The `Modern C++ based Kafka API` is quite similar with [Kafka Java's API](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

We'd recommend users to cross-reference them, --especially the examples.

Unlike Java's KafkaConsumer, here we introduced two derived classes, --KafkaAutoCommitConsumer and KafkaManualCommitConsumer, --depending on whether users should call `commit` manually.

## KafkaAutoCommitConsumer

* Friendly for users, --would not care about when to commit the offsets for these received messages.

* Internally, it would commit the offsets (for received records) within the next `poll` and the final `close`.
Note, each internal `commit` would "try its best", but "not guaranteed to succeed", -- it's supposed to be called periodically, thus occasional failure doesn't matter.

### Example
```cpp
    ConsumerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345");

    KafkaAutoCommitConsumer consumer(props);

    consumer.subscribe({"topic1", "topic2"});

    while (true) {
        auto records = consumer.poll(1000);
        for (auto& record: records) {
            // Process the message...
            process(record);
        }
    }
```

* `ConsumerConfig::BOOTSTRAP_SERVERS` is mandatory for `ConsumerConfig`.

* `subscribe` could take a topic list. And it's a blocking operation, -- would return after the rebalance event triggered callback was executed.

* `poll` must be periodically called, and it would trigger kinds of callback handling internally. As in this example, just put it in a "while loop" would be OK.

* At the end, the user could `close` the consumer manually, or just leave it to the destructor (which would `close` anyway).

## KafkaManualCommitConsumer

* Users must commit the offsets for received records manually.

### Example
```cpp
    ConsumerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345");

    KafkaManualCommitConsumer consumer(props);

    consumer.subscribe({"topic1", "topic2"});

    while (true) {
        auto records = consumer.poll(1000);
        for (auto& record: records) {
            // Process the message...
            process(record);

            // Then, must commit the offset manually
            consumer.commitSync(*record);
        }
    }
```

* The example is quite similar with the KafkaAutoCommitConsumer, with only 1 more line added for manual-commit.

* `commitSync` and `commitAsync` are both available for a KafkaManualConsumer. Normally, use `commitSync` to guarantee the commitment, or use `commitAsync`(with `OffsetCommitCallback`) to get a better performance.

## KafkaManualCommitConsumer with `KafkaClient::EventsPollingOption::MANUAL`

While we construct a `KafkaManualCommitConsumer` with option `KafkaClient::EventsPollingOption::AUTO` (default), an internal thread would be created for `OffsetCommit` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to process the messages and handle the `OffsetCommit` responses.

Here we have another choice, -- using `KafkaClient::EventsPollingOption::MANUAL`, thus the `OffsetCommit` callbacks would be called within member function `pollEvents()`.

### Example
```cpp
    ConsumerConfig props;
    props.put(ConsumerConfig::BOOTSTRAP_SERVERS, "127.0.0.1:1234,127.0.0.1:2345");

    KafkaManualCommitConsumer consumer(props, KafkaClient::EventsPollingOption::MANUAL);

    consumer.subscribe({"topic1", "topic2"});

    while (true) {
        auto records = consumer.poll(1000);
        for (auto& record: records) {
            // Process the message...
            process(record);

            // Here we commit the offset manually
            consumer.commitSync(*record);
        }

        // Here we call the `OffsetCommit` callbacks
        // Note, we can only do this while the consumer was constructed with `EventsPollingOption::MANUAL`.
        consumer.pollEvents();
    }
```

## Error handling

No exception would be thrown by `KafkaProducer::poll()`.

Once an error occurs, the `ErrorCode` would be embedded in the `Consumer::ConsumerRecord`.

There're 2 cases,

1. Success

    - RD_KAFKA_RESP_ERR__NO_ERROR (0),    -- got a message successfully

    - RD_KAFKA_RESP_ERR__PARTITION_EOF,   -- reached the end of a partition (no message got)

2. Failure

    - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)

## Frequently Asked Questions

* What're the available configurations?

    - [KafkaProducerConfiguration](KafkaClientConfiguration.md#kafkaconsumer-configuration)

    - [Inline doxygen page](../doxygen/classKAFKA__CPP__APIS__NAMESPACE_1_1ConsumerConfig.html)

* How to enhance the polling performance?

    `ConsumerConfig::QUEUED_MIN_MESSAGES` determines how frequently the consumer would send the FetchRequest towards brokers.
    The default configuration (i.e, 100000) might not be good enough for small (less than 1KB) messages, and suggest using a larger value (e.g, 1000000) for it.

* How many threads would be created by a KafkaConsumer?

    Excluding the user's main thread, `KafkaAutoCommitConsumer` would start another (N + 2) threads in the background, while `KafkaManualConsumer` would start (N + 3) background threads. (N means the number of BOOTSTRAP_SERVERS)

    1. Each broker (in the list of BOOTSTRAP_SERVERS) would take a seperate thread to transmit messages towards a kafka cluster server.

    2. Another 3 threads will handle internal operations, consumer group operations, and kinds of timers, etc.

    3. KafkaManualConsumer has one more thread, which keeps polling the offset-commit callback event.

    E.g, if a KafkaAutoCommitConsumer was created with property of `BOOTSTRAP_SERVERS=127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890`, it would take 6 threads in total (including the main thread).

* Which one of these threads will handle the callbacks?

    There are 2 kinds of callbacks for a KafkaConsumer,

    1. `RebalanceCallback` will be triggered internally by the user's thread, -- within the `poll` function.

    2. `OffsetCommitCallback` (only available for `KafkaManualCommitConsumer`) will be triggered by a background thread, not by the user's thread.

