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
        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers", brokers},
        });

        // Create a consumer instance.
        kafka::KafkaAutoCommitConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        // Read messages from the topic.
        std::cout << "% Reading messages from topic: " << topic << std::endl;
        while (true) {
            auto records = consumer.poll(std::chrono::milliseconds(100));
            for (const auto& record: records) {
                // In this example, quit on empty message
                if (record.value().size() == 0) return 0;

                if (!record.error()) {
                    std::cout << "% Got a new message..." << std::endl;
                    std::cout << "    Topic    : " << record.topic() << std::endl;
                    std::cout << "    Partition: " << record.partition() << std::endl;
                    std::cout << "    Offset   : " << record.offset() << std::endl;
                    std::cout << "    Timestamp: " << record.timestamp().toString() << std::endl;
                    std::cout << "    Headers  : " << kafka::toString(record.headers()) << std::endl;
                    std::cout << "    Key   [" << record.key().toString() << "]" << std::endl;
                    std::cout << "    Value [" << record.value().toString() << "]" << std::endl;
                } else {
                    std::cerr << record.toString() << std::endl;
                }
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
        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers", brokers},
        });

        // Create a consumer instance.
        kafka::KafkaManualCommitConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        auto lastTimeCommitted = std::chrono::steady_clock::now();

        // Read messages from the topic.
        std::cout << "% Reading messages from topic: " << topic << std::endl;
        bool allCommitted = true;
        bool running      = true;
        while (running) {
            auto records = consumer.poll(std::chrono::milliseconds(100));
            for (const auto& record: records) {
                // In this example, quit on empty message
                if (record.value().size() == 0) {
                    running = false;
                    break;
                }

                if (!record.error()) {
                    std::cout << "% Got a new message..." << std::endl;
                    std::cout << "    Topic    : " << record.topic() << std::endl;
                    std::cout << "    Partition: " << record.partition() << std::endl;
                    std::cout << "    Offset   : " << record.offset() << std::endl;
                    std::cout << "    Timestamp: " << record.timestamp().toString() << std::endl;
                    std::cout << "    Headers  : " << kafka::toString(record.headers()) << std::endl;
                    std::cout << "    Key   [" << record.key().toString() << "]" << std::endl;
                    std::cout << "    Value [" << record.value().toString() << "]" << std::endl;

                    allCommitted = false;
                } else {
                    std::cerr << record.toString() << std::endl;
                }
            }

            if (!allCommitted) {
                auto now = std::chrono::steady_clock::now();
                if (now - lastTimeCommitted > std::chrono::seconds(1)) {
                    // Commit offsets for messages polled
                    std::cout << "% syncCommit offsets: " << kafka::Utility::getCurrentTime() << std::endl;
                    consumer.commitSync(); // or commitAsync()

                    lastTimeCommitted = now;
                    allCommitted      = true;
                }
            }
        }
```

* The example is quite similar with the KafkaAutoCommitConsumer, with only 1 more line added for manual-commit.

* `commitSync` and `commitAsync` are both available for a KafkaManualConsumer. Normally, use `commitSync` to guarantee the commitment, or use `commitAsync`(with `OffsetCommitCallback`) to get a better performance.

## KafkaManualCommitConsumer with `KafkaClient::EventsPollingOption::Manual`

While we construct a `KafkaManualCommitConsumer` with option `KafkaClient::EventsPollingOption::AUTO` (default), an internal thread would be created for `OffsetCommit` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to process the messages and handle the `OffsetCommit` responses.

Here we have another choice, -- using `KafkaClient::EventsPollingOption::Manual`, thus the `OffsetCommit` callbacks would be called within member function `pollEvents()`.

### Example
```cpp
    KafkaManualCommitConsumer consumer(props, KafkaClient::EventsPollingOption::Manual);

    consumer.subscribe({"topic1", "topic2"});

    while (true) {
        auto records = consumer.poll(std::chrono::milliseconds(100));
        for (auto& record: records) {
            // Process the message...
            process(record);

            // Here we commit the offset manually
            consumer.commitSync(*record);
        }

        // Here we call the `OffsetCommit` callbacks
        // Note, we can only do this while the consumer was constructed with `EventsPollingOption::Manual`.
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

