# KafkaConsumer Quick Start

Generally speaking, The `Modern C++ Kafka API` is quite similar with [Kafka Java's API](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

We'd recommend users to cross-reference them, --especially the examples.

Unlike Java's KafkaConsumer, here we introduced two derived classes, --KafkaAutoCommitConsumer and KafkaManualCommitConsumer, --depending on whether users should call `commit` manually.

## KafkaConsumer (`enable.auto.commit=true`)

* Automatically commits previously polled offsets on each `poll` (and the final `close`) operations.

    * Note, the internal `offset commit` is asynchronous, and is not guaranteed to succeed. It's supposed to be triggered (within each `poll` operation) periodically, thus the occasional failure doesn't quite matter.

### Example
```cpp
        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers",  brokers},
            {"enable.auto.commit", "true"}
        });

        // Create a consumer instance
        kafka::clients::KafkaConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        // Read messages from the topic
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
        
        // consumer.close(); // No explicit close is needed, RAII will take care of it
```

* `bootstrap.servers` property is mandatory for a Kafka client.

* `subscribe` could take a topic list. It's a block operation, would wait the consumer to get partitions assigned.

* `poll` must be called periodically, thus to trigger kinds of callback handling internally. In practice, it could be put in a "while loop".

* At the end, we could `close` the consumer explicitly, or just leave it to the destructor.

## KafkaConsumer (`enable.auto.commit=false`)

* Users must commit the offsets for received records manually.

### Example
```cpp
        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers", brokers},
        });

        // Create a consumer instance
        kafka::clients::KafkaConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        auto lastTimeCommitted = std::chrono::steady_clock::now();

        // Read messages from the topic
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
                    std::cout << "% syncCommit offsets: " << kafka::utility::getCurrentTime() << std::endl;
                    consumer.commitSync(); // or commitAsync()

                    lastTimeCommitted = now;
                    allCommitted      = true;
                }
            }
        }

        // consumer.close(); // No explicit close is needed, RAII will take care of it
```

* The example is quite similar with the KafkaAutoCommitConsumer, with only 1 more line added for manual-commit.

* `commitSync` and `commitAsync` are both available for a KafkaManualConsumer. Normally, use `commitSync` to guarantee the commitment, or use `commitAsync`(with `OffsetCommitCallback`) to get a better performance.

## `KafkaConsumer` with `kafka::clients::KafkaClient::EventsPollingOption`

While we construct a `KafkaConsumer` with `kafka::clients::KafkaClient::EventsPollingOption::Auto` (i.e. the default option), an internal thread would be created for `OffsetCommit` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to process the messages and handle the `OffsetCommit` responses.

Here we have another choice, -- using `kafka::clients::KafkaClient::EventsPollingOption::Manual`, thus the `OffsetCommit` callbacks would be called within member function `pollEvents()`.

### Example
```cpp
    KafkaConsumer consumer(props, kafka::clients::KafkaClient::EventsPollingOption::Manual);

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

No exception would be thrown from a consumer's `poll` operation.

Instead, once an error occurs, the `Error` would be embedded in the `Consumer::ConsumerRecord`.

About `Error`'s `value()`s, there are 2 cases

1. Success

    - `RD_KAFKA_RESP_ERR__NO_ERROR` (`0`),  -- got a message successfully

    - `RD_KAFKA_RESP_ERR__PARTITION_EOF`,   -- reached the end of a partition (no message got)

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

    Excluding the user's main thread, if `enable.auto.commit` is `false`, the `KafkaConsumer` would start another (N + 2) threads in the background; otherwise, the `KafkaConsumer` would start (N + 3) background threads. (N means the number of BOOTSTRAP_SERVERS)

    1. Each broker (in the list of BOOTSTRAP_SERVERS) would take a seperate thread to transmit messages towards a kafka cluster server.

    2. Another 3 threads will handle internal operations, consumer group operations, and kinds of timers, etc.

    3. To enable the auto commit, one more thread would be create, which keeps polling/processing the offset-commit callback event.

    E.g, if a KafkaConsumer was created with property of `BOOTSTRAP_SERVERS=127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890`, it would take 6 threads in total (including the main thread).

* Which one of these threads will handle the callbacks?

    There are 2 kinds of callbacks for a KafkaConsumer,

    1. `RebalanceCallback` will be triggered internally by the user's thread, -- within the `poll` function.

    2. If `enable.auto.commit=true`, the `OffsetCommitCallback` will be triggered by the user's `poll` thread; otherwise, it would be triggered by a background thread.
