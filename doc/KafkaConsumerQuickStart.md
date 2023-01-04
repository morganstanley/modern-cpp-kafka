# KafkaConsumer Quick Start

Generally speaking, The `modern-cpp-kafka API` is quite similar with [Kafka Java's API](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

We'd recommend users to cross-reference them, --especially the examples.


## KafkaConsumer (with `enable.auto.commit=true`)

* Automatically commits previously polled offsets on each `poll` (and the final `close`) operations.

    * Note, the internal `offset commit` is asynchronous, and is not guaranteed to succeed. It's supposed to be triggered (within each `poll` operation) periodically, thus the occasional failure doesn't quite matter.

### [Example](https://github.com/morganstanley/modern-cpp-kafka/blob/main/examples/kafka_auto_commit_consumer.cc)

```cpp
        using namespace kafka;
        using namespace kafka::clients::consumer;

        // Create configuration object
        const Properties props ({
            {"bootstrap.servers", {brokers}},
        });

        // Create a consumer instance
        KafkaConsumer consumer(props);

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
                    std::cout << "    Headers  : " << toString(record.headers()) << std::endl;
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


## KafkaConsumer (with `enable.auto.commit=false`)

* Users must commit the offsets for received records manually.

### [Example](https://github.com/morganstanley/modern-cpp-kafka/blob/main/examples/kafka_manual_commit_consumer.cc)

```cpp
        // Create configuration object
        const Properties props ({
            {"bootstrap.servers",  {brokers}},
            {"enable.auto.commit", {"false" }}
        });

        // Create a consumer instance
        KafkaConsumer consumer(props);

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
                    std::cout << "    Headers  : " << toString(record.headers()) << std::endl;
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
                    std::cout << "% syncCommit offsets: " << utility::getCurrentTime() << std::endl;
                    consumer.commitSync(); // or commitAsync()

                    lastTimeCommitted = now;
                    allCommitted      = true;
                }
            }
        }

        // consumer.close(); // No explicit close is needed, RAII will take care of it
```

* The example is quite similar with the previous `enable.auto.commit=true` case, but has to call `commitSync`(or `commitAsync`) manually.

* `commitSync` and `commitAsync` are both available here. Normally, use `commitSync` to guarantee the commitment, or use `commitAsync`(with `OffsetCommitCallback`) to get a better performance.

## About `enable.manual.events.poll`

While we construct a `KafkaConsumer` with `enable.manual.events.poll=false` (i.e. the default option), an internal thread would be created for `OffsetCommit` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to process the messages and handle the `OffsetCommit` responses.

Here we have another choice, -- using `enable.manual.events.poll=true`, thus the `OffsetCommit` callbacks would be called within member function `pollEvents()`.

### Example

```cpp
    KafkaConsumer consumer(props.put("enable.manual.events.poll", "true"));

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
        // Note, we can only do this while the consumer was constructed with `enable.manual.events.poll=true`.
        consumer.pollEvents();
    }
```


## Error handling

Normally, exceptions might be thrown while operations fail, but not for a `poll` operation, -- if an error occurs, the `Error` would be embedded in the `Consumer::ConsumerRecord`.

About the `Error` `value()`, there are 2 cases

* Success

    - `RD_KAFKA_RESP_ERR__NO_ERROR` (`0`),  -- got a message successfully

    - `RD_KAFKA_RESP_ERR__PARTITION_EOF`,   -- reached the end of a partition (no message got)

* Failure

    - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)


## Frequently Asked Questions

* How to enhance the throughput

    * Try with a larger `queued.min.messages`, especially for small messages.

    * Use multiple KafkaConsumers to distribute the payload.

* How to avoid polling duplicated messages

    * To commit the offsets more frequently (e.g, always do commit after finishing processing a message).

    * Don't use quite a large `max.poll.records` for a `KafkaConsumer` (with `enable.auto.commit=true`) -- you might fail to commit all these messages before crash, thus more duplications with the next `poll`.

* How many threads would be created by a KafkaConsumer?

    * Each broker (in the list of `bootstrap.servers`) would take a seperate thread to transmit messages towards a kafka cluster server.

    * Another 3 threads will handle internal operations, consumer group operations, and kinds of timers, etc.

    * By default, `enable.manual.events.poll=false`, then one more background thread would be created, which keeps polling/processing the offset-commit callback event.

* Which one of these threads will handle the callbacks?

    * `RebalanceCallback` will be triggered internally by the user's thread, -- within the `poll` function.

    * If `enable.auto.commit=true`, the `OffsetCommitCallback` will be triggered by the user's `poll` thread; otherwise, it would be triggered by a background thread (if `enable.manual.events.poll=true`), or by the `pollEvents()` call (if `enable.manual.events.poll=false`).
