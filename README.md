About the *Modern C++ Kafka API*
=================================

![Lifecycle Active](https://badgen.net/badge/Lifecycle/Active/green)  


The [modern-cpp-kafka API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html) is a layer of ***C++*** wrapper based on [librdkafka](https://github.com/confluentinc/librdkafka) (the ***C*** part only), with high quality, but more friendly to users.

- By now, [modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka) is compatible with [librdkafka v2.0.2](https://github.com/confluentinc/librdkafka/releases/tag/v2.0.2).


```
KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by modern-cpp-kafka. modern-cpp-kafka has no
affiliation with and is not endorsed by The Apache Software Foundation.
```



# Why it's here

The ***librdkafka*** is a robust high performance C/C++ library, widely used and well maintained.

Unfortunately, to maintain ***C++98*** compatibility, the ***C++*** interface of ***librdkafka*** is not quite object-oriented or user-friendly.

Since C++ is evolving quickly, we want to take advantage of new C++ features, thus making life easier for developers. And this led us to create a new C++ API for Kafka clients.

Eventually, we worked out the ***modern-cpp-kafka***, -- a ***header-only*** library that uses idiomatic ***C++*** features to provide a safe, efficient and easy to use way of producing and consuming Kafka messages.



# Features

* __Header-only__

    * Easy to deploy, and no extra library required to link

* __Ease of Use__

    * Interface/Naming matches the Java API

    * Object-oriented

    * RAII is used for lifetime management

    * ***librdkafka***'s polling and queue management is now hidden

* __Robust__

    * Verified with kinds of test cases, which cover many abnormal scenarios (edge cases)

        * Stability test with unstable brokers

        * Memory leak check for failed client with in-flight messages

        * Client failure and taking over, etc.

* __Efficient__

    * No extra performance cost (No deep copy introduced internally)

    * Much better (2~4 times throughput) performance result than those native language (Java/Scala) implementation, in most commonly used cases (message size: 256 B ~ 2 KB)



# Installation / Requirements

* Just include the [`include/kafka`](https://github.com/morganstanley/modern-cpp-kafka/tree/main/include/kafka) directory for your project

* The compiler should support ***C++17***

    * Or, ***C++14***, but with pre-requirements

        - Need ***boost*** headers (for `boost::optional`)

        - For ***GCC*** compiler, it needs optimization options (e.g. `-O2`)

* Dependencies

    * [**librdkafka**](https://github.com/confluentinc/librdkafka) headers and library (only the C part)

        - Also see the [requirements from **librdkafka**](https://github.com/confluentinc/librdkafka#requirements)

    * [**rapidjson**](https://github.com/Tencent/rapidjson) headers: only required by `addons/KafkaMetrics.h`



# User Manual

* [Release Notes](https://github.com/morganstanley/modern-cpp-kafka/releases)

* [Class List](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html)


## Properties

[kafka::Properties Class Reference](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1Properties.html)

* It is a map which contains all configuration info needed to initialize a Kafka client, and it's **the only** parameter needed for a constructor.

* The configuration items are ***key-value*** pairs, -- the type of ***key*** is always `std::string`, while the type for a ***value*** could be one of the followings

    * `std::string`

        * Most items are identical with [**librdkafka** configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

        * But with exceptions

            * Default value changes

                | Key String  | Default       | Description                                               |
                | ----------- | ------------- | --------------------------------------------------------- |
                | `log_level` | `5`           | Default was `6` from **librdkafka**                       |
                | `client.id` | random string | No default from **librdkafka**                            |
                | `group.id`  | random string | (for `KafkaConsumer` only) No default from **librdkafka** |-

            * Additional options

                | Key String                  | Default       | Description                                                                                         |
                | --------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
                | `enable.manual.events.poll` | `false`       | To poll the (offset-commit/message-delivery callback) events manually                               |
                | `max.poll.records`          | `500`         | (for `KafkaConsumer` only) The maximum number of records that a single call to `poll()` would return |

            * Ignored options

                | Key String                  | Explanation                                                                        |
                | --------------------------- | ---------------------------------------------------------------------------------- |
                | `enable.auto.offset.store`  | ***modern-cpp-kafka*** will save the offsets in its own way                        |
                | `auto.commit.interval.ms`   | ***modern-cpp-kafka*** will only commit the offsets within each `poll()` operation |

    * `std::function<...>`

        * For kinds of callbacks

        | Key String                     | Value Type                                                                                    |
        | ------------------------------ | --------------------------------------------------------------------------------------------- |
        | `log_cb`                       | `LogCallback` (`std::function<void(int, const char*, int, const char* msg)>`)                 |
        | `error_cb`                     | `ErrorCallback` (`std::function<void(const Error&)>`)                                         |
        | `stats_cb`                     | `StatsCallback` (`std::function<void(const std::string&)>`)                                   |
        | `oauthbearer_token_refresh_cb` | `OauthbearerTokenRefreshCallback` (`std::function<SaslOauthbearerToken(const std::string&)>`) |

    * `Interceptors`

        * To intercept thread start/exit events, etc.

        | Key String     | Value Type     |
        | -------------- | -------------- |
        | `interceptors` | `Interceptors` |

### Examples

1.
    ```
    std::string brokers = "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092";

    kafka::Properties props ({
        {"bootstrap.servers",  {brokers}},
        {"enable.idempotence", {"true"}},
    });
    ```

2.
    ```
    kafka::Properties props;
    props.put("bootstrap.servers", brokers);
    props.put("enable.idempotence", "true");
    ```
* Note: `bootstrap.servers` is the only mandatory property for a Kafka client


## KafkaProducer

[kafka::clients::producer::KafkaProducer Class Reference](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1clients_1_1producer_1_1KafkaProducer.html)

### A Simple Example

Here's a very simple example to see how to send a message with a `KafkaProducer`.

```
#include <kafka/KafkaProducer.h>

#include <cstdlib>
#include <iostream>
#include <string>


int main()
{
    using namespace kafka;
    using namespace kafka::clients::producer;

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // Prepare the configuration
    const Properties props({{"bootstrap.servers", brokers}});

    // Create a producer
    KafkaProducer producer(props);

    // Prepare a message
    std::cout << "Type message value and hit enter to produce message..." << std::endl;
    std::string line;
    std::getline(std::cin, line);

    ProducerRecord record(topic, NullKey, Value(line.c_str(), line.size()));

    // Prepare delivery callback
    auto deliveryCb = [](const RecordMetadata& metadata, const Error& error) {
        if (!error) {
            std::cout << "Message delivered: " << metadata.toString() << std::endl;
        } else {
            std::cerr << "Message failed to be delivered: " << error.message() << std::endl;
        }
    };

    // Send a message
    producer.send(record, deliveryCb);

    // Close the producer explicitly(or not, since RAII will take care of it)
    producer.close();
}
```

#### Notes

* The `send()` is an unblocked operation unless the message buffering queue is full. 

* Make sure the memory block for `ProducerRecord`'s `key` is valid until the `send` is called.

* Make sure the memory block for `ProducerRecord`'s `value` is valid until the message delivery callback is called (unless the `send` is with option `KafkaProducer::SendOption::ToCopyRecordValue`).

* It's guaranteed that the message delivery callback would be triggered anyway after `send`, -- a producer would even be waiting for it before close.

* At the end, we could close Kafka client (i.e. `KafkaProducer` or `KafkaConsumer`) explicitly, or just leave it to the destructor. 

### The Lifecycle of the Message

The message for the KafkaProducer is called `ProducerRecord`, it contains `Topic`, `Partition` (optional), `Key` and `Value`. Both `Key` & `Value` are `const_buffer`, and since there's no deep-copy for the `Value`, the user should make sure the memory block for the `Value` be valid, until the delivery callback has been executed.

In the previous example, we don't need to worry about the lifecycle of `Value`, since the content of the `line` keeps to be available before closing the producer, and all message delivery callbacks would be triggered before finishing closing the producer.

#### Example for shared_ptr

A trick is capturing the shared pointer (for the memory block of `Value`) in the message delivery callback.

```
    std::cout << "Type message value and hit enter to produce message... (empty line to quit)" << std::endl;

    // Get input lines and forward them to Kafka
    for (auto line = std::make_shared<std::string>();
         std::getline(std::cin, *line);
         line = std::make_shared<std::string>()) {

        // Empty line to quit
        if (line->empty()) break;

        // Prepare a message
        ProducerRecord record(topic, NullKey, Value(line->c_str(), line->size()));

        // Prepare delivery callback
        // Note: Here we capture the shared pointer of `line`, which holds the content for `record.value()`
        auto deliveryCb = [line](const RecordMetadata& metadata, const Error& error) {
            if (!error) {
                std::cout << "Message delivered: " << metadata.toString() << std::endl;
            } else {
                std::cerr << "Message failed to be delivered: " << error.message() << std::endl;
            }
        };

        // Send the message
        producer.send(record, deliveryCb);
    }
```

#### Example for deep-copy

The option `KafkaProducer::SendOption::ToCopyRecordValue` could be used for `producer.send(...)`, thus the memory block of `record.value()` would be copied into the internal sending buffer.

```
    std::cout << "Type message value and hit enter to produce message... (empty line to quit)" << std::endl;

    // Get input lines and forward them to Kafka
    for (std::string line; std::getline(std::cin, line); ) {

        // Empty line to quit
        if (line.empty()) break;

        // Prepare a message
        ProducerRecord record(topic, NullKey, Value(line.c_str(), line.size()));

        // Prepare delivery callback
        auto deliveryCb = [](const RecordMetadata& metadata, const Error& error) {
            if (!error) {
                std::cout << "Message delivered: " << metadata.toString() << std::endl;
            } else {
                std::cerr << "Message failed to be delivered: " << error.message() << std::endl;
            }
        };

        // Send the message (deep-copy the payload)
        producer.send(record, deliveryCb, KafkaProducer::SendOption::ToCopyRecordValue);
    }
```

### Embed More Info in a `ProducerRecord`

Besides the `payload` (i.e. `value()`), a `ProducerRecord` could also put extra info in its `key()` & `headers()`.

`Headers` is a vector of `Header` which contains `kafka::Header::Key` (i.e. `std::string`) and `kafka::Header::Value` (i.e. `const_buffer`).

#### Example

```
    const kafka::Topic     topic     = "someTopic";
    const kafka::Partition partition = 0;

    const std::string key       = "some key";
    const std::string value     = "some payload";

    const std::string category  = "categoryA";
    const std::size_t sessionId = 1;

    {
        kafka::clients::producer::ProducerRecord record(topic,
                                                        partition,
                                                        kafka::Key{key.c_str(), key.size()},
                                                        kafka::Value{value.c_str(), value.size()});

        record.headers() = {{
            kafka::Header{kafka::Header::Key{"Category"},  kafka::Header::Value{category.c_str(), category.size()}},
            kafka::Header{kafka::Header::Key{"SessionId"}, kafka::Header::Value{&sessionId, sizeof(sessionId)}}
        }};

        std::cout << "ProducerRecord: " << record.toString() << std::endl;
    }
```

### About `enable.manual.events.poll`

By default, `KafkaProducer` would be constructed with `enable.manual.events.poll=false` configuration.
That means, a background thread would be created, which keeps polling the events (thus calls the message delivery callbacks)

Here we have another choice, -- using `enable.manual.events.poll=true`, thus the MessageDelivery callbacks would be called within member function `pollEvents()`.

* Note: in this case, the send() will be an unblocked operation even if the message buffering queue is full, -- it would throw an exception (or return an error code with the input reference parameter), instead of blocking there.

#### Example

```
    // Prepare the configuration (with "enable.manual.events.poll=true")
    const Properties props({{"bootstrap.servers",         {brokers}},
                            {"enable.manual.events.poll", {"true" }}});

    // Create a producer
    KafkaProducer producer(props);

    std::cout << "Type message value and hit enter to produce message... (empty line to finish)" << std::endl;

    // Get all input lines
    std::list<std::shared_ptr<std::string>> messages;
    for (auto line = std::make_shared<std::string>(); std::getline(std::cin, *line) && !line->empty();) {
        messages.emplace_back(line);
    }

    while (!messages.empty()) {
        // Pop out a message to be sent
        auto payload = messages.front();
        messages.pop_front();

        // Prepare the message
        ProducerRecord record(topic, NullKey, Value(payload->c_str(), payload->size()));

        // Prepare the delivery callback
        // Note: if fails, the message will be pushed back to the sending queue, and then retries later
        auto deliveryCb = [payload, &messages](const RecordMetadata& metadata, const Error& error) {
            if (!error) {
                std::cout << "Message delivered: " << metadata.toString() << std::endl;
            } else {
                std::cerr << "Message failed to be delivered: " << error.message() << ", will be retried later" << std::endl;
                messages.emplace_back(payload);
            }
        };

        // Send the message
        producer.send(record, deliveryCb);

        // Poll events (e.g. message delivery callback)
        producer.pollEvents(std::chrono::milliseconds(0));
    }
```

### Error Handling

[`kafka::Error`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1Error.html) might occur at different places while sending a message,

* A [`kafka::KafkaException`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1KafkaException.html) would be triggered if `KafkaProducer` fails to call the `send` operation.

* Delivery [`kafka::Error`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1Error.html) could be fetched via the delivery-callback.

* The `kafka::Error::value()` for failures

    * Local errors

        - `RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC`      -- The topic doesn't exist

        - `RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION`  -- The partition doesn't exist

        - `RD_KAFKA_RESP_ERR__INVALID_ARG`        -- Invalid topic (topic is null or the length is too long (>512))

        - `RD_KAFKA_RESP_ERR__MSG_TIMED_OUT`      -- No ack received within the time limit

        - `RD_KAFKA_RESP_ERR_INVALID_MSG_SIZE`    -- The message size conflicts with local configuration `message.max.bytes`

    * Broker errors

        - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)

        - Typical errors are

            * Invalid message: `RD_KAFKA_RESP_ERR_CORRUPT_MESSAGE`, `RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE`, `RD_KAFKA_RESP_ERR_INVALID_REQUIRED_ACKS`, `RD_KAFKA_RESP_ERR_UNSUPPORTED_FOR_MESSAGE_FORMAT`, `RD_KAFKA_RESP_ERR_RECORD_LIST_TOO_LARGE`.

            * Topic/Partition not exist: `RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART`, -- automatic topic creation is disabled on the broker or the application is specifying a partition that does not exist.

            * Authorization failure: `RD_KAFKA_RESP_ERR_TOPIC_AUTHORIZATION_FAILED`, `RD_KAFKA_RESP_ERR_CLUSTER_AUTHORIZATION_FAILED`

### Idempotent Producer

The `enable.idempotence=true` configuration is highly RECOMMENDED.

#### Example

```
        kafka::Properties props;
        props.put("bootstrap.servers", brokers);
        props.put("enable.idempotence", "true");

        // Create an idempotent producer
        kafka::clients::producer::KafkaProducer producer(props);
```

* Note: please refer to the [document from **librdkafka**](https://github.com/confluentinc/librdkafka/blob/master/INTRODUCTION.md#idempotent-producer) for more details.


## Kafka Consumer

[kafka::clients::consumer::KafkaConsumer Class Reference](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1clients_1_1consumer_1_1KafkaConsumer.html)

### A Simple Example

```
#include <kafka/KafkaConsumer.h>

#include <cstdlib>
#include <iostream>
#include <signal.h>
#include <string>

std::atomic_bool running = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running) {
        running = false;
    } else {
        // Restore the signal handler, -- to avoid stuck with this handler
        signal(SIGINT, SIG_IGN); // NOLINT
    }
}

int main()
{
    using namespace kafka;
    using namespace kafka::clients::consumer;

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning);    // NOLINT

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // Prepare the configuration
    const Properties props({{"bootstrap.servers", {brokers}}});

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Subscribe to topics
    consumer.subscribe({topic});

    while (running) {
        // Poll messages from Kafka brokers
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            if (!record.error()) {
                std::cout << "Got a new message..." << std::endl;
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

    // No explicit close is needed, RAII will take care of it
    consumer.close();
}
```

* By default, the `KafkaConsumer` is constructed with property `enable.auto.commit=true`

    * It means it will automatically commit previously polled offsets on each poll (and the final close) operations.

        * Note: the internal offset commit is asynchronous, which is not guaranteed to succeed. Since the operation is supposed to be triggered (again) at a later time (within each `poll`), thus the occasional failure doesn't matter.

* `subscribe` could take a topic list. It's a block operation, and would wait for the consumer to get partitions assigned.

* `poll` must be called periodically, thus to trigger kinds of callback handling internally. In practice, it could be put in a `while loop`.

### Rebalance events

The `KafkaConsumer` could specify the `RebalanceCallback` while it subscribes the topics, and the callback will be triggered while partitions are assigned or revoked.

#### Example

```
    // The consumer would read all messages from the topic and then quit.

    // Prepare the configuration
    const Properties props({{"bootstrap.servers",    {brokers}},
                            // Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event
                            // whenever the consumer reaches the end of a partition.
                            {"enable.partition.eof", {"true"}},
                            // Action to take when there is no initial offset in offset store
                            // it means the consumer would read from the very beginning
                            {"auto.offset.reset",    {"earliest"}}});

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Prepare the rebalance callbacks
    std::atomic<std::size_t> assignedPartitions{};
    auto rebalanceCb = [&assignedPartitions](kafka::clients::consumer::RebalanceEventType et, const kafka::TopicPartitions& tps) {
                           if (et == kafka::clients::consumer::RebalanceEventType::PartitionsAssigned) {
                               assignedPartitions += tps.size();
                               std::cout << "Assigned partitions: " << kafka::toString(tps) << std::endl;
                           } else {
                               assignedPartitions -= tps.size();
                               std::cout << "Revoked partitions: " << kafka::toString(tps) << std::endl;
                           }
                       };

    // Subscribe to topics with rebalance callback
    consumer.subscribe({topic}, rebalanceCb);

    TopicPartitions finishedPartitions;
    while (finishedPartitions.size() != assignedPartitions.load()) {
        // Poll messages from Kafka brokers
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            if (!record.error()) {
                std::cerr << record.toString() << std::endl;
            } else {
                if (record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    // Record the partition which has been reached the end
                    finishedPartitions.emplace(record.topic(), record.partition());
                } else {
                    std::cerr << record.toString() << std::endl;
                }
            }
        }
    }
```

### To Commit Offset Manually

Once the KafkaConsumer is configured with `enable.auto.commit=false`, the user has to find out the right places to call `commitSync(...)`/`commitAsync(...)`.

#### Example

```
    // Prepare the configuration
    Properties props({{"bootstrap.servers", {brokers}}});
    props.put("enable.auto.commit", "false");

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Subscribe to topics
    consumer.subscribe({topic});

    while (running) {
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            std::cout << record.toString() << std::endl;
        }

        if (!records.empty()) {
            consumer.commitAsync();
        }
    }

    consumer.commitSync();

    // No explicit close is needed, RAII will take care of it
    // consumer.close();
```

### Error Handling

* Normally, [`kafka::KafkaException`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1KafkaException.html) will be thrown if an operation fails.

* But if the `poll` operation fails, the [`kafka::Error`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1Error.html) would be embedded in the [`kafka::clients::consumer::ConsumerRecord`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1clients_1_1consumer_1_1ConsumerRecord.html).

* There're 2 cases for the `kafka::Error::value()`

    * Success

        * `RD_KAFKA_RESP_ERR__NO_ERROR` (`0`), -- got a message successfully

        * `RD_KAFKA_RESP_ERR__PARTITION_EOF` (`-191`), -- reached the end of a partition (no message got)

    * Failure

        * [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)


## Callbacks for KafkaClient

We're free to set callbacks in `Properties` with a `kafka::clients::ErrorCallback`, `kafka::clients::LogCallback`, or `kafka::clients::StatsCallback`.

#### Example

```
    // Prepare the configuration
    Properties props({{"bootstrap.servers", {brokers}}});

    // To print out the error
    props.put("error_cb", [](const kafka::Error& error) {
                              // https://en.wikipedia.org/wiki/ANSI_escape_code
                              std::cerr << "\033[1;31m" << "[" << kafka::utility::getCurrentTime() << "] ==> Met Error: " << "\033[0m";
                              std::cerr << "\033[4;35m" << error.toString() << "\033[0m" << std::endl;
                          });

    // To enable the debug-level log
    props.put("log_level", "7");
    props.put("debug", "all");
    props.put("log_cb", [](int /*level*/, const char* /*filename*/, int /*lineno*/, const char* msg) {
                            std::cout << "[" << kafka::utility::getCurrentTime() << "]" << msg << std::endl;
                        });

    // To enable the statistics dumping
    props.put("statistics.interval.ms", "1000");
    props.put("stats_cb", [](const std::string& jsonString) {
                              std::cout << "Statistics: " << jsonString << std::endl;
                          });
```


## Thread Model

* Number of Background Threads within a Kafka Client

    * __N__ threads for the message transmission (towards __N__ brokers).

    * __2__ (for `KafkaProducer`) / __3__ (for `KafkaConsumer`) threads to handle internal operations, timers, consumer group operations, etc.

    * __1__ thread for (message-delivery/offset-commit) callback events polling, -- the thread only exists while the client is configured with `enable.manual.events.poll=false` (the default config)

* Which Thread Handles the Callbacks

    * `consumer::RebalanceCallback`: the thread which calls  `consumer.poll(...)`

    * `consumer::OffsetCommitCallback`

        * While `enable.manual.events.poll=false`: the thread which calls `consumer.pollEvents(...)`

        * While `enable.manual.events.poll=true`: the background (events polling) thread

    * `producer::Callback`

        * While `enable.manual.events.poll=false`: the thread which calls `producer.pollEvents(...)`

        * While `enable.manual.events.poll=true`: the background (events polling) thread



# For Developers

## Build (for [tests](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests)/[tools](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tools)/[examples](https://github.com/morganstanley/modern-cpp-kafka/tree/main/examples))

* Specify library locations with environment variables

    | Environment Variable             | Description                                              | 
    | -------------------------------- | -------------------------------------------------------- |
    | `LIBRDKAFKA_INCLUDE_DIR`         | ***librdkafka*** headers                                 |
    | `LIBRDKAFKA_LIBRARY_DIR`         | ***librdkafka*** libraries                               |
    | `GTEST_ROOT`                     | ***googletest*** headers and libraries                   |
    | `BOOST_ROOT`                     | ***boost*** headers and libraries                        |
    | `SASL_LIBRARYDIR`/`SASL_LIBRARY` | [optional] for SASL connection support                    |
    | `RAPIDJSON_INCLUDE_DIRS`         | `addons/KafkaMetrics.h` requires ***rapidjson*** headers |

* Build commands

    * `cd empty-folder-for-build`

    * `cmake path-to-project-root` (following options could be used with `-D`)

        | Build Option                     | Description                                                   |
        | -------------------------------- | ------------------------------------------------------------- |
        | `BUILD_OPTION_USE_TSAN=ON`       | Use Thread Sanitizer                                          |
        | `BUILD_OPTION_USE_ASAN=ON`       | Use Address Sanitizer                                         |
        | `BUILD_OPTION_USE_UBSAN=ON`      | Use Undefined Behavior Sanitizer                              |
        | `BUILD_OPTION_CLANG_TIDY=ON`     | Enable clang-tidy checking                                    |
        | `BUILD_OPTION_GEN_DOC=ON`        | Generate documentation as well                                |
        | `BUILD_OPTION_DOC_ONLY=ON`       | Only generate documentation                                   |
        | `BUILD_OPTION_GEN_COVERAGE=ON`   | Generate test coverage, only support by clang currently       |

    * `make`

    * `make install` (to install `tools`)


## Run Tests

* Kafka cluster setup

    * [Quick Start For Cluster Setup](https://kafka.apache.org/documentation/#quickstart)

    * [Cluster Setup Scripts For Test](https://github.com/morganstanley/modern-cpp-kafka/blob/main/scripts/start-local-kafka-cluster.py)

    * [Kafka Broker Configuration](doc/KafkaBrokerConfiguration.md)

* To run the binary, the test runner requires following environment variables

    | Environment Variable               | Descrioption                                                | Example                                                                    |
    | ---------------------------------- | ----------------------------------------------------------- | -------------------------------------------------------------------------- |
    | `KAFKA_BROKER_LIST`                | The broker list for the Kafka cluster                       | `export KAFKA_BROKER_LIST=127.0.0.1:29091,127.0.0.1:29092,127.0.0.1:29093` |
    | `KAFKA_BROKER_PIDS`                | The broker PIDs for test runner to manipulate               | `export KAFKA_BROKER_PIDS=61567,61569,61571`                               |
    | `KAFKA_CLIENT_ADDITIONAL_SETTINGS` | Could be used for addtional configuration for Kafka clients | `export KAFKA_CLIENT_ADDITIONAL_SETTINGS="security.protocol=SASL_PLAINTEXT;sasl.kerberos.service.name=...;sasl.kerberos.keytab=...;sasl.kerberos.principal=..."` |

    * The environment variable `KAFKA_BROKER_LIST` is mandatory for integration/robustness test, which requires the Kafka cluster.

    * The environment variable `KAFKA_BROKER_PIDS` is mandatory for robustness test, which requires the Kafka cluster and the privilege to stop/resume the brokers.

    | Test Type                                                                                          | `KAFKA_BROKER_LIST`  | `KAFKA_BROKER_PIDS` |
    | -------------------------------------------------------------------------------------------------- | -------------------- | ------------------- |
    | [tests/unit](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/unit)               | -                    | -                   |
    | [tests/integration](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/integration) | Required             | -                   |
    | [tests/robustness](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/robustness)   | Required             | Required            |

