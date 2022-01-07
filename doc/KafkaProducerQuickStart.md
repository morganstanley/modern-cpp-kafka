# KafkaProducer Quick Start

Generally speaking, The `Modern C++ Kafka API` is quite similar to the [Kafka Java's API](https://kafka.apache.org/10/javadoc/org/apache/kafka/clients/producer/KafkaProducer.html).

We'd recommend users to cross-reference them, --especially the examples.

## KafkaProducer

* The `send` is an unblock operation, and the result (including errors) could only be got from the delivery callback.

### Example
```cpp
        using namespace kafka::clients;

        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers",  brokers},
            {"enable.idempotence", "true"},
        });

        // Create a producer instance
        KafkaProducer producer(props);

        // Read messages from stdin and produce to the broker
        std::cout << "% Type message value and hit enter to produce message. (empty line to quit)" << std::endl;

        for (auto line = std::make_shared<std::string>();
             std::getline(std::cin, *line);
             line = std::make_shared<std::string>()) {
            // The ProducerRecord doesn't own `line`, it is just a thin wrapper
            auto record = producer::ProducerRecord(topic,
                                                   kafka::NullKey,
                                                   kafka::Value(line->c_str(), line->size()));

            // Send the message
            producer.send(record,
                          // The delivery report handler
                          // Note: Here we capture the shared_pointer of `line`,
                          //       which holds the content for `record.value()`.
                          //       It makes sure the memory block is valid until the lambda finishes.
                          [line](const producer::RecordMetadata& metadata, const kafka::Error& error) {
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

## `KafkaProducer` with `kafka::clients::KafkaClient::EventsPollingOption`

While we construct a `KafkaProducer` with `kafka::clients::KafkaClient::EventsPollingOption::Auto` (the default option), an internal thread would be created for `MessageDelivery` callbacks handling.

This might not be what you want, since then you have to use 2 different threads to send the messages and handle the `MessageDelivery` responses.

Here we have another choice, -- using `kafka::clients::KafkaClient::EventsPollingOption::Manual`, thus the `MessageDelivery` callbacks would be called within member function `pollEvents()`.

* Note, if you constructed the `KafkaProducer` with `EventsPollingOption::Manual`, the `send()` would be an `unblocked` operation.
I.e, once the `message buffering queue` becomes full, the `send()` operation would throw an exception (or return an `error code` with the input reference parameter), -- instead of blocking there.
This makes sense, since you might want to call `pollEvents()` later, thus delivery-callback could be called for some messages (which could then be removed from the `message buffering queue`).

### Example
```cpp
    using namespace kafka::clients;

    KafkaProducer producer(props, KafkaClient::EventsPollingOption::Manual);

    // Prepare "msgsToBeSent"
    auto std::map<int, std::pair<Key, Value>> msgsToBeSent = ...;
    
    for (const auto& msg : msgsToBeSent) {
        auto record = producer::ProducerRecord(topic, partition, msg.second.first, msg.second.second, msg.first);
        kafka::Error sendError;
        producer.send(sendError,
                      record,
                      // Ack callback
                      [&msg](const producer::RecordMetadata& metadata, const kafka::Error& deliveryError) {
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
    // Note, we can only do this while the producer was constructed with `EventsPollingOption::MANUAL`.
    producer.pollEvents();
```

## Headers in ProducerRecord

* A `ProducerRecord` could take extra information with `headers`.

    * Note, the `header` within `headers` contains the pointer of the memory block for its `value`. The memory block MUST be valid until the `ProducerRecord` is read by `producer.send()`.

### Example
```cpp
    using namespace kafka::clients;

    kafak::KafkaProducer producer(props);

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
                      [&msg](const kafka::Producer::RecordMetadata& metadata, , const kafka::Error& error) {
                           if (error)  {
                               std::cerr << "% Message delivery failed: " << error.message() << std::endl;
                           }
                       });
    }
```

## Error handling

`Error` might occur at different places while sending a message,

1. A `KafkaException` would be triggered if `KafkaProducer` failed to trigger the send operation.

2. Delivery `Error` would be passed through the delivery-callback.

About `Error`'s `value()`s, there are 2 cases

1. Local errors,

    - `RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC`      -- The topic doesn't exist

    - `RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION`  -- The partition doesn't exist

    - `RD_KAFKA_RESP_ERR__INVALID_ARG`        -- Invalid topic (topic is null or the length is too long (>512))

    - `RD_KAFKA_RESP_ERR__MSG_TIMED_OUT`      -- No ack received within the time limit

2. Broker errors,

    - [Error Codes](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)

## Frequently Asked Questions

### What are the available configurations?

- [KafkaProducerConfiguration](KafkaClientConfiguration.md#kafkaproducer-configuration)

- [Inline doxygen page](../doxygen/classKAFKA__CPP__APIS__NAMESPACE_1_1ProducerConfig.html)

### About the automatic `topic creation`

If the cluster's configuration is with `auto.create.topics.enable=true`, the producer/consumer could trigger the brokers to create a new topic (with `send`, `subscribe`, etc)

Note, the default created topic may be not what you want (e.g, with `default.replication.factor=1` configuration as default, etc), thus causing other unexpected problems.

### How to enhance the sending performance?

Enlarging the default `BATCH_NUM_MESSAGES` and `LINGER_MS` might improve message batching, thus enhancing the throughput.

While, on the other hand, `LINGER_MS` would highly impact the latency.

The `QUEUE_BUFFERING_MAX_MESSAGES` and `QUEUE_BUFFERING_MAX_KBYTES` would determine the `max in flight requests (some materials about Kafka would call it in this way)`. If the queue buffer is full, the `send` operation would be blocked.

Larger `QUEUE_BUFFERING_MAX_MESSAGES`/`QUEUE_BUFFERING_MAX_KBYTES` might help to improve throughput as well, while also means more messages locally buffering.

### How to achieve reliable delivery

* Quick Answer,

    1. The Kafka cluster should be configured with `min.insync.replicas = 2` at least

    2. Configure the `KafkaProducer` with property `{ProducerConfig::ENABLE_IDEMPOTENCE, "true"}`, together with proper error-handling (within the delivery callback).

* Complete Answer,

    * [How to Make KafkaProducer Reliable](HowToMakeKafkaProducerReliable.md)

### How many threads would be created by a KafkaProducer?

Excluding the user's main thread, `KafkaProducer` would start (N + 3) background threads. (N means the number of BOOTSTRAP_SERVERS)

Most of these background threads are started internally by librdkafka.

Here is a brief introduction what they're used for,

1. Each broker (in the list of BOOTSTRAP_SERVERS) would take a separate thread to transmit messages towards a kafka cluster server.

2. Another 2 background threads would handle internal operations and kinds of timers, etc.

3. One more background thread to keep polling the delivery callback event.

E.g, if a `KafkaProducer` was created with property of `BOOTSTRAP_SERVERS=127.0.0.1:8888,127.0.0.1:8889,127.0.0.1:8890`, it would take 7 threads in total (including the main thread).

### Which one of these threads will handle the callbacks

It will be handled by a background thread, not by the user's thread.

Note, should be careful if both the `KafkaProducer::send()` and the `producer::Callback` might access the same container at the same time.

