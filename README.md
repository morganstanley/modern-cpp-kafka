# About the `Modern C++ based Kafka API`

## Introduction

The `Modern C++ based Kafka API` is a layer of C++ wrapper based on [librdkafka](https://github.com/edenhill/librdkafka) (the C part), with high quality, but more friendly to users.

Here is the [doxygen documentation for developers](doxygen/annotated.html).

- At present, the `Modern C++ based Kafka API` is compatible with `librdkafka` v1.6.0.

KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by **modern-cpp-kafka**. **modern-cpp-kafka** has no
affiliation with and is not endorsed by The Apache Software Foundation.

## Why it's here

The `librdkafka` is a robust high performance C/C++ library, widely used and well maintained.

Unfortunately, the C++ interface of `librdkafka` is not quite object-oriented or user-friendly, since it has to be confined to C++ 98 for compatibility.

To make the life easier, we worked out the `Modern C++ based Kafka API`, -- a header-only library that uses idiomatic C++ features to provide a safe, efficient and easy to use way of producing and consuming Kafka messages.

## Features

* Java-like APIs

    * Here're some reference links for Java's native kafka clients, -- much helpful for cross-reference

        [org.apache.kafka.clients.producer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/producer/package-summary.html)

        [org.apache.kafka.clients.consumer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/package-summary.html)

        [org.apache.kafka.clients.admin](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/admin/package-summary.html)

* Robust

    * Verified with kinds of test cases, which cover many abnormal scenarios (edge cases)

        * Stability test with unstable brokers

        * Memory leak check for failed client with on-flight messages

        * Client failure and taking over, etc.

* Efficient

    * No extra performance cost (No deep copy introduced internally)

    * Much better (2~4 times throughput) performance result than those native language (Java/Scala) implementation, in most commonly used cases (message size: 256 B ~ 2 KB)

* Headers only

    * No extra library required to link

## Build

* To build its `tools`/`tests/examples`, you should

    * Specify library locations with environment variables

        * `LIBRDKAFKA_ROOT`                 -- `librdkafka` headers and libraries

        * `GTEST_ROOT`                      -- `googletest` headers and libraries

        * `BOOST_ROOT`                      -- `boost` headers and libraries

        * `SASL_LIBRARYDIR`/`SASL_LIBRARY`  -- if SASL connection support is wanted

    * Create an empty directory for the build, and `cd` to it

    * Build commands

        * Type `cmake path-to-project-root`

        * Type `make` (could follow build options with `-D`)

            * `BUILD_OPTION_USE_ASAN=ON`      -- Use Address Sanitizer

            * `BUILD_OPTION_USE_TSAN=ON`      -- Use Thread Sanitizer

            * `BUILD_OPTION_USE_UBSAN=ON`     -- Use Undefined Behavior Sanitizer

            * `BUILD_OPTION_CLANG_TIDY=ON`    -- Enable clang-tidy checking

            * `BUILD_OPTION_GEN_DOC=ON`       -- Generate documentation as well

            * `BUILD_OPTION_DOC_ONLY=ON`       -- Only generate documentation

            * `BUILD_OPTION_GEN_COVERAGE=ON`  -- Generate test coverage, only support by clang currently

        * Type `make install`

## Install

    * The APIs is headers only

        * Just need to include the `include/kafka` directory in your project

    * The compiler should support

        * Option 1: C++17

        * Option 2: C++14, together with `boost` headers (would depend on `boost::optional` in the case)

## Start-up

* Prepare the servers (ZooKeeper/Kafka cluster)

    * [Start the servers](https://kafka.apache.org/documentation/#quickstart_startserver)

* [KafkaProducer Quick Start](doc/KafkaProducerQuickStart.md)

* [KafkaConsumer Quick Start](doc/KafkaConsumerQuickStart.md)

* [KafkaClient Configuration](doc/KafkaClientConfiguration.md)

## How to achieve good availability & performance

* [Kafka Broker Configuration](doc/KafkaBrokerConfiguration.md)

* [Good Practices to Use KafkaProducer](doc/GoodPracticesToUseKafkaProducer.md)

* [Good Practices to Use KafkaConsumer](doc/GoodPracticesToUseKafkaConsumer.md)

* [How to Make KafkaProducer Reliable](doc/HowToMakeKafkaProducerReliable.md)

