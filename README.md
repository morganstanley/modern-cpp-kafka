# About the *Modern C++ Kafka API*

![Lifecycle Active](https://badgen.net/badge/Lifecycle/Active/green)  

## Introduction

The [Modern C++ Kafka API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html) is a layer of C++ wrapper based on [librdkafka](https://github.com/edenhill/librdkafka) (the C part), with high quality, but more friendly to users.

- By now, [modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka) is compatible with [librdkafka v1.9.0](https://github.com/edenhill/librdkafka/releases/tag/v1.9.0).

```
KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by modern-cpp-kafka. modern-cpp-kafka has no
affiliation with and is not endorsed by The Apache Software Foundation.
```

## Why it's here

The ***librdkafka*** is a robust high performance C/C++ library, widely used and well maintained.

Unfortunately, to maintain C++98 compatibility, the C++ interface of ***librdkafka*** is not quite object-oriented or user-friendly.

Since C++ is evolving quickly, we want to take advantage of new C++ features, thus make the life easier for developers. And this led us to create a new C++ API for Kafka clients.

Eventually, we worked out the ***modern-cpp-kafka***, -- a header-only library that uses idiomatic C++ features to provide a safe, efficient and easy to use way of producing and consuming Kafka messages.

## Features

* Header-only

    * Easy to deploy, and no extra library required to link

* Ease of Use

    * Interface/Naming matches the Java API

    * Object-oriented

    * RAII is used for lifetime management

    * ***librdkafka***'s polling and queue management is now hidden

* Robust

    * Verified with kinds of test cases, which cover many abnormal scenarios (edge cases)

        * Stability test with unstable brokers

        * Memory leak check for failed client with in-flight messages

        * Client failure and taking over, etc.

* Efficient

    * No extra performance cost (No deep copy introduced internally)

    * Much better (2~4 times throughput) performance result than those native language (Java/Scala) implementation, in most commonly used cases (message size: 256 B ~ 2 KB)


## Build

* No need to build for installation

* To build its `tools`/`tests`/`examples`, you should

    * Specify library locations with environment variables

        * `LIBRDKAFKA_ROOT`                 -- ***librdkafka*** headers and libraries

        * `GTEST_ROOT`                      -- ***googletest*** headers and libraries

        * `BOOST_ROOT`                      -- ***boost*** headers and libraries

        * `SASL_LIBRARYDIR`/`SASL_LIBRARY`  -- if SASL connection support is wanted

        * `RAPIDJSON_INCLUDE_DIRS`          -- `addons/KafkaMetrics` requires **rapidjson** headers

    * Create an empty directory for the build, and `cd` to it

    * Build commands

        * Type `cmake path-to-project-root`

        * Type `make` (could follow build options with `-D`)

            * `BUILD_OPTION_USE_ASAN=ON`      -- Use Address Sanitizer

            * `BUILD_OPTION_USE_TSAN=ON`      -- Use Thread Sanitizer

            * `BUILD_OPTION_USE_UBSAN=ON`     -- Use Undefined Behavior Sanitizer

            * `BUILD_OPTION_CLANG_TIDY=ON`    -- Enable clang-tidy checking

            * `BUILD_OPTION_GEN_DOC=ON`       -- Generate documentation as well

            * `BUILD_OPTION_DOC_ONLY=ON`      -- Only generate documentation

            * `BUILD_OPTION_GEN_COVERAGE=ON`  -- Generate test coverage, only support by clang currently

        * Type `make install`

## Install

* Include the `include/kafka` directory in your project

* To work together with ***modern-cpp-kafka*** API, the compiler should support

    * Option 1: C++17

    * Option 2: C++14 (with pre-requirements)

        * Need ***boost*** headers (for `boost::optional`)

        * GCC only (with optimization, e.g. -O2)

## How to Run Tests

* Unit test (`tests/unit`)

    * The test could be run with no Kafka cluster depolyed

* Integration test (`tests/integration`)

    * The test should be run with Kafka cluster depolyed

    * The environment variable `KAFKA_BROKER_LIST` should be set

        * E.g. `export KAFKA_BROKER_LIST=127.0.0.1:29091,127.0.0.1:29092,127.0.0.1:29093`

* Robustness test (`tests/robustness`)

    * The test should be run with Kafka cluster depolyed locally

    * The environment variable `KAFKA_BROKER_LIST` should be set

    * The environment variable `KAFKA_BROKER_PIDS` should be set

        * Make sure the test runner gets the privilege to stop/resume the pids

        * E.g. `export KAFKA_BROKER_PIDS=61567,61569,61571`

* Additional settings for clients

    * The environment variable `KAFKA_CLIENT_ADDITIONAL_SETTINGS` could be used for customized test environment

        * Especially for Kafka cluster with SASL(or SSL) connections

        * E.g. `export KAFKA_CLIENT_ADDITIONAL_SETTINGS="security.protocol=SASL_PLAINTEXT;sasl.kerberos.service.name=...;sasl.kerberos.keytab=...;sasl.kerberos.principal=..."`

## To Start

* Tutorial

    * Confluent Blog [Debuting a Modern C++ API for Apache Kafka](https://www.confluent.io/blog/modern-cpp-kafka-api-for-safe-easy-messaging)

    * [KafkaProducer Quick Start](doc/KafkaProducerQuickStart.md)

    * [KafkaConsumer Quick Start](doc/KafkaConsumerQuickStart.md)

* User's Manual

    * [Kafka Client API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html)


    * Kafka Client Properties

        * In most cases, the `Properties` settings for ***modern-cpp-kafka*** are identical with [librdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

        * With following exceptions

            * KafkaConsumer

                * Properties with random string as default

                    * `client.id`

                    * `group.id`

                * More properties than ***librdkafka***

                    * `max.poll.records` (default: `500`): The maxmum number of records that a single call to `poll()` would return

                * Property which overrides the one from ***librdkafka***

                    * `enable.auto.commit` (default: `false`): To automatically commit the previously polled offsets on each `poll` operation

                * Properties not supposed to be used (internally shadowed by ***modern-cpp-kafka***)

                    * `enable.auto.offset.store`

                    * `auto.commit.interval.ms`

            * KafkaProducer

                * Properties with random string as default

                    * `client.id`

            * Log level

                * The default `log_level` is `NOTICE` (`5`) for all these clients

* Test Environment (ZooKeeper/Kafka cluster) Setup

    * [Start the servers](https://kafka.apache.org/documentation/#quickstart_startserver)


## How to Achieve High Availability & Performance

* [Kafka Broker Configuration](doc/KafkaBrokerConfiguration.md)

* [Good Practices to Use KafkaProducer](doc/GoodPracticesToUseKafkaProducer.md)

* [Good Practices to Use KafkaConsumer](doc/GoodPracticesToUseKafkaConsumer.md)

* [How to Make KafkaProducer Reliable](doc/HowToMakeKafkaProducerReliable.md)


## Other References

* Java API for Kafka clients

    * [org.apache.kafka.clients.producer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/producer/package-summary.html)

    * [org.apache.kafka.clients.consumer](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/consumer/package-summary.html)

    * [org.apache.kafka.clients.admin](https://kafka.apache.org/22/javadoc/org/apache/kafka/clients/admin/package-summary.html)

