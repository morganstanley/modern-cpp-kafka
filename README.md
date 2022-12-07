# About the *Modern C++ Kafka API*

![Lifecycle Active](https://badgen.net/badge/Lifecycle/Active/green)  

## Introduction

The [Modern C++ Kafka API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html) is a layer of C++ wrapper based on [librdkafka](https://github.com/edenhill/librdkafka) (the C part), with high quality, but more friendly to users.

- By now, [modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka) is compatible with [librdkafka v1.9.2](https://github.com/edenhill/librdkafka/releases/tag/v1.9.2).

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

        * `LIBRDKAFKA_INCLUDE_DIR`          -- ***librdkafka*** headers

        * `LIBRDKAFKA_LIBRARY_DIR`          -- ***librdkafka*** libraries

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

    * `Properties` for Kafka clients

        * `Properties` is a map which contains all configuration info needed to initialize a Kafka client. These configuration items are key-value pairs, -- the "key" is a `std::string`, while the "value" could be a `std::string`, a `std::function<...>`, or an `Interceptors`.

            * K-V Types: `std::string` -> `std::string`

                * Most are identical with [librdkafka configuration](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)

                * But with Exceptions

                    * Default Value Changes

                        * `log_level`(default: `5`): default was `6` from **librdkafka**

                        * `client.id` (default: random string): no default string from **librdkafka**

                        * `group.id` (default: random string, for `KafkaConsumer` only): no default string from **librdkafka**

                    * Additional Options

                        * `enable.manual.events.poll` (default: `false`): To poll the (offset-commit/message-delivery callback) events manually

                        *  `max.poll.records` (default: `500`, for `KafkaConsumer` only): The maxmum number of records that a single call to `poll()` would return

                    * Ignored Options

                        * `enable.auto.offset.store`: ***modern-cpp-kafka*** will save the offsets in its own way

                        * `auto.commit.interval.ms`: ***modern-cpp-kafka*** will not commit the offsets periodically, instead, it would do it in the next `poll()`.


            * K-V Types: `std::string` -> `std::function<...>`

                * `log_cb` -> `LogCallback` (`std::function<void(int, const char*, int, const char* msg)>`)

                * `error_cb` -> `ErrorCallback` (`std::function<void(const Error&)>`)

                * `stats_cb` -> `StatsCallback` (`std::function<void(const std::string&)>`)

                * `oauthbearer_token_refresh_cb` -> `OauthbearerTokenRefreshCallback` (`std::function<SaslOauthbearerToken(const std::string&)>`)

            * K-V Types: `std::string` -> `Interceptors`

                * `interceptors`: takes `Interceptors` as the value type

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

