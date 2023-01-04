# About the *Modern C++ Kafka API*

![Lifecycle Active](https://badgen.net/badge/Lifecycle/Active/green)  


The [modern-cpp-kafka API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html) is a layer of ***C++*** wrapper based on [librdkafka](https://github.com/confluentinc/librdkafka) (the ***C*** part only), with high quality, but more friendly to users.

- By now, [modern-cpp-kafka](https://github.com/morganstanley/modern-cpp-kafka) is compatible with [librdkafka v1.9.2](https://github.com/confluentinc/librdkafka/releases/tag/v1.9.2).


```
KAFKA is a registered trademark of The Apache Software Foundation and
has been licensed for use by modern-cpp-kafka. modern-cpp-kafka has no
affiliation with and is not endorsed by The Apache Software Foundation.
```


## Why it's here

The ***librdkafka*** is a robust high performance C/C++ library, widely used and well maintained.

Unfortunately, to maintain ***C++98*** compatibility, the ***C++*** interface of ***librdkafka*** is not quite object-oriented or user-friendly.

Since C++ is evolving quickly, we want to take advantage of new C++ features, thus make the life easier for developers. And this led us to create a new C++ API for Kafka clients.

Eventually, we worked out the ***modern-cpp-kafka***, -- a ***header-only*** library that uses idiomatic ***C++*** features to provide a safe, efficient and easy to use way of producing and consuming Kafka messages.


## Features

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


## Installation / Requirements

* Just include the [`include/kafka`](https://github.com/morganstanley/modern-cpp-kafka/tree/main/include/kafka) directory for your project

* The compiler should support ***C++17***

    * Or, ***C++14***, but with pre-requirements

        - Need ***boost*** headers (for `boost::optional`)

        - For ***GCC*** compiler, it needs optimization options (e.g. `-O2`)

* Dependencies

    * [**librdkafka**](https://github.com/confluentinc/librdkafka) headers and library (only the C part)

        - Also see the [requirements from **librdkafka**](https://github.com/confluentinc/librdkafka#requirements)

    * [**rapidjson**](https://github.com/Tencent/rapidjson) headers: only required by `addons/KafkaMetrics.h`


## To Start

* Tutorials

    * Confluent Blog [Debuting a Modern C++ API for Apache Kafka](https://www.confluent.io/blog/modern-cpp-kafka-api-for-safe-easy-messaging)

        * Note： it's a bit out of date, since [the API changed from time to time](https://github.com/morganstanley/modern-cpp-kafka/releases)

    * [KafkaProducer Quick Start](doc/KafkaProducerQuickStart.md)

    * [KafkaConsumer Quick Start](doc/KafkaConsumerQuickStart.md)

* User Manual

    * [Kafka Client API](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/annotated.html)

    * About [`Properties`](http://opensource.morganstanley.com/modern-cpp-kafka/doxygen/classKAFKA__API_1_1Properties.html)

        * It is a map which contains all configuration info needed to initialize a Kafka client.

        * The configuration items are ***key-value*** pairs, -- the type of ***key*** is always `std::string`, while the type for a ***value*** could be one of the followings

            * `std::string`

                * Most items are identical with [librdkafka configuration](https://github.com/confluentinc/librdkafka/blob/master/CONFIGURATION.md)

                * But with exceptions

                    * Default value changes

                        | Key String  | Default       | Description                                               |
                        | ----------- | ------------- | --------------------------------------------------------- |
                        | `log_level` | `5`           | Default was `6` from **librdkafka**                       |
                        | `client.id` | random string | No default from **librdkafka**                            |
                        | `group.id`  | random string | (for `KafkaConsumer` only) No default from **librdkafka** | 

                    * Additional options

                        | Key String                  | Default       | Description                                                                                         |
                        | --------------------------- | ------------- | --------------------------------------------------------------------------------------------------- |
                        | `enable.manual.events.poll` | `false`       | To poll the (offset-commit/message-delivery callback) events manually                               |
                        | `max.poll.records`          | `500`         | (for `KafkaConsumer` only) The maxmum number of records that a single call to `poll()` would return |

                    * Ignored options

                        | Key String                  | Explanation                                                                        |
                        | --------------------------- | ---------------------------------------------------------------------------------- |
                        | `enable.auto.offset.store`  | ***modern-cpp-kafka*** will save the offsets in its own way                        |
                        | `auto.commit.interval.ms`   | ***modern-cpp-kafka*** will only commit the offsets within each `poll()` operation |

            * `std::function<...>`

                | Key String                     | Value Type                                                                                    |
                | ------------------------------ | --------------------------------------------------------------------------------------------- |
                | `log_cb`                       | `LogCallback` (`std::function<void(int, const char*, int, const char* msg)>`)                 |
                | `error_cb`                     | `ErrorCallback` (`std::function<void(const Error&)>`)                                         |
                | `stats_cb`                     | `StatsCallback` (`std::function<void(const std::string&)>`)                                   |
                | `oauthbearer_token_refresh_cb` | `OauthbearerTokenRefreshCallback` (`std::function<SaslOauthbearerToken(const std::string&)>`) |

            * `Interceptors`

                | Key String     | Value Type     |
                | -------------- | -------------- |
                | `interceptors` | `Interceptors` |



## For Developers

### Build (for [tests](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests)/[tools](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tools)/[examples](https://github.com/morganstanley/modern-cpp-kafka/tree/main/examples))

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

    * `cmake path-to-project-root`

    * `make` (following options could be used with `-D`)

        | Build Option                     | Description                                                   |
        | -------------------------------- | ------------------------------------------------------------- |
        | `BUILD_OPTION_USE_TSAN=ON`       | Use Thread Sanitizer                                          |
        | `BUILD_OPTION_USE_ASAN=ON`       | Use Address Sanitizer                                         |
        | `BUILD_OPTION_USE_UBSAN=ON`      | Use Undefined Behavior Sanitizer                              |
        | `BUILD_OPTION_CLANG_TIDY=ON`     | Enable clang-tidy checking                                    |
        | `BUILD_OPTION_GEN_DOC=ON`        | Generate documentation as well                                |
        | `BUILD_OPTION_DOC_ONLY=ON`       | Only generate documentation                                   |
        | `BUILD_OPTION_GEN_COVERAGE=ON`   | Generate test coverage, only support by clang currently       |

     * `make install` (to install `tools`)

### Run Tests

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

    * The environment variable `KAFKA_BROKER_LIST` is mandatory for integration/robustness test

    * The environment variable `KAFKA_BROKER_PIDS` is mandatory for robustness test

    | Test Type                                                                                          | Requires Kafka Cluster   | Requires Privilege to Stop/Resume the Brokers |
    | -------------------------------------------------------------------------------------------------- | ------------------------ | --------------------------------------------- |
    | [tests/unit](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/unit)               | -                        | -                                             |
    | [tests/integration](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/integration) | Y (`KAFKA_BROKER_LIST`)  | -                                             | 
    | [tests/robustness`](https://github.com/morganstanley/modern-cpp-kafka/tree/main/tests/robustness)  | Y (`KAFKA_BROKER_LIST`)  | Y (`KAFKA_BROKER_PIDS`)                       |
