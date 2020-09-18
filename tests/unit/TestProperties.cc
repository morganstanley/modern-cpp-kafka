#include "kafka/AdminClientConfig.h"
#include "kafka/ConsumerConfig.h"
#include "kafka/ProducerConfig.h"
#include "kafka/Properties.h"

#include "gtest/gtest.h"

namespace Kafka = KAFKA_API;

TEST(Properties, Basic)
{
    // Construct properties
    Kafka::Properties props;
    props.put("bootstrap.servers", "127.0.0.1:9000,127.0.0.1:9001");
    props.put("auto.offset.reset", "earliest");
    props.put("max.poll.records",  "500");

    // Fetch a property
    auto getBootstrapServers = props.getProperty("bootstrap.servers");
    EXPECT_TRUE(getBootstrapServers);
    EXPECT_EQ("127.0.0.1:9000,127.0.0.1:9001", *getBootstrapServers);

    // Remove a property
    props.eraseProperty("bootstrap.servers");
    EXPECT_FALSE(props.getProperty("bootstrap.servers"));

    // To string
    EXPECT_EQ("auto.offset.reset=earliest|max.poll.records=500", props.toString());

    // Get the internal map ref
    const auto& m = props.map();
    EXPECT_EQ(2, m.size());

    // Initialize with initializer list
    Kafka::Properties anotherProps
    {{
        { "bootstrap.servers", "127.0.0.1:9000,127.0.0.1:9001" },
        { "auto.offset.reset", "earliest"                      },
        { "max.poll.records",  "500"                           }
    }};

    // Assignment
    anotherProps = props;
    EXPECT_EQ(props, anotherProps);
}


TEST(Properties, ConsumerConfig)
{
    Kafka::ConsumerConfig props
    {{
        { Kafka::ConsumerConfig::BOOTSTRAP_SERVERS,    "127.0.0.1:9000,127.0.0.1:9001" },
        { Kafka::ConsumerConfig::AUTO_OFFSET_RESET,    "earliest"                      },
        { Kafka::ConsumerConfig::ENABLE_PARTITION_EOF, "false"                         }
    }};

    EXPECT_EQ("auto.offset.reset=earliest|bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.partition.eof=false", props.toString());
}

TEST(Properties, ProducerConfig)
{
    Kafka::ProducerConfig props
    {{
        { Kafka::ProducerConfig::BOOTSTRAP_SERVERS,  "127.0.0.1:9000,127.0.0.1:9001" },
        { Kafka::ProducerConfig::LINGER_MS,          "20"                            },
        { Kafka::ProducerConfig::ENABLE_IDEMPOTENCE, "true"                          }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.idempotence=true|linger.ms=20", props.toString());
}

TEST(Properties, AdminClientConfig)
{
    Kafka::AdminClientConfig props
    {{
        { Kafka::AdminClientConfig::BOOTSTRAP_SERVERS, "127.0.0.1:9000,127.0.0.1:9001" },
        { Kafka::AdminClientConfig::SECURITY_PROTOCOL, "SASL_PLAINTEXT"                }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|security.protocol=SASL_PLAINTEXT", props.toString());
}
