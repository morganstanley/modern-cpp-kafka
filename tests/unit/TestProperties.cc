#include "kafka/AdminClientConfig.h"
#include "kafka/ConsumerConfig.h"
#include "kafka/ProducerConfig.h"
#include "kafka/Properties.h"

#include "gtest/gtest.h"


TEST(Properties, Basic)
{
    // Construct properties
    kafka::Properties props;
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
    kafka::Properties anotherProps
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
    using namespace kafka::clients::consumer;

    Config props
    {{
        { Config::BOOTSTRAP_SERVERS,    "127.0.0.1:9000,127.0.0.1:9001" },
        { Config::AUTO_OFFSET_RESET,    "earliest"                      },
        { Config::ENABLE_PARTITION_EOF, "false"                         }
    }};

    EXPECT_EQ("auto.offset.reset=earliest|bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.partition.eof=false", props.toString());
}

TEST(Properties, ProducerConfig)
{
    using namespace kafka::clients::producer;

    Config props
    {{
        { Config::BOOTSTRAP_SERVERS,  "127.0.0.1:9000,127.0.0.1:9001" },
        { Config::LINGER_MS,          "20"                            },
        { Config::ENABLE_IDEMPOTENCE, "true"                          }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.idempotence=true|linger.ms=20", props.toString());
}

TEST(Properties, AdminClientConfig)
{
    using namespace kafka::clients::admin;
    Config props
    {{
        { Config::BOOTSTRAP_SERVERS, "127.0.0.1:9000,127.0.0.1:9001" },
        { Config::SECURITY_PROTOCOL, "SASL_PLAINTEXT"                }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|security.protocol=SASL_PLAINTEXT", props.toString());
}

TEST(Properties, SensitiveProperties)
{
    kafka::Properties props
    {{
        { "ssl.key.password",      "passwordA" },
        { "ssl.keystore.password", "passwordB" },
        { "sasl.username",         "userName" },
        { "sasl.password",         "passwordC" },
    }};

    EXPECT_EQ("sasl.password=*|sasl.username=*|ssl.key.password=*|ssl.keystore.password=*", props.toString());
}
