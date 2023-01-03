#include "kafka/AdminClientConfig.h"
#include "kafka/ClientConfig.h"
#include "kafka/ConsumerConfig.h"
#include "kafka/Interceptors.h"
#include "kafka/ProducerConfig.h"
#include "kafka/Properties.h"
#include "kafka/Utility.h"

#include "gtest/gtest.h"


TEST(Properties, Basic)
{
    // Construct properties
    kafka::Properties props;
    props.put("bootstrap.servers", "127.0.0.1:9000,127.0.0.1:9001");
    props.put("auto.offset.reset", "earliest");
    props.put("max.poll.records",  "500");
    props.put("log_cb", [](int /*level*/, const char* /*filename*/, int /*lineno*/, const char* msg) {
                            std::cout << "log_cb: [" << kafka::utility::getCurrentTime() << "]" << msg << std::endl;
                        });
    props.put("error_cb", [](const kafka::Error& err) {
                              std::cout << "error_cb: [" << kafka::utility::getCurrentTime() << "]" << err.toString() << std::endl;
                          });
    props.put("stats_cb", [](const std::string& stats) {
                              std::cout << "stats_cb: [" << kafka::utility::getCurrentTime() << "]" << stats << std::endl;
                          });
    props.put("interceptors", kafka::clients::Interceptors{});

    // Fetch a property
    auto getBootstrapServers = props.getProperty("bootstrap.servers");
    ASSERT_TRUE(getBootstrapServers);
    EXPECT_EQ("127.0.0.1:9000,127.0.0.1:9001", *getBootstrapServers); // NOLINT

    // Remove a property
    props.eraseProperty("bootstrap.servers");
    EXPECT_FALSE(props.getProperty("bootstrap.servers"));

    // To string
    const std::regex re(R"(auto\.offset\.reset=earliest\|error_cb=.+\|interceptors=.+\|log_cb=.+\|max\.poll\.records=500\|stats_cb=.+)");
    EXPECT_TRUE(std::regex_match(props.toString(), re));

    // Get the internal map ref
    EXPECT_EQ(6, props.map().size());

    // Initialize with initializer list
    kafka::clients::Interceptors interceptors;
    interceptors.onThreadStart([](const std::string& threadName, const std::string& /*threadType*/) {
                    std::cout << threadName << " started!" << std::endl;
                 })
                .onThreadExit([](const std::string& threadName, const std::string& /*threadType*/) {
                     std::cout << threadName << " exited!" << std::endl;
                 });

    kafka::Properties anotherProps
    {{
        { "bootstrap.servers", { "127.0.0.1:9000,127.0.0.1:9001"} },
        { "auto.offset.reset", { "earliest"                     } },
        { "max.poll.records",  { "500"                          } },
        { "error_cb",          { [](const kafka::Error& error) { std::cout << "error_cb: [" << kafka::utility::getCurrentTime() << "]" << error.toString() << std::endl; } } },
        { "interceptors",      { interceptors                   } }
    }};

    std::cout << anotherProps.toString() << std::endl;

    // Assignment
    anotherProps = props;
    EXPECT_EQ(props, anotherProps);
}

TEST(Properties, ConsumerConfig)
{
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;

    const Config props
    {{
        { Config::BOOTSTRAP_SERVERS,            { "127.0.0.1:9000,127.0.0.1:9001" } },
        { ConsumerConfig::AUTO_OFFSET_RESET,    { "earliest"                      } },
        { ConsumerConfig::ENABLE_PARTITION_EOF, { "false"                         } }
    }};

    EXPECT_EQ("auto.offset.reset=earliest|bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.partition.eof=false", props.toString());
}

TEST(Properties, ProducerConfig)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;

    const ProducerConfig props
    {{
        { Config::BOOTSTRAP_SERVERS,          { "127.0.0.1:9000,127.0.0.1:9001" } },
        { ProducerConfig::LINGER_MS,          { "20"                            } },
        { ProducerConfig::ENABLE_IDEMPOTENCE, { "true"                          } }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|enable.idempotence=true|linger.ms=20", props.toString());
}

TEST(Properties, AdminClientConfig)
{
    using namespace kafka::clients;
    using namespace kafka::clients::admin;

    const AdminClientConfig props
    {{
        { Config::BOOTSTRAP_SERVERS, { "127.0.0.1:9000,127.0.0.1:9001" } },
        { Config::SECURITY_PROTOCOL, { "SASL_PLAINTEXT"                } }
    }};

    EXPECT_EQ("bootstrap.servers=127.0.0.1:9000,127.0.0.1:9001|security.protocol=SASL_PLAINTEXT", props.toString());
}

TEST(Properties, SensitiveProperties)
{
    const kafka::Properties props
    {{
        { "ssl.key.password",      { "passwordA" } },
        { "ssl.keystore.password", { "passwordB" } },
        { "sasl.username",         { "userName"  } },
        { "sasl.password",         { "passwordC" } },
        { "ssl.key.pem",           { "pem"       } },
        { "ssl_key",               { "key"       } },
    }};

    EXPECT_EQ("sasl.password=*|sasl.username=*|ssl.key.password=*|ssl.key.pem=*|ssl.keystore.password=*|ssl_key=*", props.toString());
}
