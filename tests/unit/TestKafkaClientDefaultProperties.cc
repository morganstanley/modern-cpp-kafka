#include "../utils/TestUtility.h"

#include "kafka/ConsumerConfig.h"
#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"
#include "kafka/ProducerConfig.h"
#include "kafka/Properties.h"

#include "gtest/gtest.h"

#include <string>
#include <utility>
#include <vector>


namespace {

// Here we even don't need a valid bootstrap server address
const kafka::Properties commonProps({{"bootstrap.servers", {"127.0.0.1:9092"}}, {"log_level", {"0"}}});

using KVMap = std::vector<std::pair<std::string, std::string>>;

bool checkProperties(const std::string& description, const kafka::clients::KafkaClient& client, const KVMap& expectedKVs)
{
    std::cout << "Check default properties for " << description << ":" << std::endl;

    auto ret = true;
    for (const auto& kv: expectedKVs)
    {
        auto property = client.getProperty(kv.first);

        std::cout << "    " << std::setw(30) << kv.first << " = " << std::setw(-20) << (property ? *property : "N/A")
            << ((!property || kv.second != *property) ? "   <--- NOT AS EXPECTED!" : "") << std::endl;

        ret &= (property && (kv.second == *property));
    }

    return ret;
}

} // end of namespace


TEST(KafkaClient, KafkaProducerDefaultProperties)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;

    {
        const KafkaProducer producer(commonProps);

        const KVMap expectedKVs =
        {
            // { ProducerConfig::ACKS,                          "-1"        },
            { ProducerConfig::QUEUE_BUFFERING_MAX_MESSAGES,  "100000"    },
            { ProducerConfig::QUEUE_BUFFERING_MAX_KBYTES,    "1048576"   }, // 0x100000
            { ProducerConfig::LINGER_MS,                     "5"         },
            { ProducerConfig::BATCH_NUM_MESSAGES,            "10000"     },
            { ProducerConfig::BATCH_SIZE,                    "1000000"   },
            { ProducerConfig::MESSAGE_MAX_BYTES,             "1000000"   },
            // { ProducerConfig::MESSAGE_TIMEOUT_MS,            "300000"    },
            // { ProducerConfig::REQUEST_TIMEOUT_MS,            "30000"     },
            // { ProducerConfig::PARTITIONER,                   "consistent_random"   },
            { ProducerConfig::SECURITY_PROTOCOL,             "plaintext" },
            { ProducerConfig::MAX_IN_FLIGHT,                 "1000000"   },
            { ProducerConfig::ENABLE_IDEMPOTENCE,            "false"     },
        };

        EXPECT_TRUE(checkProperties("KafkaProducer", producer, expectedKVs));
    }

    KafkaTestUtility::PrintDividingLine();

    {
        auto props = commonProps;
        props.put(ProducerConfig::ENABLE_IDEMPOTENCE, "true");
        const KafkaProducer producer(props);

        const KVMap expectedKVs =
        {
            { ProducerConfig::MAX_IN_FLIGHT,       "5"    },
            { ProducerConfig::ENABLE_IDEMPOTENCE,  "true" },
        };

        EXPECT_TRUE(checkProperties("KafkaProducer[enable.idempotence=true]", producer, expectedKVs));
    }
}
TEST(KafkaClient, KafkaConsumerDefaultProperties)
{
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;

    {
        const KafkaConsumer consumer(commonProps);

        const KVMap expectedKVs =
        {
            { ConsumerConfig::ENABLE_AUTO_COMMIT,    "true"       },
            { ConsumerConfig::ENABLE_PARTITION_EOF,  "false"      },
            { ConsumerConfig::MAX_POLL_RECORDS,      "500"        },
            { ConsumerConfig::QUEUED_MIN_MESSAGES,   "100000"     },
            { ConsumerConfig::SESSION_TIMEOUT_MS,    "45000"      },
            { ConsumerConfig::SOCKET_TIMEOUT_MS,     "60000"      },
            { ConsumerConfig::SECURITY_PROTOCOL,     "plaintext"  },
            { "auto.commit.interval.ms",             "0"          },
            { "enable.auto.offset.store",            "true"       }
        };

        EXPECT_TRUE(checkProperties("KakfaConsumer[enable.auto.commit=true]", consumer, expectedKVs));

        // Interesting, -- no default for AUTO_OFFSET_RESET within librdkafka
        EXPECT_FALSE(consumer.getProperty(ConsumerConfig::AUTO_OFFSET_RESET));
    }

    KafkaTestUtility::PrintDividingLine();

    {
        auto props = commonProps;
        props.put(ConsumerConfig::ENABLE_AUTO_COMMIT, "false");
        const KafkaConsumer consumer(props);

        const KVMap expectedKVs =
        {
            { ConsumerConfig::ENABLE_AUTO_COMMIT,    "false"      },
        };
        EXPECT_TRUE(checkProperties("KakfaConsumer[enable.auto.commit=false]", consumer, expectedKVs));
    }
}

