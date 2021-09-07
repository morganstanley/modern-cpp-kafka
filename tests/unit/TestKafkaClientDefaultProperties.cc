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

namespace Kafka = KAFKA_API;

namespace {

// Here we even don't need a valid bootstrap server address
Kafka::Properties commonProps({{"bootstrap.servers", "127.0.0.1:9092"}, {"log_level", "0"}});

using KVMap = std::vector<std::pair<std::string, std::string>>;

bool checkProperties(const std::string& description, const Kafka::KafkaClient& client, const KVMap& expectedKVs)
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
    {
        Kafka::KafkaProducer producer(commonProps);

        const KVMap expectedKVs =
        {
            // { Kafka::ProducerConfig::ACKS,                          "-1"        },
            { Kafka::ProducerConfig::QUEUE_BUFFERING_MAX_MESSAGES,  "100000"    },
            { Kafka::ProducerConfig::QUEUE_BUFFERING_MAX_KBYTES,    "1048576"   }, // 0x100000
            { Kafka::ProducerConfig::LINGER_MS,                     "5"         },
            { Kafka::ProducerConfig::BATCH_NUM_MESSAGES,            "10000"     },
            { Kafka::ProducerConfig::BATCH_SIZE,                    "1000000"   },
            { Kafka::ProducerConfig::MESSAGE_MAX_BYTES,             "1000000"   },
            // { Kafka::ProducerConfig::MESSAGE_TIMEOUT_MS,            "300000"    },
            // { Kafka::ProducerConfig::REQUEST_TIMEOUT_MS,            "30000"     },
            // { Kafka::ProducerConfig::PARTITIONER,                   "consistent_random"   },
            { Kafka::ProducerConfig::SECURITY_PROTOCOL,             "plaintext" },
            { Kafka::ProducerConfig::MAX_IN_FLIGHT,                 "1000000"   },
            { Kafka::ProducerConfig::ENABLE_IDEMPOTENCE,            "false"     },
        };

        EXPECT_TRUE(checkProperties("KafkaProducer", producer, expectedKVs));
    }

    KafkaTestUtility::PrintDividingLine();

    {
        auto props = commonProps.put(Kafka::ProducerConfig::ENABLE_IDEMPOTENCE, "true");

        Kafka::KafkaProducer producer(props);

        const KVMap expectedKVs =
        {
            { Kafka::ProducerConfig::MAX_IN_FLIGHT,       "5"    },
            { Kafka::ProducerConfig::ENABLE_IDEMPOTENCE,  "true" },
        };

        EXPECT_TRUE(checkProperties("KafkaProducer(idempotence enabled)", producer, expectedKVs));
    }
}

TEST(KafkaClient, KafkaConsumerDefaultProperties)
{
    {
        Kafka::KafkaAutoCommitConsumer consumer(commonProps);

        const KVMap expectedKVs =
        {
            { Kafka::ConsumerConfig::ENABLE_PARTITION_EOF,  "false"      },
            { Kafka::ConsumerConfig::MAX_POLL_RECORDS,      "500"        },
            { Kafka::ConsumerConfig::QUEUED_MIN_MESSAGES,   "100000"     },
            { Kafka::ConsumerConfig::SESSION_TIMEOUT_MS,    "45000"      },
            { Kafka::ConsumerConfig::SOCKET_TIMEOUT_MS,     "60000"      },
            { Kafka::ConsumerConfig::SECURITY_PROTOCOL,     "plaintext"  },
            { "enable.auto.commit",                         "false"      },
            { "auto.commit.interval.ms",                    "0"          },
            { "enable.auto.offset.store",                   "false"      }
        };

        EXPECT_TRUE(checkProperties("KakfaAutoCommitConsumer", consumer, expectedKVs));

        // Interesting, -- no default for AUTO_OFFSET_RESET within librdkafka
        EXPECT_FALSE(consumer.getProperty(Kafka::ConsumerConfig::AUTO_OFFSET_RESET));
    }

    KafkaTestUtility::PrintDividingLine();

    {
        Kafka::KafkaManualCommitConsumer consumer(commonProps);

        const KVMap expectedKVs =
        {
            { "enable.auto.offset.store", "true" }
        };
        EXPECT_TRUE(checkProperties("KakfaManualCommitConsumer", consumer, expectedKVs));
    }
}

