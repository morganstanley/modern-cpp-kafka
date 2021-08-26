#include "kafka/Utility.h"

#include "gtest/gtest.h"

namespace Kafka = KAFKA_API;

TEST(Utility, LibRdKafkaVersion)
{
    std::cout << "librdkafka version: " << Kafka::Utility::getLibRdKafkaVersion() << std::endl;
}

