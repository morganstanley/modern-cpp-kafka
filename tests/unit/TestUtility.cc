#include "kafka/Utility.h"

#include "gtest/gtest.h"


TEST(Utility, LibRdKafkaVersion)
{
    std::cout << "librdkafka version: " << kafka::utility::getLibRdKafkaVersion() << std::endl;
}

