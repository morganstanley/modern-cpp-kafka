#include "gtest/gtest.h"

#include <cstdlib>


TEST(CheckEnvs, KafkaBrokerList)
{
    char* broker_list = getenv("KAFKA_BROKER_LIST"); // NOLINT
    ASSERT_TRUE(broker_list != nullptr);
    std::cout << "KAFKA_BROKER_LIST=[" << broker_list << "]" << std::endl;

    char* kafka_client_additional_settings = getenv("KAFKA_CLIENT_ADDITIONAL_SETTINGS");  // NOLINT
    if (kafka_client_additional_settings)
    {
        std::cout << "KAFKA_CLIENT_ADDITIONAL_SETTINGS=[" << kafka_client_additional_settings << "]" << std::endl;
    }
}

