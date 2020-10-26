#include "../utils/TestUtility.h"

#include "kafka/AdminClient.h"

#include "gtest/gtest.h"

#include <iostream>

using namespace KAFKA_API;


TEST(AdminClient, createListDeleteTopics)
{
    AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

    const Topics topics = {Utility::getRandomString(), Utility::getRandomString()};
    const int numPartitions = 5;
    const int replicaFactor = 3;

    std::cout << "[" << Utility::getCurrentTime() << "] topics: " << toString(topics) << std::endl;

    // Create Topics
    const auto timeout = std::chrono::seconds(30);
    auto createResult = adminClient.createTopics(topics, numPartitions, replicaFactor, Properties(), timeout);
    std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " topics created, result: " << createResult.detail << std::endl;
    EXPECT_FALSE(createResult.error);

    // Check Topics (might wait a while for new created topics to take effect)
    bool areTopicsSuccessfullyCreated = false;
    constexpr int MAX_RETRY = 5;
    for (int retry = 0; retry < MAX_RETRY; ++retry)
    {
        // Just wait a short while
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Check the topic list
        auto listResult = adminClient.listTopics();
        auto foundAllTopics = std::all_of(topics.cbegin(), topics.cend(),
                                          [&listResult](const Topic& topic) {
                                              bool ret = (listResult.topics.count(topic) == 1);
                                              if (!ret) {
                                                  std::cout << "[" << Utility::getCurrentTime() << "] can't find topic " << topic << " by now!" << std::endl;
                                              }
                                              return ret;
                                          });
        if (!foundAllTopics) continue;

        // Check metadata
        auto foundAllMetadatas = std::all_of(topics.cbegin(), topics.cend(),
                                             [&adminClient, numPartitions, replicaFactor](const Topic& topic) {
                                                 if (auto metadata = adminClient.fetchBrokerMetadata(topic)) {
                                                     auto partitions = metadata->partitions();
                                                     EXPECT_EQ(numPartitions, partitions.size());
                                                     for (const auto& partitionInfo: partitions) {
                                                         EXPECT_EQ(replicaFactor, partitionInfo.second.replicas.size());
                                                     }

                                                     std::cout << "[" << Utility::getCurrentTime() << "] BrokerMetadata: " << metadata->toString() << std::endl;
                                                     return true;
                                                 }

                                                 std::cout << "[" << Utility::getCurrentTime() << "] can't find metadata for topic " << topic << " by now!" << std::endl;
                                                 return false;
                                             });
        if (!foundAllMetadatas) continue;

        areTopicsSuccessfullyCreated = true;
        break;
    }
    EXPECT_TRUE(areTopicsSuccessfullyCreated);

    KafkaTestUtility::WaitMetadataSyncUpBetweenBrokers();

    // Delete Topics
    auto deleteResult = adminClient.deleteTopics(topics);
    std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " topics deleted, result: " << deleteResult.detail << std::endl;
    EXPECT_FALSE(deleteResult.error);
}

TEST(AdminClient, DuplicatedCreateDeleteTopics)
{
    const Topic topic(Utility::getRandomString());
    const int numPartitions = 5;
    const int replicaFactor = 3;

    AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

    constexpr int MAX_REPEAT = 10;
    for (int i = 0; i < MAX_REPEAT; ++i)
    {
        auto createResult = adminClient.createTopics({topic}, numPartitions, replicaFactor);
        std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " topics created, result: " << createResult.detail << std::endl;
        EXPECT_TRUE(!createResult.error || createResult.error.value() == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS);
    }

    for (int i = 0; i < MAX_REPEAT; ++i)
    {
        auto deleteResult = adminClient.deleteTopics({topic});
        std::cout << "[" << Utility::getCurrentTime() << "] " << adminClient.name() << " topics deleted, result: " << deleteResult.detail << std::endl;
        EXPECT_TRUE(!deleteResult.error || deleteResult.error.value() == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
    }
}

