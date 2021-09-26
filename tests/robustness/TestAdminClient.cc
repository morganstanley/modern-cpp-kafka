#include "../utils/TestUtility.h"

#include "kafka/AdminClient.h"

#include "gtest/gtest.h"

#include <iostream>


TEST(AdminClient, BrokersTimeout)
{
    kafka::Topic topic = kafka::utility::getRandomString();
    const int numPartitions = 5;
    const int replicaFactor = 3;

    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
        std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

        KafkaTestUtility::PauseBrokers();

        // Fetch metadata, -- timeout
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will ListTopics" << std::endl;
        {
            auto fetchResult = adminClient.fetchBrokerMetadata(topic, std::chrono::seconds(1));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] FetchMetadata: result[" << (fetchResult ? fetchResult->toString() : "NA") << "]" << std::endl;
            EXPECT_FALSE(fetchResult);
        }

        // List Topics, -- timeout
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will ListTopics" << std::endl;
        {
            auto listResult = adminClient.listTopics(std::chrono::seconds(1));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] ListTopics: result[" << listResult.error.message() << "]. Result: " << listResult.error.message() << std::endl;
            EXPECT_TRUE(listResult.error.value() == RD_KAFKA_RESP_ERR__TRANSPORT || listResult.error.value() == RD_KAFKA_RESP_ERR__TIMED_OUT);
            EXPECT_EQ(0, listResult.topics.size());
        }

        // Create Topics, -- timeout
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will CreateTopics" << std::endl;
        {
            auto createResult = adminClient.createTopics({topic}, numPartitions, replicaFactor, kafka::Properties(), std::chrono::seconds(3));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] createTopics: result[" << createResult.error.message() << "]" << std::endl;
            EXPECT_TRUE(createResult.error);
        }
    }

    KafkaTestUtility::ResumeBrokers();

    // Since the brokers might not be ready during the short time, sometimes we have to retry...
    constexpr int maxRetry = 5;
    for (int i = 0; i < maxRetry; ++i)
    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());

        // Create Topics, -- success
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will CreateTopics" << std::endl;
        auto createResult = adminClient.createTopics({topic}, numPartitions, replicaFactor);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] CreateTopics: result[" << createResult.error.message() << "]" << std::endl;
        if (!createResult.error || createResult.error.value() == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS)
        {
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));

        auto listResult = adminClient.listTopics(std::chrono::seconds(1));
        if (!listResult.error && listResult.topics.count(topic) == 1)
            {
            auto metadata = adminClient.fetchBrokerMetadata(topic, std::chrono::seconds(5), false);
            std::cout << "[" << kafka::utility::getCurrentTime() << "] broker metadata: " << (metadata ? metadata->toString() : "NA") << std::endl;
        }

        EXPECT_NE(maxRetry - 1, i);
    }

    KafkaTestUtility::WaitMetadataSyncUpBetweenBrokers();

    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());

        // List Topics, -- success
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will ListTopics" << std::endl;
        {
            auto listResult = adminClient.listTopics();
            std::cout << "[" << kafka::utility::getCurrentTime() << "] ListTopics: result[" << listResult.error.message() << "]" << std::endl;
            EXPECT_FALSE(listResult.error);
            EXPECT_EQ(1, listResult.topics.count(topic));
        }

        // Fetch metadata, -- success
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will FetchMetadata" << std::endl;
        {
            auto fetchResult = adminClient.fetchBrokerMetadata(topic, std::chrono::seconds(5));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] FetchMetadata: result[" << (fetchResult ? fetchResult->toString() : "NA") << "]" << std::endl;
            EXPECT_TRUE(fetchResult);
        }
    }

    KafkaTestUtility::PauseBrokers();

    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());

        // Delete Topics, -- timeout
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will DeleteTopics" << std::endl;
        {
            auto deleteResult = adminClient.deleteTopics({topic}, std::chrono::seconds(5));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] DeleteTopics: result[" << deleteResult.error.message() << "]" << std::endl;
            EXPECT_TRUE(deleteResult.error);
        }
    }

    KafkaTestUtility::ResumeBrokers();

    // Since the brokers might not be ready during the short time, sometimes we have to retry...
    for (int i = 0; i < maxRetry; ++i)
    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
        // Delete Topics, -- success
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will DeleteTopics" << std::endl;
        auto deleteResult = adminClient.deleteTopics({topic});
        std::cout << "[" << kafka::utility::getCurrentTime() << "] DeleteTopics: result[" << deleteResult.error.message() << "]" << std::endl;
        // In some edge cases, the result might be "timed out", while in fact the topic had already been deleted.
        // Then, even we keep retrying, we could only get response of "unknown topic or partition", and this should be treated as "SUCCESS".
        if (!deleteResult.error || deleteResult.error.value() == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART)
        {
            break;
        }

        std::this_thread::sleep_for(std::chrono::seconds(1));
        EXPECT_NE(maxRetry - 1, i);
    }

    KafkaTestUtility::WaitMetadataSyncUpBetweenBrokers();

    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());

        // List Topics, -- success
        std::cout << "[" << kafka::utility::getCurrentTime() << "] will ListTopics" << std::endl;
        {
            auto listResult = adminClient.listTopics(std::chrono::seconds(1));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] ListTopics: result[" << listResult.error.message() << "]" << std::endl;
            EXPECT_FALSE(listResult.error);
            EXPECT_EQ(0, listResult.topics.count(topic));
        }
    }
}

