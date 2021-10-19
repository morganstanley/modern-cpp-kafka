#include "../utils/TestUtility.h"

#include "kafka/AdminClient.h"

#include "gtest/gtest.h"

#include <iostream>


TEST(AdminClient, CreateListDeleteTopics)
{
    kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

    const kafka::Topics topics = {kafka::utility::getRandomString(), kafka::utility::getRandomString()};
    const int numPartitions = 5;
    const int replicaFactor = 3;

    std::cout << "[" << kafka::utility::getCurrentTime() << "] topics: " << kafka::toString(topics) << std::endl;

    // Create Topics
    const auto timeout = std::chrono::seconds(30);
    auto createResult = adminClient.createTopics(topics, numPartitions, replicaFactor, kafka::Properties(), timeout);
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " topics created, result: " << createResult.error.message() << std::endl;
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
                                          [&listResult](const kafka::Topic& topic) {
                                              bool ret = (listResult.topics.count(topic) == 1);
                                              if (!ret) {
                                                  std::cout << "[" << kafka::utility::getCurrentTime() << "] can't find topic " << topic << " by now!" << std::endl;
                                              }
                                              return ret;
                                          });
        if (!foundAllTopics) continue;

        // Check metadata
        auto foundAllMetadatas = std::all_of(topics.cbegin(), topics.cend(),
                                             [&adminClient, numPartitions, replicaFactor](const kafka::Topic& topic) {
                                                 if (auto metadata = adminClient.fetchBrokerMetadata(topic)) {
                                                     auto partitions = metadata->partitions();
                                                     EXPECT_EQ(numPartitions, partitions.size());
                                                     for (const auto& partitionInfo: partitions) {
                                                         EXPECT_EQ(replicaFactor, partitionInfo.second.replicas.size());
                                                     }

                                                     std::cout << "[" << kafka::utility::getCurrentTime() << "] BrokerMetadata: " << metadata->toString() << std::endl;
                                                     return true;
                                                 }

                                                 std::cout << "[" << kafka::utility::getCurrentTime() << "] can't find metadata for topic " << topic << " by now!" << std::endl;
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
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " topics deleted, result: " << deleteResult.error.message() << std::endl;
    EXPECT_FALSE(deleteResult.error);
}

TEST(AdminClient, DuplicatedCreateDeleteTopics)
{
    const kafka::Topic topic(kafka::utility::getRandomString());
    const int numPartitions = 5;
    const int replicaFactor = 3;

    kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

    constexpr int MAX_REPEAT = 10;
    for (int i = 0; i < MAX_REPEAT; ++i)
    {
        auto createResult = adminClient.createTopics({topic}, numPartitions, replicaFactor);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " topics created, result: " << createResult.error.message() << std::endl;
        EXPECT_TRUE(!createResult.error || createResult.error.value() == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS);
    }

    for (int i = 0; i < MAX_REPEAT; ++i)
    {
        auto deleteResult = adminClient.deleteTopics({topic});
        std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " topics deleted, result: " << deleteResult.error.message() << std::endl;
        EXPECT_TRUE(!deleteResult.error || deleteResult.error.value() == RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART);
    }
}

TEST(AdminClient, DeleteRecords)
{
    const kafka::Topic     topic      = kafka::utility::getRandomString();
    const kafka::Partition partition1 = 0;
    const kafka::Partition partition2 = 1;
    const kafka::Partition partition3 = 2;

    std::cout << "[" << kafka::utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    const std::vector<std::tuple<kafka::Headers, std::string, std::string>> messages = {
            {kafka::Headers{}, "key1", "value1"},
            {kafka::Headers{}, "key2", "value2"},
            {kafka::Headers{}, "key3", "value3"},
    };

    // Send the messages
    auto metadatas1 = KafkaTestUtility::ProduceMessages(topic, partition1, messages);
    auto metadatas2 = KafkaTestUtility::ProduceMessages(topic, partition2, messages);
    auto metadatas3 = KafkaTestUtility::ProduceMessages(topic, partition3, messages);

    // Prepare the AdminClient
    kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " started" << std::endl;

    // Prepare offsets for `deleteRecords`
    kafka::TopicPartitionOffsets offsetsToDeleteWith = {
        {kafka::TopicPartition{topic, partition1}, *metadatas1[0].offset()},  // the very first offset, which means no message would be deleted
        {kafka::TopicPartition{topic, partition2}, *metadatas2[1].offset()},  // there's only 1 message before the offset
        {kafka::TopicPartition{topic, partition3}, *metadatas3[2].offset() + 1} // the offset beyond the end, which means all messages would be deleted
    };

    // Delete some records
    auto deleteResult = adminClient.deleteRecords(offsetsToDeleteWith);
    std::cout << "[" << kafka::utility::getCurrentTime() << "] " << adminClient.name() << " just deleted some records, result: " << deleteResult.error.message() << std::endl;
    EXPECT_FALSE(deleteResult.error);

    KafkaTestUtility::WaitMetadataSyncUpBetweenBrokers();

    kafka::clients::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    {
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        EXPECT_EQ(0, records.size());
    }

    // Check whether the left records are as expected
    {
        // Check the first parition
        const kafka::TopicPartition topicPartition = {topic, partition1};
        consumer.assign({topicPartition});
        RETRY_FOR_FAILURE(consumer.seekToBeginning(), 2);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] topic-partition[" << kafka::toString(topicPartition) << "], position: " << consumer.position(topicPartition) << std::endl;

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        EXPECT_EQ(messages.size(), records.size());
        if (records.size() == messages.size())
        {
            for (std::size_t i = 0; i < records.size(); ++i)
            {
                EXPECT_EQ(std::get<1>(messages[i]), records[i].key().toString());
                EXPECT_EQ(std::get<2>(messages[i]), records[i].value().toString());
            }
        }
    }
    {
        // Check the second partition
        consumer.unsubscribe();
        const kafka::TopicPartition topicPartition = {topic, partition2};
        consumer.assign({topicPartition});
        RETRY_FOR_FAILURE(consumer.seekToBeginning(), 2);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] topic-partition[" << kafka::toString(topicPartition) << "], position: " << consumer.position(topicPartition) << std::endl;

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        EXPECT_EQ(messages.size() - 1, records.size());
        if (records.size() == messages.size() - 1)
        {
            for (std::size_t i = 0; i < records.size(); ++i)
            {
                EXPECT_EQ(std::get<1>(messages[i + 1]), records[i].key().toString());
                EXPECT_EQ(std::get<2>(messages[i + 1]), records[i].value().toString());
            }
        }
    }
    {
        // Check the third partition
        consumer.unsubscribe();
        const kafka::TopicPartition topicPartition = {topic, partition3};
        consumer.assign({topicPartition});
        RETRY_FOR_FAILURE(consumer.seekToBeginning(), 2);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] topic-partition[" << kafka::toString(topicPartition) << "], position: " << consumer.position(topicPartition) << std::endl;

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        EXPECT_TRUE(records.empty());
    }
}

