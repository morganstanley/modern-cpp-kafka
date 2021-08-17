#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <boost/algorithm/string.hpp>

#include <atomic>
#include <chrono>
#include <cstring>
#include <future>
#include <thread>


using namespace KAFKA_API;


TEST(KafkaManualCommitConsumer, AlwaysFinishClosing_ManuallyPollEvents)
{
    Topic     topic     = Utility::getRandomString();
    Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Producer some messages
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Consumer properties
    auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
    props.put(ConsumerConfig::MAX_POLL_RECORDS,  "1");        // Only poll 1 message each time
    props.put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest");
    props.put(ConsumerConfig::SOCKET_TIMEOUT_MS, "2000");

    volatile std::size_t commitCbCount = 0;
    {
        // Start a consumer (which need to call `pollEvents()` to trigger the commit callback)
        KafkaManualCommitConsumer consumer(props, KafkaClient::EventsPollingOption::Manual);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe the topic
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        ASSERT_TRUE(std::none_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error(); }));
        ASSERT_EQ(messages.size(), records.size());

        for (std::size_t i = 0; i < records.size(); ++i)
        {
            EXPECT_EQ(topic,     records[i].topic());
            EXPECT_EQ(partition, records[i].partition());
            EXPECT_EQ(std::get<1>(messages[i]), records[i].key().toString());
            EXPECT_EQ(std::get<2>(messages[i]), records[i].value().toString());

            Offset expectedOffset = records[i].offset() + 1;
            consumer.commitAsync(records[i],
                                 [expectedOffset, topic, partition, &commitCbCount](const TopicPartitionOffsets& tpos, const Error& error){
                                     std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << expectedOffset << "], got result[" << error.message() << "], tpos[" << toString(tpos) << "]" << std::endl;
                                     EXPECT_EQ(expectedOffset, tpos.at({topic, partition}));
                                     ++commitCbCount;
                                 });
        }

        // Pause the brokers (before polling the commit callbacks)
        KafkaTestUtility::PauseBrokers();

        // Don't wait for the offset-commit callback (to be triggered)
        std::cout << "[" << Utility::getCurrentTime() << "] Before closing the consumer, committed callback count[" << commitCbCount << "]" << std::endl;
    }

    std::cout << "[" << Utility::getCurrentTime() << "] After closing the consumer, committed callback count[" << commitCbCount << "]" << std::endl;
    EXPECT_EQ(messages.size(), commitCbCount);

    // resume the brokers
    KafkaTestUtility::ResumeBrokers();
}

TEST(KafkaManualCommitConsumer, CommitOffsetWhileBrokersStop)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Producer some messages
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"}
    };
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Consumer properties
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                            .put(ConsumerConfig::MAX_POLL_RECORDS,  "1")         // Only poll 1 message each time
                            .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                            .put(ConsumerConfig::SOCKET_TIMEOUT_MS, "2000");     // Just don't want to wait too long for the commit-offset callback.

    volatile std::size_t commitCbCount = 0;
    {
        // Start a consumer
        KafkaManualCommitConsumer consumer(props);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe th topic
        consumer.subscribe({topic},
                           [](Consumer::RebalanceEventType et, const TopicPartitions&  /*unused*/) {
                               std::cout << "[" << Utility::getCurrentTime() << "] rebalance-event triggered, event type["
                                   << (et == Consumer::RebalanceEventType::PartitionsAssigned ? "PartitionAssigned" : "PartitionRevolked") << "]" << std::endl;
                            });
        EXPECT_FALSE(consumer.subscription().empty());

        {
            // Poll messages
            auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
            ASSERT_TRUE(std::none_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error(); }));
            ASSERT_EQ(messages.size(), records.size());
            std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " polled "  << records.size() << " messages" << std::endl;

            // Pause the brokers before committing the offsets
            KafkaTestUtility::PauseBrokers();

            for (std::size_t i = 0; i < records.size(); ++i)
            {
                EXPECT_EQ(topic,     records[i].topic());
                EXPECT_EQ(partition, records[i].partition());
                EXPECT_EQ(std::get<1>(messages[i]), records[i].key().toString());
                EXPECT_EQ(std::get<2>(messages[i]), records[i].value().toString());

                // Try to commit the offsets
                Offset expectedOffset = records[i].offset() + 1;
                consumer.commitAsync(records[i],
                                     [expectedOffset, topic, partition, &commitCbCount](const TopicPartitionOffsets& tpos, const Error& error){
                                         std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << expectedOffset << "], result[" << error.message() << "], tpos[" << toString(tpos) << "]" << std::endl;
                                         EXPECT_EQ(expectedOffset, tpos.at({topic, partition}));
                                         ++commitCbCount;
                                     });
            }
        }
    }

    EXPECT_EQ(messages.size(), commitCbCount);

    KafkaTestUtility::ResumeBrokers();
}

TEST(KafkaAutoCommitConsumer, BrokerStopBeforeConsumerStart)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Pause the brokers for a while
    auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

    // Consumer properties
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                            .put(ConsumerConfig::SESSION_TIMEOUT_MS,   "30000")
                            .put(ConsumerConfig::ENABLE_PARTITION_EOF, "true");

    // Start the consumer
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;


    TopicPartitions assignment;
    // In some corner cases, the assigned partitions might be empty (due to "Local: Broker node update" error), and we'll retry
    while (assignment.empty())
    {
        // Subscribe the topic
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will subscribe" << std::endl;
        consumer.subscribe({topic},
                            [&consumer, &assignment](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                                if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                    assignment = tps;
                                    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                                } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                                }
                            });
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;
    }

    // Fetch the broker metadata
    if (auto metadata = consumer.fetchBrokerMetadata(topic))
    {
        std::cout << "[" << Utility::getCurrentTime() << "] topic[" << topic << "], metadata[" << metadata->toString() << "]" << std::endl;
    }

    // Fetch all these EOFs
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_FALSE(records.empty());
    ASSERT_TRUE(std::all_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF; }));

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " polled " << records.size() << " EOFs" << std::endl;
}

TEST(KafkaAutoCommitConsumer, BrokerStopBeforeSubscription)
{
    // Consumer properties
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                            .put(ConsumerConfig::SESSION_TIMEOUT_MS,   "30000")
                            .put(ConsumerConfig::ENABLE_PARTITION_EOF, "true");

    // Start the consumer
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Pause the brokers for a while
    auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    TopicPartitions assignment;
    // In some corner cases, the assigned partitions might be empty (due to "Local: Broker node update" error), and we'll retry
    while (assignment.empty())
    {
        // Subscribe the topic
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will subscribe" << std::endl;
        consumer.subscribe({topic},
                            [&consumer, &assignment](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                                if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                    assignment = tps;
                                    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                                } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                                }
                            });
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;
    }

    // Fetch the broker metadata
    if (auto metadata = consumer.fetchBrokerMetadata(topic))
    {
        std::cout << "[" << Utility::getCurrentTime() << "] topic[" << topic << "], metadata[" << metadata->toString() << "]" << std::endl;
    }

    // Fetch all these EOFs
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_FALSE(records.empty());
    ASSERT_TRUE(std::all_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF; }));

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " polled " << records.size() << " EOFs" << std::endl;
}

TEST(KafkaAutoCommitConsumer, BrokerStopBeforeSeek)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Consumer properties
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                            .put(ConsumerConfig::SESSION_TIMEOUT_MS,   "30000")
                            .put(ConsumerConfig::ENABLE_PARTITION_EOF, "true");

    // Start the consumer
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe the topic
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will subscribe" << std::endl;
    consumer.subscribe({topic},
                        [&consumer](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                            }
                        });
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;

    // Pause the brokers for a while
    auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

    // Seed to the end (might throw an exception)
    constexpr int maxRetry = 3;
    for (int i = 0; i < maxRetry; ++i)
    {
        try
        {
            std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will seek to end" << std::endl;
            consumer.seekToEnd();
            std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " seeked to end" << std::endl;
            break;
        }
        catch (const KafkaException& e)
        {
            std::cout << "Exception caught: " << e.what() << std::endl;
        }
    }

    // Fetch messages (only EOFs could be got)
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(10));
    EXPECT_FALSE(records.empty());
    ASSERT_TRUE(std::all_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF; }));

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " polled " << records.size() << " EOFs" << std::endl;
}

TEST(KafkaAutoCommitConsumer, BrokerStopDuringMsgPoll)
{
    const Topic topic  = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare messages to test
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Produce some messages (with a producer)
    KafkaTestUtility::ProduceMessages(topic, 0, messages);

    // Consumer properties
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                            .put(ConsumerConfig::SESSION_TIMEOUT_MS, "30000")
                            .put(ConsumerConfig::AUTO_OFFSET_RESET,  "earliest"); // Seek to the very beginning

    // Start the consumer
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe the topic
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will subscribe" << std::endl;
    consumer.subscribe({topic},
                        [&consumer](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                            }
                        });
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;

    // Pause the brokers for a while
    auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

    // Fetch all these messages (would get messages once the brokers recover)
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(10));
    EXPECT_EQ(messages.size(), records.size());
    EXPECT_TRUE(std::none_of(records.cbegin(), records.cend(), [](const auto& record){ return record.error(); }));

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " polled " << records.size() << " messages" << std::endl;
}

