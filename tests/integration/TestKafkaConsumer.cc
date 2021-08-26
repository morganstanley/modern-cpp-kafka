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


TEST(KafkaAutoCommitConsumer, BasicPoll)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // The auto-commit consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic},
                       [](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                // assignment finished
                                std::cout << "[" << Utility::getCurrentTime() << "] assigned partitions: " << toString(tps) << std::endl;
                            }
                       });
    EXPECT_FALSE(consumer.subscription().empty());

    // No message yet
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
    EXPECT_EQ(0, records.size());

    // Try to get the beginning offsets
    const TopicPartition tp{topic, partition};
    std::cout << "[" << Utility::getCurrentTime() << "] Consumer get the beginningOffset[" << consumer.beginningOffsets({tp})[tp] << "]" << std::endl;;

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());

    // Check messages
    std::size_t rcvMsgCount = 0;
    for (auto& record: records)
    {
        ASSERT_TRUE(rcvMsgCount < messages.size());

        EXPECT_EQ(topic, record.topic());
        EXPECT_EQ(partition, record.partition());
        EXPECT_EQ(0, record.headers().size());
        EXPECT_EQ(std::get<1>(messages[rcvMsgCount]).size(), record.key().size());
        EXPECT_EQ(0, std::memcmp(std::get<1>(messages[rcvMsgCount]).c_str(), record.key().data(), record.key().size()));
        EXPECT_EQ(std::get<2>(messages[rcvMsgCount]).size(), record.value().size());
        EXPECT_EQ(0, std::memcmp(std::get<2>(messages[rcvMsgCount]).c_str(), record.value().data(), record.value().size()));

        ++rcvMsgCount;
    }

    // Close the consumer
    consumer.close();
}

TEST(KafkaAutoCommitConsumer, PollWithHeaders)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // The auto-commit consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic},
                       [](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                // assignment finished
                                std::cout << "[" << Utility::getCurrentTime() << "] assigned partitions: " << toString(tps) << std::endl;
                            }
                       });
    EXPECT_FALSE(consumer.subscription().empty());

    // No message yet
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
    EXPECT_EQ(0, records.size());

    // Prepare some messages to send
    const std::string v1 = "v1";
    const std::string v2 = "k2";
    const int         v3 = 134;  // the "value" is an "int" instead of a "string"
    const Headers headers = {
        Header("k1", Header::Value{v1.c_str(), v1.size()}),
        Header("k2", Header::Value{v2.c_str(), v2.size()}),
        Header("k1", Header::Value{&v3,        sizeof(v3)})     // Note, duplicated "key" in "headers"
    };
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {headers,   "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());

    // Check mesages
    std::size_t rcvMsgCount = 0;
    for (auto& record: records)
    {
        EXPECT_FALSE(record.error());
        ASSERT_TRUE(rcvMsgCount < messages.size());

        EXPECT_EQ(topic, record.topic());
        EXPECT_EQ(partition, record.partition());
        EXPECT_EQ(std::get<1>(messages[rcvMsgCount]), record.key().toString());
        EXPECT_EQ(std::get<2>(messages[rcvMsgCount]), record.value().toString());

        Headers headersInRecord = record.headers();
        const Headers& expectedHeader = std::get<0>(messages[rcvMsgCount]);
        ASSERT_EQ(expectedHeader.size(), headersInRecord.size());
        for (std::size_t i = 0; i < expectedHeader.size(); ++i)
        {
            EXPECT_EQ(expectedHeader[i].key, headersInRecord[i].key);
            EXPECT_EQ(expectedHeader[i].value.toString(), headersInRecord[i].value.toString());
        }

        // Here only check the first message, which has the headers
        if (!headersInRecord.empty())
        {
            // Get value from headers, for "k1"
            auto value = record.lastHeaderValue("k1");
            // The last header value for "k1", should be "v3", instead of "v1"
            ASSERT_EQ(sizeof(int), value.size());
            EXPECT_EQ(0, std::memcmp(&v3, value.data(), value.size()));

            // Nothing for a nonexist key
            value = record.lastHeaderValue("nonexist");
            EXPECT_EQ(nullptr, value.data());
            EXPECT_EQ(0, value.size());
        }

        ++rcvMsgCount;
    }

    // Close the consumer
    consumer.close();
}

TEST(KafkaAutoCommitConsumer, SeekAndPoll)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // The auto-commit consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                       .put(ConsumerConfig::MAX_POLL_RECORDS,   "1")         // Only poll 1 message each time
                       .put(ConsumerConfig::AUTO_OFFSET_RESET,  "earliest"); // Seek to the earliest offset at the beginning

    KafkaAutoCommitConsumer consumer(props);

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic},
                       [](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                // assignment finished
                                std::cout << "[" << Utility::getCurrentTime() << "] assigned partitions: " << toString(tps) << std::endl;
                            }
                       });
    EXPECT_FALSE(consumer.subscription().empty());

    // No message yet
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
    EXPECT_EQ(0, records.size());

    // Try to get the beginning offsets
    const TopicPartition tp{topic, partition};
    std::cout << "[" << Utility::getCurrentTime() << "] Consumer get the beginningOffset[" << consumer.beginningOffsets({tp})[tp] << "]" << std::endl;;

    // Prepare some messages to send
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());

    // Check messages
    std::size_t rcvMsgCount = 0;
    for (auto& record: records)
    {
        ASSERT_TRUE(rcvMsgCount < messages.size());

        EXPECT_EQ(topic, record.topic());
        EXPECT_EQ(partition, record.partition());
        EXPECT_EQ(0, record.headers().size());
        EXPECT_EQ(std::get<1>(messages[rcvMsgCount]).size(), record.key().size());
        EXPECT_EQ(0, std::memcmp(std::get<1>(messages[rcvMsgCount]).c_str(), record.key().data(), record.key().size()));
        EXPECT_EQ(std::get<2>(messages[rcvMsgCount]).size(), record.value().size());
        EXPECT_EQ(0, std::memcmp(std::get<2>(messages[rcvMsgCount]).c_str(), record.value().data(), record.value().size()));

        ++rcvMsgCount;
    }

    // Poll messages again (would get nothing)
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Seed to the end
    consumer.seekToEnd();
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " seeked to end" << std::endl;

    // Poll messages again (would get nothing)
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Seed to the beginning
    consumer.seekToBeginning();
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " seeked to beginning" << std::endl;

    // Poll messages again (would get all these messages)
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());

    // Check messages
    rcvMsgCount = 0;
    for (auto& record: records)
    {
        ASSERT_TRUE(rcvMsgCount < messages.size());

        EXPECT_EQ(topic, record.topic());
        EXPECT_EQ(partition, record.partition());
        EXPECT_EQ(0, record.headers().size());
        EXPECT_EQ(std::get<1>(messages[rcvMsgCount]).size(), record.key().size());
        EXPECT_EQ(0, std::memcmp(std::get<1>(messages[rcvMsgCount]).c_str(), record.key().data(), record.key().size()));
        EXPECT_EQ(std::get<2>(messages[rcvMsgCount]).size(), record.value().size());
        EXPECT_EQ(0, std::memcmp(std::get<2>(messages[rcvMsgCount]).c_str(), record.value().data(), record.value().size()));

        ++rcvMsgCount;
    }

    // Close the consumer
    consumer.close();
}

TEST(KafkaManualCommitConsumer, NoOffsetCommitCallback)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    {
        const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"); // Seek to the earliest offset at the beginning

        KafkaManualCommitConsumer consumer(props);

        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics, and seek to the beginning
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages and commit the offsets
        for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
        {
            auto records = consumer.poll(KafkaTestUtility::POLL_INTERVAL);
            numMsgPolled += records.size();

            if (records.empty()) continue;

            consumer.commitAsync(records.back());
        }
    }

    std::cout << "[" << Utility::getCurrentTime() << "] Consumer closed" << std::endl;
}

TEST(KafkaManualCommitConsumer, OffsetCommitCallback)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                       .put(ConsumerConfig::AUTO_OFFSET_RESET,  "earliest") // Seek to the earliest offset at the beginning
                       .put(ConsumerConfig::MAX_POLL_RECORDS,   "1");       // Only poll 1 message each time

    KafkaManualCommitConsumer consumer(props);

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics, and seek to the beginning
    consumer.subscribe({topic});
    EXPECT_FALSE(consumer.subscription().empty());

    std::atomic<std::size_t> commitCbCount = {0};

    // Poll messages and commit the offsets
    for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
    {
        auto records = consumer.poll(KafkaTestUtility::POLL_INTERVAL);
        numMsgPolled += records.size();

        if (records.empty()) continue;

        EXPECT_EQ(1, records.size());
        auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
        consumer.commitAsync(records[0],
                             [expected, &commitCbCount](const TopicPartitionOffsets& tpos, const Error& error) {
                                 std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << error.message()<< "]" << std::endl;
                                 EXPECT_EQ(1, tpos.size());
                                 EXPECT_EQ(std::get<2>(expected), tpos.at(TopicPartition{std::get<0>(expected), std::get<1>(expected)}));

                                 ++commitCbCount;
                             });
    }

    KafkaTestUtility::WaitUntil([&commitCbCount, expectedCnt = messages.size()](){ return expectedCnt == commitCbCount; },
                                KafkaTestUtility::MAX_OFFSET_COMMIT_TIMEOUT);

    EXPECT_EQ(messages.size(), commitCbCount);

    consumer.close();
    std::cout << "[" << Utility::getCurrentTime() << "] Consumer closed" << std::endl;
}

TEST(KafkaManualCommitConsumer, OffsetCommitCallbackTriggeredBeforeClose)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    std::size_t commitCbCount = 0;

    // The manual-commit consumer
    {
        const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                           .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest") // Seek to the earliest offset at the beginning
                           .put(ConsumerConfig::MAX_POLL_RECORDS,  "1");       // Only poll 1 message each time

        KafkaManualCommitConsumer consumer(props);

        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics, and seek to the beginning
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages and commit the offsets
        for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
        {
            auto records = consumer.poll(KafkaTestUtility::POLL_INTERVAL);
            numMsgPolled += records.size();

            if (records.empty()) continue;

            EXPECT_EQ(1, records.size());
            auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
            consumer.commitAsync(records[0],
                                 [expected, &commitCbCount](const TopicPartitionOffsets& tpos, const Error& error) {
                                     std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << error.message()<< "]" << std::endl;
                                     EXPECT_EQ(1, tpos.size());
                                     EXPECT_EQ(std::get<2>(expected), tpos.at(TopicPartition{std::get<0>(expected), std::get<1>(expected)}));

                                     ++commitCbCount;
                                 });
        }
    }

    std::cout << "[" << Utility::getCurrentTime() << "] Consumer closed" << std::endl;

    EXPECT_EQ(messages.size(), commitCbCount);
}

TEST(KafkaManualCommitConsumer, OffsetCommitCallback_ManuallyPollEvents)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                       .put(ConsumerConfig::AUTO_OFFSET_RESET,  "earliest") // Seek to the earliest offset at the beginning
                       .put(ConsumerConfig::MAX_POLL_RECORDS,   "1");       // Only poll 1 message each time

    KafkaManualCommitConsumer consumer(props, KafkaClient::EventsPollingOption::Manual);

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics, and seek to the beginning
    consumer.subscribe({topic});
    EXPECT_FALSE(consumer.subscription().empty());

    std::size_t commitCbCount = 0;

    // Poll messages and commit the offsets
    for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
    {
        auto records = consumer.poll(KafkaTestUtility::POLL_INTERVAL);
        numMsgPolled += records.size();

        if (records.empty()) continue;

        EXPECT_EQ(1, records.size());
        auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
        consumer.commitAsync(records[0],
                             [expected, &commitCbCount](const TopicPartitionOffsets& tpos, const Error& error) {
                                 std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << error.message()<< "]" << std::endl;
                                 EXPECT_EQ(1, tpos.size());
                                 EXPECT_EQ(std::get<2>(expected), tpos.at(TopicPartition{std::get<0>(expected), std::get<1>(expected)}));

                                 ++commitCbCount;
                             });
    }

    // Wait for the offset-commit callback (to be triggered)
    const auto end = std::chrono::steady_clock::now() + KafkaTestUtility::MAX_OFFSET_COMMIT_TIMEOUT;
    do
    {
        // keep polling for the offset commit callbacks
        consumer.pollEvents(KafkaTestUtility::POLL_INTERVAL);
    } while (std::chrono::steady_clock::now() < end && commitCbCount != messages.size());

    EXPECT_EQ(messages.size(), commitCbCount);

    consumer.close();
}

TEST(KafkaManualCommitConsumer, OffsetCommitAndPosition)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
        {Headers{}, "key4", "value4"},
    };

    const auto getMsgKey   = [&messages](std::size_t i) { return std::get<1>(messages[i]); };
    const auto getMsgValue = [&messages](std::size_t i) { return std::get<2>(messages[i]); };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // To save the configuration (including the client.id/group.id)
    Properties savedProps;

    std::size_t startCount = 0;

    // Start consumer a few times, but only commit the offset for the first message each time
    {
        auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::MAX_POLL_RECORDS,   "1");    // Only poll 1 message each time

        KafkaManualCommitConsumer consumer(props);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Save the configurations (including the client.id/group.id)
        savedProps = consumer.properties();

        // Subscribe topics, and seek to the beginning
        consumer.subscribe({topic},
                           [&consumer](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                               if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                   std::cout << "[" << Utility::getCurrentTime() << "] PartitionsAssigned: " << toString(tps) << std::endl;
                                   consumer.seekToBeginning();
                                   std::cout << "[" << Utility::getCurrentTime() << "] Seeked to the beginning" << std::endl;
                               }
                            });

        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages
        std::size_t rcvMsgCount = 0;
        std::atomic<std::size_t> commitCbCount = {0};
        {
            auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
            std::for_each(records.cbegin(), records.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
            EXPECT_EQ(messages.size() - startCount, records.size());

            for (auto& record: records)
            {
                ASSERT_TRUE(rcvMsgCount + startCount < messages.size());

                EXPECT_EQ(topic, record.topic());
                EXPECT_EQ(partition, record.partition());
                auto msgKey   = getMsgKey(rcvMsgCount + startCount);
                auto msgValue = getMsgValue(rcvMsgCount + startCount);
                EXPECT_EQ(msgKey.size(), record.key().size());
                EXPECT_EQ(0, std::memcmp(msgKey.c_str(), record.key().data(), record.key().size()));
                EXPECT_EQ(msgValue.size(), record.value().size());
                EXPECT_EQ(0, std::memcmp(msgValue.c_str(), record.value().data(), record.value().size()));

                ++rcvMsgCount;

                // Only commit 1 offset each time
                if (rcvMsgCount == 1)
                {
                    std::cout << "[" << Utility::getCurrentTime() << "] will commit offset for record[" << record.toString() << "]" << std::endl;
                    consumer.commitAsync(record,
                                         [expectedOffset = record.offset() + 1, topic, partition, &commitCbCount, &startCount, rcvMsgCount]
                                         (const TopicPartitionOffsets& tpos, const Error& error) {
                                             std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << expectedOffset << "], got result[" << error.message() << "], tpos[" << toString(tpos) << "]" << std::endl;
                                             if (!error) {
                                                 EXPECT_EQ(expectedOffset, tpos.at({topic, partition}));
                                                 startCount = rcvMsgCount;
                                             }
                                             ++commitCbCount;
                                         });
                }
            }
        }

        // Wait for the offset-commit callback (to be triggered)
        KafkaTestUtility::WaitUntil([&commitCbCount]() {return commitCbCount == 1; }, KafkaTestUtility::MAX_OFFSET_COMMIT_TIMEOUT);
    }

    // Start the consumer (2nd time)
    {
        KafkaManualCommitConsumer consumer(savedProps);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages
        std::size_t rcvMsgCount = 0;
        {
            auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
            std::for_each(records.cbegin(), records.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
            EXPECT_EQ(messages.size() - startCount, records.size());

            for (auto& record: records)
            {
                ASSERT_TRUE(rcvMsgCount + startCount < messages.size());

                EXPECT_EQ(topic, record.topic());
                EXPECT_EQ(partition, record.partition());
                auto msgKey   = getMsgKey(rcvMsgCount + startCount);
                auto msgValue = getMsgValue(rcvMsgCount + startCount);
                EXPECT_EQ(msgKey.size(), record.key().size());
                EXPECT_EQ(0, std::memcmp(msgKey.c_str(), record.key().data(), record.key().size()));
                EXPECT_EQ(msgValue.size(), record.value().size());
                EXPECT_EQ(0, std::memcmp(msgValue.c_str(), record.value().data(), record.value().size()));

                ++rcvMsgCount;

                // Only commit 1 offset each time
                if (rcvMsgCount == 1)
                {
                    std::cout << "[" << Utility::getCurrentTime() << "] will commit offset for record[" << record.toString() << "]" << std::endl;
                    // Retry for "Broker: Request timed out" error (if any)
                    RETRY_FOR_ERROR(consumer.commitSync(record), RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, 2);
                }
            }
        }

        EXPECT_EQ(messages.size() - startCount, rcvMsgCount);
    }

    ++startCount;

    // Start the consumer (3rd time)
    {
        KafkaManualCommitConsumer consumer(savedProps);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages
        std::size_t rcvMsgCount = 0;
        {
            auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
            std::for_each(records.cbegin(), records.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
            EXPECT_EQ(messages.size() - startCount, records.size());

            for (auto& record: records)
            {
                ASSERT_TRUE(rcvMsgCount + startCount < messages.size());

                EXPECT_EQ(topic, record.topic());
                EXPECT_EQ(partition, record.partition());
                auto msgKey   = getMsgKey(rcvMsgCount + startCount);
                auto msgValue = getMsgValue(rcvMsgCount + startCount);
                EXPECT_EQ(msgKey.size(), record.key().size());
                EXPECT_EQ(0, std::memcmp(msgKey.c_str(), record.key().data(), record.key().size()));
                EXPECT_EQ(msgValue.size(), record.value().size());
                EXPECT_EQ(0, std::memcmp(msgValue.c_str(), record.value().data(), record.value().size()));

                ++rcvMsgCount;

                // Only commit 1 offset each time
                if (rcvMsgCount == 1)
                {
                    std::cout << "[" << Utility::getCurrentTime() << "] will commit offset for record[" << record.toString() << "]" << std::endl;
                    TopicPartitionOffsets tpos;
                    tpos[{topic, partition}] = record.offset() + 1;

                    // Retry for "Broker: Request timed out" error (if any)
                    RETRY_FOR_ERROR(consumer.commitSync(tpos), RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, 2);
                }
            }
        }

        EXPECT_EQ(messages.size() - startCount, rcvMsgCount);
    }

    ++startCount;

    // Start the consumer (4th time)
    {
        KafkaManualCommitConsumer consumer(savedProps);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages
        std::size_t rcvMsgCount = 0;
        {
            auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
            std::for_each(records.cbegin(), records.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
            EXPECT_EQ(messages.size() - startCount, records.size());

            for (auto& record: records)
            {
                ASSERT_TRUE(rcvMsgCount + startCount < messages.size());

                EXPECT_EQ(topic, record.topic());
                EXPECT_EQ(partition, record.partition());
                auto msgKey   = getMsgKey(rcvMsgCount + startCount);
                auto msgValue = getMsgValue(rcvMsgCount + startCount);
                EXPECT_EQ(msgKey.size(), record.key().size());
                EXPECT_EQ(0, std::memcmp(msgKey.c_str(), record.key().data(), record.key().size()));
                EXPECT_EQ(msgValue.size(), record.value().size());
                EXPECT_EQ(0, std::memcmp(msgValue.c_str(), record.value().data(), record.value().size()));

                ++rcvMsgCount;
            }

            std::cout << "[" << Utility::getCurrentTime() << "] will commit for all polled messages" << std::endl;
            // Retry for "Broker: Request timed out" error (if any)
            RETRY_FOR_ERROR(consumer.commitSync(), RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, 1);
        }

        EXPECT_EQ(messages.size() - startCount, rcvMsgCount);
    }

    // Start the consumer, -- since all records have been committed, no record polled any more
    {
        KafkaManualCommitConsumer consumer(savedProps);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // poll messages (nothing left)
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        EXPECT_TRUE(records.empty());
    }
}

TEST(KafkaManualCommitConsumer, CommitOffsetBeforeRevolkingPartitions)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {NullHeaders, "key1", "value1"},
        {NullHeaders, "key2", "value2"},
        {NullHeaders, "key3", "value3"},
        {NullHeaders, "key4", "value4"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Prepare poperties for consumers
    auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                    .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                    .put(ConsumerConfig::GROUP_ID, Utility::getRandomString());

    {
        // First consumer starts
        KafkaManualCommitConsumer consumer(props);


        consumer.subscribe({topic},
                           [&consumer](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                               if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                   std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assigned partitions: " << toString(tps) << std::endl;
                               } else {
                                   consumer.commitSync();
                                   std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " sync-committed offsets"  << std::endl;
                                   std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " will revolke partitions: " << toString(tps) << std::endl;
                               }
                           });

        // Get all messages
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        //for (const auto& record: records) tpos[TopicPartition{record.topic(), record.partition()}] = record.offset() + 1;
        EXPECT_EQ(messages.size(), records.size());
    }

    // Send one more message
    KafkaTestUtility::ProduceMessages(topic, partition, {{NullHeaders, "key4", "value4"}});

    {
        // Second consumer starts
        KafkaManualCommitConsumer consumer(props);

        consumer.subscribe({topic});

        // Get all messages (but none)
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        EXPECT_EQ(1, records.size());
    }
}

TEST(KafkaAutoCommitConsumer, OffsetCommitAndPosition)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
        {Headers{}, "key4", "value4"},
    };

    const auto getMsgKey   = [&messages](std::size_t i) { return std::get<1>(messages[i]); };
    const auto getMsgValue = [&messages](std::size_t i) { return std::get<2>(messages[i]); };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // To save the configuration (including the client.id/group.id)
    Properties savedProps;

    constexpr int maxRecordsPolledAtFirst = 2;

    // Consumer will poll twice, -- Note, the last polled message offset would not be committed (no following `poll`)
    {
        const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::MAX_POLL_RECORDS, "1");

        KafkaAutoCommitConsumer consumer(props);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Save the properties
        savedProps = consumer.properties();

        // Subscribe topics
        consumer.subscribe({topic});

        // Check the metadata
        auto metadata = consumer.fetchBrokerMetadata(topic);
        ASSERT_TRUE(metadata);
        std::cout << "[" << Utility::getCurrentTime() << "] topic[" << topic << "], metadata[" << (metadata ? metadata->toString() : "NA" ) << "]" << std::endl;

        // Seek to beginning
        consumer.seekToBeginning();
        std::cout << "[" << Utility::getCurrentTime() << "] Seeked to the beginning" << std::endl;

        // Poll messages
        std::size_t rcvMsgCount = 0;

        while (rcvMsgCount < maxRecordsPolledAtFirst)
        {
            auto polled = consumer.poll(KafkaTestUtility::POLL_INTERVAL);
            if (polled.empty()) continue;

            EXPECT_TRUE(std::none_of(polled.cbegin(), polled.cend(), [](const auto& record) { return record.error(); }));
            std::for_each(polled.cbegin(), polled.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
            // Due to "max.poll.records=1" property, one message fetched at most
            EXPECT_EQ(1, polled.size());

            EXPECT_TRUE(rcvMsgCount < messages.size());
            ++rcvMsgCount;

            const auto& record = polled[0];

            // Check the position() , -- should return the offset of the next message, which could be fetched
            EXPECT_EQ(record.offset() + 1, consumer.position({record.topic(), record.partition()}));

            // committed() should return the "offset(previous polled) + 1"
            if (rcvMsgCount > 1)
            {
                for (int retry = 0; retry < 5 && (record.offset() - 1 + 1 != consumer.committed({record.topic(), record.partition()})); ++retry)
                {
                    // Wait a while for the async commit to take effect
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                EXPECT_EQ(record.offset() - 1 + 1, consumer.committed({record.topic(), record.partition()}));
            }
        }

        // Close the consumer (and commit all pending offset)
        consumer.close();
    }

    // Note, the last message was not committed previously
    // Here we'll start another consumer to continue...
    {
        KafkaAutoCommitConsumer consumer(savedProps);
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        std::for_each(records.cbegin(), records.cend(), [](const auto& record) { std::cout << record.toString() << std::endl; });
        ASSERT_EQ(messages.size(), maxRecordsPolledAtFirst + records.size());

        // Check all these records polled
        std::size_t msgIndex = maxRecordsPolledAtFirst;
        for (auto& record: records)
        {
            EXPECT_EQ(topic, record.topic());
            EXPECT_EQ(partition, record.partition());
            auto msgKey   = getMsgKey(msgIndex);
            auto msgValue = getMsgValue(msgIndex);
            EXPECT_EQ(msgKey.size(), record.key().size());
            EXPECT_EQ(0, std::memcmp(msgKey.c_str(), record.key().data(), record.key().size()));
            EXPECT_EQ(msgValue.size(), record.value().size());
            EXPECT_EQ(0, std::memcmp(msgValue.c_str(), record.value().data(), record.value().size()));

            ++msgIndex;
        }
    }
}

TEST(KafkaAutoCommitConsumer, RebalancePartitionsAssign)
{
    const Topic       topic     = Utility::getRandomString();
    const std::string group     = Utility::getRandomString();
    const Partition   partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "], group[" << group << "] would be used" << std::endl;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare the consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::GROUP_ID, group);

    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    std::vector<TopicPartitions> partitionsAssigned;
    // Subscribe topics
    consumer.subscribe({topic},
                        [&partitionsAssigned](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                std::cout << "[" << Utility::getCurrentTime() << "] Consumer PartitionsAssigned: " << toString(tps) << std::endl;
                                partitionsAssigned.emplace_back(tps);
                            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                std::cout << "[" << Utility::getCurrentTime() << "] Consumer PartitionsRevoked: " << toString(tps) << std::endl;
                            }
                        });

    // Start another consumer, and it would take some partitions away during the time
    std::cout << "[" << Utility::getCurrentTime() << "] Second consumer will start" << std::endl;
    auto fut = std::async(std::launch::async,
                          [topic, group]() {
                              auto consumerProps = KafkaTestUtility::GetKafkaClientCommonConfig()
                                                   .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                                   .put(ConsumerConfig::GROUP_ID,          group);
                              KafkaAutoCommitConsumer anotherConsumer(consumerProps);
                              anotherConsumer.subscribe({topic});
                              KafkaTestUtility::ConsumeMessagesUntilTimeout(anotherConsumer);
                          });

    // Keep polling, in order to trigger any callback
    const auto KEEP_POLLING_TIMEOUT = std::chrono::seconds(30);
    const auto end = std::chrono::steady_clock::now() + KEEP_POLLING_TIMEOUT;
    do
    {
        consumer.poll(KafkaTestUtility::POLL_INTERVAL);
    } while (std::chrono::steady_clock::now() < end);

    fut.get();
    std::cout << "[" << Utility::getCurrentTime() << "] Second consumer closed" << std::endl;

    // Start a producer to send some messages
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    KafkaTestUtility::ProduceMessages(topic, partition, messages);

    // Keep polling
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());
    for (const auto& record: records)
    {
        std::cout << "Got record: " << record.toString() << std::endl;
    }

    // Partitions assignment would be triggered 3 times
    ASSERT_EQ(3, partitionsAssigned.size());
    // The 2nd partitions assignment would take part of these partitions (the other part would be assigned to the other consumer)
    EXPECT_TRUE(partitionsAssigned[1].size() < partitionsAssigned[0].size());
    // While the other consumer is closed, it would take all partitions again
    EXPECT_EQ(partitionsAssigned[0], partitionsAssigned[2]);

    // Close the consumer
    consumer.close();
}

TEST(KafkaAutoCommitConsumer, ThreadCount)
{
    {
        KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;
        std::cout << "[" << Utility::getCurrentTime() << "] librdkafka thread cnt[" << Utility::getLibRdKafkaThreadCount() << "]" << std::endl;

        // Just wait a short while, thus make sure all background threads be started
        std::this_thread::sleep_for(std::chrono::seconds(1));

        EXPECT_EQ(KafkaTestUtility::GetNumberOfKafkaBrokers() + 3, Utility::getLibRdKafkaThreadCount());
    }

    EXPECT_EQ(0, Utility::getLibRdKafkaThreadCount());
}

TEST(KafkaAutoCommitConsumer, PartitionAssignment)
{
    const Topic     topic      = Utility::getRandomString();
    const Partition partition1 = 0;
    const Partition partition2 = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Assign topic-partitions
    const TopicPartitions tps{{topic, partition1}, {topic, partition2}};
    consumer.assign(tps);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assigned" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assignment[" << toString(consumer.assignment()) << "]" << std::endl;

    EXPECT_EQ(tps, consumer.assignment());
    EXPECT_TRUE(consumer.subscription().empty());
}

TEST(KafkaAutoCommitConsumer, TopicSubscription)
{
    const Topics topics = { Utility::getRandomString(), Utility::getRandomString(), Utility::getRandomString() };

    constexpr int NUM_PARTITIONS = 5;
    constexpr int REPLICA_FACTOR = 3;
    for (const auto& topic: topics) KafkaTestUtility::CreateKafkaTopic(topic, NUM_PARTITIONS, REPLICA_FACTOR);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe(topics);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscription[" << toString(consumer.subscription()) << "]" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assignment[" << toString(consumer.assignment()) << "]" << std::endl;

    EXPECT_EQ(topics.size(), consumer.subscription().size());
    std::for_each(topics.cbegin(), topics.cend(), [&consumer](const auto& topic) { EXPECT_EQ(1, consumer.subscription().count(topic)); });
    EXPECT_EQ(NUM_PARTITIONS * topics.size(), consumer.assignment().size());
}

TEST(KafkaAutoCommitConsumer, SubscribeUnsubscribeThenAssign)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic});
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;

    EXPECT_EQ(1, consumer.subscription().size());
    EXPECT_EQ(1, consumer.subscription().count(topic));

    // Unsubscribe topics
    consumer.unsubscribe();
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " unsubscribed" << std::endl;

    EXPECT_EQ(0, consumer.subscription().size());

    // Assign topic-partitions
    const TopicPartitions tps{{topic, partition}};
    consumer.assign(tps);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assigned" << std::endl;

    EXPECT_EQ(tps, consumer.assignment());
    EXPECT_TRUE(consumer.subscription().empty());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assignment[" << toString(consumer.assignment()) << "]" << std::endl;
}

TEST(KafkaAutoCommitConsumer, AssignUnassignAndSubscribe)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    constexpr int NUM_PARTITIONS = 5;
    constexpr int REPLICA_FACTOR = 3;
    KafkaTestUtility::CreateKafkaTopic(topic, NUM_PARTITIONS, REPLICA_FACTOR);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Assign topic-partitions
    TopicPartitions tps{{topic, partition}};
    consumer.assign(tps);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assigned" << std::endl;

    EXPECT_EQ(tps, consumer.assignment());

    // Assign empty topic-partitions
    tps = TopicPartitions();
    consumer.assign(tps);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assigned" << std::endl;

    EXPECT_TRUE(consumer.assignment().empty());

    // Subscribe topics
    consumer.subscribe({topic});
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscription[" << toString(consumer.subscription()) << "]" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " assignment[" << toString(consumer.assignment()) << "]" << std::endl;

    EXPECT_EQ(1, consumer.subscription().size());
    EXPECT_EQ(1, consumer.subscription().count(topic));
    EXPECT_EQ(NUM_PARTITIONS, consumer.assignment().size());
}

TEST(KafkaAutoCommitConsumer, WrongOperation_SeekBeforePartitionsAssigned)
{
    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " would seekToBeginning" << std::endl;

    // Seek with an unassigned partition -- would throw exception
    EXPECT_KAFKA_THROW(consumer.seek({"unassigned_topic", 0}, 0), RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION);
}

TEST(KafkaAutoCommitConsumer, WrongOperation_SubscribeThenAssign)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic});
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;

    EXPECT_EQ(1, consumer.subscription().size());
    EXPECT_EQ(1, consumer.subscription().count(topic));

    // Assign topic-partitions -- would throw exception
    EXPECT_KAFKA_THROW(consumer.assign({{topic, partition}}), RD_KAFKA_RESP_ERR__FAIL);
}

TEST(KafkaAutoCommitConsumer, WrongOperation_AssignThenSubscribe)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Assign topic-partitions
    TopicPartitions tps{{topic, partition}};
    consumer.assign(tps);

    EXPECT_EQ(tps, consumer.assignment());

    // Subscribe topics -- would throw exception
    EXPECT_KAFKA_THROW(consumer.subscribe({topic}), RD_KAFKA_RESP_ERR__FAIL);
}

TEST(KafkaClient, FetchBrokerMetadata)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Start consumer
    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    TopicPartitions assignment;
    consumer.subscribe({topic},
                        [&consumer, &assignment](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                                assignment = tps;
                            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                                assignment.clear();
                            }
                        });
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;

    auto brokerMetadata = consumer.fetchBrokerMetadata(topic);
    ASSERT_TRUE(brokerMetadata);

    std::cout << "[" << Utility::getCurrentTime() << "] Brokers' metadata: " << brokerMetadata->toString() << std::endl;

    EXPECT_EQ(topic, brokerMetadata->topic());
    EXPECT_EQ(assignment.size(), brokerMetadata->partitions().size());

    consumer.close();
}

TEST(KafkaAutoCommitConsumer, SubscribeAndPoll)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::ENABLE_PARTITION_EOF, "true");

    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    TopicPartitions assignedPartitions;

    // Subscribe topics
    consumer.subscribe({topic},
                       [&assignedPartitions](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                assignedPartitions = tps;
                                // assignment finished
                                std::cout << "[" << Utility::getCurrentTime() << "] PartitionsAssigned: " << toString(tps) << std::endl;
                            } else {
                                assignedPartitions.clear();
                                std::cout << "[" << Utility::getCurrentTime() << "] PartitionsRevoked" << std::endl;
                            }
                       });

    EXPECT_FALSE(consumer.subscription().empty());
    EXPECT_FALSE(assignedPartitions.empty());

    // Poll all the messages
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

    // Count the EOF
    EXPECT_EQ(assignedPartitions.size(),
              std::count_if(records.cbegin(), records.cend(),
                            [](const auto& record) { return record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF; }));

    // Close the consumer
    consumer.close();
}

TEST(KafkaAutoCommitConsumer, PauseAndResume)
{
    const Topic topic1 = Utility::getRandomString();
    const Topic topic2 = Utility::getRandomString();

    KafkaTestUtility::CreateKafkaTopic(topic1, 5, 3);
    KafkaTestUtility::CreateKafkaTopic(topic2, 5, 3);

    // Produce messages towards topic1
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "", "msg1"},
        {Headers{}, "", "msg2"},
        {Headers{}, "", "msg3"}
    };
    KafkaTestUtility::ProduceMessages(topic1, 0, messages);

    // An auto-commit Consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::AUTO_OFFSET_RESET,    "earliest")
                        .put(ConsumerConfig::MAX_POLL_RECORDS,     "1");
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic1, topic2});

    // Poll 1 message from topic1
    auto records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages[0]), records.front().value().toString());

    // Pause, then could not poll any message
    consumer.pause();
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    // Resume, would be able to poll again
    consumer.resume();
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages[1]), records.front().value().toString());

    // Try to pause an invalid partition
    EXPECT_KAFKA_THROW(consumer.pause({TopicPartition("invalid_topic", 12345)}), RD_KAFKA_RESP_ERR__INVALID_ARG);

    // Pause a partition of topic1
    consumer.pause({TopicPartition(topic1, 0)});
    // No message could be polled from topic1
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    // Producer messages towards topic2
    const std::vector<std::tuple<Headers, std::string, std::string>> messages2 = {{ Headers{}, "", "msg4" }};
    KafkaTestUtility::ProduceMessages(topic2, 0, messages2);

    // Could still poll from topic2
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages2[0]), records.front().value().toString());

    // No more message (only 1 message from topic2)
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    // Resume the partition of topic1 (then could poll message again)
    consumer.resume({TopicPartition(topic1, 0)});
    records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages[2]), records.front().value().toString());

    consumer.close();
}

TEST(KafkaManualCommitConsumer, SeekAfterPause)
{
    const Topic topic = Utility::getRandomString();

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Produce messages towards topic
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "", "msg1"},
        {Headers{}, "", "msg2"},
        {Headers{}, "", "msg3"}
    };
    KafkaTestUtility::ProduceMessages(topic, 0, messages);

    // An auto-commit Consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                        .put(ConsumerConfig::MAX_POLL_RECORDS,  "1");
    KafkaManualCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic});

    // Poll 1 message from topic
    auto records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages[0]), records.front().value().toString());

    // First, pause the partition
    consumer.pause();

    // Then, seek back (to the very first offset)
    consumer.seek({topic, 0}, records[0].offset());

    // Could not poll any message (with partition paused)
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Resume the partition (and continue)
    consumer.resume();

    // Then would be able to poll from the very beginning
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    ASSERT_EQ(messages.size(), records.size());
    for (std::size_t i = 0; i < messages.size(); ++i)
    {
        EXPECT_EQ(std::get<2>(messages[i]), records[i].value().toString());
    }
}

TEST(KafkaManualCommitConsumer, DISABLED_SeekBeforePause)
{
    const Topic topic = Utility::getRandomString();

    KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

    // Produce messages towards topic
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "", "msg1"},
        {Headers{}, "", "msg2"},
        {Headers{}, "", "msg3"}
    };
    KafkaTestUtility::ProduceMessages(topic, 0, messages);

    // An auto-commit Consumer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                        .put(ConsumerConfig::MAX_POLL_RECORDS,  "1")
                        .put("log_level", "7")
                        .put("debug",     "all");
    KafkaManualCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic});

    // Poll 1 message from topic
    auto records = consumer.poll(KafkaTestUtility::MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    EXPECT_EQ(std::get<2>(messages[0]), records.front().value().toString());

    // First, seek back (to the very first offset)
    consumer.seek({topic, 0}, records[0].offset());

    // Then, pause the partition
    consumer.pause();

    // Could not poll any message (with partition paused)
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Resume the partition (and continue)
    consumer.resume();

    // Then would be able to poll from the very beginning
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    ASSERT_EQ(messages.size(), records.size());
    for (std::size_t i = 0; i < messages.size(); ++i)
    {
        EXPECT_EQ(std::get<2>(messages[i]), records[i].value().toString());
    }
}

TEST(KafkaAutoCommitConsumer, PauseStillWorksAfterRebalance)
{
    const Topic topic1 = Utility::getRandomString();
    const Topic topic2 = Utility::getRandomString();

    KafkaTestUtility::CreateKafkaTopic(topic1, 5, 3);
    KafkaTestUtility::CreateKafkaTopic(topic2, 5, 3);

    // Start the consumer1
    auto props1 = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::SESSION_TIMEOUT_MS, "60000")
                        .put(ConsumerConfig::MAX_POLL_RECORDS,   "1");
    KafkaAutoCommitConsumer consumer1(props1);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer1.name() << " started" << std::endl;

    // Subscribe topics
    consumer1.subscribe({topic1, topic2},
                        [&consumer1](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer1.name() << " PartitionsAssigned: " << toString(tps) << std::endl;
                            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                std::cout << "[" << Utility::getCurrentTime() << "] " << consumer1.name() << " PartitionsRevoked: " << toString(tps) << std::endl;
                            }
                        });

    // Let's pause and then see what happens after partitions-rebalance
    consumer1.pause({TopicPartition(topic1, 0)});

    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer1);
    EXPECT_EQ(0, records.size());

    std::promise<void> p;
    auto fu = p.get_future();
    // Anther consumer with the same group.id
    const auto props2 = props1.put(ConsumerConfig::GROUP_ID, *consumer1.getProperty(ConsumerConfig::GROUP_ID));
    KafkaTestUtility::JoiningThread consumer2Thread(
        [props2, topic1, topic2, &p]() {
            KafkaAutoCommitConsumer consumer2(props2);
            consumer2.subscribe({topic1, topic2});
            for (int i = 0; i < 50; ++i) {
                consumer2.poll(std::chrono::milliseconds(100));
            }
            consumer2.close();
            p.set_value();
        }
    );

    // Keep polling thus rebalance works (for both consumer1 & consumer2)
    for (int i = 0; i < 300; ++i)
    {
        consumer1.poll(std::chrono::milliseconds(100));
    }

    // Wait for consumer2 finishing the partitions-rebalance (first partitions-assigned, then partitions-revoked)
    fu.wait();

    // Produce a message
    std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"}
    };
    KafkaTestUtility::ProduceMessages(topic1, 0, messages);

    // Try to poll from a paused partition
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer1);
    EXPECT_EQ(0, records.size());

    // Resume the consumer
    consumer1.resume();

    // Now we get the message
    records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer1);
    EXPECT_EQ(1, records.size());

    consumer1.close();
}

TEST(KafkaManualCommitConsumer, OffsetsForTime)
{
    const Topic     topic1     = Utility::getRandomString();
    const Partition partition1 = 0;
    const Topic     topic2     = Utility::getRandomString();
    const Partition partition2 = 1;

    KafkaTestUtility::CreateKafkaTopic(topic1, 5, 3);
    KafkaTestUtility::CreateKafkaTopic(topic2, 5, 3);

    using namespace std::chrono;

    constexpr int MESSAGES_NUM = 5;

    std::vector<time_point<system_clock>> checkPoints;
    std::vector<TopicPartitionOffsets>    expectedOffsets;

    std::cout << "Produce messages:" << std::endl;
    {
        Kafka::KafkaSyncProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        for (int i = 0; i < MESSAGES_NUM; ++i)
        {
            checkPoints.emplace_back(system_clock::now());

            TopicPartitionOffsets expected;

            auto metadata1 = producer.send(Kafka::ProducerRecord(topic1, partition1, Kafka::NullKey, Kafka::NullValue));
            std::cout << "[" << Utility::getCurrentTime() << "] Just send a message, metadata: " << metadata1.toString() << std::endl;
            if (auto offset = metadata1.offset())
            {
                expected[{topic1, partition1}] = *offset;
            }

            auto metadata2 = producer.send(Kafka::ProducerRecord(topic2, partition2, Kafka::NullKey, Kafka::NullValue));
            std::cout << "[" << Utility::getCurrentTime() << "] Just send a message, metadata: " << metadata2.toString() << std::endl;
            if (auto offset = metadata2.offset())
            {
                expected[{topic2, partition2}] = *offset;
            }

            expectedOffsets.emplace_back(expected);

            std::this_thread::sleep_for(milliseconds(200));
        }
    }

    std::cout << "Try with normal case:" << std::endl;
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
        consumer.subscribe({topic1, topic2});
        for (int i = 0; i < MESSAGES_NUM; ++i)
        {
            const auto timepoint = checkPoints[i];
            const auto expected  = expectedOffsets[i];

            auto offsets = consumer.offsetsForTime({{topic1, partition1}, {topic2, partition2}}, timepoint);

            std::cout << "Got offsets: " << Kafka::toString(offsets) << ", for time: " << Timestamp(duration_cast<milliseconds>(timepoint.time_since_epoch()).count()).toString() << std::endl;
            EXPECT_EQ(expected, offsets);
        }
    }

    std::cout << "Try with no subcription:" << std::endl;
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());

        // Here we doesn't subsribe to topic1 or topic2 (the result is undefined)
        for (int i = 0; i < MESSAGES_NUM; ++i)
        {
            try
            {
                const auto timepoint = checkPoints[i];
                const auto tp1       = Kafka::TopicPartition{topic1, partition1};
                const auto tp2       = Kafka::TopicPartition{topic2, partition2};
                const auto offsets   = consumer.offsetsForTime({tp1, tp2}, timepoint);

                EXPECT_TRUE((offsets == Kafka::TopicPartitionOffsets{{tp1, expectedOffsets[i][tp1]}}
                             || offsets == Kafka::TopicPartitionOffsets{{tp2, expectedOffsets[i][tp2]}}
                             || offsets == expectedOffsets[i]));                    // Might (partially) succeed
            }
            catch (const Kafka::KafkaException& e)
            {
                EXPECT_EQ(RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION, e.error().value()); // Or, fail
            }
        }
     }

    std::cout << "Try with all invalid topic-partitions: (exception caught)" << std::endl;
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());

        const auto timepoint = checkPoints[0];

        EXPECT_KAFKA_THROW({consumer.offsetsForTime({{Utility::getRandomString(), 100}}, timepoint);},
                           RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION);
    }

    std::cout << "Try with partial valid topic-partitions:" << std::endl;
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());
        consumer.subscribe({topic1, topic2});

        for (int i = 0; i < MESSAGES_NUM; ++i)
        {
            const auto timepoint = checkPoints[i];
            const auto validTp   = Kafka::TopicPartition{topic1, partition1};
            const auto invalidTp = Kafka::TopicPartition{Utility::getRandomString(), 100};
            const auto offsets   = consumer.offsetsForTime({validTp, invalidTp}, timepoint);

            std::cout << "Got offsets: " << Kafka::toString(offsets) << ", for time: " << Timestamp(duration_cast<milliseconds>(timepoint.time_since_epoch()).count()).toString() << std::endl;
            EXPECT_EQ((Kafka::TopicPartitionOffsets{{validTp, expectedOffsets[i][validTp]}}), offsets);
       }
    }
}

TEST(KafkaManualCommitConsumer, RecoverByTime)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Prepare some messages to send
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
        {"key4", "value4"},
        {"key5", "value5"},
    };

    // Send the messages
    {
        Kafka::KafkaSyncProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        for (const auto& msg: messages)
        {
            auto record = Kafka::ProducerRecord(topic,
                                                partition,
                                                Kafka::Key(msg.first.c_str(), msg.first.size()),
                                                Kafka::Value(msg.second.c_str(), msg.second.size()));
            auto metadata = producer.send(record);

            std::cout << "[" << Utility::getCurrentTime() << "] Just sent a message: " << record.toString() << ", metadata: " << metadata.toString() << std::endl;
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }


    std::chrono::time_point<std::chrono::system_clock> persistedTimepoint;
    std::vector<std::pair<std::string, std::string>>   messagesProcessed;

    // The first consumer quits, and fails to handle all messages
    constexpr int FAILURE_MSG_INDEX = 3;
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        for (std::size_t i = 0; i < records.size(); ++i)
        {
            const auto& record = records[i];

            // Save the timepoint
            persistedTimepoint = record.timestamp();

            // E.g, Something fails, and consumer will quit
            if (i == FAILURE_MSG_INDEX) break;

            // Process messages
            messagesProcessed.emplace_back(record.key().toString(), record.value().toString());

            std::cout << "[" << Utility::getCurrentTime() << "] Processed message: " << record.toString() << std::endl;
        }
    }

    // The second consumer catches up and continue
    {
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());

        TopicPartitions assignedPartitions;
        // Subscribe topics
        consumer.subscribe({topic},
                           [&assignedPartitions](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                                if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                    std::cout << "[" << Utility::getCurrentTime() << "] assigned partitions: " << toString(tps) << std::endl;
                                    assignedPartitions = tps;
                                }
                           });
        // No message yet
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
        EXPECT_EQ(0, records.size());

        // Query where the consumer should start from
        const auto offsetsToSeek = consumer.offsetsForTime(assignedPartitions, persistedTimepoint);
        std::cout << "Offsets to seek: " << toString(offsetsToSeek) << std::endl;

        // Seek to these positions
        for (const auto& tpo: offsetsToSeek)
        {
            const TopicPartition& tp = tpo.first;
            const Offset&         o  = tpo.second;

            consumer.seek(tp, o);
        }

        // Process messages
        records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        for (const auto& record: records)
        {
            messagesProcessed.emplace_back(record.key().toString(), record.value().toString());

            std::cout << "[" << Utility::getCurrentTime() << "] Processed message: " << record.toString() << std::endl;
        }
    }

    // All messages should be processed
    ASSERT_EQ(messages.size(), messagesProcessed.size());
    for (std::size_t i = 0; i < messages.size(); ++i)
    {
        EXPECT_EQ(messages[i].first,  messagesProcessed[i].first);
        EXPECT_EQ(messages[i].second, messagesProcessed[i].second);
    }
}

// `allow.auto.create.topics` has no longer been supported since librdkafka v1.6.0
TEST(KafkaAutoCommitConsumer, AutoCreateTopics)
{
    const Topic topic = Utility::getRandomString();

    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                     .put("allow.auto.create.topics", "true"));

    // The error would be triggered while consumer tries to subscribe a non-existed topic.
    consumer.setErrorCallback([](const Error& error) {
                                  std::cout << "consumer met an error: " << error.toString() << std::endl;
                                  EXPECT_EQ(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, error.value());
                              });

    // Subscribe topics, but would never make it!
    EXPECT_KAFKA_THROW(consumer.subscribe({topic}, Consumer::NullRebalanceCallback, std::chrono::seconds(10)),
                       RD_KAFKA_RESP_ERR__TIMED_OUT);

    EXPECT_TRUE(consumer.assignment().empty());
}

TEST(KafkaAutoCommitConsumer, CreateTopicAfterSubscribe)
{
    const Topic topic = Utility::getRandomString();

    auto createTopicAfterSeconds = [topic](int seconds) {
        std::this_thread::sleep_for(std::chrono::seconds(seconds));
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 1);
    };

    KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig());

    bool errCbTriggered = false;

    // The error would be triggered while consumer tries to subscribe a non-existed topic.
    consumer.setErrorCallback([&errCbTriggered](const Error& error) {
                                 errCbTriggered = true;
                                 KafkaTestUtility::DumpError(error);
                                 EXPECT_EQ(RD_KAFKA_RESP_ERR_UNKNOWN_TOPIC_OR_PART, error.value());
                              });

    // The topic would be created after 5 seconds
    KafkaTestUtility::JoiningThread consumer1Thread(createTopicAfterSeconds, 5);

    std::cout << "[" << Utility::getCurrentTime() << "] Consumer will subscribe" << std::endl;
    EXPECT_KAFKA_NO_THROW(consumer.subscribe({topic}));
    std::cout << "[" << Utility::getCurrentTime() << "] Consumer just subscribed" << std::endl;

    EXPECT_TRUE(errCbTriggered);
    EXPECT_FALSE(consumer.assignment().empty());
}

TEST(KafkaAutoCommitConsumer, CooperativeRebalance)
{
    constexpr int NUM_TOPICS     = 3;
    constexpr int NUM_PARTITIONS = 5;

    const std::string topicPrefix  = Utility::getRandomString();
    for (int i = 0; i < NUM_TOPICS; i++)
    {
        Topic topic = topicPrefix + std::to_string(i);
        KafkaTestUtility::CreateKafkaTopic(topic, NUM_PARTITIONS, 1);
    }

    const std::string groupId = Utility::getRandomString();
    const std::string topicPattern = "^" + topicPrefix + "\\.*";
    auto startConsumer = [groupId, topicPattern](const std::string& clientId, int runningSec) {
        TopicPartitions partitionsJustRevoked;
        auto rebalanceCb = [clientId, &partitionsJustRevoked](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
            if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                std::cout << "[" << Utility::getCurrentTime() << "] " << clientId << " assigned partitions: " << toString(tps) << std::endl;
                EXPECT_TRUE(std::none_of(tps.cbegin(), tps.cend(), [&partitionsJustRevoked](const auto& tp) { return partitionsJustRevoked.count(tp); }));
            } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                std::cout << "[" << Utility::getCurrentTime() << "] " << clientId << " unassigned partitions: " << toString(tps) << std::endl;
                partitionsJustRevoked = tps;
            }
        };

        Properties props = KafkaTestUtility::GetKafkaClientCommonConfig()
                             .put(ConsumerConfig::CLIENT_ID, clientId)
                             .put(ConsumerConfig::GROUP_ID,  groupId)
                             .put(ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY, "cooperative-sticky");

        KafkaTestUtility::PrintDividingLine(clientId + " is starting");

        KafkaAutoCommitConsumer consumer(props);

        consumer.subscribe({topicPattern}, rebalanceCb);

        for (int i = 0; i < runningSec; ++i) {
            consumer.poll(std::chrono::seconds(1));
        }

        KafkaTestUtility::PrintDividingLine(clientId + " is quitting");
    };

    KafkaTestUtility::JoiningThread consumer1Thread(startConsumer, "consumer1", 10);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    KafkaTestUtility::JoiningThread consumer2Thread(startConsumer, "consumer2", 10);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    KafkaTestUtility::JoiningThread consumer3Thread(startConsumer, "consumer3", 10);

    std::this_thread::sleep_for(std::chrono::seconds(5));

    KafkaTestUtility::JoiningThread consumer4Thread(startConsumer, "consumer4", 10);
}

TEST(KafkaAutoCommitConsumer, FetchBrokerMetadataTriggersRejoin)
{
    const std::string topicPrefix  = Utility::getRandomString();
    const std::string topicPattern = "^" + topicPrefix + "\\.*";

    Topic topic1 = topicPrefix + "_1";
    Topic topic2 = topicPrefix + "_2";

    KafkaTestUtility::CreateKafkaTopic(topic1, 1, 1);

    auto rebalanceCb = [](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
        if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
            std::cout << "[" << Utility::getCurrentTime() << "] newly assigned partitions: " << toString(tps) << std::endl;
        } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
            std::cout << "[" << Utility::getCurrentTime() << "] newly unassigned partitions: " << toString(tps) << std::endl;
        }
    };

    Properties props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY, "cooperative-sticky");

    KafkaAutoCommitConsumer consumer(props);

    // Subscribe to the topic pattern
    consumer.subscribe({topicPattern}, rebalanceCb);

    consumer.poll(std::chrono::seconds(1));

    // Create one more topic (with the same subscription pattern)
    KafkaTestUtility::CreateKafkaTopic(topic2, 1, 1);

    // Should be able to get the metadata for the new topic
    // Note: here the Metadata response information would trigger a re-join as well
    auto metadata2 = consumer.fetchBrokerMetadata(topic2);
    ASSERT_TRUE(metadata2);
    std::cout << "[" << Utility::getCurrentTime() << "] brokerMetadata for topic[" << topic2 << "]: " << metadata2->toString() << std::endl;

    consumer.poll(std::chrono::seconds(1));

    auto assignment = consumer.assignment();
    std::cout << "[" << Utility::getCurrentTime() << "] assignment: " << toString(assignment) << std::endl;

    // The newly created topic-partitions should be within the assignment as well
    EXPECT_EQ(1, assignment.count({topic2, 0}));
}

TEST(KafkaAutoCommitConsumer, SubscribeNotConflictWithStatsEvent)
{
    const Topic topic1 = Utility::getRandomString();
    const Topic topic2 = Utility::getRandomString();
    const Topic topic3 = Utility::getRandomString();

    // Prepare topics
    KafkaTestUtility::CreateKafkaTopic(topic1, 1, 1);
    KafkaTestUtility::CreateKafkaTopic(topic2, 1, 1);
    KafkaTestUtility::CreateKafkaTopic(topic3, 1, 1);

    auto testNormalOperations = [topic1, topic2, topic3](const Properties& props) {
        KafkaTestUtility::PrintDividingLine("[Normal operations] Test with consumer properties[" + props.toString() + "]");

        KafkaAutoCommitConsumer consumer(props);

        // Subscribe topics
        Topics topicsToSubscribe = {topic1, topic2};
        KafkaTestUtility::PrintDividingLine("Subscribe to [" + toString(topicsToSubscribe) + "]");
        consumer.subscribe(topicsToSubscribe);
        EXPECT_EQ((TopicPartitions{{topic1, 0}, {topic2, 0}}), consumer.assignment());

        KafkaTestUtility::PrintDividingLine("Unsubscribe");
        consumer.unsubscribe();
        EXPECT_TRUE(consumer.assignment().empty());

        TopicPartitions topicPartitionsToAssign = {{topic2, 0}, {topic3, 0}};
        KafkaTestUtility::PrintDividingLine("Assign [" + toString(topicPartitionsToAssign) + "]");
        consumer.assign(topicPartitionsToAssign);
        EXPECT_EQ(topicPartitionsToAssign, consumer.assignment());

        KafkaTestUtility::PrintDividingLine("Unsubscribe");
        consumer.unsubscribe();
        EXPECT_TRUE(consumer.assignment().empty());

        topicPartitionsToAssign = {{topic1, 0}};
        KafkaTestUtility::PrintDividingLine("Assign [" + toString(topicPartitionsToAssign) + "]");
        consumer.assign(topicPartitionsToAssign);
        EXPECT_EQ(topicPartitionsToAssign, consumer.assignment());

        KafkaTestUtility::PrintDividingLine("Unsubscribe");
        consumer.unsubscribe();
        EXPECT_TRUE(consumer.assignment().empty());

        topicsToSubscribe = {topic3};
        KafkaTestUtility::PrintDividingLine("Subscribe to [" + toString(topicsToSubscribe) + "]");
        consumer.subscribe(topicsToSubscribe);
        EXPECT_EQ((TopicPartitions{{topic3, 0}}), consumer.assignment());

        KafkaTestUtility::PrintDividingLine("END");
    };

    auto testDuplicatedOperations = [topic1, topic2, topic3](const Properties& props) {
        KafkaTestUtility::PrintDividingLine("[Duplicated operations] Test with consumer properties[" + props.toString() + "]");

        KafkaAutoCommitConsumer consumer(props);

        // Rebalance callback
        auto rebalanceCb = [](Consumer::RebalanceEventType et, const TopicPartitions& tps) {
                               if (et == Consumer::RebalanceEventType::PartitionsAssigned) {
                                   std::cout << "[" << Utility::getCurrentTime() << "] PartitionsAssigned: " << toString(tps) << std::endl;
                               } else if (et == Consumer::RebalanceEventType::PartitionsRevoked) {
                                   std::cout << "[" << Utility::getCurrentTime() << "] PartitionsRevoked: " << toString(tps) << std::endl;
                               }
                           };

        // Subscribe topics
        Topics topicsToSubscribe = {topic1};
        KafkaTestUtility::PrintDividingLine("Subscribe to [" + toString(topicsToSubscribe) + "]");
        consumer.subscribe(topicsToSubscribe, rebalanceCb);
        EXPECT_EQ((TopicPartitions{{topic1, 0}}), consumer.assignment());

        KafkaTestUtility::PrintDividingLine("Subscribe to [" + toString(topicsToSubscribe) + "], again");
        consumer.subscribe(topicsToSubscribe, rebalanceCb);
        EXPECT_EQ((TopicPartitions{{topic1, 0}}), consumer.assignment());

        topicsToSubscribe = {topic2, topic3};
        KafkaTestUtility::PrintDividingLine("Subscribe to total different topics[" + toString(topicsToSubscribe) + "]");
        consumer.subscribe(topicsToSubscribe, rebalanceCb);
        EXPECT_EQ((TopicPartitions{{topic2, 0}, {topic3, 0}}), consumer.assignment());

        topicsToSubscribe = {topic3};
        KafkaTestUtility::PrintDividingLine("Subscribe to topics[" + toString(topicsToSubscribe) + "], less then before");
        consumer.subscribe(topicsToSubscribe, rebalanceCb);
        EXPECT_EQ((TopicPartitions{{topic3, 0}}), consumer.assignment());

        KafkaTestUtility::PrintDividingLine("Unsubscribe");
        consumer.unsubscribe();
        EXPECT_TRUE(consumer.assignment().empty());

        KafkaTestUtility::PrintDividingLine("Unsubscribe, again");
        consumer.unsubscribe();
        EXPECT_TRUE(consumer.assignment().empty());

        KafkaTestUtility::PrintDividingLine("END");
    };

    // Prepare the properties
    auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
    props.put("log_level", "6");

    testDuplicatedOperations(props);
    testNormalOperations(props);

    // Enable statistics event (5 ms)
    props.put("statistics.interval.ms", "5");
    testDuplicatedOperations(props);
    testNormalOperations(props);

    // Try with incremental partitions assignment
    props.put(ConsumerConfig::PARTITION_ASSIGNMENT_STRATEGY, "cooperative-sticky");
    testDuplicatedOperations(props);
    testNormalOperations(props);
}

