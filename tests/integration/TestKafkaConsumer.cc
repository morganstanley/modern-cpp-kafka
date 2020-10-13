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
using namespace KafkaTestUtility;


TEST(KafkaAutoCommitConsumer, BasicPoll)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    // The auto-commit consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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
    auto records = ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
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
    ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = ConsumeMessagesUntilTimeout(consumer);
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

    // The auto-commit consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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
    auto records = ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
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
    ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());

    // Check mesages
    std::size_t rcvMsgCount = 0;
    for (auto& record: records)
    {
        ASSERT_TRUE(rcvMsgCount < messages.size());

        EXPECT_EQ(topic, record.topic());
        EXPECT_EQ(partition, record.partition());
        EXPECT_EQ(std::get<1>(messages[rcvMsgCount]).size(), record.key().size());
        EXPECT_EQ(0, std::memcmp(std::get<1>(messages[rcvMsgCount]).c_str(), record.key().data(), record.key().size()));
        EXPECT_EQ(std::get<2>(messages[rcvMsgCount]).size(), record.value().size());
        EXPECT_EQ(0, std::memcmp(std::get<2>(messages[rcvMsgCount]).c_str(), record.value().data(), record.value().size()));

        Headers headersInRecord = record.headers();
        const Headers& expectedHeader = std::get<0>(messages[rcvMsgCount]);
        ASSERT_EQ(expectedHeader.size(), headersInRecord.size());
        for (std::size_t i = 0; i < expectedHeader.size(); ++i)
        {
            EXPECT_EQ(expectedHeader[i].key, headersInRecord[i].key);
            ASSERT_EQ(expectedHeader[i].value.size(), headersInRecord[i].value.size());
            EXPECT_EQ(0, std::memcmp(expectedHeader[i].value.data(), headersInRecord[i].value.data(), expectedHeader[i].value.size()));
        }

        // Here only check the first message, which has the headers
        if (!headersInRecord.empty())
        {
            // Get value from headers, for "k1"
            auto value = record.lastHeaderValue("k1");
            // The last header value for "k1", should be "v3", instead of "v1"
            ASSERT_EQ(sizeof(int), value.size());
            EXPECT_EQ(v3, *static_cast<const int*>(value.data()));

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

    // The auto-commit consumer
    const auto props = getKafkaClientCommonConfig()
                       .put(ConsumerConfig::MAX_POLL_RECORDS,  "1")         // Only poll 1 message each time
                       .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"); // Seek to the earliest offset at the beginning

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
    auto records = ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));
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
    ProduceMessages(topic, partition, messages);

    // Poll these messages
    records = ConsumeMessagesUntilTimeout(consumer);
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
    records = ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Seed to the end
    consumer.seekToEnd();
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " seeked to end" << std::endl;

    // Poll messages again (would get nothing)
    records = ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(0, records.size());

    // Seed to the beginning
    consumer.seekToBeginning();
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " seeked to beginning" << std::endl;

    // Poll messages again (would get all these messages)
    records = ConsumeMessagesUntilTimeout(consumer);
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

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    {
        const auto props = getKafkaClientCommonConfig().put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"); // Seek to the earliest offset at the beginning

        KafkaManualCommitConsumer consumer(props);

        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

        // Subscribe topics, and seek to the beginning
        consumer.subscribe({topic});
        EXPECT_FALSE(consumer.subscription().empty());

        // Poll messages and commit the offsets
        for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
        {
            auto records = consumer.poll(POLL_INTERVAL);
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

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    const auto props = getKafkaClientCommonConfig()
                       .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest") // Seek to the earliest offset at the beginning
                       .put(ConsumerConfig::MAX_POLL_RECORDS,  "1");       // Only poll 1 message each time

    KafkaManualCommitConsumer consumer(props);

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics, and seek to the beginning
    consumer.subscribe({topic});
    EXPECT_FALSE(consumer.subscription().empty());

    std::atomic<std::size_t> commitCbCount = {0};

    // Poll messages and commit the offsets
    for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
    {
        auto records = consumer.poll(POLL_INTERVAL);
        numMsgPolled += records.size();

        if (records.empty()) continue;

        EXPECT_EQ(1, records.size());
        auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
        consumer.commitAsync(records[0],
                             [expected, &commitCbCount](const TopicPartitionOffsets& tpos, std::error_code ec) {
                                 std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << ec.message()<< "]" << std::endl;
                                 EXPECT_EQ(1, tpos.size());
                                 EXPECT_EQ(std::get<2>(expected), tpos.at(TopicPartition{std::get<0>(expected), std::get<1>(expected)}));

                                 ++commitCbCount;
                             });
    }


    WaitUntilTimeout([&commitCbCount, expectedCnt = messages.size()](){ return expectedCnt == commitCbCount; },
                     MAX_OFFSET_COMMIT_TIMEOUT);

    EXPECT_EQ(messages.size(), commitCbCount);

    consumer.close();
    std::cout << "[" << Utility::getCurrentTime() << "] Consumer closed" << std::endl;
}

TEST(KafkaManualCommitConsumer, OffsetCommitCallbackTriggeredBeforeClose)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    ProduceMessages(topic, partition, messages);

    std::size_t commitCbCount = 0;

    // The manual-commit consumer
    {
        const auto props = getKafkaClientCommonConfig()
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
            auto records = consumer.poll(POLL_INTERVAL);
            numMsgPolled += records.size();

            if (records.empty()) continue;

            EXPECT_EQ(1, records.size());
            auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
            consumer.commitAsync(records[0],
                                 [expected, &commitCbCount](const TopicPartitionOffsets& tpos, std::error_code ec) {
                                     std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << ec.message()<< "]" << std::endl;
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

    // Prepare some messages to send
    const std::vector<std::tuple<Headers, std::string, std::string>> messages = {
        {Headers{}, "key1", "value1"},
        {Headers{}, "key2", "value2"},
        {Headers{}, "key3", "value3"},
    };

    // Send the messages
    ProduceMessages(topic, partition, messages);

    // The manual-commit consumer
    const auto props = getKafkaClientCommonConfig()
                       .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest") // Seek to the earliest offset at the beginning
                       .put(ConsumerConfig::MAX_POLL_RECORDS,  "1");       // Only poll 1 message each time

    KafkaManualCommitConsumer consumer(props, KafkaClient::EventsPollingOption::Manual);

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics, and seek to the beginning
    consumer.subscribe({topic});
    EXPECT_FALSE(consumer.subscription().empty());

    std::size_t commitCbCount = 0;

    // Poll messages and commit the offsets
    for (std::size_t numMsgPolled = 0; numMsgPolled < messages.size(); )
    {
        auto records = consumer.poll(POLL_INTERVAL);
        numMsgPolled += records.size();

        if (records.empty()) continue;

        EXPECT_EQ(1, records.size());
        auto expected = std::make_tuple(records[0].topic(), records[0].partition(), records[0].offset() + 1);
        consumer.commitAsync(records[0],
                             [expected, &commitCbCount](const TopicPartitionOffsets& tpos, std::error_code ec) {
                                 std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << toString(tpos) << "], result[" << ec.message()<< "]" << std::endl;
                                 EXPECT_EQ(1, tpos.size());
                                 EXPECT_EQ(std::get<2>(expected), tpos.at(TopicPartition{std::get<0>(expected), std::get<1>(expected)}));

                                 ++commitCbCount;
                             });
    }

    // Wait for the offset-commit callback (to be triggered)
    const auto end = std::chrono::steady_clock::now() + MAX_OFFSET_COMMIT_TIMEOUT;
    do
    {
        // keep polling for the offset commit callbacks
        consumer.pollEvents(POLL_INTERVAL);
    } while (std::chrono::steady_clock::now() < end && commitCbCount != messages.size());

    EXPECT_EQ(messages.size(), commitCbCount);
}

TEST(KafkaManualCommitConsumer, OffsetCommitAndPosition)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;

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
    ProduceMessages(topic, partition, messages);

    // To save the configuration (including the client.id/group.id)
    Properties savedProps;

    std::size_t startCount = 0;

    // Start consumer a few times, but only commit the offset for the first message each time
    {
        auto props = getKafkaClientCommonConfig().put(ConsumerConfig::MAX_POLL_RECORDS, "1"); // Only poll 1 message each time

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
            auto records = ConsumeMessagesUntilTimeout(consumer);
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
                                         [expectedOffset = record.offset() + 1, topic, partition, &commitCbCount](const TopicPartitionOffsets& tpos, std::error_code ec) {
                                             std::cout << "[" << Utility::getCurrentTime() << "] offset commit callback for offset[" << expectedOffset << "], got result[" << ec.message() << "], tpos[" << toString(tpos) << "]" << std::endl;
                                             ASSERT_FALSE(ec);
                                             EXPECT_EQ(expectedOffset, tpos.at({topic, partition}));
                                             ++commitCbCount;
                                         });
                }
            }
        }

        // Wait for the offset-commit callback (to be triggered)
        WaitUntilTimeout([&commitCbCount]() {return commitCbCount == 1; }, MAX_OFFSET_COMMIT_TIMEOUT);
        EXPECT_EQ(1, commitCbCount);
        EXPECT_EQ(messages.size() - startCount, rcvMsgCount);
    }

    ++startCount;

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
            auto records = ConsumeMessagesUntilTimeout(consumer);
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
                    consumer.commitSync(record);
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
            auto records = ConsumeMessagesUntilTimeout(consumer);
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
                    consumer.commitSync(tpos);
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
            auto records = ConsumeMessagesUntilTimeout(consumer);
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
        }

        std::cout << "[" << Utility::getCurrentTime() << "] will commit for all polled messages" << std::endl;
        consumer.commitSync();

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
        auto records = ConsumeMessagesUntilTimeout(consumer);
        EXPECT_TRUE(records.empty());
    }
}

TEST(KafkaAutoCommitConsumer, OffsetCommitAndPosition)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    std::cout << "[" << Utility::getCurrentTime() << "] Topic[" << topic << "] would be used" << std::endl;
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
    ProduceMessages(topic, partition, messages);

    // To save the configuration (including the client.id/group.id)
    Properties savedProps;

    constexpr int maxRecordsPolledAtFirst = 2;

    // Consumer will poll twice, -- Note, the last polled message offset would not be committed (no following `poll`)
    {
        const auto props = getKafkaClientCommonConfig().put(ConsumerConfig::MAX_POLL_RECORDS, "1");

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
            auto polled = consumer.poll(POLL_INTERVAL);
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

        auto records = ConsumeMessagesUntilTimeout(consumer);
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

    // Prepare the consumer
    const auto props = getKafkaClientCommonConfig().put(ConsumerConfig::GROUP_ID, group);

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
                              auto consumerProps = getKafkaClientCommonConfig()
                                                   .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                                   .put(ConsumerConfig::GROUP_ID,          group);
                              KafkaAutoCommitConsumer anotherConsumer(consumerProps);
                              anotherConsumer.subscribe({topic});
                              ConsumeMessagesUntilTimeout(anotherConsumer);
                          });

    // Keep polling, in order to trigger any callback
    const auto KEEP_POLLING_TIMEOUT = std::chrono::seconds(30);
    const auto end = std::chrono::steady_clock::now() + KEEP_POLLING_TIMEOUT;
    do
    {
        consumer.poll(POLL_INTERVAL);
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
    ProduceMessages(topic, partition, messages);

    // Keep polling
    auto records = ConsumeMessagesUntilTimeout(consumer);
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
        KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
        std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;
        std::cout << "[" << Utility::getCurrentTime() << "] librdkafka thread cnt[" << Utility::getLibRdKafkaThreadCount() << "]" << std::endl;

        // Just wait a short while, thus make sure all background threads be started
        std::this_thread::sleep_for(std::chrono::seconds(1));

        EXPECT_EQ(getNumberOfKafkaBrokers() + 3, Utility::getLibRdKafkaThreadCount());
    }

    EXPECT_EQ(0, Utility::getLibRdKafkaThreadCount());
}

TEST(KafkaAutoCommitConsumer, PartitionAssignment)
{
    const Topic     topic      = Utility::getRandomString();
    const Partition partition1 = 0;
    const Partition partition2 = 0;

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe(topics);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscribed" << std::endl;
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscription[" << toString(consumer.subscription()) << "]" << std::endl;

    EXPECT_EQ(topics.size(), consumer.subscription().size());
    std::for_each(topics.cbegin(), topics.cend(), [&consumer](const auto& topic) { EXPECT_EQ(1, consumer.subscription().count(topic)); });
    EXPECT_EQ(0, consumer.assignment().size());
}

TEST(KafkaAutoCommitConsumer, SubscribeUnsubscribeThenAssign)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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

    EXPECT_EQ(1, consumer.subscription().size());
    EXPECT_EQ(1, consumer.subscription().count(topic));
    EXPECT_EQ(0, consumer.assignment().size());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " subscription[" << toString(consumer.subscription()) << "]" << std::endl;
}

TEST(KafkaAutoCommitConsumer, WrongOperation_SeekBeforePartitionsAssigned)
{
    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " would seekToBeginning" << std::endl;

    // Seek with an unassigned partition -- would throw exception
    EXPECT_KAFKA_THROW(consumer.seek({"unassigned_topic", 0}, 0), RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION);
}

TEST(KafkaAutoCommitConsumer, WrongOperation_SubscribeThenAssign)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Assign topic-partitions
    TopicPartitions tps{{topic, partition}};
    consumer.assign(tps);

    EXPECT_EQ(tps, consumer.assignment());

    // Subscribe topics -- would throw exception
    EXPECT_KAFKA_THROW(consumer.subscribe({topic}), RD_KAFKA_RESP_ERR__FAIL);
}

TEST(KafkaClient, GetBrokerMetadata)
{
    const Topic topic = Utility::getRandomString();

    // Start consumer
    KafkaAutoCommitConsumer consumer(getKafkaClientCommonConfig());
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

    const auto props = getKafkaClientCommonConfig().put(ConsumerConfig::ENABLE_PARTITION_EOF, "true");

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
    auto records = ConsumeMessagesUntilTimeout(consumer);

    // Count the EOF
    EXPECT_EQ(assignedPartitions.size(),
              std::count_if(records.cbegin(), records.cend(),
                            [](const auto& record) { return record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF; }));

    // Close the consumer
    consumer.close();
}

TEST(KafkaAutoCommitConsumer, PauseAndResume)
{
    const Topic     topic1    = Utility::getRandomString();
    const Topic     topic2    = Utility::getRandomString();
    const Partition partition = 0;

    // constuct a producer for sending messages
    KafkaSyncProducer producer(getKafkaClientCommonConfig());

    // send 3 messages towards topic1
    std::string msg = "msg1";
    producer.send(ProducerRecord(topic1, partition, Key(), Value(msg.c_str(), msg.size())));
    msg = "msg2";
    producer.send(ProducerRecord(topic1, partition, Key(), Value(msg.c_str(), msg.size())));
    msg = "msg3";
    producer.send(ProducerRecord(topic1, partition, Key(), Value(msg.c_str(), msg.size())));

    // An auto-commit Consumer
    const auto props = getKafkaClientCommonConfig()
                       .put(ConsumerConfig::SESSION_TIMEOUT_MS,   "30000")
                       .put(ConsumerConfig::AUTO_OFFSET_RESET,    "earliest")
                       .put(ConsumerConfig::MAX_POLL_RECORDS,     "1");
    KafkaAutoCommitConsumer consumer(props);
    std::cout << "[" << Utility::getCurrentTime() << "] " << consumer.name() << " started" << std::endl;

    // Subscribe topics
    consumer.subscribe({topic1, topic2});

    std::cout << "--------------------" << std::endl;

    // Poll 1 messaged from topic1
    auto records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    std::cout << records[0].toString() << std::endl;

    std::cout << "--------------------" << std::endl;

    // Pause, then could not poll any message
    consumer.pause();
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    std::cout << "--------------------" << std::endl;

    // Resume, would be able to poll again
    consumer.resume();
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    std::cout << records[0].toString() << std::endl;

    std::cout << "--------------------" << std::endl;

    // Try to pause an invalid partition
    EXPECT_KAFKA_THROW(consumer.pause({TopicPartition("invalid_topic", 12345)}), RD_KAFKA_RESP_ERR__INVALID_ARG);

    std::cout << "--------------------" << std::endl;

    // Pause partition of topic1
    consumer.pause({TopicPartition(topic1, partition)});
    // No message could be polled from topic1
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    std::cout << "--------------------" << std::endl;

    msg = "msg4";
    producer.send(ProducerRecord(topic2, partition, Key(), Value(msg.c_str(), msg.size())));
    // Could still poll from topic2
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    std::cout << records[0].toString() << std::endl;

    // No more message, only 1 message from topic2
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    EXPECT_EQ(0, records.size());

    std::cout << "--------------------" << std::endl;

    // Resume partition of topic1, then could poll message again
    consumer.resume({TopicPartition(topic1, partition)});
    records = consumer.poll(MAX_POLL_MESSAGES_TIMEOUT);
    ASSERT_EQ(1, records.size());
    std::cout << records[0].toString() << std::endl;

    consumer.close();
}

