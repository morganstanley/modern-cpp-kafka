#include "../utils/TestUtility.h"

#include "kafka/AdminClient.h"
#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <boost/algorithm/string.hpp>

using namespace KAFKA_API;


TEST(KafkaProducer, SendMessagesWithAcks1)
{
    // Prepare messages to test
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Properties for the producer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ProducerConfig::ACKS, "1");

    // Sync-send producer
    KafkaProducer producer(props);

    // Send messages
    for (const auto& msg: messages)
    {
        auto record = ProducerRecord(topic, partition, Key(msg.first.c_str(), msg.first.size()), Value(msg.second.c_str(), msg.second.size()));
        std::cout << "[" << Utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;
        auto metadata = producer.syncSend(record);
        std::cout << "[" << Utility::getCurrentTime() << "] Producer::Metadata: " << metadata.toString() << std::endl;
    }

    // Prepare a consumer
    Kafka::KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"));
    consumer.setLogLevel(Kafka::Log::Level::Crit);
    consumer.subscribe({topic});

    // Poll these messages
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

    // Check the messages
    EXPECT_EQ(messages.size(), records.size());
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        EXPECT_EQ(messages[i].first,  std::string(static_cast<const char*>(records[i].key().data()), records[i].key().size()));
        EXPECT_EQ(messages[i].second, std::string(static_cast<const char*>(records[i].value().data()), records[i].value().size()));
    }
}

TEST(KafkaProducer, SendMessagesWithAcksAll)
{
    // Prepare messages to test
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Properties for the producer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(ProducerConfig::ACKS, "all");

    // Async-send producer
    KafkaProducer producer(props);

    // Send messages
    for (const auto& msg: messages)
    {
        auto record = ProducerRecord(topic, partition, Key(msg.first.c_str(), msg.first.size()), Value(msg.second.c_str(), msg.second.size()));
        std::cout << "[" << Utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;
        auto metadata = producer.syncSend(record);
        std::cout << "[" << Utility::getCurrentTime() << "] Producer::Metadata: " << metadata.toString() << std::endl;
    }

    // Prepare a consumer
    const auto consumerProps = KafkaTestUtility::GetKafkaClientCommonConfig().put(Kafka::ConsumerConfig::AUTO_OFFSET_RESET, "earliest");
    Kafka::KafkaAutoCommitConsumer consumer(consumerProps);
    consumer.setLogLevel(Kafka::Log::Level::Crit);
    consumer.subscribe({topic});

    // Poll these messages
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

    // Check the messages
    EXPECT_EQ(messages.size(), records.size());
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        EXPECT_EQ(messages[i].first,  std::string(static_cast<const char*>(records[i].key().data()), records[i].key().size()));
        EXPECT_EQ(messages[i].second, std::string(static_cast<const char*>(records[i].value().data()), records[i].value().size()));
    }
}

TEST(KafkaProducer, FailToSendMessagesWithAcksAll)
{
    // Prepare messages to test
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    // Create a topic (replication factor is 1) with AdminClient
    const Topic topic             = Utility::getRandomString();
    const int   numPartitions     = 5;
    const int   replicationFactor = 1;
    KafkaTestUtility::CreateKafkaTopic(topic, numPartitions, replicationFactor);

    // Properties for the producer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                        .put(ProducerConfig::ACKS,               "all")
                        .put(ProducerConfig::MESSAGE_TIMEOUT_MS, "5000"); // To shorten the test

    // Async-send producer
    KafkaProducer producer(props);

    if (auto brokerMetadata = producer.fetchBrokerMetadata(topic))
    {
        std::cout << brokerMetadata->toString() << std::endl;
    }

    // Send messages
    for (const auto& msg: messages)
    {
        auto record = ProducerRecord(topic, Key(msg.first.c_str(), msg.first.size()), Value(msg.second.c_str(), msg.second.size()));
        std::cout << "[" << Utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;
        // Since "no in-sync replica" for the topic, it would keep trying
        EXPECT_KAFKA_THROW(producer.syncSend(record), RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);
    }
}

TEST(KafkaProducer, InSyncBrokersAckTimeout)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    const auto key    = std::string(100000, 'a');
    const auto value  = std::string(100000, 'a');
    const auto record = ProducerRecord(topic, partition, Key(key.c_str(), key.size()), Value(value.c_str(), value.size()));

    {
        const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                           .put(ProducerConfig::ACKS,               "all")
                           .put(ProducerConfig::MESSAGE_TIMEOUT_MS, "1000")
                           .put(ProducerConfig::REQUEST_TIMEOUT_MS, "1"); // Here it's a short value, more likely to trigger the timeout

        KafkaProducer producer(props);

        constexpr int MAX_RETRIES = 100;
        for (int i = 0; i < MAX_RETRIES; ++i)
        {
            try
            {
                auto metadata = producer.syncSend(record);
                // will retry, to see if timeout could occure next time
            }
            catch (const KafkaException& e)
            {
                std::cout << "[" << Utility::getCurrentTime() << "] Exception caught: " << e.what() << std::endl;
                EXPECT_EQ(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT, e.error().value());
                break;
            }
        }
    }
}

TEST(KafkaProducer, DefaultPartitioner)
{
    KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());

    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    std::map<Partition, int> partitionCounts;
    static constexpr int MSG_NUM = 20;
    for (int i = 0; i < MSG_NUM; ++i)
    {
        std::string key   = "k" + std::to_string(i);
        std::string value = "v" + std::to_string(i);

        auto record = ProducerRecord(topic, Key(key.c_str(), key.size()), Value(value.c_str(), value.size()));

        auto metadata = producer.syncSend(record);

        partitionCounts[metadata.partition()]++;
    }

    // Not all be sent to the same paritition
    for (const auto& count: partitionCounts) EXPECT_NE(MSG_NUM, count.second);
}

TEST(KafkaProducer, TryOtherPartitioners)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Try another "partitioner" instead of the default one
    {
        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        // Partitioner "murmur2": if with no available key, all these records would be partitioned to the same partition
        props.put(ProducerConfig::PARTITIONER, "murmur2");

        KafkaProducer producer(props);

        std::map<Partition, int> partitionCounts;
        static constexpr int MSG_NUM = 20;
        for (int i = 0; i < MSG_NUM; ++i)
        {
            std::string key;
            std::string value = "v" + std::to_string(i);

            auto record = ProducerRecord(topic, Key(key.c_str(), key.size()), Value(value.c_str(), value.size()));

            auto metadata = producer.syncSend(record);
            std::cout << metadata.toString() << std::endl;

            partitionCounts[metadata.partition()]++;
        }

        // All were hashed to the same paritition
        EXPECT_EQ(1, partitionCounts.size());
        EXPECT_TRUE(std::all_of(partitionCounts.cbegin(), partitionCounts.cend(), [](const auto& count) {return count.second == MSG_NUM; }));
    }

    {
        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        // An invalid partitioner
        props.put(ProducerConfig::PARTITIONER, "invalid");

        // An exception would be thrown for invalid "partitioner" setting
        EXPECT_KAFKA_THROW(KafkaProducer producer(props), RD_KAFKA_RESP_ERR__INVALID_ARG);
    }
}

TEST(KafkaProducer, RecordWithEmptyOrNullFields)
{
    auto sendMessages = [](const Kafka::ProducerRecord& record, std::size_t repeat, const std::string& partitioner) {
        Kafka::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                          .put(ProducerConfig::PARTITIONER, partitioner));
        producer.setLogLevel(Kafka::Log::Level::Crit);
        for (std::size_t i = 0; i < repeat; ++i) {
            producer.syncSend(record);
        }
    };

    enum class FieldType { Empty, Null };
    auto runTest = [sendMessages](FieldType fieldType, const std::string& partitioner, bool expectRandomlyPartitioned) {
        KafkaTestUtility::PrintDividingLine("Run test for partitioner[" + partitioner + "], with " + (fieldType == FieldType::Empty ?  "EmptyField" : "NullField"));

        const Topic topic = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

        const std::string emptyStr{};
        const auto emptyField = ConstBuffer(emptyStr.c_str(), emptyStr.size());
        auto producerRecord =  (fieldType == FieldType::Empty ?
                                    Kafka::ProducerRecord(topic, emptyField, emptyField) : Kafka::ProducerRecord(topic, Kafka::NullKey, Kafka::NullValue));

        sendMessages(producerRecord, 10, partitioner);

        // The auto-commit consumer
        KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                         .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"));
        // Subscribe topics
        consumer.subscribe({topic});

        // Poll all messages
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        // Check the key/value (empty or null)
        std::map<int, int> counts;
        for (const auto& record: records) {
            if (fieldType == FieldType::Empty) {
                EXPECT_TRUE(record.key().size()   == 0);
                EXPECT_TRUE(record.value().size() == 0);
                EXPECT_TRUE(record.key().data()   != nullptr);
                EXPECT_TRUE(record.value().data() != nullptr);
            } else {
                EXPECT_TRUE(record.key().size()   == 0);
                EXPECT_TRUE(record.value().size() == 0);
                EXPECT_TRUE(record.key().data()   == nullptr);
                EXPECT_TRUE(record.value().data() == nullptr);
            }

            counts[record.partition()] += 1;
        }
        // Should be hashed to the same partition?
        if (expectRandomlyPartitioned) {
            EXPECT_TRUE(counts.size() > 1);
        } else {
            EXPECT_TRUE(counts.size() == 1);
        }
    };

    runTest(FieldType::Null,  "consistent_random", true);
    runTest(FieldType::Empty, "consistent_random", true);

    runTest(FieldType::Null,  "murmur2_random",    true);
    runTest(FieldType::Empty, "murmur2_random",    false); // empty keys are mapped to a single partition

    runTest(FieldType::Null,  "fnv1a_random",      true);
    runTest(FieldType::Empty, "fnv1a_random",      false); // empty keys are mapped to a single partition

    runTest(FieldType::Null,  "consistent",        false);
    runTest(FieldType::Empty, "consistent",        false);
}

TEST(KafkaProducer, ThreadCount)
{
    {
        KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        std::cout << "[" << Utility::getCurrentTime() << "] " << producer.name() << " started" << std::endl;
        std::cout << "[" << Utility::getCurrentTime() << "] librdkafka thread cnt[" << Utility::getLibRdKafkaThreadCount() << "]" << std::endl;

        // Just wait a short while, thus make sure all background threads be started
        std::this_thread::sleep_for(std::chrono::seconds(1));

        EXPECT_EQ(KafkaTestUtility::GetNumberOfKafkaBrokers() + 2, Utility::getLibRdKafkaThreadCount());
    }

    EXPECT_EQ(0, Utility::getLibRdKafkaThreadCount());
}

TEST(KafkaProducer, MessageDeliveryCallback)
{
    // Prepare messages to test
    const std::vector<std::tuple<std::string, std::string, int>> messages = {
        {"key1", "value1", 1},
        {"key2", "value2", 2},
        {"key3", "value3", 3},
    };

    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Properties for the producer
    std::set<ProducerRecord::Id> msgIdsSent;

    // Delivery callback
    Producer::Callback drCallback =
        [&msgIdsSent, topic, partition](const Producer::RecordMetadata& metadata, const Error& error) {
            std::cout << "[" << Utility::getCurrentTime() << "] Producer::Metadata: " << metadata.toString() << std::endl;
            EXPECT_FALSE(error);
            EXPECT_EQ(topic, metadata.topic());
            EXPECT_EQ(partition, metadata.partition());
            msgIdsSent.emplace(*metadata.recordId());
        };

    // The producer would close anyway
    {
        KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        for (const auto& msg: messages)
        {
            auto record = ProducerRecord(topic, partition,
                                         Key(std::get<0>(msg).c_str(), std::get<0>(msg).size()),
                                         Value(std::get<1>(msg).c_str(), std::get<1>(msg).size()),
                                         std::get<2>(msg));
            std::cout << "[" << Utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;
            producer.send(record, drCallback);
        }
    }

    // Make sure all delivery callbacks be called
    EXPECT_EQ(messages.size(), msgIdsSent.size());
    for (const auto& msg: messages)
    {
        auto id = std::get<2>(msg);
        EXPECT_NE(msgIdsSent.end(), msgIdsSent.find(id));
    }
}

TEST(KafkaProducer, DeliveryCallback_ManuallyPollEvents)
{
    // Prepare messages to test
    const std::vector<std::tuple<std::string, std::string, int>> messages = {
        {"key1", "value1", 1},
        {"key2", "value2", 2},
        {"key3", "value3", 3},
    };

    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Properties for the producer
    std::set<ProducerRecord::Id> msgIdsSent;

    // Delivery callback
    Producer::Callback drCallback = [&msgIdsSent, topic, partition, appThreadId = std::this_thread::get_id()]
        (const Producer::RecordMetadata& metadata, const Error& error) {
            std::cout << "[" << Utility::getCurrentTime() << "] Producer::Metadata: " << metadata.toString() << std::endl;
            EXPECT_FALSE(error);
            EXPECT_EQ(topic, metadata.topic());
            EXPECT_EQ(partition, metadata.partition());
            msgIdsSent.emplace(*metadata.recordId());

            EXPECT_EQ(std::this_thread::get_id(), appThreadId); // It would be polled by the same thread
        };

    // The producer would close anyway
    {
        KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig(), KafkaClient::EventsPollingOption::Manual);
        for (const auto& msg: messages)
        {
            auto record = ProducerRecord(topic, partition,
                                         Key(std::get<0>(msg).c_str(), std::get<0>(msg).size()),
                                         Value(std::get<1>(msg).c_str(), std::get<1>(msg).size()),
                                         std::get<2>(msg));
            std::cout << "[" << Utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;
            producer.send(record, drCallback);
        }

        // Wait for the delivery callback (to be triggered)
        const auto end = std::chrono::steady_clock::now() + KafkaTestUtility::MAX_DELIVERY_TIMEOUT;
        do
        {
            // Keep polling for the delivery-callbacks
            producer.pollEvents(KafkaTestUtility::POLL_INTERVAL);
        } while (std::chrono::steady_clock::now() < end);
    }

    // Make sure all delivery callbacks be called
    EXPECT_EQ(messages.size(), msgIdsSent.size());
    for (const auto& msg: messages)
    {
        auto id = std::get<2>(msg);
        EXPECT_NE(msgIdsSent.end(), msgIdsSent.find(id));
    }
}

TEST(KafkaProducer, NoBlockSendingWhileQueueIsFull_ManuallyPollEvents)
{
    const Topic topic       = Utility::getRandomString();
    const auto  appThreadId = std::this_thread::get_id();

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    int msgSentCnt  = 0;

    Producer::Callback drCallback =
        [&msgSentCnt, appThreadId](const Producer::RecordMetadata& metadata, const Error& error) {
            EXPECT_EQ(std::this_thread::get_id(), appThreadId); // It should be polled by the same thread
            EXPECT_FALSE(error);
            std::cout << "[" << Utility::getCurrentTime() << "] Delivery callback called. RecordMetadata: " << metadata.toString() << std::endl;
            ++msgSentCnt;
        };

    auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
    // Here we limit the queue buffering size to be only 1
    props.put(ProducerConfig::QUEUE_BUFFERING_MAX_MESSAGES, "1");

    // Maunally poll producer
    KafkaProducer producer(props, KafkaClient::EventsPollingOption::Manual);

    // Prepare messages to test
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    auto record = ProducerRecord(topic, NullKey, NullValue);

    // To send the 1st message, should succeed
    record.setKey(Key(messages[0].first.c_str(), messages[0].first.size()));
    record.setValue(Value(messages[0].second.c_str(), messages[0].second.size()));
    std::cout << "[" << Utility::getCurrentTime() << "] About to send ProducerRecord: " << record.toString() << std::endl;
    producer.send(record, drCallback);

    // To send the 2nd message, should fail (throw an exception)
    record.setKey(Key(messages[1].first.c_str(), messages[1].first.size()));
    record.setValue(Value(messages[1].second.c_str(), messages[1].second.size()));
    EXPECT_KAFKA_THROW(
        {
            std::cout << "[" << Utility::getCurrentTime() << "] About to send ProducerRecord: " << record.toString() << std::endl;
            producer.send(record, drCallback);
        },
        RD_KAFKA_RESP_ERR__QUEUE_FULL
    );

    // To send the 3rd message, should fail (return the error code)
    record.setKey(Key(messages[2].first.c_str(), messages[2].first.size()));
    record.setValue(Value(messages[2].second.c_str(), messages[2].second.size()));
    Error error;
    std::cout << "[" << Utility::getCurrentTime() << "] About to send ProducerRecord: " << record.toString() << std::endl;
    producer.send(record, drCallback, error);
    EXPECT_EQ(RD_KAFKA_RESP_ERR__QUEUE_FULL, error.value());

    // Wait for the delivery callback (to be triggered)
    const auto end = std::chrono::steady_clock::now() + KafkaTestUtility::MAX_DELIVERY_TIMEOUT;
    do
    {
        // Keep polling for the delivery-callbacks
        producer.pollEvents(KafkaTestUtility::POLL_INTERVAL);
    } while (std::chrono::steady_clock::now() < end);

    // The producer will wait for all delivery callbacks before close
    producer.close();

    // Only the 1st message should be sent succesfully
    EXPECT_EQ(1, msgSentCnt);
}

TEST(KafkaProducer, TooLargeMessageForBroker)
{
    const Topic     topic     = Utility::getRandomString();
    const Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    const auto value  = std::string(2048, 'a');
    const auto record = ProducerRecord(topic, partition, Key(nullptr, 0), Value(value.c_str(), value.size()));

    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                       .put(ProducerConfig::BATCH_SIZE,        "2000000")
                       .put(ProducerConfig::MESSAGE_MAX_BYTES, "2000000") // Note: by default, the brokers only support messages no larger than 1M
                       .put(ProducerConfig::LINGER_MS,         "100");    // Here use a large value to make sure it's long enough to generate a large message-batch

    KafkaProducer producer(props);

    constexpr std::size_t MSG_NUM = 2000;
    std::size_t failedCount = 0;
    for (std::size_t i = 0; i < MSG_NUM; ++i) {
        producer.send(record,
                      [&failedCount] (const Producer::RecordMetadata& /*metadata*/, const Error& error) {
                          if (error) {
                              ++failedCount;
                              EXPECT_EQ(RD_KAFKA_RESP_ERR_MSG_SIZE_TOO_LARGE, error.value());
                          }
                      });
    }

    producer.close();

    std::cout << "[" << Utility::getCurrentTime() << "] Messages total number[" << MSG_NUM << "], with [" << failedCount  << "] failed." << std::endl;
    EXPECT_NE(0, failedCount);
}

TEST(KafkaProducer, CopyRecordValueWithinSend)
{
    const Topic topic = Utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig()
                       .put(ProducerConfig::PARTITIONER, "murmur2"); // `ProducerRecord`s with empty key are mapped to a single partition

    // Send messages (with option "ToCopyRecordValue")
    constexpr std::size_t MSG_NUM = 100;
    {
        KafkaProducer producer(props);

        for (std::size_t i = 0; i < MSG_NUM; ++i)
        {
            auto value  = std::to_string(i); // The payload is string for integar
            auto record = ProducerRecord(topic, Key(nullptr, 0), Value(value.c_str(), value.size()));
            producer.send(record,
                          [] (const Producer::RecordMetadata& /*metadata*/, const Error& error) { EXPECT_FALSE(error); },
                          KafkaProducer::SendOption::ToCopyRecordValue); // Copy the payload internally
        }
    }
    std::cout << "[" << Utility::getCurrentTime() << "] " << MSG_NUM << " messages were delivered." << std::endl;

    // Poll all messages & check
    {
        // Prepare a consumer
        Kafka::KafkaManualCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig().put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest"));
        consumer.setLogLevel(Kafka::Log::Level::Crit);
        consumer.subscribe({topic});

        // Check messages
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        ASSERT_EQ(MSG_NUM, records.size());
        for (std::size_t i = 0; i < records.size(); ++i)
        {
            EXPECT_EQ(std::to_string(i), records[i].value().toString());
        }
    }
}

