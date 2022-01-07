#include "../utils/TestUtility.h"

#include "kafka/AdminClient.h"
#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"


TEST(KafkaProducer, RecordTimestamp)
{
    const kafka::Topic topicWithRecordCreateTime = kafka::utility::getRandomString();
    const kafka::Topic topicWithLogAppendTime    = kafka::utility::getRandomString();

    // Create topics with different "message.timestamp.type" settings
    {
        kafka::clients::AdminClient adminClient(KafkaTestUtility::GetKafkaClientCommonConfig());

        auto createResult = adminClient.createTopics({topicWithRecordCreateTime}, 5, 3, kafka::Properties{{{"message.timestamp.type", "CreateTime"}}}, std::chrono::minutes(1));
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Topic[" << topicWithRecordCreateTime << "] (with CreateTime) was created, result: " << createResult.error.message() << std::endl;
        ASSERT_FALSE(createResult.error);

        createResult = adminClient.createTopics({topicWithLogAppendTime}, 5, 3, kafka::Properties{{{"message.timestamp.type", "LogAppendTime"}}}, std::chrono::minutes(1));
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Topic[" << topicWithLogAppendTime << "] (with LogAppendTime) was created, result: " << createResult.error.message() << std::endl;
        ASSERT_FALSE(createResult.error);

        KafkaTestUtility::WaitMetadataSyncUpBetweenBrokers();
    }

    // Prepare a producer
    kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
    producer.setErrorCallback(KafkaTestUtility::DumpError);

    constexpr int TIME_LAPSE_THRESHOLD_MS = 1000;
    using namespace std::chrono;

    // Test with "CreateTime" topic
    {
        // This would block the brokers for a while (not impact on "CreateTime")
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(3));

        const auto& topic = topicWithRecordCreateTime;

        const std::string payload = "message.timestamp.type=CreateTime";
        auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::Value(payload.c_str(), payload.size()));

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer is about to send a message to topic [" << topic << "]" << std::endl;
        kafka::Timestamp::Value tsMsgAboutToSend = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        auto metadata = producer.syncSend(record);
        kafka::Timestamp::Value tsMsgJustSent = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer has just sent a message to topic [" << topic << "], with metadata[" << metadata.toString() << "]" << std::endl;

        // Poll the message
        kafka::clients::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest"));
        consumer.subscribe({topic});
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        ASSERT_EQ(1, records.size());

        kafka::Timestamp tsRecord = records.front().timestamp();
        std::cout << "Consumer got record from topic[" << topic << "]: " << records.front().toString() << std::endl;

        EXPECT_EQ(kafka::Timestamp::Type::CreateTime, tsRecord.type);
        EXPECT_TRUE(std::abs(tsRecord.msSinceEpoch - tsMsgAboutToSend) < TIME_LAPSE_THRESHOLD_MS);
        EXPECT_TRUE(std::abs(tsRecord.msSinceEpoch - tsMsgJustSent) > TIME_LAPSE_THRESHOLD_MS);
    }

    // Test with "LogAppendTime" topic
    {
        // This would block the brokers for a while ("LogAppend" would delay a few seconds)
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(3));

        const auto& topic = topicWithLogAppendTime;

        const std::string payload = "message.timestamp.type=LogAppendTime";
        auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::Value(payload.c_str(), payload.size()));

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer is about to send a message to topic [" << topic << "]" << std::endl;
        kafka::Timestamp::Value tsMsgAboutToSend = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        auto metadata = producer.syncSend(record);
        kafka::Timestamp::Value tsMsgJustSent = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer has just sent a message to topic [" << topic << "], with metadata[" << metadata.toString() << "]" << std::endl;

        // Poll the message
        kafka::clients::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest"));
        consumer.subscribe({topic});
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        ASSERT_EQ(1, records.size());

        kafka::Timestamp tsRecord = records.front().timestamp();
        std::cout << "Consumer got record from topic[" << topic << "]: " << records.front().toString() << std::endl;

        EXPECT_EQ(kafka::Timestamp::Type::LogAppendTime, tsRecord.type);
        EXPECT_TRUE(std::abs(tsRecord.msSinceEpoch - tsMsgAboutToSend) > TIME_LAPSE_THRESHOLD_MS);
        EXPECT_TRUE(std::abs(tsRecord.msSinceEpoch - tsMsgJustSent) < TIME_LAPSE_THRESHOLD_MS);
    }
}

TEST(KafkaProducer, NoMissedDeliveryCallback)
{
    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    std::mutex                   inFlightMutex;
    std::set<kafka::clients::producer::ProducerRecord::Id> inFlightIds;

    auto insertIdInFlight = [&inFlightMutex, &inFlightIds](kafka::clients::producer::ProducerRecord::Id id) {
        std::lock_guard<std::mutex> guard(inFlightMutex);
        ASSERT_EQ(0, inFlightIds.count(id));
        inFlightIds.insert(id);
    };

    auto removeIdInFlight = [&inFlightMutex, &inFlightIds](kafka::clients::producer::ProducerRecord::Id id) {
        std::lock_guard<std::mutex> guard(inFlightMutex);
        ASSERT_EQ(1, inFlightIds.count(id));
        inFlightIds.erase(id);
    };

    auto sizeOfIdsInFlight = [&inFlightMutex, &inFlightIds]() {
        std::lock_guard<std::mutex> guard(inFlightMutex);
        return inFlightIds.size();
    };

    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "5000"));
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

        constexpr int NUM_OF_MESSAGES = 10;
        for (std::size_t i = 0; i < NUM_OF_MESSAGES; ++i)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::NullValue, i);
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message will be sent with record id[" << i << "]" << std::endl;
            insertIdInFlight(i);

            producer.send(record,
                          [&removeIdInFlight](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                  std::cout << "[" << kafka::utility::getCurrentTime() << "] Delivery callback: metadata[" << metadata.toString() << "], result[" << error.message() << "]" << std::endl;
                                  removeIdInFlight(*metadata.recordId());
                          });
        }

        std::cout << "[" << kafka::utility::getCurrentTime() << "] producer will be closed" << std::endl;
    }
    std::cout << "[" << kafka::utility::getCurrentTime() << "] producer was closed" << std::endl;

    EXPECT_EQ(0, sizeOfIdsInFlight());
}

TEST(KafkaProducer, DeliveryCallbackTriggeredByPurgeWithinClose)
{
    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    constexpr int NUM_OF_MESSAGES = 10;

    std::size_t deliveryCbTriggeredCount = 0;
    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        KafkaTestUtility::PauseBrokers();

        for (std::size_t i = 0; i < NUM_OF_MESSAGES; ++i)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::NullValue, i);
            producer.send(record,
                          [&deliveryCbTriggeredCount](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                  std::cout << "[" << kafka::utility::getCurrentTime() << "] Delivery callback: metadata[" << metadata.toString() << "], result[" << error.message() << "]" << std::endl;
                                  ++deliveryCbTriggeredCount;
                          });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message was just sent: " << record.toString() << std::endl;
        }

        // Would fail since no response from brokers
        auto error = producer.flush(std::chrono::seconds(1));
        EXPECT_EQ(RD_KAFKA_RESP_ERR__TIMED_OUT, error.value());
        std::cout << "[" << kafka::utility::getCurrentTime() << "] producer flush result[" << error.message() << "]" << std::endl;

        // The in-flight messages would be purged within `close()` (thus trigger the delivery callbacks)
        producer.close(std::chrono::seconds(1));
        std::cout << "[" << kafka::utility::getCurrentTime() << "] producer closed" << std::endl;
    }

    EXPECT_EQ(NUM_OF_MESSAGES, deliveryCbTriggeredCount);

    KafkaTestUtility::ResumeBrokers();
}

TEST(KafkaProducer, BrokerStopWhileSendingMessages)
{
    std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    std::size_t deliveryCount = 0;
    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig());
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        // Pause the brokers for a while (shorter then the default "MESSAGE_TIMEOUT_MS" for producer, which is 10 seconds)
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

        for (const auto& msg: messages)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic, 0,
                                                                   kafka::Key(msg.first.c_str(), msg.first.size()),
                                                                   kafka::Value(msg.second.c_str(), msg.second.size()));

            producer.send(record, [&deliveryCount]( const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                      std::cout << "[" << kafka::utility::getCurrentTime() << "] delivery callback: metadata[" << metadata.toString() << "], result[" << error.message() << "]" << std::endl;
                                      EXPECT_FALSE(error); // since the brokers just pause for a short while (< MESSAGE_TIMEOUT_MS), the delivery would success
                                      ++deliveryCount;
                                  });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message was just sent: " << record.toString() << std::endl;
        }
    }

    // Wait for the deliveries
    KafkaTestUtility::WaitUntil([&deliveryCount, msgNum = messages.size()]() { return deliveryCount == msgNum; }, std::chrono::minutes(1));
    ASSERT_EQ(messages.size(), deliveryCount);

    // Fetch & check all messages
    kafka::clients::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                            .put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest"));
    consumer.subscribe({topic});
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
    EXPECT_EQ(messages.size(), records.size());
    for (std::size_t i = 0; i < records.size(); ++i)
    {
        EXPECT_EQ(messages[i].first,  records[i].key().toString());
        EXPECT_EQ(messages[i].second, records[i].value().toString());
    }
}

TEST(KafkaProducer, Send_AckTimeout)
{
    std::vector<std::pair<std::string, std::string>> messages = {
        {"1", "value1"},
        {"2", "value2"},
        {"3", "value3"},
    };

    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "3000")); // If with no response, the delivery would fail in a short time
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

        std::size_t failureCount = 0;
        for (const auto& msg: messages)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic,
                                                                   kafka::Key(msg.first.c_str(), msg.first.size()),
                                                                   kafka::Value(msg.second.c_str(), msg.second.size()));

            producer.send(record, [&failureCount](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                      std::cout << "[" << kafka::utility::getCurrentTime() << "] delivery callback: result[" << error.message() << "],  metadata[" << metadata.toString() << "]" << std::endl;
                                      EXPECT_EQ(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT, error.value());
                                      ++failureCount;
                                  });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message was just sent: " << record.toString() << std::endl;
        }

        std::this_thread::sleep_for(std::chrono::seconds(10));

        EXPECT_EQ(messages.size(), failureCount);
    }
}

TEST(KafkaProducer, ManuallyPollEvents_AckTimeout)
{
    std::vector<std::pair<std::string, std::string>> messages = {
        {"1", "value1"},
        {"2", "value2"},
        {"3", "value3"},
    };

    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                 .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "3000"), // If with no response, the delivery would fail in a short time
                                               kafka::clients::KafkaClient::EventsPollingOption::Manual);                    // Manually call `pollEvents()`
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

        std::size_t failureCount = 0;
        for (const auto& msg: messages)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic,
                                                                   kafka::Key(msg.first.c_str(), msg.first.size()),
                                                                   kafka::Value(msg.second.c_str(), msg.second.size()));

            producer.send(record, [&failureCount](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                      std::cout << "[" << kafka::utility::getCurrentTime() << "] delivery callback: result[" << error.message() << "],  metadata[" << metadata.toString() << "]" << std::endl;
                                      EXPECT_EQ(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT, error.value());
                                      ++failureCount;
                                  });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message was just sent: " << record.toString() << std::endl;
        }

        const auto timeout  = std::chrono::seconds(10);
        const auto interval = std::chrono::milliseconds(100);

        for (const auto end = std::chrono::steady_clock::now() + timeout; std::chrono::steady_clock::now() < end;)
        {
            // Keep polling for the delivery-callbacks
            producer.pollEvents(interval);
        }

        EXPECT_EQ(messages.size(), failureCount);
    }
}

TEST(KafkaProducer, ManuallyPollEvents_AlwaysFinishClosing)
{
    std::vector<std::pair<std::string, std::string>> messages = {
        {"1", "value1"},
        {"2", "value2"},
        {"3", "value3"},
    };

    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    std::size_t failureCount = 0;
    {
        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                 .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "3000"), // If with no response, the delivery would fail in a short time
                                               kafka::clients::KafkaClient::EventsPollingOption::Manual);            // Manually call `pollEvents()`
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

        const auto appThreadId = std::this_thread::get_id();
        for (const auto& msg: messages)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic,
                                                                   kafka::Key(msg.first.c_str(), msg.first.size()),
                                                                   kafka::Value(msg.second.c_str(), msg.second.size()));

            producer.send(record, [&failureCount, appThreadId](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                      std::cout << "[" << kafka::utility::getCurrentTime() << "] delivery callback: result[" << error.message() << "],  metadata[" << metadata.toString() << "]" << std::endl;
                                      EXPECT_EQ(RD_KAFKA_RESP_ERR__MSG_TIMED_OUT, error.value());
                                      EXPECT_EQ(appThreadId, std::this_thread::get_id());
                                      ++failureCount;
                                  });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Message was just sent: " << record.toString() << std::endl;
        }
        // KafkaProducer would always flush message within `close()`, --even with no `pollEvents()` explicitly called
    }

    EXPECT_EQ(messages.size(), failureCount);
}

TEST(KafkaProducer, SyncSend_AckTimeout)
{
    const kafka::Topic topic = kafka::utility::getRandomString();
    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                            .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "3000"));
    producer.setErrorCallback(KafkaTestUtility::DumpError);

    // Pause the brokers for a while
    auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(5));

    auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::NullValue);
    std::cout << "[" << kafka::utility::getCurrentTime() << "] About to send record: " << record.toString() << std::endl;

    EXPECT_KAFKA_THROW(producer.syncSend(record), RD_KAFKA_RESP_ERR__MSG_TIMED_OUT);
}

