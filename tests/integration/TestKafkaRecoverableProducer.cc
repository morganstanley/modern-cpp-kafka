#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/addons/KafkaRecoverableProducer.h"

#include "gtest/gtest.h"


TEST(KafkaRecoverableProducer, SendMessages)
{
    // Prepare messages to test
    const std::vector<std::pair<std::string, std::string>> messages = {
        {"key1", "value1"},
        {"key2", "value2"},
        {"key3", "value3"},
    };

    const kafka::Topic     topic     = kafka::utility::getRandomString();
    const kafka::Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    {
        // Properties for the producer
        const auto props = KafkaTestUtility::GetKafkaClientCommonConfig().put(kafka::clients::producer::Config::ACKS, "all");

        // Recoverable producer
        kafka::clients::KafkaRecoverableProducer producer(props);

        // Send messages
        kafka::clients::producer::ProducerRecord::Id id = 0;
        for (const auto& msg: messages)
        {
            auto record = kafka::clients::producer::ProducerRecord(topic, partition,
                                                                   kafka::Key(msg.first.c_str(), msg.first.size()),
                                                                   kafka::Value(msg.second.c_str(), msg.second.size()),
                                                                   id++);
            std::cout << "[" <<kafka::utility::getCurrentTime() << "] ProducerRecord: " << record.toString() << std::endl;

            // sync-send
            {
                auto metadata = producer.syncSend(record);
                std::cout << "[" <<kafka::utility::getCurrentTime() << "] Message sync-sent. Metadata: " << metadata.toString() << std::endl;
            }

            // async-send
            {
                producer.send(record,
                              [] (const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                                    EXPECT_FALSE(error);
                                    std::cout << "[" <<kafka::utility::getCurrentTime() << "] Message async-sent. Metadata: " << metadata.toString() << std::endl;
                              });
            }
        }

        producer.close();
    }

    {
        // Prepare a consumer
        const auto consumerProps = KafkaTestUtility::GetKafkaClientCommonConfig().put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest");
        kafka::clients::KafkaConsumer consumer(consumerProps);
        consumer.setLogLevel(kafka::Log::Level::Crit);
        consumer.subscribe({topic});

        // Poll these messages
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        // Check the messages
        EXPECT_EQ(messages.size() * 2, records.size());
        for (std::size_t i = 0; i < records.size(); ++i)
        {
            EXPECT_EQ(messages[i/2].first,  std::string(static_cast<const char*>(records[i].key().data()), records[i].key().size()));
            EXPECT_EQ(messages[i/2].second, std::string(static_cast<const char*>(records[i].value().data()), records[i].value().size()));
        }
    }
}

#ifdef KAFKA_API_ENABLE_UNIT_TEST_STUBS
TEST(KafkaRecoverableProducer, MockFatalError)
{
    const kafka::Topic     topic     = kafka::utility::getRandomString();
    const kafka::Partition partition = 0;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    // Properties for the producer
    const auto props = KafkaTestUtility::GetKafkaClientCommonConfig();

    // Recoverable producer
    kafka::clients::KafkaRecoverableProducer producer(props);

    // Prepare messages to send
    static constexpr int MSG_NUM = 50;
    std::mutex messagesMutex;
    std::list<kafka::clients::producer::ProducerRecord::Id> messagesToSend;
    for (std::size_t i = 0; i < MSG_NUM; ++i) messagesToSend.push_back(i);

    int sendCount     = 0;
    int deliveryCount = 0;
    while (deliveryCount < sendCount || !messagesToSend.empty())
    {
        if (!messagesToSend.empty())
        {
            auto toSend = messagesToSend.front();
            {
                std::lock_guard<std::mutex> lock(messagesMutex);
                messagesToSend.pop_front();
            }

            std::shared_ptr<std::string> payload = std::make_shared<std::string>(std::to_string(toSend));
            auto record = kafka::clients::producer::ProducerRecord(topic, partition,
                                                                   kafka::NullKey,
                                                                   kafka::Value(payload->c_str(), payload->size()),
                                                                   toSend);

            std::cout << "[" <<kafka::utility::getCurrentTime() << "] about to send ProducerRecord: " << record.toString() << std::endl;
            producer.send(record,
                          [payload, &deliveryCount, &messagesMutex, &messagesToSend, &producer](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                             std::cout << "[" <<kafka::utility::getCurrentTime() << "] Delivery callback triggered! Metadata: " << metadata.toString() << ", error: " << error.toString() << std::endl;

                             // Would resend the message
                             if (error) {
                                std::lock_guard<std::mutex> lock(messagesMutex);
                                messagesToSend.push_front(static_cast<kafka::clients::producer::ProducerRecord::Id>(std::stoi(*payload)));
                             }

                             ++deliveryCount;

                             // Mock fatal error
                             if (deliveryCount % 11 == 0) {
                                 std::cout << "[" <<kafka::utility::getCurrentTime() << "] Mock a fatal error" << std::endl;
                                 producer.mockFatalError();
                             }

                          });
            ++sendCount;

            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    }
    EXPECT_EQ(sendCount, deliveryCount);

    // Prepare a consumer
    const auto consumerProps = KafkaTestUtility::GetKafkaClientCommonConfig().put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest");
    kafka::clients::KafkaConsumer consumer(consumerProps);
    consumer.setLogLevel(kafka::Log::Level::Crit);
    consumer.subscribe({topic});

    // Poll these messages
    auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

    // Check the messages
    std::cout << records.size() << " message got by consumer" << std::endl;

    std::map<kafka::clients::producer::ProducerRecord::Id, int> countMap;
    for (const auto& record: records)
    {
        std::string payload(static_cast<const char*>(record.value().data()), record.value().size());
        ++countMap[static_cast<kafka::clients::producer::ProducerRecord::Id>(std::stoi(payload))];
    }

    for (std::size_t i = 0; i < MSG_NUM; ++i)
    {
        auto id = static_cast<kafka::clients::producer::ProducerRecord::Id>(i);
        auto count = countMap[id];

        EXPECT_TRUE(count > 0);

        if (count > 1) std::cout << "Message " << id << " was duplicated! count[" << count << "]" << std::endl;
    }
}

#endif

