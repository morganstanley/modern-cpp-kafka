#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <chrono>


TEST(Transaction, DeliveryFailure)
{
    const kafka::Topic topic         = kafka::utility::getRandomString();
    const std::string  transactionId = kafka::utility::getRandomString();
    const std::string  messageToSent = "message to sent";
    const std::size_t  numMessages   = 10;

    KafkaTestUtility::CreateKafkaTopic(topic, 5, 3);

    {
        auto record = kafka::clients::producer::ProducerRecord(topic, kafka::NullKey, kafka::Value(messageToSent.c_str(), messageToSent.size()));

        kafka::clients::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                 .put(kafka::clients::producer::Config::MESSAGE_TIMEOUT_MS, "3000")  // The delivery would fail in a short timeout
                                                 .put(kafka::clients::producer::Config::TRANSACTIONAL_ID,   transactionId));
        producer.setErrorCallback(KafkaTestUtility::DumpError);

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer created." << std::endl;

        producer.initTransactions();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer initialized the transaction." << std::endl;

        producer.beginTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer began the transaction." << std::endl;

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(10));

        // Send messages
        for (std::size_t i = 0; i < numMessages; ++i)
        {
            producer.send(record,
                          [](const kafka::clients::producer::RecordMetadata& metadata, const kafka::Error& error) {
                              std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer got the delivery result: " << error.message()
                                  << ", with metadata: " << metadata.toString() << std::endl;
                          });
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;
        }

        // Will fail to commit and abortTransaction
        try
        {
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer will commit the transaction" << record.toString() << std::endl;
            producer.commitTransaction();
        }
        catch (const kafka::KafkaException& e)
        {
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Exception caught: " << e.what() << std::endl;
            EXPECT_EQ(RD_KAFKA_RESP_ERR__INCONSISTENT, e.error().value());

            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer will abort the transaction" << record.toString() << std::endl;
            producer.abortTransaction();
        }
    }

    // Check all received messages (incluing uncommitted)
    {
        kafka::clients::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                               .put(kafka::clients::consumer::Config::AUTO_OFFSET_RESET, "earliest")
                                               .put(kafka::clients::consumer::Config::ISOLATION_LEVEL,   "read_uncommitted"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));

        EXPECT_EQ(0, records.size());
    }
}

