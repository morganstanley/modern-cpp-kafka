#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <chrono>


using namespace KAFKA_API;


TEST(Transaction, DeliveryFailure)
{
    const Topic       topic         = Utility::getRandomString();
    const std::string transactionId = Utility::getRandomString();
    const std::string messageToSent = "message to sent";
    const std::size_t numMessages   = 10;

    {
        auto record = ProducerRecord(topic, NullKey, Value(messageToSent.c_str(), messageToSent.size()));

        KafkaAsyncProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                      .put(ProducerConfig::MESSAGE_TIMEOUT_MS, "3000")  // The delivery would fail in a short timeout
                                      .put(ProducerConfig::TRANSACTIONAL_ID, transactionId));

        std::cout << "[" << Utility::getCurrentTime() << "] Producer created." << std::endl;

        producer.initTransactions();
        std::cout << "[" << Utility::getCurrentTime() << "] Producer initialized the transaction." << std::endl;

        producer.beginTransaction();
        std::cout << "[" << Utility::getCurrentTime() << "] Producer began the transaction." << std::endl;

        // Pause the brokers for a while
        auto asyncTask = KafkaTestUtility::PauseBrokersForAWhile(std::chrono::seconds(10));

        // Send messages
        for (std::size_t i = 0; i < numMessages; ++i)
        {
            producer.send(record,
                          [](const Producer::RecordMetadata& metadata, std::error_code ec) {
                              std::cout << "[" << Utility::getCurrentTime() << "] Producer got the delivery result: " << ec.message()
                                  << ", with metadata: " << metadata.toString() << std::endl;
                          });
            std::cout << "[" << Utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;
        }

        // Will fail to commit and abortTransaction
        try
        {
            std::cout << "[" << Utility::getCurrentTime() << "] Producer will commit the transaction" << record.toString() << std::endl;
            producer.commitTransaction();
        }
        catch (const KafkaException& e)
        {
            std::cout << "[" << Utility::getCurrentTime() << "] Exception caught: " << e.what() << std::endl;
            EXPECT_EQ(RD_KAFKA_RESP_ERR__INCONSISTENT, e.error().errorCode().value());

            std::cout << "[" << Utility::getCurrentTime() << "] Producer will abort the transaction" << record.toString() << std::endl;
            producer.abortTransaction();
        }
    }

    // Check all received messages (incluing uncommitted)
    {
        KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                           .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                           .put(ConsumerConfig::ISOLATION_LEVEL,   "read_uncommitted"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer, std::chrono::seconds(1));

        EXPECT_EQ(0, records.size());
    }
}

