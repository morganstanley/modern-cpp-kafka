#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <chrono>


using namespace KAFKA_API;


TEST(Transaction, CommitTransaction)
{
    enum class TransactionClosureAction { ToComplete, ToAbort, NoAction };

    auto sendMessageWithTransactions = [](const std::string& message, const Topic& topic, TransactionClosureAction closureAction) {
        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);
        std::cout << "[" << Utility::getCurrentTime() << "] Producer created." << std::endl;

        producer.initTransactions(std::chrono::seconds(10));
        std::cout << "[" << Utility::getCurrentTime() << "] Producer initialized the transaction." << std::endl;

        producer.beginTransaction();

        auto payload = std::make_shared<std::string>(message);
        auto record = ProducerRecord(topic, NullKey, Value(payload->c_str(), payload->size()));

        producer.send(record,
                      [payload](const Producer::RecordMetadata& metadata, std::error_code ec) {
                          std::cout << "[" << Utility::getCurrentTime() << "] Producer got the delivery result: " << ec.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << Utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;

        if (closureAction == TransactionClosureAction::ToComplete) {
            producer.commitTransaction(std::chrono::seconds(10));
            std::cout << "[" << Utility::getCurrentTime() << "] Producer committed the transaction." << std::endl;
        } else if (closureAction == TransactionClosureAction::ToAbort) {
            producer.abortTransaction();
            std::cout << "[" << Utility::getCurrentTime() << "] Producer aborted the transaction." << std::endl;
        } else {
            std::cout << "[" << Utility::getCurrentTime() << "] Producer would not complete/abort the transaction." << std::endl;
        }
    };

    enum class IsolationLevel { ReadCommitted, ReadUnCommitted };

    auto receiveMessages = [](const Topic& topic, IsolationLevel isolationLevel) {
        const std::string isolationConf = (isolationLevel == IsolationLevel::ReadCommitted) ? "read_committed" : "read_uncommitted";

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest");
        props.put(ConsumerConfig::ISOLATION_LEVEL,   isolationConf);

        Kafka::KafkaManualCommitConsumer consumer(props);
        consumer.setLogLevel(LOG_CRIT);
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        std::vector<std::string> messages;
        for (const auto& record: records) {
            std::cout << "[" << Utility::getCurrentTime() << "] Consumer got message: " << record.toString() << std::endl;
            messages.emplace_back(record.value().toString());
        }

        return messages;
    };

    {
        KafkaTestUtility::PrintDividingLine("Producer: commitTransaction, Consumer: isolation.level=read_committed");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to commitTransaction", topic, TransactionClosureAction::ToComplete);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(1, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: commitTransaction, Consumer: isolation.level=read_uncommitted");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to commitTransaction", topic, TransactionClosureAction::ToComplete);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(1, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: abortTransaction, Consumer: isolation.level=read_committed");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to abortTransaction", topic, TransactionClosureAction::ToAbort);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: abortTransaction, Consumer: isolation.level=read_uncommitted");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to abortTransaction", topic, TransactionClosureAction::ToAbort);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: no commit/abortTransaction, Consumer: isolation.level=read_committed");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message with no commit/abortTransaction", topic, TransactionClosureAction::NoAction);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: no commit/abortTransaction, Consumer: isolation.level=read_uncommitted");

        const Topic     topic     = Utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message with no commit/abortTransaction", topic, TransactionClosureAction::NoAction);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(1, messages.size());
    }
}

TEST(Transaction, CatchException)
{
    {
        KafkaTestUtility::PrintDividingLine("No transaction.id configured");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();

        KafkaAsyncProducer producer(props);

        EXPECT_KAFKA_THROW(producer.initTransactions(), RD_KAFKA_RESP_ERR__NOT_CONFIGURED);
    }

    {
        KafkaTestUtility::PrintDividingLine("No initTransactions");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        EXPECT_KAFKA_THROW(producer.beginTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("No beginTransaction");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        producer.initTransactions();

        EXPECT_KAFKA_THROW(producer.commitTransaction(), RD_KAFKA_RESP_ERR__STATE);
        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no initTransactions)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no beginTransaction)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        producer.initTransactions();

        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no message sent)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        producer.initTransactions();

        producer.beginTransaction();

        producer.abortTransaction();
    }

    {
        KafkaTestUtility::PrintDividingLine("commitTransation (with no message sent)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, Utility::getRandomString());

        KafkaAsyncProducer producer(props);

        producer.initTransactions();

        producer.beginTransaction();

        producer.commitTransaction();
    }
}

TEST(Transaction, ContinueTheTransaction)
{
    const Topic       topic         = Utility::getRandomString();
    const std::string transactionId = Utility::getRandomString();
    const std::string messageToSent = "message to sent";

    // Start a producer to send the message, but fail to commit
    {
        KafkaAsyncProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                      .put(ProducerConfig::TRANSACTIONAL_ID, transactionId));

        producer.initTransactions();

        producer.beginTransaction();

        auto record = ProducerRecord(topic, NullKey, Value(messageToSent.c_str(), messageToSent.size()));

        producer.send(record,
                      [](const Producer::RecordMetadata& metadata, std::error_code ec) {
                          std::cout << "[" << Utility::getCurrentTime() << "] Producer got the delivery result: " << ec.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << Utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;
    }

    // Start another producer, continue to send the message (with the same transaction.id)
    {
        KafkaAsyncProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                      .put(ProducerConfig::TRANSACTIONAL_ID, transactionId));

        producer.initTransactions();

        producer.beginTransaction();

        auto record = ProducerRecord(topic, NullKey, Value(messageToSent.c_str(), messageToSent.size()));

        producer.send(record,
                      [](const Producer::RecordMetadata& metadata, std::error_code ec) {
                          std::cout << "[" << Utility::getCurrentTime() << "] Producer got the delivery result: " << ec.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << Utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;

        producer.commitTransaction();
    }

    // Check all received messages (committed only)
    {
        KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                           .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                           .put(ConsumerConfig::ISOLATION_LEVEL,   "read_committed"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        for (const auto& record: records)
        {
            std::cout << record.toString() << std::endl;
        }

        EXPECT_EQ(1, records.size());
    }

    // Check all received messages (incluing uncommitted)
    {
        KafkaAutoCommitConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                           .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                           .put(ConsumerConfig::ISOLATION_LEVEL,   "read_uncommitted"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        for (const auto& record: records)
        {
            std::cout << record.toString() << std::endl;
        }

        // Uncertain result: most of the time, it would be 2.
        EXPECT_TRUE(records.size() == 2 || records.size() == 1);
    }
}

