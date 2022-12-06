#include "../utils/TestUtility.h"

#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <chrono>


TEST(Transaction, CommitTransaction)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;
    using namespace kafka::clients::consumer;

    enum class TransactionClosureAction { ToComplete, ToAbort, NoAction };

    auto sendMessageWithTransactions = [](const std::string& message, const kafka::Topic& topic, TransactionClosureAction closureAction) {
        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer created." << std::endl;

        producer.initTransactions();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer initialized the transaction." << std::endl;

        producer.beginTransaction();

        auto payload = std::make_shared<std::string>(message);
        auto record  = ProducerRecord(topic, kafka::NullKey, kafka::Value(payload->c_str(), payload->size()));

        producer.send(record,
                      [payload](const RecordMetadata& metadata, const kafka::Error& error) {
                          std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer got the delivery result: " << error.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;

        if (closureAction == TransactionClosureAction::ToComplete) {
            producer.commitTransaction(std::chrono::seconds(10));
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer committed the transaction." << std::endl;
        } else if (closureAction == TransactionClosureAction::ToAbort) {
            producer.abortTransaction();
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer aborted the transaction." << std::endl;
        } else {
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer would not complete/abort the transaction." << std::endl;
        }
    };

    enum class IsolationLevel { ReadCommitted, ReadUnCommitted };

    auto receiveMessages = [](const kafka::Topic& topic, IsolationLevel isolationLevel) {
        const std::string isolationConf = (isolationLevel == IsolationLevel::ReadCommitted) ? "read_committed" : "read_uncommitted";

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest");
        props.put(ConsumerConfig::ISOLATION_LEVEL,   isolationConf);

        KafkaConsumer consumer(props);
        consumer.setLogLevel(kafka::Log::Level::Crit);
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        std::vector<std::string> messages;
        for (const auto& record: records) {
            std::cout << "[" << kafka::utility::getCurrentTime() << "] Consumer got message: " << record.toString() << std::endl;
            messages.emplace_back(record.value().toString());
        }

        return messages;
    };

    {
        KafkaTestUtility::PrintDividingLine("Producer: commitTransaction, Consumer: isolation.level=read_committed");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to commitTransaction", topic, TransactionClosureAction::ToComplete);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(1, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: commitTransaction, Consumer: isolation.level=read_uncommitted");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to commitTransaction", topic, TransactionClosureAction::ToComplete);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(1, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: abortTransaction, Consumer: isolation.level=read_committed");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to abortTransaction", topic, TransactionClosureAction::ToAbort);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: abortTransaction, Consumer: isolation.level=read_uncommitted");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message to abortTransaction", topic, TransactionClosureAction::ToAbort);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: no commit/abortTransaction, Consumer: isolation.level=read_committed");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message with no commit/abortTransaction", topic, TransactionClosureAction::NoAction);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadCommitted);
        ASSERT_EQ(0, messages.size());
    }

    {
        KafkaTestUtility::PrintDividingLine("Producer: no commit/abortTransaction, Consumer: isolation.level=read_uncommitted");

        const kafka::Topic     topic     = kafka::utility::getRandomString();
        KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

        sendMessageWithTransactions("message with no commit/abortTransaction", topic, TransactionClosureAction::NoAction);

        const auto messages = receiveMessages(topic, IsolationLevel::ReadUnCommitted);
        ASSERT_EQ(1, messages.size());
    }
}

TEST(Transaction, CatchException)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;

    {
        KafkaTestUtility::PrintDividingLine("No transaction.id configured");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();

        KafkaProducer producer(props);

        EXPECT_KAFKA_THROW(producer.initTransactions(), RD_KAFKA_RESP_ERR__NOT_CONFIGURED);
    }

    {
        KafkaTestUtility::PrintDividingLine("No initTransactions");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        EXPECT_KAFKA_THROW(producer.beginTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("No beginTransaction");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        producer.initTransactions();

        EXPECT_KAFKA_THROW(producer.commitTransaction(), RD_KAFKA_RESP_ERR__STATE);
        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no initTransactions)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no beginTransaction)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        producer.initTransactions();

        EXPECT_KAFKA_THROW(producer.abortTransaction(), RD_KAFKA_RESP_ERR__STATE);
    }

    {
        KafkaTestUtility::PrintDividingLine("abortTransaction (with no message sent)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        producer.initTransactions();

        producer.beginTransaction();

        producer.abortTransaction();
    }

    {
        KafkaTestUtility::PrintDividingLine("commitTransation (with no message sent)");

        auto props = KafkaTestUtility::GetKafkaClientCommonConfig();
        props.put(ProducerConfig::TRANSACTIONAL_ID, kafka::utility::getRandomString());

        KafkaProducer producer(props);

        producer.initTransactions();

        producer.beginTransaction();

        producer.commitTransaction();
    }
}

TEST(Transaction, ContinueTheTransaction)
{
    using namespace kafka::clients::producer;

    const kafka::Topic topic         = kafka::utility::getRandomString();
    const std::string  transactionId = kafka::utility::getRandomString();
    const std::string  messageToSend = "message to send";

    KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

    // Start a producer to send the message, but fail to commit
    {
        kafka::clients::producer::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(kafka::clients::producer::ProducerConfig::TRANSACTIONAL_ID, transactionId));

        producer.initTransactions();

        producer.beginTransaction();

        auto record = ProducerRecord(topic, kafka::NullKey, kafka::Value(messageToSend.c_str(), messageToSend.size()));

        producer.send(record,
                      [](const RecordMetadata& metadata, const kafka::Error& error) {
                          std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer got the delivery result: " << error.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;
    }

    // Start another producer, continue to send the message (with the same transaction.id)
    {
        kafka::clients::producer::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(kafka::clients::producer::ProducerConfig::TRANSACTIONAL_ID, transactionId));

        producer.initTransactions();

        producer.beginTransaction();

        auto record = ProducerRecord(topic, kafka::NullKey, kafka::Value(messageToSend.c_str(), messageToSend.size()));

        producer.send(record,
                      [](const RecordMetadata& metadata, const kafka::Error& error) {
                          std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer got the delivery result: " << error.message()
                              << ", with metadata: " << metadata.toString() << std::endl;
                      });

        std::cout << "[" << kafka::utility::getCurrentTime() << "] Producer async-sent the message: " << record.toString() << std::endl;

        producer.commitTransaction();
    }

    // Check all received messages (committed only)
    {
        kafka::clients::consumer::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                               .put(kafka::clients::consumer::ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                               .put(kafka::clients::consumer::ConsumerConfig::ISOLATION_LEVEL,   "read_committed"));
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
        kafka::clients::consumer::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                               .put(kafka::clients::consumer::ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                               .put(kafka::clients::consumer::ConsumerConfig::ISOLATION_LEVEL,   "read_uncommitted"));
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

TEST(Transaction, ContinueTheTransaction2)
{
    using namespace kafka::clients::producer;
    using namespace kafka::clients::consumer;

    const kafka::Topic topic         = kafka::utility::getRandomString();
    const std::string  transactionId = kafka::utility::getRandomString();
    const std::string  clientId      = "someTransactionalProducer";

    constexpr std::size_t NUM_MESSAGES = 100;
    std::vector<std::string>  messagesToSend;
    for(std::size_t i = 0; i < NUM_MESSAGES; ++i)
    {
        messagesToSend.emplace_back(std::to_string(i));
    }

    KafkaTestUtility::CreateKafkaTopic(topic, 1, 3);

    // Start a producer to send the messages, but fail to commit the transaction for some messages (before close)
    {
        kafka::clients::producer::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                .put(ProducerConfig::TRANSACTIONAL_ID, transactionId)
                                                .put(ProducerConfig::CLIENT_ID, clientId));

        producer.initTransactions();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer initialized the transaction" << std::endl;

        // Send the first batch of messages
        producer.beginTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer began the transaction" << std::endl;

        std::atomic<std::size_t> delivered(0);
        for (std::size_t i = 0; i < NUM_MESSAGES / 2; ++i)
        {
            const auto& msg = messagesToSend[i];
            auto record = ProducerRecord(topic,
                                         kafka::NullKey,
                                         kafka::Value(msg.c_str(), msg.size()));

            producer.send(record,
                          [&delivered](const RecordMetadata& metadata, const kafka::Error& error) {
                              ++delivered;

                              if (error) {
                                  std::cerr << "[" << kafka::utility::getCurrentTime() << "] Producer got delivery failure: " << error.message()
                                      << ", with metadata: " << metadata.toString() << std::endl;
                              }
                              ASSERT_FALSE(error);
                          });
        }

        KafkaTestUtility::WaitUntil([&delivered, count = NUM_MESSAGES / 2](){ return delivered == count; }, std::chrono::seconds(5));
        ASSERT_EQ(NUM_MESSAGES / 2, delivered.load());

        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer async-sent " << delivered.load() << " messages" << std::endl;

        producer.commitTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer committed the transaction" << std::endl;

        // Send the second batch of messages
        producer.beginTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer began the transaction" << std::endl;

        for (std::size_t i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i)
        {
            const auto& msg = messagesToSend[i];
            auto record = ProducerRecord(topic,
                                         kafka::NullKey,
                                         kafka::Value(msg.c_str(), msg.size()));

            producer.send(record,
                          [&delivered](const RecordMetadata& metadata, const kafka::Error& error) {
                              ++delivered;

                              if (error) {
                                  std::cerr << "[" << kafka::utility::getCurrentTime() << "] Producer got delivery failure: " << error.message()
                                      << ", with metadata: " << metadata.toString() << std::endl;
                              }
                              ASSERT_FALSE(error);
                          });
        }

        // Wait the batch of messages to be delivered
        KafkaTestUtility::WaitUntil([&delivered, count = NUM_MESSAGES](){ return delivered == count; }, std::chrono::seconds(5));
        ASSERT_EQ(NUM_MESSAGES, delivered.load());

        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer async-sent another " << NUM_MESSAGES / 2 << " messages" << std::endl;

        // No commitTransaction for the second batch of messages
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The first producer would NOT commit the transaction" << std::endl;
    }

    // Re-start the producer, continue to send the message (with the same transaction.id)
    {
        kafka::clients::producer::KafkaProducer producer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                                 .put(ProducerConfig::TRANSACTIONAL_ID, transactionId)
                                                 .put(ProducerConfig::CLIENT_ID, clientId ) );

        producer.initTransactions();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The second producer initialized the transaction" << std::endl;

        producer.beginTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The second producer began the transaction" << std::endl;

        // Continue to send the second batch of messages
        std::atomic<std::size_t> delivered(0);
        for (std::size_t i = NUM_MESSAGES / 2; i < NUM_MESSAGES; ++i)
        {
            const auto& msg = messagesToSend[i];
            auto record = ProducerRecord(topic,
                                         kafka::NullKey,
                                         kafka::Value(msg.c_str(), msg.size()));

            producer.send(record,
                          [&delivered](const RecordMetadata& metadata, const kafka::Error& error) {
                              ++delivered;

                              if (error) {
                                  std::cerr << "[" << kafka::utility::getCurrentTime() << "] Producer got delivery failure: " << error.message()
                                      << ", with metadata: " << metadata.toString() << std::endl;
                              }
                              ASSERT_FALSE(error);
                          });
        }

        KafkaTestUtility::WaitUntil([&delivered, count = NUM_MESSAGES / 2](){ return delivered == count; }, std::chrono::seconds(5));
        ASSERT_EQ(NUM_MESSAGES / 2, delivered.load());

        std::cout << "[" << kafka::utility::getCurrentTime() << "] The second producer async-sent " << delivered.load() << " messages" << std::endl;

        producer.commitTransaction();
        std::cout << "[" << kafka::utility::getCurrentTime() << "] The second producer committed the transaction" << std::endl;
    }

    // Check all received messages (committed only)
    {
        kafka::clients::consumer::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                               .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                               .put(ConsumerConfig::ISOLATION_LEVEL,   "read_committed"));
        consumer.subscribe({topic});

        // No message lost, no message duplicated
        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);
        for (std::size_t i = 0; i < records.size(); ++i)
        {
            EXPECT_EQ(std::to_string(i), records[i].value().toString());
        }

        EXPECT_EQ(NUM_MESSAGES, records.size());
    }

    // Check all received messages (incluing uncommitted)
    {
        kafka::clients::consumer::KafkaConsumer consumer(KafkaTestUtility::GetKafkaClientCommonConfig()
                                               .put(ConsumerConfig::AUTO_OFFSET_RESET, "earliest")
                                               .put(ConsumerConfig::ISOLATION_LEVEL,   "read_uncommitted"));
        consumer.subscribe({topic});

        auto records = KafkaTestUtility::ConsumeMessagesUntilTimeout(consumer);

        // Those uncommitted messages would be got as well
        EXPECT_EQ(NUM_MESSAGES + NUM_MESSAGES / 2, records.size());
    }
}

