#pragma once

#include "kafka/AdminClient.h"
#include "kafka/KafkaConsumer.h"
#include "kafka/KafkaProducer.h"

#include "gtest/gtest.h"

#include <boost/algorithm/string.hpp>

#include <cstdlib>
#include <list>
#include <regex>
#include <signal.h>
#include <vector>


#define EXPECT_KAFKA_THROW(expr, err)                                   \
    do {                                                                \
        try {                                                           \
            expr;                                                       \
        } catch (const KafkaException& e) {                                                                  \
            const auto& error = e.error();                                                                   \
            std::cout << "Exception caught: " << e.what()                                                    \
                << (error.isFatal() ? (*error.isFatal() ? ", fatal" : ", non-fatal") : "")                      \
                << (error.isRetriable() ? (*error.isRetriable() ? ",  retriable" : ", non-retriable") : "")  \
                << std::endl;                                                                                \
            EXPECT_EQ(err, e.error().errorCode().value());                                                   \
            break;                                                                                           \
        } catch (...){                                                  \
        }                                                               \
        EXPECT_FALSE(true);                                             \
    } while(false)

#define EXPECT_KAFKA_NO_THROW(expr)   \
    try {                             \
        expr;                         \
    } catch (...){                    \
        EXPECT_FALSE(true);           \
    }

#define EXPECT_KAFKA_NO_THROW(expr)   \
    try {                             \
        expr;                         \
    } catch (...){                    \
        EXPECT_FALSE(true);           \
    }

#define RETRY_FOR_ERROR(expr, errToRetry, maxRetries)                                       \
    for (int cnt = 0; cnt <= maxRetries; ++cnt) {                                           \
        try {                                                                               \
            expr;                                                                           \
            break;                                                                          \
        } catch (const KafkaException& e) {                                                 \
            if (e.error().errorCode().value() == errToRetry && cnt != maxRetries) continue; \
            throw;                                                                          \
        }                                                                                   \
    }

namespace Kafka = KAFKA_API;

namespace KafkaTestUtility {

inline Kafka::Properties
GetKafkaClientCommonConfig()
{
    Kafka::Properties props;
    if (!getenv("KAFKA_BROKER_LIST"))
    {
        EXPECT_TRUE(false);
        return props;
    }

    std::string additionalSettings;
    if (const auto* additionalSettingEnv = getenv("KAFKA_CLIENT_ADDITIONAL_SETTINGS"))
    {
        additionalSettings = additionalSettingEnv;
    }

    props.put("bootstrap.servers", getenv("KAFKA_BROKER_LIST"));
    if (!additionalSettings.empty())
    {
        std::vector<std::string> keyValuePairs;
        boost::algorithm::split(keyValuePairs, additionalSettings, boost::is_any_of(";"));
        for (const auto& keyValue: keyValuePairs)
        {
            std::vector<std::string> kv;
            boost::algorithm::split(kv, keyValue, boost::is_any_of("="));
            EXPECT_EQ(2, kv.size());
            if (kv.size() == 2)
            {
                props.put(kv[0], kv[1]);
            }
            else
            {
                std::cout << "Wrong setting: " << keyValue << std::endl;
            }
        }
    }

    return props;
}

inline std::size_t
GetNumberOfKafkaBrokers()
{
    if (!getenv("KAFKA_BROKER_LIST")) return 0;

    std::string brokers = getenv("KAFKA_BROKER_LIST");
    return std::count(brokers.cbegin(), brokers.cend(), ',') + 1;
}

const auto POLL_INTERVAL             = std::chrono::milliseconds(100);
const auto MAX_POLL_MESSAGES_TIMEOUT = std::chrono::seconds(5);
const auto MAX_OFFSET_COMMIT_TIMEOUT = std::chrono::seconds(15);
const auto MAX_DELIVERY_TIMEOUT      = std::chrono::seconds(5);

inline std::vector<Kafka::ConsumerRecord>
ConsumeMessagesUntilTimeout(Kafka::KafkaConsumer& consumer,
                            std::chrono::milliseconds timeout = MAX_POLL_MESSAGES_TIMEOUT)
{
    std::vector<Kafka::ConsumerRecord> records;

    const auto end = std::chrono::steady_clock::now() + timeout;
    do
    {
        auto polled = consumer.poll(POLL_INTERVAL);
        records.insert(records.end(), std::make_move_iterator(polled.begin()), std::make_move_iterator(polled.end()));
    } while (std::chrono::steady_clock::now() < end);

    std::cout << "[" << Kafka::Utility::getCurrentTime() << "] " << consumer.name() << " polled "  << records.size() << " messages" << std::endl;

    EXPECT_TRUE(std::none_of(records.cbegin(), records.cend(),
                             [](const auto& record) {
                                 return record.error() && record.error().value() != RD_KAFKA_RESP_ERR__PARTITION_EOF;
                             }));

    return records;
}

inline void
WaitUntil(const std::function<bool()>& checkDone, std::chrono::milliseconds timeout)
{
    constexpr int CHECK_INTERVAL_MS = 100;

    const auto end = std::chrono::steady_clock::now() + timeout;

    for (; !checkDone() && std::chrono::steady_clock::now() < end; )
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(CHECK_INTERVAL_MS));
    }
}

inline void
ProduceMessages(const std::string& topic, int partition, const std::vector<std::tuple<Kafka::Headers, std::string, std::string>>& msgs)
{
    Kafka::KafkaSyncProducer producer(GetKafkaClientCommonConfig());
    producer.setLogLevel(LOG_CRIT);

    for (const auto& msg: msgs)
    {
        auto record = Kafka::ProducerRecord(topic, partition, Kafka::Key(std::get<1>(msg).c_str(), std::get<1>(msg).size()), Kafka::Value(std::get<2>(msg).c_str(), std::get<2>(msg).size()));
        record.headers() = std::get<0>(msg);
        producer.send(record);
    }

    std::cout << "[" << Kafka::Utility::getCurrentTime() << "] " << __FUNCTION__ << ": " << msgs.size() << " messages have been sent." << std::endl;
}

inline void
CreateKafkaTopic(const Kafka::Topic& topic, int numPartitions, int replicationFactor)
{
    Kafka::AdminClient adminClient(GetKafkaClientCommonConfig());
    auto createResult = adminClient.createTopics({topic}, numPartitions, replicationFactor);
    ASSERT_FALSE(createResult.errorCode());
    std::cout << "[" << Kafka::Utility::getCurrentTime() << "] " << __FUNCTION__ << ": topic[" << topic << "] created with numPartitions[" << numPartitions << "], replicationFactor[" << replicationFactor << "]." << std::endl;
    std::this_thread::sleep_for(std::chrono::seconds(1));
}

class JoiningThread {
public:
    template <typename F, typename... Args>
    explicit JoiningThread(F&& f, Args&&... args): _t(f, args...) {}
    ~JoiningThread() { if (_t.joinable()) _t.join(); }
private:
   std::thread _t;
};

inline void
WaitMetadataSyncUpBetweenBrokers()
{
    std::this_thread::sleep_for(std::chrono::seconds(5));
}

inline std::vector<int>
getAllBrokersPids()
{
    std::vector<std::string> pidsString;
    if (getenv("KAFKA_BROKER_PIDS"))
    {
        std::string toSplit = getenv("KAFKA_BROKER_PIDS");
        boost::algorithm::split(pidsString, toSplit, boost::is_any_of(","));
    }

    std::vector<int> pids;
    std::for_each(pidsString.begin(), pidsString.end(), [&pids](const auto& s) { pids.push_back(std::stoi(s)); });
    return pids;
}

inline void
signalToAllBrokers(int sig)
{
    auto pids = getAllBrokersPids();
    std::for_each(pids.begin(), pids.end(), [sig](int pid) { kill(pid, sig); });

    if (sig == SIGSTOP)
    {
        std::cout << "[" << Kafka::Utility::getCurrentTime() << "] Brokers paused"  << std::endl;
    }
    else if (sig == SIGCONT)
    {
        std::cout << "[" << Kafka::Utility::getCurrentTime() << "] Brokers resumed"  << std::endl;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }
}

inline void
PauseBrokers()
{
    signalToAllBrokers(SIGSTOP);
}

inline void
ResumeBrokers()
{
    signalToAllBrokers(SIGCONT);
}

inline std::shared_ptr<JoiningThread>
PauseBrokersForAWhile(std::chrono::milliseconds duration)
{
    PauseBrokers();

    auto cb = [](int ms){ std::this_thread::sleep_for(std::chrono::milliseconds(ms)); ResumeBrokers(); };
    return std::make_shared<JoiningThread>(cb, duration.count());
}

} // end of namespace KafkaTestUtility

