#include "kafka/ProducerRecord.h"

#include "gtest/gtest.h"

namespace Kafka = KAFKA_API;

TEST(ProducerRecord, WithSpecificPartition)
{
    Kafka::Topic topic = "topic1";
    Kafka::Partition partition = 1;
    std::string keyStr = "key1";
    Kafka::Key key = Kafka::Key(keyStr.c_str(), keyStr.size());
    std::string payload = "hello world";
    Kafka::Value value = Kafka::Value(payload.c_str(), payload.size());

    Kafka::ProducerRecord record(topic, partition, key, value);
    std::string headerValue1 = "hv1";
    std::string headerValue2 = "hv2";
    record.headers().emplace_back("hk1", Kafka::Header::Value(headerValue1.c_str(), headerValue1.size()));
    record.headers().emplace_back("hk2", Kafka::Header::Value(headerValue2.c_str(), headerValue2.size()));

    EXPECT_EQ("topic1-1: headers[hk1:hv1,hk2:hv2], key1/hello world", record.toString());

    std::string payload2 = "it has been changed";
    record.setValue(Kafka::Value(payload2.c_str(), payload2.size()));
    record.headers().clear();

    EXPECT_EQ("topic1-1: key1/it has been changed", record.toString());
}

TEST(ProducerRecord, WithNoSpecificPartition)
{
    Kafka::Topic topic = "topic1";
    std::string keyStr = "key1";
    Kafka::Key key = Kafka::Key(keyStr.c_str(), keyStr.size());
    std::string payload = "hello world";
    Kafka::Value value = Kafka::Value(payload.c_str(), payload.size());

    Kafka::ProducerRecord::Id id = 1000;
    Kafka::ProducerRecord record(topic, key, value, id);

    EXPECT_EQ("topic1-NA:1000, key1/hello world", record.toString());
}

