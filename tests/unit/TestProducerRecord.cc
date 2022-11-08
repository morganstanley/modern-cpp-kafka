#include "kafka/ProducerRecord.h"

#include "gtest/gtest.h"


TEST(ProducerRecord, WithSpecificPartition)
{
    const kafka::Topic topic = "topic1";
    const kafka::Partition partition = 1;
    const std::string keyStr = "key1";
    const kafka::Key key = kafka::Key(keyStr.c_str(), keyStr.size());
    const std::string payload = "hello world";
    const kafka::Value value = kafka::Value(payload.c_str(), payload.size());

    kafka::clients::producer::ProducerRecord record(topic, partition, key, value);
    const std::string headerValue1 = "hv1";
    const std::string headerValue2 = "hv2";
    record.headers().emplace_back("hk1", kafka::Header::Value(headerValue1.c_str(), headerValue1.size()));
    record.headers().emplace_back("hk2", kafka::Header::Value(headerValue2.c_str(), headerValue2.size()));

    EXPECT_EQ("topic1-1: headers[hk1:hv1,hk2:hv2], key1/hello world", record.toString());

    const std::string payload2 = "it has been changed";
    record.setValue(kafka::Value(payload2.c_str(), payload2.size()));
    record.headers().clear();

    EXPECT_EQ("topic1-1: key1/it has been changed", record.toString());
}

TEST(ProducerRecord, WithNoSpecificPartition)
{
    const kafka::Topic topic = "topic1";
    const std::string keyStr = "key1";
    const kafka::Key key = kafka::Key(keyStr.c_str(), keyStr.size());
    const std::string payload = "hello world";
    const kafka::Value value = kafka::Value(payload.c_str(), payload.size());

    const kafka::clients::producer::ProducerRecord::Id id = 1000;
    const kafka::clients::producer::ProducerRecord record(topic, key, value, id);

    EXPECT_EQ("topic1-NA:1000, key1/hello world", record.toString());
}

