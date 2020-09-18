#include "kafka/Types.h"

#include "gtest/gtest.h"

namespace Kafka = KAFKA_API;

TEST(Types, Topics)
{
    Kafka::Topics emptyTopics;
    EXPECT_EQ("", Kafka::toString(emptyTopics));

    Kafka::Topics topics = {"topic1", "topic2"};
    EXPECT_EQ("topic1,topic2", Kafka::toString(topics));
}

TEST(Types, TopicPartition)
{
    Kafka::TopicPartition topicPartition = {"topic1", 0};
    EXPECT_EQ("topic1-0", Kafka::toString(topicPartition));
}

TEST(Types, TopicPartitions)
{
    Kafka::TopicPartitions topicPartitions = {{"topic1", 1}, {"topic2", 2}};
    EXPECT_EQ("topic1-1,topic2-2", Kafka::toString(topicPartitions));
}


TEST(Types, TopicPartitionOffsets)
{
    Kafka::TopicPartitionOffsets tpos = {{{"topic1", 1}, 10}, {{"topic2", 2}, 20}};
    EXPECT_EQ("topic1-1:10,topic2-2:20", Kafka::toString(tpos));
}

TEST(Types, ConstBuffer)
{
    std::string str = "hello world";
    Kafka::ConstBuffer strBuf(str.c_str(), str.size());
    EXPECT_EQ(str.c_str(), strBuf.data());
    EXPECT_EQ(str.size(), strBuf.size());
    EXPECT_EQ("hello world", strBuf.toString());

    char nonPrintable[] = "hello\0world";
    Kafka::ConstBuffer nonPrintableBuf(nonPrintable, sizeof(nonPrintable));
    EXPECT_EQ(nonPrintable, nonPrintableBuf.data());
    EXPECT_EQ(sizeof(nonPrintable), nonPrintableBuf.size());
    EXPECT_EQ("hello[0x00]world[0x00]", nonPrintableBuf.toString());
}

