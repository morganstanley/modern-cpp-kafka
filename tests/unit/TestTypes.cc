#include "kafka/Types.h"

#include "gtest/gtest.h"


TEST(Types, Topics)
{
    const kafka::Topics emptyTopics;
    EXPECT_EQ("", kafka::toString(emptyTopics));

    const kafka::Topics topics = {"topic1", "topic2"};
    EXPECT_EQ("topic1,topic2", kafka::toString(topics));
}

TEST(Types, TopicPartition)
{
    const kafka::TopicPartition topicPartition = {"topic1", 0};
    EXPECT_EQ("topic1-0", kafka::toString(topicPartition));
}

TEST(Types, TopicPartitions)
{
    const kafka::TopicPartitions topicPartitions = {{"topic1", 1}, {"topic2", 2}};
    EXPECT_EQ("topic1-1,topic2-2", kafka::toString(topicPartitions));
}


TEST(Types, TopicPartitionOffsets)
{
    const kafka::TopicPartitionOffsets tpos = {{{"topic1", 1}, 10}, {{"topic2", 2}, 20}};
    EXPECT_EQ("topic1-1:10,topic2-2:20", kafka::toString(tpos));
}

TEST(Types, ConstBuffer)
{
    const std::string str = "hello world";
    const kafka::ConstBuffer strBuf(str.c_str(), str.size());
    EXPECT_EQ(str.c_str(), strBuf.data());
    EXPECT_EQ(str.size(), strBuf.size());
    EXPECT_EQ("hello world", strBuf.toString());

    const std::string nonPrintable({'h', 'e', 'l', 'l', 'o', 0, 'w', 'o', 'r', 'l', 'd'});
    const kafka::ConstBuffer nonPrintableBuf(nonPrintable.data(), nonPrintable.size());
    EXPECT_EQ(nonPrintable.data(), nonPrintableBuf.data());
    EXPECT_EQ(nonPrintable.size(), nonPrintableBuf.size());
    EXPECT_EQ("hello[0x00]world", nonPrintableBuf.toString());
}

