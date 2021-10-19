#include "kafka/Types.h"

#include "gtest/gtest.h"


TEST(Types, Topics)
{
    kafka::Topics emptyTopics;
    EXPECT_EQ("", kafka::toString(emptyTopics));

    kafka::Topics topics = {"topic1", "topic2"};
    EXPECT_EQ("topic1,topic2", kafka::toString(topics));
}

TEST(Types, TopicPartition)
{
    kafka::TopicPartition topicPartition = {"topic1", 0};
    EXPECT_EQ("topic1-0", kafka::toString(topicPartition));
}

TEST(Types, TopicPartitions)
{
    kafka::TopicPartitions topicPartitions = {{"topic1", 1}, {"topic2", 2}};
    EXPECT_EQ("topic1-1,topic2-2", kafka::toString(topicPartitions));
}


TEST(Types, TopicPartitionOffsets)
{
    kafka::TopicPartitionOffsets tpos = {{{"topic1", 1}, 10}, {{"topic2", 2}, 20}};
    EXPECT_EQ("topic1-1:10,topic2-2:20", kafka::toString(tpos));
}

TEST(Types, ConstBuffer)
{
    std::string str = "hello world";
    kafka::ConstBuffer strBuf(str.c_str(), str.size());
    EXPECT_EQ(str.c_str(), strBuf.data());
    EXPECT_EQ(str.size(), strBuf.size());
    EXPECT_EQ("hello world", strBuf.toString());

    char nonPrintable[] = "hello\0world";
    kafka::ConstBuffer nonPrintableBuf(nonPrintable, sizeof(nonPrintable));
    EXPECT_EQ(nonPrintable, nonPrintableBuf.data());
    EXPECT_EQ(sizeof(nonPrintable), nonPrintableBuf.size());
    EXPECT_EQ("hello[0x00]world[0x00]", nonPrintableBuf.toString());
}

