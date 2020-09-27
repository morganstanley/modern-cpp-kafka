#include "kafka/ConsumerRecord.h"

#include "gtest/gtest.h"

#include <cstring>

namespace Kafka = KAFKA_API;

namespace {

inline rd_kafka_message_t* mockRdKafkaMessage(Kafka::Partition partition, Kafka::Offset offset,
                                              const std::string& key, const std::string& value,
                                              rd_kafka_resp_err_t respErr = RD_KAFKA_RESP_ERR_NO_ERROR)
{
    constexpr std::size_t MSG_PRIVATE_LEN = 128;    // the underlying `rk_kafka_msg_t` is longer than `rk_kafka_message_t`
    std::size_t msgSize = sizeof(rd_kafka_message_t) + MSG_PRIVATE_LEN + key.size() + 1 + value.size() + 1;

    void* memBlock = std::malloc(msgSize);
    auto* rkMsg = static_cast<rd_kafka_message_t*>(memBlock);
    char* msgBuf = static_cast<char*>(memBlock);
    std::memset(msgBuf, 0, msgSize);

    char *keyBuf = msgBuf + sizeof(rd_kafka_message_t) + MSG_PRIVATE_LEN;
    std::memcpy(keyBuf, key.c_str(), key.size() + 1);
    rkMsg->key     = keyBuf;
    rkMsg->key_len = key.size();

    char *payloadBuf = keyBuf + key.size() + 1;
    std::memcpy(payloadBuf, value.c_str(), value.size() + 1);
    rkMsg->payload = payloadBuf;
    rkMsg->len     = value.size();

    rkMsg->partition = partition;
    rkMsg->offset = offset;
    rkMsg->err = respErr;

    return rkMsg;
}

} // end of namespace


TEST(ConsumerRecord, Basic)
{
    Kafka::Partition partition = 1;
    Kafka::Offset    offset    = 100;
    std::string      key       = "some key";
    std::string      value     = "some value";

    rd_kafka_message_t* rkMsg = mockRdKafkaMessage(partition, offset, key, value);
    // Here the ConsumerRecord will take over the ownership
    Kafka::ConsumerRecord record(rkMsg);

    EXPECT_FALSE(record.error());
    EXPECT_EQ(partition, record.partition());
    EXPECT_EQ(offset, record.offset());
    EXPECT_EQ(key, std::string(static_cast<const char *>(record.key().data()), record.key().size()));
    EXPECT_EQ(value, std::string(static_cast<const char *>(record.value().data()), record.value().size()));
}

TEST(ConsumerRecord, WithError)
{
    Kafka::Partition partition = 2;
    Kafka::Offset    offset    = 200;
    rd_kafka_resp_err_t err    = RD_KAFKA_RESP_ERR_UNKNOWN;

    rd_kafka_message_t* rkMsg = mockRdKafkaMessage(partition, offset, "", "", err);
    // Here the ConsumerRecord will take over the ownership
    Kafka::ConsumerRecord record(rkMsg);

    EXPECT_EQ(RD_KAFKA_RESP_ERR_UNKNOWN, record.error().value());
    EXPECT_EQ(partition, record.partition());
    EXPECT_EQ(offset, record.offset());

    EXPECT_EQ("ERROR[Unknown broker error, -2:200]", record.toString());
}

TEST(ConsumerRecord, EndOfPartition)
{
    Kafka::Partition    partition = 1;
    Kafka::Offset       offset    = 100;
    rd_kafka_resp_err_t err       = RD_KAFKA_RESP_ERR__PARTITION_EOF;

    rd_kafka_message_t* rkMsg = mockRdKafkaMessage(partition, offset, "", "", err);
    // Here the ConsumerRecord will take over the ownership
    Kafka::ConsumerRecord record(rkMsg);

    EXPECT_EQ(RD_KAFKA_RESP_ERR__PARTITION_EOF, record.error().value());
    EXPECT_EQ(partition, record.partition());
    EXPECT_EQ(offset, record.offset());

    EXPECT_EQ("EOF[-1:100]", record.toString());
}

