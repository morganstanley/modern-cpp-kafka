#include "kafka/Error.h"

#include "librdkafka/rdkafka.h"

#include "gtest/gtest.h"

#include <sstream>

namespace Kafka = KAFKA_API;

namespace {

std::string getStringFromStream(std::error_code errorCode)
{
    std::ostringstream oss;
    oss << errorCode;
    return oss.str();
}

} // end of namespace

TEST(Error, ErrorCode)
{
    std::error_code defaultError = Kafka::ErrorCode();
    EXPECT_EQ(Kafka::ErrorCode(RD_KAFKA_RESP_ERR_NO_ERROR), defaultError);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, defaultError.value());
    EXPECT_EQ("KafkaError:0", getStringFromStream(defaultError));
    EXPECT_EQ("Success", defaultError.message());

    std::error_code noError = Kafka::ErrorCode(RD_KAFKA_RESP_ERR_NO_ERROR);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, noError.value());
    EXPECT_EQ("KafkaError:0", getStringFromStream(noError));
    EXPECT_EQ("Success", noError.message());

    std::error_code localError = Kafka::ErrorCode(RD_KAFKA_RESP_ERR__TIMED_OUT);
    EXPECT_EQ(RD_KAFKA_RESP_ERR__TIMED_OUT, localError.value());
    EXPECT_EQ("KafkaError:-185", getStringFromStream(localError));
    EXPECT_EQ("Local: Timed out", localError.message());

    std::error_code brokerError = Kafka::ErrorCode(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, brokerError.value());
    EXPECT_EQ("KafkaError:7", getStringFromStream(brokerError));
    EXPECT_EQ("Broker: Request timed out", brokerError.message());
}

