#include "kafka/Error.h"

#include "librdkafka/rdkafka.h"

#include "gtest/gtest.h"

#include <sstream>


namespace {

std::string getStringFromStream(std::error_code ec)
{
    std::ostringstream oss;
    oss << ec;
    return oss.str();
}

std::error_code getErrorCode(int errNo = 0)
{
    return {errNo, kafka::ErrorCategory::Global<>::category};
}


} // end of namespace

TEST(Error, ErrorCode)
{
    const std::error_code defaultEc = getErrorCode();
    EXPECT_EQ(getErrorCode(RD_KAFKA_RESP_ERR_NO_ERROR), defaultEc);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, defaultEc.value());
    EXPECT_EQ("KafkaError:0", getStringFromStream(defaultEc));
    EXPECT_EQ("Success", defaultEc.message());

    const std::error_code noEc = getErrorCode(RD_KAFKA_RESP_ERR_NO_ERROR);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, noEc.value());
    EXPECT_EQ("KafkaError:0", getStringFromStream(noEc));
    EXPECT_EQ("Success", noEc.message());

    const std::error_code localEc = getErrorCode(RD_KAFKA_RESP_ERR__TIMED_OUT);
    EXPECT_EQ(RD_KAFKA_RESP_ERR__TIMED_OUT, localEc.value());
    EXPECT_EQ("KafkaError:-185", getStringFromStream(localEc));
    EXPECT_EQ("Local: Timed out", localEc.message());

    const std::error_code brokerEc = getErrorCode(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT);
    EXPECT_EQ(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, brokerEc.value());
    EXPECT_EQ("KafkaError:7", getStringFromStream(brokerEc));
    EXPECT_EQ("Broker: Request timed out", brokerEc.message());
}

TEST(Error, KafkaError)
{
    const auto defaultError = kafka::Error{};
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, defaultError.value());
    EXPECT_EQ("Success", defaultError.message());
    EXPECT_EQ(getErrorCode(), static_cast<std::error_code>(defaultError));

    const auto noError = kafka::Error{RD_KAFKA_RESP_ERR_NO_ERROR};
    EXPECT_EQ(RD_KAFKA_RESP_ERR_NO_ERROR, noError.value());
    EXPECT_EQ("Success", noError.message());
    EXPECT_EQ(getErrorCode(0), static_cast<std::error_code>(noError));

    const auto localError = kafka::Error{RD_KAFKA_RESP_ERR__TIMED_OUT};
    EXPECT_EQ(RD_KAFKA_RESP_ERR__TIMED_OUT, localError.value());
    EXPECT_EQ("Local: Timed out", localError.message());
    EXPECT_EQ(getErrorCode(RD_KAFKA_RESP_ERR__TIMED_OUT), static_cast<std::error_code>(localError));

    const auto  brokerError = kafka::Error{RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT};
    EXPECT_EQ(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, brokerError.value());
    EXPECT_EQ("Broker: Request timed out", brokerError.message());
    EXPECT_EQ(getErrorCode(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT), static_cast<std::error_code>(brokerError));
}

