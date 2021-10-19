#include "kafka/KafkaException.h"

#include "gtest/gtest.h"

#include <regex>


#define EXPECT_KAFKA_THROW(expr, err)           \
    do {                                        \
        try {                                   \
            expr;                               \
        } catch (const KafkaException& e) {     \
            EXPECT_EQ(err, e.error().value());  \
            break;                              \
        } catch (...){                          \
        }                                       \
        EXPECT_FALSE(true);                     \
    } while(false)

#define EXPECT_KAFKA_NO_THROW(expr)   \
    try {                             \
        expr;                         \
    } catch (...){                    \
        EXPECT_FALSE(true);           \
    }


TEST(KafkaException, Basic)
{
    // Try KafkaException with no specified error message
    try
    {
        throw kafka::KafkaException("some_filename", 100, kafka::Error{RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT});
        EXPECT_FALSE(true);
    }
    catch (const kafka::KafkaException& e)
    {
        std::cout << e.what() << std::endl;
        std::regex reMatch(R"(.*Broker: Request timed out \[7\] \(some_filename:100\))");
        EXPECT_TRUE(std::regex_match(e.what(), reMatch));
    }

    // Try KafkaException with specified error message
    try
    {
        throw kafka::KafkaException("some_filename", 100, kafka::Error{RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, "something wrong here"});
        EXPECT_FALSE(true);
    }
    catch (const kafka::KafkaException& e)
    {
        std::cout << e.what() << std::endl;
        std::regex reMatch(R"(.*Broker: Request timed out \[7\] \| something wrong here \(some_filename:100\))");
        EXPECT_TRUE(std::regex_match(e.what(), reMatch));
    }
}


TEST(KafkaException, Macros)
{
    using kafka::KafkaException;

    // Try KAFKA_THROW_IF_WITH_ERROR (with no error)
    EXPECT_KAFKA_NO_THROW(KAFKA_THROW_IF_WITH_ERROR(kafka::Error(RD_KAFKA_RESP_ERR_NO_ERROR)));

    // Try KAFKA_THROW_IF_WITH_ERROR (with error)
    EXPECT_KAFKA_THROW(KAFKA_THROW_IF_WITH_ERROR(kafka::Error(RD_KAFKA_RESP_ERR__TIMED_OUT)), RD_KAFKA_RESP_ERR__TIMED_OUT);

    // Try KAFKA_THROW_ERROR (with no specified error message)
    try
    {
        KAFKA_THROW_IF_WITH_ERROR(kafka::Error(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT));
        EXPECT_FALSE(true);
    }
    catch (const kafka::KafkaException& e)
    {
        std::regex reMatch(".*Broker: Request timed out.*TestKafkaException\\.cc.*");
        EXPECT_TRUE(std::regex_match(e.what(), reMatch));
    }

    // Try KAFKA_THROW_ERROR (with specified error message)
    try
    {
        KAFKA_THROW_ERROR(kafka::Error(RD_KAFKA_RESP_ERR_REQUEST_TIMED_OUT, "something wrong here"));
        EXPECT_FALSE(true);
    }
    catch (const kafka::KafkaException& e)
    {
        std::regex reMatch(".*something wrong here.*TestKafkaException\\.cc.*");
        EXPECT_TRUE(std::regex_match(e.what(), reMatch));
    }
}

