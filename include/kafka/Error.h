#pragma once

#include "kafka/Project.h"

#include "librdkafka/rdkafka.h"

#include <string>
#include <system_error>


namespace KAFKA_API {

struct ErrorCategory: public std::error_category
{
    const char* name() const noexcept override { return "KafkaError"; }
    std::string message(int ev) const override { return rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(ev)); }

    template <typename T = void>
    struct Global { static ErrorCategory category; };
};

template <typename T>
ErrorCategory ErrorCategory::Global<T>::category;

/**
 * A utility function which converts an error number to `std::error_code` (with KafkaError category)
 */
inline std::error_code ErrorCode(int errNo = 0)
{
    /**
     * The error code for external interfaces.
     * Actually, it's the same as 'rd_kafka_resp_err_t', which is defined by librdkafka.
     * 1. The negative values are for internal errors.
     * 2. Non-negative values are for external errors. See the defination at,
     *    - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    return {errNo, ErrorCategory::Global<>::category};
}

/**
 * A utility fucntion which converts the `librdkafka`'s internal `rd_kafka_resp_err_t` to `std::error_code` (with KafkaError category)
 */
inline std::error_code ErrorCode(rd_kafka_resp_err_t respErr)
{
    return ErrorCode(static_cast<int>(respErr));
}

/**
 * It would contain detailed message.
 */
struct ErrorWithDetail
{
    ErrorWithDetail(std::error_code code, std::string detailedMsg)
        : error(code), detail(std::move(detailedMsg)) {}

    ErrorWithDetail(rd_kafka_resp_err_t respErr, std::string detailedMsg)
        : ErrorWithDetail(ErrorCode(respErr), std::move(detailedMsg)) {}

    explicit operator bool() const { return static_cast<bool>(error); }

    std::error_code error;
    std::string     detail;
};

} // end of KAFKA_API

