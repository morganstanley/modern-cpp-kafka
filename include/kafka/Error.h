#pragma once

#include "kafka/Project.h"

#include "kafka/RdKafkaHelper.h"

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
 * Unified error type.
 */
class Error
{
public:
    // The error with rich info
    explicit Error(rd_kafka_error_t* error = nullptr): _rkError(error, RkErrorDeleter) {}
    // The error with brief info
    explicit Error(rd_kafka_resp_err_t respErr): _respErr(respErr) {}
    // The error with detailed message
    Error(rd_kafka_resp_err_t respErr, std::string message)
        : _respErr(respErr), _message(std::move(message)) {}
    Error(rd_kafka_resp_err_t respErr, std::string message, bool fatal)
        : _respErr(respErr), _message(std::move(message)), _fatal(fatal) {}

    explicit operator bool() const { return static_cast<bool>(value()); }

    /**
     * Conversion to `std::error_code`
     */
    explicit operator std::error_code() const
    {
        return {value(), ErrorCategory::Global<>::category};
    }

    /**
     * Obtains the underlying error code value.
     *
     * Actually, it's the same as 'rd_kafka_resp_err_t', which is defined by librdkafka.
     * 1. The negative values are for internal errors.
     * 2. Non-negative values are for external errors. See the defination at,
     *    - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
      */
    int             value()        const
    {
        return static_cast<int>(_rkError ? rd_kafka_error_code(_rkError.get()) : _respErr);
    }

    /**
     * Readable error string.
     */
    std::string     message()     const
    {
        return _message ? *_message :
                 (_rkError ? rd_kafka_error_string(_rkError.get()) : rd_kafka_err2str(_respErr));
    }

    /**
     * Fatal error indicates that the client instance is no longer usable.
     */
    Optional<bool>  isFatal()     const
    {
        return  _rkError ? rd_kafka_error_is_fatal(_rkError.get()) : _fatal;
    }

    /**
     * Show whether the operation may be retried.
     */
    Optional<bool>  isRetriable() const
    {
        return _rkError ? rd_kafka_error_is_retriable(_rkError.get()) : Optional<bool>{};
    }

private:
    rd_kafka_error_shared_ptr _rkError;     // For error with rich info
    rd_kafka_resp_err_t       _respErr{};   // For error with a simple response code
    Optional<std::string>     _message;     // Additional detailed message (if any)
    Optional<bool>            _fatal;       // Fatal flag (if any)
};

} // end of KAFKA_API

