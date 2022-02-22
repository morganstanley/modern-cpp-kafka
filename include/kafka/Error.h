#pragma once

#include <kafka/Project.h>

#include <kafka/RdKafkaHelper.h>

#include <librdkafka/rdkafka.h>

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
    Error(rd_kafka_resp_err_t respErr, std::string message, bool fatal = false)
        : _respErr(respErr), _message(std::move(message)), _isFatal(fatal) {}
    // Copy constructor
    Error(const Error& error) { *this = error; }

    // Assignment operator
    Error& operator=(const Error& error)
    {
        if (this == &error) return *this;

        _rkError.reset();

        _respErr          = static_cast<rd_kafka_resp_err_t>(error.value());
        _message          = error._message;
        _isFatal          = error.isFatal();
        _txnRequiresAbort = error.transactionRequiresAbort();
        _isRetriable      = error.isRetriable();

        return *this;
    }

    /**
     * Check if the error is valid.
     */
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
     * Detailed error string.
     */
    std::string     toString()     const
    {
        std::ostringstream oss;

        oss << rd_kafka_err2str(static_cast<rd_kafka_resp_err_t>(value())) << " [" << value() << "]" << (isFatal() ? " fatal" : "");
        if (transactionRequiresAbort())     oss << " | transaction-requires-abort";
        if (auto retriable = isRetriable()) oss << " | " << (*retriable ? "retriable" : "non-retriable");
        if (_message)                       oss << " | " << *_message;

        return oss.str();
    }

    /**
     * Fatal error indicates that the client instance is no longer usable.
     */
    bool            isFatal()     const
    {
        return  _rkError ? rd_kafka_error_is_fatal(_rkError.get()) : _isFatal;
    }

    /**
     * Show whether the operation may be retried.
     */
    Optional<bool>  isRetriable() const
    {
        return _rkError ? rd_kafka_error_is_retriable(_rkError.get()) : _isRetriable;
    }

    /**
     * Show whether the error is an abortable transaction error.
     *
     * Note:
     * 1. Only valid for transactional API.
     * 2. If `true`, the producer must call `abortTransaction` and start a new transaction with `beginTransaction` to proceed with transactions.
     */
    bool transactionRequiresAbort() const
    {
        return _rkError ? rd_kafka_error_txn_requires_abort(_rkError.get()) : false;
    }

private:
    rd_kafka_error_shared_ptr _rkError;     // For error with rich info
    rd_kafka_resp_err_t       _respErr{};   // For error with a simple response code
    Optional<std::string>     _message;     // Additional detailed message (if any)
    bool                      _isFatal          = false;
    bool                      _txnRequiresAbort = false;
    Optional<bool>            _isRetriable; // Retriable flag (if any)
};

} // end of KAFKA_API

