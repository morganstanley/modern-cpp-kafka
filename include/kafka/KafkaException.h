#pragma once

#include "kafka/Project.h"

#include "kafka/Error.h"
#include "kafka/Utility.h"

#include "librdkafka/rdkafka.h"

#include <exception>
#include <string>


namespace KAFKA_API {

/**
 * Specific exception for Kafka clients.
 */
class KafkaException: public std::exception
{
public:
    KafkaException(const char* filename, int lineno, std::error_code ec, const std::string& errMsg)
        : _errCode(ec)
    {
        _errMsg = Utility::getCurrentTime() + ": " + errMsg + " [" + std::to_string(ec.value())
                  + "] (" + std::string(filename) + ":" + std::to_string(lineno) + ")";
    }

    KafkaException(const char* filename, int lineno, std::error_code ec)
        : KafkaException(filename, lineno, ec, ec.message())
    {}

    /**
     * Obtains the underlying error code.
     */
    std::error_code error() const { return _errCode; }

    /**
     * Obtains explanatory string.
     */
    const char* what() const noexcept override { return _errMsg.c_str(); }

private:
    std::string     _errMsg;
    std::error_code _errCode;
};

#define KAFKA_THROW(respErr)                throw KafkaException(__FILE__, __LINE__, ErrorCode(respErr))
#define KAFKA_THROW_WITH_MSG(respErr, ...)  throw KafkaException(__FILE__, __LINE__, ErrorCode(respErr), __VA_ARGS__)
#define KAFKA_THROW_IF_WITH_ERROR(respErr)  if (respErr != RD_KAFKA_RESP_ERR_NO_ERROR) KAFKA_THROW(respErr)

} // end of KAFKA_API

