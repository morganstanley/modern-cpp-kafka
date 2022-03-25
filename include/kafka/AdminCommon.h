#pragma once

#include <kafka/Project.h>

#include <kafka/Error.h>
#include <kafka/Types.h>


namespace KAFKA_API { namespace clients { namespace admin {

/**
 * The result of AdminClient::createTopics().
 */
struct CreateTopicsResult
{
    explicit CreateTopicsResult(const Error& err): error(err) {}

    /**
     * The result error.
     */
    Error error;
};

/**
 * The result of AdminClient::deleteTopics().
 */
struct DeleteTopicsResult
{
    explicit DeleteTopicsResult(const Error& err): error(err) {}

    /**
     * The result error.
     */
    Error error;
};

/**
 * The result of AdminClient::deleteRecords().
 */
struct DeleteRecordsResult
{
    explicit DeleteRecordsResult(const Error& err): error(err) {}

    /**
     * The result error.
     */
    Error error;
};

/**
 * The result of AdminClient::listTopics().
 */
struct ListTopicsResult
{
    explicit ListTopicsResult(const Error& err): error(err) {}
    explicit ListTopicsResult(Topics names): topics(std::move(names)) {}

    /**
     * The result error.
     */
    Error  error;

    /**
     * The topics fetched.
     */
    Topics topics;
};

} } } // end of KAFKA_API::clients::admin

