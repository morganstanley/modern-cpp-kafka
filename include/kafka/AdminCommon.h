#pragma once

#include "kafka/Project.h"

#include "kafka/Error.h"
#include "kafka/Types.h"


namespace KAFKA_API::clients::admin {

/**
 * The result of AdminClient::createTopics().
 */
struct CreateTopicsResult
{
    explicit CreateTopicsResult(Error err): error(std::move(err)) {}

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
    explicit DeleteTopicsResult(Error err): error(std::move(err)) {}

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
    explicit DeleteRecordsResult(Error err): error(std::move(err)) {}

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
    explicit ListTopicsResult(Error err):    error(std::move(err))    {}
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

} // end of KAFKA_API::clients::admin

