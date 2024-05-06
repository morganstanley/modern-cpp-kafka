#pragma once

#include <kafka/Project.h>

#include <kafka/AdminClientConfig.h>
#include <kafka/AdminCommon.h>
#include <kafka/Error.h>
#include <kafka/KafkaClient.h>
#include <kafka/RdKafkaHelper.h>

#include <librdkafka/rdkafka.h>

#include <array>
#include <cassert>
#include <list>
#include <memory>
#include <thread>
#include <vector>


namespace KAFKA_API { namespace clients { namespace admin {

/**
 * The administrative client for Kafka, which supports managing and inspecting topics, etc.
 */
class AdminClient: public KafkaClient
{
public:
    explicit AdminClient(const Properties& properties)
        : KafkaClient(ClientType::AdminClient, KafkaClient::validateAndReformProperties(properties))
    {
    }

    /**
     * Create a batch of new topics.
     */
    admin::CreateTopicsResult createTopics(const Topics&             topics,
                                           int                       numPartitions,
                                           int                       replicationFactor,
                                           const Properties&         topicConfig = Properties(),
                                           std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_COMMAND_TIMEOUT_MS));
    /**
     * Delete a batch of topics.
     */
    admin::DeleteTopicsResult deleteTopics(const Topics&             topics,
                                           std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_COMMAND_TIMEOUT_MS));
    /**
     * List the topics available in the cluster.
     */
    admin::ListTopicsResult   listTopics(std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_COMMAND_TIMEOUT_MS));

    /**
     * Delete records whose offset is smaller than the given offset of the corresponding partition.
     * @param topicPartitionOffsets a batch of offsets for partitions
     * @param timeout
     * @return
     */
    admin::DeleteRecordsResult deleteRecords(const TopicPartitionOffsets& topicPartitionOffsets,
                                             std::chrono::milliseconds    timeout = std::chrono::milliseconds(DEFAULT_COMMAND_TIMEOUT_MS));

private:
    static std::list<Error> getPerTopicResults(const rd_kafka_topic_result_t** topicResults, std::size_t topicCount);
    static std::list<Error> getPerTopicPartitionResults(const rd_kafka_topic_partition_list_t* partitionResults);
    static Error combineErrors(const std::list<Error>& errors);

#if COMPILER_SUPPORTS_CPP_17
    static constexpr int DEFAULT_COMMAND_TIMEOUT_MS = 30000;
#else
    enum { DEFAULT_COMMAND_TIMEOUT_MS = 30000 };
#endif
};


inline std::list<Error>
AdminClient::getPerTopicResults(const rd_kafka_topic_result_t** topicResults, std::size_t topicCount)
{
    std::list<Error> errors;

    for (std::size_t i = 0; i < topicCount; ++i)
    {
        const rd_kafka_topic_result_t* topicResult = topicResults[i];
        if (const rd_kafka_resp_err_t topicError = rd_kafka_topic_result_error(topicResult))
        {
            const std::string detailedMsg = "topic[" + std::string(rd_kafka_topic_result_name(topicResult)) + "] with error[" + rd_kafka_topic_result_error_string(topicResult) + "]";
            errors.emplace_back(topicError, detailedMsg);
        }
    }
    return errors;
}

inline std::list<Error>
AdminClient::getPerTopicPartitionResults(const rd_kafka_topic_partition_list_t* partitionResults)
{
    std::list<Error> errors;

    for (int i = 0; i < (partitionResults ? partitionResults->cnt : 0); ++i)
    {
        if (const rd_kafka_resp_err_t partitionError = partitionResults->elems[i].err)
        {
            const std::string detailedMsg = "topic-partition[" + std::string(partitionResults->elems[i].topic) + "-" + std::to_string(partitionResults->elems[i].partition) + "] with error[" + rd_kafka_err2str(partitionError) + "]";
            errors.emplace_back(partitionError, detailedMsg);
        }
    }
    return errors;
}

inline Error
AdminClient::combineErrors(const std::list<Error>& errors)
{
    if (!errors.empty())
    {
        std::string detailedMsg;
        std::for_each(errors.cbegin(), errors.cend(),
                      [&detailedMsg](const auto& error) {
                          if (!detailedMsg.empty()) detailedMsg += "; ";

                          detailedMsg += error.message();
                      });

        return  Error{static_cast<rd_kafka_resp_err_t>(errors.front().value()), detailedMsg};
    }

    return Error{RD_KAFKA_RESP_ERR_NO_ERROR, "Success"};
}

inline admin::CreateTopicsResult
AdminClient::createTopics(const Topics&             topics,
                          int                       numPartitions,
                          int                       replicationFactor,
                          const Properties&         topicConfig,
                          std::chrono::milliseconds timeout)
{
    LogBuffer<500> errInfo;

    std::vector<rd_kafka_NewTopic_unique_ptr> rkNewTopics;

    for (const auto& topic: topics)
    {
        rkNewTopics.emplace_back(rd_kafka_NewTopic_new(topic.c_str(), numPartitions, replicationFactor, errInfo.str(), errInfo.capacity()));
        if (!rkNewTopics.back())
        {
            return admin::CreateTopicsResult(Error{RD_KAFKA_RESP_ERR__INVALID_ARG, rd_kafka_err2str(RD_KAFKA_RESP_ERR__INVALID_ARG)});
        }

        for (const auto& conf: topicConfig.map())
        {
            const auto& k = conf.first;
            const auto& v = topicConfig.getProperty(k);
            if (!v) continue;

            const rd_kafka_resp_err_t err = rd_kafka_NewTopic_set_config(rkNewTopics.back().get(), k.c_str(), v->c_str());
            if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
            {
                const std::string errMsg = "Invalid config[" + k + "=" + *v + "]";
                KAFKA_API_DO_LOG(Log::Level::Err, errMsg.c_str());
                return admin::CreateTopicsResult(Error{RD_KAFKA_RESP_ERR__INVALID_ARG, errMsg});
            }
        }
    }

    std::vector<rd_kafka_NewTopic_t*> rk_topics;
    rk_topics.reserve(rkNewTopics.size());
    for (const auto& topic : rkNewTopics) { rk_topics.emplace_back(topic.get()); }

    auto rk_queue = rd_kafka_queue_unique_ptr(rd_kafka_queue_new(getClientHandle()));

    rd_kafka_CreateTopics(getClientHandle(), rk_topics.data(), rk_topics.size(), nullptr, rk_queue.get());

    auto rk_ev = rd_kafka_event_unique_ptr();

    const auto end = std::chrono::steady_clock::now() + timeout;
    do
    {
        rk_ev.reset(rd_kafka_queue_poll(rk_queue.get(), EVENT_POLLING_INTERVAL_MS));

        if (rd_kafka_event_type(rk_ev.get()) == RD_KAFKA_EVENT_CREATETOPICS_RESULT) break;

        if (rk_ev)
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "rd_kafka_queue_poll got event[%s], with error[%s]", rd_kafka_event_name(rk_ev.get()), rd_kafka_event_error_string(rk_ev.get()));
            rk_ev.reset();
        }
    } while (std::chrono::steady_clock::now() < end);

    if (!rk_ev)
    {
        return admin::CreateTopicsResult(Error{RD_KAFKA_RESP_ERR__TIMED_OUT, "No response within the time limit"});
    }

    std::list<Error> errors;

    if (const rd_kafka_resp_err_t respErr = rd_kafka_event_error(rk_ev.get()))
    {
        errors.emplace_back(respErr, rd_kafka_event_error_string(rk_ev.get()));
    }

    // Fetch per-topic results
    const rd_kafka_CreateTopics_result_t* res = rd_kafka_event_CreateTopics_result(rk_ev.get());
    std::size_t res_topic_cnt{};
    const rd_kafka_topic_result_t** res_topics = rd_kafka_CreateTopics_result_topics(res, &res_topic_cnt);

    errors.splice(errors.end(), getPerTopicResults(res_topics, res_topic_cnt));

    // Return the error if any
    if (!errors.empty())
    {
        return admin::CreateTopicsResult{combineErrors(errors)};
    }

    // Update metedata
    do
    {
        auto listResult = listTopics();
        if (!listResult.error)
        {
            return admin::CreateTopicsResult(Error{RD_KAFKA_RESP_ERR_NO_ERROR, "Success"});
        }
    } while (std::chrono::steady_clock::now() < end);

    return admin::CreateTopicsResult(Error{RD_KAFKA_RESP_ERR__TIMED_OUT, "Updating metadata timed out"});
}

inline admin::DeleteTopicsResult
AdminClient::deleteTopics(const Topics& topics, std::chrono::milliseconds timeout)
{
    std::vector<rd_kafka_DeleteTopic_unique_ptr> rkDeleteTopics;

    for (const auto& topic: topics)
    {
        rkDeleteTopics.emplace_back(rd_kafka_DeleteTopic_new(topic.c_str()));
        assert(rkDeleteTopics.back());
    }

    std::vector<rd_kafka_DeleteTopic_t*> rk_topics;
    rk_topics.reserve(rkDeleteTopics.size());
    for (const auto& topic : rkDeleteTopics) { rk_topics.emplace_back(topic.get()); }

    auto rk_queue = rd_kafka_queue_unique_ptr(rd_kafka_queue_new(getClientHandle()));

    rd_kafka_DeleteTopics(getClientHandle(), rk_topics.data(), rk_topics.size(), nullptr, rk_queue.get());

    auto rk_ev = rd_kafka_event_unique_ptr();

    const auto end = std::chrono::steady_clock::now() + timeout;
    do
    {
        rk_ev.reset(rd_kafka_queue_poll(rk_queue.get(), EVENT_POLLING_INTERVAL_MS));

        if (rd_kafka_event_type(rk_ev.get()) == RD_KAFKA_EVENT_DELETETOPICS_RESULT) break;

        if (rk_ev)
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "rd_kafka_queue_poll got event[%s], with error[%s]", rd_kafka_event_name(rk_ev.get()), rd_kafka_event_error_string(rk_ev.get()));
            rk_ev.reset();
        }
    } while (std::chrono::steady_clock::now() < end);

    if (!rk_ev)
    {
        return admin::DeleteTopicsResult(Error{RD_KAFKA_RESP_ERR__TIMED_OUT, "No response within the time limit"});
    }

    std::list<Error> errors;

    if (const rd_kafka_resp_err_t respErr = rd_kafka_event_error(rk_ev.get()))
    {
        errors.emplace_back(respErr, rd_kafka_event_error_string(rk_ev.get()));
    }

    // Fetch per-topic results
    const rd_kafka_DeleteTopics_result_t* res = rd_kafka_event_DeleteTopics_result(rk_ev.get());
    std::size_t res_topic_cnt{};
    const rd_kafka_topic_result_t** res_topics = rd_kafka_DeleteTopics_result_topics(res, &res_topic_cnt);

    errors.splice(errors.end(), getPerTopicResults(res_topics, res_topic_cnt));

    return admin::DeleteTopicsResult(combineErrors(errors));
}

inline admin::ListTopicsResult
AdminClient::listTopics(std::chrono::milliseconds timeout)
{
    const rd_kafka_metadata_t* rk_metadata = nullptr;
    const rd_kafka_resp_err_t err = rd_kafka_metadata(getClientHandle(), true, nullptr, &rk_metadata, convertMsDurationToInt(timeout));
    auto guard = rd_kafka_metadata_unique_ptr(rk_metadata);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        return admin::ListTopicsResult(Error{err, rd_kafka_err2str(err)});
    }

    Topics names;
    for (int i = 0; i < rk_metadata->topic_cnt; ++i)
    {
        names.insert(rk_metadata->topics[i].topic);
    }
    return admin::ListTopicsResult(names);
}

inline admin::DeleteRecordsResult
AdminClient::deleteRecords(const TopicPartitionOffsets& topicPartitionOffsets,
                           std::chrono::milliseconds    timeout)
{
    auto rk_queue = rd_kafka_queue_unique_ptr(rd_kafka_queue_new(getClientHandle()));

    const rd_kafka_DeleteRecords_unique_ptr rkDeleteRecords(rd_kafka_DeleteRecords_new(createRkTopicPartitionList(topicPartitionOffsets)));
    std::array<rd_kafka_DeleteRecords_t*, 1> rk_del_records{rkDeleteRecords.get()};

    rd_kafka_DeleteRecords(getClientHandle(), rk_del_records.data(), rk_del_records.size(), nullptr, rk_queue.get());

    auto rk_ev = rd_kafka_event_unique_ptr();

    const auto end = std::chrono::steady_clock::now() + timeout;
    do
    {
        rk_ev.reset(rd_kafka_queue_poll(rk_queue.get(), EVENT_POLLING_INTERVAL_MS));

        if (rd_kafka_event_type(rk_ev.get()) == RD_KAFKA_EVENT_DELETERECORDS_RESULT) break;

        if (rk_ev)
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "rd_kafka_queue_poll got event[%s], with error[%s]", rd_kafka_event_name(rk_ev.get()), rd_kafka_event_error_string(rk_ev.get()));
            rk_ev.reset();
        }
    } while (std::chrono::steady_clock::now() < end);

    if (!rk_ev)
    {
        return admin::DeleteRecordsResult(Error{RD_KAFKA_RESP_ERR__TIMED_OUT, "No response within the time limit"});
    }

    std::list<Error> errors;

    if (const rd_kafka_resp_err_t respErr = rd_kafka_event_error(rk_ev.get()))
    {
        errors.emplace_back(respErr, rd_kafka_event_error_string(rk_ev.get()));
    }

    const rd_kafka_DeleteRecords_result_t* res = rd_kafka_event_DeleteRecords_result(rk_ev.get());
    const rd_kafka_topic_partition_list_t* res_offsets = rd_kafka_DeleteRecords_result_offsets(res);

    errors.splice(errors.end(), getPerTopicPartitionResults(res_offsets));

    return admin::DeleteRecordsResult(combineErrors(errors));
}

} } } // end of KAFKA_API::clients::admin

