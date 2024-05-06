#pragma once

#include <kafka/Project.h>

#include <kafka/ConsumerCommon.h>
#include <kafka/ConsumerConfig.h>
#include <kafka/ConsumerRecord.h>
#include <kafka/Error.h>
#include <kafka/KafkaClient.h>

#include <librdkafka/rdkafka.h>

#include <algorithm>
#include <cassert>
#include <chrono>
#include <iterator>
#include <memory>


namespace KAFKA_API { namespace clients { namespace consumer {

/**
 * KafkaConsumer class.
 */
class KafkaConsumer: public KafkaClient
{
public:
    // Default value for property "max.poll.records" (which is same with Java API)
    static const constexpr char* DEFAULT_MAX_POLL_RECORDS_VALUE = "500";

    /**
     * The constructor for KafkaConsumer.
     *
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__INVALID_ARG      : Invalid BOOTSTRAP_SERVERS property
     *   - RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE: Fail to create internal threads
     */
    explicit KafkaConsumer(const Properties& properties);

    /**
     * The destructor for KafkaConsumer.
     */
    ~KafkaConsumer() override { if (_opened) close(); }

    /**
     * Close the consumer, waiting for any needed cleanup.
     */
    void close();

    /**
     * To get group ID.
     */
    std::string getGroupId() const                 { return _groupId; }

    /**
     * To set group ID. The group ID is mandatory for a Consumer.
     */
    void        setGroupId(const std::string& id)  { _groupId = id; }

    /**
     * Subscribe to the given list of topics to get dynamically assigned partitions.
     * An exception would be thrown if assign is called previously (without a subsequent call to unsubscribe())
     */
    void subscribe(const Topics&               topics,
                   consumer::RebalanceCallback rebalanceCallback = consumer::NullRebalanceCallback,
                   std::chrono::milliseconds   timeout           = std::chrono::milliseconds(DEFAULT_SUBSCRIBE_TIMEOUT_MS));
    /**
     * Get the current subscription.
     */
    Topics subscription() const;

    /**
     * Unsubscribe from topics currently subscribed.
     */
    void unsubscribe(std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_UNSUBSCRIBE_TIMEOUT_MS));

    /**
     * Manually assign a list of partitions to this consumer.
     * An exception would be thrown if subscribe is called previously (without a subsequent call to unsubscribe())
     */
    void assign(const TopicPartitions& topicPartitions);

    /**
     * Get the set of partitions currently assigned to this consumer.
     */
    TopicPartitions assignment() const;

    // Seek & Position
    /**
     * Overrides the fetch offsets that the consumer will use on the next poll(timeout).
     * If this API is invoked for the same partition more than once, the latest offset will be used on the next poll().
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT:         Operation timed out
     *   - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: Invalid partition
     *   - RD_KAFKA_RESP_ERR__STATE:             Invalid broker state
     */
    void seek(const TopicPartition& topicPartition, Offset offset, std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_SEEK_TIMEOUT_MS));

    /**
     * Seek to the first offset for each of the given partitions.
     * This function evaluates lazily, seeking to the first offset in all partitions only when poll(long) or position(TopicPartition) are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT:         Operation timed out
     *   - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: Invalid partition
     *   - RD_KAFKA_RESP_ERR__STATE:             Invalid broker state
     */
    void seekToBeginning(const TopicPartitions&    topicPartitions,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_SEEK_TIMEOUT_MS)) { seekToBeginningOrEnd(topicPartitions, true, timeout); }
    void seekToBeginning(std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_SEEK_TIMEOUT_MS)) { seekToBeginningOrEnd(_assignment, true, timeout); }

    /**
     * Seek to the last offset for each of the given partitions.
     * This function evaluates lazily, seeking to the final offset in all partitions only when poll(long) or position(TopicPartition) are called.
     * If no partitions are provided, seek to the first offset for all of the currently assigned partitions.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT:         Operation timed out
     *   - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: Invalid partition
     *   - RD_KAFKA_RESP_ERR__STATE:             Invalid broker state
     */
    void seekToEnd(const TopicPartitions&    topicPartitions,
                   std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_SEEK_TIMEOUT_MS)) { seekToBeginningOrEnd(topicPartitions, false, timeout); }
    void seekToEnd(std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_SEEK_TIMEOUT_MS)) { seekToBeginningOrEnd(_assignment, false, timeout); }

    /**
     * Get the offset of the next record that will be fetched (if a record with that offset exists).
     */
    Offset position(const TopicPartition& topicPartition) const;

    /**
     * Get the first offset for the given partitions.
     * This method does not change the current consumer position of the partitions.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__FAIL:  Generic failure
     */
    std::map<TopicPartition, Offset> beginningOffsets(const TopicPartitions&    topicPartitions,
                                                      std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_QUERY_TIMEOUT_MS)) const
    {
        return getOffsets(topicPartitions, true, timeout);
    }

    /**
     * Get the last offset for the given partitions.  The last offset of a partition is the offset of the upcoming message, i.e. the offset of the last available message + 1.
     * This method does not change the current consumer position of the partitions.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__FAIL:  Generic failure
     */
    std::map<TopicPartition, Offset> endOffsets(const TopicPartitions&    topicPartitions,
                                                std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_QUERY_TIMEOUT_MS)) const
    {
        return getOffsets(topicPartitions, false, timeout);
    }

    /**
     * Get the offsets for the given partitions by time-point.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT:           Not all offsets could be fetched in time.
     *   - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION:   All partitions are unknown.
     *   - RD_KAFKA_RESP_ERR_LEADER_NOT_AVAILABLE: Unable to query leaders from the given partitions.
     */
    std::map<TopicPartition, Offset> offsetsForTime(const TopicPartitions&                             topicPartitions,
                                                    std::chrono::time_point<std::chrono::system_clock> timepoint,
                                                    std::chrono::milliseconds                          timeout = std::chrono::milliseconds(DEFAULT_QUERY_TIMEOUT_MS)) const;

    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partitions.
     */
    void commitSync();

    /**
     * Commit the specified offsets for the specified records
     */
    void commitSync(const consumer::ConsumerRecord& record);

    /**
     * Commit the specified offsets for the specified list of topics and partitions.
     */
    void commitSync(const TopicPartitionOffsets& topicPartitionOffsets);

    /**
     * Commit offsets returned on the last poll() for all the subscribed list of topics and partition.
     * Note: If a callback is provided, it's guaranteed to be triggered (before closing the consumer).
     */
    void commitAsync(const consumer::OffsetCommitCallback& offsetCommitCallback = consumer::NullOffsetCommitCallback);

    /**
     * Commit the specified offsets for the specified records
     * Note: If a callback is provided, it's guaranteed to be triggered (before closing the consumer).
     */
    void commitAsync(const consumer::ConsumerRecord& record, const consumer::OffsetCommitCallback& offsetCommitCallback = consumer::NullOffsetCommitCallback);

    /**
     * Commit the specified offsets for the specified list of topics and partitions to Kafka.
     * Note: If a callback is provided, it's guaranteed to be triggered (before closing the consumer).
     */
    void commitAsync(const TopicPartitionOffsets& topicPartitionOffsets, const consumer::OffsetCommitCallback& offsetCommitCallback = consumer::NullOffsetCommitCallback);

    /**
     * Get the last committed offset for the given partition (whether the commit happened by this process or another).This offset will be used as the position for the consumer in the event of a failure.
     * This call will block to do a remote call to get the latest committed offsets from the server.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__INVALID_ARG:  Invalid partition
     */
    Offset committed(const TopicPartition& topicPartition);

    /**
     * Fetch data for the topics or partitions specified using one of the subscribe/assign APIs.
     * Returns the polled records.
     * Note: 1) The result could be fetched through ConsumerRecord (with member function `error`).
     *       2) Make sure the `ConsumerRecord` be destructed before the `KafkaConsumer.close()`.
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: Unknow partition
     */
    std::vector<consumer::ConsumerRecord> poll(std::chrono::milliseconds timeout);

    /**
     * Suspend fetching from the requested partitions. Future calls to poll() will not return any records from these partitions until they have been resumed using resume().
     * Note: 1) After pausing, the application still need to call `poll()` at regular intervals.
     *       2) This method does not affect partition subscription/assignment (i.e, pause fetching from partitions would not trigger a rebalance, since the consumer is still alive).
     *       3) If none of the provided partitions is assigned to this consumer, an exception would be thrown.
     * Throws KafkaException with error:
     *   - RD_KAFKA_RESP_ERR__INVALID_ARG: Invalid arguments
     */
    void pause(const TopicPartitions& topicPartitions);

    /**
     * Suspend fetching from all assigned partitions. Future calls to poll() will not return any records until they have been resumed using resume().
     * Note: This method does not affect partition subscription/assignment.
     */
    void pause();

    /**
     * Resume specified partitions which have been paused with pause(). New calls to poll() will return records from these partitions if there are any to be fetched.
     * Note: If the partitions were not previously paused, this method is a no-op.
     */
    void resume(const TopicPartitions& topicPartitions);

    /**
     * Resume all partitions which have been paused with pause(). New calls to poll() will return records from these partitions if there are any to be fetched.
     */
    void resume();

    /**
     * Return the current group metadata associated with this consumer.
     */
    consumer::ConsumerGroupMetadata groupMetadata();

private:
    static const constexpr char* ENABLE_AUTO_OFFSET_STORE = "enable.auto.offset.store";
    static const constexpr char* AUTO_COMMIT_INTERVAL_MS  = "auto.commit.interval.ms";

#if COMPILER_SUPPORTS_CPP_17
    static constexpr int DEFAULT_SUBSCRIBE_TIMEOUT_MS   = 30000;
    static constexpr int DEFAULT_UNSUBSCRIBE_TIMEOUT_MS = 10000;
    static constexpr int DEFAULT_QUERY_TIMEOUT_MS       = 10000;
    static constexpr int DEFAULT_SEEK_TIMEOUT_MS        = 10000;
    static constexpr int SEEK_RETRY_INTERVAL_MS         = 5000;
#else
    enum { DEFAULT_SUBSCRIBE_TIMEOUT_MS   = 30000 };
    enum { DEFAULT_UNSUBSCRIBE_TIMEOUT_MS = 10000 };
    enum { DEFAULT_QUERY_TIMEOUT_MS       = 10000 };
    enum { DEFAULT_SEEK_TIMEOUT_MS        = 10000 };
    enum { SEEK_RETRY_INTERVAL_MS         = 5000  };
#endif

    enum class CommitType { Sync, Async };
    void commit(const TopicPartitionOffsets& topicPartitionOffsets, CommitType type);

    // Offset Commit Callback (for librdkafka)
    static void offsetCommitCallback(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* rk_tpos, void* opaque);

    // Validate properties (and fix it if necesary)
    static Properties validateAndReformProperties(Properties properties);

    void commitStoredOffsetsIfNecessary(CommitType type);
    void storeOffsetsIfNecessary(const std::vector<consumer::ConsumerRecord>& records);

    void seekToBeginningOrEnd(const TopicPartitions& topicPartitions, bool toBeginning, std::chrono::milliseconds timeout);
    std::map<TopicPartition, Offset> getOffsets(const TopicPartitions&    topicPartitions,
                                                bool                      atBeginning,
                                                std::chrono::milliseconds timeout) const;

    enum class PartitionsRebalanceEvent { Assign, Revoke, IncrementalAssign, IncrementalUnassign };
    void changeAssignment(PartitionsRebalanceEvent event, const TopicPartitions& tps);

    std::string  _groupId;

    std::size_t _maxPollRecords   = 500;  // From "max.poll.records" property, and here is the default for batch-poll
    bool        _enableAutoCommit = true; // From "enable.auto.commit" property

    rd_kafka_queue_unique_ptr _rk_queue;

    // Save assignment info (from "assign()" call or rebalance callback) locally, to accelerate seeking procedure
    TopicPartitions _assignment;
    // Assignment from user's input, -- by calling "assign()"
    TopicPartitions _userAssignment;
    // Subscription from user's input, -- by calling "subscribe()"
    Topics          _userSubscription;

    enum class PendingEvent { PartitionsAssignment, PartitionsRevocation };
    Optional<PendingEvent> _pendingEvent;

    // Identify whether the "partition.assignment.strategy" is "cooperative-sticky"
    Optional<bool> _cooperativeEnabled;
    bool isCooperativeEnabled() const { return _cooperativeEnabled && *_cooperativeEnabled; }

    // The offsets to store (and commit later)
    std::map<TopicPartition, Offset> _offsetsToStore;

    // Register Callbacks for rd_kafka_conf_t
    static void registerConfigCallbacks(rd_kafka_conf_t* conf);

    enum class PauseOrResumeOperation { Pause, Resume };
    void pauseOrResumePartitions(const TopicPartitions& topicPartitions, PauseOrResumeOperation op);

    // Rebalance Callback (for librdkafka)
    static void rebalanceCallback(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* partitions, void* opaque);
    // Rebalance Callback (for class instance)
    void onRebalance(rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* rk_partitions);

    consumer::RebalanceCallback _rebalanceCb;

    rd_kafka_queue_t* getCommitCbQueue() { return _rk_commit_cb_queue.get(); }

    rd_kafka_queue_unique_ptr _rk_commit_cb_queue;

    void pollCallbacks(int timeoutMs)
    {
        rd_kafka_queue_t* queue = getCommitCbQueue();
        rd_kafka_queue_poll_callback(queue, timeoutMs);
    }
};


// Validate properties (and fix it if necesary)
inline Properties
KafkaConsumer::validateAndReformProperties(Properties properties)
{
    using namespace consumer;

    // Don't pass the "max.poll.records" property to librdkafka
    properties.remove(ConsumerConfig::MAX_POLL_RECORDS);

    // Let the base class validate first
    auto newProperties = KafkaClient::validateAndReformProperties(properties);

    // If no "group.id" configured, generate a random one for user
    if (!newProperties.getProperty(ConsumerConfig::GROUP_ID))
    {
        newProperties.put(ConsumerConfig::GROUP_ID, utility::getRandomString());
    }

    // Disable the internal auto-commit from librdkafka, since we want to customize the behavior
    newProperties.put(ConsumerConfig::ENABLE_AUTO_COMMIT,   "false");
    newProperties.put(AUTO_COMMIT_INTERVAL_MS,              "0");
    newProperties.put(ENABLE_AUTO_OFFSET_STORE,             "true");

    return newProperties;
}

// Register Callbacks for rd_kafka_conf_t
inline void
KafkaConsumer::registerConfigCallbacks(rd_kafka_conf_t* conf)
{
    // Rebalance Callback
    // would turn off librdkafka's automatic partition assignment/revocation
    rd_kafka_conf_set_rebalance_cb(conf, KafkaConsumer::rebalanceCallback);
}

inline
KafkaConsumer::KafkaConsumer(const Properties& properties)
    : KafkaClient(ClientType::KafkaConsumer, validateAndReformProperties(properties), registerConfigCallbacks)
{
    using namespace consumer;

    // Pick up the "max.poll.records" property
    if (auto maxPollRecordsProperty = properties.getProperty(ConsumerConfig::MAX_POLL_RECORDS))
    {
        const std::string maxPollRecords = *maxPollRecordsProperty;
        _maxPollRecords = static_cast<std::size_t>(std::stoi(maxPollRecords));
    }
    _properties.put(ConsumerConfig::MAX_POLL_RECORDS, std::to_string(_maxPollRecords));

    // Pick up the "enable.auto.commit" property
    if (auto enableAutoCommitProperty = properties.getProperty(ConsumerConfig::ENABLE_AUTO_COMMIT))
    {
        const std::string enableAutoCommit = *enableAutoCommitProperty;

        auto isTrue  = [](const std::string& str) { return str == "1" || str == "true"; };
        auto isFalse = [](const std::string& str) { return str == "0" || str == "false"; };

        if (!isTrue(enableAutoCommit) && !isFalse(enableAutoCommit))
        {
            KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, std::string("Invalid property[enable.auto.commit=").append(enableAutoCommit).append("], which MUST be true(1) or false(0)!")));
        }

        _enableAutoCommit = isTrue(enableAutoCommit);
    }
    _properties.put(ConsumerConfig::ENABLE_AUTO_COMMIT, (_enableAutoCommit ? "true" : "false"));

    // Fetch groupId from reformed configuration
    auto groupId = _properties.getProperty(ConsumerConfig::GROUP_ID);
    assert(groupId);
    setGroupId(*groupId);

    // Redirect the reply queue (to the client group queue)
    const Error result{ rd_kafka_poll_set_consumer(getClientHandle()) };
    KAFKA_THROW_IF_WITH_ERROR(result);

    // Initialize message-fetching queue
    _rk_queue.reset(rd_kafka_queue_get_consumer(getClientHandle()));

    // Initialize commit-callback queue
    _rk_commit_cb_queue.reset(rd_kafka_queue_new(getClientHandle()));

    // Start background polling (if needed)
    startBackgroundPollingIfNecessary([this](int timeoutMs){ pollCallbacks(timeoutMs); });

    const auto propsStr = KafkaClient::properties().toString();
    KAFKA_API_DO_LOG(Log::Level::Notice, "initialized with properties[%s]", propsStr.c_str());
}

inline void
KafkaConsumer::close()
{
    _opened = false;

    stopBackgroundPollingIfNecessary();

    try
    {
        // Commit the offsets for these messages which had been polled last time (for `enable.auto.commit=true` case.)
        commitStoredOffsetsIfNecessary(CommitType::Sync);
    }
    catch (const KafkaException& e)
    {
        KAFKA_API_DO_LOG(Log::Level::Err, "met error[%s] while closing", e.what());
    }

    rd_kafka_consumer_close(getClientHandle());

    while (rd_kafka_outq_len(getClientHandle()))
    {
        rd_kafka_poll(getClientHandle(), KafkaClient::TIMEOUT_INFINITE);
    }

    rd_kafka_queue_t* queue = getCommitCbQueue();
    while (rd_kafka_queue_length(queue))
    {
        rd_kafka_queue_poll_callback(queue, TIMEOUT_INFINITE);
    }

    KAFKA_API_DO_LOG(Log::Level::Notice, "closed");
}


// Subscription
inline void
KafkaConsumer::subscribe(const Topics& topics, consumer::RebalanceCallback rebalanceCallback, std::chrono::milliseconds timeout)
{
    if (!_userAssignment.empty())
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__FAIL, "Unexpected Operation! Once assign() was used, subscribe() should not be called any more!"));
    }

    if (isCooperativeEnabled() && topics == _userSubscription)
    {
        KAFKA_API_DO_LOG(Log::Level::Info, "skip subscribe (no change since last time)");
        return;
    }

    _userSubscription = topics;

    const std::string topicsStr = toString(topics);
    KAFKA_API_DO_LOG(Log::Level::Info, "will subscribe, topics[%s]", topicsStr.c_str());

    _rebalanceCb = std::move(rebalanceCallback);

    auto rk_topics = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList(topics));

    const Error result{ rd_kafka_subscribe(getClientHandle(), rk_topics.get()) };
    KAFKA_THROW_IF_WITH_ERROR(result);

    _pendingEvent = PendingEvent::PartitionsAssignment;

    // The rebalcance callback would be served during the time (within this thread)
    for (const auto end = std::chrono::steady_clock::now() + timeout; std::chrono::steady_clock::now() < end; )
    {
        rd_kafka_poll(getClientHandle(), EVENT_POLLING_INTERVAL_MS);

        if (!_pendingEvent)
        {
            KAFKA_API_DO_LOG(Log::Level::Notice, "subscribed, topics[%s]", topicsStr.c_str());
            return;
        }
    }

    _pendingEvent.reset();
    KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__TIMED_OUT, "subscribe() timed out!"));
}

inline void
KafkaConsumer::unsubscribe(std::chrono::milliseconds timeout)
{
    if (_userSubscription.empty() && _userAssignment.empty())
    {
        KAFKA_API_DO_LOG(Log::Level::Info, "skip unsubscribe (no assignment/subscription yet)");
        return;
    }

    KAFKA_API_DO_LOG(Log::Level::Info, "will unsubscribe");

    // While it's for the previous `assign(...)`
    if (!_userAssignment.empty())
    {
        changeAssignment(isCooperativeEnabled() ? PartitionsRebalanceEvent::IncrementalUnassign : PartitionsRebalanceEvent::Revoke,
                         _userAssignment);
        _userAssignment.clear();

        KAFKA_API_DO_LOG(Log::Level::Notice, "unsubscribed (the previously assigned partitions)");
        return;
    }

    _userSubscription.clear();

    const Error result{ rd_kafka_unsubscribe(getClientHandle()) };
    KAFKA_THROW_IF_WITH_ERROR(result);

    _pendingEvent = PendingEvent::PartitionsRevocation;

    // The rebalance callback would be served during the time (within this thread)
    for (const auto end = std::chrono::steady_clock::now() + timeout; std::chrono::steady_clock::now() < end; )
    {
        rd_kafka_poll(getClientHandle(), EVENT_POLLING_INTERVAL_MS);

        if (!_pendingEvent)
        {
            KAFKA_API_DO_LOG(Log::Level::Notice, "unsubscribed");
            return;
        }
    }

    _pendingEvent.reset();
    KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__TIMED_OUT, "unsubscribe() timed out!"));
}

inline Topics
KafkaConsumer::subscription() const
{
    rd_kafka_topic_partition_list_t* raw_topics = nullptr;
    const Error result{ rd_kafka_subscription(getClientHandle(), &raw_topics) };
    auto rk_topics = rd_kafka_topic_partition_list_unique_ptr(raw_topics);

    KAFKA_THROW_IF_WITH_ERROR(result);

    return getTopics(rk_topics.get());
}

inline void
KafkaConsumer::changeAssignment(PartitionsRebalanceEvent event, const TopicPartitions& tps)
{
    auto rk_tps = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList(tps));

    Error result;
    switch (event)
    {
        case PartitionsRebalanceEvent::Assign:
            result = Error{ rd_kafka_assign(getClientHandle(), rk_tps.get()) };
            // Update assignment
            _assignment = tps;
            break;

        case PartitionsRebalanceEvent::Revoke:
            result = Error{ rd_kafka_assign(getClientHandle(), nullptr) };
            // Update assignment
            _assignment.clear();
            break;

        case PartitionsRebalanceEvent::IncrementalAssign:
            result = Error{ rd_kafka_incremental_assign(getClientHandle(), rk_tps.get()) };
            // Update assignment
            for (const auto& tp: tps)
            {
                auto found = _assignment.find(tp);
                if (found != _assignment.end())
                {
                    const std::string tpStr = toString(tp);
                    KAFKA_API_DO_LOG(Log::Level::Err, "incremental assign partition[%s] has already been assigned", tpStr.c_str());
                    continue;
                }
                _assignment.emplace(tp);
            }
            break;

        case PartitionsRebalanceEvent::IncrementalUnassign:
            result = Error{ rd_kafka_incremental_unassign(getClientHandle(), rk_tps.get()) };
            // Update assignment
            for (const auto& tp: tps)
            {
                auto found = _assignment.find(tp);
                if (found == _assignment.end())
                {
                    const std::string tpStr = toString(tp);
                    KAFKA_API_DO_LOG(Log::Level::Err, "incremental unassign partition[%s] could not be found", tpStr.c_str());
                    continue;
                }
                _assignment.erase(found);
            }
            break;
    }

    KAFKA_THROW_IF_WITH_ERROR(result);
}

// Assign Topic-Partitions
inline void
KafkaConsumer::assign(const TopicPartitions& topicPartitions)
{
    if (!_userSubscription.empty())
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__FAIL, "Unexpected Operation! Once subscribe() was used, assign() should not be called any more!"));
    }

    _userAssignment = topicPartitions;

    changeAssignment(isCooperativeEnabled() ? PartitionsRebalanceEvent::IncrementalAssign : PartitionsRebalanceEvent::Assign,
                     topicPartitions);
}

// Assignment
inline TopicPartitions
KafkaConsumer::assignment() const
{
    rd_kafka_topic_partition_list_t* raw_tps = nullptr;
    const Error result{ rd_kafka_assignment(getClientHandle(), &raw_tps) };

    auto rk_tps = rd_kafka_topic_partition_list_unique_ptr(raw_tps);

    KAFKA_THROW_IF_WITH_ERROR(result);

    return getTopicPartitions(rk_tps.get());
}


// Seek & Position
inline void
KafkaConsumer::seek(const TopicPartition& topicPartition, Offset offset, std::chrono::milliseconds timeout)
{
    const std::string topicPartitionStr = toString(topicPartition);
    KAFKA_API_DO_LOG(Log::Level::Info, "will seek with topic-partition[%s], offset[%d]", topicPartitionStr.c_str(), offset);

    auto rkt = rd_kafka_topic_unique_ptr(rd_kafka_topic_new(getClientHandle(), topicPartition.first.c_str(), nullptr));
    if (!rkt)
    {
        KAFKA_THROW_ERROR(Error(rd_kafka_last_error()));
    }

    const auto end = std::chrono::steady_clock::now() + timeout;

    rd_kafka_resp_err_t respErr = RD_KAFKA_RESP_ERR_NO_ERROR;
    do
    {
        respErr = rd_kafka_seek(rkt.get(), topicPartition.second, offset, SEEK_RETRY_INTERVAL_MS);
        if (respErr != RD_KAFKA_RESP_ERR__STATE && respErr != RD_KAFKA_RESP_ERR__TIMED_OUT && respErr != RD_KAFKA_RESP_ERR__OUTDATED)
        {
            break;
        }

        // If the "seek" was called just after "assign", there's a chance that the toppar's "fetch_state" (async setted) was not ready yes.
        // If that's the case, we would retry again (normally, just after a very short while, the "seek" would succeed)
        std::this_thread::yield();
    } while (std::chrono::steady_clock::now() < end);

    KAFKA_THROW_IF_WITH_ERROR(Error(respErr));

    KAFKA_API_DO_LOG(Log::Level::Info, "seeked with topic-partition[%s], offset[%d]", topicPartitionStr.c_str(), offset);
}

inline void
KafkaConsumer::seekToBeginningOrEnd(const TopicPartitions& topicPartitions, bool toBeginning, std::chrono::milliseconds timeout)
{
    for (const auto& topicPartition: topicPartitions)
    {
        seek(topicPartition, (toBeginning ? RD_KAFKA_OFFSET_BEGINNING : RD_KAFKA_OFFSET_END), timeout);
    }
}

inline Offset
KafkaConsumer::position(const TopicPartition& topicPartition) const
{
    auto rk_tp = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList({topicPartition}));

    const Error error{ rd_kafka_position(getClientHandle(), rk_tp.get()) };
    KAFKA_THROW_IF_WITH_ERROR(error);

    return rk_tp->elems[0].offset;
}

inline std::map<TopicPartition, Offset>
KafkaConsumer::offsetsForTime(const TopicPartitions& topicPartitions,
                              std::chrono::time_point<std::chrono::system_clock> timepoint,
                              std::chrono::milliseconds timeout) const
{
    if (topicPartitions.empty()) return {};

    auto msSinceEpoch = std::chrono::duration_cast<std::chrono::milliseconds>(timepoint.time_since_epoch()).count();

    auto rk_tpos = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList(topicPartitions));

    for (int i = 0; i < rk_tpos->cnt; ++i)
    {
        rd_kafka_topic_partition_t& rk_tp = rk_tpos->elems[i];
        // Here the `msSinceEpoch` would be overridden by the offset result (after called by `rd_kafka_offsets_for_times`)
        rk_tp.offset = msSinceEpoch;
    }

    Error error{ rd_kafka_offsets_for_times(getClientHandle(), rk_tpos.get(), static_cast<int>(timeout.count())) }; // NOLINT
    KAFKA_THROW_IF_WITH_ERROR(error);

    auto results = getTopicPartitionOffsets(rk_tpos.get());

    // Remove invalid results (which are not updated with an valid offset)
    for (auto it = results.begin(); it != results.end(); )
    {
        it = ((it->second == msSinceEpoch) ? results.erase(it) : std::next(it));
    }

    return results;
}

inline std::map<TopicPartition, Offset>
KafkaConsumer::getOffsets(const TopicPartitions&    topicPartitions,
                          bool                      atBeginning,
                          std::chrono::milliseconds timeout) const
{
    std::map<TopicPartition, Offset> result;

    for (const auto& topicPartition: topicPartitions)
    {
        Offset beginning{}, end{};
        const Error error{ rd_kafka_query_watermark_offsets(getClientHandle(),
                                                            topicPartition.first.c_str(),
                                                            topicPartition.second,
                                                            &beginning,
                                                            &end,
                                                            static_cast<int>(timeout.count())) };
        KAFKA_THROW_IF_WITH_ERROR(error);

        result[topicPartition] = (atBeginning ? beginning : end);
    }

    return result;
}

// Commit
inline void
KafkaConsumer::commit(const TopicPartitionOffsets& topicPartitionOffsets, CommitType type)
{
    auto rk_tpos = rd_kafka_topic_partition_list_unique_ptr(topicPartitionOffsets.empty() ? nullptr : createRkTopicPartitionList(topicPartitionOffsets));

    Error error{ rd_kafka_commit(getClientHandle(), rk_tpos.get(), type == CommitType::Async ? 1 : 0) };
    // No stored offset to commit (it might happen and should not be treated as a mistake)
    if (topicPartitionOffsets.empty() && error.value() == RD_KAFKA_RESP_ERR__NO_OFFSET)
    {
        error = Error{};
    }

    KAFKA_THROW_IF_WITH_ERROR(error);
}

// Fetch committed offset
inline Offset
KafkaConsumer::committed(const TopicPartition& topicPartition)
{
    auto rk_tps = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList({topicPartition}));

    const Error error {rd_kafka_committed(getClientHandle(), rk_tps.get(), TIMEOUT_INFINITE) };
    KAFKA_THROW_IF_WITH_ERROR(error);

    return rk_tps->elems[0].offset;
}

// Commit stored offsets
inline void
KafkaConsumer::commitStoredOffsetsIfNecessary(CommitType type)
{
    if (_enableAutoCommit && !_offsetsToStore.empty())
    {
        for (auto& o: _offsetsToStore)
        {
            ++o.second;
        }
        commit(_offsetsToStore, type);
        _offsetsToStore.clear();
    }
}

// Store offsets
inline void
KafkaConsumer::storeOffsetsIfNecessary(const std::vector<consumer::ConsumerRecord>& records)
{
    if (_enableAutoCommit)
    {
        for (const auto& record: records)
        {
            _offsetsToStore[TopicPartition(record.topic(), record.partition())] = record.offset();
        }
    }
}

// Fetch messages
inline std::vector<consumer::ConsumerRecord>
KafkaConsumer::poll(std::chrono::milliseconds timeout)
{
    // Commit the offsets for these messages which had been polled last time (for "enable.auto.commit=true" case)
    commitStoredOffsetsIfNecessary(CommitType::Async);

    // Poll messages with librdkafka's API
    std::vector<rd_kafka_message_t*> msgPtrArray(_maxPollRecords);
    auto msgReceived = rd_kafka_consume_batch_queue(_rk_queue.get(), convertMsDurationToInt(timeout), msgPtrArray.data(), _maxPollRecords);
    if (msgReceived < 0)
    {
        KAFKA_THROW_ERROR(Error(rd_kafka_last_error()));
    }

    // Wrap messages with ConsumerRecord
    std::vector<consumer::ConsumerRecord> records(msgPtrArray.begin(), msgPtrArray.begin() + msgReceived);

    // Store the offsets for all these polled messages (for "enable.auto.commit=true" case)
    storeOffsetsIfNecessary(records);

    return records;
}

inline void
KafkaConsumer::pauseOrResumePartitions(const TopicPartitions& topicPartitions, PauseOrResumeOperation op)
{
    auto rk_tpos = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList(topicPartitions));

    const Error error{ (op == PauseOrResumeOperation::Pause) ?
                          rd_kafka_pause_partitions(getClientHandle(), rk_tpos.get())
                          : rd_kafka_resume_partitions(getClientHandle(), rk_tpos.get()) };
    KAFKA_THROW_IF_WITH_ERROR(error);

    const char* opString = (op == PauseOrResumeOperation::Pause) ? "pause" : "resume";
    int cnt = 0;
    for (int i = 0; i < rk_tpos->cnt; ++i)
    {
        const rd_kafka_topic_partition_t& rk_tp = rk_tpos->elems[i];
        if (rk_tp.err != RD_KAFKA_RESP_ERR_NO_ERROR)
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "%s topic-partition[%s-%d] error[%s]", opString, rk_tp.topic, rk_tp.partition, rd_kafka_err2str(rk_tp.err));
        }
        else
        {
            KAFKA_API_DO_LOG(Log::Level::Notice, "%sd topic-partition[%s-%d]", opString, rk_tp.topic, rk_tp.partition, rd_kafka_err2str(rk_tp.err));
            ++cnt;
        }
    }

    if (cnt == 0 && op == PauseOrResumeOperation::Pause)
    {
        const std::string errMsg = std::string("No partition could be ") + opString + std::string("d among TopicPartitions[") + toString(topicPartitions) + std::string("]");
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, errMsg));
    }
}

inline void
KafkaConsumer::pause(const TopicPartitions& topicPartitions)
{
    pauseOrResumePartitions(topicPartitions, PauseOrResumeOperation::Pause);
}

inline void
KafkaConsumer::pause()
{
    pause(_assignment);
}

inline void
KafkaConsumer::resume(const TopicPartitions& topicPartitions)
{
    pauseOrResumePartitions(topicPartitions, PauseOrResumeOperation::Resume);
}

inline void
KafkaConsumer::resume()
{
    resume(_assignment);
}

// Rebalance Callback (for class instance)
inline void
KafkaConsumer::onRebalance(rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* rk_partitions)
{
    const TopicPartitions tps    = getTopicPartitions(rk_partitions);
    const std::string     tpsStr = toString(tps);

    if (err != RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS && err != RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS)
    {
        KAFKA_API_DO_LOG(Log::Level::Err, "unknown re-balance event[%d], topic-partitions[%s]",  err, tpsStr.c_str());
        return;
    }

    // Initialize attribute for cooperative protocol
    if (!_cooperativeEnabled)
    {
        if (const char* protocol = rd_kafka_rebalance_protocol(getClientHandle()))
        {
            _cooperativeEnabled = (std::string(protocol) == "COOPERATIVE");
        }
    }

    KAFKA_API_DO_LOG(Log::Level::Notice, "re-balance event triggered[%s], cooperative[%s], topic-partitions[%s]",
                     err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ? "ASSIGN_PARTITIONS" : "REVOKE_PARTITIONS",
                     isCooperativeEnabled() ? "enabled" : "disabled",
                     tpsStr.c_str());

    // Remove the mark for pending event
    if (_pendingEvent
        && ((err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS && *_pendingEvent == PendingEvent::PartitionsAssignment)
            || (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS && *_pendingEvent == PendingEvent::PartitionsRevocation)))
    {
        _pendingEvent.reset();
    }

    const PartitionsRebalanceEvent event = (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ?
                                             (isCooperativeEnabled() ? PartitionsRebalanceEvent::IncrementalAssign : PartitionsRebalanceEvent::Assign)
                                             : (isCooperativeEnabled() ? PartitionsRebalanceEvent::IncrementalUnassign : PartitionsRebalanceEvent::Revoke));

    if (err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS)
    {
        changeAssignment(event, tps);
    }

    if (_rebalanceCb)
    {
        _rebalanceCb(err == RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS ? consumer::RebalanceEventType::PartitionsAssigned : consumer::RebalanceEventType::PartitionsRevoked,
                     tps);
    }

    if (err == RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS)
    {
        changeAssignment(event, isCooperativeEnabled() ? tps : TopicPartitions{});
    }
}

// Rebalance Callback (for librdkafka)
inline void
KafkaConsumer::rebalanceCallback(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* partitions, void* /* opaque */)
{
    KafkaClient& client   = kafkaClient(rk);
    auto&        consumer = dynamic_cast<KafkaConsumer&>(client);
    consumer.onRebalance(err, partitions);
}

// Offset Commit Callback (for librdkafka)
inline void
KafkaConsumer::offsetCommitCallback(rd_kafka_t* rk, rd_kafka_resp_err_t err, rd_kafka_topic_partition_list_t* rk_tpos, void* opaque)
{
    const TopicPartitionOffsets tpos = getTopicPartitionOffsets(rk_tpos);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        auto tposStr = toString(tpos);
        kafkaClient(rk).KAFKA_API_DO_LOG(Log::Level::Err, "invoked offset-commit callback. offsets[%s], result[%s]", tposStr.c_str(), rd_kafka_err2str(err));
    }

    auto* cb = static_cast<consumer::OffsetCommitCallback*>(opaque);
    if (cb && *cb)
    {
        (*cb)(tpos, Error(err));
    }
    delete cb;
}

inline consumer::ConsumerGroupMetadata
KafkaConsumer::groupMetadata()
{
    return consumer::ConsumerGroupMetadata{rd_kafka_consumer_group_metadata(getClientHandle())};
}

inline void
KafkaConsumer::commitSync()
{
    commit(TopicPartitionOffsets(), CommitType::Sync);
}

inline void
KafkaConsumer::commitSync(const consumer::ConsumerRecord& record)
{
    TopicPartitionOffsets tpos;
    // committed offset should be "current-received-offset + 1"
    tpos[TopicPartition(record.topic(), record.partition())] = record.offset() + 1;

    commit(tpos, CommitType::Sync);
}

inline void
KafkaConsumer::commitSync(const TopicPartitionOffsets& topicPartitionOffsets)
{
    commit(topicPartitionOffsets, CommitType::Sync);
}

inline void
KafkaConsumer::commitAsync(const TopicPartitionOffsets& topicPartitionOffsets, const consumer::OffsetCommitCallback& offsetCommitCallback)
{
    auto rk_tpos = rd_kafka_topic_partition_list_unique_ptr(topicPartitionOffsets.empty() ? nullptr : createRkTopicPartitionList(topicPartitionOffsets));

    const Error error{ rd_kafka_commit_queue(getClientHandle(),
                                       rk_tpos.get(),
                                       getCommitCbQueue(),
                                       &KafkaConsumer::offsetCommitCallback,
                                       new consumer::OffsetCommitCallback(offsetCommitCallback)) };
    KAFKA_THROW_IF_WITH_ERROR(error);
}

inline void
KafkaConsumer::commitAsync(const consumer::ConsumerRecord& record, const consumer::OffsetCommitCallback& offsetCommitCallback)
{
    TopicPartitionOffsets tpos;
    // committed offset should be "current received record's offset" + 1
    tpos[TopicPartition(record.topic(), record.partition())] = record.offset() + 1;
    commitAsync(tpos, offsetCommitCallback);
}

inline void
KafkaConsumer::commitAsync(const consumer::OffsetCommitCallback& offsetCommitCallback)
{
    commitAsync(TopicPartitionOffsets(), offsetCommitCallback);
}

} } } // end of KAFKA_API::clients::consumer

