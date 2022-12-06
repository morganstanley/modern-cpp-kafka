#pragma once

#include <kafka/Project.h>

#include <kafka/ConsumerCommon.h>
#include <kafka/KafkaClient.h>
#include <kafka/ProducerCommon.h>
#include <kafka/ProducerConfig.h>
#include <kafka/ProducerRecord.h>
#include <kafka/Timestamp.h>
#include <kafka/Types.h>

#include <librdkafka/rdkafka.h>

#include <cassert>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <unordered_map>


namespace KAFKA_API { namespace clients { namespace producer {

/**
 * KafkaProducer class.
 */
class KafkaProducer: public KafkaClient
{
public:
    /**
     * The constructor for KafkaProducer.
     *
     * Throws KafkaException with errors:
     *   - RD_KAFKA_RESP_ERR__INVALID_ARG      : Invalid BOOTSTRAP_SERVERS property
     *   - RD_KAFKA_RESP_ERR__CRIT_SYS_RESOURCE: Fail to create internal threads
     */
    explicit KafkaProducer(const Properties& properties);

    /**
     * The destructor for KafkaProducer.
     */
    ~KafkaProducer() override { if (_opened) close(); }

    /**
     * Invoking this method makes all buffered records immediately available to send, and blocks on the completion of the requests associated with these records.
     *
     * Possible error values:
     *   - RD_KAFKA_RESP_ERR__TIMED_OUT: The `timeout` was reached before all outstanding requests were completed.
     */
    Error flush(std::chrono::milliseconds timeout = InfiniteTimeout);

    /**
     * Purge messages currently handled by the KafkaProducer.
     */
    Error purge();

    /**
     * Close this producer. This method would wait up to timeout for the producer to complete the sending of all incomplete requests (before purging them).
     */
    void close(std::chrono::milliseconds timeout = InfiniteTimeout);

    /**
     * Options for sending messages.
     */
    enum class SendOption { NoCopyRecordValue, ToCopyRecordValue };

    /**
     * Choose the action while the sending buffer is full.
     */
    enum class ActionWhileQueueIsFull { Block, NoBlock };

    /**
     * Asynchronously send a record to a topic.
     *
     * Note:
     *   - If a callback is provided, it's guaranteed to be triggered (before closing the producer).
     *   - If any error occured, an exception would be thrown.
     *   - Make sure the memory block (for ProducerRecord's value) is valid until the delivery callback finishes; Otherwise, should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.
     *
     * Possible errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *     - RD_KAFKA_RESP_ERR__QUEUE_FULL:        The message buffing queue is full
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    void send(const producer::ProducerRecord& record,
              const producer::Callback&       deliveryCb,
              SendOption                      option = SendOption::NoCopyRecordValue,
              ActionWhileQueueIsFull          action = ActionWhileQueueIsFull::Block);

    /**
     * Asynchronously send a record to a topic.
     *
     * Note:
     *   - If a callback is provided, it's guaranteed to be triggered (before closing the producer).
     *   - The input reference parameter `error` will be set if an error occurred.
     *   - Make sure the memory block (for ProducerRecord's value) is valid until the delivery callback finishes; Otherwise, should be with option `KafkaProducer::SendOption::ToCopyRecordValue`.
     *
     * Possible errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *     - RD_KAFKA_RESP_ERR__QUEUE_FULL:        The message buffing queue is full
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    void send(const producer::ProducerRecord& record,
              const producer::Callback&       deliveryCb,
              Error&                          error,
              SendOption                      option = SendOption::NoCopyRecordValue,
              ActionWhileQueueIsFull          action = ActionWhileQueueIsFull::Block)
    {
        try { send(record, deliveryCb, option, action); } catch (const KafkaException& e) { error = e.error(); }
    }

    /**
     * Synchronously send a record to a topic.
     * Throws KafkaException with errors:
     *   Local errors,
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_TOPIC:     The topic doesn't exist
     *     - RD_KAFKA_RESP_ERR__UNKNOWN_PARTITION: The partition doesn't exist
     *     - RD_KAFKA_RESP_ERR__INVALID_ARG:       Invalid topic(topic is null, or the length is too long (> 512)
     *     - RD_KAFKA_RESP_ERR__MSG_TIMED_OUT:     No ack received within the time limit
     *   Broker errors,
     *     - [Error Codes] (https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-ErrorCodes)
     */
    producer::RecordMetadata syncSend(const producer::ProducerRecord& record);

    /**
     * Needs to be called before any other methods when the transactional.id is set in the configuration.
     */
    void initTransactions(std::chrono::milliseconds timeout = InfiniteTimeout);

    /**
     * Should be called before the start of each new transaction.
     */
    void beginTransaction();

    /**
     * Commit the ongoing transaction.
     */
    void commitTransaction(std::chrono::milliseconds timeout = InfiniteTimeout);

    /**
     * Abort the ongoing transaction.
     */
    void abortTransaction(std::chrono::milliseconds timeout = InfiniteTimeout);


    /**
     * Send a list of specified offsets to the consumer group coodinator, and also marks those offsets as part of the current transaction.
     */
    void sendOffsetsToTransaction(const TopicPartitionOffsets&           topicPartitionOffsets,
                                  const consumer::ConsumerGroupMetadata& groupMetadata,
                                  std::chrono::milliseconds              timeout);

private:
    void pollCallbacks(int timeoutMs)
    {
        rd_kafka_poll(getClientHandle(), timeoutMs);
    }

    // Define datatypes for "opaque" (as an input for rd_kafka_produceva), in order to handle the delivery callback
    class DeliveryCbOpaque
    {
    public:
        DeliveryCbOpaque(Optional<producer::ProducerRecord::Id> id, producer::Callback cb): _recordId(id), _deliveryCb(std::move(cb)) {}

        void operator()(rd_kafka_t* /*rk*/, const rd_kafka_message_t* rkmsg)
        {
            _deliveryCb(producer::RecordMetadata{rkmsg, _recordId}, Error{rkmsg->err});
        }

    private:
        const Optional<producer::ProducerRecord::Id> _recordId;
        const producer::Callback                     _deliveryCb;
    };

    // Validate properties (and fix it if necesary)
    static Properties validateAndReformProperties(const Properties& properties);

    // Delivery Callback (for librdkafka)
    static void deliveryCallback(rd_kafka_t* rk, const rd_kafka_message_t* rkmsg, void* opaque);

    // Register Callbacks for rd_kafka_conf_t
    static void registerConfigCallbacks(rd_kafka_conf_t* conf);

#ifdef KAFKA_API_ENABLE_UNIT_TEST_STUBS
public:
    using HandleProduceResponseCb = std::function<rd_kafka_resp_err_t(rd_kafka_t* /*rk*/, int32_t /*brokerid*/, uint64_t /*msgseq*/, rd_kafka_resp_err_t /*err*/)>;

    /**
     * Stub for ProduceResponse handing.
     * Note: Only for internal unit tests
     */
    void stubHandleProduceResponse(HandleProduceResponseCb cb = HandleProduceResponseCb()) { _handleProduceRespCb = std::move(cb); }

private:
    static rd_kafka_resp_err_t handleProduceResponse(rd_kafka_t* rk, int32_t brokerId, uint64_t msgSeq, rd_kafka_resp_err_t err)
    {
        auto* client   = static_cast<KafkaClient*>(rd_kafka_opaque(rk));
        auto* producer = dynamic_cast<KafkaProducer*>(client);
        auto  respCb   = producer->_handleProduceRespCb;
        return respCb ? respCb(rk, brokerId, msgSeq, err) : err;
    }

    HandleProduceResponseCb _handleProduceRespCb;
#endif
};

inline
KafkaProducer::KafkaProducer(const Properties&      properties)
    : KafkaClient(ClientType::KafkaProducer, validateAndReformProperties(properties), registerConfigCallbacks)
{
    // Start background polling (if needed)
    startBackgroundPollingIfNecessary([this](int timeoutMs){ pollCallbacks(timeoutMs); });

    const auto propStr = KafkaClient::properties().toString();
    KAFKA_API_DO_LOG(Log::Level::Notice, "initializes with properties[%s]", propStr.c_str());
}

inline void
KafkaProducer::registerConfigCallbacks(rd_kafka_conf_t* conf)
{
    // Delivery Callback
    rd_kafka_conf_set_dr_msg_cb(conf, deliveryCallback);

#ifdef KAFKA_API_ENABLE_UNIT_TEST_STUBS
    // UT stub for ProduceResponse
    LogBuffer<LOG_BUFFER_SIZE> errInfo;
    if (rd_kafka_conf_set(conf, "ut_handle_ProduceResponse", reinterpret_cast<char*>(&handleProduceResponse), errInfo.str(), errInfo.capacity()))   // NOLINT
    {
        KafkaClient* client = nullptr;
        size_t clientPtrSize = 0;
        if (rd_kafka_conf_get(conf, "opaque", reinterpret_cast<char*>(&client), &clientPtrSize))    // NOLINT
        {
            KAFKA_API_LOG(Log::Level::Crit, "failed to stub ut_handle_ProduceResponse! error[%s]. Meanwhile, failed to get the Kafka client!", errInfo.c_str());
        }
        else
        {
            assert(clientPtrSize == sizeof(client));                                                // NOLINT
            client->KAFKA_API_DO_LOG(Log::Level::Err, "failed to stub ut_handle_ProduceResponse! error[%s]", errInfo.c_str());
        }
    }
#endif
}

inline Properties
KafkaProducer::validateAndReformProperties(const Properties& properties)
{
    using namespace producer;

    // Let the base class validate first
    auto newProperties = KafkaClient::validateAndReformProperties(properties);

    // Check whether it's an available partitioner
    const std::set<std::string> availPartitioners = {"murmur2_random", "murmur2", "random", "consistent", "consistent_random", "fnv1a", "fnv1a_random"};
    auto partitioner = newProperties.getProperty(ProducerConfig::PARTITIONER);
    if (partitioner && !availPartitioners.count(*partitioner))
    {
        std::string errMsg = "Invalid partitioner [" + *partitioner + "]! Valid options: ";
        bool isTheFirst = true;
        for (const auto& availPartitioner: availPartitioners)
        {
            errMsg += (std::string(isTheFirst ? (isTheFirst = false, "") : ", ") + availPartitioner);
        }
        errMsg += ".";

        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, errMsg));
    }

    // For "idempotence" feature
    constexpr int KAFKA_IDEMP_MAX_INFLIGHT = 5;
    const auto enableIdempotence = newProperties.getProperty(ProducerConfig::ENABLE_IDEMPOTENCE);
    if (enableIdempotence && *enableIdempotence == "true")
    {
        if (const auto maxInFlight = newProperties.getProperty(ProducerConfig::MAX_IN_FLIGHT))
        {
            if (std::stoi(*maxInFlight) > KAFKA_IDEMP_MAX_INFLIGHT)
            {
                KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                        "`max.in.flight` must be set <= " + std::to_string(KAFKA_IDEMP_MAX_INFLIGHT) + " when `enable.idempotence` is `true`"));
            }
        }

        if (const auto acks = newProperties.getProperty(ProducerConfig::ACKS))
        {
            if (*acks != "all" && *acks != "-1")
            {
                KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                        "`acks` must be set to `all`/`-1` when `enable.idempotence` is `true`"));
            }
        }
    }

    return newProperties;
}

// Delivery Callback (for librdkafka)
inline void
KafkaProducer::deliveryCallback(rd_kafka_t* rk, const rd_kafka_message_t* rkmsg, void* /*opaque*/)
{
    if (auto* deliveryCbOpaque = static_cast<DeliveryCbOpaque*>(rkmsg->_private))
    {
        (*deliveryCbOpaque)(rk, rkmsg);
        delete deliveryCbOpaque;
    }
}

inline void
KafkaProducer::send(const producer::ProducerRecord& record,
                    const producer::Callback&       deliveryCb,
                    SendOption                      option,
                    ActionWhileQueueIsFull          action)
{
    auto deliveryCbOpaque = std::make_unique<DeliveryCbOpaque>(record.id(), deliveryCb);
    auto queueFullAction  = (isWithAutoEventsPolling() ? action : ActionWhileQueueIsFull::NoBlock);

    const auto* topic     = record.topic().c_str();
    const auto  partition = record.partition();
    const auto  msgFlags  = (static_cast<unsigned int>(option == SendOption::ToCopyRecordValue ? RD_KAFKA_MSG_F_COPY : 0)
                             | static_cast<unsigned int>(queueFullAction == ActionWhileQueueIsFull::Block ? RD_KAFKA_MSG_F_BLOCK : 0));
    const auto* keyPtr    = record.key().data();
    const auto  keyLen    = record.key().size();
    const auto* valuePtr  = record.value().data();
    const auto  valueLen  = record.value().size();

    auto* rk        = getClientHandle();
    auto* opaquePtr = deliveryCbOpaque.get();

    constexpr std::size_t VU_LIST_SIZE_WITH_NO_HEADERS = 6;
    std::vector<rd_kafka_vu_t> rkVUs(VU_LIST_SIZE_WITH_NO_HEADERS + record.headers().size());

    std::size_t uvCount = 0;

    {   // Topic
        auto& vu = rkVUs[uvCount++];
        vu.vtype  = RD_KAFKA_VTYPE_TOPIC;
        vu.u.cstr = topic;
    }

    {   // Partition
        auto& vu = rkVUs[uvCount++];
        vu.vtype = RD_KAFKA_VTYPE_PARTITION;
        vu.u.i32 = partition;
    }

    {   // Message flags
        auto& vu = rkVUs[uvCount++];
        vu.vtype = RD_KAFKA_VTYPE_MSGFLAGS;
        vu.u.i   = static_cast<int>(msgFlags);
    }

    {   // Key
        auto& vu = rkVUs[uvCount++];
        vu.vtype      = RD_KAFKA_VTYPE_KEY;
        vu.u.mem.ptr  = const_cast<void*>(keyPtr);      // NOLINT
        vu.u.mem.size = keyLen;
    }

    {   // Value
        auto& vu = rkVUs[uvCount++];
        vu.vtype      = RD_KAFKA_VTYPE_VALUE;
        vu.u.mem.ptr  = const_cast<void*>(valuePtr);    // NOLINT
        vu.u.mem.size = valueLen;
    }

    {   // Opaque
        auto& vu = rkVUs[uvCount++];
        vu.vtype = RD_KAFKA_VTYPE_OPAQUE;
        vu.u.ptr = opaquePtr;
    }

    // Headers
    for (const auto& header: record.headers())
    {
        auto& vu = rkVUs[uvCount++];
        vu.vtype         = RD_KAFKA_VTYPE_HEADER;
        vu.u.header.name = header.key.c_str();
        vu.u.header.val  = header.value.data();
        vu.u.header.size = static_cast<int64_t>(header.value.size());
    }

    assert(uvCount == rkVUs.size());

    const Error sendResult{ rd_kafka_produceva(rk, rkVUs.data(), rkVUs.size()) };
    KAFKA_THROW_IF_WITH_ERROR(sendResult);

    // KafkaProducer::deliveryCallback would delete the "opaque"
    deliveryCbOpaque.release();
}

inline producer::RecordMetadata
KafkaProducer::syncSend(const producer::ProducerRecord& record)
{
    Optional<Error>          deliveryResult;
    producer::RecordMetadata recordMetadata;
    std::mutex               mtx;
    std::condition_variable  delivered;

    auto deliveryCb = [&deliveryResult, &recordMetadata, &mtx, &delivered] (const producer::RecordMetadata& metadata, const Error& error) {
        const std::lock_guard<std::mutex> guard(mtx);

        deliveryResult = error;
        recordMetadata = metadata;

        delivered.notify_one();
    };

    send(record, deliveryCb);

    std::unique_lock<std::mutex> lock(mtx);
    delivered.wait(lock, [&deliveryResult]{ return static_cast<bool>(deliveryResult); });

    KAFKA_THROW_IF_WITH_ERROR(*deliveryResult); // NOLINT

    return recordMetadata;
}

inline Error
KafkaProducer::flush(std::chrono::milliseconds timeout)
{
    return Error{rd_kafka_flush(getClientHandle(), convertMsDurationToInt(timeout))};
}

inline Error
KafkaProducer::purge()
{
    return Error{rd_kafka_purge(getClientHandle(),
                                (static_cast<unsigned>(RD_KAFKA_PURGE_F_QUEUE) | static_cast<unsigned>(RD_KAFKA_PURGE_F_INFLIGHT)))};
}

inline void
KafkaProducer::close(std::chrono::milliseconds timeout)
{
    _opened = false;

    stopBackgroundPollingIfNecessary();

    const Error result = flush(timeout);
    if (result.value() == RD_KAFKA_RESP_ERR__TIMED_OUT)
    {
        KAFKA_API_DO_LOG(Log::Level::Notice, "purge messages before close, outQLen[%d]", rd_kafka_outq_len(getClientHandle()));
        purge();
    }

    rd_kafka_poll(getClientHandle(), 0);

    KAFKA_API_DO_LOG(Log::Level::Notice, "closed");

}

inline void
KafkaProducer::initTransactions(std::chrono::milliseconds timeout)
{
    const Error result{ rd_kafka_init_transactions(getClientHandle(), static_cast<int>(timeout.count())) };
    KAFKA_THROW_IF_WITH_ERROR(result);
}

inline void
KafkaProducer::beginTransaction()
{
    const Error result{ rd_kafka_begin_transaction(getClientHandle()) };
    KAFKA_THROW_IF_WITH_ERROR(result);
}

inline void
KafkaProducer::commitTransaction(std::chrono::milliseconds timeout)
{
    const Error result{ rd_kafka_commit_transaction(getClientHandle(), static_cast<int>(timeout.count())) };
    KAFKA_THROW_IF_WITH_ERROR(result);
}

inline void
KafkaProducer::abortTransaction(std::chrono::milliseconds timeout)
{
    const Error result{ rd_kafka_abort_transaction(getClientHandle(), static_cast<int>(timeout.count())) };
    KAFKA_THROW_IF_WITH_ERROR(result);
}

inline void
KafkaProducer::sendOffsetsToTransaction(const TopicPartitionOffsets&           topicPartitionOffsets,
                                        const consumer::ConsumerGroupMetadata& groupMetadata,
                                        std::chrono::milliseconds              timeout)
{
    auto rk_tpos = rd_kafka_topic_partition_list_unique_ptr(createRkTopicPartitionList(topicPartitionOffsets));
    const Error result{ rd_kafka_send_offsets_to_transaction(getClientHandle(),
                                                             rk_tpos.get(),
                                                             groupMetadata.rawHandle(),
                                                             static_cast<int>(timeout.count())) };
    KAFKA_THROW_IF_WITH_ERROR(result);
}

} } } // end of KAFKA_API::clients::producer

