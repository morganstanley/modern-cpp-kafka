#pragma once

#include <kafka/Project.h>

#include <kafka/BrokerMetadata.h>
#include <kafka/Error.h>
#include <kafka/Interceptors.h>
#include <kafka/KafkaException.h>
#include <kafka/Log.h>
#include <kafka/Properties.h>
#include <kafka/RdKafkaHelper.h>
#include <kafka/Types.h>

#include <librdkafka/rdkafka.h>

#include <atomic>
#include <cassert>
#include <climits>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>


namespace KAFKA_API { namespace clients {

/**
 * The base class for Kafka clients.
 */
class KafkaClient
{
public:
    /**
     * The option shows whether user wants to call `pollEvents()` manually to trigger internal callbacks.
     */
    enum class EventsPollingOption { Manual, Auto };

    virtual ~KafkaClient() = default;

    /**
     * Get the client id.
     */
    const std::string& clientId()   const { return _clientId; }

    /**
     * Get the client name (i.e. client type + id).
     */
    const std::string& name()       const { return _clientName; }

    /**
     * Set a log callback for kafka clients, which do not have a client specific logging callback configured (see `setLogger`).
     */
    static void setGlobalLogger(Logger logger = NullLogger)
    {
        std::call_once(Global<>::initOnce, [](){}); // Then no need to init within KafkaClient constructor
        Global<>::logger = std::move(logger);
    }

    /**
     * Set the log callback for the kafka client (it's a per-client setting).
     */
    void setLogger(Logger logger) { _logger = std::move(logger); }

    /**
     * Set log level for the kafka client (the default value: 5).
     */
    void setLogLevel(int level);

    /**
     * Callback type for statistics info dumping.
     */
    using StatsCallback = std::function<void(const std::string&)>;

    /**
     * Set callback to receive the periodic statistics info.
     * Note: 1) It only works while the "statistics.interval.ms" property is configured with a non-0 value.
     *       2) The callback would be triggered periodically, receiving the internal statistics info (with JSON format) emited from librdkafka.
     */
    void setStatsCallback(StatsCallback cb) { _statsCb = std::move(cb); }

    /**
     * Callback type for error notification.
     */
    using ErrorCallback = std::function<void(const Error&)>;

    /**
     * Set callback for error notification.
     */
    void setErrorCallback(ErrorCallback cb) { _errorCb = std::move(cb); }

    /**
     * Return the properties which took effect.
     */
    const Properties& properties() const { return _properties; }

    /**
     * Fetch the effected property (including the property internally set by librdkafka).
     */
    Optional<std::string> getProperty(const std::string& name) const;

    /**
     * Call the OffsetCommit callbacks (if any)
     * Note: The Kafka client should be constructed with option `EventsPollingOption::Manual`.
     */
    void pollEvents(std::chrono::milliseconds timeout)
    {
        _pollable->poll(convertMsDurationToInt(timeout));
    }

    /**
     * Fetch matadata from a available broker.
     * Note: the Metadata response information may trigger a re-join if any subscribed topic has changed partition count or existence state.
     */
    Optional<BrokerMetadata> fetchBrokerMetadata(const std::string& topic,
                                                 std::chrono::milliseconds timeout = std::chrono::milliseconds(DEFAULT_METADATA_TIMEOUT_MS),
                                                 bool disableErrorLogging = false);

    template<class ...Args>
    void doLog(int level, const char* filename, int lineno, const char* format, Args... args) const
    {
        const auto& logger = _logger ? _logger : Global<>::logger;
        if (level >= 0 && level <= _logLevel && logger)
        {
            LogBuffer<LOG_BUFFER_SIZE> logBuffer;
            logBuffer.print("%s ", name().c_str()).print(format, args...);
            logger(level, filename, lineno, logBuffer.c_str());
        }
    }

    void doLog(int level, const char* filename, int lineno, const char* msg) const
    {
        doLog(level, filename, lineno, "%s", msg);
    }

#define KAFKA_API_DO_LOG(lvl, ...) doLog(lvl, __FILE__, __LINE__, ##__VA_ARGS__)

    template<class ...Args>
    static void doGlobalLog(int level, const char* filename, int lineno, const char* format, Args... args)
    {
        if (!Global<>::logger) return;

        LogBuffer<LOG_BUFFER_SIZE> logBuffer;
        logBuffer.print(format, args...);
        Global<>::logger(level, filename, lineno, logBuffer.c_str());
    }
    static void doGlobalLog(int level, const char* filename, int lineno, const char* msg)
    {
        doGlobalLog(level, filename, lineno, "%s", msg);
    }

/**
 * Log for kafka clients, with the callback which `setGlobalLogger` assigned.
 *
 * E.g,
 *     KAFKA_API_LOG(Log::Level::Err, "something wrong happened! %s", detailedInfo.c_str());
 */
#define KAFKA_API_LOG(lvl, ...) KafkaClient::doGlobalLog(lvl, __FILE__, __LINE__, ##__VA_ARGS__)

#if COMPILER_SUPPORTS_CPP_17
    static constexpr int DEFAULT_METADATA_TIMEOUT_MS = 10000;
#else
    enum { DEFAULT_METADATA_TIMEOUT_MS = 10000 };
#endif

protected:
    // There're 3 derived classes: KafkaConsumer, KafkaProducer, AdminClient
    enum class ClientType { KafkaConsumer, KafkaProducer, AdminClient };

    using ConfigCallbacksRegister = std::function<void(rd_kafka_conf_t*)>;

    KafkaClient(ClientType                      clientType,
                const Properties&               properties,
                const ConfigCallbacksRegister&  extraConfigRegister,
                EventsPollingOption             eventsPollingOption,
                Interceptors                    interceptors);

    rd_kafka_t* getClientHandle() const { return _rk.get(); }

    static const KafkaClient& kafkaClient(const rd_kafka_t* rk) { return *static_cast<const KafkaClient*>(rd_kafka_opaque(rk)); }
    static       KafkaClient& kafkaClient(rd_kafka_t* rk)       { return *static_cast<KafkaClient*>(rd_kafka_opaque(rk)); }

    static constexpr int TIMEOUT_INFINITE  = -1;

    static int convertMsDurationToInt(std::chrono::milliseconds ms)
    {
        return ms > std::chrono::milliseconds(INT_MAX) ? TIMEOUT_INFINITE : static_cast<int>(ms.count());
    }

    // Show whether it's using automatical events polling
    bool isWithAutoEventsPolling() const { return _eventsPollingOption == EventsPollingOption::Auto; }

    // Buffer size for single line logging
    static const constexpr int LOG_BUFFER_SIZE = 1024;

    // Global logger
    template <typename T = void>
    struct Global
    {
        static Logger         logger;
        static std::once_flag initOnce;
    };

    // Validate properties (and fix it if necesary)
    static Properties validateAndReformProperties(const Properties& properties);

    // To avoid double-close
    bool       _opened = false;

    // Accepted properties
    Properties _properties;

#if COMPILER_SUPPORTS_CPP_17
    static constexpr int EVENT_POLLING_INTERVAL_MS = 100;
#else
    enum { EVENT_POLLING_INTERVAL_MS  = 100 };
#endif

private:
    std::string         _clientId;
    std::string         _clientName;
    std::atomic<int>    _logLevel = {Log::Level::Notice};
    Logger              _logger;
    StatsCallback       _statsCb;
    ErrorCallback       _errorCb;

    EventsPollingOption _eventsPollingOption;
    Interceptors        _interceptors;

    rd_kafka_unique_ptr _rk;

    static std::string getClientTypeString(ClientType type)
    {
        return (type == ClientType::KafkaConsumer ? "KafkaConsumer"
                    : (type == ClientType::KafkaProducer ? "KafkaProducer" : "AdminClient"));
    }

    // Log callback (for librdkafka)
    static void logCallback(const rd_kafka_t* rk, int level, const char* fac, const char* buf);

    // Statistics callback (for librdkafka)
    static int statsCallback(rd_kafka_t* rk, char* jsonStrBuf, size_t jsonStrLen, void* opaque);

    // Error callback (for librdkafka)
    static void errorCallback(rd_kafka_t* rk, int err, const char* reason, void* opaque);

    // Interceptor callback (for librdkafka)
    static rd_kafka_resp_err_t configInterceptorOnNew(rd_kafka_t* rk, const rd_kafka_conf_t* conf, void* opaque, char* errStr, std::size_t maxErrStrSize);
    static rd_kafka_resp_err_t interceptorOnThreadStart(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* opaque);
    static rd_kafka_resp_err_t interceptorOnThreadExit(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* opaque);

    // Log callback (for class instance)
    void onLog(int level, const char* fac, const char* buf) const;

    // Stats callback (for class instance)
    void onStats(const std::string& jsonString);

    // Error callback (for class instance)
    void onError(const Error& error);

    // Interceptor callback (for class instance)
    void interceptThreadStart(const std::string& threadName, const std::string& threadType);
    void interceptThreadExit(const std::string& threadName, const std::string& threadType);

    static const constexpr char* BOOTSTRAP_SERVERS = "bootstrap.servers";
    static const constexpr char* CLIENT_ID         = "client.id";
    static const constexpr char* LOG_LEVEL         = "log_level";

protected:
    struct Pollable
    {
        virtual ~Pollable() = default;
        virtual void poll(int timeoutMs) = 0;
    };

    class PollableCallback: public Pollable
    {
    public:
        using Callback = std::function<void(int)>;

        explicit PollableCallback(Callback cb): _cb(std::move(cb)) {}

        void poll(int timeoutMs) override { _cb(timeoutMs); }

    private:
        const Callback _cb;
    };

    class PollThread
    {
    public:
        using InterceptorCb = std::function<void()>;
        explicit PollThread(const InterceptorCb& entryCb, const InterceptorCb& exitCb, Pollable& pollable)
            : _running(true), _thread(keepPolling, std::ref(_running), entryCb, exitCb, std::ref(pollable))
        {
        }

        ~PollThread()
        {
            _running = false;

            if (_thread.joinable()) _thread.join();
        }

    private:
        static void keepPolling(std::atomic_bool&       running,
                                const InterceptorCb&    entryCb,
                                const InterceptorCb&    exitCb,
                                Pollable&               pollable)
        {
            entryCb();

            while (running.load())
            {
                pollable.poll(CALLBACK_POLLING_INTERVAL_MS);
            }

            exitCb();
        }

        static constexpr int CALLBACK_POLLING_INTERVAL_MS = 10;

        std::atomic_bool _running;
        std::thread      _thread;
    };

    void startBackgroundPollingIfNecessary(const PollableCallback::Callback& pollableCallback)
    {
        _pollable = std::make_unique<KafkaClient::PollableCallback>(pollableCallback);

        auto entryCb = [this]() { interceptThreadStart("events-polling", "background"); };
        auto exitCb =  [this]() { interceptThreadExit("events-polling", "background"); };

        if (isWithAutoEventsPolling()) _pollThread = std::make_unique<PollThread>(entryCb, exitCb, *_pollable);
    }

    void stopBackgroundPollingIfNecessary()
    {
        _pollThread.reset(); // Join the polling thread (in case it's running)

        _pollable.reset();
    }

private:
    std::unique_ptr<Pollable>   _pollable;
    std::unique_ptr<PollThread> _pollThread;
};

template <typename T>
Logger KafkaClient::Global<T>::logger;

template <typename T>
std::once_flag KafkaClient::Global<T>::initOnce;

inline
KafkaClient::KafkaClient(ClientType                     clientType,
                         const Properties&              properties,
                         const ConfigCallbacksRegister& extraConfigRegister,
                         EventsPollingOption            eventsPollingOption,
                         Interceptors                   interceptors)
    : _eventsPollingOption(eventsPollingOption),
      _interceptors(std::move(interceptors))
{
    static const std::set<std::string> PRIVATE_PROPERTY_KEYS = { "max.poll.records" };

    // Save clientID
    if (auto clientId = properties.getProperty(CLIENT_ID))
    {
        _clientId   = *clientId;
        _clientName = getClientTypeString(clientType) + "[" + _clientId + "]";
    }

    // Init global logger
    std::call_once(Global<>::initOnce, [](){ Global<>::logger = DefaultLogger; });

    // Save LogLevel
    if (auto logLevel = properties.getProperty(LOG_LEVEL))
    {
        try
        {
            _logLevel = std::stoi(*logLevel);
        }
        catch (const std::exception& e)
        {
            KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, std::string("Invalid log_level[").append(*logLevel).append("], which must be an number!").append(e.what())));
        }

        if (_logLevel < Log::Level::Emerg || _logLevel > Log::Level::Debug)
        {
            KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, std::string("Invalid log_level[").append(*logLevel).append("], which must be a value between 0 and 7!")));
        }
    }

    LogBuffer<LOG_BUFFER_SIZE> errInfo;

    auto rk_conf = rd_kafka_conf_unique_ptr(rd_kafka_conf_new());

    for (const auto& prop: properties.map())
    {
        // Those private properties are only available for `C++ wrapper`, not for librdkafka
        if (PRIVATE_PROPERTY_KEYS.count(prop.first))
        {
            _properties.put(prop.first, prop.second);
            continue;
        }

        rd_kafka_conf_res_t result = rd_kafka_conf_set(rk_conf.get(), prop.first.c_str(), prop.second.c_str(), errInfo.str(), errInfo.capacity());
        if (result == RD_KAFKA_CONF_OK)
        {
            _properties.put(prop.first, prop.second);
        }
        else
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "failed to be initialized with property[%s:%s], result[%d]", prop.first.c_str(), prop.second.c_str(), result);
        }
    }

    // Save KafkaClient's raw pointer to the "opaque" field, thus we could fetch it later (for kinds of callbacks)
    rd_kafka_conf_set_opaque(rk_conf.get(), this);

    // Log Callback
    rd_kafka_conf_set_log_cb(rk_conf.get(), KafkaClient::logCallback);

    // Statistics Callback
    rd_kafka_conf_set_stats_cb(rk_conf.get(), KafkaClient::statsCallback);

    // Error Callback
    rd_kafka_conf_set_error_cb(rk_conf.get(), KafkaClient::errorCallback);

    // Other Callbacks
    if (extraConfigRegister) extraConfigRegister(rk_conf.get());

    // Interceptor
    if (!_interceptors.empty())
    {
        Error result{ rd_kafka_conf_interceptor_add_on_new(rk_conf.get(), "on_new", KafkaClient::configInterceptorOnNew, nullptr) };
        KAFKA_THROW_IF_WITH_ERROR(result);
    }

    // Set client handler
    _rk.reset(rd_kafka_new((clientType == ClientType::KafkaConsumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER),
                           rk_conf.release(),  // rk_conf's ownship would be transferred to rk, after the "rd_kafka_new()" call
                           errInfo.clear().str(),
                           errInfo.capacity()));
    KAFKA_THROW_IF_WITH_ERROR(Error(rd_kafka_last_error()));

    // Add brokers
    auto brokers = properties.getProperty(BOOTSTRAP_SERVERS);
    if (rd_kafka_brokers_add(getClientHandle(), brokers->c_str()) == 0)
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                "No broker could be added successfully, BOOTSTRAP_SERVERS=[" + *brokers + "]"));
    }

    _opened = true;
}

inline Properties
KafkaClient::validateAndReformProperties(const Properties& properties)
{
    auto newProperties = properties;

    // BOOTSTRAP_SERVERS property is mandatory
    if (!newProperties.getProperty(BOOTSTRAP_SERVERS))
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                "Validation failed! With no property [" + std::string(BOOTSTRAP_SERVERS) + "]"));
    }

    // If no "client.id" configured, generate a random one for user
    if (!newProperties.getProperty(CLIENT_ID))
    {
        newProperties.put(CLIENT_ID, utility::getRandomString());
    }

    // If no "log_level" configured, use Log::Level::Notice as default
    if (!newProperties.getProperty(LOG_LEVEL))
    {
        newProperties.put(LOG_LEVEL, std::to_string(static_cast<int>(Log::Level::Notice)));
    }

    return newProperties;
}

inline Optional<std::string>
KafkaClient::getProperty(const std::string& name) const
{
    // Find it in pre-saved properties
    if (auto property = _properties.getProperty(name)) return *property;

    constexpr int DEFAULT_BUF_SIZE = 512;

    const rd_kafka_conf_t* conf = rd_kafka_conf(getClientHandle());

    std::vector<char> valueBuf(DEFAULT_BUF_SIZE);
    std::size_t       valueSize = valueBuf.size();

    // Try with a default buf size. If could not find the property, return immediately.
    if (rd_kafka_conf_get(conf, name.c_str(), valueBuf.data(), &valueSize) != RD_KAFKA_CONF_OK) return Optional<std::string>{};

    // If the default buf size is not big enough, retry with a larger one
    if (valueSize > valueBuf.size())
    {
        valueBuf.resize(valueSize);
        [[maybe_unused]] rd_kafka_conf_res_t result = rd_kafka_conf_get(conf, name.c_str(), valueBuf.data(), &valueSize);
        assert(result == RD_KAFKA_CONF_OK);
    }

    return std::string(valueBuf.data());
}

inline void
KafkaClient::setLogLevel(int level)
{
    _logLevel = level < Log::Level::Emerg ? Log::Level::Emerg : (level > Log::Level::Debug ? Log::Level::Debug : level);
    rd_kafka_set_log_level(getClientHandle(), _logLevel);
}

inline void
KafkaClient::onLog(int level, const char* fac, const char* buf) const
{
    doLog(level, "LIBRDKAFKA", 0, "%s | %s", fac, buf); // The log is coming from librdkafka
}

inline void
KafkaClient::logCallback(const rd_kafka_t* rk, int level, const char* fac, const char* buf)
{
    kafkaClient(rk).onLog(level, fac, buf);
}

inline void
KafkaClient::onStats(const std::string& jsonString)
{
    if (_statsCb) _statsCb(jsonString);
}

inline int
KafkaClient::statsCallback(rd_kafka_t* rk, char* jsonStrBuf, size_t jsonStrLen, void* /*opaque*/)
{
    std::string stats(jsonStrBuf, jsonStrBuf+jsonStrLen);
    kafkaClient(rk).onStats(stats);
    return 0;
}

inline void
KafkaClient::onError(const Error& error)
{
    if (_errorCb) _errorCb(error);
}

inline void
KafkaClient::errorCallback(rd_kafka_t* rk, int err, const char* reason, void* /*opaque*/)
{
    auto respErr = static_cast<rd_kafka_resp_err_t>(err);

    Error error;
    if (respErr != RD_KAFKA_RESP_ERR__FATAL)
    {
        error = Error{respErr, reason};
    }
    else
    {
        LogBuffer<LOG_BUFFER_SIZE> errInfo;
        respErr = rd_kafka_fatal_error(rk, errInfo.str(), errInfo.capacity());
        error = Error{respErr, errInfo.c_str(), true};
    }

    kafkaClient(rk).onError(error);
}

inline void
KafkaClient::interceptThreadStart(const std::string& threadName, const std::string& threadType)
{
    if (const auto& cb = _interceptors.onThreadStart()) cb(threadName, threadType);
}

inline void
KafkaClient::interceptThreadExit(const std::string& threadName, const std::string& threadType)
{
    if (const auto& cb = _interceptors.onThreadExit()) cb(threadName, threadType);
}

inline rd_kafka_resp_err_t
KafkaClient::configInterceptorOnNew(rd_kafka_t* rk, const rd_kafka_conf_t* /*conf*/, void* opaque, char* /*errStr*/, std::size_t /*maxErrStrSize*/)
{
    if (auto result = rd_kafka_interceptor_add_on_thread_start(rk, "on_thread_start", KafkaClient::interceptorOnThreadStart, opaque))
    {
        return result;
    }

    if (auto result = rd_kafka_interceptor_add_on_thread_exit(rk, "on_thread_exit", KafkaClient::interceptorOnThreadExit, opaque))
    {
        return result;
    }

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline rd_kafka_resp_err_t
KafkaClient::interceptorOnThreadStart(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* /*opaque*/)
{
    kafkaClient(rk).interceptThreadStart(threadName, toString(threadType));

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline rd_kafka_resp_err_t
KafkaClient::interceptorOnThreadExit(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* /*opaque*/)
{
    kafkaClient(rk).interceptThreadExit(threadName, toString(threadType));

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline Optional<BrokerMetadata>
KafkaClient::fetchBrokerMetadata(const std::string& topic, std::chrono::milliseconds timeout, bool disableErrorLogging)
{
    const rd_kafka_metadata_t* rk_metadata = nullptr;
    // Here the input parameter for `all_topics` is `true`, since we want the `cgrp_update`
    rd_kafka_resp_err_t err = rd_kafka_metadata(getClientHandle(), true, nullptr, &rk_metadata, convertMsDurationToInt(timeout));

    auto guard = rd_kafka_metadata_unique_ptr(rk_metadata);

    if (err != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        if (!disableErrorLogging)
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "failed to get BrokerMetadata! error[%s]", rd_kafka_err2str(err));
        }
        return Optional<BrokerMetadata>{};
    }

    const rd_kafka_metadata_topic* metadata_topic = nullptr;
    for (int i = 0; i < rk_metadata->topic_cnt; ++i)
    {
        if (rk_metadata->topics[i].topic == topic)
        {
            metadata_topic = &rk_metadata->topics[i];
            break;
        }
    }

    if (!metadata_topic || metadata_topic->err)
    {
        if (!disableErrorLogging)
        {
            if (!metadata_topic)
            {
                KAFKA_API_DO_LOG(Log::Level::Err, "failed to find BrokerMetadata for topic[%s]", topic.c_str());
            }
            else
            {
                KAFKA_API_DO_LOG(Log::Level::Err, "failed to get BrokerMetadata for topic[%s]! error[%s]", topic.c_str(), rd_kafka_err2str(metadata_topic->err));
            }
        }
        return Optional<BrokerMetadata>{};
    }

    // Construct the BrokerMetadata
    BrokerMetadata metadata(metadata_topic->topic);
    metadata.setOrigNodeName(rk_metadata->orig_broker_name ? std::string(rk_metadata->orig_broker_name) : "");

    for (int i = 0; i < rk_metadata->broker_cnt; ++i)
    {
        metadata.addNode(rk_metadata->brokers[i].id, rk_metadata->brokers[i].host, rk_metadata->brokers[i].port);
    }

    for (int i = 0; i < metadata_topic->partition_cnt; ++i)
    {
        const rd_kafka_metadata_partition& metadata_partition = metadata_topic->partitions[i];

        Partition partition = metadata_partition.id;

        if (metadata_partition.err != 0)
        {
            if (!disableErrorLogging)
            {
                KAFKA_API_DO_LOG(Log::Level::Err, "got error[%s] while constructing BrokerMetadata for topic[%s]-partition[%d]", rd_kafka_err2str(metadata_partition.err), topic.c_str(), partition);
            }

            continue;
        }

        BrokerMetadata::PartitionInfo partitionInfo(metadata_partition.leader);

        for (int j = 0; j < metadata_partition.replica_cnt; ++j)
        {
            partitionInfo.addReplica(metadata_partition.replicas[j]);
        }

        for (int j = 0; j < metadata_partition.isr_cnt; ++j)
        {
            partitionInfo.addInSyncReplica(metadata_partition.isrs[j]);
        }

        metadata.addPartitionInfo(partition, partitionInfo);
    }

    return metadata;
}


} } // end of KAFKA_API::clients

