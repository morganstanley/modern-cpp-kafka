#pragma once

#include <kafka/Project.h>

#include <kafka/BrokerMetadata.h>
#include <kafka/ClientCommon.h>
#include <kafka/ClientConfig.h>
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
     * Set log level for the kafka client (the default value: 5).
     */
    void setLogLevel(int level);

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
     * Note: The Kafka client should be constructed with option `enable.manual.events.poll=true`!
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
        if (level >= 0 && level <= _logLevel && _logCb)
        {
            LogBuffer<LOG_BUFFER_SIZE> logBuffer;
            logBuffer.print("%s ", name().c_str()).print(format, args...);
            _logCb(level, filename, lineno, logBuffer.c_str());
        }
    }

    void doLog(int level, const char* filename, int lineno, const char* msg) const
    {
        doLog(level, filename, lineno, "%s", msg);
    }

#define KAFKA_API_DO_LOG(lvl, ...) doLog(lvl, __FILE__, __LINE__, ##__VA_ARGS__)

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
                const ConfigCallbacksRegister&  extraConfigRegister = ConfigCallbacksRegister{});

    rd_kafka_t* getClientHandle() const { return _rk.get(); }

    static const KafkaClient& kafkaClient(const rd_kafka_t* rk) { return *static_cast<const KafkaClient*>(rd_kafka_opaque(rk)); }
    static       KafkaClient& kafkaClient(rd_kafka_t* rk)       { return *static_cast<KafkaClient*>(rd_kafka_opaque(rk)); }

    static constexpr int TIMEOUT_INFINITE  = -1;

    static int convertMsDurationToInt(std::chrono::milliseconds ms)
    {
        return ms > std::chrono::milliseconds(INT_MAX) ? TIMEOUT_INFINITE : static_cast<int>(ms.count());
    }

    void setLogCallback(LogCallback cb) { _logCb = std::move(cb); }
    void setStatsCallback(StatsCallback cb) { _statsCb = std::move(cb); }
    void setErrorCallback(ErrorCallback cb) { _errorCb = std::move(cb); }
    void setOauthbearerTokenRefreshCallback(OauthbearerTokenRefreshCallback cb) { _oauthbearerTokenRefreshCb = std::move(cb); }

    void setInterceptors(Interceptors interceptors) { _interceptors = std::move(interceptors); }

    // Show whether it's using automatical events polling
    bool isWithAutoEventsPolling() const { return !_enableManualEventsPoll; }

    // Buffer size for single line logging
    static const constexpr int LOG_BUFFER_SIZE = 1024;

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

    LogCallback                     _logCb = DefaultLogger;
    StatsCallback                   _statsCb;
    ErrorCallback                   _errorCb;
    OauthbearerTokenRefreshCallback _oauthbearerTokenRefreshCb;

    bool                _enableManualEventsPoll = false;
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

    // OAUTHBEARER Toker Refresh Callback (for librdkafka)
    static void oauthbearerTokenRefreshCallback(rd_kafka_t* rk, const char* oauthbearerConfig, void* /*opaque*/);

    // Interceptor callback (for librdkafka)
    static rd_kafka_resp_err_t configInterceptorOnNew(rd_kafka_t* rk, const rd_kafka_conf_t* conf, void* opaque, char* errStr, std::size_t maxErrStrSize);
    static rd_kafka_resp_err_t interceptorOnThreadStart(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* opaque);
    static rd_kafka_resp_err_t interceptorOnThreadExit(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* opaque);
    static rd_kafka_resp_err_t interceptorOnBrokerStateChange(rd_kafka_t* rk, int id, const char* secproto, const char* host, int port, const char* state, void* opaque);

    // Log callback (for class instance)
    void onLog(int level, const char* fac, const char* buf) const;

    // Stats callback (for class instance)
    void onStats(const std::string& jsonString);

    // Error callback (for class instance)
    void onError(const Error& error);

    // OAUTHBEARER Toker Refresh Callback (for class instance)
    SaslOauthbearerToken onOauthbearerTokenRefresh(const std::string& oauthbearerConfig);

    // Interceptor callback (for class instance)
    void interceptThreadStart(const std::string& threadName, const std::string& threadType);
    void interceptThreadExit(const std::string& threadName, const std::string& threadType);
    void interceptBrokerStateChange(int id, const std::string& secproto, const std::string& host, int port, const std::string& state);

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


inline
KafkaClient::KafkaClient(ClientType                     clientType,
                         const Properties&              properties,
                         const ConfigCallbacksRegister& extraConfigRegister)
{
    static const std::set<std::string> PRIVATE_PROPERTY_KEYS = { "max.poll.records", "enable.manual.events.poll" };

    // Save clientID
    if (auto clientId = properties.getProperty(Config::CLIENT_ID))
    {
        _clientId   = *clientId;
        _clientName = getClientTypeString(clientType) + "[" + _clientId + "]";
    }

    // Log Callback
    if (properties.contains("log_cb"))
    {
        setLogCallback(properties.get<LogCallback>("log_cb"));
    }

    // Save LogLevel
    if (auto logLevel = properties.getProperty(Config::LOG_LEVEL))
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

    // Save "enable.manual.events.poll" option
    if (auto enableManualEventsPoll = properties.getProperty(Config::ENABLE_MANUAL_EVENTS_POLL))
    {
        if (*enableManualEventsPoll == "true" || *enableManualEventsPoll == "t" || *enableManualEventsPoll == "1")
        {
            _enableManualEventsPoll = true;
        }
        else if (*enableManualEventsPoll == "false" || *enableManualEventsPoll == "f" || *enableManualEventsPoll == "0")
        {
            _enableManualEventsPoll = false;
        }
        else
        {
            KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, std::string("Invalid option[" + *enableManualEventsPoll + "] for \"enable.manual.events.poll\", which must be a bool value (true or false)!")));
        }
    }

    LogBuffer<LOG_BUFFER_SIZE> errInfo;

    auto rk_conf = rd_kafka_conf_unique_ptr(rd_kafka_conf_new());

    for (const auto& prop: properties.map())
    {
        const auto& k = prop.first;
        const auto& v = properties.getProperty(k);
        if (!v) continue;

        // Those private properties are only available for `C++ wrapper`, not for librdkafka
        if (PRIVATE_PROPERTY_KEYS.count(prop.first))
        {
            _properties.put(prop.first, prop.second);
            continue;
        }

        const rd_kafka_conf_res_t result = rd_kafka_conf_set(rk_conf.get(),
                                                             k.c_str(),
                                                             v->c_str(),
                                                             errInfo.str(),
                                                             errInfo.capacity());
        if (result == RD_KAFKA_CONF_OK)
        {
            _properties.put(prop.first, prop.second);
        }
        else
        {
            KAFKA_API_DO_LOG(Log::Level::Err, "failed to be initialized with property[%s:%s], result[%d]: %s", k.c_str(), v->c_str(), result, errInfo.c_str());
        }
    }

    // Save KafkaClient's raw pointer to the "opaque" field, thus we could fetch it later (for kinds of callbacks)
    rd_kafka_conf_set_opaque(rk_conf.get(), this);

    // Log Callback
    if (properties.contains("log_cb"))
    {
        rd_kafka_conf_set_log_cb(rk_conf.get(), KafkaClient::logCallback);
    }

    // Statistics Callback
    if (properties.contains("stats_cb"))
    {
        setStatsCallback(properties.get<StatsCallback>("stats_cb"));

        rd_kafka_conf_set_stats_cb(rk_conf.get(), KafkaClient::statsCallback);
    }

    // Error Callback
    if (properties.contains("error_cb"))
    {
        setErrorCallback(properties.get<ErrorCallback>("error_cb"));

        rd_kafka_conf_set_error_cb(rk_conf.get(), KafkaClient::errorCallback);
    }

    // OAUTHBEARER Toker Refresh Callback
    if (properties.contains("oauthbearer_token_refresh_cb"))
    {
        setOauthbearerTokenRefreshCallback(properties.get<OauthbearerTokenRefreshCallback>("oauthbearer_token_refresh_cb"));

        rd_kafka_conf_set_oauthbearer_token_refresh_cb(rk_conf.get(), KafkaClient::oauthbearerTokenRefreshCallback);
    }

    // Interceptor
    if (properties.contains("interceptors"))
    {
        setInterceptors(properties.get<Interceptors>("interceptors"));

        const Error result{ rd_kafka_conf_interceptor_add_on_new(rk_conf.get(), "on_new", KafkaClient::configInterceptorOnNew, nullptr) };
        KAFKA_THROW_IF_WITH_ERROR(result);
    }

    // Other Callbacks
    if (extraConfigRegister) extraConfigRegister(rk_conf.get());

    // Set client handler
    _rk.reset(rd_kafka_new((clientType == ClientType::KafkaConsumer ? RD_KAFKA_CONSUMER : RD_KAFKA_PRODUCER),
                           rk_conf.release(),  // rk_conf's ownship would be transferred to rk, after the "rd_kafka_new()" call
                           errInfo.clear().str(),
                           errInfo.capacity()));
    KAFKA_THROW_IF_WITH_ERROR(Error(rd_kafka_last_error()));

    // Add brokers
    auto brokers = properties.getProperty(Config::BOOTSTRAP_SERVERS);
    if (!brokers || rd_kafka_brokers_add(getClientHandle(), brokers->c_str()) == 0)
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                "No broker could be added successfully, BOOTSTRAP_SERVERS=[" + (brokers ? *brokers : "NA") + "]"));
    }

    _opened = true;
}

inline Properties
KafkaClient::validateAndReformProperties(const Properties& properties)
{
    auto newProperties = properties;

    // BOOTSTRAP_SERVERS property is mandatory
    if (!newProperties.getProperty(Config::BOOTSTRAP_SERVERS))
    {
        KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG,\
                                "Validation failed! With no property [" + std::string(Config::BOOTSTRAP_SERVERS) + "]"));
    }

    // If no "client.id" configured, generate a random one for user
    if (!newProperties.getProperty(Config::CLIENT_ID))
    {
        newProperties.put(Config::CLIENT_ID, utility::getRandomString());
    }

    // If no "log_level" configured, use Log::Level::Notice as default
    if (!newProperties.getProperty(Config::LOG_LEVEL))
    {
        newProperties.put(Config::LOG_LEVEL, std::to_string(static_cast<int>(Log::Level::Notice)));
    }

    return newProperties;
}

inline Optional<std::string>
KafkaClient::getProperty(const std::string& name) const
{
    // Find it in pre-saved properties
    if (auto property = _properties.getProperty(name)) return *property;

    const rd_kafka_conf_t* conf = rd_kafka_conf(getClientHandle());

    constexpr int DEFAULT_BUF_SIZE = 512;

    std::vector<char> valueBuf(DEFAULT_BUF_SIZE);
    std::size_t       valueSize = valueBuf.size();

    // Try with a default buf size. If could not find the property, return immediately.
    if (rd_kafka_conf_get(conf, name.c_str(), valueBuf.data(), &valueSize) != RD_KAFKA_CONF_OK) return Optional<std::string>{};

    // If the default buf size is not big enough, retry with a larger one
    if (valueSize > valueBuf.size())
    {
        valueBuf.resize(valueSize);
        [[maybe_unused]] const rd_kafka_conf_res_t result = rd_kafka_conf_get(conf, name.c_str(), valueBuf.data(), &valueSize);
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
    const std::string stats(jsonStrBuf, jsonStrBuf+jsonStrLen);
    kafkaClient(rk).onStats(stats);
    return 0;
}

inline void
KafkaClient::onError(const Error& error)
{
    if (_errorCb) _errorCb(error);
}

inline SaslOauthbearerToken
KafkaClient::onOauthbearerTokenRefresh(const std::string& oauthbearerConfig)
{
    if (!_oauthbearerTokenRefreshCb)
    {
        throw std::runtime_error("No OAUTHBEARER token refresh callback configured!");
    }

    return _oauthbearerTokenRefreshCb(oauthbearerConfig);
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
KafkaClient::oauthbearerTokenRefreshCallback(rd_kafka_t* rk, const char* oauthbearerConfig, void* /* opaque */)
{
    SaslOauthbearerToken oauthbearerToken;

    try
    {
        oauthbearerToken = kafkaClient(rk).onOauthbearerTokenRefresh(oauthbearerConfig != nullptr ? oauthbearerConfig : "");
    }
    catch (const std::exception& e)
    {
        rd_kafka_oauthbearer_set_token_failure(rk, e.what());
        return;
    }

    LogBuffer<LOG_BUFFER_SIZE> errInfo;

    std::vector<const char*> extensions;
    extensions.reserve(oauthbearerToken.extensions.size() * 2);
    for (const auto& kv: oauthbearerToken.extensions)
    {
        extensions.push_back(kv.first.c_str());
        extensions.push_back(kv.second.c_str());
    }

    if (rd_kafka_oauthbearer_set_token(rk,
                                       oauthbearerToken.value.c_str(),
                                       oauthbearerToken.mdLifetime.count(),
                                       oauthbearerToken.mdPrincipalName.c_str(),
                                       extensions.data(), extensions.size(),
                                       errInfo.str(), errInfo.capacity()) != RD_KAFKA_RESP_ERR_NO_ERROR)
    {
        rd_kafka_oauthbearer_set_token_failure(rk, errInfo.c_str());
    }
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

inline void
KafkaClient::interceptBrokerStateChange(int id, const std::string& secproto, const std::string& host, int port, const std::string& state)
{
    if (const auto& cb = _interceptors.onBrokerStateChange()) cb(id, secproto, host, port, state);
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

    if (auto result = rd_kafka_interceptor_add_on_broker_state_change(rk, "on_broker_state_change", KafkaClient::interceptorOnBrokerStateChange, opaque))
    {
        return result;
    }

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline rd_kafka_resp_err_t
KafkaClient::interceptorOnThreadStart(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* /* opaque */)
{
    kafkaClient(rk).interceptThreadStart(threadName, toString(threadType));

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline rd_kafka_resp_err_t
KafkaClient::interceptorOnThreadExit(rd_kafka_t* rk, rd_kafka_thread_type_t threadType, const char* threadName, void* /* opaque */)
{
    kafkaClient(rk).interceptThreadExit(threadName, toString(threadType));

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline rd_kafka_resp_err_t
KafkaClient::interceptorOnBrokerStateChange(rd_kafka_t* rk, int id, const char* secproto, const char* host, int port, const char* state, void* /* opaque */)
{
    kafkaClient(rk).interceptBrokerStateChange(id, secproto, host, port, state);

    return RD_KAFKA_RESP_ERR_NO_ERROR;
}

inline Optional<BrokerMetadata>
KafkaClient::fetchBrokerMetadata(const std::string& topic, std::chrono::milliseconds timeout, bool disableErrorLogging)
{
    const rd_kafka_metadata_t* rk_metadata = nullptr;
    // Here the input parameter for `all_topics` is `true`, since we want the `cgrp_update`
    const rd_kafka_resp_err_t err = rd_kafka_metadata(getClientHandle(),
                                                      true,
                                                      nullptr,
                                                      &rk_metadata,
                                                      convertMsDurationToInt(timeout));

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

        const Partition partition = metadata_partition.id;

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

