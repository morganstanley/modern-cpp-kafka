#pragma once

#include <kafka/Project.h>

#include <functional>


namespace KAFKA_API { namespace clients {

/**
 * Interceptors for Kafka clients.
 */
class Interceptors
{
public:
    /**
    * Callback type for thread-start interceptor.
    */
    using ThreadStartCb = std::function<void(const std::string&, const std::string&)>;

    /**
    * Callback type for thread-exit interceptor.
    */
    using ThreadExitCb  = std::function<void(const std::string&, const std::string&)>;

    /**
    * Callback type for broker-state-change interceptor.
    */
    using BrokerStateChangeCb = std::function<void(int, const std::string&, const std::string&, int, const std::string&)>;

    /**
    * Set interceptor for thread start.
    */
    Interceptors& onThreadStart(ThreadStartCb cb) { _valid = true; _threadStartCb = std::move(cb); return *this; }

    /**
    * Set interceptor for thread exit.
    */
    Interceptors& onThreadExit(ThreadExitCb cb)   { _valid = true; _threadExitCb = std::move(cb);  return *this; }

    /**
    * Set interceptor for broker state change.
    */
    Interceptors& onBrokerStateChange(BrokerStateChangeCb cb) { _valid = true; _brokerStateChangeCb = std::move(cb);  return *this; }

    /**
    * Get interceptor for thread start.
    */
    ThreadStartCb onThreadStart() const { return _threadStartCb; }

    /**
    * Get interceptor for thread exit.
    */
    ThreadExitCb  onThreadExit()  const { return _threadExitCb; }

    /**
    * Get interceptor for broker state change.
    */
    BrokerStateChangeCb onBrokerStateChange() const { return _brokerStateChangeCb; }

    /**
    * Check if there's no interceptor.
    */
    bool empty() const { return !_valid; }

private:
    ThreadStartCb       _threadStartCb;
    ThreadExitCb        _threadExitCb;
    BrokerStateChangeCb _brokerStateChangeCb;

    bool _valid  = false;
};

} } // end of KAFKA_API::clients

