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
    using ThreadStartCallback = std::function<void(const std::string&, const std::string&)>;

    /**
    * Callback type for thread-exit interceptor.
    */
    using ThreadExitCallback  = std::function<void(const std::string&, const std::string&)>;

    /**
    * Set interceptor for thread start.
    */
    Interceptors& onThreadStart(ThreadStartCallback cb) { _valid = true; _threadStartCb = std::move(cb); return *this; }

    /**
    * Set interceptor for thread exit.
    */
    Interceptors& onThreadExit(ThreadExitCallback cb)   { _valid = true; _threadExitCb = std::move(cb);  return *this; }

    /**
    * Get interceptor for thread start.
    */
    ThreadStartCallback onThreadStart() const { return _threadStartCb; }

    /**
    * Get interceptor for thread exit.
    */
    ThreadExitCallback  onThreadExit()  const { return _threadExitCb; }

    /**
    * Check if there's no interceptor.
    */
    bool empty() const { return !_valid; }

private:
    ThreadStartCallback _threadStartCb;
    ThreadExitCallback  _threadExitCb;
    bool                _valid  = false;
};

} } // end of KAFKA_API::clients

