#pragma once

#include <kafka/Project.h>

#include <functional>


namespace KAFKA_API { namespace clients {

class Interceptors
{
public:
    using ThreadStartCallback = std::function<void(const std::string&, const std::string&)>;
    using ThreadExitCallback  = std::function<void(const std::string&, const std::string&)>;

    Interceptors& onThreadStart(ThreadStartCallback cb) { _valid = true; _threadStartCb = std::move(cb); return *this; }
    Interceptors& onThreadExit(ThreadExitCallback cb)   { _valid = true; _threadExitCb = std::move(cb);  return *this; }

    ThreadStartCallback onThreadStart() const { return _threadStartCb; }
    ThreadExitCallback  onThreadExit()  const { return _threadExitCb; }

    bool empty() const { return !_valid; }

private:
    ThreadStartCallback _threadStartCb;
    ThreadExitCallback  _threadExitCb;
    bool                _valid  = false;
};

} } // end of KAFKA_API::clients

