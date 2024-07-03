#pragma once

#include <kafka/Project.h>

#include <kafka/Utility.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <functional>
#include <iostream>
#include <mutex>


namespace KAFKA_API {

struct Log
{
    enum Level
    {
        Emerg   = 0,
        Alert   = 1,
        Crit    = 2,
        Err     = 3,
        Warning = 4,
        Notice  = 5,
        Info    = 6,
        Debug   = 7
    };

    static const std::string& levelString(std::size_t level)
    {
        static const std::vector<std::string> levelNames = {"EMERG", "ALERT", "CRIT", "ERR", "WARNING", "NOTICE", "INFO", "DEBUG", "INVALID"};
        static const std::size_t              maxIndex   = levelNames.size() - 1;

        return levelNames[(std::min)(level, maxIndex)];
    }
};


// Log Buffer
template <std::size_t MAX_CAPACITY>
class LogBuffer
{
public:
    LogBuffer() { clear(); }

    LogBuffer& clear()
    {
        _buf[0] = 0;
        _wptr = _buf.data();
        return *this;
    }

    template<class ...Args>
    LogBuffer& print(const char* format, Args... args)
    {
        assert(!(_buf[0] != 0 && _wptr == _buf.data())); // means it has already been used as a plain buffer (with `str()`)

        auto cnt = std::snprintf(_wptr, capacity(), format, args...); // returns number of characters written if successful (not including '\0')
        if (cnt > 0)
        {
            _wptr = (std::min)(_wptr + cnt, _buf.data() + MAX_CAPACITY - 1);
        }
        return *this;
    }
    LogBuffer& print(const char* format) { return print("%s", format); }

    std::size_t capacity() const { return static_cast<size_t>(_buf.data() + MAX_CAPACITY - _wptr); }
    char* str() { return _buf.data(); }
    const char* c_str() const { return _buf.data(); }

private:
    std::array<char, MAX_CAPACITY> _buf;
    char*                          _wptr = nullptr;
};


// Default Logger
inline void DefaultLogger(int level, const char* /*filename*/, int /*lineno*/, const char* msg)
{
    std::cout << "[" << utility::getCurrentTime() << "]" << Log::levelString(static_cast<std::size_t>(level)) << " " << msg;
    std::cout << std::endl;
}

// Null Logger
inline void NullLogger(int /*level*/, const char* /*filename*/, int /*lineno*/, const char* /*msg*/)
{
}


// Global Logger
template <typename T = void>
struct GlobalLogger
{
    static clients::LogCallback logCb;
    static std::once_flag       initOnce;

    static const constexpr int LOG_BUFFER_SIZE = 1024;

    template<class ...Args>
    static void doLog(int level, const char* filename, int lineno, const char* format, Args... args)
    {
        if (!GlobalLogger<>::logCb) return;

        LogBuffer<LOG_BUFFER_SIZE> logBuffer;
        logBuffer.print(format, args...);
        GlobalLogger<>::logCb(level, filename, lineno, logBuffer.c_str());
    }
};

template <typename T>
clients::LogCallback GlobalLogger<T>::logCb;

template <typename T>
std::once_flag GlobalLogger<T>::initOnce;

/**
 * Set a global log interface for kafka API (Note: it takes no effect on Kafka clients).
 */
inline void setGlobalLogger(clients::LogCallback cb)
{
    std::call_once(GlobalLogger<>::initOnce, [](){}); // Then no need to init within the first KAFKA_API_LOG call.
    GlobalLogger<>::logCb = std::move(cb);
}

/**
 * Log for kafka API (Note: not for Kafka client instances).
 *
 * E.g,
 *     KAFKA_API_LOG(Log::Level::Err, "something wrong happened! %s", detailedInfo.c_str());
 */
#define KAFKA_API_LOG(level, ...) do {                                                                \
    std::call_once(GlobalLogger<>::initOnce, [](){ GlobalLogger<>::logCb = DefaultLogger; });       \
    GlobalLogger<>::doLog(level, __FILE__, __LINE__, ##__VA_ARGS__);                               \
} while (0)


} // end of KAFKA_API

