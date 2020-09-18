#pragma once

#include "kafka/Project.h"

#include "kafka/Utility.h"

#include <cassert>
#include <functional>
#include <iostream>

namespace KAFKA_API {

template <int MAX_CAPACITY>
class LogBuffer
{
public:
    LogBuffer():_wptr(_buf) { _buf[0] = 0; } // NOLINT

    LogBuffer& clear()
    {
        _wptr = _buf;
        _buf[0] = 0;
        return *this;
    }

    template<class ...Args>
    LogBuffer& print(const char* format, Args... args)
    {
        assert(!(_buf[0] != 0 && _wptr == _buf)); // means it has already been used as a plain buffer (with `str()`)

        auto cnt = std::snprintf(_wptr, capacity(), format, args...); // returns number of characters written if successful (not including '\0')
        if (cnt > 0)
        {
            _wptr = std::min(_wptr + cnt, _buf + MAX_CAPACITY - 1);
        }
        return *this;
    }
    LogBuffer& print(const char* format) { return print("%s", format); }

    int capacity() const { return _buf + MAX_CAPACITY - _wptr; }
    char* str() { return _buf; }
    const char* c_str() const { return _buf; }

private:
    char* _wptr;
    char _buf[MAX_CAPACITY];
};

using Logger = std::function<void(int, const char*, int, const char* msg)>;

inline void DefaultLogger(int level, const char* /*filename*/, int /*lineno*/, const char* msg)
{
    const char levelNames[][10] = {"EMERG", "ALERT", "CRIT", "ERR", "WARNING", "NOTICE", "INFO", "DEBUG", "INVALID"};
    constexpr int INVALID_LEVEL = sizeof(levelNames)/sizeof(levelNames[0]) - 1;
    const char* levelName = level < INVALID_LEVEL ? levelNames[level] : levelNames[INVALID_LEVEL];
    std::cout << "[" << Utility::getCurrentTime() << "]" << levelName << " " << msg;
    std::cout << std::endl;
}

inline void NoneLogger(int /*level*/, const char* /*filename*/, int /*lineno*/, const char* /*msg*/)
{
}

} // end of KAFKA_API

