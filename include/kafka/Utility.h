#pragma once

#include "kafka/Project.h"

#include <chrono>
#include <iomanip>
#include <random>
#include <sstream>
#include <string>
#include <time.h>

namespace KAFKA_API {

namespace Utility {

/**
 * Get current local time as string.
 */
inline std::string getCurrentTime()
{
    using namespace std::chrono;
    auto current = system_clock::now();
    auto micros = duration_cast<microseconds>(current.time_since_epoch()) % 1000000;
    auto time = system_clock::to_time_t(current);

    std::ostringstream oss;
    std::tm tmBuf = {};
    oss << std::put_time(localtime_r(&time, &tmBuf), "%F %T") <<  "." << std::setfill('0') << std::setw(6) << micros.count();
    return oss.str();
}

/**
 * Get random string.
 */
inline std::string getRandomString()
{
    using namespace std::chrono;
    std::uint32_t timestamp = static_cast<std::uint32_t>(duration_cast<nanoseconds>(system_clock::now().time_since_epoch()).count());

    std::random_device r;
    std::default_random_engine e(r());
    std::uniform_int_distribution<std::uint64_t> uniform_dist(0, 0xFFFFFFFF);
    int rand = uniform_dist(e);

    std::ostringstream oss;
    oss << std::setfill('0') << std::setw(sizeof(std::uint32_t) * 2) << std::hex << timestamp << "-" << rand;
    return oss.str();
}

/**
 * Get librdkafka version string.
 */
inline std::string getLibRdKafkaVersion()
{
    return rd_kafka_version_str();
}

/**
 * Current number of threads created by rdkafka.
 */
inline int getLibRdKafkaThreadCount()
{
    return rd_kafka_thread_cnt();
}

} // end of Utility

} // end of KAFKA_API

