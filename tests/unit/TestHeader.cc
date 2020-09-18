#include "kafka/Header.h"

#include "gtest/gtest.h"

#include <algorithm>

namespace Kafka = KAFKA_API;

TEST(Header, Basic)
{
    Kafka::Header defaultHeader;

    std::string v = "v";
    Kafka::Header::Value value{v.c_str(), v.size()};
    Kafka::Header header("k", value);
    EXPECT_EQ("k", header.key);
    EXPECT_EQ(v.c_str(), header.value.data());
    EXPECT_EQ(v.size(), header.value.size());
    EXPECT_EQ("k:v", header.toString());
}

TEST(Header, Headers)
{
    std::vector<std::pair<std::string, std::string>> kvs =
    {
        {"k1", "v1"},
        {"k2", "v2"},
        {"k3", "v3"}
    };

    Kafka::Headers headers;
    std::for_each(kvs.cbegin(), kvs.cend(),
                  [&headers](const auto& kv) { headers.emplace_back(kv.first, Kafka::Header::Value(kv.second.c_str(), kv.second.size())); });

    EXPECT_EQ("k1:v1,k2:v2,k3:v3", Kafka::toString(headers));
}

