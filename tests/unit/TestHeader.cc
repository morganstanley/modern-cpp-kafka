#include "kafka/Header.h"

#include "gtest/gtest.h"

#include <algorithm>


TEST(Header, Basic)
{
    const kafka::Header defaultHeader;

    EXPECT_EQ("", defaultHeader.key);
    EXPECT_EQ(0, defaultHeader.value.size());
    EXPECT_EQ(nullptr, defaultHeader.value.data());
    EXPECT_EQ("[null]:[null]", defaultHeader.toString());

    const std::string v = "v";
    const kafka::Header header("k", kafka::Header::Value{v.c_str(), v.size()});

    EXPECT_EQ("k", header.key);
    EXPECT_EQ(v.c_str(), header.value.data());
    EXPECT_EQ(v.size(), header.value.size());
    EXPECT_EQ("k:v", header.toString());
}

TEST(Header, Headers)
{
    const std::vector<std::pair<std::string, std::string>> kvs =
    {
        {"k1", "v1"},
        {"k2", "v2"},
        {"k3", "v3"}
    };

    kafka::Headers headers;
    std::for_each(kvs.cbegin(), kvs.cend(),
                  [&headers](const auto& kv) { headers.emplace_back(kv.first, kafka::Header::Value(kv.second.c_str(), kv.second.size())); });

    EXPECT_EQ("k1:v1,k2:v2,k3:v3", kafka::toString(headers));
}

