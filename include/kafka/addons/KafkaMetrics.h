#pragma once

#include <kafka/Project.h>

// https://github.com/Tencent/rapidjson/releases/tag/v1.1.0
#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>


namespace KAFKA_API {

/**
 * \brief Helps to parse the metrics string with JSON format.
 */
class KafkaMetrics
{
public:
    /**
     * \brief Initilize with the metrics string.
     */
    explicit KafkaMetrics(std::string jsonMetrics);

    static const constexpr char* WILDCARD = "*";

    using KeysType = std::vector<std::string>;

    /**
     * \brief The matched keys (for wildcards) and the value.
     */
    template<typename ValueType>
    using ResultsType = std::vector<std::pair<KeysType, ValueType>>;

    /**
     * \brief Get integer value(s) for the specified metrics.
     * Note: the wildcard ("*") is supported.
     */
    ResultsType<std::int64_t> getInt(const KeysType& keys) const { return get<std::int64_t>(keys); }

    /**
     * \brief Get string value(s) for the specified metrics.
     * Note: the wildcard ("*") is supported.
     */
    ResultsType<std::string> getString(const KeysType& keys) const { return get<std::string>(keys); }

    static std::string toString(const KafkaMetrics::KeysType& keys);

    template<typename ValueType>
    static std::string toString(const KafkaMetrics::ResultsType<ValueType>& results);

private:
    template<typename ValueType>
    ResultsType<ValueType> get(const KeysType& keys) const;

    template<typename ValueType>
    static void getResults(ResultsType<ValueType>&               results,
                           KeysType&                             keysForWildcards,
                           rapidjson::Value::ConstMemberIterator iter,
                           KeysType::const_iterator              keysToParse,
                           KeysType::const_iterator              keysEnd);

    template<typename ValueType>
    static ValueType getValue(rapidjson::Value::ConstMemberIterator iter);

#if COMPILER_SUPPORTS_CPP_17
    std::string         _decodeBuf;
#else
    std::vector<char>   _decodeBuf;
#endif
    rapidjson::Document _jsonDoc;
};

inline
KafkaMetrics::KafkaMetrics(std::string jsonMetrics)
#if COMPILER_SUPPORTS_CPP_17
    : _decodeBuf(std::move(jsonMetrics))
#else
    : _decodeBuf(jsonMetrics.cbegin(), jsonMetrics.cend() + 1)
#endif
{
    if (_jsonDoc.ParseInsitu(_decodeBuf.data()).HasParseError())
    {
        throw std::runtime_error("Failed to parse string with JSON format!");
    }
}

template<>
inline std::int64_t
KafkaMetrics::getValue<std::int64_t>(rapidjson::Value::ConstMemberIterator iter)
{
    return iter->value.GetInt();
}

template<>
inline std::string
KafkaMetrics::getValue<std::string>(rapidjson::Value::ConstMemberIterator iter)
{
    return iter->value.GetString();
}

template<typename ValueType>
inline KafkaMetrics::ResultsType<ValueType>
KafkaMetrics::get(const KeysType& keys) const
{
    if (keys.empty())             throw std::invalid_argument("Input keys cannot be empty!");
    if (keys.front() == WILDCARD) throw std::invalid_argument("The first key cannot be wildcard!");
    if (keys.back() == WILDCARD)  throw std::invalid_argument("The last key cannot be wildcard!");

    ResultsType<ValueType> results;

    const rapidjson::Value::ConstMemberIterator iter = _jsonDoc.FindMember(keys.front().c_str());
    if (iter == _jsonDoc.MemberEnd()) return results;

    if (keys.size() == 1)
    {
        if (std::is_same<ValueType, std::string>::value ? iter->value.IsString() : iter->value.IsInt())
        {
            results.emplace_back(KeysType{}, getValue<ValueType>(iter));
        }

        return results;
    }

    KeysType keysForWildcards;

    getResults(results, keysForWildcards, iter, keys.cbegin() + 1, keys.cend());
    return results;
}

template<typename ValueType>
inline void
KafkaMetrics::getResults(KafkaMetrics::ResultsType<ValueType>& results,
                         KeysType&                             keysForWildcards,
                         rapidjson::Value::ConstMemberIterator iter,
                         KeysType::const_iterator              keysToParse,
                         KeysType::const_iterator              keysEnd)
{
    if (!iter->value.IsObject()) return;

    const auto& key      = *(keysToParse++);
    const bool  isTheEnd = (keysToParse == keysEnd);

    if (key == WILDCARD)
    {
        for (rapidjson::Value::ConstMemberIterator subIter = iter->value.MemberBegin(); subIter != iter->value.MemberEnd(); ++subIter)
        {
            KeysType newKeysForWildcards = keysForWildcards;
            newKeysForWildcards.emplace_back(subIter->name.GetString());

            getResults(results, newKeysForWildcards, subIter, keysToParse, keysEnd);
        }
    }
    else
    {
        const rapidjson::Value::ConstMemberIterator subIter = iter->value.FindMember(key.c_str());
        if (subIter == iter->value.MemberEnd()) return;

        if (!isTheEnd)
        {
            getResults(results, keysForWildcards, subIter, keysToParse, keysEnd);
        }
        else if (std::is_same<ValueType, std::string>::value ? subIter->value.IsString() : subIter->value.IsInt())
        {
            results.emplace_back(keysForWildcards, getValue<ValueType>(subIter));
        }
    }
}

inline std::string
KafkaMetrics::toString(const KafkaMetrics::KeysType& keys)
{
    std::string ret;

    std::for_each(keys.cbegin(), keys.cend(),
                  [&ret](const auto& key){ ret.append((ret.empty() ? std::string() : std::string(", ")) + "\"" + key + "\""); });

    return ret;
}

template<typename ValueType>
inline std::string
KafkaMetrics::toString(const KafkaMetrics::ResultsType<ValueType>& results)
{
    std::ostringstream oss;
    bool isTheFirstOne = true;

    std::for_each(results.cbegin(), results.cend(),
                  [&oss, &isTheFirstOne](const auto& result) {
                     const auto keysString  = toString(result.first);

                     oss << (isTheFirstOne ? (isTheFirstOne = false, "") : ", ")
                         << (keysString.empty() ? "" : (std::string("[") + keysString + "]:"));
                     oss << (std::is_same<ValueType, std::string>::value ? "\"" : "")  << result.second << (std::is_same<ValueType, std::string>::value ? "\"" : "");
                  });

    return oss.str();
}

} // end of KAFKA_API

