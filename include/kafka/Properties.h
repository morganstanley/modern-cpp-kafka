#pragma once

#include <kafka/Project.h>

#include <kafka/ClientCommon.h>
#include <kafka/Error.h>
#include <kafka/Interceptors.h>
#include <kafka/KafkaException.h>
#include <kafka/Types.h>

#include <algorithm>
#include <map>
#include <regex>
#include <string>
#include <typeinfo>


namespace KAFKA_API {

/**
 * The properties for Kafka clients.
 */
class Properties
{
private:
    using LogCallback                     = clients::LogCallback;
    using ErrorCallback                   = clients::ErrorCallback;
    using StatsCallback                   = clients::StatsCallback;
    using OauthbearerTokenRefreshCallback = clients::OauthbearerTokenRefreshCallback;
    using Interceptors                    = clients::Interceptors;

    struct ValueType
    {
        struct Object
        {
            virtual ~Object() = default;
            virtual std::string toString() const = 0;
        };

        template<class T>
        static std::string getString(const T& /*value*/) { return typeid(T).name(); }
        template<class T>
        static std::string getString(const std::string& value) { return value; }

        const ValueType& validate(const std::string& key) const
        {
            static const std::vector<std::string> nonStringValueKeys = {
                "log_cb", "error_cb", "stats_cb", "oauthbearer_token_refresh_cb", "interceptors"
            };

            if ((expectedKey.empty() && std::any_of(nonStringValueKeys.cbegin(), nonStringValueKeys.cend(), [key](const auto& k) { return k == key; }))
               || (!expectedKey.empty() && key != expectedKey))
            {
                throw std::runtime_error("Invalid key/value for configuration: " + key);
            }

            return *this;
        }

        template<class T>
        struct ObjWrap: public Object
        {
            explicit ObjWrap(T v): value(std::move(v)) {}
            std::string toString() const override { return getString<T>(value); }
            T value;
        };

        template<class T>
        T& getValue() const { return (dynamic_cast<ObjWrap<T>&>(*object)).value; }

        ValueType() = default;

        ValueType(const std::string& value)                                         // NOLINT
        { object = std::make_shared<ObjWrap<std::string>>(value); }

        ValueType(const LogCallback& cb)                                            // NOLINT
            : expectedKey("log_cb")
        { object = std::make_shared<ObjWrap<LogCallback>>(cb);    }

        ValueType(const ErrorCallback& cb)                                          // NOLINT
            : expectedKey("error_cb")
        { object = std::make_shared<ObjWrap<ErrorCallback>>(cb);  }

        ValueType(const StatsCallback& cb)                                          // NOLINT
            : expectedKey("stats_cb")
        { object = std::make_shared<ObjWrap<StatsCallback>>(cb);  }

        ValueType(const OauthbearerTokenRefreshCallback& cb)                        // NOLINT
            : expectedKey("oauthbearer_token_refresh_cb")
        { object = std::make_shared<ObjWrap<OauthbearerTokenRefreshCallback>>(cb); }

        ValueType(const Interceptors& interceptors)                                 // NOLINT
            : expectedKey("interceptors")
        { object = std::make_shared<ObjWrap<Interceptors>>(interceptors); }

        bool operator==(const ValueType& rhs) const { return toString() == rhs.toString(); }

        std::string toString() const { return object->toString(); }

    private:
        std::string             expectedKey;
        std::shared_ptr<Object> object;
    };

public:
    // Just make sure key will printed in order
    using PropertiesMap = std::map<std::string, ValueType>;

    Properties() = default;
    Properties(const Properties&) = default;
    Properties(PropertiesMap kvMap): _kvMap(std::move(kvMap))   // NOLINT
    {
        for (const auto& kv: _kvMap)
        {
            kv.second.validate(kv.first);
        }
    }
    virtual ~Properties() = default;

    bool operator==(const Properties& rhs) const { return map() == rhs.map(); }

    /**
     * Set a property.
     * If the map previously contained a mapping for the key, the old value is replaced by the specified value.
     */
    template <class T>
    Properties& put(const std::string& key, const T& value)
    {
        _kvMap[key] = ValueType(value).validate(key);
        return *this;
    }

    /**
     * Remove the property (if one exists).
     */
    void remove(const std::string& key)
    {
        _kvMap.erase(key);
    }

    /**
     * Check whether the map contains a property.
     */
    bool contains(const std::string& key) const
    {
        auto search = _kvMap.find(key);
        return search != _kvMap.end();
    }

    /**
     * Get a property reference.
     * If the property doesn't exist, an execption would be thrown.
     */
    template<class T>
    T& get(const std::string& key) const
    {
        auto search = _kvMap.find(key);
        if (search == _kvMap.end())
        {
            KAFKA_THROW_ERROR(Error(RD_KAFKA_RESP_ERR__INVALID_ARG, "Failed to get \"" + key + "\" from Properties!"));
        }

        const ValueType& v = search->second;
        return v.getValue<T>();
    }

    /**
     * Get a property.
     */
    Optional<std::string> getProperty(const std::string& key) const
    {
        if (!contains(key)) return Optional<std::string>{};

        try
        {
            return get<std::string>(key);
        }
        catch (const std::bad_cast&)
        {
            return Optional<std::string>{};
        }
    }

    /**
     * Remove a property.
     */
    void eraseProperty(const std::string& key)
    {
      _kvMap.erase(key);
    }

    std::string toString() const
    {

        std::string ret;
        std::for_each(_kvMap.cbegin(), _kvMap.cend(),
                      [&ret](const auto& kv) {
                          const std::string& key   = kv.first;
                          const std::string  value = kv.second.toString();

                          static const std::regex reSensitiveKey(R"(.+\.password|.+\.username|.+secret|.+key|.+pem)");
                          const bool isSensitive = std::regex_match(key, reSensitiveKey);

                          ret.append(ret.empty() ? "" : "|").append(key).append("=").append(isSensitive ? "*" : value);
                      });
        return ret;
    }

    /**
     * Get all properties with a map.
     */
    const PropertiesMap& map() const { return _kvMap; }

private:
    PropertiesMap _kvMap;
};

} // end of KAFKA_API

