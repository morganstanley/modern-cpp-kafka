#pragma once

#include <chrono>
#include <kafka/Project.h>

#include <kafka/Error.h>

#include <functional>


namespace KAFKA_API { namespace clients {

    /**
     * Callback type for logging.
     */
    using LogCallback   = std::function<void(int, const char*, int, const char* msg)>;

    /**
     * Callback type for error notification.
     */
    using ErrorCallback = std::function<void(const Error&)>;

    /**
     * Callback type for statistics info dumping.
     */
    using StatsCallback = std::function<void(const std::string&)>;

    /**
     * SASL OAUTHBEARER token info.
     */
    struct SaslOauthbearerToken
    {
        using KeyValuePairs = std::map<std::string, std::string>;

        std::string               value;
        std::chrono::milliseconds mdLifetime{};
        std::string               mdPrincipalName;
        KeyValuePairs             extensions;
    };

    /**
     * Callback type for OAUTHBEARER token refresh.
     */
    using OauthbearerTokenRefreshCallback = std::function<SaslOauthbearerToken(const std::string&)>;

} } // end of KAFKA_API::clients

