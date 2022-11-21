#pragma once

#include <kafka/Project.h>

#include <kafka/Properties.h>


namespace KAFKA_API { namespace clients { namespace admin {

/**
 * Configuration for the Kafka Consumer.
 */
class Config: public Properties
{
public:
    Config() = default;
    Config(const Config&) = default;
    explicit Config(const PropertiesMap& kvMap): Properties(kvMap) {}

    /**
     * The string contains host:port pairs of brokers (splitted by ",") that the administrative client will use to establish initial connection to the Kafka cluster.
     * Note: It's mandatory.
     */
    static const constexpr char* BOOTSTRAP_SERVERS          = "bootstrap.servers";

    /**
     * Protocol used to communicate with brokers.
     * Default value: plaintext
     */
    static const constexpr char* SECURITY_PROTOCOL            = "security.protocol";

    /**
     * SASL mechanism to use for authentication.
     * Default value: GSSAPI
     */
    static const constexpr char* SASL_MECHANISM               = "sasl.mechanisms";

    /**
     * SASL username for use with the PLAIN and SASL-SCRAM-.. mechanism.
     */
    static const constexpr char* SASL_USERNAME                = "sasl.username";

    /**
     * SASL password for use with the PLAIN and SASL-SCRAM-.. mechanism.
     */
    static const constexpr char* SASL_PASSWORD                = "sasl.password";

    /**
     * Shell command to refresh or acquire the client's Kerberos ticket.
     */
    static const constexpr char* SASL_KERBEROS_KINIT_CMD      = "sasl.kerberos.kinit.cmd";

    /**
     * The client's Kerberos principal name.
     */
    static const constexpr char* SASL_KERBEROS_SERVICE_NAME   = "sasl.kerberos.service.name";

    /**
     * Set to "default" or "oidc" to control with login method to be used.
     * If set to "oidc", the following properties must also be specified:
     *     sasl.oauthbearer.client.id
     *     sasl.oauthbearer.client.secret
     *     sasl.oauthbearer.token.endpoint.url
     * Default value: default
     */
    static const constexpr char* SASL_OAUTHBEARER_METHOD              = "sasl.oauthbearer.method";

    /**
     * Public identifier for the applicaition.
     * Only used with "sasl.oauthbearer.method=oidc".
     */
    static const constexpr char* SASL_OAUTHBEARER_CLIENT_ID           = "sasl.oauthbearer.client.id";

    /**
     * Client secret only known to the application and the authorization server.
     * Only used with "sasl.oauthbearer.method=oidc".
     */
    static const constexpr char* SASL_OAUTHBEARER_CLIENT_SECRET       = "sasl.oauthbearer.client.secret";

    /**
     * Allow additional information to be provided to the broker. Comma-separated list of key=value pairs.
     * Only used with "sasl.oauthbearer.method=oidc".
     */
    static const constexpr char* SASL_OAUTHBEARER_EXTENSIONS          = "sasl.oauthbearer.extensions";

    /**
     * Client use this to specify the scope of the access request to the broker.
     * Only used with "sasl.oauthbearer.method=oidc".
     */
    static const constexpr char* SASL_OAUTHBEARER_SCOPE               = "sasl.oauthbearer.scope";

    /**
     * OAuth/OIDC issuer token endpoint HTTP(S) URI used to retreve token.
     * Only used with "sasl.oauthbearer.method=oidc".
     */
    static const constexpr char* SASL_OAUTHBEARER_TOKEN_ENDPOINT_URL  = "sasl.oauthbearer.token.endpoint.url";

    /**
     * SASL/OAUTHBEARER configuration.
     * The format is implementation-dependent and must be parsed accordingly.
     */
    static const constexpr char* SASL_OAUTHBEARER_CONFIG              = "sasl.oauthbearer.config";

    /**
     * Enable the builtin unsecure JWT OAUTHBEARER token handler if no oauthbearer_refresh_cb has been set.
     * Should only be used for development or testing, and not in production.
     * Default value: false
     */
    static const constexpr char* ENABLE_SASL_OAUTHBEARER_UNSECURE_JWT = "enable.sasl.oauthbearer.unsecure.jwt";
};

} } } // end of KAFKA_API::clients::admin

