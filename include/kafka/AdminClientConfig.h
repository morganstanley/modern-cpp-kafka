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
    static const constexpr char* SECURITY_PROTOCOL          = "security.protocol";

    /**
     * Shell command to refresh or acquire the client's Kerberos ticket.
     */
    static const constexpr char* SASL_KERBEROS_KINIT_CMD    = "sasl.kerberos.kinit.cmd";

    /**
     * The client's Kerberos principal name.
     */
    static const constexpr char* SASL_KERBEROS_SERVICE_NAME = "sasl.kerberos.service.name";
};

} } } // end of KAFKA_API::clients::admin

