#pragma once

#include <kafka/Project.h>

#include <kafka/ClientConfig.h>


namespace KAFKA_API { namespace clients { namespace admin {

/**
 * Configuration for the Kafka Consumer.
 */
class AdminClientConfig: public Config
{
public:
    AdminClientConfig() = default;
    AdminClientConfig(const AdminClientConfig&) = default;
    explicit AdminClientConfig(const PropertiesMap& kvMap): Config(kvMap) {}
};

} } } // end of KAFKA_API::clients::admin

