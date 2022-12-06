#pragma once

#include <kafka/Project.h>

#include <kafka/ClientConfig.h>


namespace KAFKA_API { namespace clients { namespace consumer {

/**
 * Configuration for the Kafka Consumer.
 */
class ConsumerConfig: public Config
{
public:
    ConsumerConfig() = default;
    ConsumerConfig(const ConsumerConfig&) = default;
    explicit ConsumerConfig(const PropertiesMap& kvMap): Config(kvMap) {}


    /**
     * Group identifier.
     * Note: It's better to configure it manually, otherwise a random one would be used for it.
     *
     */
    static const constexpr char* GROUP_ID                = "group.id";

    /**
     * Automatically commits previously polled offsets on each `poll` operation.
     */
    static const constexpr char* ENABLE_AUTO_COMMIT      = "enable.auto.commit";

    /**
     * This property controls the behavior of the consumer when it starts reading a partition for which it doesn't have a valid committed offset.
     * The "latest" means the consumer will begin reading the newest records written after the consumer started. While "earliest" means that the consumer will read from the very beginning.
     * Available options: latest, earliest
     * Default value: latest
     */
    static const constexpr char* AUTO_OFFSET_RESET       = "auto.offset.reset";

    /**
     * Emit RD_KAFKA_RESP_ERR_PARTITION_EOF event whenever the consumer reaches the end of a partition.
     * Default value: false
     */
    static const constexpr char* ENABLE_PARTITION_EOF    = "enable.partition.eof";

    /**
     * This controls the maximum number of records that a single call to poll() will return.
     * Default value: 500
     */
    static const constexpr char* MAX_POLL_RECORDS        = "max.poll.records";

    /**
     * Minimum number of messages per topic/partition tries to maintain in the local consumer queue.
     * Note: With a larger value configured, the consumer would send FetchRequest towards brokers more frequently.
     * Defalut value: 100000
     */
    static const constexpr char* QUEUED_MIN_MESSAGES     = "queued.min.messages";

    /**
     * Client group session and failure detection timeout.
     * If no heartbeat received by the broker within this timeout, the broker will remove the consumer and trigger a rebalance.
     * Default value: 10000
     */
    static const constexpr char* SESSION_TIMEOUT_MS      = "session.timeout.ms";

    /**
     * Control how to read messages written transactionally.
     * Available options: read_uncommitted, read_committed
     * Default value: read_committed
     */
    static const constexpr char* ISOLATION_LEVEL         = "isolation.level";

    /*
     * The name of one or more partition assignment strategies.
     * The elected group leader will use a strategy supported by all members of the group to assign partitions to group members.
     * Available options: range, roundrobin, cooperative-sticky
     * Default value: range,roundrobin
     */
    static const constexpr char* PARTITION_ASSIGNMENT_STRATEGY = "partition.assignment.strategy";
};

} } } // end of KAFKA_API::clients::consumer

