#pragma once

#include "kafka/Project.h"

#include "kafka/KafkaException.h"
#include "kafka/Types.h"

#include "librdkafka/rdkafka.h"

#include <vector>


namespace KAFKA_API {

/**
 * The metadata info for a topic.
 */
struct BrokerMetadata {
    /**
     * Information for a Kafka node.
     */
    struct Node
    {
      public:
        using Id   = int;
        using Host = std::string;
        using Port = int;

        Node(Id i, Host h, Port p): id(i), host(std::move(h)), port(p) {}

        /**
         * The node id.
         */
        Node::Id  id;

        /**
         * The host name.
         */
        Node::Host host;

        /**
         * The port.
         */
        Node::Port port;

        /**
         * Obtains explanatory string.
         */
        std::string toString() const { return host + ":" + std::to_string(port) + "/" + std::to_string(id); }
    };

    /**
     * It is used to describe per-partition state in the MetadataResponse.
     */
    struct PartitionInfo
    {
        void setLeader(const std::shared_ptr<Node>& ldr)            { leader = ldr; }
        void addReplica(const std::shared_ptr<Node>& replica)       { replicas.emplace_back(replica); }
        void addInSyncReplica(const std::shared_ptr<Node>& replica) { inSyncReplicas.emplace_back(replica); }

        /**
         * The node currently acting as a leader for this partition or null if there is no leader.
         */
        std::shared_ptr<Node> leader;

        /**
         * The complete set of replicas for this partition regardless of whether they are alive or up-to-date.
         */
        std::vector<std::shared_ptr<Node>> replicas;

        /**
         * The subset of the replicas that are in sync, that is caught-up to the leader and ready to take over as leader if the leader should fail.
         */
        std::vector<std::shared_ptr<Node>> inSyncReplicas;

        /**
         * Obtains explanatory string.
         */
        std::string toString() const;
    };

    /**
     * The BrokerMetadata is per-topic constructed
     */
    explicit BrokerMetadata(Topic topic): _topic(std::move(topic)) {}

    /**
     * The topic name.
     */
    std::string topic()      const { return _topic; }

    /**
     * The partitions' state in the MetadataResponse.
     */
    const std::map<Partition, PartitionInfo>& partitions() const { return _partitions; }

    /**
     * Obtains explanatory string.
     */
    std::string toString()   const;

    void setOrigNodeName(const std::string& origNodeName)                          { _origNodeName = origNodeName; }
    void addNode(Node::Id nodeId, const Node::Host& host, Node::Port port)         { _nodes[nodeId] = std::make_shared<Node>(nodeId, host, port); }
    std::shared_ptr<Node> getNode(Node::Id nodeId)                                 { return _nodes[nodeId]; }
    void addPartitionInfo(Partition partition, const PartitionInfo& partitionInfo) { _partitions.emplace(partition, partitionInfo); }

private:
    Topic                                     _topic;
    std::string                               _origNodeName;
    std::map<Node::Id, std::shared_ptr<Node>> _nodes;
    std::map<Partition, PartitionInfo>        _partitions;
};

inline std::string
BrokerMetadata::PartitionInfo::toString() const
{
    std::ostringstream oss;

    auto streamNodes = [](std::ostringstream& ss, const std::vector<std::shared_ptr<Node>>& nodes) -> std::ostringstream& {
        bool isTheFirst = true;
        std::for_each(nodes.cbegin(), nodes.cend(),
                      [&isTheFirst, &ss](const auto& node) {
                          ss << (isTheFirst ? (isTheFirst = false, "") : ", ") << node->toString();
                      });
        return ss;
    };

    oss << "leader[" << leader->toString() << "], replicas[";
    streamNodes(oss, replicas) << "], inSyncReplicas[";
    streamNodes(oss, inSyncReplicas) << "]";

    return oss.str();
}

inline std::string
BrokerMetadata::toString() const
{
    std::ostringstream oss;

    oss << "originatingNode[" << _origNodeName << "], topic[" << _topic <<  "], partitions{";
    bool isTheFirst = true;
    for (const auto& partitionInfoPair: _partitions)
    {
        const Partition       partition     = partitionInfoPair.first;
        const PartitionInfo&  partitionInfo = partitionInfoPair.second;
        oss << (isTheFirst ? (isTheFirst = false, "") : "; ") << partition << ": " << partitionInfo.toString();
    }
    oss << "}";

    return oss.str();
}

}

