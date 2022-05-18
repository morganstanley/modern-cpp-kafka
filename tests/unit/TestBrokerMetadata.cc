#include "kafka/BrokerMetadata.h"

#include "gtest/gtest.h"


TEST(BrokerMetadata, Node)
{
    const kafka::BrokerMetadata::Node::Id id     = 1;
    const kafka::BrokerMetadata::Node::Host host = "127.0.0.1";
    const kafka::BrokerMetadata::Node::Port port = 9000;

    kafka::BrokerMetadata::Node node(id, host, port);

    EXPECT_EQ(id,   node.id);
    EXPECT_EQ(host, node.host);
    EXPECT_EQ(port, node.port);
    EXPECT_EQ("127.0.0.1:9000/1", node.toString());
}

TEST(BrokerMetadata, Basic)
{
    const kafka::Topic topic("topicName");
    const std::vector<kafka::BrokerMetadata::Node> nodes = {{1, "server1", 9000}, {2, "server2", 9000}, {3, "server3", 9000}};
    const std::size_t numNode      = nodes.size();
    const std::size_t numPartition = numNode;

    kafka::BrokerMetadata metadata(topic);
    metadata.setOrigNodeName(nodes[0].host);

    // Add nodes
    for (const auto& node: nodes)
    {
        metadata.addNode(node.id, node.host, node.port);
    }

    EXPECT_EQ(nodes.size(), metadata.nodes().size());

    // Add info for partitions
    for (kafka::Partition partition = 0; partition < static_cast<int>(numPartition); ++partition)
    {
        kafka::BrokerMetadata::PartitionInfo partitionInfo(nodes[static_cast<std::size_t>(partition)].id);
        for (const auto& node: nodes)
        {
            partitionInfo.addReplica(node.id);
            partitionInfo.addInSyncReplica(node.id);
        }
        metadata.addPartitionInfo(partition, partitionInfo);
    }

    EXPECT_EQ(topic, metadata.topic());
    EXPECT_EQ(numPartition, metadata.partitions().size());

    for (kafka::Partition partition = 0; partition < static_cast<int>(numPartition); ++partition)
    {
        const auto& partitionInfo = metadata.partitions().at(partition);
        EXPECT_EQ(nodes[static_cast<std::size_t>(partition)].id, partitionInfo.leader);
        EXPECT_EQ(numNode, partitionInfo.replicas.size());
        EXPECT_EQ(numNode, partitionInfo.inSyncReplicas.size());
    }

    std::string expectedMetadata = std::string("originatingNode[server1], topic[topicName], partitions{")
        + "0: leader[server1:9000/1], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]; "
        + "1: leader[server2:9000/2], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]; "
        + "2: leader[server3:9000/3], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]}";
    EXPECT_EQ(expectedMetadata, metadata.toString());
}

TEST(BrokerMetadata, IncompleteInfo)
{
    const kafka::Topic topic("topicName");
    const std::vector<kafka::BrokerMetadata::Node> nodes = {{1, "server1", 9000}, {2, "server2", 9000}, {3, "server3", 9000}};
    const std::size_t numNode      = nodes.size();
    const std::size_t numPartition = numNode;

    kafka::BrokerMetadata metadata(topic);
    metadata.setOrigNodeName(nodes[0].host);

    // Add nodes (not complete)
    metadata.addNode(nodes[0].id, nodes[0].host, nodes[0].port);

    // Add info for partitions
    for (kafka::Partition partition = 0; partition < static_cast<int>(numPartition); ++partition)
    {
        kafka::BrokerMetadata::PartitionInfo partitionInfo(nodes[static_cast<std::size_t>(partition)].id);
        for (const auto& node: nodes)
        {
            partitionInfo.addReplica(node.id);
            partitionInfo.addInSyncReplica(node.id);
        }
        metadata.addPartitionInfo(partition, partitionInfo);
    }

    std::string expectedMetadata = std::string("originatingNode[server1], topic[topicName], partitions{")
        + "0: leader[server1:9000/1], replicas[server1:9000/1, -:-/2, -:-/3], inSyncReplicas[server1:9000/1, -:-/2, -:-/3]; "
        + "1: leader[-:-/2], replicas[server1:9000/1, -:-/2, -:-/3], inSyncReplicas[server1:9000/1, -:-/2, -:-/3]; "
        + "2: leader[-:-/3], replicas[server1:9000/1, -:-/2, -:-/3], inSyncReplicas[server1:9000/1, -:-/2, -:-/3]}";
    EXPECT_EQ(expectedMetadata, metadata.toString());
}

