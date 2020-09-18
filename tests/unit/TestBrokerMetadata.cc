#include "kafka/BrokerMetadata.h"

#include "gtest/gtest.h"

namespace Kafka = KAFKA_API;

TEST(BrokerMetadata, Node)
{
    Kafka::BrokerMetadata::Node::Id id     = 1;
    Kafka::BrokerMetadata::Node::Host host = "127.0.0.1";
    Kafka::BrokerMetadata::Node::Port port = 9000;

    Kafka::BrokerMetadata::Node node(id, host, port);

    EXPECT_EQ(id,   node.id);
    EXPECT_EQ(host, node.host);
    EXPECT_EQ(port, node.port);
    EXPECT_EQ("127.0.0.1:9000/1", node.toString());
}

TEST(BrokerMetadata, PartitionInfo)
{
    auto node1 = std::make_shared<Kafka::BrokerMetadata::Node>(1, "leader_node",     9001);
    auto node2 = std::make_shared<Kafka::BrokerMetadata::Node>(2, "in_sync_node",     9002);
    auto node3 = std::make_shared<Kafka::BrokerMetadata::Node>(3, "out_of_sync_node", 9003);

    Kafka::BrokerMetadata::PartitionInfo partitionInfo;

    partitionInfo.setLeader(node1);
    partitionInfo.addInSyncReplica(node1);
    partitionInfo.addInSyncReplica(node2);
    partitionInfo.addReplica(node1);
    partitionInfo.addReplica(node2);
    partitionInfo.addReplica(node3);

    std::string expectedPartitionInfo = std::string("leader[leader_node:9001/1], ")
        + "replicas[leader_node:9001/1, in_sync_node:9002/2, out_of_sync_node:9003/3], "
        + "inSyncReplicas[leader_node:9001/1, in_sync_node:9002/2]";
    EXPECT_EQ(expectedPartitionInfo, partitionInfo.toString());
}

TEST(BrokerMetadata, Basic)
{
    Kafka::Topic topic("topicName");
    std::vector<Kafka::BrokerMetadata::Node> nodes = {{1, "server1", 9000}, {2, "server2", 9000}, {3, "server3", 9000}};
    int numNode      = nodes.size();
    int numPartition = numNode;

    Kafka::BrokerMetadata metadata(topic);
    metadata.setOrigNodeName(nodes[0].host);

    // Add nodes
    for (const auto& node: nodes)
    {
        metadata.addNode(node.id, node.host, node.port);
    }

    // Add info for partitions
    for (Kafka::Partition partition = 0; partition < numPartition; ++partition)
    {
        Kafka::BrokerMetadata::PartitionInfo partitionInfo;
        partitionInfo.setLeader(metadata.getNode(nodes[partition].id));
        for (const auto& node: nodes)
        {
            partitionInfo.addReplica(metadata.getNode(node.id));
            partitionInfo.addInSyncReplica(metadata.getNode(node.id));
        }
        metadata.addPartitionInfo(partition, partitionInfo);
    }

    EXPECT_EQ(topic, metadata.topic());
    EXPECT_EQ(numPartition, metadata.partitions().size());

    for (Kafka::Partition partition = 0; partition < numPartition; ++partition)
    {
        const auto& partitionInfo = metadata.partitions().at(partition);
        EXPECT_EQ(nodes[partition].id,   partitionInfo.leader->id);
        EXPECT_EQ(nodes[partition].host, partitionInfo.leader->host);
        EXPECT_EQ(nodes[partition].port, partitionInfo.leader->port);
        EXPECT_EQ(numNode, partitionInfo.replicas.size());
        EXPECT_EQ(numNode, partitionInfo.inSyncReplicas.size());
    }

    std::string expectedMetadata = std::string("originatingNode[server1], topic[topicName], partitions{")
        + "0: leader[server1:9000/1], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]; "
        + "1: leader[server2:9000/2], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]; "
        + "2: leader[server3:9000/3], replicas[server1:9000/1, server2:9000/2, server3:9000/3], inSyncReplicas[server1:9000/1, server2:9000/2, server3:9000/3]}";
    EXPECT_EQ(expectedMetadata, metadata.toString());
}

