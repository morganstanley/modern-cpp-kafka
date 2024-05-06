#include <kafka/KafkaConsumer.h>

#include <atomic>
#include <cstdlib>
#include <iostream>
#include <string>

int main()
{
    using namespace kafka;
    using namespace kafka::clients::consumer;

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // The consumer would read all messages from the topic and then quit.

    // Prepare the configuration
    const Properties props({{"bootstrap.servers",    {brokers}},
                            // Emit RD_KAFKA_RESP_ERR__PARTITION_EOF event
                            // whenever the consumer reaches the end of a partition.
                            {"enable.partition.eof", {"true"}},
                            // Action to take when there is no initial offset in offset store
                            // it means the consumer would read from the very beginning
                            {"auto.offset.reset",    {"earliest"}}});

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Prepare the rebalance callbacks
    std::atomic<std::size_t> assignedPartitions{};
    auto rebalanceCb = [&assignedPartitions](kafka::clients::consumer::RebalanceEventType et, const kafka::TopicPartitions& tps) {
                           if (et == kafka::clients::consumer::RebalanceEventType::PartitionsAssigned) {
                               assignedPartitions += tps.size();
                               std::cout << "Assigned partitions: " << kafka::toString(tps) << std::endl;
                           } else {
                               assignedPartitions -= tps.size();
                               std::cout << "Revoked partitions: " << kafka::toString(tps) << std::endl;
                           }
                       };

    // Subscribe to topics with rebalance callback
    consumer.subscribe({topic}, rebalanceCb);

    TopicPartitions finishedPartitions;
    while (finishedPartitions.size() != assignedPartitions.load()) {
        // Poll messages from Kafka brokers
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            if (!record.error()) {
                std::cerr << record.toString() << std::endl;
            } else {
                if (record.error().value() == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
                    // Record the partition which has been reached the end
                    finishedPartitions.emplace(record.topic(), record.partition());
                } else {
                    std::cerr << record.toString() << std::endl;
                }
            }
        }
    }

    // No explicit close is needed, RAII will take care of it
    // consumer.close();
}

