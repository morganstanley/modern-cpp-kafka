#include <kafka/KafkaProducer.h>

#include <cstdlib>
#include <iostream>
#include <string>


int main()
{
    using namespace kafka;
    using namespace kafka::clients::producer;

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // Prepare the configuration
    const Properties props({{"bootstrap.servers", brokers}});

    // Create a producer
    KafkaProducer producer(props);

    std::cout << "Type message value and hit enter to produce message... (empty line to quit)" << std::endl;

    // Get input lines and forward them to Kafka
    for (std::string line; std::getline(std::cin, line); ) {

        // Empty line to quit
        if (line.empty()) break;

        // Prepare a message
        const ProducerRecord record(topic, NullKey, Value(line.c_str(), line.size()));

        // Prepare delivery callback
        auto deliveryCb = [](const RecordMetadata& metadata, const Error& error) {
            if (!error) {
                std::cout << "Message delivered: " << metadata.toString() << std::endl;
            } else {
                std::cerr << "Message failed to be delivered: " << error.message() << std::endl;
            }
        };

        // Send the message (deep-copy the payload)
        producer.send(record, deliveryCb, KafkaProducer::SendOption::ToCopyRecordValue);
    }

    // Close the producer explicitly(or not, since RAII will take care of it)
    producer.close();
}

