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

    // Prepare a message
    std::cout << "Type message value and hit enter to produce message..." << std::endl;
    std::string line;
    std::getline(std::cin, line);

    const ProducerRecord record(topic, NullKey, Value(line.c_str(), line.size()));

    // Prepare delivery callback
    auto deliveryCb = [](const RecordMetadata& metadata, const Error& error) {
        if (!error) {
            std::cout << "Message delivered: " << metadata.toString() << std::endl;
        } else {
            std::cerr << "Message failed to be delivered: " << error.message() << std::endl;
        }
    };

    // Send a message
    producer.send(record, deliveryCb);

    // Close the producer explicitly(or not, since RAII will take care of it)
    producer.close();
}

