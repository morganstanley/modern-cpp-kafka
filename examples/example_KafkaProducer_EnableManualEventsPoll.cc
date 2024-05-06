#include <kafka/KafkaProducer.h>

#include <iostream>
#include <list>
#include <memory>
#include <string>


int main()
{
    using namespace kafka;
    using namespace kafka::clients::producer;

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // Prepare the configuration (with "enable.manual.events.poll=true")
    const Properties props({{"bootstrap.servers",         {brokers}},
                            {"enable.manual.events.poll", {"true" }}});

    // Create a producer
    KafkaProducer producer(props);

    std::cout << "Type message value and hit enter to produce message... (empty line to finish)" << std::endl;

    // Get all input lines
    std::list<std::shared_ptr<std::string>> messages;
    for (auto line = std::make_shared<std::string>(); std::getline(std::cin, *line) && !line->empty();) {
        messages.emplace_back(line);
    }

    while (!messages.empty()) {
        // Pop out a message to be sent
        auto payload = messages.front();
        messages.pop_front();

        // Prepare the message
        const ProducerRecord record(topic, NullKey, Value(payload->c_str(), payload->size()));

        // Prepare the delivery callback
        // Note: if fails, the message will be pushed back to the sending queue, and then retries later
        auto deliveryCb = [payload, &messages](const RecordMetadata& metadata, const Error& error) {
            if (!error) {
                std::cout << "Message delivered: " << metadata.toString() << std::endl;
            } else {
                std::cerr << "Message failed to be delivered: " << error.message() << ", will be retried later" << std::endl;
                messages.emplace_back(payload);
            }
        };

        // Send the message
        producer.send(record, deliveryCb);

        // Poll events (e.g. message delivery callback)
        producer.pollEvents(std::chrono::milliseconds(0));
    }

    // Close the producer explicitly(or not, since RAII will take care of it)
    producer.close();
}

