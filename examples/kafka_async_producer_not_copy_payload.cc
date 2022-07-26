#include "kafka/KafkaProducer.h"

#include <csignal>
#include <iostream>
#include <string>

int main(int argc, char **argv)
{
    using namespace kafka::clients;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <brokers> <topic>\n";
        exit(argc == 1 ? 0 : 1); // NOLINT
    }

    std::string brokers = argv[1];
    kafka::Topic topic  = argv[2];

    try {

        // Create configuration object
        kafka::Properties props ({
            {"bootstrap.servers",  brokers},
            {"enable.idempotence", "true"},
        });

        // Create a producer instance
        KafkaProducer producer(props);

        // Read messages from stdin and produce to the broker
        std::cout << "% Type message value and hit enter to produce message. (empty line to quit)" << std::endl;

        for (auto line = std::make_shared<std::string>();
             std::getline(std::cin, *line);
             line = std::make_shared<std::string>()) {
            // The ProducerRecord doesn't own `line`, it is just a thin wrapper
            auto record = producer::ProducerRecord(topic,
                                                   kafka::NullKey,
                                                   kafka::Value(line->c_str(), line->size()));

            // Send the message
            producer.send(record,
                          // The delivery report handler
                          // Note: Here we capture the shared_pointer of `line`,
                          //       which holds the content for `record.value()`.
                          //       It makes sure the memory block is valid until the lambda finishes.
                          [line](const producer::RecordMetadata& metadata, const kafka::Error& error) {
                              if (!error) {
                                  std::cout << "% Message delivered: " << metadata.toString() << std::endl;
                              } else {
                                  std::cerr << "% Message delivery failed: " << error.message() << std::endl;
                              }
                          });

            if (line->empty()) break;
        }

        // producer.close(); // No explicit close is needed, RAII will take care of it

    } catch (const kafka::KafkaException& e) {
        std::cerr << "% Unexpected exception caught: " << e.what() << std::endl;
    }
}

