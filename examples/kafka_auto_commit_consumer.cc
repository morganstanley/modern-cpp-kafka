#include "kafka/KafkaConsumer.h"

#include <iostream>
#include <string>

int main(int argc, char **argv)
{
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
            {"enable.auto.commit", "true"}
        });

        // Create a consumer instance
        kafka::clients::KafkaConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        // Read messages from the topic
        std::cout << "% Reading messages from topic: " << topic << std::endl;
        while (true) {
            auto records = consumer.poll(std::chrono::milliseconds(100));
            for (const auto& record: records) {
                // In this example, quit on empty message
                if (record.value().size() == 0) return 0;

                if (!record.error()) {
                    std::cout << "% Got a new message..." << std::endl;
                    std::cout << "    Topic    : " << record.topic() << std::endl;
                    std::cout << "    Partition: " << record.partition() << std::endl;
                    std::cout << "    Offset   : " << record.offset() << std::endl;
                    std::cout << "    Timestamp: " << record.timestamp().toString() << std::endl;
                    std::cout << "    Headers  : " << kafka::toString(record.headers()) << std::endl;
                    std::cout << "    Key   [" << record.key().toString() << "]" << std::endl;
                    std::cout << "    Value [" << record.value().toString() << "]" << std::endl;
                } else {
                    std::cerr << record.toString() << std::endl;
                }
            }
        }

        // consumer.close(); // No explicit close is needed, RAII will take care of it

    } catch (const kafka::KafkaException& e) {
        std::cerr << "% Unexpected exception caught: " << e.what() << std::endl;
    }
}

