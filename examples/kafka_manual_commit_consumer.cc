#include "kafka/KafkaConsumer.h"

#include <iostream>
#include <string>

int main(int argc, char **argv)
{
    using namespace kafka;
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;

    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <brokers> <topic>\n";
        exit(argc == 1 ? 0 : 1); // NOLINT
    }

    const std::string brokers = argv[1];
    const Topic       topic   = argv[2];

    try {

        // Create configuration object
        const Properties props ({
            {"bootstrap.servers",  {brokers}},
            {"enable.auto.commit", {"false"}}
        });

        // Create a consumer instance
        KafkaConsumer consumer(props);

        // Subscribe to topics
        consumer.subscribe({topic});

        auto lastTimeCommitted = std::chrono::steady_clock::now();

        // Read messages from the topic
        std::cout << "% Reading messages from topic: " << topic << std::endl;
        bool allCommitted = true;
        bool running      = true;
        while (running) {
            auto records = consumer.poll(std::chrono::milliseconds(100));
            for (const auto& record: records) {
                // In this example, quit on empty message
                if (record.value().size() == 0) {
                    running = false;
                    break;
                }

                if (!record.error()) {
                    std::cout << "% Got a new message..." << std::endl;
                    std::cout << "    Topic    : " << record.topic() << std::endl;
                    std::cout << "    Partition: " << record.partition() << std::endl;
                    std::cout << "    Offset   : " << record.offset() << std::endl;
                    std::cout << "    Timestamp: " << record.timestamp().toString() << std::endl;
                    std::cout << "    Headers  : " << toString(record.headers()) << std::endl;
                    std::cout << "    Key   [" << record.key().toString() << "]" << std::endl;
                    std::cout << "    Value [" << record.value().toString() << "]" << std::endl;

                    allCommitted = false;
                } else {
                    std::cerr << record.toString() << std::endl;
                }
            }

            if (!allCommitted) {
                auto now = std::chrono::steady_clock::now();
                if (now - lastTimeCommitted > std::chrono::seconds(1)) {
                    // Commit offsets for messages polled
                    std::cout << "% syncCommit offsets: " << utility::getCurrentTime() << std::endl;
                    consumer.commitSync(); // or commitAsync()

                    lastTimeCommitted = now;
                    allCommitted      = true;
                }
            }
        }

        // consumer.close(); // No explicit close is needed, RAII will take care of it

    } catch (const KafkaException& e) {
        std::cerr << "% Unexpected exception caught: " << e.what() << std::endl;
    }
}

