#include <kafka/KafkaConsumer.h>

#include <cstdlib>
#include <iostream>
#include <signal.h>
#include <string>

std::atomic_bool running = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running) {
        running = false;
    } else {
        // Restore the signal handler, -- to avoid stuck with this handler
        signal(SIGINT, SIG_IGN); // NOLINT
    }
}

int main()
{
    using namespace kafka;
    using namespace kafka::clients::consumer;

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning);    // NOLINT

    // E.g. KAFKA_BROKER_LIST: "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092"
    const std::string brokers = getenv("KAFKA_BROKER_LIST"); // NOLINT
    const Topic topic = getenv("TOPIC_FOR_TEST");            // NOLINT

    // Prepare the configuration
    Properties props({{"bootstrap.servers", {brokers}}});
    props.put("enable.auto.commit", "false");

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Subscribe to topics
    consumer.subscribe({topic});

    while (running) {
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            std::cout << record.toString() << std::endl;
        }

        if (!records.empty()) {
            consumer.commitAsync();
        }
    }

    consumer.commitSync();

    // No explicit close is needed, RAII will take care of it
    // consumer.close();
}

