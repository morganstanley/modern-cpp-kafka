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

    // To print out the error
    props.put("error_cb", [](const kafka::Error& error) {
                              // https://en.wikipedia.org/wiki/ANSI_escape_code
                              std::cerr << "\033[1;31m" << "[" << kafka::utility::getCurrentTime() << "] ==> Met Error: " << "\033[0m";
                              std::cerr << "\033[4;35m" << error.toString() << "\033[0m" << std::endl;
                          });

    // To enable the debug-level log
    props.put("log_level", "7");
    props.put("debug", "all");
    props.put("log_cb", [](int /*level*/, const char* /*filename*/, int /*lineno*/, const char* msg) {
                            std::cout << "[" << kafka::utility::getCurrentTime() << "]" << msg << std::endl;
                        });

    // To enable the statistics dumping
    props.put("statistics.interval.ms", "1000");
    props.put("stats_cb", [](const std::string& jsonString) {
                              std::cout << "Statistics: " << jsonString << std::endl;
                          });

    // Create a consumer instance
    KafkaConsumer consumer(props);

    // Subscribe to topics
    consumer.subscribe({topic});

    while (running) {
        auto records = consumer.poll(std::chrono::milliseconds(100));

        for (const auto& record: records) {
            std::cerr << record.toString() << std::endl;
        }
    }
}

