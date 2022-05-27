#include "kafka/KafkaConsumer.h"
#include "kafka/Timestamp.h"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <atomic>
#include <iostream>
#include <signal.h>
#include <string>
#include <vector>


std::atomic_bool running = {true};

void stopRunning(int sig) {
    if (sig != SIGINT) return;

    if (running)
    {
        running = false;
    }
    else
    {
        // Restore the signal handler, -- to avoid stucking with this handler
        signal(SIGINT, SIG_IGN); // NOLINT
    }
}

struct Arguments
{
    std::vector<std::string>           brokerList;
    std::string                        topic;
    std::map<std::string, std::string> props;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;

    namespace po = boost::program_options;
    po::options_description desc("Options description");
    desc.add_options()
            ("help,h",
                "Print usage information.")
            ("broker-list",
                po::value<std::vector<std::string>>(&args->brokerList)->multitoken()->required(),
                "REQUIRED: The server(s) to connect to.")
            ("topic",
                po::value<std::string>(&args->topic)->required(),
                "REQUIRED: The topic to consume from.")
            ("props",
                po::value<std::vector<std::string>>(&propList)->multitoken(),
                "Kafka consumer properties in key=value format.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help") || argc == 1)
    {
        std::cout << "Read data from a given Kafka topic and write it to the standard output" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << desc << std::endl;
        return nullptr;
    }

    po::notify(vm);

    for (const auto& prop: propList)
    {
        std::vector<std::string> keyValue;
        boost::algorithm::split(keyValue, prop, boost::is_any_of("="));
        if (keyValue.size() != 2)
        {
            throw std::invalid_argument("Unexpected --props value! Expected key=value format");
        }
        args->props[keyValue[0]] = keyValue[1];
    }

    return args;
}

void RunConsumer(const std::string& topic, const kafka::clients::consumer::Config& props)
{
    using namespace kafka::clients;
    using namespace kafka::clients::consumer;
    // Create a manual-commit consumer
    KafkaClient::setGlobalLogger(kafka::Logger());
    KafkaConsumer consumer(props);

    // Subscribe to topic
    consumer.subscribe({topic});
    std::cout << "--------------------" << std::endl;

    // Poll & print messages
    while (running)
    {
        const auto POLL_TIMEOUT = std::chrono::milliseconds(100);
        auto records = consumer.poll(POLL_TIMEOUT);
        for (const auto& record: records)
        {
            if (!record.error())
            {
                std::cout << "Current Local Time [" << kafka::utility::getCurrentTime() << "]" << std::endl;
                std::cout << "  Topic    : " << record.topic() << std::endl;
                std::cout << "  Partition: " << record.partition() << std::endl;
                std::cout << "  Offset   : " << record.offset() << std::endl;
                std::cout << "  Timestamp: " << record.timestamp().toString() << std::endl;
                std::cout << "  Headers  : " << kafka::toString(record.headers()) << std::endl;
                std::cout << "  Key   [" << std::setw(4) << record.key().size()   << " B]: " << record.key().toString() << std::endl;
                std::cout << "  Value [" << std::setw(4) << record.value().size() << " B]: " << record.value().toString() << std::endl;
                std::cout << "--------------------" << std::endl;
            }
            else
            {
                std::cerr << record.toString() << std::endl;
            }
        }
    }
}

int main (int argc, char **argv)
{
    // Parse input arguments
    std::unique_ptr<Arguments> args;
    try
    {
        args = ParseArguments(argc, argv);
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    if (!args) // Only for "help"
    {
        return EXIT_SUCCESS;
    }

    // Use Ctrl-C to terminate the program
    signal(SIGINT, stopRunning); // NOLINT

    // Prepare consumer properties
    //
    using namespace kafka::clients::consumer;
    Config props;
    props.put(Config::BOOTSTRAP_SERVERS, boost::algorithm::join(args->brokerList, ","));
    // Get client id
    std::ostringstream oss;
    oss << "consumer-" << std::this_thread::get_id();
    props.put(Config::CLIENT_ID, oss.str());
    // For other properties user assigned
    for (const auto& prop: args->props)
    {
        props.put(prop.first, prop.second);
    }

    // Start consumer
    try
    {
        RunConsumer(args->topic, props);
    }
    catch (const kafka::KafkaException& e)
    {
        std::cerr << "Exception thrown by consumer: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

