#include "kafka/KafkaProducer.h"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <iostream>
#include <string>
#include <vector>


struct Arguments
{
    std::vector<std::string>   brokerList;
    std::string                topic;
    Optional<kafka::Partition> partition;
    std::map<std::string, std::string> props;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> propList;
    int partition = -1;

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
                "REQUIRED: The topic to publish to.")
            ("partition",
                po::value<int>(&partition),
                "The partition to publish to.")
            ("props",
                po::value<std::vector<std::string>>(&propList)->multitoken(),
                "Kafka producer properties in key=value format.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help") || argc == 1)
    {
        std::cout << "Read data from the standard input and send it to the given Kafka topic" << std::endl;
        std::cout << "    (with librdkafka v" << kafka::utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << desc << std::endl;
        return nullptr;
    }

    po::notify(vm);

    if (partition >= 0)
    {
        args->partition = partition;
    }

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


int main (int argc, char **argv)
{
    using namespace kafka::clients;
    using namespace kafka::clients::producer;

    try
    {
        // Parse input arguments
        std::unique_ptr<Arguments> args;
        args = ParseArguments(argc, argv);
        if (!args) return EXIT_SUCCESS;  // Only for "help"

        // Prepare consumer properties
        ProducerConfig props;
        props.put(Config::BOOTSTRAP_SERVERS, boost::algorithm::join(args->brokerList, ","));
        // Get client id
        std::ostringstream oss;
        oss << "producer-" << std::this_thread::get_id();
        props.put(Config::CLIENT_ID, oss.str());
        // For other properties user assigned
        for (const auto& prop: args->props)
        {
            props.put(prop.first, prop.second);
        }

        // Create a sync-send producer
        KafkaClient::setGlobalLogger(kafka::Logger());
        KafkaProducer producer(props);

        auto startPromptLine = []() { std::cout << "> "; };

        // Keep reading lines and send it towards kafka cluster
        startPromptLine();

        std::string line;
        while (std::getline(std::cin, line))
        {
            const kafka::Key   key;
            const kafka::Value value(line.c_str(), line.size());
            const auto         topic           = args->topic;
            const auto         partitionOption = args->partition;

            const producer::ProducerRecord record =
                (partitionOption ? producer::ProducerRecord(topic, *partitionOption, key, value) : producer::ProducerRecord(topic, key, value));

            std::cout << "Current Local Time [" << kafka::utility::getCurrentTime() << "]" << std::endl;

            // Note: might throw exceptions if with unknown topic, unknown partition, invalid message length, etc.
            const auto metadata = producer.syncSend(record);
            const auto offsetOption = metadata.offset();
            std::cout << "Just Sent Key[" << metadata.keySize()   << " B]/Value["  << metadata.valueSize() << " B]"
                << " ==> " << metadata.topic() << "-" << std::to_string(metadata.partition()) << "@" <<  (offsetOption ? std::to_string(*offsetOption) : "NA")
                << ", " << metadata.timestamp().toString() << ", " << metadata.persistedStatusString() << std::endl;

            std::cout << "--------------------" << std::endl;
            startPromptLine();
        }
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

