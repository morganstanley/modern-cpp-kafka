#include "kafka/AdminClient.h"

#include <boost/algorithm/string.hpp>
#include <boost/program_options.hpp>

#include <iostream>
#include <string>
#include <vector>


namespace Kafka = KAFKA_API;


struct Arguments
{
    enum OpType { Create, Delete, List };

    std::string broker;
    std::string topic;
    OpType      opType{};
    int         partitions{};
    int         replicationFactor{};

    Kafka::Properties adminConfig;
    Kafka::Properties topicProps;
};

std::unique_ptr<Arguments> ParseArguments(int argc, char **argv)
{
    auto args = std::make_unique<Arguments>();
    std::vector<std::string> adminConfigList;
    std::vector<std::string> topicPropList;

    namespace po = boost::program_options;
    po::options_description desc("Options description");
    desc.add_options()
            ("help,h",
                "Print usage information.")

            ("bootstrap-server",
                po::value<std::string>(&args->broker)->required(),
                "REQUIRED: One broker from the Kafka cluster.")

            ("admin-config",
                po::value<std::vector<std::string>>(&adminConfigList)->multitoken(),
                "Properties for the Admin Client (E.g, would be useful for kerberos connection)")

            ("list",
                "List topics.")
            ("create",
                "Create a topic.")
            ("delete",
                "Delete a topic.")

            ("topic",
                po::value<std::string>(&args->topic),
                "Only used (and REQUIRED) for topic creation: the topic name.")
            ("partitions",
                po::value<int>(&args->partitions),
                "Only used (and REQUIRED) for topic creation: partitions number of the topic.")
            ("replication-factor",
                po::value<int>(&args->replicationFactor),
                "Only used (and REQUIRED) for topic creation: replication factor of the topic.")
            ("topic-props",
                po::value<std::vector<std::string>>(&topicPropList)->multitoken(),
                "Only used (and REQUIRED) for topic creation: properties for the topic.");

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, desc), vm);

    if (vm.count("help") || argc == 1)
    {

        std::cout << "This tool helps in Kafka topic operations" << std::endl;
        std::cout << "    (with librdkafka v" << Kafka::Utility::getLibRdKafkaVersion() << ")" << std::endl;
        std::cout << desc << std::endl;
        return nullptr;
    }

    po::notify(vm);

    for (const auto& prop: adminConfigList)
    {
        std::vector<std::string> keyValue;
        boost::algorithm::split(keyValue, prop, boost::is_any_of("="));
        if (keyValue.size() != 2)
        {
            throw std::invalid_argument("Wrong option for --admin-config! MUST follow with key=value format!");
        }
        args->adminConfig.put(keyValue[0], keyValue[1]);
    }

    if (vm.count("list") + vm.count("create") + vm.count("delete") != 1)
    {
        throw std::invalid_argument("MUST choose exactly one operation from '--list/--create/--delete'");
    }
    args->opType = vm.count("list") ? Arguments::OpType::List :
                    (vm.count("create") ? Arguments::OpType::Create : Arguments::OpType::Delete);

    switch (args->opType)
    {
        case Arguments::OpType::List:
            if (vm.count("topic") || vm.count("partitions") || vm.count("replication-factor") || vm.count("topic-props"))
            {
                throw std::invalid_argument("The --list operation CANNOT take any '--topic/--partitions/--replication-factor/--topic-props' option!");
            }
            break;
        case Arguments::OpType::Create:
            if (!vm.count("topic") || !vm.count("partitions") || !vm.count("replication-factor"))
            {
                throw std::invalid_argument("The --create operation MUST be with '--topic/--partitions/--replication-factor' options!");
            }

            for (const auto& prop: topicPropList)
            {
                std::vector<std::string> keyValue;
                boost::algorithm::split(keyValue, prop, boost::is_any_of("="));
                if (keyValue.size() != 2)
                {
                    throw std::invalid_argument("Wrong option for --topic-props! MUST follow with key=value format!");
                }
                args->topicProps.put(keyValue[0], keyValue[1]);
            }

            break;
        case Arguments::OpType::Delete:
            if (!vm.count("topic"))
            {
                throw std::invalid_argument("The --delete operation MUST be with '--topic' option!");
            }
            if (vm.count("partitions") || vm.count("replication-factor") || vm.count("topic-props"))
            {
                throw std::invalid_argument("The --delete operation CANNOT take any of '--partitions/--replication-factor/--topic-props' options!");
            }
            break;
    }
    return args;
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

    Kafka::Properties adminConf = args->adminConfig;
    adminConf.put(Kafka::AdminClientConfig::BOOTSTRAP_SERVERS, args->broker);

    Kafka::AdminClient adminClient(adminConf);

    if (args->opType == Arguments::OpType::List)
    {
        auto listResult = adminClient.listTopics();
        if (listResult.errorCode())
        {
            std::cerr << "Error: " << listResult.message() << std::endl;
            return EXIT_FAILURE;
        }

        for (const auto& topic: listResult.topics)
        {
            std::cout << topic << std::endl;
        }
    }
    else if (args->opType == Arguments::OpType::Create)
    {
        auto createResult = adminClient.createTopics({args->topic}, args->partitions, args->replicationFactor, args->topicProps);
        if (createResult.errorCode())
        {
            std::cerr << "Error: " << createResult.message() << std::endl;
            return EXIT_FAILURE;
        }
    }
    else
    {
        auto deleteResult = adminClient.deleteTopics({args->topic});
        if (deleteResult.errorCode())
        {
            std::cerr << "Error: " << deleteResult.message() << std::endl;
            return EXIT_FAILURE;
        }
    }

    return EXIT_SUCCESS;
}

