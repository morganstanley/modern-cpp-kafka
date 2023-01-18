#include <kafka/Properties.h>

#include <iostream>
#include <string>

std::string brokers = "192.168.0.1:9092,192.168.0.2:9092,192.168.0.3:9092";

int main()
{
    {
        const kafka::Properties props ({
            {"bootstrap.servers",  {brokers}},
            {"enable.idempotence", {"true" }},
        });

        std::cout << "Properties: " << props.toString() << std::endl;
    }

    {
        kafka::Properties props;
        props.put("bootstrap.servers", brokers);
        props.put("enable.idempotence", "true");

        std::cout << "Properties: " << props.toString() << std::endl;
    }
}

