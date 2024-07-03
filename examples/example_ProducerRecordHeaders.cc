#include <kafka/ProducerRecord.h>
#include <kafka/Types.h>

#include <cstddef>
#include <iostream>
#include <string>


int main()
{
    const kafka::Topic     topic = "someTopic";
    const kafka::Partition partition = 0;

    const std::string category  = "categoryA";
    const std::size_t sessionId = 1;
    const std::string key       = "some key";
    const std::string value     = "some payload";


    {
        kafka::clients::producer::ProducerRecord record(topic,
                                                        partition,
                                                        kafka::Key{key.c_str(), key.size()},
                                                        kafka::Value{value.c_str(), value.size()});
        record.headers() = {{
            kafka::Header{kafka::Header::Key{"Category"},  kafka::Header::Value{category.c_str(), category.size()}},
            kafka::Header{kafka::Header::Key{"SessionId"}, kafka::Header::Value{&sessionId, sizeof(sessionId)}}
        }};

        std::cout << "ProducerRecord: " << record.toString() << std::endl;
    }
}

