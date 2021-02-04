#include "kafka/KafkaProducer.h"

#include <string>
#include <csignal>
#include <iostream>

int main(int argc, char **argv)
{
  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <brokers> <topic>\n";
    exit(1);
  }

  std::string brokers = argv[1];
  kafka::Topic topic  = argv[2];

  // Create configuration object
  kafka::Properties props ({
    {"bootstrap.servers",  brokers},
    {"enable.idempotence", "true"},
  });

  // Create a producer instance.
  kafka::KafkaAsyncProducer producer(props);

  // Read messages from stdin and produce to the broker.
  std::cout << "% Type message value and hit enter to produce message. (empty line to quit)" << std::endl;

  for (auto line = std::make_shared<std::string>();
       std::getline(std::cin, *line);
       line = std::make_shared<std::string>()) {
    // The ProducerRecord doesn't own `line`, it is just a thin wrapper
    auto record = kafka::ProducerRecord(topic,
                                        kafka::Key(),
                                        kafka::Value(line->c_str(), line->size()));

    // Send the message.
    producer.send(record,
                  // Note: Here we capture the shared_pointer of `line`,
                  //       which holds the content for `record.value()`.
                  //       It makes sure the memory block is valid until the lambda finishes.
                  [line](const kafka::Producer::RecordMetadata& metadata, std::error_code ec) {
                    if (!ec)
                      std::cout << "% Message delivered: " << metadata.toString() << std::endl;
                    else
                      std::cerr << "% Message delivery failed: " << ec.message() << std::endl;
                  });

    if (line->empty()) break;
  };

  // producer.close(); // No explicit close is needed, RAII will take care of it
}
