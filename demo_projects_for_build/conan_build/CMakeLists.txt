cmake_minimum_required(VERSION "3.20")
project("kafka-examples")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

# Target: kafka_producer
add_executable("kafka_producer" "../../examples/kafka_async_producer_not_copy_payload.cc")
target_link_libraries("kafka_producer" ${CONAN_LIBS})

# Target: kafka_consumer
add_executable("kafka_consumer" "../../examples/kafka_auto_commit_consumer.cc")
target_link_libraries("kafka_consumer" ${CONAN_LIBS})

