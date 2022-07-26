cmake_minimum_required(VERSION "3.8")
project("kafka-examples")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

# Target: kafka_sync_producer
add_executable("kafka_sync_producer" "../../examples/kafka_sync_producer.cc")
target_link_libraries("kafka_sync_producer" ${CONAN_LIBS})

# Target: kafka_async_producer_copy_payload
add_executable("kafka_async_producer_copy_payload" "../../examples/kafka_async_producer_copy_payload.cc")
target_link_libraries("kafka_async_producer_copy_payload" ${CONAN_LIBS})

# Target: kafka_async_producer_not_copy_payload
add_executable("kafka_async_producer_not_copy_payload" "../../examples/kafka_async_producer_not_copy_payload.cc")
target_link_libraries("kafka_async_producer_not_copy_payload" ${CONAN_LIBS})

# Target: kafka_auto_commit_consumer
add_executable("kafka_auto_commit_consumer" "../../examples/kafka_auto_commit_consumer.cc")
target_link_libraries("kafka_auto_commit_consumer" ${CONAN_LIBS})

# Target: kafka_manual_commit_consumer
add_executable("kafka_manual_commit_consumer" "../../examples/kafka_manual_commit_consumer.cc")
target_link_libraries("kafka_manual_commit_consumer" ${CONAN_LIBS})
