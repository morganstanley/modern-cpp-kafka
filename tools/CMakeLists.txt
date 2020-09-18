project("kafka-tools")

#---------------------------
# Check boost directory
#---------------------------
if(DEFINED ENV{BOOST_ROOT})
    set(BOOST_ROOT $ENV{BOOST_ROOT})
    message(STATUS "boost root directory: ${BOOST_ROOT}")
else()
    message(FATAL_ERROR "No BOOST_ROOT defined!")
endif()

#---------------------------
# Check pthread lib directory
#---------------------------
find_library(PTHREAD_LIB pthread)
if (PTHREAD_LIB)
    message(STATUS "pthread library: ${PTHREAD_LIB}")
else()
    message(FATAL_ERROR "Could not find library: pthread!")
endif()

#---------------------------
# Check sasl2 lib directory
#---------------------------
if(DEFINED ENV{SASL2_LIBRARYDIR})
    set(SASL2_LIBRARYDIR $ENV{SASL2_LIBRARYDIR})
    message(STATUS "sasl2 library directory: ${SASL2_LIBRARYDIR}")
else()
    message(FATAL_ERROR "No SASL2_LIBRARYDIR defined!")
endif()


# Directories
include_directories(SYSTEM ${BOOST_ROOT}/include)
link_directories(${BOOST_ROOT}/lib ${SASL2_LIBRARYDIR})


# Target: kafka-topics
set(KafkaTopics kafka-topics)

add_executable("${KafkaTopics}" "KafkaTopics.cc")

target_link_libraries("${KafkaTopics}" modern-cpp-kafka-api)
target_link_libraries("${KafkaTopics}" "pthread;boost_program_options;gssapiv2;")

install(TARGETS "${KafkaTopics}" DESTINATION tools)


# Target: kafka-console-consumer
set(KafkaConsoleConsumer kafka-console-consumer)

add_executable("${KafkaConsoleConsumer}" "KafkaConsoleConsumer.cc")

target_link_libraries("${KafkaConsoleConsumer}" modern-cpp-kafka-api)
target_link_libraries("${KafkaConsoleConsumer}" "pthread;boost_program_options;gssapiv2;")

install(TARGETS "${KafkaConsoleConsumer}" DESTINATION tools)


# Target: kafka-console-producer
set(KafkaConsoleProducer kafka-console-producer)

add_executable("${KafkaConsoleProducer}" "KafkaConsoleProducer.cc")

target_link_libraries("${KafkaConsoleProducer}" modern-cpp-kafka-api)
target_link_libraries("${KafkaConsoleProducer}" "pthread;boost_program_options;gssapiv2;")

install(TARGETS "${KafkaConsoleProducer}" DESTINATION tools)

