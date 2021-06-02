project("kafka-tools")

#---------------------------
# Check boost directory
#---------------------------
if(DEFINED ENV{BOOST_ROOT})
    set(Boost_INCLUDE_DIRS $ENV{BOOST_ROOT}/include)
    set(Boost_LIBRARY_DIRS $ENV{BOOST_ROOT}/lib)
else()
    find_package(Boost)
    if(NOT Boost_FOUND)
        message(FATAL_ERROR "Boost not found!")
    endif()
endif()

message(STATUS "boost include directory: ${Boost_INCLUDE_DIRS}")
message(STATUS "boost library directory: ${Boost_LIBRARY_DIRS}")

#---------------------------
# Check pthread lib directory
#---------------------------
find_library(PTHREAD_LIB pthread)
if(PTHREAD_LIB)
    message(STATUS "pthread library: ${PTHREAD_LIB}")
else()
    # todo: kenneth
    # message(FATAL_ERROR "Could not find library: pthread!")
    message(STATUS "Could not find library: pthread!")
endif()

#---------------------------
# Check sasl lib directory
#---------------------------
if(DEFINED ENV{SASL_LIBRARYDIR})
    set(SASL_LIBRARYDIR $ENV{SASL_LIBRARYDIR})
    link_directories(${SASL_LIBRARYDIR})
    message(STATUS "sasl2 library directory: ${SASL_LIBRARYDIR}")
endif()
if(DEFINED ENV{SASL_LIBRARY})
    set(SASL_LIBRARY $ENV{SASL_LIBRARY})
    message(STATUS "sasl2 library: ${SASL_LIBRARY}")
endif()


# Directories
include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


# Target: kafka-topics
set(KafkaTopics kafka-topics)

add_executable("${KafkaTopics}" "KafkaTopics.cc")

target_link_libraries("${KafkaTopics}" modern-cpp-kafka-api)
target_link_libraries("${KafkaTopics}" "pthread;boost_program_options;${SASL_LIBRARY}")

install(TARGETS "${KafkaTopics}" DESTINATION tools)


# Target: kafka-console-consumer
set(KafkaConsoleConsumer kafka-console-consumer)

add_executable("${KafkaConsoleConsumer}" "KafkaConsoleConsumer.cc")

target_link_libraries("${KafkaConsoleConsumer}" modern-cpp-kafka-api)
target_link_libraries("${KafkaConsoleConsumer}" "pthread;boost_program_options;${SASL_LIBRARY}")

install(TARGETS "${KafkaConsoleConsumer}" DESTINATION tools)


# Target: kafka-console-producer
set(KafkaConsoleProducer kafka-console-producer)

add_executable("${KafkaConsoleProducer}" "KafkaConsoleProducer.cc")

target_link_libraries("${KafkaConsoleProducer}" modern-cpp-kafka-api)
target_link_libraries("${KafkaConsoleProducer}" "pthread;boost_program_options;${SASL_LIBRARY}")

install(TARGETS "${KafkaConsoleProducer}" DESTINATION tools)

