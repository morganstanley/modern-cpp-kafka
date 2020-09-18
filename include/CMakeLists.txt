project(modern-cpp-kafka-api)

add_library(${PROJECT_NAME} INTERFACE)

target_include_directories(${PROJECT_NAME} INTERFACE "./")
target_link_libraries(${PROJECT_NAME} INTERFACE "rdkafka")

install(DIRECTORY kafka DESTINATION include)
