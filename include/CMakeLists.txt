project(modern-cpp-kafka-api)

add_library(${PROJECT_NAME} INTERFACE)

target_include_directories(${PROJECT_NAME} INTERFACE ${CMAKE_CURRENT_LIST_DIR})

#---------------------------
# librdkafka
#---------------------------
target_include_directories(${PROJECT_NAME} SYSTEM INTERFACE ${LIBRDKAFKA_INCLUDE_DIR})
target_link_directories(${PROJECT_NAME} INTERFACE ${LIBRDKAFKA_LIBRARY_DIR})
target_link_libraries(${PROJECT_NAME} INTERFACE rdkafka)

#---------------------------
# pthread
#---------------------------
if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    target_link_libraries(${PROJECT_NAME} INTERFACE pthread)
endif ()

#---------------------------
# sasl (if required)
#---------------------------
if (SASL_LIBRARY)
    target_link_directories(${PROJECT_NAME} INTERFACE ${SASL_LIBRARYDIR})
    target_link_libraries(${PROJECT_NAME} INTERFACE ${SASL_LIBRARY})
endif ()

# Header-only
install(DIRECTORY kafka DESTINATION include)
