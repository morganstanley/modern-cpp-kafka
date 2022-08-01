project("kafka-unit-test")

#---------------------------
# rapidjson
#---------------------------
if (DEFINED ENV{RAPIDJSON_INCLUDE_DIRS})
    set(RAPIDJSON_INCLUDE_DIRS $ENV{RAPIDJSON_INCLUDE_DIRS})
else ()
    set(RAPIDJSON_INCLUDE_DIRS /usr/include)
endif ()

if (EXISTS "${RAPIDJSON_INCLUDE_DIRS}/rapidjson/rapidjson.h")
    message(STATUS "rapidjson include directory: ${RAPIDJSON_INCLUDE_DIRS}")
else ()
    message(FATAL_ERROR "rapidjson not found!")
endif ()


# Target
file(GLOB TEST_SRCS *.cc)

include_directories(${PROJECT_NAME} SYSTEM INTERFACE ${RAPIDJSON_INCLUDE_DIRS})

add_executable("${PROJECT_NAME}" ${TEST_SRCS})
target_link_libraries("${PROJECT_NAME}" modern-cpp-kafka-api gtest gtest_main)

add_test(NAME ${PROJECT_NAME} COMMAND ./${PROJECT_NAME})
set_tests_properties(${PROJECT_NAME} PROPERTIES LABELS "unit")

if (BUILD_OPTION_USE_ASAN OR BUILD_OPTION_USE_TSAN)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    target_link_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    if (BUILD_OPTION_USE_ASAN)
        set_property(TEST ${PROJECT_NAME} PROPERTY ENVIRONMENT ASAN_OPTIONS=detect_leaks=1)
    endif ()
endif ()
