project("kafka-integration-test")

# Directories
include_directories(SYSTEM ${GTEST_ROOT}/include)
link_directories(${GTEST_ROOT}/lib)

# Target
file(GLOB TEST_SRCS *.cc)

add_executable("${PROJECT_NAME}" ${TEST_SRCS})

target_link_libraries("${PROJECT_NAME}" modern-cpp-kafka-api gtest gtest_main pthread ${SASL_LIBRARY})

add_test(NAME ${PROJECT_NAME} COMMAND ./${PROJECT_NAME})
set_tests_properties(${PROJECT_NAME} PROPERTIES LABELS "IT")

if (BUILD_OPTION_USE_ASAN OR BUILD_OPTION_USE_TSAN)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    target_link_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    if (BUILD_OPTION_USE_ASAN)
        set_property(TEST ${PROJECT_NAME} PROPERTY ENVIRONMENT ASAN_OPTIONS=detect_leaks=1)
    endif ()
endif ()

