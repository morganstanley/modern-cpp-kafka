project("kafka-examples")

# Target
file(GLOB EXAMPLE_SRCS *.cc)

add_executable("${PROJECT_NAME}" ${EXAMPLE_SRCS})

target_link_libraries("${PROJECT_NAME}" modern-cpp-kafka-api)

if (BUILD_OPTION_USE_ASAN OR BUILD_OPTION_USE_ASAN)
    target_compile_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    target_link_options(${PROJECT_NAME} PRIVATE "-fno-sanitize=all")
    if (UILD_OPTION_USE_ASAN)
        set_property(TEST ${PROJECT_NAME} PROPERTY ENVIRONMENT ASAN_OPTIONS=detect_leaks=1)
    endif ()
endif ()

