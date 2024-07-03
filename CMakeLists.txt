cmake_minimum_required(VERSION "3.20")

project("Modern C++ Kafka API" VERSION 1.0.0)

get_property(parent_directory DIRECTORY PROPERTY PARENT_DIRECTORY)
if (NOT parent_directory)
    set(cppkafka_master_project ON)
    # Use Strict Options
    if ((CMAKE_CXX_COMPILER_ID STREQUAL "Clang") OR (CMAKE_CXX_COMPILER_ID STREQUAL "GNU"))
        add_compile_options("-Wall" "-Werror" "-Wextra" "-Wshadow" "-Wno-unused-result" "-Wno-array-bounds")
    elseif (CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
        add_definitions(-D_CRT_SECURE_NO_WARNINGS)
    endif ()
    if (CMAKE_CXX_STANDARD EQUAL 14)
        add_compile_options("-Wno-maybe-uninitialized")
    endif ()
endif ()

option(CPPKAFKA_ENABLE_TESTS "Generate the test targets" ${cppkafka_master_project})

include(CheckCXXCompilerFlag)
include(CMakePushCheckState)


#---------------------------
# C++17 (by default)
#---------------------------
if (NOT CMAKE_CXX_STANDARD)
    set(CMAKE_CXX_STANDARD 17)
endif ()
set(CMAKE_CXX_STANDARD_REQUIRED False)


#---------------------------
# Check Dependencies
#---------------------------
if (NOT BUILD_OPTION_DOC_ONLY)

    #---------------------------
    # librdkafka library
    #---------------------------
    if (DEFINED ENV{LIBRDKAFKA_INCLUDE_DIR})
        set(LIBRDKAFKA_INCLUDE_DIR $ENV{LIBRDKAFKA_INCLUDE_DIR})
    else ()
        find_file(LIBRDKAFKA_HEADER
             NAMES rdkafka.h
             HINTS /usr/include/librdkafka /usr/local/include/librdkafka /opt/homebrew/include/librdkafka)

        cmake_path(GET LIBRDKAFKA_HEADER PARENT_PATH LIBRDKAFKA_INCLUDE_DIR)
        cmake_path(GET LIBRDKAFKA_INCLUDE_DIR PARENT_PATH LIBRDKAFKA_INCLUDE_DIR)
    endif ()

    if (DEFINED ENV{LIBRDKAFKA_LIBRARY_DIR})
        set(LIBRDKAFKA_LIBRARY_DIR $ENV{LIBRDKAFKA_LIBRARY_DIR})
    else ()
        find_library(LIBRDKAFKA_LIB
             NAMES rdkafka
             HINTS /usr/lib /usr/local/lib /opt/homebrew/lib)

        cmake_path(GET LIBRDKAFKA_LIB PARENT_PATH LIBRDKAFKA_LIBRARY_DIR)
    endif ()

    if (EXISTS "${LIBRDKAFKA_INCLUDE_DIR}/librdkafka/rdkafka.h")
        message(STATUS "librdkafka include directory: ${LIBRDKAFKA_INCLUDE_DIR}")
    else ()
        message(FATAL_ERROR "Could not find headers for librdkafka!")
    endif ()

    if (EXISTS "${LIBRDKAFKA_LIBRARY_DIR}/librdkafka.a" OR EXISTS "${LIBRDKAFKA_LIBRARY_DIR}/librdkafka.so" OR EXISTS "${LIBRDKAFKA_LIBRARY_DIR}/rdkafka.lib" )
        message(STATUS "librdkafka library directory: ${LIBRDKAFKA_LIBRARY_DIR}")
    else ()
        message(FATAL_ERROR "Could not find library for librdkafka!")
    endif ()


    #---------------------------
    # pthread library (for linux only)
    #---------------------------
    if (${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
        find_library(PTHREAD_LIB pthread)
        if (PTHREAD_LIB)
            message(STATUS "pthread library: ${PTHREAD_LIB}")
        else ()
            message(FATAL_ERROR "Could not find library for pthread!")
        endif ()
    endif ()


    #---------------------------
    # sasl library
    #---------------------------
    if (DEFINED ENV{SASL_LIBRARYDIR})
        set(SASL_LIBRARYDIR $ENV{SASL_LIBRARYDIR})
        link_directories(${SASL_LIBRARYDIR})
        message(STATUS "sasl2 library directory: ${SASL_LIBRARYDIR}")
    endif ()
    if (DEFINED ENV{SASL_LIBRARY})
        set(SASL_LIBRARY $ENV{SASL_LIBRARY})
        message(STATUS "sasl2 library: ${SASL_LIBRARY}")
    endif ()


    #---------------------------
    # boost headers (for C++14 support)
    #---------------------------
    if (CMAKE_CXX_STANDARD EQUAL 14)
        if (DEFINED ENV{BOOST_ROOT})
            set(Boost_INCLUDE_DIRS $ENV{BOOST_ROOT}/include)
        else ()
            find_package(Boost)
            if (NOT Boost_FOUND)
                message(FATAL_ERROR "Cound not find library: boost!")
            endif ()
        endif ()
        include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
    endif ()

endif ()


#---------------------------
# Build Option: UT stubs
#---------------------------
if (BUILD_OPTION_ENABLE_UT_STUBS)
    add_definitions(-DKAFKA_API_ENABLE_UNIT_TEST_STUBS)
endif ()


#---------------------------
# Build Option: clang-tidy
#---------------------------
option(BUILD_OPTION_CLANG_TIDY "Build with clang-tidy enabled" OFF)
if (BUILD_OPTION_CLANG_TIDY)
    find_program(CLANG_TIDY_EXE NAMES "clang-tidy")

    if (CLANG_TIDY_EXE)
        message(STATUS "Use clang-tidy: ${CLANG_TIDY_EXE}")
        set(CMAKE_CXX_CLANG_TIDY clang-tidy -warnings-as-errors=* -header-filter=.*)
    else ()
        message(FATAL_ERROR "The clang-tidy executable not found!")
    endif ()

else ()
    message(STATUS "With NO clang-tidy build option")
endif ()


#---------------------------
# Build Option: ASAN
#---------------------------
option(BUILD_OPTION_USE_ASAN "Build with Address Sanitizer (ASAN) enabled" OFF)

if (BUILD_OPTION_USE_ASAN)
    check_cxx_compiler_flag("-fsanitize=address" HAS_ASAN)

    CMAKE_PUSH_CHECK_STATE(RESET)
        # Make check_cxx_compiler_flag pass required flags to linker as well:
        set(CMAKE_REQUIRED_FLAGS "-fsanitize=address -static-libasan")
        check_cxx_compiler_flag("-fsanitize=address -static-libasan" HAS_ASAN_NEEDS_LIB)
    CMAKE_POP_CHECK_STATE()

    if (HAS_ASAN)
        add_compile_options("-fsanitize=address")
        add_link_options("-fsanitize=address")
    elseif (HAS_ASAN_NEEDS_LIB)
        add_compile_options("-fsanitize=address" "-static-libasan")
        add_link_options("-fsanitize=address" "-static-libasan")
    else ()
        message(FATAL_ERROR "Address Sanitizer requested by BUILD_OPTION_USE_ASAN, but appears to be not supported on this platform")
    endif ()

    set(MEMORYCHECK_TYPE AddressSanitizer)

    message(STATUS "Use Address Sanitizer")
endif ()


#---------------------------
# Build Option: TSAN
#---------------------------
option(BUILD_OPTION_USE_TSAN "Build with Thread Sanitizer (TSAN) enabled" OFF)

if (BUILD_OPTION_USE_TSAN)
    check_cxx_compiler_flag("-fsanitize=thread" HAS_TSAN)

    CMAKE_PUSH_CHECK_STATE(RESET)
        # Make check_cxx_compiler_flag pass required flags to linker as well:
        set(CMAKE_REQUIRED_FLAGS "-fsanitize=thread -static-libtsan")
        check_cxx_compiler_flag("-fsanitize=thread -static-libtsan" HAS_TSAN_NEEDS_LIB)
    CMAKE_POP_CHECK_STATE()

    if (HAS_TSAN)
        add_compile_options("-fsanitize=thread")
        add_link_options("-fsanitize=thread")
    elseif (HAS_TSAN_NEEDS_LIB)
        add_compile_options("-fsanitize=thread" "-static-libtsan")
        add_link_options("-fsanitize=thread" "-static-libtsan")
    else ()
        message(FATAL_ERROR "Thread Sanitizer requested by BUILD_OPTION_USE_TSAN, but appears to be not supported on this platform")
    endif ()

    set(MEMORYCHECK_TYPE ThreadSanitizer)

    message(STATUS "Use Thread Sanitizer")
endif ()


#---------------------------
# Build Option: UBSAN
#---------------------------
option(BUILD_OPTION_USE_UBSAN "Build with Undefined Behavior Sanitizer (UBSAN) enabled" OFF)

if (BUILD_OPTION_USE_UBSAN)
    check_cxx_compiler_flag("-fsanitize=undefined" HAS_UBSAN)

    CMAKE_PUSH_CHECK_STATE(RESET)
        # Make check_cxx_compiler_flag pass required flags to linker as well:
        set(CMAKE_REQUIRED_FLAGS "-fsanitize=undefined -static-libubsan")
        check_cxx_compiler_flag("-fsanitize=undefined -static-libubsan" HAS_UBSAN_NEEDS_LIB)
    CMAKE_POP_CHECK_STATE()

    if (HAS_UBSAN_NEEDS_LIB)
        add_compile_options("-fsanitize=undefined" "-static-libubsan")
        add_link_options("-fsanitize=undefined" "-static-libubsan")
    elseif (HAS_UBSAN)
        add_compile_options("-fsanitize=undefined")
        add_link_options("-fsanitize=undefined")
    else ()
        message(FATAL_ERROR "Undefined Behavior Sanitizer requested by BUILD_OPTION_USE_UBSAN, but appears to be not supported on this platform")
    endif ()

    message(STATUS "Use Undefined Behavior Sanitizer")
endif ()


#---------------------------
# Build Option: generate doc
#---------------------------
option(BUILD_OPTION_GEN_DOC "Generate html files for doxygen/markdown doc" OFF)


#---------------------------
# Build Option: generate coverage report
#---------------------------
option(BUILD_OPTION_GEN_COVERAGE "Generate code coverage report" OFF)

if (BUILD_OPTION_GEN_COVERAGE)
    check_cxx_compiler_flag("-fprofile-instr-generate -fcoverage-mapping" HAS_CLANG_COV)

    if (HAS_CLANG_COV)
        add_compile_options("-fprofile-instr-generate" "-fcoverage-mapping")
        add_link_options("-fprofile-instr-generate" "-fcoverage-mapping")

        add_custom_target(coverage_init
            COMMENT "Initialize coverage counters"
            COMMAND "rm" "-f" "tests/unit/default.profraw" "tests/default.profdata"
        )

        add_custom_target(coverage
            COMMENT "Generate coverage report"
            COMMAND "llvm-profdata" "merge" "-sparse" "tests/unit/default.profraw"
                                  "-o" "tests/default.profdata"
            COMMAND "llvm-cov" "show" "-format" "html" "-instr-profile" "tests/default.profdata" "tests/unit/kafka-unit-test"
                             ">" "coverage_report.html"
            COMMAND "echo" "Coverage report generated: coverage_report.html"
        )

    else ()
        message(FATAL_ERROR "Coverage report requrested by BUILD_OPTION_GEN_COVERAGE, but only supported with Clang build")
    endif ()

    message(STATUS "Enable code coverage data generation")
endif ()


#---------------------------
# Build Sub-directories
#---------------------------
if (BUILD_OPTION_DOC_ONLY)
    add_subdirectory("doc")
else ()
    if (BUILD_OPTION_GEN_DOC)
        add_subdirectory("doc")
    endif ()

    add_subdirectory("include")

    if (CPPKAFKA_ENABLE_TESTS)
        include(CTest)
        add_subdirectory("tests")
    endif()

    if (cppkafka_master_project)
        add_subdirectory("tools")
        add_subdirectory("examples")
    endif()
endif ()
