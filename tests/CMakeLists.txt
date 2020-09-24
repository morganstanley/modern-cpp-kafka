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
# Check googletest directory
#---------------------------
if(DEFINED ENV{GTEST_ROOT})
    set(GTEST_ROOT $ENV{GTEST_ROOT})
else()
    set(GTEST_ROOT /usr/local)
endif()

message(STATUS "googletest root directory: ${GTEST_ROOT}")


add_subdirectory(unit)
add_subdirectory(integration)
