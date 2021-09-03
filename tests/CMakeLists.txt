#---------------------------
# boost
#---------------------------
if (DEFINED ENV{BOOST_ROOT})
    set(Boost_INCLUDE_DIRS $ENV{BOOST_ROOT}/include)
    set(Boost_LIBRARY_DIRS "$ENV{BOOST_ROOT}/lib $ENV{BOOST_ROOT}/bin")
else ()
    find_package(Boost)
    if (NOT Boost_FOUND)
        message(FATAL_ERROR "Boost not found!")
    endif ()
endif ()

message(STATUS "boost include directory: ${Boost_INCLUDE_DIRS}")
message(STATUS "boost library directory: ${Boost_LIBRARY_DIRS}")

include_directories(SYSTEM ${Boost_INCLUDE_DIRS})
link_directories(${Boost_LIBRARY_DIRS})


#---------------------------
# googletest
#---------------------------
if (DEFINED ENV{GTEST_ROOT})
    set(GTEST_ROOT $ENV{GTEST_ROOT})
else ()
    set(GTEST_ROOT /usr/local)
endif ()

message(STATUS "googletest root directory: ${GTEST_ROOT}")

include_directories(SYSTEM ${GTEST_ROOT}/include)
link_directories(${GTEST_ROOT}/lib ${GTEST_ROOT}/bin)


#---------------------------
# Sub-directories
#---------------------------
enable_testing()

add_subdirectory(unit)
add_subdirectory(integration)
add_subdirectory(robustness)
