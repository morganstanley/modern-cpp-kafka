# Check googlemock directory
if(DEFINED ENV{GMOCK_ROOT})
    set(GMOCK_ROOT $ENV{GMOCK_ROOT})
    message(STATUS "googlemock root directory: ${GMOCK_ROOT}")
else()
    message(FATAL_ERROR "No GMOCK_ROOT specified!")
endif()

add_subdirectory(unit)
add_subdirectory(integration)
