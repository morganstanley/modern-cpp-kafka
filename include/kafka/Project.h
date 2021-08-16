#pragma once

// Customize the namespace (default is `kafka`) if necessary
#ifndef KAFKA_API
#define KAFKA_API kafka
#endif

// Here is the MACRO to enable internal stubs for UT
// #ifndef KAFKA_API_ENABLE_UNIT_TEST_STUBS
// #define KAFKA_API_ENABLE_UNIT_TEST_STUBS
// #endif

#if defined(WIN32) && !defined(NOMINMAX)
#define NOMINMAX

#define COMPILER_SUPPORTS_CPP_17 ((__cplusplus >= 201703L) || (defined(_MSVC_LANG) && _MSVC_LANG >= 201703L))

#endif

