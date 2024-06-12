if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    # Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP:
    cmake_policy(SET CMP0135 NEW)
endif()

# Policy about variables override by options
# https://cmake.org/cmake/help/latest/policy/CMP0077.html
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(INSTALL_GTEST OFF)

set(MSGPACK_BUILD_TESTS OFF)
set(MSGPACK_GEN_COVERAGE OFF)
set(MSGPACK_BUILD_EXAMPLES OFF)

set(INSTALL_MBEDTLS_HEADERS OFF)
set(ENABLE_PROGRAMS OFF)
set(ENABLE_TESTING OFF)
set(MBEDTLS_AS_SUBPROJECT ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_definitions(MBEDTLS_USER_CONFIG_FILE="${CMAKE_CURRENT_SOURCE_DIR}/ignite/common/ignite_mbedtls_config.h")

if (${USE_LOCAL_DEPS})
    find_package(msgpack REQUIRED)
    if (${msgpack_FOUND})
        if (NOT TARGET msgpack-c-static)
            # ALIAS for the msgpack-c version lower than 6.0.0
            add_library(msgpack-c-static ALIAS msgpackc-static)
        endif()
        message(STATUS "MSGPACK FOUND: " ${msgpack_VERSION})
    endif()

    if (NOT DEFINED MBEDTLS_SOURCE_DIR)
        message( FATAL_ERROR "With USE_LOCAL_DEPS specified you have to set MBEDTLS_SOURCE_DIR to path to the MbedTLS source code")
    endif()

    add_subdirectory(${MBEDTLS_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/mbedtls EXCLUDE_FROM_ALL)

    if (${ENABLE_TESTS})
        find_package(GTest REQUIRED)
        if (${GTest_FOUND})
            if (TARGET GTest::Main AND NOT TARGET GTest::gtest_main)
                add_library(GTest::gtest_main ALIAS GTest::Main)
            endif()
            message(STATUS "GTEST FOUND: " ${GTest_VERSION})
        endif()
    endif()
else()
    include(FetchContent)
    message(STATUS "DOWNLOAD MSGPACK")
    FetchContent_Declare(
            msgpack-c
            URL https://github.com/msgpack/msgpack-c/releases/download/c-6.0.1/msgpack-c-6.0.1.tar.gz
            URL_HASH MD5=090df53a59b845767fcfc48221b30ee9
            OVERRIDE_FIND_PACKAGE
    )
    FetchContent_GetProperties(msgpack-c)
    if(NOT msgpack-c_POPULATED)
        FetchContent_Populate(msgpack-c)
        add_subdirectory(${msgpack-c_SOURCE_DIR} ${msgpack-c_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()

    message(STATUS "DOWNLOAD MBEDTLS")
    FetchContent_Declare(
            mbedtls
            URL https://github.com/Mbed-TLS/mbedtls/releases/download/v3.6.0/mbedtls-3.6.0.tar.bz2
            URL_HASH MD5=6b5a45b10e7d1c768ecec69ecf8e7abd
            OVERRIDE_FIND_PACKAGE
    )
    FetchContent_GetProperties(mbedtls)
    if(NOT mbedtls_POPULATED)
        FetchContent_Populate(mbedtls)
        add_subdirectory(${mbedtls_SOURCE_DIR} ${mbedtls_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()

    if (${ENABLE_TESTS})
        message(STATUS "DOWNLOAD GTEST")
        FetchContent_Declare(
                googletest
                URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
                URL_HASH MD5=c8340a482851ef6a3fe618a082304cfc
                OVERRIDE_FIND_PACKAGE
        )
        FetchContent_GetProperties(googletest)
        if(NOT googletest_POPULATED)
            FetchContent_Populate(googletest)
            add_subdirectory(${googletest_SOURCE_DIR} ${googletest_BINARY_DIR} EXCLUDE_FROM_ALL)
        endif()
    endif()
endif()

target_compile_definitions(mbedtls INTERFACE MBEDTLS_ALLOW_PRIVATE_ACCESS)
