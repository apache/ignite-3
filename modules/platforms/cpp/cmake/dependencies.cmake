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
set(MSGPACK_ENABLE_STATIC ON)
set(MSGPACK_ENABLE_SHARED OFF)

set(INSTALL_MBEDTLS_HEADERS OFF)
set(ENABLE_PROGRAMS OFF)
set(ENABLE_TESTING OFF)
set(MBEDTLS_AS_SUBPROJECT ON)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

add_compile_definitions(MBEDTLS_USER_CONFIG_FILE="${CMAKE_CURRENT_SOURCE_DIR}/ignite/common/detail/ignite_mbedtls_config.h")

if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.30.0")
    # Avoid warning about FetchContent_Populate:
    cmake_policy(SET CMP0169 OLD)
endif()

function(fetch_dependency NAME URL MD5)
    message(STATUS "Download dependency: ${NAME}")
    FetchContent_Declare(
        ${NAME}
        URL ${URL}
        URL_HASH MD5=${MD5}
    )
    FetchContent_GetProperties(${NAME})
    if(NOT ${NAME}_POPULATED)
        FetchContent_Populate(${NAME})
        add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR} EXCLUDE_FROM_ALL)
    endif()
endfunction()

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
    find_package(uni-algo CONFIG REQUIRED)

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
    fetch_dependency(msgpack-c https://github.com/msgpack/msgpack-c/releases/download/c-6.0.1/msgpack-c-6.0.1.tar.gz 090df53a59b845767fcfc48221b30ee9)
    fetch_dependency(mbedtls https://github.com/Mbed-TLS/mbedtls/releases/download/v3.6.0/mbedtls-3.6.0.tar.bz2 6b5a45b10e7d1c768ecec69ecf8e7abd)
    fetch_dependency(uni-algo https://github.com/uni-algo/uni-algo/archive/v1.2.0.tar.gz 6e0cce94a6b45ebee7b904316df9f87f)
    if (${ENABLE_TESTS})
        fetch_dependency(googletest https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz c8340a482851ef6a3fe618a082304cfc)
    endif()
endif()

target_compile_definitions(mbedtls INTERFACE MBEDTLS_ALLOW_PRIVATE_ACCESS)
