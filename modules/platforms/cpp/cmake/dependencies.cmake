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

add_compile_definitions(TF_PSA_CRYPTO_USER_CONFIG_FILE="${IGNITE_CMAKE_TOP_DIR}/ignite/common/detail/ignite_mbedtls_config.h")

if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.30.0")
    # Avoid warning about FetchContent_Populate:
    cmake_policy(SET CMP0169 OLD)
endif()

function(fetch_dependency NAME URL SHA256 ADD_SUBDIR)
    message(STATUS "Download dependency: ${NAME}")
    FetchContent_Declare(
        ${NAME}
        URL ${URL}
        URL_HASH SHA256=${SHA256}
    )
    FetchContent_GetProperties(${NAME})
    if(NOT ${NAME}_POPULATED)
        FetchContent_Populate(${NAME})

        if(${ADD_SUBDIR})
            add_subdirectory(${${NAME}_SOURCE_DIR} ${${NAME}_BINARY_DIR} EXCLUDE_FROM_ALL)
        endif()
    endif()
endfunction()

function(add_asio_dependency)
    fetch_dependency(asio https://github.com/chriskohlhoff/asio/archive/refs/tags/asio-1-36-0.tar.gz 0310a76b27e1854f09f696b30de57dc490b5e1b17faed1eb8c9a2891f956e52b FALSE)

    FetchContent_GetProperties(asio)
    if (NOT TARGET asio)
        add_library(asio INTERFACE)
        target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/asio/include)
        target_compile_definitions(asio INTERFACE ASIO_STANDALONE)
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

    if (NOT DEFINED TF_PSA_SOURCE_DIR)
        message( FATAL_ERROR "With USE_LOCAL_DEPS specified you have to set TF_PSA_SOURCE_DIR to path to the TF-PSA-Crypto source code")
    endif()

    add_subdirectory(${TF_PSA_SOURCE_DIR} ${CMAKE_CURRENT_BINARY_DIR}/tf-psa EXCLUDE_FROM_ALL)
    find_package(uni-algo CONFIG REQUIRED)

    if (${ENABLE_TESTS})
        find_package(GTest REQUIRED)
        if (${GTest_FOUND})
            if (TARGET GTest::Main AND NOT TARGET GTest::gtest_main)
                add_library(GTest::gtest_main ALIAS GTest::Main)
            endif()
            message(STATUS "GTEST FOUND: " ${GTest_VERSION})
        endif()

        find_package(asio REQUIRED)
        if (${asio_FOUND})
            if (NOT TARGET asio)
                add_library(asio INTERFACE)
                target_include_directories(asio INTERFACE ${asio_SOURCE_DIR}/asio/include)
                target_compile_definitions(asio INTERFACE ASIO_STANDALONE)

                message(STATUS "asio FOUND: " ${asio_VERSION})
            endif()
        endif()
    endif()
else()
    include(FetchContent)
    fetch_dependency(msgpack-c https://github.com/msgpack/msgpack-c/releases/download/c-6.0.1/msgpack-c-6.0.1.tar.gz a349cd9af28add2334c7009e331335af4a5b97d8558b2e9804d05f3b33d97925 TRUE)
    fetch_dependency(tf-psa https://github.com/Mbed-TLS/TF-PSA-Crypto/releases/download/tf-psa-crypto-1.0.0/tf-psa-crypto-1.0.0.tar.bz2 31f0df2ca17897b5db2757cb0307dcde267292ba21ade831663d972a7a5b7d40 TRUE)
    fetch_dependency(uni-algo https://github.com/uni-algo/uni-algo/archive/v1.2.0.tar.gz f2a1539cd8635bc6088d05144a73ecfe7b4d74ee0361fabed6f87f9f19e74ca9 TRUE)
    if (${ENABLE_TESTS})
        fetch_dependency(googletest https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz 8ad598c73ad796e0d8280b082cebd82a630d73e73cd3c70057938a6501bba5d7 TRUE)
        add_asio_dependency()
    endif()
endif()

target_compile_definitions(tfpsacrypto INTERFACE MBEDTLS_ALLOW_PRIVATE_ACCESS)
