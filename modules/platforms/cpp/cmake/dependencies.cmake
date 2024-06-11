if (CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    # Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP:
    cmake_policy(SET CMP0135 NEW)
endif()

# Policy about variables override by options
# https://cmake.org/cmake/help/latest/policy/CMP0077.html
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

set(MSGPACK_BUILD_TESTS OFF)
set(MSGPACK_GEN_COVERAGE OFF)
set(MSGPACK_BUILD_EXAMPLES OFF)

if (${USE_LOCAL_DEPS})
    find_package(msgpack REQUIRED)
    if (${msgpack_FOUND})
        # ALIAS for the msgpack-c version lower than 6.0.0
        add_library(msgpack-c-static ALIAS msgpackc-static)
        message(STATUS "SYSTEM MSGPACK FOUND: " ${msgpack_VERSION})
    endif()

    if (${ENABLE_TESTS})
        find_package(GTest REQUIRED)
        if (${GTest_FOUND})
            if (TARGET GTest::Main AND NOT TARGET GTest::gtest_main)
                add_library(GTest::gtest_main ALIAS GTest::Main)
            endif()
            message(STATUS "SYSTEM GTEST FOUND: " ${GTest_VERSION})
        endif()
    endif()
else()
    include(FetchContent)
    message(STATUS "DOWNLOAD MSGPACK")
    FetchContent_Declare(
            msgpack-c
            URL https://github.com/msgpack/msgpack-c/releases/download/c-6.0.1/msgpack-c-6.0.1.tar.gz
            URL_HASH MD5=090df53a59b845767fcfc48221b30ee9
    )
    FetchContent_MakeAvailable(msgpack-c)

    if (${ENABLE_TESTS})
        message(STATUS "DOWNLOAD GTEST")
        FetchContent_Declare(
                googletest
                URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.tar.gz
                URL_HASH MD5=c8340a482851ef6a3fe618a082304cfc
        )
        FetchContent_MakeAvailable(googletest)
    endif()
endif()
