#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 3.18)

include(CMakePackageConfigHelpers)

file(READ _version.txt IGNITE_VERSION)
if(NOT DEFINED IGNITE_VERSION)
  set(IGNITE_VERSION 3.0.0)
  message(WARNING "Failed to read version from _version.txt file. Using default version \"${IGNITE_VERSION}\".")
endif()

project(ignite3 VERSION ${IGNITE_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_PROJECT_VERSION ${PROJECT_VERSION})

option(USE_LOCAL_DEPS "Use local dependencies." OFF)
option(ENABLE_CLIENT "Build Ignite.C++ Client module" ON)
option(ENABLE_ODBC "Build Ignite ODBC driver module" OFF)
option(ENABLE_PROTOCOL "Build Ignite Protocol library" ON)
option(ENABLE_TESTS "Build Ignite.C++ tests" OFF)
option(ENABLE_COMPATIBILITY_TESTS "Build Ignite.C++ compatibility tests" OFF)
option(ENABLE_ADDRESS_SANITIZER "If address sanitizer is enabled" OFF)
option(ENABLE_UB_SANITIZER "If undefined behavior sanitizer is enabled" OFF)
option(WARNINGS_AS_ERRORS "Treat warning as errors" OFF)
option(INSTALL_IGNITE_FILES "Install Ignite files" ON)
option(ENABLE_IWYU "Enable include-what-you-use tool" OFF)
option(ENABLE_CLANG_TIDY "Enable clang-tidy static analyzer" OFF)
option(ENABLE_CPPCHECK "Enable cppcheck static analyzer" OFF)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR})

get_property(is_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)

set(IGNITE_CMAKE_TOP_DIR ${CMAKE_CURRENT_LIST_DIR})

include(dependencies)

if(ENABLE_IWYU)
    find_program(IWYU_PATH NAMES include-what-you-use iwyu)
    if(NOT IWYU_PATH)
        message(FATAL_ERROR "Could not find the program include-what-you-use")
    endif()
    set(CMAKE_CXX_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
    set(CMAKE_C_INCLUDE_WHAT_YOU_USE ${IWYU_PATH})
endif()

if(ENABLE_CLANG_TIDY)
    find_program(CLANG_TIDY_BIN clang-tidy REQUIRED)
    if(CLANG_TIDY_BIN)
        message(STATUS "clang-tidy: ${CLANG_TIDY_BIN}")
        set(CMAKE_CXX_CLANG_TIDY ${CLANG_TIDY_BIN}
                "--checks=-*,clang-analyzer-*,clang-analyzer-cplusplus*,bugprone*,concurrency-*,cppcoreguidelines-*,performance-*,portability-*,readability-*"
                "--header-filter=\"^.*(client|common|network|odbc|protocol|tuple).*\"gm"
                "--exclude-header-filter=\"^.*(build|tests).*\"gm")
        set(CMAKE_CXX_CLANG_TIDY_EXPORT_FIXES_DIR "clang-tidy-reports-dir")
    else()
        message(STATUS "clang-tidy not found!")
    endif()
endif()

if(ENABLE_CPPCHECK)
    find_program(CPPCHECK_BIN cppcheck REQUIRED)
    if(CPPCHECK_BIN)
        message(STATUS "cppcheck: ${CPPCHECK_BIN}")
        message(STATUS "Please do not use parallel build with cppcheck enabled! Cppcheck writes results to the file and result might be corrupted in case of parallel build.")
        set(CMAKE_CXX_CPPCHECK ${CPPCHECK_BIN} --enable=all --xml --xml-version=3 --output-file=cppcheck_report.xml)
        add_custom_target(cppcheck_html_report ALL
            COMMAND cppcheck-htmlreport
                --file ${CMAKE_CURRENT_BINARY_DIR}/ignite/tuple/cppcheck_report.xml
                --file ${CMAKE_CURRENT_BINARY_DIR}/ignite/protocol/cppcheck_report.xml
                --file ${CMAKE_CURRENT_BINARY_DIR}/ignite/common/cppcheck_report.xml
                --file ${CMAKE_CURRENT_BINARY_DIR}/ignite/network/cppcheck_report.xml
                $<$<BOOL:${ENABLE_TESTS}>:--file ${CMAKE_CURRENT_BINARY_DIR}/tests/client-test/cppcheck_report.xml>
                $<$<BOOL:${ENABLE_TESTS}>:--file ${CMAKE_CURRENT_BINARY_DIR}/tests/fake_server/cppcheck_report.xml>
                $<$<BOOL:${ENABLE_TESTS}>:--file ${CMAKE_CURRENT_BINARY_DIR}/tests/odbc-test/cppcheck_report.xml>
                $<$<BOOL:${ENABLE_TESTS}>:--file ${CMAKE_CURRENT_BINARY_DIR}/tests/test-common/cppcheck_report.xml>
                $<$<BOOL:${ENABLE_ODBC}>:--file ${CMAKE_CURRENT_BINARY_DIR}/ignite/odbc/cppcheck_report.xml>
                $<$<BOOL:${ENABLE_CLIENT}>:--file ${CMAKE_CURRENT_BINARY_DIR}/ignite/client/cppcheck_report.xml>
                --report-dir=${CMAKE_CURRENT_BINARY_DIR}/cppcheck_report
            DEPENDS
                ignite-tuple
                ignite-protocol
                ignite-common
                ignite-network
                $<$<BOOL:${ENABLE_TESTS}>:ignite-client-test>
                $<$<BOOL:${ENABLE_TESTS}>:ignite-fake-server>
                $<$<BOOL:${ENABLE_TESTS}>:ignite-odbc-test>
                $<$<BOOL:${ENABLE_TESTS}>:ignite-test-common>
                $<$<BOOL:${ENABLE_ODBC}>:ignite3-odbc>
                $<$<BOOL:${ENABLE_CLIENT}>:ignite3-client>
            WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
            VERBATIM
        )
    else()
        message(STATUS "cppcheck not found!")
    endif()
endif()

# Configure build paths.
if (is_multi_config)
    foreach(TYPE ${CMAKE_CONFIGURATION_TYPES})
        string(TOUPPER ${TYPE} UTYPE)

        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${UTYPE} ${CMAKE_BINARY_DIR}/${TYPE}/lib)
        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${UTYPE} ${CMAKE_BINARY_DIR}/${TYPE}/lib)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${UTYPE} ${CMAKE_BINARY_DIR}/${TYPE}/bin)
    endforeach()
else()
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
endif()

if (WIN32)
    add_definitions(-DNOMINMAX)
else()
    include(GNUInstallDirs)
endif()

set(IGNITE_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR}/ignite)

include(cmake/packaging.cmake)
include(CPack)

message(STATUS "IGNITE_INCLUDEDIR=${IGNITE_INCLUDEDIR}")
include(ignite_install_headers)

# Turn on DLL export directives.
add_definitions(-DIGNITE_IMPL)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if (MSVC)
    add_compile_options(/source-charset:utf-8 /execution-charset:utf-8)
    add_definitions(-D_CRT_SECURE_NO_WARNINGS -D_SCL_SECURE_NO_WARNINGS)
endif()

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    add_compile_options(-Wall -Wextra -Wno-variadic-macros)

    if (ENABLE_ADDRESS_SANITIZER)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=address>)
        add_link_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=address>)
    endif()

    if (ENABLE_UB_SANITIZER)
        add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=undefined>)
        add_link_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=undefined>)
    endif()
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Debug" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    add_compile_options("-fstandalone-debug")
endif()

if (${WARNINGS_AS_ERRORS})
    if (MSVC)
        add_compile_options(/WX)
    else()
        add_compile_options(-Werror)
    endif()
endif()

# Setup gtest for unit & integration tests.
if (${ENABLE_TESTS})
    include(GoogleTest)
    enable_testing()
endif()

include(ignite_test)

# Add common libraries along with their unit tests if any.
add_subdirectory(ignite/common)
add_subdirectory(ignite/tuple)

# Add client and ODBC libraries.
if (${ENABLE_CLIENT} OR ${ENABLE_ODBC} OR ${ENABLE_PROTOCOL})
    add_subdirectory(ignite/protocol)
    add_subdirectory(ignite/network)
endif()

# Add client library.
if (${ENABLE_CLIENT})
    add_subdirectory(ignite/client)
endif()

# Add client libraries.
if (${ENABLE_ODBC})
    add_subdirectory(ignite/odbc)
endif()

# Add integration tests.
if (${ENABLE_TESTS})
    if (${ENABLE_CLIENT} OR ${ENABLE_ODBC} OR ${ENABLE_COMPATIBILITY_TESTS})
        add_subdirectory(tests/test-common)
    endif()

    if (${ENABLE_CLIENT})
        add_subdirectory(tests/client-test)
        add_subdirectory(tests/fake_server)
    endif()

    if (${ENABLE_ODBC})
        add_subdirectory(tests/odbc-test)
    endif()

    if (${ENABLE_COMPATIBILITY_TESTS})
        add_subdirectory(tests/compatibility-tests)
    endif()
endif()

# Source code formatting with clang-format.
# TODO: require clang-format version 13 or higher
find_program(CLANG_FORMAT_BIN clang-format)
if (CLANG_FORMAT_BIN)
    message(STATUS "Found clang-format: ${CLANG_FORMAT_BIN}")
    message(STATUS "Add 'format' target to run clang-format for entire source code.")

    file(GLOB_RECURSE ALL_SOURCE_FILES *.cpp *.h)

    add_custom_target(format
        COMMENT "Running clang-format to change files"
        COMMAND ${CLANG_FORMAT_BIN} -i ${ALL_SOURCE_FILES})
else()
    message(STATUS "Failed to find clang-format. So there is no 'format' target now.")
endif()

if (${ENABLE_CLIENT})
    list(APPEND IGNITE_AVAILABLE_COMPONENTS client)
endif()
if (${ENABLE_ODBC})
    list(APPEND IGNITE_AVAILABLE_COMPONENTS odbc)
endif()

configure_package_config_file(
    "cmake/ignite-config.cmake.in"
    "cmake/ignite-config.cmake"
    INSTALL_DESTINATION "cmake"
)

add_subdirectory(pkgconfig)

write_basic_package_version_file(
    "cmake/ignite-config-version.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion)

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/ignite-config.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/cmake/ignite-config-version.cmake"
    COMPONENT client
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ignite"
)

# Do not export the package by default
cmake_policy(SET CMP0090 NEW)

# Make this package visible to the system
export(PACKAGE ignite)
