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
project(Ignite.C++ VERSION 3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_PROJECT_VERSION ${PROJECT_VERSION})

option(ENABLE_CONAN "Use Conan package manager to get dependencies" ON)
option(ENABLE_CLIENT "Build Ignite.C++ Client module" ON)
option(ENABLE_ODBC "Build Ignite ODBC driver module" OFF)
option(ENABLE_TESTS "Build Ignite.C++ tests" OFF)
option(ENABLE_ADDRESS_SANITIZER "If address sanitizer is enabled" OFF)
option(ENABLE_UB_SANITIZER "If undefined behavior sanitizer is enabled" OFF)
option(WARNINGS_AS_ERRORS "Treat warning as errors" OFF)
option(INSTALL_IGNITE_FILES "Install Ignite files" ON)

set(CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)
list(APPEND CMAKE_MODULE_PATH ${CMAKE_BINARY_DIR})
list(APPEND CMAKE_PREFIX_PATH ${CMAKE_BINARY_DIR})

get_property(is_multi_config GLOBAL PROPERTY GENERATOR_IS_MULTI_CONFIG)

set(conan_packages
    msgpack-c/4.0.0
    date/3.0.1
)

if (${ENABLE_TESTS})
    list(APPEND conan_packages
        gtest/1.12.1
    )
endif()

# Install required libraries.
if (${ENABLE_CONAN})
    include(conan)

    if (is_multi_config)
        # These settings are for multi-config generators like Visual Studio.
        message(STATUS "Using a multi-config generator")

        conan_cmake_configure(REQUIRES ${conan_packages}
                              GENERATORS cmake_find_package_multi)

        foreach(TYPE ${CMAKE_CONFIGURATION_TYPES})
            conan_cmake_autodetect(conan_settings BUILD_TYPE ${TYPE})

            conan_cmake_install(PATH_OR_REFERENCE .
                                BUILD missing
                                REMOTE conancenter
                                SETTINGS ${conan_settings})
        endforeach()
    else()
        # These settings are for single-config generators like makefiles, ninja.
        message(STATUS "Using a single-config generator")

        conan_cmake_configure(REQUIRES ${conan_packages}
                              GENERATORS cmake_find_package)

        conan_cmake_autodetect(conan_settings)

        conan_cmake_install(PATH_OR_REFERENCE .
                            BUILD missing
                            REMOTE conancenter
                            SETTINGS ${conan_settings})
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
message(STATUS "CMAKE_INSTALL_PREFIX=${CMAKE_INSTALL_PREFIX}")
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
    find_package(GTest REQUIRED)
    include(GoogleTest)
    enable_testing()
endif()

include(ignite_test)

# Add common libraries along with their unit tests if any.
add_subdirectory(ignite/common)
add_subdirectory(ignite/tuple)

# Add client and ODBC libraries.
if (${ENABLE_CLIENT} OR ${ENABLE_ODBC})
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
    if (${ENABLE_CLIENT} OR ${ENABLE_ODBC})
        add_subdirectory(tests/test-common)
    endif()

    if (${ENABLE_CLIENT})
        add_subdirectory(tests/client-test)
    endif()

    if (${ENABLE_ODBC})
        add_subdirectory(tests/odbc-test)
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
