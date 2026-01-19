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

project(ignite_package_install_test)

set(CMAKE_CXX_STANDARD 17)

option(ENABLE_CLIENT "Build Ignite.C++ Client module" OFF)
option(ENABLE_ODBC "Build Ignite ODBC driver module" OFF)

set(IGNITE_SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../../")
set(IGNITE_INSTALL_DIR "${CMAKE_CURRENT_BINARY_DIR}/cmake_package_install")
set(IGNITE_BINARY_DIR "${IGNITE_INSTALL_DIR}${CMAKE_FILES_DIRECTORY}")

execute_process(
    COMMAND "${CMAKE_COMMAND}"
    "-H${IGNITE_SOURCE_DIR}"
    "-B${IGNITE_BINARY_DIR}"
    "-DENABLE_ODBC=${ENABLE_ODBC}"
    "-DENABLE_CLIENT=${ENABLE_CLIENT}"
    "-DENABLE_TESTS=OFF"
    "-DCMAKE_INSTALL_PREFIX=${IGNITE_INSTALL_DIR}"
    RESULT_VARIABLE CONFIGURE_RESULT
    ERROR_VARIABLE CONFIGURE_ERROR
)

if(NOT CONFIGURE_RESULT EQUAL 0)
    message(FATAL_ERROR "CMake configure step failed: ${CONFIGURE_ERROR}")
endif()

execute_process(
    COMMAND "${CMAKE_COMMAND}"
        --build "${IGNITE_BINARY_DIR}"
        --target install
    RESULT_VARIABLE BUILD_RESULT
    ERROR_VARIABLE BUILD_ERROR
)

if(NOT BUILD_RESULT EQUAL 0)
    message(FATAL_ERROR "CMake build step failed: ${BUILD_ERROR}")
endif()

set(IGNITE_COMPONENTS "")
if(ENABLE_CLIENT)
    list(APPEND IGNITE_COMPONENTS client)
endif()
if(ENABLE_ODBC)
    list(APPEND IGNITE_COMPONENTS odbc)
endif()

list(INSERT CMAKE_PREFIX_PATH 0 "${IGNITE_INSTALL_DIR}")
find_package(ignite REQUIRED COMPONENTS ${IGNITE_COMPONENTS})

if(NOT ignite_FOUND)
    message(FATAL_ERROR "Ignite is not found!")
endif()

if(ENABLE_CLIENT)
    if(NOT ignite_client_FOUND)
        message(FATAL_ERROR "Ignite client component is not found!")
    endif()
    add_executable(test_client ${CMAKE_CURRENT_LIST_DIR}/../test_client.cpp)
    target_link_libraries(test_client ignite::client)
endif()

if(ENABLE_ODBC)
    if(NOT ignite_odbc_FOUND)
        message(FATAL_ERROR "Ignite odbc component is not found!")
    endif()
    add_executable(test_odbc ${CMAKE_CURRENT_LIST_DIR}/../test_odbc.cpp)
    target_link_libraries(test_odbc ignite::odbc)
endif()
