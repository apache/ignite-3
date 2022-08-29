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

cmake_minimum_required(VERSION 3.10)
project(Ignite.C++ VERSION 3 LANGUAGES CXX)

find_package(Boost REQUIRED COMPONENTS unit_test_framework)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_PROJECT_VERSION ${PROJECT_VERSION})

include(${CMAKE_BINARY_DIR}/conanbuildinfo.cmake)
conan_basic_setup()

if (UNIX)
	include(GNUInstallDirs)
endif()

if (MSVC)
	add_compile_options(/source-charset:utf-8 /execution-charset:utf-8)
	add_definitions(-D_CRT_SECURE_NO_WARNINGS -D_SCL_SECURE_NO_WARNINGS)
endif()

set(INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR}/ignite)

option(ENABLE_ADDRESS_SANITIZER "If address sanitizer is enabled" OFF)
option(ENABLE_UB_SANITIZER "If undefined behavior sanitizer is enabled" OFF)
option(ENABLE_CLIENT "Build Ignite.C++ Client module" ON)
option(ENABLE_TESTS "Build Ignite.C++ tests" OFF)
option(WARNINGS_AS_ERRORS "Treat warning as errors" OFF)

# Turn on DLL export directives
add_compile_definitions(IGNITE_IMPL)

if (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU")
    include(GNUInstallDirs)
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
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstandalone-debug")
endif()

if (${WARNINGS_AS_ERRORS})
    if (MSVC)
        add_compile_options(/WX)
    else()
        add_compile_options(-Wall -Wextra -Werror -Wno-variadic-macros)
    endif()
endif()

add_subdirectory(common)
add_subdirectory(schema)

enable_testing()

add_subdirectory(tests)

if (${ENABLE_CLIENT})
    add_subdirectory(client)
endif()

if (${ENABLE_TESTS})
	if (${ENABLE_CLIENT})
		add_subdirectory(test-common)
    	add_subdirectory(client-test)
	endif()
endif()
