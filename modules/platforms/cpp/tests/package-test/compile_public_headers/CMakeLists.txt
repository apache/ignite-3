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

# Ignite header compilation test
#
# For every public header of ignite3-client, compiles a minimal .cpp that
# includes ONLY that header (via the installed/exported ignite::client target).
# This verifies that each header includes all its own dependencies and can be
# used by consumers without relying on a particular include order.
#
# Invoked by the parent build's 'compile-public-headers' target:
#   cmake -S <this_dir> -B <build_dir>
#       -Dignite_DIR=<ignite_build>/cmake
#       -DIGNITE_HEADERS_LIST_FILE=<path/to/headers_list.cmake>
#       -DCMAKE_CXX_COMPILER=<compiler>
#       [-DCMAKE_BUILD_TYPE=<type>]
#   cmake --build <build_dir>

cmake_minimum_required(VERSION 3.18)

project(ignite_compile_public_headers CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

if(NOT IGNITE_HEADERS_LIST_FILE OR NOT EXISTS "${IGNITE_HEADERS_LIST_FILE}")
    message(FATAL_ERROR
        "IGNITE_HEADERS_LIST_FILE must point to the generated headers list cmake file. "
        "Got: '${IGNITE_HEADERS_LIST_FILE}'")
endif()

find_package(ignite REQUIRED COMPONENTS client)

# Load the list of public headers (variable: IGNITE_PUBLIC_HEADERS).
include("${IGNITE_HEADERS_LIST_FILE}")

set(CPH_INDEX 0)
foreach(H IN LISTS IGNITE_PUBLIC_HEADERS)
    # Use a sequence number as the target/file name to keep paths short.
    # A full MAKE_C_IDENTIFIER name can exceed 260-char Windows path limits
    # when combined with MSBuild's .tlog directory structure (VS 2017 does not
    # honour the LongPathsEnabled registry key).
    set(CPH_CPP "${CMAKE_CURRENT_BINARY_DIR}/cph_${CPH_INDEX}.cpp")
    # The comment makes the generated file identifiable when inspecting build
    # artefacts or compiler error messages.
    file(WRITE "${CPH_CPP}" "// Header under test: ${H}\n#include <${H}>\n")

    # Compile the generated file as an object (no link step needed).
    add_library(cph_${CPH_INDEX} OBJECT "${CPH_CPP}")
    target_link_libraries(cph_${CPH_INDEX} PRIVATE ignite::client)

    math(EXPR CPH_INDEX "${CPH_INDEX} + 1")
endforeach()
