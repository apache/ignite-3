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

# ignite_test(<test-name> [DISCOVER] SOURCES <test-src>... [LIBS <lib>...])
#
# Function to add a unit test.
function(ignite_test TEST_NAME)
    if (NOT ${ENABLE_TESTS})
        return()
    endif()

    set(OPTIONAL_ARGUMENT_TAGS DISCOVER)
    set(SINGLE_ARGUMENT_TAGS)
    set(MULTI_ARGUMENT_TAGS LIBS SOURCES)

    cmake_parse_arguments(IGNITE_TEST
            "${OPTIONAL_ARGUMENT_TAGS}"
            "${SINGLE_ARGUMENT_TAGS}"
            "${MULTI_ARGUMENT_TAGS}"
            ${ARGN})

    add_executable(${TEST_NAME} ${IGNITE_TEST_SOURCES})

    target_link_libraries(${TEST_NAME} ${IGNITE_TEST_LIBS} GTest::gtest_main GTest::gmock_main)

    if(${IGNITE_TEST_DISCOVER})
        gtest_discover_tests(${TEST_NAME} XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/Testing/Result)
    endif()
endfunction()
