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

# ignite_test(<test-name> <test-src> [LIBS <lib>...])
#
# Function to add a unit test.
function(ignite_test TEST_NAME TEST_SOURCE)
    if (NOT ${ENABLE_TESTS})
        return()
    endif()

    cmake_parse_arguments(IGNITE_TEST "" "" "LIBS" ${ARGN})

    add_executable(${TEST_NAME} ${TEST_SOURCE})

    # Older versions of CMake provide the GTest::Main target while newer versions
    # provide the GTest::gtest_main target. The old target is deprecated but still
    # available for now. However Conan-generated build recipe takes over original
    # CMake behavior and provides the new target only. This mess is handled here.
    # So we can build with and without Conan, with newer and with older CMake.
    if (TARGET GTest::gtest_main)
        target_link_libraries(${TEST_NAME} ${IGNITE_TEST_LIBS} GTest::gtest GTest::gtest_main)
    else()
        target_link_libraries(${TEST_NAME} ${IGNITE_TEST_LIBS} GTest::GTest GTest::Main)
    endif()

    gtest_discover_tests(${TEST_NAME} XML_OUTPUT_DIR ${CMAKE_BINARY_DIR}/Testing/Result)
endfunction()
