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

if (WIN32)
    set(Boost_USE_STATIC_LIBS ON)
endif()

find_package(Boost REQUIRED COMPONENTS unit_test_framework)

include_directories(${Boost_INCLUDE_DIRS})

# This function makes executable from given source file with Boost
# test cases and registers them as individual CTest test cases.
function(add_boost_test SOURCE_FILE DEPENDENCIES)
    get_filename_component(EXECUTABLE_NAME ${SOURCE_FILE} NAME_WE)

    string(REGEX REPLACE "^test" "" TEST_MODULE ${EXECUTABLE_NAME})

    add_executable(${EXECUTABLE_NAME} ${SOURCE_FILE})

    target_compile_definitions(${EXECUTABLE_NAME} PUBLIC -DBOOST_TEST_MODULE=${TEST_MODULE})

    if (NOT WIN32)
        add_definitions(-DBOOST_TEST_DYN_LINK)
        target_link_libraries(${EXECUTABLE_NAME} -rdynamic)
    endif()

    target_link_libraries(${EXECUTABLE_NAME} Boost::unit_test_framework ${DEPENDENCIES})

    target_link_libraries(${EXECUTABLE_NAME} $<$<AND:$<CXX_COMPILER_ID:GNU>,$<VERSION_LESS:$<CXX_COMPILER_VERSION>,8.0>>:stdc++fs>)

    file(READ ${SOURCE_FILE} SOURCE)

    string(REGEX MATCHALL "BOOST_(AUTO|FIXTURE)_TEST_CASE\\(([A-Za-z_0-9_,_ ]+)\\)" FOUND_TESTS ${SOURCE})

    foreach(TEST ${FOUND_TESTS})
        string(REGEX REPLACE ".*\\( *([A-Za-z_0-9]+).*" "\\1" TEST_NAME ${TEST})

        add_test(NAME ${EXECUTABLE_NAME}.${TEST_NAME} COMMAND ${EXECUTABLE_NAME} -t ${TEST_NAME}
                 WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    endforeach()
endfunction()

add_boost_test(testBits.cpp ignite-common)
add_boost_test(testBytes.cpp ignite-common)
add_boost_test(testUuid.cpp ignite-common)

add_boost_test(testDecimal.cpp ignite-schema)
