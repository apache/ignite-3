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

project(ignite-test-common)

set(TARGET ${PROJECT_NAME})

set(SOURCES
    basic_auth_test_suite.h
    cmd_process.h
    ignite_runner.cpp ignite_runner.h
    process.cpp
    test_utils.cpp test_utils.h
    hidden_param.h
)

add_library(${TARGET} OBJECT ${SOURCES})
target_link_libraries(${TARGET} ignite-common)
target_include_directories(${TARGET} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)
