#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
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

#include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${JNI_INCLUDE_DIRS})
include_directories(include)

set(SOURCES
	src/test_utils.cpp
)

add_library(${TARGET} OBJECT ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)
target_include_directories(${TARGET} INTERFACE include)
