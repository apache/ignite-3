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

project(_pyignite3_extension)

set(TARGET ${PROJECT_NAME})

find_package(ODBC REQUIRED)

set(SOURCES
    module.cpp
    py_connection.cpp
    py_cursor.cpp
)

set(LIBRARIES
    ignite-common
    ignite-tuple
    ignite-network
    ignite-protocol
    ignite3-odbc-obj
)

add_library(${TARGET} SHARED ${SOURCES})

target_include_directories(${TARGET} PUBLIC
    ${Python3_INCLUDE_DIRS}
    ${ODBC_INCLUDE_DIRS}
)

if(NOT DEFINED EXTENSION_FILENAME)
    set(EXTENSION_FILENAME ${TARGET})
endif()

target_link_libraries(${TARGET} ${LIBRARIES})
set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME ${EXTENSION_FILENAME})

if (WIN32)
    add_compile_definitions(NOMINMAX)
    set_target_properties(${TARGET} PROPERTIES SUFFIX ".pyd")
    target_link_libraries(${TARGET} ${Python3_LIBRARIES})
    remove_definitions(-DUNICODE=1)
endif()
