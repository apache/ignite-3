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

project(_pyignite3)

set(TARGET ${PROJECT_NAME})

find_package(ODBC REQUIRED)

set(SOURCES
    module.cpp
)

set(LIBRARIES
    ignite3-odbc-obj
    ${Python3_LIBRARIES}
)

add_library(${TARGET} SHARED ${SOURCES})

target_include_directories(${TARGET} PUBLIC
    ${Python3_INCLUDE_DIRS}
    ${ODBC_INCLUDE_DIRS}
)

target_link_libraries(${TARGET} ${LIBRARIES})

if (WIN32)
    set_target_properties(${TARGET} PROPERTIES SUFFIX ".pyd")
    target_link_libraries(${TARGET} ${PYTHON_LIBRARIES})
    remove_definitions(-DUNICODE=1)
endif()
