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

project(ignite-odbc-test)

set(TARGET ${PROJECT_NAME})

find_package(ODBC REQUIRED)

set(SOURCES
    api_robustness_test.cpp
    auth_test.cpp
    connection_test.cpp
    error_test.cpp
    main.cpp
    many_cursors_test.cpp
    meta_queries_test.cpp
    queries_test.cpp
    timeout_test.cpp
    transaction_test.cpp
)

ignite_test(${TARGET} DISCOVER FALSE SOURCES ${SOURCES} LIBS ignite-test-common ignite-client ${ODBC_LIBRARY})

if (WIN32)
    remove_definitions(-DUNICODE=1)
endif()
