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

set(TARGET ignite-tuple)

set(SOURCES
    binary_tuple_builder.cpp
    binary_tuple_parser.cpp
)

set(PUBLIC_HEADERS
    binary_tuple_builder.h
    binary_tuple_common.h
    binary_tuple_parser.h
)

set(PRIVATE_HEADERS)

ignite_check_headers(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PUBLIC ${PUBLIC_HEADERS} PRIVATE ${PRIVATE_HEADERS})

ignite_collect_public_headers(tuple PUBLIC_HEADERS)

add_library(${TARGET} STATIC ${SOURCES})

target_link_libraries(${TARGET} ignite-common)

ignite_test(tuple_test DISCOVER SOURCES tuple_test.cpp LIBS ${TARGET})

ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/tuple COMPONENT client)
