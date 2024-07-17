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

project(ignite-tuple)

set(TARGET ${PROJECT_NAME})

set(SOURCES
    binary_tuple_builder.cpp
    binary_tuple_parser.cpp
)

set(PUBLIC_HEADERS
    binary_tuple_builder.h
    binary_tuple_common.h
    binary_tuple_parser.h
)

add_library(${TARGET} STATIC ${SOURCES})

target_link_libraries(${TARGET} ignite-common)

ignite_test(tuple_test DISCOVER SOURCES tuple_test.cpp LIBS ${TARGET})

if (${INSTALL_IGNITE_FILES})
    install(TARGETS ${TARGET}
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
    )

    ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/tuple)
endif()
