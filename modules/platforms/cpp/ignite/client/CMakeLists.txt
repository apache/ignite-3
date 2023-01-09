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

project(ignite-client)

set(TARGET ${PROJECT_NAME})

set(SOURCES
    ignite_client.cpp
    sql/sql.cpp
    sql/result_set.cpp
    table/record_view.cpp
    table/table.cpp
    table/tables.cpp
    detail/cluster_connection.cpp
    detail/utils.cpp
    detail/node_connection.cpp
    detail/sql/sql_impl.cpp
    detail/table/table_impl.cpp
    detail/table/tables_impl.cpp
)

set(PUBLIC_HEADERS
    ignite_client.h
    ignite_client_configuration.h
    ignite_logger.h
    sql/sql.h
    table/ignite_tuple.h
    table/record_view.h
    table/table.h
    table/tables.h
    transaction/transaction.h
    transaction/transactions.h
)

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

if (WIN32)
    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite-client")
endif()

target_link_libraries(${TARGET} ignite-common ignite-schema ignite-network ignite-protocol)

install(TARGETS ${TARGET}
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/client)
