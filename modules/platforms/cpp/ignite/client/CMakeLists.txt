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

project(ignite3-client)

set(TARGET ${PROJECT_NAME})

set(SOURCES
    cancel_handle.cpp
    ignite_client.cpp
    ignite_client_configuration.cpp
    compute/broadcast_job_target.cpp
    compute/compute.cpp
    compute/job_execution.cpp
    compute/job_target.cpp
    sql/sql.cpp
    sql/result_set.cpp
    table/key_value_view.cpp
    table/qualified_name.cpp
    table/record_view.cpp
    table/table.cpp
    table/tables.cpp
    transaction/transaction.cpp
    transaction/transactions.cpp
    detail/cancellation_token_impl.cpp
    detail/cluster_connection.cpp
    detail/ignite_client_impl.cpp
    detail/utils.cpp
    detail/node_connection.cpp
    detail/compute/compute_impl.cpp
    detail/compute/job_execution_impl.cpp
    detail/sql/sql_impl.cpp
    detail/table/name_utils.cpp
    detail/table/table_impl.cpp
    detail/table/tables_impl.cpp
)

set(PUBLIC_HEADERS
    basic_authenticator.h
    cancel_handle.h
    cancellation_token.h
    ignite_client.h
    ignite_client_authenticator.h
    ignite_client_configuration.h
    ignite_logger.h
    ssl_mode.h
    type_mapping.h
    compute/broadcast_execution.h
    compute/broadcast_job_target.h
    compute/compute.h
    compute/deployment_unit.h
    compute/job_descriptor.h
    compute/job_execution.h
    compute/job_execution_options.h
    compute/job_state.h
    compute/job_status.h
    compute/job_target.h
    detail/compute/job_target_type.h
    detail/type_mapping_utils.h
    network/cluster_node.h
    sql/column_metadata.h
    sql/column_origin.h
    sql/result_set.h
    sql/result_set_metadata.h
    sql/sql.h
    sql/sql_statement.h
    table/ignite_tuple.h
    table/key_value_view.h
    table/qualified_name.h
    table/record_view.h
    table/table.h
    table/tables.h
    transaction/transaction.h
    transaction/transactions.h
    transaction/transaction_options.h
)

add_library(${TARGET}-obj OBJECT ${SOURCES})
target_include_directories(${TARGET}-obj PUBLIC ${IGNITE_CMAKE_TOP_DIR})

add_library(${TARGET} SHARED ${SOURCES})
set_target_properties(${TARGET} PROPERTIES EXPORT_NAME client)

add_library(${TARGET}-static STATIC ${SOURCES})
set_target_properties(${TARGET}-static PROPERTIES EXPORT_NAME client-static)

set(LIBRARIES
    ignite-common
    ignite-tuple
    ignite-network
    ignite-protocol
    uni-algo::uni-algo
)

set(_target_libs ${TARGET} ${TARGET}-obj ${TARGET}-static)

foreach(_target_lib IN LISTS _target_libs)
    set_target_properties(${_target_lib} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
    set_target_properties(${_target_lib} PROPERTIES POSITION_INDEPENDENT_CODE 1)

    target_link_libraries(${_target_lib} PRIVATE ${LIBRARIES})
    target_include_directories(${_target_lib}
        PUBLIC $<BUILD_INTERFACE:${IGNITE_CMAKE_TOP_DIR}/>
               $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}/>
    )
endforeach()
unset(_target_libs)

add_library(ignite::client ALIAS ${TARGET})
add_library(ignite::client-static ALIAS ${TARGET}-static)

install(TARGETS ${TARGET}
    EXPORT ignite-client-targets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

export(
    EXPORT ignite-client-targets
    NAMESPACE ignite::
    FILE "cmake/ignite-client-targets.cmake"
)

install(
    EXPORT ignite-client-targets
    NAMESPACE ignite::
    DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ignite"
    COMPONENT client
    FILE "ignite-client-targets.cmake"
)

ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/client)

ignite_test(utils_test DISCOVER SOURCES detail/utils_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(name_utils_test DISCOVER SOURCES detail/table/name_utils_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(qualified_name_test DISCOVER SOURCES table/qualified_name_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
