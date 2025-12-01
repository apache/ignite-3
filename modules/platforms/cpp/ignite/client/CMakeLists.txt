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
)

add_library(${TARGET}-obj OBJECT ${SOURCES})
target_include_directories(${TARGET}-obj PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})

add_library(${TARGET} SHARED ${SOURCES})

add_library(${TARGET}-static STATIC ${SOURCES})

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

    if (WIN32)
        set_target_properties(${_target_lib} PROPERTIES OUTPUT_NAME ${PROJECT_NAME})
    endif()

    target_link_libraries(${_target_lib} ${LIBRARIES})
endforeach()
unset(_target_libs)


if (${INSTALL_IGNITE_FILES})
    install(TARGETS ${TARGET}
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
    )

    ignite_install_headers(FILES ${PUBLIC_HEADERS} DESTINATION ${IGNITE_INCLUDEDIR}/client)
endif()

ignite_test(utils_test DISCOVER SOURCES detail/utils_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(name_utils_test DISCOVER SOURCES detail/table/name_utils_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(qualified_name_test DISCOVER SOURCES table/qualified_name_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
