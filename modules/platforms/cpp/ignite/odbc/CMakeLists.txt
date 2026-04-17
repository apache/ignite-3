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

set(TARGET ${PROJECT_NAME}-odbc)

set(PUBLIC_HEADERS)

set(PRIVATE_HEADERS
    app/application_data_buffer.h
    app/parameter.h
    app/parameter_set.h
    common_types.h
    config/config_tools.h
    config/configuration.h
    config/connection_info.h
    config/value_with_default.h
    diagnostic/diagnosable.h
    diagnostic/diagnosable_adapter.h
    diagnostic/diagnostic_record.h
    diagnostic/diagnostic_record_storage.h
    log.h
    meta/primary_key_meta.h
    meta/table_meta.h
    odbc.h
    odbc_error.h
    query/column_metadata_query.h
    query/cursor.h
    query/data_query.h
    query/foreign_keys_query.h
    query/primary_keys_query.h
    query/query.h
    query/result_page.h
    query/special_columns_query.h
    query/table_metadata_query.h
    query/type_info_query.h
    sql_connection.h
    sql_environment.h
    sql_statement.h
    ssl_mode.h
    system/odbc_constants.h
    type_traits.h
    utility.h
)

ignite_check_headers(SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR} PUBLIC ${PUBLIC_HEADERS} PRIVATE ${PRIVATE_HEADERS})

ignite_collect_public_headers(odbc PUBLIC_HEADERS)

include(find_odbc)
find_package(ODBC REQUIRED)

set(SOURCES
    app/application_data_buffer.cpp
    app/parameter.cpp
    app/parameter_set.cpp
    common_types.cpp
    config/config_tools.cpp
    config/configuration.cpp
    config/connection_info.cpp
    diagnostic/diagnosable_adapter.cpp
    diagnostic/diagnostic_record.cpp
    diagnostic/diagnostic_record_storage.cpp
    meta/table_meta.cpp
    query/column_metadata_query.cpp
    query/data_query.cpp
    query/foreign_keys_query.cpp
    query/primary_keys_query.cpp
    query/special_columns_query.cpp
    query/table_metadata_query.cpp
    query/type_info_query.cpp
    odbc.cpp
    entry_points.cpp
    ssl_mode.cpp
    sql_connection.cpp
    sql_environment.cpp
    sql_statement.cpp
    type_traits.cpp
    utility.cpp
    log.cpp
)

set(EXTRA_FILES)

if (WIN32)
    string(REPLACE "." "," CMAKE_PROJECT_VERSION_COMMAS ${CMAKE_PROJECT_VERSION})
    configure_file(${IGNITE_CMAKE_TOP_DIR}/ignite/odbc/version.rc.in ${CMAKE_CURRENT_BINARY_DIR}/version.rc @ONLY)
    set(EXTRA_FILES ${CMAKE_CURRENT_BINARY_DIR}/version.rc)
endif()

add_compile_definitions(CMAKE_PROJECT_VERSION="${CMAKE_PROJECT_VERSION}")

add_library(${TARGET}-obj OBJECT ${SOURCES})
target_include_directories(${TARGET}-obj PUBLIC ${IGNITE_CMAKE_TOP_DIR} ${ODBC_INCLUDE_DIRS})

add_library(${TARGET} SHARED $<TARGET_OBJECTS:${TARGET}-obj> module.def ${EXTRA_FILES})
set_target_properties(${TARGET} PROPERTIES EXPORT_NAME odbc)

set(LIBRARIES
    ignite-common
    ignite-tuple
    ignite-network
    ignite-protocol
    ${ODBC_LIBRARIES}
)

set(_target_libs ${TARGET} ${TARGET}-obj)

foreach(_target_lib IN LISTS _target_libs)
    set_target_properties(${_target_lib} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
    set_target_properties(${_target_lib} PROPERTIES POSITION_INDEPENDENT_CODE 1)

    if (WIN32)
        list(APPEND LIBRARIES odbccp32 shlwapi)

        remove_definitions(-DUNICODE=1)

        if (MSVC_VERSION GREATER_EQUAL 1900)
            list(APPEND LIBRARIES legacy_stdio_definitions)
        endif()
    elseif(NOT APPLE)
        list(APPEND LIBRARIES odbcinst)
    endif()

    target_include_directories(${_target_lib} SYSTEM INTERFACE ${ODBC_INCLUDE_DIRS})
    target_include_directories(${_target_lib}
        INTERFACE $<BUILD_INTERFACE:${IGNITE_CMAKE_TOP_DIR}/>
                  $<INSTALL_INTERFACE:${IGNITE_INSTALL_INCLUDE_DIR}/>
    )
    target_link_libraries(${_target_lib} PRIVATE ${LIBRARIES})
endforeach()
unset(_target_libs)

add_library(ignite::odbc ALIAS ${TARGET})

install(TARGETS ${TARGET}
    EXPORT ignite-odbc-targets
    COMPONENT odbc
    ARCHIVE DESTINATION ${IGNITE_INSTALL_ARCHIVE_DIR}
    LIBRARY DESTINATION ${IGNITE_INSTALL_LIBRARY_DIR}
    RUNTIME DESTINATION ${IGNITE_INSTALL_RUNTIME_DIR}
    INCLUDES DESTINATION ${IGNITE_INSTALL_INCLUDE_DIR}
)

export(
    EXPORT ignite-odbc-targets
    NAMESPACE ignite::
    FILE "cmake/ignite-odbc-targets.cmake"
)

install(
    EXPORT ignite-odbc-targets
    NAMESPACE ignite::
    DESTINATION "${IGNITE_INSTALL_LIBRARY_DIR}/cmake/ignite"
    COMPONENT odbc-local
    FILE "ignite-odbc-targets.cmake"
)

ignite_test(connection_info_test DISCOVER SOURCES config/connection_info_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(config_tools_test DISCOVER SOURCES config/config_tools_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(application_data_buffer_test DISCOVER SOURCES app/application_data_buffer_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
