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

project(ignite3-odbc)

set(TARGET ${PROJECT_NAME})

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
    configure_file(${IGNITE_CMAKE_TOP_DIR}/version.rc.in ${CMAKE_CURRENT_BINARY_DIR}/version.rc @ONLY)
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
    target_link_libraries(${_target_lib} PRIVATE ${LIBRARIES})
endforeach()
unset(_target_libs)

add_library(ignite::odbc ALIAS ${TARGET})

if (${INSTALL_IGNITE_FILES})
    install(TARGETS ${TARGET}
            EXPORT ignite-odbc-targets
            RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
            LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
            INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )

    export(EXPORT ignite-odbc-targets
            NAMESPACE ignite::
            FILE "${CMAKE_BINARY_DIR}/cmake/ignite-odbc-targets.cmake"
    )

    install(EXPORT ignite-odbc-targets
            NAMESPACE ignite::
            DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/ignite"
            FILE "ignite-odbc-targets.cmake"
    )
endif()

ignite_test(connection_info_test DISCOVER SOURCES config/connection_info_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(config_tools_test DISCOVER SOURCES config/config_tools_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
ignite_test(application_data_buffer_test DISCOVER SOURCES app/application_data_buffer_test.cpp LIBS ${TARGET}-obj ${LIBRARIES})
