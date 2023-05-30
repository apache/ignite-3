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

project(ignite-odbc)

set(TARGET ${PROJECT_NAME})

find_package(ODBC REQUIRED)

set(SOURCES
    app/application_data_buffer.cpp
    app/parameter.cpp
    app/parameter_set.cpp
    common_types.cpp
    config/config_tools.cpp
    config/configuration.cpp
    config/connection_info.cpp
    config/connection_string_parser.cpp
    diagnostic/diagnosable_adapter.cpp
    diagnostic/diagnostic_record.cpp
    diagnostic/diagnostic_record_storage.cpp
    meta/column_meta.cpp
    meta/table_meta.cpp
    odbc.cpp
    entry_points.cpp
    ssl_mode.cpp
    sql_connection.cpp
    sql_environment.cpp
    sql_statement.cpp
    protocol_version.cpp
    type_traits.cpp
    utility.cpp
    log.cpp
)

if (WIN32)
    list(APPEND SOURCES
        module.def
    )
endif ()

add_library(${TARGET} SHARED ${SOURCES})
target_link_libraries(${TARGET} ignite-common ignite-tuple ignite-network ignite-protocol ${ODBC_LIBRARIES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)

if (WIN32)
    target_link_libraries(${TARGET} odbccp32 shlwapi)

    remove_definitions(-DUNICODE=1)

    if (MSVC_VERSION GREATER_EQUAL 1900)
        target_link_libraries(${TARGET} legacy_stdio_definitions)
    endif()

    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite-odbc")
else()
    target_link_libraries(${TARGET} odbcinst)
endif()

target_include_directories(${TARGET} SYSTEM INTERFACE ${ODBC_INCLUDE_DIRS})

if (${INSTALL_IGNITE_FILES})
    install(TARGETS ${TARGET}
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib
    )
endif()
