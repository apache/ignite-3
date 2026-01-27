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

set(CPACK_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION ON)

set(IGNITE_CMAKE_TOP_DIR "${CMAKE_CURRENT_LIST_DIR}/..")

set(CPACK_COMPONENTS_ALL client odbc)

set(CPACK_PACKAGE_NAME ignite3 CACHE STRING "The resulting package name")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Apache Ignite 3 client library for C++"
        CACHE STRING "Package description for the package metadata"
)
set(CPACK_PACKAGE_VENDOR "Apache Software Foundation")

set(CPACK_VERBATIM_VARIABLES YES)

set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
SET(CPACK_OUTPUT_FILE_PREFIX "_packages")

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

set(CPACK_PACKAGE_CONTACT "Apache Software Foundation")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Apache Software Foundation")

set(CPACK_RESOURCE_FILE_LICENSE "${IGNITE_CMAKE_TOP_DIR}/LICENSE")

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)

set(CPACK_ARCHIVE_COMPONENT_INSTALL 1)
set(CPACK_TGZ_COMPONENT_INSTALL ON)

set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${IGNITE_CMAKE_TOP_DIR}/cmake/scripts/odbc/post_install.sh.in)
set_property(DIRECTORY APPEND PROPERTY CMAKE_CONFIGURE_DEPENDS ${IGNITE_CMAKE_TOP_DIR}/cmake/scripts/odbc/pre_uninstall.sh.in)

get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()

set(ODBC_SCRIPT_DIR "${IGNITE_CMAKE_TOP_DIR}/cmake/scripts/odbc")

configure_file("${ODBC_SCRIPT_DIR}/post_install.sh"  "${CMAKE_CURRENT_BINARY_DIR}/postinst" COPYONLY)
configure_file("${ODBC_SCRIPT_DIR}/pre_uninstall.sh" "${CMAKE_CURRENT_BINARY_DIR}/prerm" COPYONLY)
set(CPACK_DEBIAN_ODBC_PACKAGE_CONTROL_EXTRA "${CMAKE_CURRENT_BINARY_DIR}/postinst;${CMAKE_CURRENT_BINARY_DIR}/prerm")
set(CPACK_DEBIAN_ODBC_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)
set(CPACK_DEBIAN_ODBC_PACKAGE_DEPENDS unixodbc)

configure_file("${ODBC_SCRIPT_DIR}/post_install.sh"  "${CMAKE_CURRENT_BINARY_DIR}/post_install.sh" COPYONLY)
configure_file("${ODBC_SCRIPT_DIR}/pre_uninstall.sh" "${CMAKE_CURRENT_BINARY_DIR}/pre_uninstall.sh" COPYONLY)
set(CPACK_RPM_ODBC_POST_INSTALL_SCRIPT_FILE  "${CMAKE_CURRENT_BINARY_DIR}/post_install.sh")
set(CPACK_RPM_ODBC_PRE_UNINSTALL_SCRIPT_FILE "${CMAKE_CURRENT_BINARY_DIR}/pre_uninstall.sh")
set(CPACK_RPM_ODBC_PACKAGE_DEPENDS unixodbc)

install(FILES
    "${ODBC_SCRIPT_DIR}/ignite3-odbc-linux.ini"
    COMPONENT odbc
    DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/ignite/"
    RENAME "ignite3-odbc.ini"
)

set(CPACK_COMPONENT_CLIENT_DESCRIPTION "Apache Ignite 3 client library for C++")
set(CPACK_COMPONENT_ODBC_DESCRIPTION "Apache Ignite ODBC driver")
set(CPACK_PACKAGE_CHECKSUM "SHA256")