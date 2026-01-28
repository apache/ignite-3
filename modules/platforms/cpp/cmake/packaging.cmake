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

set(CPACK_PACKAGE_NAME ignite3)

if(ENABLE_ADDRESS_SANITIZER)
    set(CPACK_PACKAGE_NAME ${CPACK_PACKAGE_NAME}-asan)
endif()

if(ENABLE_UB_SANITIZER)
    set(CPACK_PACKAGE_NAME ${CPACK_PACKAGE_NAME}-ubsan)
endif()

set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")
set(PACKAGE_SO_PATH "${CPACK_PACKAGING_INSTALL_PREFIX}/${CMAKE_INSTALL_LIBDIR}")
message(STATUS "[CPACK_PACKAGING_INSTALL_PREFIX] ${CPACK_PACKAGING_INSTALL_PREFIX}")
message(STATUS "[PACKAGE_SO_PATH] ${PACKAGE_SO_PATH}")

set(CPACK_ERROR_ON_ABSOLUTE_INSTALL_DESTINATION ON)

set(CPACK_COMPONENTS_ALL client odbc)

set(CPACK_PACKAGE_VENDOR "Apache Software Foundation")

set(CPACK_VERBATIM_VARIABLES YES)

set(CPACK_PACKAGE_INSTALL_DIRECTORY ${CPACK_PACKAGE_NAME})
set(CPACK_OUTPUT_FILE_PREFIX "_packages")

set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})

set(CPACK_PACKAGE_CONTACT "dev@ignite.apache.org")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "Apache Software Foundation")

set(CPACK_RESOURCE_FILE_LICENSE "${IGNITE_CMAKE_TOP_DIR}/LICENSE")

set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_COMPONENTS_GROUPING ONE_PER_GROUP)
set(CPACK_DEB_COMPONENT_INSTALL ON)
set(CPACK_RPM_COMPONENT_INSTALL ON)

set(CPACK_ARCHIVE_COMPONENT_INSTALL 1)
set(CPACK_TGZ_COMPONENT_INSTALL ON)

set(ODBC_SCRIPT_DIR "${IGNITE_CMAKE_TOP_DIR}/cmake/scripts/odbc")

#configure_file can set permissions as well, but it will raise required cmake version to 3.20
configure_file("${ODBC_SCRIPT_DIR}/post_install.sh"  "${CMAKE_CURRENT_BINARY_DIR}/postinst" @ONLY)
configure_file("${ODBC_SCRIPT_DIR}/pre_uninstall.sh" "${CMAKE_CURRENT_BINARY_DIR}/prerm" @ONLY)

configure_file("${ODBC_SCRIPT_DIR}/post_install.sh"  "${CMAKE_CURRENT_BINARY_DIR}/post_install.sh" @ONLY)
configure_file("${ODBC_SCRIPT_DIR}/pre_uninstall.sh" "${CMAKE_CURRENT_BINARY_DIR}/pre_uninstall.sh" @ONLY)

set(SCRIPTS_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/scripts/")

file(MAKE_DIRECTORY ${SCRIPTS_BINARY_DIR})

file(COPY
    "${CMAKE_CURRENT_BINARY_DIR}/post_install.sh"
    "${CMAKE_CURRENT_BINARY_DIR}/pre_uninstall.sh"
    "${CMAKE_CURRENT_BINARY_DIR}/postinst"
    "${CMAKE_CURRENT_BINARY_DIR}/prerm"
    DESTINATION "${SCRIPTS_BINARY_DIR}"
    FILE_PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE
    GROUP_READ GROUP_EXECUTE
    WORLD_READ WORLD_EXECUTE
)

set(CPACK_DEBIAN_ODBC_PACKAGE_CONTROL_EXTRA "${SCRIPTS_BINARY_DIR}/postinst;${SCRIPTS_BINARY_DIR}/prerm")
set(CPACK_DEBIAN_ODBC_PACKAGE_CONTROL_STRICT_PERMISSION TRUE)

set(CPACK_DEBIAN_CLIENT_PACKAGE_DEPENDS unixodbc libc6 libstdc++6)
set(CPACK_DEBIAN_ODBC_PACKAGE_DEPENDS unixodbc libc6 libstdc++6)

set(CPACK_RPM_ODBC_POST_INSTALL_SCRIPT_FILE  "${SCRIPTS_BINARY_DIR}/post_install.sh")
set(CPACK_RPM_ODBC_PRE_UNINSTALL_SCRIPT_FILE "${SCRIPTS_BINARY_DIR}/pre_uninstall.sh")

set(CPACK_RPM_CLIENT_PACKAGE_DEPENDS unixodbc libc6 libstdc++6)
set(CPACK_RPM_ODBC_PACKAGE_DEPENDS unixodbc libc6 libstdc++6)

configure_file("${ODBC_SCRIPT_DIR}/ignite3-odbc-linux.ini.in" "${CMAKE_CURRENT_BINARY_DIR}/ignite3-odbc-linux.ini")

install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/ignite3-odbc-linux.ini"
    COMPONENT odbc
    DESTINATION "${CMAKE_INSTALL_DATAROOTDIR}/ignite/"
    RENAME "ignite3-odbc.ini"
)

set(CPACK_COMPONENT_CLIENT_DESCRIPTION "Apache Ignite 3 client library for C++")
set(CPACK_COMPONENT_ODBC_DESCRIPTION "Apache Ignite 3 ODBC driver")
set(CPACK_PACKAGE_CHECKSUM "SHA256")