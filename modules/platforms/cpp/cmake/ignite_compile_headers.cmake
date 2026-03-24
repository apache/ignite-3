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

# Compile-time check for public headers (always enabled when ENABLE_CLIENT=ON).
#
# For every public header of ignite3-client, compiles a minimal .cpp that
# includes ONLY that header against the INSTALLED package.  This catches:
#   1. Headers missing their own #include dependencies.
#   2. Public headers that #include internal (non-installed) headers.
#   3. Headers missing from the installed package.
#
# The check installs the already-built client to a temporary prefix inside
# the build tree, then configures a sub-project under
# tests/package-test/compile_public_headers/ against that prefix.
# Because INSTALL_INTERFACE is used, the compiler only sees the installed
# include tree - internal headers absent from the package are not accessible.
#
# Target produced:
#   compile-public-headers

if (NOT ENABLE_CLIENT)
    message(STATUS "compile-public-headers: ENABLE_CLIENT=OFF, skipping.")
elseif (NOT IGNITE3_CLIENT_PUBLIC_HEADERS)
    message(WARNING "compile-public-headers: IGNITE3_CLIENT_PUBLIC_HEADERS is empty. "
                    "Check ignite/client/CMakeLists.txt.")
elseif (NOT INSTALL_IGNITE_FILES)
    message(STATUS "compile-public-headers: INSTALL_IGNITE_FILES=OFF, skipping header compile check.")
else()
    set(_hcc_dir "${CMAKE_BINARY_DIR}/hcc")

    # Write the list of public headers to a cmake file that the
    # sub-project will include. This avoids command-line quoting
    # issues when passing a list with semicolons.
    set(_hcc_list_file "${_hcc_dir}/headers_list.cmake")
    set(_hcc_list_content "set(IGNITE_PUBLIC_HEADERS\n")
    foreach(_h IN LISTS IGNITE3_CLIENT_PUBLIC_HEADERS)
        string(APPEND _hcc_list_content "    \"${_h}\"\n")
    endforeach()
    string(APPEND _hcc_list_content ")\n")
    file(MAKE_DIRECTORY "${_hcc_dir}")
    file(WRITE "${_hcc_list_file}" "${_hcc_list_content}")

    set(_hcc_install_prefix "${_hcc_dir}/install")
    set(_hcc_sub_src "${CMAKE_SOURCE_DIR}/tests/package-test/compile_public_headers")
    set(_hcc_sub_bin "${_hcc_dir}/build")

    add_custom_target(compile-public-headers ALL
        # Install the already-built client to a temp prefix.
        # Only files declared with COMPONENT client are installed -
        # internal headers not in PUBLIC_HEADERS are absent.
        COMMAND ${CMAKE_COMMAND} --install "${CMAKE_BINARY_DIR}"
                    --prefix "${_hcc_install_prefix}"
                    --component client
        # Configure sub-project against the installed package.
        # INTERFACE_INCLUDE_DIRECTORIES resolves to <prefix>/include
        # (INSTALL_INTERFACE), so the compiler cannot reach internal headers.
        COMMAND ${CMAKE_COMMAND}
                    "-DCMAKE_PREFIX_PATH=${_hcc_install_prefix}"
                    "-DIGNITE_HEADERS_LIST_FILE=${_hcc_list_file}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    "-S${_hcc_sub_src}"
                    "-B${_hcc_sub_bin}"
        COMMAND ${CMAKE_COMMAND} --build "${_hcc_sub_bin}"
        DEPENDS ignite3-client
        COMMENT "compile-public-headers: compiling each public header against installed package"
        VERBATIM
    )
endif()
