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
else()
    set(CPH_DIR "${CMAKE_BINARY_DIR}/compile-public-headers")

    # Write the list of public headers to a cmake file that the
    # sub-project will include. This avoids command-line quoting
    # issues when passing a list with semicolons.
    set(CPH_LIST_FILE "${CPH_DIR}/headers_list.cmake")
    set(CPH_LIST_CONTENT "set(IGNITE_PUBLIC_HEADERS\n")
    foreach(H IN LISTS IGNITE3_CLIENT_PUBLIC_HEADERS)
        string(APPEND CPH_LIST_CONTENT "    \"${H}\"\n")
    endforeach()
    string(APPEND CPH_LIST_CONTENT ")\n")
    file(MAKE_DIRECTORY "${CPH_DIR}")
    file(WRITE "${CPH_LIST_FILE}" "${CPH_LIST_CONTENT}")

    set(CPH_INSTALL_PREFIX "${CPH_DIR}/install")
    set(CPH_SUB_SRC "${CMAKE_SOURCE_DIR}/tests/package-test/compile_public_headers")
    set(CPH_SUB_BIN "${CPH_DIR}/build")
    set(CPH_STAMP   "${CPH_DIR}/compile-public-headers.stamp")

    # Forward the generator (and platform on Windows) so the sub-project uses the
    # same toolchain as the parent.  Without -A x64, Visual Studio generators
    # default to 32-bit and cannot locate the installed 64-bit ignite package.
    set(CPH_GENERATOR_ARGS -G "${CMAKE_GENERATOR}")
    if (CMAKE_GENERATOR_PLATFORM)
        list(APPEND CPH_GENERATOR_ARGS -A "${CMAKE_GENERATOR_PLATFORM}")
    endif()

    # Use add_custom_command so the check is skipped when ignite3-client has
    # not been rebuilt since the last successful run (stamp file is up-to-date).
    add_custom_command(
        OUTPUT  "${CPH_STAMP}"
        # Install the already-built client to a temp prefix.
        # Only files declared with COMPONENT client are installed -
        # internal headers not in PUBLIC_HEADERS are absent.
        COMMAND ${CMAKE_COMMAND} --install "${CMAKE_BINARY_DIR}"
                    --prefix "${CPH_INSTALL_PREFIX}"
                    --component client
        # Configure sub-project against the installed package.
        # INTERFACE_INCLUDE_DIRECTORIES resolves to <prefix>/include
        # (INSTALL_INTERFACE), so the compiler cannot reach internal headers.
        COMMAND ${CMAKE_COMMAND}
                    ${CPH_GENERATOR_ARGS}
                    "-DCMAKE_PREFIX_PATH=${CPH_INSTALL_PREFIX}"
                    "-DIGNITE_HEADERS_LIST_FILE=${CPH_LIST_FILE}"
                    "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
                    "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
                    "-S${CPH_SUB_SRC}"
                    "-B${CPH_SUB_BIN}"
        COMMAND ${CMAKE_COMMAND} --build "${CPH_SUB_BIN}"
        COMMAND ${CMAKE_COMMAND} -E touch "${CPH_STAMP}"
        DEPENDS ignite3-client
        COMMENT "compile-public-headers: compiling each public header against installed package"
        VERBATIM
    )

    add_custom_target(compile-public-headers ALL DEPENDS "${CPH_STAMP}")
endif()
