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

# ABI and source-level API compatibility checking.
#
#   ENABLE_ABI_COMPLIANCE_CHECKER       - uses abi-dumper + abi-compliance-checker
#   ENABLE_HEADER_SELF_CONTAINMENT_CHECK - compiles each public header in isolation via find_package(ignite)
#
# Baseline files live under <source_root>/abi/ and are committed to the repo.
# They must be regenerated whenever a deliberate ABI break is released.
#
# Targets produced (when the respective option is ON):
#
#   ENABLE_ABI_COMPLIANCE_CHECKER:
#     abichecker-dump            dump ABI of all enabled libs to build/abi/
#     abichecker-check           compare build/abi/ dumps against abi/ baselines
#     abichecker-update-baseline copy build/abi/ dumps back to abi/ (update baseline)
#
#   ENABLE_HEADER_SELF_CONTAINMENT_CHECK:
#     header-self-containment-check  compile each public header in isolation

set(ABI_BASELINE_DIR "${CMAKE_SOURCE_DIR}/abi")
set(ABI_BUILD_DIR    "${CMAKE_BINARY_DIR}/abi")

# Create the shared ABI output directory (used by both checkers).
add_custom_command(
    OUTPUT  "${ABI_BUILD_DIR}/.created"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ABI_BUILD_DIR}"
    COMMAND ${CMAKE_COMMAND} -E touch "${ABI_BUILD_DIR}/.created"
    COMMENT "Creating ABI output directory"
)
add_custom_target(_abi_build_dir DEPENDS "${ABI_BUILD_DIR}/.created")

# ---------------------------------------------------------------------------
# ENABLE_ABI_COMPLIANCE_CHECKER
# ---------------------------------------------------------------------------
if (ENABLE_ABI_COMPLIANCE_CHECKER)
    # Warn when debug info is likely missing.
    if (NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|RelWithDebInfo)$")
        message(WARNING "abi_check: CMAKE_BUILD_TYPE is '${CMAKE_BUILD_TYPE}'. "
                        "abi-compliance-checker requires debug symbols (-g). "
                        "Consider using -DCMAKE_BUILD_TYPE=RelWithDebInfo.")
    endif()

    find_program(ABI_DUMPER_BIN             abi-dumper             REQUIRED)
    find_program(ABI_COMPLIANCE_CHECKER_BIN abi-compliance-checker REQUIRED)

    message(STATUS "abi-compliance-checker: abi-dumper=${ABI_DUMPER_BIN}")
    message(STATUS "abi-compliance-checker: abi-compliance-checker=${ABI_COMPLIANCE_CHECKER_BIN}")

    # Collect the libraries that are actually being built.
    set(_ABI_TARGETS)
    if (ENABLE_CLIENT)
        list(APPEND _ABI_TARGETS ignite3-client)
    endif()
    if (ENABLE_ODBC)
        list(APPEND _ABI_TARGETS ignite3-odbc)
    endif()

    if (NOT _ABI_TARGETS)
        message(WARNING "abi-compliance-checker: no libraries enabled "
                        "(ENABLE_CLIENT / ENABLE_ODBC are both OFF). "
                        "ABI check targets will not be created.")
    else()
        set(_reports_dir "${ABI_BUILD_DIR}/reports")

        add_custom_command(
            OUTPUT  "${_reports_dir}/.created"
            COMMAND ${CMAKE_COMMAND} -E make_directory "${_reports_dir}"
            COMMAND ${CMAKE_COMMAND} -E touch "${_reports_dir}/.created"
            COMMENT "Creating ABI reports directory"
        )
        add_custom_target(_abichecker_reports_dir DEPENDS "${_reports_dir}/.created")

        set(_abidumper_deps _abi_build_dir)

        foreach(_lib IN LISTS _ABI_TARGETS)
            set(_out "${ABI_BUILD_DIR}/${_lib}.dump")

            add_custom_command(
                OUTPUT  "${_out}"
                COMMAND ${ABI_DUMPER_BIN}
                            $<TARGET_FILE:${_lib}>
                            -o "${_out}"
                            -lver "${PROJECT_VERSION}"
                DEPENDS ${_lib} _abi_build_dir
                COMMENT "abi-dumper: dumping ABI of ${_lib}"
                VERBATIM
            )
            list(APPEND _abidumper_deps "${_out}")
        endforeach()

        add_custom_target(abichecker-dump DEPENDS ${_abidumper_deps}
            COMMENT "abi-compliance-checker: dumps written to ${ABI_BUILD_DIR}/")

        set(_checker_deps abichecker-dump _abichecker_reports_dir)

        foreach(_lib IN LISTS _ABI_TARGETS)
            set(_baseline "${ABI_BASELINE_DIR}/${_lib}.dump")
            set(_current  "${ABI_BUILD_DIR}/${_lib}.dump")
            set(_report   "${_reports_dir}/${_lib}-compat-report.html")

            if (NOT EXISTS "${_baseline}")
                message(WARNING "abi-compliance-checker: no baseline found at ${_baseline}. "
                                "Run 'cmake --build . --target abichecker-update-baseline' first.")
            endif()

            add_custom_target(abichecker-check-${_lib}
                COMMAND ${ABI_COMPLIANCE_CHECKER_BIN}
                            -lib "${_lib}"
                            -old "${_baseline}"
                            -new "${_current}"
                            -report-path "${_report}"
                DEPENDS abichecker-dump _abichecker_reports_dir
                COMMENT "abi-compliance-checker: checking ${_lib} against baseline"
                VERBATIM
            )
            list(APPEND _checker_deps abichecker-check-${_lib})
        endforeach()

        add_custom_target(abichecker-check DEPENDS ${_checker_deps}
            COMMENT "abi-compliance-checker: reports written to ${_reports_dir}/")

        add_custom_target(abichecker-update-baseline
            COMMAND ${CMAKE_COMMAND} -E make_directory "${ABI_BASELINE_DIR}"
            COMMENT "abi-compliance-checker: updating baselines in ${ABI_BASELINE_DIR}/"
            VERBATIM
        )

        foreach(_lib IN LISTS _ABI_TARGETS)
            add_custom_target(_abichecker-update-${_lib}
                COMMAND ${CMAKE_COMMAND} -E copy
                            "${ABI_BUILD_DIR}/${_lib}.dump"
                            "${ABI_BASELINE_DIR}/${_lib}.dump"
                DEPENDS abichecker-dump
                COMMENT "abi-compliance-checker: updating baseline for ${_lib}"
                VERBATIM
            )
            add_dependencies(abichecker-update-baseline _abichecker-update-${_lib})
        endforeach()
    endif()

endif() # ENABLE_ABI_COMPLIANCE_CHECKER

# ---------------------------------------------------------------------------
# ENABLE_HEADER_SELF_CONTAINMENT_CHECK
#
# For every public header of ignite3-client, compiles a minimal .cpp that
# includes ONLY that header against the INSTALLED package.  This catches:
#   1. Headers missing their own #include dependencies.
#   2. Public headers that #include internal (non-installed) headers.
#   3. Headers missing from the installed package.
#
# The check installs the already-built client to a temporary prefix inside
# the build tree, then configures a sub-project under
# tests/package-test/header_self_containment/ against that prefix.
# Because INSTALL_INTERFACE is used, the compiler only sees the installed
# include tree — internal headers absent from the package are not accessible.
#
# Target produced:
#   header-self-containment-check
# ---------------------------------------------------------------------------
if (ENABLE_HEADER_SELF_CONTAINMENT_CHECK)
    if (NOT ENABLE_CLIENT)
        message(WARNING "header-self-containment: ENABLE_CLIENT=OFF. "
                        "The header-self-containment-check target will not be created.")
    else()
        if (NOT IGNITE3_CLIENT_PUBLIC_HEADERS)
            message(WARNING "header-self-containment: IGNITE3_CLIENT_PUBLIC_HEADERS is empty. "
                            "Check ignite/client/CMakeLists.txt.")
        else()
            set(_hcc_dir "${ABI_BUILD_DIR}/hcc")

            # Write the list of public headers to a cmake file that the
            # sub-project will include.  This avoids command-line quoting
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
            set(_hcc_sub_src "${CMAKE_SOURCE_DIR}/tests/package-test/header_self_containment")
            set(_hcc_sub_bin "${_hcc_dir}/build")

            add_custom_target(header-self-containment-check
                # Install the already-built client to a temp prefix.
                # Only files declared with COMPONENT client are installed —
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
                COMMENT "Header self-containment: compiling each public header against installed package"
                VERBATIM
            )
        endif()
    endif()

endif() # ENABLE_HEADER_SELF_CONTAINMENT_CHECK
