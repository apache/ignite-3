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

# ABI compatibility checking for ignite3-client and ignite3-odbc.
#
#   ENABLE_ABI_COMPLIANCE_CHECKER  - uses abi-dumper + abi-compliance-checker
#
# Baseline file lives under <source_root>/abi/ and are committed to the repo.
# They must be regenerated whenever a deliberate ABI break is released.
#
# Targets produced (when the respective option is ON):
#
#   abi-compliance-checker
#     abichecker-dump            dump ABI of all enabled libs to build/abi/
#     abichecker-check           compare build/abi/ dumps against abi/ baselines
#     abichecker-update-baseline copy build/abi/ dumps back to abi/ (update baseline)

set(ABI_BASELINE_DIR "${CMAKE_SOURCE_DIR}/abi")
set(ABI_BUILD_DIR    "${CMAKE_BINARY_DIR}/abi")

# Collect the libraries that are actually being built.
set(_ABI_TARGETS)
if (ENABLE_CLIENT)
    list(APPEND _ABI_TARGETS ignite3-client)
endif()
if (ENABLE_ODBC)
    list(APPEND _ABI_TARGETS ignite3-odbc)
endif()

if (NOT _ABI_TARGETS)
    message(WARNING "abi_check: no libraries enabled (ENABLE_CLIENT / ENABLE_ODBC are both OFF). "
                    "ABI check target will not be created.")
    return()
endif()

# Warn when debug info is likely missing.
if (NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|RelWithDebInfo)$")
    message(WARNING "abi_check: CMAKE_BUILD_TYPE is '${CMAKE_BUILD_TYPE}'. "
                    "abi-compliance-checker require debug symbols (-g). "
                    "Consider using -DCMAKE_BUILD_TYPE=RelWithDebInfo.")
endif()

add_custom_command(
    OUTPUT  "${ABI_BUILD_DIR}/.created"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ABI_BUILD_DIR}"
    COMMAND ${CMAKE_COMMAND} -E touch "${ABI_BUILD_DIR}/.created"
    COMMENT "Creating ABI output directory"
)
add_custom_target(_abi_build_dir DEPENDS "${ABI_BUILD_DIR}/.created")

if (ENABLE_ABI_COMPLIANCE_CHECKER)
    find_program(ABI_DUMPER_BIN             abi-dumper             REQUIRED)
    find_program(ABI_COMPLIANCE_CHECKER_BIN abi-compliance-checker REQUIRED)

    message(STATUS "abi-compliance-checker: abi-dumper=${ABI_DUMPER_BIN}")
    message(STATUS "abi-compliance-checker: abi-compliance-checker=${ABI_COMPLIANCE_CHECKER_BIN}")

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

endif() # ENABLE_ABI_COMPLIANCE_CHECKER
