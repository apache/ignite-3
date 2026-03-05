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
# Two tools are supported (enabled independently):
#
#   ENABLE_LIBABIGAIL              - uses abidw + abidiff from the libabigail suite
#   ENABLE_ABI_COMPLIANCE_CHECKER  - uses abi-dumper + abi-compliance-checker
#
# Baseline files live under <source_root>/abi/ and are committed to the repo.
# They must be regenerated whenever a deliberate ABI break is released.
#
# Targets produced (when the respective option is ON):
#
#   libabigail
#     abi-dump                   dump ABI of all enabled libs to build/abi/
#     abi-check                  compare build/abi/ dumps against abi/ baselines
#     abi-update-baseline        copy build/abi/ dumps back to abi/ (update baseline)
#
#   abi-compliance-checker
#     abichecker-dump            dump ABI of all enabled libs to build/abi/
#     abichecker-check           compare build/abi/ dumps against abi/ baselines
#     abichecker-update-baseline copy build/abi/ dumps back to abi/ (update baseline)
#
# Both tools read DWARF debug information from the shared libraries, so the
# build must include debug symbols.  A warning is emitted when they are absent.

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
                    "ABI check targets will not be created.")
    return()
endif()

# Warn when debug info is likely missing.
if (NOT CMAKE_BUILD_TYPE MATCHES "^(Debug|RelWithDebInfo)$")
    message(WARNING "abi_check: CMAKE_BUILD_TYPE is '${CMAKE_BUILD_TYPE}'. "
                    "Both libabigail and abi-compliance-checker require debug symbols (-g). "
                    "Consider using -DCMAKE_BUILD_TYPE=RelWithDebInfo.")
endif()

# ---------------------------------------------------------------------------
# Helper: create the build-time ABI output directory once
# ---------------------------------------------------------------------------
add_custom_command(
    OUTPUT  "${ABI_BUILD_DIR}/.created"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${ABI_BUILD_DIR}"
    COMMAND ${CMAKE_COMMAND} -E touch "${ABI_BUILD_DIR}/.created"
    COMMENT "Creating ABI output directory"
)
add_custom_target(_abi_build_dir DEPENDS "${ABI_BUILD_DIR}/.created")

# ===========================================================================
# 1. libabigail  (abidw + abidiff)
# ===========================================================================
if (ENABLE_LIBABIGAIL)

    find_program(ABIDW_BIN    abidw    REQUIRED)
    find_program(ABIDIFF_BIN  abidiff  REQUIRED)

    message(STATUS "libabigail: abidw=${ABIDW_BIN}")
    message(STATUS "libabigail: abidiff=${ABIDIFF_BIN}")

    # abi-dump  ---------------------------------------------------------------
    # Dumps the ABI of every enabled library into build/abi/<lib>.abi
    set(_abidw_stamp_deps _abi_build_dir)

    foreach(_lib IN LISTS _ABI_TARGETS)
        set(_out "${ABI_BUILD_DIR}/${_lib}.abi")

        add_custom_command(
            OUTPUT  "${_out}"
            COMMAND ${ABIDW_BIN}
                        $<TARGET_FILE:${_lib}>
                        --out-file "${_out}"
            DEPENDS ${_lib} _abi_build_dir
            COMMENT "abidw: dumping ABI of ${_lib}"
            VERBATIM
        )
        list(APPEND _abidw_stamp_deps "${_out}")
    endforeach()

    add_custom_target(abi-dump DEPENDS ${_abidw_stamp_deps}
        COMMENT "libabigail: ABI dumps written to ${ABI_BUILD_DIR}/")

    # abi-check  --------------------------------------------------------------
    # Compares each fresh dump against the committed baseline.
    # abidiff exits non-zero when an incompatible change is detected.
    set(_abidiff_deps abi-dump)

    foreach(_lib IN LISTS _ABI_TARGETS)
        set(_baseline "${ABI_BASELINE_DIR}/${_lib}.abi")
        set(_current  "${ABI_BUILD_DIR}/${_lib}.abi")

        if (NOT EXISTS "${_baseline}")
            message(WARNING "libabigail: no baseline found at ${_baseline}. "
                            "Run 'cmake --build . --target abi-update-baseline' first.")
        endif()

        add_custom_target(abi-check-${_lib}
            COMMAND ${ABIDIFF_BIN}
                        --no-show-locs
                        --drop-private-types
                        "${_baseline}"
                        "${_current}"
            DEPENDS abi-dump
            COMMENT "abidiff: checking ${_lib} against baseline"
            VERBATIM
        )
        list(APPEND _abidiff_deps abi-check-${_lib})
    endforeach()

    add_custom_target(abi-check DEPENDS ${_abidiff_deps}
        COMMENT "libabigail: ABI check complete")

    # abi-update-baseline  ----------------------------------------------------
    # Copies fresh dumps over the committed baselines (run after intentional ABI change).
    set(_update_commands)
    add_custom_target(abi-update-baseline
        COMMAND ${CMAKE_COMMAND} -E make_directory "${ABI_BASELINE_DIR}"
        COMMENT "libabigail: updating baselines in ${ABI_BASELINE_DIR}/"
        VERBATIM
    )

    foreach(_lib IN LISTS _ABI_TARGETS)
        add_custom_target(_abi-update-${_lib}
            COMMAND ${CMAKE_COMMAND} -E copy
                        "${ABI_BUILD_DIR}/${_lib}.abi"
                        "${ABI_BASELINE_DIR}/${_lib}.abi"
            DEPENDS abi-dump
            COMMENT "libabigail: updating baseline for ${_lib}"
            VERBATIM
        )
        add_dependencies(abi-update-baseline _abi-update-${_lib})
    endforeach()

endif() # ENABLE_LIBABIGAIL

# ===========================================================================
# 2. abi-compliance-checker  (abi-dumper + abi-compliance-checker)
# ===========================================================================
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

    # abichecker-dump  --------------------------------------------------------
    # Runs abi-dumper on each library to produce a Perl-serialised dump.
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

    # abichecker-check  -------------------------------------------------------
    # Compares each fresh dump against the committed baseline.
    # abi-compliance-checker exits non-zero when compatibility is below 100%.
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

    # abichecker-update-baseline  ---------------------------------------------
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
