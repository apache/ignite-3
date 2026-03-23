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

# ignite_check_headers(SOURCE_DIR <dir> PUBLIC <list> PRIVATE <list>)
#
# Configure-time check that every .h file found under SOURCE_DIR belongs to
# exactly one of the PUBLIC or PRIVATE header lists.
#
# Fails with FATAL_ERROR when:
#   - A header is not listed in either PUBLIC or PRIVATE.
#   - A header is listed in both PUBLIC and PRIVATE.
function(ignite_check_headers)
    cmake_parse_arguments(ARGS "" "SOURCE_DIR" "PUBLIC;PRIVATE" ${ARGN})

    if (NOT ARGS_SOURCE_DIR)
        message(FATAL_ERROR "ignite_check_headers: SOURCE_DIR is required")
    endif()

    file(GLOB_RECURSE _all_headers RELATIVE "${ARGS_SOURCE_DIR}" "${ARGS_SOURCE_DIR}/*.h")
    list(SORT _all_headers)

    set(_errors)

    # Check for headers that appear in both lists.
    foreach(_h IN LISTS ARGS_PUBLIC)
        if (_h IN_LIST ARGS_PRIVATE)
            list(APPEND _errors "  ${_h}  [listed in both PUBLIC_HEADERS and PRIVATE_HEADERS]")
        endif()
    endforeach()

    # Check for headers that are not listed in either list.
    foreach(_h IN LISTS _all_headers)
        set(_in_public FALSE)
        set(_in_private FALSE)
        if (_h IN_LIST ARGS_PUBLIC)
            set(_in_public TRUE)
        endif()
        if (_h IN_LIST ARGS_PRIVATE)
            set(_in_private TRUE)
        endif()
        if (NOT _in_public AND NOT _in_private)
            list(APPEND _errors "  ${_h}  [not listed in PUBLIC_HEADERS or PRIVATE_HEADERS]")
        endif()
    endforeach()

    if (_errors)
        list(JOIN _errors "\n" _error_msg)
        message(FATAL_ERROR
            "Header classification error in ${ARGS_SOURCE_DIR}:\n"
            "${_error_msg}\n"
            "Every header must appear in exactly one of PUBLIC_HEADERS or PRIVATE_HEADERS.")
    endif()
endfunction()
