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

    file(GLOB_RECURSE CONFIGURE_DEPENDS ALL_HEADERS RELATIVE "${ARGS_SOURCE_DIR}" "${ARGS_SOURCE_DIR}/*.h")
    list(SORT ALL_HEADERS)

    set(ERRORS)

    # Check for headers that appear in both lists.
    foreach(H IN LISTS ARGS_PUBLIC)
        if (H IN_LIST ARGS_PRIVATE)
            list(APPEND ERRORS "  ${H}  [listed in both PUBLIC_HEADERS and PRIVATE_HEADERS]")
        endif()
    endforeach()

    # Check for headers that are not listed in either list.
    foreach(H IN LISTS ALL_HEADERS)
        set(IN_PUBLIC FALSE)
        set(IN_PRIVATE FALSE)
        if (H IN_LIST ARGS_PUBLIC)
            set(IN_PUBLIC TRUE)
        endif()
        if (H IN_LIST ARGS_PRIVATE)
            set(IN_PRIVATE TRUE)
        endif()
        if (NOT IN_PUBLIC AND NOT IN_PRIVATE)
            list(APPEND ERRORS "  ${H}  [not listed in PUBLIC_HEADERS or PRIVATE_HEADERS]")
        endif()
    endforeach()

    if (ERRORS)
        list(JOIN ERRORS "\n" ERROR_MSG)
        message(FATAL_ERROR
            "Header classification error in ${ARGS_SOURCE_DIR}:\n"
            "${ERROR_MSG}\n"
            "Every header must appear in exactly one of PUBLIC_HEADERS or PRIVATE_HEADERS.")
    endif()
endfunction()
