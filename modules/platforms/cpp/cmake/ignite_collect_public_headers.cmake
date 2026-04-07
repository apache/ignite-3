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

# ignite_collect_public_headers(<MODULE> <HEADERS_VAR>)
#
# Registers public headers for compile-time checking.
#
# Arguments:
#   <MODULE>      The module name (e.g. "common", "tuple", "client").
#   <HEADERS_VAR> Name of a variable containing the list of headers as paths
#                 relative to the module source directory
#                 (e.g. "big_integer.h", "compute/compute.h").
#
# Appends "ignite/<MODULE>/<header>" to the IGNITE3_ALL_PUBLIC_HEADERS list
# and propagates the updated list to the parent scope.
#
# Implemented as a macro (not a function) so that PARENT_SCOPE resolves to
# the scope above the calling subdirectory — i.e. the top-level CMakeLists.txt
# — rather than only one level above the call site.
macro(ignite_collect_public_headers MODULE HEADERS_VAR)
    foreach(H IN LISTS ${HEADERS_VAR})
        list(APPEND IGNITE3_ALL_PUBLIC_HEADERS "ignite/${MODULE}/${H}")
    endforeach()
    set(IGNITE3_ALL_PUBLIC_HEADERS "${IGNITE3_ALL_PUBLIC_HEADERS}" PARENT_SCOPE)
endmacro()
