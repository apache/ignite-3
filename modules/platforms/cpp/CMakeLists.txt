#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 3.10)
project(Ignite.C++ VERSION 3 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

include(GNUInstallDirs)
set(INSTALL_INCLUDEDIR ${CMAKE_INSTALL_INCLUDEDIR}/ignite)

option(ENABLE_ADDRESS_SANITIZER "If address sanitizer is enabled" OFF)
option(ENABLE_UB_SANITIZER "If undefined behavior sanitizer is enabled" OFF)

if (ENABLE_ADDRESS_SANITIZER)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=address>)
    add_link_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=address>)
endif()

if (ENABLE_UB_SANITIZER)
    add_compile_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=undefined>)
    add_link_options($<$<COMPILE_LANGUAGE:CXX>:-fsanitize=undefined>)
endif()

add_subdirectory(common)
add_subdirectory(schema)
