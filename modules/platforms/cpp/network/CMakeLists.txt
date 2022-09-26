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

project(ignite-network)

set(TARGET ${PROJECT_NAME})

include_directories(include src)

set(SOURCES
    src/network/async_client_pool_adapter.cpp
    src/network/error_handling_filter.cpp
    src/network/codec_data_filter.cpp
    src/network/length_prefix_codec.cpp
    src/network/network.cpp
    src/network/tcp_range.cpp
)

if (WIN32)
    include_directories(os/win/src)

    list(APPEND SOURCES
        os/win/src/network/sockets.cpp
        os/win/src/network/utils.cpp
        os/win/src/network/win_async_client.cpp
        os/win/src/network/win_async_client_pool.cpp
        os/win/src/network/win_async_connecting_thread.cpp
        os/win/src/network/win_async_worker_thread.cpp
    )
else()
    include_directories(os/linux/src)

    list(APPEND SOURCES
        os/linux/src/network/connecting_context.cpp
        os/linux/src/network/linux_async_client.cpp
        os/linux/src/network/linux_async_client_pool.cpp
        os/linux/src/network/linux_async_worker_thread.cpp
        os/linux/src/network/sockets.cpp
        os/linux/src/network/utils.cpp
    )
endif()

add_library(${TARGET} OBJECT ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)

if (WIN32)
    add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
    target_link_libraries(${TARGET} wsock32 ws2_32 iphlpapi crypt32)
endif()

target_link_libraries(${TARGET} ignite-common ignite-protocol)
target_include_directories(${TARGET} INTERFACE include)
