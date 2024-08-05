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

project(ignite-network)

set(TARGET ${PROJECT_NAME})

find_package(OpenSSL REQUIRED)

set(SOURCES
    async_client_pool_adapter.cpp
    error_handling_filter.cpp
    codec_data_filter.cpp
    length_prefix_codec.cpp
    network.cpp
    tcp_range.cpp
    ssl/secure_data_filter.cpp
    ssl/secure_socket_client.cpp
    ssl/secure_utils.cpp
    ssl/ssl_gateway.cpp
)

if (WIN32)
    list(APPEND SOURCES
        detail/win/dynamic_module.cpp
        detail/win/sockets.cpp
        detail/win/utils.cpp
        detail/win/win_async_client.cpp
        detail/win/win_async_client_pool.cpp
        detail/win/win_async_connecting_thread.cpp
        detail/win/win_async_worker_thread.cpp
    )
elseif(APPLE)
    list(APPEND SOURCES
        detail/linux/connecting_context.cpp
        detail/linux/dynamic_module.cpp
        detail/macos/macos_async_client.cpp
        detail/linux/linux_async_client_pool.cpp
        detail/macos/macos_async_worker_thread.cpp
        detail/linux/sockets.cpp
        detail/linux/utils.cpp
    )
elseif(UNIX)
    list(APPEND SOURCES
        detail/linux/connecting_context.cpp
        detail/linux/dynamic_module.cpp
        detail/linux/linux_async_client.cpp
        detail/linux/linux_async_client_pool.cpp
        detail/linux/linux_async_worker_thread.cpp
        detail/linux/sockets.cpp
        detail/linux/utils.cpp
    )
endif()

add_library(${TARGET} OBJECT ${SOURCES})

target_link_libraries(${TARGET} PUBLIC ignite-common ignite-protocol)
target_include_directories(${TARGET} PRIVATE ${OPENSSL_INCLUDE_DIR})

if (WIN32)
    add_definitions(-D_WINSOCK_DEPRECATED_NO_WARNINGS)
    target_link_libraries(${TARGET} PUBLIC wsock32 ws2_32 iphlpapi crypt32)
endif()

if (APPLE)
    if (${USE_LOCAL_DEPS})
        find_package(epoll-shim REQUIRED)
        target_link_libraries(${TARGET} PUBLIC epoll-shim::epoll-shim)
    else()
        fetch_dependency(epoll-shim https://github.com/jiixyj/epoll-shim/archive/refs/tags/v0.0.20240608.tar.gz 9751ab5cad7bff8a1388a951276247bf)
        target_link_libraries(${TARGET} PUBLIC epoll-shim)
    endif()
    add_compile_definitions(EPOLL_SHIM_NO_VARIADICS)
endif()

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})
set_target_properties(${TARGET} PROPERTIES POSITION_INDEPENDENT_CODE 1)
