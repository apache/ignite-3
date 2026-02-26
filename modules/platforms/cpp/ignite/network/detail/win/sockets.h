/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#define WIN32_LEAN_AND_MEAN
#define _WINSOCKAPI_ // NOLINT(bugprone-reserved-identifier)

// clang-format off
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <mstcpip.h>
// clang-format on

#include <string>

namespace ignite::network::detail {

/**
 * Get socket error message for the error code.
 * @param error Error code.
 * @return Socket error message string.
 */
std::string get_socket_error_message(HRESULT error);

/**
 * Get last socket error message.
 * @return Last socket error message string.
 */
std::string get_last_socket_error_message();

/**
 * Try and set socket options.
 *
 * @param socket Socket.
 * @param buf_size Buffer size.
 * @param no_delay Set no-delay mode.
 * @param out_of_band Set out-of-Band mode.
 * @param keep_alive Keep alive mode.
 */
void try_set_socket_options(SOCKET socket, int buf_size, BOOL no_delay, BOOL out_of_band, BOOL keep_alive);

/**
 * Set non blocking mode for socket.
 *
 * @param socket_handle Socket file descriptor.
 * @param non_blocking Non-blocking mode.
 */
bool set_non_blocking_mode(SOCKET socket_handle, bool non_blocking);

/**
 * Wait on the socket for any event for specified time.
 * This function uses poll to achieve timeout functionality for every separate socket operation.
 *
 * @param socket Socket handle.
 * @param timeout Timeout.
 * @param rd Wait for read if @c true, or for write if @c false.
 * @return -errno on error, wait_result::TIMEOUT on timeout and wait_result::SUCCESS on success.
 */
int wait_on_socket(SOCKET socket, std::int32_t timeout, bool rd);

/**
 * Init windows sockets.
 *
 * Thread-safe.
 */
void init_wsa();

int send(SOCKET socket, const void* buf, size_t len);

int recv(SOCKET socket, void* buf, int len);

void close(SOCKET socket);

} // namespace ignite::network::detail
