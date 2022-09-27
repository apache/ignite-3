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

#include <cstdint>
#include <string>

#define SOCKET_ERROR (-1)

namespace ignite::network::sockets
{

/**
 * Get socket error message for the error code.
 * @param error Error code.
 * @return Socket error message string.
 */
std::string getSocketErrorMessage(int error);

/**
 * Get last socket error message.
 * @return Last socket error message string.
 */
std::string getLastSocketErrorMessage();

/**
 * Try and set socket options.
 *
 * @param socketFd Socket file descriptor.
 * @param bufSize Buffer size.
 * @param noDelay Set no-delay mode.
 * @param outOfBand Set out-of-Band mode.
 * @param keepAlive Keep alive mode.
 */
void trySetSocketOptions(int socketFd, int bufSize, bool noDelay, bool outOfBand, bool keepAlive);

/**
 * Set non blocking mode for socket.
 *
 * @param socketFd Socket file descriptor.
 * @param nonBlocking Non-blocking mode.
 */
bool setNonBlockingMode(int socketFd, bool nonBlocking);

} // namespace ignite::network::sockets
