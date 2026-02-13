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

#ifdef _WIN32
#include "win/server_socket_adapter.h"
#include "win/client_socket_adapter.h"

#define LAST_SOCKET_ERROR() WSAGetLastError()
#else
#include "posix/server_socket_adapter.h"
#include "posix/client_socket_adapter.h"
#include <cerrno>
#include <cstring>

#define LAST_SOCKET_ERROR() strerror(errno)
#endif