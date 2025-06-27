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

#include <ignite/network/async_client_pool.h>
#include <ignite/network/data_filter.h>
#include <ignite/network/socket_client.h>
#include <ignite/network/ssl/secure_configuration.h>

#include <string>

namespace ignite::network {

/**
 * Make basic TCP socket.
 */
std::unique_ptr<socket_client> make_tcp_socket_client();

/**
 * Make asynchronous client pool.
 *
 * @param filters Filters.
 * @return Async client pool.
 */
std::shared_ptr<async_client_pool> make_async_client_pool(data_filters filters);

/**
 * Ensure that SSL library is loaded.
 *
 * Called implicitly when secure_socket is created, so there is no need to call this function explicitly.
 *
 * @throw ignite_error if it is not possible to load SSL library.
 */
void ensure_ssl_loaded();

/**
 * Make secure socket for SSL/TLS connection.
 *
 * @param cfg Configuration.
 *
 * @throw ignite_error if it is not possible to load SSL library.
 */
std::unique_ptr<socket_client> make_secure_socket_client(secure_configuration cfg);

} // namespace ignite::network
