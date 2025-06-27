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

#include "network.h"

#include "async_client_pool_adapter.h"
#include "ssl/secure_socket_client.h"

#include "ignite/common/detail/config.h"

#ifdef _WIN32
# include "detail/win/tcp_socket_client.h"
# include "detail/win/win_async_client_pool.h"
#else
# include "detail/linux/linux_async_client_pool.h"
# include "detail/linux/tcp_socket_client.h"
#endif

# include "ignite/network/ssl/ssl_gateway.h"

namespace ignite::network {

std::unique_ptr<socket_client> make_tcp_socket_client() {
    return std::make_unique<tcp_socket_client>();
}

std::shared_ptr<async_client_pool> make_async_client_pool(data_filters filters) {
    auto pool =
        std::make_shared<IGNITE_SWITCH_WIN_OTHER(detail::win_async_client_pool, detail::linux_async_client_pool)>();

    return std::make_shared<async_client_pool_adapter>(std::move(filters), std::move(pool));
}

void ensure_ssl_loaded()
{
    ssl_gateway::get_instance().load_all();
}

std::unique_ptr<socket_client> make_secure_socket_client(secure_configuration cfg)
{
    ensure_ssl_loaded();

    return std::make_unique<secure_socket_client>(std::move(cfg));
}

} // namespace ignite::network
