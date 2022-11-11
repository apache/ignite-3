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

#include <ignite/common/config.h>

#ifdef _WIN32
# include "detail/win/win_async_client_pool.h"
#else
# include "detail/linux/linux_async_client_pool.h"
#endif

namespace ignite::network {

std::shared_ptr<async_client_pool> make_async_client_pool(data_filters filters) {
    auto pool =
        std::make_shared<IGNITE_SWITCH_WIN_OTHER(detail::win_async_client_pool, detail::linux_async_client_pool)>();

    return std::make_shared<async_client_pool_adapter>(std::move(filters), std::move(pool));
}

} // namespace ignite::network
