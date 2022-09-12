/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifdef _WIN32
#   include "network/win_async_client_pool.h"
#else // Other. Assume Linux
#   include "network/linux_async_client_pool.h"
#endif

#include "common/Platform.h"

#include "ignite/network/network.h"
#include "network/async_client_pool_adapter.h"

namespace ignite::network
{
    std::shared_ptr<AsyncClientPool> makeAsyncClientPool(DataFilters filters)
    {
        auto platformPool = std::make_shared<SWITCH_WIN_OTHER(WinAsyncClientPool, LinuxAsyncClientPool)>();

        return std::make_shared<AsyncClientPoolAdapter>(std::move(filters), std::move(platformPool));
    }
}
