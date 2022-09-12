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

#include "ignite/ignite_client.h"

#include "ignite_client_impl.h"

namespace ignite
{

std::future<IgniteClient> IgniteClient::startAsync(IgniteClientConfiguration configuration)
{
    // TODO: stop connection process on error.
    return std::async(std::launch::async, [cfg = std::move(configuration)] () mutable {
        auto impl = std::make_shared<impl::IgniteClientImpl>(std::move(cfg));

        impl->start();

        return IgniteClient(std::move(impl));
    });
}

IgniteClient::IgniteClient(std::shared_ptr<void> impl) :
    m_impl(std::move(impl)) { }

} // namespace ignite
