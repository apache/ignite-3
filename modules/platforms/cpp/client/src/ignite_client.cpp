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

#include <thread>

#include "common/ignite_error.h"
#include "ignite/ignite_client.h"
#include "ignite_client_impl.h"

namespace ignite
{

void IgniteClient::startAsync(IgniteClientConfiguration configuration, std::chrono::milliseconds timeout,
    IgniteCallback<IgniteClient> callback)
{
    (void) std::async([cfg = std::move(configuration), timeout, callback = std::move(callback)]() mutable {
        auto res = IgniteResult<IgniteClient>::ofOperation([cfg = std::move(cfg), timeout] () {
            return start(cfg, timeout);
        });
        callback(std::move(res));
    });
}

IgniteClient IgniteClient::start(IgniteClientConfiguration configuration, std::chrono::milliseconds timeout)
{
    auto impl = std::make_shared<impl::IgniteClientImpl>(std::move(configuration));

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    impl->start([impl, promise] (IgniteResult<void> res) mutable {
        auto err = res.getError();
        if (err) {
            impl->stop();
            promise->set_exception(std::make_exception_ptr(err));
        }
        else
            promise->set_value();
    });

    auto status = future.wait_for(timeout);
    if (status == std::future_status::timeout) {
        impl->stop();
        throw IgniteError("Can not establish connection within timeout");
    }

    return IgniteClient(std::move(impl));
}

IgniteClient::IgniteClient(std::shared_ptr<void> impl) :
    m_impl(std::move(impl)) { }

const IgniteClientConfiguration &IgniteClient::getConfiguration() const {
    return getImpl().getConfiguration();
}

Tables IgniteClient::getTables() const {
    return Tables(getImpl().getTablesImpl());
}

impl::IgniteClientImpl &IgniteClient::getImpl() {
    return *((impl::IgniteClientImpl*)(m_impl.get()));
}

const impl::IgniteClientImpl &IgniteClient::getImpl() const {
    return *((impl::IgniteClientImpl*)(m_impl.get()));
}

} // namespace ignite
