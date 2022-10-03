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

#include <thread>

#include "common/ignite_error.h"
#include "ignite/ignite_client.h"
#include "ignite_client_impl.h"

namespace ignite {

void IgniteClient::startAsync(IgniteClientConfiguration configuration, std::chrono::milliseconds timeout,
    ignite_callback<IgniteClient> callback) {
    // TODO: IGNITE-17762 Async start should not require starting thread internally. Replace with async timer.
    (void)std::async([cfg = std::move(configuration), timeout, callback = std::move(callback)]() mutable {
        auto res = result_of_operation<IgniteClient>(
            [cfg = std::move(cfg), timeout]() { return start(cfg, timeout); });
        callback(std::move(res));
    });
}

IgniteClient IgniteClient::start(IgniteClientConfiguration configuration, std::chrono::milliseconds timeout) {
    auto impl = std::make_shared<detail::IgniteClientImpl>(std::move(configuration));

    auto promise = std::make_shared<std::promise<void>>();
    auto future = promise->get_future();

    impl->start([impl, promise](ignite_result<void> res) mutable {
        if (!res) {
            impl->stop();
            promise->set_exception(std::make_exception_ptr(res.error()));
        } else
            promise->set_value();
    });

    auto status = future.wait_for(timeout);
    if (status == std::future_status::timeout) {
        impl->stop();
        throw ignite_error("Can not establish connection within timeout");
    }

    return IgniteClient(std::move(impl));
}

IgniteClient::IgniteClient(std::shared_ptr<void> impl)
    : m_impl(std::move(impl)) {
}

const IgniteClientConfiguration &IgniteClient::getConfiguration() const noexcept {
    return getImpl().getConfiguration();
}

Tables IgniteClient::getTables() const noexcept {
    return Tables(getImpl().getTablesImpl());
}

detail::IgniteClientImpl &IgniteClient::getImpl() noexcept {
    return *((detail::IgniteClientImpl *)(m_impl.get()));
}

const detail::IgniteClientImpl &IgniteClient::getImpl() const noexcept {
    return *((detail::IgniteClientImpl *)(m_impl.get()));
}

} // namespace ignite
