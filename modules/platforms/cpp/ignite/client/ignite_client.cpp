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

#include "ignite_client.h"

#include "detail/ignite_client_impl.h"

#include <ignite/common/ignite_error.h>

#include <thread>

namespace ignite {

void ignite_client::start_async(ignite_client_configuration configuration, std::chrono::milliseconds timeout,
    ignite_callback<ignite_client> callback) {
    // TODO: IGNITE-17762 Async start should not require starting thread internally. Replace with async timer.
    (void) std::async([cfg = std::move(configuration), timeout, callback = std::move(callback)]() mutable {
        auto res =
            result_of_operation<ignite_client>([cfg = std::move(cfg), timeout]() { return start(cfg, timeout); });
        callback(std::move(res));
    });
}

ignite_client ignite_client::start(ignite_client_configuration configuration, std::chrono::milliseconds timeout) {
    auto impl = std::make_shared<detail::ignite_client_impl>(std::move(configuration));

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

    return ignite_client(std::move(impl));
}

ignite_client::ignite_client(std::shared_ptr<void> impl)
    : m_impl(std::move(impl)) {
}

const ignite_client_configuration &ignite_client::configuration() const noexcept {
    return impl().configuration();
}

tables ignite_client::get_tables() const noexcept {
    return tables(impl().get_tables_impl());
}

detail::ignite_client_impl &ignite_client::impl() noexcept {
    return *((detail::ignite_client_impl *) (m_impl.get()));
}

const detail::ignite_client_impl &ignite_client::impl() const noexcept {
    return *((detail::ignite_client_impl *) (m_impl.get()));
}

} // namespace ignite
