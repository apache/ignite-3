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

namespace ignite {

void ignite_client::start_async(ignite_client_configuration configuration, std::chrono::milliseconds timeout,
    ignite_callback<ignite_client> callback) {
    // TODO: IGNITE-17762 Async start should not require starting thread internally. Replace with async timer.
    auto fut = std::async([cfg = std::move(configuration), timeout, callback = std::move(callback)]() mutable {
        auto res =
            result_of_operation<ignite_client>([cfg = std::move(cfg), timeout]() { return start(cfg, timeout); });
        callback(std::move(res));
    });
    UNUSED_VALUE fut;
}

ignite_client ignite_client::start(ignite_client_configuration configuration, std::chrono::milliseconds timeout) {

    auto impl = std::make_shared<detail::ignite_client_impl>(std::move(configuration));

    auto promise = std::make_shared<std::promise<ignite_result<void>>>();
    auto future = promise->get_future();

    impl->start(result_promise_setter(promise));

    auto status = future.wait_for(timeout);
    if (status == std::future_status::timeout) {
        impl->stop();
        throw ignite_error(error::code::CONNECTION, "Can not establish connection within timeout");
    }

    assert(status == std::future_status::ready);
    auto res = future.get();
    if (res.has_error()) {
        impl->stop();
        throw ignite_error(res.error());
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

sql ignite_client::get_sql() const noexcept {
    return sql(impl().get_sql_impl());
}

compute ignite_client::get_compute() const noexcept {
    return compute(impl().get_compute_impl());
}

transactions ignite_client::get_transactions() const noexcept {
    return transactions(impl().get_transactions_impl());
}

void ignite_client::get_cluster_nodes_async(ignite_callback<std::vector<cluster_node>> callback) {
    return impl().get_cluster_nodes_async(std::move(callback));
}

std::vector<cluster_node> ignite_client::get_cluster_nodes() {
    return sync<std::vector<cluster_node>>(
        [this](auto callback) mutable { get_cluster_nodes_async(std::move(callback)); });
}

detail::ignite_client_impl &ignite_client::impl() noexcept {
    return *static_cast<detail::ignite_client_impl *>(m_impl.get());
}

const detail::ignite_client_impl &ignite_client::impl() const noexcept {
    return *static_cast<detail::ignite_client_impl *>(m_impl.get());
}

} // namespace ignite
