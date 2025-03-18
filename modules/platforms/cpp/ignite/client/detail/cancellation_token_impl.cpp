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

#include "ignite/client/detail/cancellation_token_impl.h"

#include <sstream>

namespace ignite
{

void cancellation_token_impl::cancel_async(ignite_callback<void> callback) {
    std::lock_guard guard(m_mutex);

    if (is_cancelled()) {
        if (m_result) {
            callback(ignite_result<void>{*m_result});
        } else {
            m_callbacks.push_back(std::move(callback));
        }
        return;
    }

    m_callbacks.push_back(std::move(callback));

    auto results = std::make_shared<std::vector<ignite_result<void>>>();
    auto results_mutex = std::make_shared<std::mutex>();
    auto expected_results = m_actions.size();
    for (auto &action : m_actions) {
        action([this, results, results_mutex, expected_results] (ignite_result<void> res) {
            bool all_done{false};
            { // Mutex locking scope
                std::lock_guard guard0(*results_mutex);
                results->push_back(std::move(res));
                all_done = results->size() == expected_results;

                if (all_done) {
                    bool error_found{false};
                    std::stringstream msg_builder;
                    for (auto &result : *results) {
                        if (!result.has_error()) {
                            continue;
                        }

                        if (!error_found) {
                            msg_builder << "One or more cancel actions failed: " << result.error().what_str();
                            error_found = true;
                            continue;
                        }

                        msg_builder << ", " << result.error().what_str();
                    }

                    { // Result mutex locking scope.
                        std::lock_guard guard1(m_mutex);
                        if (error_found) {
                            m_result = {ignite_error(msg_builder.str())};
                        } else {
                            m_result = {};
                        }
                    }
                }
            }

            if (all_done) {
                std::lock_guard guard0(m_mutex);
                for (auto &cb : m_callbacks) {
                    cb(ignite_result<void>{*m_result});
                }
            }
        });
    }
}

void cancellation_token_impl::add_action(std::shared_ptr<ignite_logger> logger,
    std::function<void(ignite_callback<void>)> action) {
    auto callback = [logger](ignite_result<void> res) {
        if (res.has_error()) {
            logger->log_warning(
                "Cancellation action that was added after the token was already canceled: " + res.error().what_str());
        }
    };

    if (is_cancelled()) {
        action(callback);
        return;
    }

    std::unique_lock lock(m_mutex);
    if (is_cancelled()) {
        lock.unlock();
        action(callback);
        return;
    }

    m_actions.push_back(std::move(action));
}
} // namespace ignite
