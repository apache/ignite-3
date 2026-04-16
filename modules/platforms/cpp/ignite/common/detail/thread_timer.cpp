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

#include "thread_timer.h"

#include "ignite_result.h"

namespace ignite::detail {

thread_timer::~thread_timer() {
    stop();
}

std::shared_ptr<thread_timer> thread_timer::start(std::function<void(ignite_error&&)> error_handler) {
    std::shared_ptr<thread_timer> res{new thread_timer()};
    res->m_thread = std::thread([state = res->m_state, error_handler = std::move(error_handler)]() {
        std::unique_lock<std::mutex> lock(state->m_mutex);
        while (true) {
            if (state->m_stopping) {
                state->m_condition.notify_one();
                return;
            }

            if (state->m_events.empty()) {
                state->m_condition.wait(lock);
                continue;
            }

            auto nearest_event_ts = state->m_events.top().timestamp;
            auto now = std::chrono::steady_clock::now();
            if (nearest_event_ts < now) {
                auto func = state->m_events.top().callback;
                state->m_events.pop();

                lock.unlock();

                // NOTE: invoking func may destroy the thread_timer object (e.g. when the last
                // shared_ptr<node_connection> held by the callback is released, triggering
                // ~node_connection -> ~thread_timer -> stop()). The timer_state shared_ptr
                // captured by this lambda keeps state alive across that destruction.
                auto res = result_of_operation(func);
                if (res.has_error()) {
                    error_handler(res.error());
                }

                lock.lock();
            } else {
                state->m_condition.wait_until(lock, nearest_event_ts);
            }
        }
    });
    return res;
}

void thread_timer::stop() {
    {
        std::unique_lock<std::mutex> lock(m_state->m_mutex);
        if (m_state->m_stopping)
            return;

        m_state->m_stopping = true;
        m_state->m_condition.notify_one();
    }

    if (std::this_thread::get_id() == m_thread.get_id()) {
        // Called from within a timer callback. Joining the current thread would deadlock, so
        // detach instead. The timer loop will see m_stopping == true on its next iteration and
        // exit cleanly. The timer_state shared_ptr held by the thread lambda keeps the state
        // (mutex, condition variable, event queue) alive until the thread actually terminates.
        m_thread.detach();
    } else {
        m_thread.join();
    }
}

void thread_timer::add(std::chrono::milliseconds timeout, std::function<void()> callback) {
    std::lock_guard<std::mutex> lock(m_state->m_mutex);
    m_state->m_events.emplace(std::chrono::steady_clock::now() + timeout, std::move(callback));
    m_state->m_condition.notify_one();
}

} // namespace ignite::detail
