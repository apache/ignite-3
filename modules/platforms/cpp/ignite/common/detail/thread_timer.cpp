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

namespace ignite::detail {

std::shared_ptr<thread_timer> thread_timer::start() {
    std::shared_ptr<thread_timer> res{new thread_timer()};
    res->m_thread = std::thread([&self = *res.get()] {
        std::unique_lock<std::mutex> lock(self.m_mutex);
        while (true) {
            if (self.m_stopping) {
                self.m_condition.notify_one();
                return;
            }

            if (self.m_events.empty()) {
                self.m_condition.wait(lock);
                continue;
            }

            auto now = std::chrono::steady_clock::now();
            if (self.m_events.top().timestamp < now) {
                auto event = std::move(self.m_events.top());
                self.m_events.pop();

                lock.unlock();

                try {
                    event.callback();
                } catch (...) {
                    // TODO: Process user exceptions
                }

                lock.lock();
            } else {
                self.m_condition.wait_until(lock, self.m_events.top().timestamp);
            }
        }
    });
    return res;
}

void thread_timer::stop() {
    std::unique_lock<std::mutex> lock(m_mutex);
    m_stopping = true;
    m_condition.notify_one();
    m_thread.join();
}

void thread_timer::add(std::chrono::milliseconds timeout, std::function<void()> callback) {
    std::lock_guard<std::mutex> lock(m_mutex);
    m_events.emplace(std::chrono::steady_clock::now() + timeout, std::move(callback));
    m_condition.notify_one();
}

} // namespace ignite::detail
