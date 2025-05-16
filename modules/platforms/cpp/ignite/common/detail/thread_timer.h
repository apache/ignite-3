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

#pragma once

#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <queue>

namespace ignite::detail {

/**
 * Thread-based timer.
 */
class thread_timer final {
    /**
     * Timed event.
     */
    struct timed_event {
        /** Trigger time. */
        std::chrono::steady_clock::time_point timestamp;

        /** Callback to call. */
        std::function<void()> callback;

        /**
         * Constructor.
         */
        timed_event() = default;

        /**
         * Constructor.
         */
        timed_event(std::chrono::steady_clock::time_point ts, std::function<void()> &&cb)
            : timestamp(ts)
            , callback(std::move(cb)) {}

        /**
         * Comparison operator for priority_queue.
         */
        bool operator<(const timed_event& other) const { return timestamp < other.timestamp; }
    };

public:
    /**
     * Start.
     *
     * @return A thread timer instance.
     */
    static std::shared_ptr<thread_timer> start() {
        auto res = std::make_shared<thread_timer>();
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

    /**
     * Stop the thread.
     */
    void stop() {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_stopping = true;
        m_condition.notify_one();
        m_thread.join();
    }

    /**
     * Add a new event.
     *
     * @param timeout Timeout.
     * @param callback Callback to call.
     */
    void add(std::chrono::milliseconds timeout, std::function<void()> callback) {
        std::lock_guard<std::mutex> lock(m_mutex);
        m_events.emplace(std::chrono::steady_clock::now() + timeout, std::move(callback));
        m_condition.notify_one();
    }

private:
    /**
     * Constructor.
     */
    thread_timer() = default;

    /** The stop flag. */
    bool m_stopping{false};

    /** Thread. */
    std::thread m_thread;

    /** Mutex. */
    std::mutex m_mutex;

    /** Conditional variable. */
    std::condition_variable m_condition;

    /** Timed event. */
    std::priority_queue<timed_event> m_events;
};

} // namespace ignite::detail
