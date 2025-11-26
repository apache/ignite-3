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

#include "ignite/common/detail/thread_timer.h"

#include <gtest/gtest.h>

#include <future>
#include <mutex>

using namespace ignite;
using namespace std::chrono_literals;

auto noop_handler = [] (ignite_error &&){};

bool run_with_timeout(std::chrono::milliseconds timeout, std::function<void()> func) {
    auto fut = std::async(std::launch::async, std::move(func));
    auto res = fut.wait_for(timeout);
    return res == std::future_status::ready;
}

TEST(thread_timer, start_stop) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);
        timer->stop();
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, multiple_stops) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);
        timer->stop();
        timer->stop();
        timer->stop();
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, fast_single_timer_event) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        std::promise<void> promise;
        timer->add(0ms, [&promise]() {
            promise.set_value();
        });

        promise.get_future().get();
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, subsequent_timer_events) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        std::mutex mutex;
        std::vector<int> events;
        timer->add(100ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(1);
        });

        timer->add(200ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(2);
        });

        timer->add(300ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(3);
        });

        while (true) {
            std::unique_lock<std::mutex> lock(mutex);
            if (events.size() == 3) {
                EXPECT_EQ(events[0], 1);
                EXPECT_EQ(events[1], 2);
                EXPECT_EQ(events[2], 3);
                return;
            }
        }
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, mixed_order_timer_events) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        std::mutex mutex;
        std::vector<int> events;
        timer->add(400ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(3);
        });

        std::this_thread::sleep_for(100ms);

        timer->add(200ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(2);
        });

        timer->add(100ms, [&]() {
            std::unique_lock<std::mutex> lock(mutex);
            events.push_back(1);
        });

        while (true) {
            std::unique_lock<std::mutex> lock(mutex);
            if (events.size() == 3) {
                EXPECT_EQ(events[0], 1);
                EXPECT_EQ(events[1], 2);
                EXPECT_EQ(events[2], 3);
                return;
            }
        }
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, stop_non_empty) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        timer->add(400ms, []() {
            // No-op.
        });

        std::this_thread::sleep_for(100ms);

        timer->stop();
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, drop_non_empty) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        timer->add(400ms, []() {
            // No-op.
        });

        std::this_thread::sleep_for(100ms);
    });

    EXPECT_TRUE(within_tm);
}

TEST(thread_timer, never_executes_after_stop) {
    auto within_tm = run_with_timeout(1s, [] {
        auto timer = detail::thread_timer::start(noop_handler);

        std::promise<void> promise;
        timer->add(300ms, [&promise]() {
            promise.set_value();
        });

        std::this_thread::sleep_for(100ms);

        timer->stop();

        auto res = promise.get_future().wait_for(300ms);
        EXPECT_EQ(std::future_status::timeout, res);
    });

    EXPECT_TRUE(within_tm);
}
