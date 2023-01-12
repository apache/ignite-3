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

#include "ignite_runner.h"

#include <ignite/common/ignite_error.h>

#include <gtest/gtest.h>

#include <chrono>
#include <csignal>
#include <thread>

namespace {
/** Shutdown handler that cleans up resources. */
std::function<void(int)> shutdown_handler;

/**
 * Receives OS signal and handles it.
 *
 * @param signum Signal value.
 */
void signal_handler(int signum) {
    shutdown_handler(signum);

    signal(signum, SIG_DFL);

    raise(signum);
}
} // namespace

/**
 * Sets process abortion (SIGABRT, SIGINT, SIGSEGV signals) handler.
 *
 * @param handler Abortion handler.
 */
void set_process_abort_handler(std::function<void(int)> handler) {
    shutdown_handler = std::move(handler);

    // Install signal handlers to clean up resources on early exit.
    signal(SIGABRT, signal_handler);
    signal(SIGINT, signal_handler);
    signal(SIGSEGV, signal_handler);
}

/**
 * Run prior to any other tests.
 */
void before_all() {
    ignite::IgniteRunner runner;

    runner.start();

    // Five minutes should be enough but feel free to increase.
    runner.join(std::chrono::minutes(5));
    runner.stop();
}

int main(int argc, char **argv) {
    int res = 0;
    before_all();

    ignite::IgniteRunner runner;

    set_process_abort_handler([&](int signal) {
        std::cout << "Caught signal " << signal << " during tests" << std::endl;

        runner.stop();
    });

    try {
        runner.start();

        // TODO: Implement node startup await
        std::this_thread::sleep_for(std::chrono::seconds(20));

        ::testing::InitGoogleTest(&argc, argv);
        res = RUN_ALL_TESTS();
    } catch (const std::exception &err) {
        std::cout << "Uncaught error: " << err.what() << std::endl;
    } catch (...) {
        std::cout << "Unknown uncaught error" << std::endl;
    }
    runner.stop();

    return res;
}
