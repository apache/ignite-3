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
#include "test_utils.h"

#include <ignite/client/ignite_client.h>
#include "ignite/common/detail/utils.h"

#include <filesystem>
#include <functional>
#include <iostream>
#include <thread>

namespace ignite {


/**
 * Checks if the path looks like binary release home directory.
 * Internally checks for presence of core library.
 * @return @c true if the path looks like binary release home directory.
 */
bool looks_like_binary_release_home(const std::filesystem::path &path) {
    std::filesystem::path core_lib_path = path / "libs";
    if (!is_directory(core_lib_path))
        return false;

    auto iter = std::filesystem::directory_iterator{core_lib_path};
    return std::any_of(iter, std::filesystem::end(iter), [](auto entry) {
        const std::filesystem::path &entry_path = entry.path();
        if (entry_path.extension() != "jar")
            return false;

        std::string stem = entry_path.stem().string();
        return stem.find("ignite-core") == 0;
    });
}

/**
 * Checks if the path looks like source release home directory.
 * Internally checks for presence of core source directory.
 * @return @c true if the path looks like binary release home directory.
 */
bool looks_like_source_release_home(const std::filesystem::path &path) {
    std::filesystem::path core_source_path =
        path / "modules" / "core" / "src" / "main" / "java" / "org" / "apache" / "ignite";

    return std::filesystem::is_directory(core_source_path);
}

std::string resolve_ignite_home(const std::string &path) {
    std::error_code error;

    std::filesystem::path home = std::filesystem::canonical(path, error);
    if (!error && std::filesystem::is_directory(path))
        return home.string();

    auto env = detail::get_env("IGNITE_HOME");
    if (env) {
        home = std::filesystem::canonical(env.value(), error);
        if (!error && std::filesystem::is_directory(home))
            return home.string();
    }

    home = std::filesystem::current_path();
    while (!home.empty() && home.has_relative_path()) {
        if (looks_like_binary_release_home(home) || looks_like_source_release_home(home))
            return home.string();

        home = home.parent_path();
    }
    return home.string();
}

std::filesystem::path resolve_test_dir() {
    auto home = resolve_ignite_home();
    if (home.empty())
        throw ignite_error("Can not resolve Ignite Home");

    std::filesystem::path home_path(home);
    auto test_path = home_path / "modules" / "platforms" / "cpp" / "tests";
    if (std::filesystem::is_directory(test_path))
        return test_path;

    throw ignite_error("Can not find a 'tests' directory in the current Ignite Home: " + home);
}

bool check_test_node_connectable(std::chrono::seconds timeout) {
    try {
        ensure_node_connectable(timeout);
        return true;
    } catch (...) {
        return false;
    }
}

void ensure_node_connectable(std::chrono::seconds timeout) {
    for (auto &addr : ignite_runner::get_node_addrs()) {
        auto client = ignite_client::start({addr}, timeout);
    }
}

bool wait_for_condition(std::chrono::seconds timeout, const std::function<bool()> &predicate) {
    auto start_time = std::chrono::steady_clock::now();
    do {
        bool success;
        try {
            success = predicate();
        } catch (...) {
            success = false;
        }

        if (success)
            return true;

        std::this_thread::sleep_for(std::chrono::milliseconds(300));
    } while ((std::chrono::steady_clock::now() - start_time) < timeout);
    return false;
}

} // namespace ignite
