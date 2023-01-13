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

#include <filesystem>
#include <functional>
#include <iostream>
#include <vector>

#include "test_utils.h"

namespace ignite {

/**
 * Checks if the path looks like binary release home directory.
 * Internally checks for presence of core library.
 * @return @c true if the path looks like binary release home directory.
 */
bool looksLikeBinaryReleaseHome(const std::filesystem::path &path) {
    std::filesystem::path coreLibPath = path / "libs";
    if (!is_directory(coreLibPath))
        return false;

    auto iter = std::filesystem::directory_iterator{coreLibPath};
    return std::any_of(iter, std::filesystem::end(iter), [](auto entry) {
        const std::filesystem::path &entryPath = entry.path();
        if (entryPath.extension() != "jar")
            return false;

        std::string stem = entryPath.stem().string();
        return stem.find("ignite-core") == 0;
    });
}

/**
 * Checks if the path looks like source release home directory.
 * Internally checks for presence of core source directory.
 * @return @c true if the path looks like binary release home directory.
 */
bool looksLikeSourceReleaseHome(const std::filesystem::path &path) {
    std::filesystem::path coreSourcePath =
        path / "modules" / "core" / "src" / "main" / "java" / "org" / "apache" / "ignite";

    return std::filesystem::is_directory(coreSourcePath);
}

std::string resolveIgniteHome(const std::string &path) {
    std::error_code error;

    std::filesystem::path home = std::filesystem::canonical(path, error);
    if (!error && std::filesystem::is_directory(path))
        return home.string();

    const char *env = std::getenv("IGNITE_HOME");
    if (env) {
        home = std::filesystem::canonical(env, error);
        if (!error && std::filesystem::is_directory(home))
            return home.string();
    }

    home = std::filesystem::current_path();
    while (!home.empty() && home.has_relative_path()) {
        if (looksLikeBinaryReleaseHome(home) || looksLikeSourceReleaseHome(home))
            return home.string();

        home = home.parent_path();
    }
    return home.string();
}

} // namespace ignite