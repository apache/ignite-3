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

#include "ignite/common/ignite_result.h"

#include <future>
#include <string>
#include <type_traits>

#include <cstdio>

namespace ignite {

/**
 * Get environment variable.
 *
 * @param name Variable name.
 * @return Variable value if it is set, or @c std::nullopt otherwise.
 */
std::optional<std::string> get_env(const std::string &name);

/**
 * Resolve IGNITE_HOME directory. Resolution is performed in several steps:
 * 1) Check for path provided as argument.
 * 2) Check for environment variable.
 * 3) Check for current working directory.
 * Result of these checks are evaluated based on existence of certain predefined folders inside possible Ignite
 * home. If they are found, IGNITE_HOME is considered resolved.
 *
 * @param path Optional path to check.
 * @return Resolved Ignite home.
 */
std::string resolve_ignite_home(const std::string &path = "");

/**
 * Check async operation result and propagate error to the promise if there is
 * any.
 *
 * @tparam T Result type.
 * @param prom Promise to set.
 * @param res Result to check.
 * @return @c true if there is no error and @c false otherwise.
 */
template<typename T1, typename T2>
bool check_and_set_operation_error(std::promise<T2> &prom, const ignite_result<T1> &res) {
    if (res.has_error()) {
        prom.set_exception(std::make_exception_ptr(res.error()));
        return false;
    }
    return true;
}

/**
 * Check whether test cluster is connectable.
 *
 * @param timeout Timeout.
 * @return @c true if cluster is connectable.
 */
bool check_test_node_connectable(std::chrono::seconds timeout);

/**
 * Make sure that test cluster is connectable.
 * Throws on fail.
 *
 * @param timeout Timeout.
 */
void ensure_node_connectable(std::chrono::seconds timeout);

/**
 * Wait for condition.
 *
 * @param timeout Timeout.
 * @param predicate Predicate.
 * @return @c true if condition is turned @c true within timeout, @c false otherwise.
 */
bool wait_for_condition(std::chrono::seconds timeout, const std::function<bool()> &predicate);

} // namespace ignite