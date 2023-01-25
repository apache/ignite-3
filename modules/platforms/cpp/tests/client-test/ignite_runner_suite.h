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

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include "gtest_logger.h"
#include "test_utils.h"

#include <gtest/gtest.h>

#include <memory>
#include <string_view>

namespace ignite {

using namespace std::string_view_literals;

/**
 * Test suite.
 */
class ignite_runner_suite : public ::testing::Test {
public:
    static constexpr std::initializer_list<std::string_view> SINGLE_NODE_ADDR = {"127.0.0.1:10942"sv};
    static constexpr std::initializer_list<std::string_view> NODE_ADDRS = {"127.0.0.1:10942"sv, "127.0.0.1:10943"sv};
    static constexpr std::string_view TABLE_1 = "tbl1"sv;

    static constexpr const char *KEY_COLUMN = "key";
    static constexpr const char *VAL_COLUMN = "val";

    /**
     * Get node addresses to use for tests.
     *
     * @return Addresses.
     */
    static std::initializer_list<std::string_view> get_node_addrs() {
        if (single_node_mode())
            return SINGLE_NODE_ADDR;

        return NODE_ADDRS;
    }

    /**
     * Get logger.
     *
     * @return Logger for tests.
     */
    static std::shared_ptr<gtest_logger> get_logger() { return std::make_shared<gtest_logger>(false, true); }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @param val Value.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id, std::string val) {
        return {{KEY_COLUMN, id}, {VAL_COLUMN, std::move(val)}};
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id) { return {{KEY_COLUMN, id}}; }

    /**
     * Clear table @c TABLE_1.
     */
    static void clear_table1() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"DELETE FROM " + std::string(TABLE_1)}, {});
    }
};

} // namespace ignite
