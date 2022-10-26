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

#include "ignite_runner_suite.h"

#include <ignite/client/ignite_client.h>
#include <ignite/client/ignite_client_configuration.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>

using namespace ignite;

template <typename T>
bool check_and_set_operation_error(std::promise<void> &operation, const ignite_result<T> &res) {
    if (res.has_error()) {
        operation.set_exception(std::make_exception_ptr(res.error()));
        return false;
    }
    if (!res.has_value()) {
        operation.set_exception(std::make_exception_ptr(ignite_error("There is no value in client result")));
        return false;
    }
    return true;
}

/**
 * Test suite.
 */
class record_binary_view_test : public ignite_runner_suite {
    static constexpr const char* KEY_COLUMN = "key";
    static constexpr const char* VAL_COLUMN = "val";

protected:
    void SetUp() override {
        ignite_client_configuration cfg{NODE_ADDRS};
        cfg.set_logger(get_logger());

        auto client = ignite_client::start(cfg, std::chrono::seconds(5));
        auto table = client.get_tables().get_table("PUB.tbl1");

        tuple_view = table->record_binary_view();
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @param val Value.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id, std::string_view val) {
        return {
            {KEY_COLUMN, id},
            {VAL_COLUMN, val}
        };
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id) {
        return {{KEY_COLUMN, id}};
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(std::string_view val) {
        return {{VAL_COLUMN, val}};
    }

    /** Record binary view. */
    record_view<ignite_tuple> tuple_view;
};

TEST_F(record_binary_view_test, tables_get_table) {
    tuple_view.upsert(nullptr, get_tuple(1, "foo"));

    auto key_tuple = get_tuple(1);
    auto res_tuple = tuple_view.get(nullptr, key_tuple);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(1L, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));
}

