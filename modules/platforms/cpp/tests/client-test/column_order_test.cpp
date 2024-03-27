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

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

#define TEST_TABLE_NAME "column_order_test"

/**
 * Test suite.
 */
class column_order_test : public ignite_runner_suite {
protected:
    static const constexpr std::int32_t test_records = 10;

    static void SetUpTestSuite() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"drop table if exists " TEST_TABLE_NAME}, {});

        client.get_sql().execute(nullptr,
            {"create table column_order_test(val1 varchar, key1 int, val2 bigint, key2 varchar, primary key(key2, "
             "key1))"},
            {});

        for (std::int32_t i = 0; i < test_records; ++i) {
            auto stri = std::to_string(i);
            client.get_sql().execute(nullptr, {"insert into " TEST_TABLE_NAME " values(?, ?, ?, ?)"},
                {"test val " + stri, std::int32_t(i), std::int64_t(i * 2), "test key " + stri});
        }
    }

    static void TearDownTestSuite() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"drop table if exists " TEST_TABLE_NAME}, {});
    }

    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
        auto table = m_client.get_tables().get_table(TEST_TABLE_NAME);
        ASSERT_TRUE(table.has_value());

        m_table = std::move(*table);
    }

    void TearDown() override {
        // remove all
    }

    /** Ignite client. */
    ignite_client m_client;

    /** Test table. */
    table m_table;
};

ignite_tuple make_test_key_tuple(std::int32_t index) {
    return {{"key1", std::int32_t(index)}, {"key2", "test key " + std::to_string(index)}};
}

ignite_tuple make_test_full_tuple(std::int32_t index) {
    auto stri = std::to_string(index);
    return {{"key1", std::int32_t(index)}, {"key2", "test key " + stri}, {"val1", "test val " + stri},
        {"val2", std::int64_t(index * 2)}};
}

void check_test_tuple(const ignite_tuple &tuple, std::int32_t index, bool key_only) {
    ASSERT_EQ(key_only ? 2 : 4, tuple.column_count());

    auto ref = make_test_full_tuple(index);
    EXPECT_EQ(ref.get("key1").get<std::int32_t>(), tuple.get("key1").get<std::int32_t>());
    EXPECT_EQ(ref.get("key2").get<std::string>(), tuple.get("key2").get<std::string>());

    if (!key_only) {
        EXPECT_EQ(ref.get("val1").get<std::string>(), tuple.get("val1").get<std::string>());
        EXPECT_EQ(ref.get("val2").get<std::int64_t>(), tuple.get("val2").get<std::int64_t>());
    }
}

TEST_F(column_order_test, test_key_columns_nondefault_ordering_get) {
    auto tuple_view = m_table.get_record_binary_view();

    for (std::int32_t i = 0; i < test_records; ++i) {
        auto res = tuple_view.get(nullptr, make_test_key_tuple(i));

        ASSERT_TRUE(res.has_value());
        check_test_tuple(*res, i, false);
    }
}

TEST_F(column_order_test, test_key_columns_nondefault_ordering_put_get_remove) {
    auto tuple_view = m_table.get_record_binary_view();

    tuple_view.upsert(nullptr, make_test_full_tuple(13));

    auto res = tuple_view.get(nullptr, make_test_key_tuple(13));
    ASSERT_TRUE(res.has_value());
    check_test_tuple(*res, 13, false);

    bool removed = tuple_view.remove(nullptr, make_test_key_tuple(13));
    ASSERT_TRUE(removed);

    res = tuple_view.get(nullptr, make_test_key_tuple(13));
    ASSERT_FALSE(res.has_value());
}
