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
#include "tests/test-common/test_utils.h"

#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

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

        m_client = ignite_client::start(cfg, std::chrono::minutes(5));
        auto table = m_client.get_tables().get_table("tbl1");

        tuple_view = table->record_binary_view();
    }

    void TearDown() override {
        std::vector<ignite_tuple> work_range;
        work_range.reserve(200);
        for (int i = -100; i < 100; ++i)
            work_range.emplace_back(get_tuple(i));

        tuple_view.delete_all(nullptr, work_range);
    }

    /**
     * Get tuple for specified column values.
     *
     * @param id ID.
     * @param val Value.
     * @return Ignite tuple instance.
     */
    static ignite_tuple get_tuple(int64_t id, std::string val) {
        return {
            {KEY_COLUMN, id},
            {VAL_COLUMN, std::move(val)}
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

    /** Ignite client. */
    ignite_client m_client;

    /** Record binary view. */
    record_view<ignite_tuple> tuple_view;
};

TEST_F(record_binary_view_test, upsert_get) {
    auto key_tuple = get_tuple(1);
    auto val_tuple = get_tuple(1, "foo");

    tuple_view.upsert(nullptr, val_tuple);
    auto res_tuple = tuple_view.get(nullptr, key_tuple);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(1L, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, upsert_get_async) {
    auto key_tuple = get_tuple(1);
    auto val_tuple = get_tuple(1, "foo");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.upsert_async(nullptr, val_tuple, [&] (ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        tuple_view.get_async(nullptr, key_tuple, [&] (auto res) {
            result_set_promise(*all_done, std::move(res));
        });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, upsert_overrides_value) {
    auto key_tuple = get_tuple(1);
    auto val_tuple = get_tuple(1, "foo");

    tuple_view.upsert(nullptr, val_tuple);
    auto res_tuple = tuple_view.get(nullptr, key_tuple);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(1L, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));

    val_tuple.set<std::string>("val", "bar");
    tuple_view.upsert(nullptr, val_tuple);
    res_tuple = tuple_view.get(nullptr, key_tuple);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(1L, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("bar", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, get_all_empty_keyset) {
    EXPECT_THROW({
        try {
            std::vector<ignite_tuple> empty;
            tuple_view.get_all(nullptr, empty);
        } catch (const ignite_error& e) {
            EXPECT_STREQ("At least one key should be supplied", e.what());
            throw;
        }
    }, ignite_error);
}

TEST_F(record_binary_view_test, get_all_empty_resultset) {
    std::vector<ignite_tuple> some;
    some.push_back(get_tuple(-42));
    auto res = tuple_view.get_all(nullptr, some);

    ASSERT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, upsert_all_empty) {
    EXPECT_THROW({
        try {
            std::vector<ignite_tuple> empty;
            tuple_view.upsert_all(nullptr, empty);
        } catch (const ignite_error& e) {
            EXPECT_STREQ("At least one record should be supplied", e.what());
            throw;
        }
    }, ignite_error);
}

TEST_F(record_binary_view_test, upsert_all_get_all) {
    static constexpr std::size_t records_num = 10;

    std::vector<ignite_tuple> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));

    std::vector<ignite_tuple> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(get_tuple(i));

    tuple_view.upsert_all(nullptr, records);
    auto res = tuple_view.get_all(nullptr, keys);

    // TODO: Key order should be preserved by the server (IGNITE-16004).
    EXPECT_EQ(res.size(), 2);

    ASSERT_TRUE(res[0].has_value());
    EXPECT_EQ(2, res[0]->column_count());
    EXPECT_EQ(9, res[0]->get<int64_t>("key"));
    EXPECT_EQ("Val9", res[0]->get<std::string>("val"));

    ASSERT_TRUE(res[1].has_value());
    EXPECT_EQ(2, res[1]->column_count());
    EXPECT_EQ(10, res[1]->get<int64_t>("key"));
    EXPECT_EQ("Val10", res[1]->get<std::string>("val"));
}

TEST_F(record_binary_view_test, upsert_all_get_all_async) {
    static constexpr std::size_t records_num = 10;

    std::vector<ignite_tuple> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));

    std::vector<ignite_tuple> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(get_tuple(i));

    auto all_done = std::make_shared<std::promise<std::vector<std::optional<ignite_tuple>>>>();

    tuple_view.upsert_all_async(nullptr, records, [&] (ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        // TODO: Key order should be preserved by the server (IGNITE-16004).
        tuple_view.get_all_async(nullptr, keys, [&] (auto res) {
            result_set_promise(*all_done, std::move(res));
        });
    });

    auto res = all_done->get_future().get();

    EXPECT_EQ(res.size(), 2);

    ASSERT_TRUE(res[0].has_value());
    EXPECT_EQ(2, res[0]->column_count());
    EXPECT_EQ(9, res[0]->get<int64_t>("key"));
    EXPECT_EQ("Val9", res[0]->get<std::string>("val"));

    ASSERT_TRUE(res[1].has_value());
    EXPECT_EQ(2, res[1]->column_count());
    EXPECT_EQ(10, res[1]->get<int64_t>("key"));
    EXPECT_EQ("Val10", res[1]->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_new_record) {
    auto val_tuple = get_tuple(42, "foo");
    auto res = tuple_view.insert(nullptr, val_tuple);

    ASSERT_TRUE(res);
}
