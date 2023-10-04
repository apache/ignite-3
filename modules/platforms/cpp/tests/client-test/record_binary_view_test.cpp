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
#include <gmock/gmock-matchers.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class record_binary_view_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
        auto table = m_client.get_tables().get_table(TABLE_1);

        tuple_view = table->get_record_binary_view();
    }

    void TearDown() override {
        std::vector<ignite_tuple> work_range;
        work_range.reserve(200);
        for (int i = -100; i < 100; ++i)
            work_range.emplace_back(get_tuple(i));

        tuple_view.remove_all(nullptr, work_range);
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

    tuple_view.upsert_async(nullptr, val_tuple, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        tuple_view.get_async(nullptr, key_tuple, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
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

TEST_F(record_binary_view_test, upsert_empty_tuple_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.upsert(nullptr, ignite_tuple());
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, upsert_tuple_with_extra_columns_throws) {
    auto val_tuple = get_tuple(1, "foo");
    val_tuple.set("extra", std::string("some value"));
    EXPECT_THROW(
        {
            try {
                tuple_view.upsert(nullptr, val_tuple);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::MatchesRegex(
                    "Key tuple doesn't match schema: schemaVersion=.+, extraColumns=extra"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, get_empty_tuple_throws) {
    EXPECT_THROW(
        {
            try {
                (void) tuple_view.get(nullptr, ignite_tuple());
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, get_all_empty) {
    auto res = tuple_view.get_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, get_all_nonexisting) {
    auto res = tuple_view.get_all(nullptr, {get_tuple(-42)});

    ASSERT_FALSE(res.empty());

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res.front(), std::nullopt);
}

TEST_F(record_binary_view_test, upsert_all_empty_no_throw) {
    tuple_view.upsert_all(nullptr, {});
}

TEST_F(record_binary_view_test, upsert_all_get_all) {
    static constexpr std::int64_t records_num = 10;

    std::vector<ignite_tuple> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));

    std::vector<ignite_tuple> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(get_tuple(i));

    tuple_view.upsert_all(nullptr, records);
    auto res = tuple_view.get_all(nullptr, keys);

    ASSERT_EQ(res.size(), keys.size());
    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i].get<std::int64_t>(0);
        auto val = res[i];

        if (key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key;
            EXPECT_EQ(2, val->column_count());
            EXPECT_EQ(key, val->get<std::int64_t>("key"));
            EXPECT_EQ("Val" + std::to_string(key), val->get<std::string>("val"));
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key << ", Res = " << val->get<std::string>("val");
        }
    }
}

TEST_F(record_binary_view_test, upsert_all_get_all_async) {
    static constexpr std::int64_t records_num = 10;

    std::vector<ignite_tuple> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));

    std::vector<ignite_tuple> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(get_tuple(i));

    auto all_done = std::make_shared<std::promise<std::vector<std::optional<ignite_tuple>>>>();

    tuple_view.upsert_all_async(nullptr, records, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        tuple_view.get_all_async(nullptr, keys, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();

    ASSERT_EQ(res.size(), keys.size());
    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i].get<std::int64_t>(0);
        auto val = res[i];

        if (key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key;
            EXPECT_EQ(2, val->column_count());
            EXPECT_EQ(key, val->get<std::int64_t>("key"));
            EXPECT_EQ("Val" + std::to_string(key), val->get<std::string>("val"));
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key << ", Res = " << val->get<std::string>("val");
        }
    }
}

TEST_F(record_binary_view_test, get_and_upsert_new_record) {
    auto val_tuple = get_tuple(42, "foo");
    auto res_tuple = tuple_view.get_and_upsert(nullptr, val_tuple);

    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, get_and_upsert_existing_record) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto res_tuple = tuple_view.get_and_upsert(nullptr, val_tuple1);

    ASSERT_FALSE(res_tuple.has_value());

    auto val_tuple2 = get_tuple(42, "bar");
    res_tuple = tuple_view.get_and_upsert(nullptr, val_tuple2);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));

    res_tuple = tuple_view.get(nullptr, get_tuple(42));

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("bar", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, get_and_upsert_existing_record_async) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto val_tuple2 = get_tuple(42, "bar");

    auto first = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.get_and_upsert_async(nullptr, val_tuple1, [&](auto res) {
        if (!check_and_set_operation_error(*first, res))
            return;

        if (res.value().has_value())
            first->set_exception(std::make_exception_ptr(ignite_error("Expected nullopt on first insertion")));

        tuple_view.get_and_upsert_async(
            nullptr, val_tuple2, [&](auto res) { result_set_promise(*first, std::move(res)); });
    });

    auto res_tuple = first->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple1.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple1.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple1.get<std::string>("val"), res_tuple->get<std::string>("val"));

    auto second = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
    tuple_view.get_async(nullptr, get_tuple(42), [&](auto res) { result_set_promise(*second, std::move(res)); });

    res_tuple = second->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple2.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple2.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple2.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_new_record) {
    auto val_tuple = get_tuple(42, "foo");
    auto res = tuple_view.insert(nullptr, val_tuple);

    ASSERT_TRUE(res);
}

TEST_F(record_binary_view_test, insert_existing_record) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto res = tuple_view.insert(nullptr, val_tuple1);
    ASSERT_TRUE(res);

    auto val_tuple2 = get_tuple(42, "bar");
    res = tuple_view.insert(nullptr, val_tuple2);
    ASSERT_FALSE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_existing_record_async) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto val_tuple2 = get_tuple(42, "bar");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_async(nullptr, val_tuple1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on first insertion")));

        tuple_view.insert_async(nullptr, val_tuple2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second insertion")));

            tuple_view.get_async(
                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple1.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple1.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple1.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_empty_tuple_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.insert(nullptr, ignite_tuple());
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, insert_all_new) {
    static constexpr std::size_t records_num = 10;

    std::vector<ignite_tuple> keys;
    std::vector<ignite_tuple> records;
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i) {
        keys.emplace_back(get_tuple(i));
        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));
    }

    auto insert_res = tuple_view.insert_all(nullptr, records);

    ASSERT_TRUE(insert_res.empty());

    auto tuples_res = tuple_view.get_all(nullptr, keys);

    EXPECT_EQ(tuples_res.size(), 10);
    ASSERT_TRUE(std::all_of(tuples_res.begin(), tuples_res.end(), [](const auto &elem) { return elem.has_value(); }));
}

TEST_F(record_binary_view_test, insert_all_overlapped) {
    auto res = tuple_view.insert_all(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});

    ASSERT_TRUE(res.empty());

    res = tuple_view.insert_all(nullptr, {get_tuple(2, "baz"), get_tuple(3, "bar")});

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(2, res.front().column_count());
    EXPECT_EQ(2, res.front().get<int64_t>("key"));
    EXPECT_EQ("baz", res.front().get<std::string>("val"));

    auto tuple2 = tuple_view.get(nullptr, get_tuple(2));

    ASSERT_TRUE(tuple2.has_value());
    EXPECT_EQ(2, tuple2->column_count());
    EXPECT_EQ(2, tuple2->get<int64_t>("key"));
    EXPECT_EQ("bar", tuple2->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_all_overlapped_async) {
    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_all_async(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")}, [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value().empty())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));

        tuple_view.insert_all_async(
            nullptr, {get_tuple(1, "foo"), get_tuple(2, "baz"), get_tuple(3, "bar")}, [&](auto res) {
                if (!check_and_set_operation_error(*all_done, res))
                    return;

                if (res.value().size() != 2)
                    all_done->set_exception(std::make_exception_ptr(
                        ignite_error("Expected 2 on second insertion but got " + std::to_string(res.value().size()))));

                tuple_view.get_async(
                    nullptr, get_tuple(2), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
            });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(2, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("bar", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, insert_all_empty) {
    auto res = tuple_view.insert_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, replace_nonexisting) {
    auto val_tuple = get_tuple(42, "foo");
    auto res = tuple_view.replace(nullptr, val_tuple);

    ASSERT_FALSE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, replace_existing) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto res = tuple_view.insert(nullptr, val_tuple1);
    ASSERT_TRUE(res);

    auto val_tuple2 = get_tuple(42, "bar");
    res = tuple_view.replace(nullptr, val_tuple2);
    ASSERT_TRUE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("bar", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, replace_existing_async) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto val_tuple2 = get_tuple(42, "bar");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_async(nullptr, val_tuple1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        tuple_view.replace_async(nullptr, val_tuple2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            tuple_view.get_async(
                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple2.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple2.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple2.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, replace_empty_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.replace(nullptr, ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, replace_exact_nonexisting) {
    auto res = tuple_view.replace(nullptr, get_tuple(42, "foo"), get_tuple(42, "bar"));

    ASSERT_FALSE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, replace_exact_existing_wrong) {
    auto res = tuple_view.insert(nullptr, get_tuple(42, "foo"));
    ASSERT_TRUE(res);

    res = tuple_view.replace(nullptr, get_tuple(42, "bar"), get_tuple(42, "baz"));
    ASSERT_FALSE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, replace_exact_existing_right) {
    auto res = tuple_view.insert(nullptr, get_tuple(42, "foo"));
    ASSERT_TRUE(res);

    res = tuple_view.replace(nullptr, get_tuple(42, "foo"), get_tuple(42, "baz"));
    ASSERT_TRUE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("baz", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, replace_exact_existing_right_async) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto val_tuple2 = get_tuple(42, "bar");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_async(nullptr, val_tuple1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        tuple_view.replace_async(nullptr, val_tuple1, val_tuple2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            tuple_view.get_async(
                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple2.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple2.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple2.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, replace_exact_empty_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.replace(nullptr, ignite_tuple{}, get_tuple(1, "foo"));
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);

    EXPECT_THROW(
        {
            try {
                tuple_view.replace(nullptr, get_tuple(1, "foo"), ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, get_and_replace_nonexisting) {
    auto val_tuple = get_tuple(42, "foo");
    auto res = tuple_view.get_and_replace(nullptr, val_tuple);

    ASSERT_FALSE(res.has_value());

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, get_and_replace_existing) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto res = tuple_view.insert(nullptr, val_tuple1);
    ASSERT_TRUE(res);

    auto val_tuple2 = get_tuple(42, "bar");
    auto res_tuple = tuple_view.get_and_replace(nullptr, val_tuple2);

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));

    res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("bar", res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, get_and_replace_existing_async) {
    auto val_tuple1 = get_tuple(42, "foo");
    auto val_tuple2 = get_tuple(42, "bar");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_async(nullptr, val_tuple1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        tuple_view.get_and_replace_async(
            nullptr, val_tuple2, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple1.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple1.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple1.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, get_and_replace_empty_throws) {
    EXPECT_THROW(
        {
            try {
                (void) tuple_view.get_and_replace(nullptr, ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, remove_nonexisting) {
    auto res = tuple_view.remove(nullptr, get_tuple(1));
    ASSERT_FALSE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(1));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, remove_existing) {
    auto res = tuple_view.insert(nullptr, get_tuple(1, "foo"));
    ASSERT_TRUE(res);

    res = tuple_view.remove(nullptr, get_tuple(1));
    ASSERT_TRUE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(1));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, remove_existing_async) {
    auto all_done = std::make_shared<std::promise<bool>>();

    tuple_view.insert_async(nullptr, get_tuple(42, "foo"), [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        tuple_view.remove_async(
            nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple);
}

TEST_F(record_binary_view_test, remove_empty_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.remove(nullptr, ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, remove_exact_nonexisting) {
    auto res = tuple_view.remove_exact(nullptr, get_tuple(1, "foo"));
    ASSERT_FALSE(res);
}

TEST_F(record_binary_view_test, remove_exact_existing) {
    auto res = tuple_view.insert(nullptr, get_tuple(1, "foo"));
    ASSERT_TRUE(res);

    res = tuple_view.remove_exact(nullptr, get_tuple(1));
    ASSERT_FALSE(res);

    res = tuple_view.remove_exact(nullptr, get_tuple(1, "bar"));
    ASSERT_FALSE(res);

    res = tuple_view.remove_exact(nullptr, get_tuple(1, "foo"));
    ASSERT_TRUE(res);

    auto res_tuple = tuple_view.get(nullptr, get_tuple(1));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, remove_exact_existing_async) {
    auto val_tuple = get_tuple(42, "foo");

    auto all_done = std::make_shared<std::promise<bool>>();

    auto res = tuple_view.insert(nullptr, val_tuple);
    ASSERT_TRUE(res);

    tuple_view.remove_exact_async(nullptr, get_tuple(42), [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on first remove")));

        tuple_view.remove_exact_async(nullptr, get_tuple(42, "bar"), [&](auto res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second remove")));

            tuple_view.remove_exact_async(
                nullptr, val_tuple, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple);
}

TEST_F(record_binary_view_test, remove_exact_empty_throws) {
    EXPECT_THROW(
        {
            try {
                tuple_view.remove_exact(nullptr, ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, get_and_remove_nonexisting) {
    auto res = tuple_view.get_and_remove(nullptr, get_tuple(42));
    ASSERT_FALSE(res.has_value());

    auto res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, get_and_remove_existing) {
    auto res = tuple_view.insert(nullptr, get_tuple(42, "foo"));
    ASSERT_TRUE(res);

    auto res_tuple = tuple_view.get_and_remove(nullptr, get_tuple(42));

    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(2, res_tuple->column_count());
    EXPECT_EQ(42, res_tuple->get<int64_t>("key"));
    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));

    res_tuple = tuple_view.get(nullptr, get_tuple(42));
    ASSERT_FALSE(res_tuple.has_value());
}

TEST_F(record_binary_view_test, get_and_remove_existing_async) {
    auto val_tuple1 = get_tuple(42, "foo");

    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();

    tuple_view.insert_async(nullptr, val_tuple1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        tuple_view.get_and_remove_async(
            nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res_tuple = all_done->get_future().get();
    ASSERT_TRUE(res_tuple.has_value());
    EXPECT_EQ(val_tuple1.column_count(), res_tuple->column_count());
    EXPECT_EQ(val_tuple1.get<int64_t>("key"), res_tuple->get<int64_t>("key"));
    EXPECT_EQ(val_tuple1.get<std::string>("val"), res_tuple->get<std::string>("val"));
}

TEST_F(record_binary_view_test, get_and_remove_empty_throws) {
    EXPECT_THROW(
        {
            try {
                (void) tuple_view.get_and_remove(nullptr, ignite_tuple{});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Tuple can not be empty", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_binary_view_test, remove_all_nonexisting_keys_return_all) {
    std::vector<ignite_tuple> non_existing = {get_tuple(1), get_tuple(2)};
    auto res = tuple_view.remove_all(nullptr, non_existing);

    EXPECT_EQ(res.size(), 2);

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    EXPECT_EQ(1, res[0].column_count());
    EXPECT_EQ(2, res[0].get<int64_t>("key"));

    EXPECT_EQ(1, res[1].column_count());
    EXPECT_EQ(1, res[1].get<int64_t>("key"));
}

TEST_F(record_binary_view_test, remove_all_only_existing) {
    std::vector<ignite_tuple> to_insert = {get_tuple(1, "foo"), get_tuple(2, "bar")};
    tuple_view.upsert_all(nullptr, to_insert);

    auto res = tuple_view.remove_all(nullptr, {get_tuple(1), get_tuple(2)});

    EXPECT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, remove_all_overlapped) {
    static constexpr std::size_t records_num = 10;

    std::vector<ignite_tuple> to_insert;
    to_insert.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        to_insert.emplace_back(get_tuple(i, "Val" + std::to_string(i)));

    tuple_view.upsert_all(nullptr, to_insert);

    std::vector<ignite_tuple> to_remove;
    for (std::int64_t i = 9; i < 13; ++i)
        to_remove.emplace_back(get_tuple(i));

    auto res = tuple_view.remove_all(nullptr, to_remove);

    EXPECT_EQ(res.size(), 2);

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    EXPECT_EQ(1, res[0].column_count());
    EXPECT_EQ(12, res[0].get<int64_t>("key"));

    EXPECT_EQ(1, res[1].column_count());
    EXPECT_EQ(11, res[1].get<int64_t>("key"));
}

TEST_F(record_binary_view_test, remove_all_empty) {
    auto res = tuple_view.remove_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, remove_all_exact_nonexisting) {
    auto res = tuple_view.remove_all_exact(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    ASSERT_EQ(2, res.size());
}

TEST_F(record_binary_view_test, remove_all_exact_overlapped) {
    auto res = tuple_view.insert_all(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});

    ASSERT_TRUE(res.empty());

    res = tuple_view.remove_all_exact(nullptr, {get_tuple(1, "baz"), get_tuple(2, "bar")});

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(2, res.front().column_count());
    EXPECT_EQ(1, res.front().get<int64_t>("key"));
    EXPECT_EQ("baz", res.front().get<std::string>("val"));

    auto tuple2 = tuple_view.get(nullptr, get_tuple(2));

    ASSERT_FALSE(tuple2.has_value());
}

TEST_F(record_binary_view_test, remove_all_exact_overlapped_async) {
    auto all_done = std::make_shared<std::promise<std::vector<ignite_tuple>>>();

    tuple_view.insert_all_async(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")}, [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value().empty())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));

        tuple_view.remove_all_exact_async(nullptr, {get_tuple(1, "baz"), get_tuple(2, "bar")},
            [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res_tuple = all_done->get_future().get();
    EXPECT_EQ(res_tuple.size(), 1);
    EXPECT_EQ(2, res_tuple.front().column_count());
    EXPECT_EQ(1, res_tuple.front().get<int64_t>("key"));
    EXPECT_EQ("baz", res_tuple.front().get<std::string>("val"));
}

TEST_F(record_binary_view_test, remove_all_exact_empty) {
    auto res = tuple_view.remove_all_exact(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_binary_view_test, types_test) {
    auto table = m_client.get_tables().get_table(TABLE_NAME_ALL_COLUMNS);
    tuple_view = table->get_record_binary_view();

    ignite_tuple inserted{
        {"key", std::int64_t(42)},
        {"str", "test"},
        {"int8", std::int8_t(1)},
        {"int16", std::int16_t(2)},
        {"int32", std::int32_t(3)},
        {"int64", std::int64_t(4)},
        {"float", .5f},
        {"double", .6},
        {"uuid", uuid(0x123e4567e89b12d3, 0x7456426614174000)},
        {"date", ignite_date(2023, 2, 7)},
        {"bitmask", bit_array(16, true)},
        {"time", ignite_time(17, 4, 12, 3543634)},
        {"time2", ignite_time(17, 4, 12, 3543634)},
        {"datetime", ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879})},
        {"datetime2", ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879})},
        {"timestamp", ignite_timestamp(3875238472, 248760634)},
        {"timestamp2", ignite_timestamp(3875238472, 248760634)},
        {"blob", std::vector<std::byte>{std::byte(1), std::byte(2), std::byte(42)}},
        {"decimal", big_decimal(123456789098765)},
        {"boolean", true},
    };

    tuple_view.upsert(nullptr, inserted);
    auto res = tuple_view.get(nullptr, {{"key", std::int64_t(42)}});

    ASSERT_TRUE(res.has_value());
    ASSERT_EQ(res->column_count(), inserted.column_count());

    for (int i = 0; i < res->column_count(); ++i) {
        const auto &column = inserted.column_name(i);

        if (column == "time2") {
            EXPECT_EQ(res->get(column), primitive{ignite_time(17, 4, 12)});
        } else if (column == "datetime2") {
            EXPECT_EQ(res->get(column), primitive{ignite_date_time({2020, 7, 28}, {2, 15, 52, 6000000})});
        } else if (column == "timestamp2") {
            EXPECT_EQ(res->get(column), primitive{ignite_timestamp(3875238472, 248700000)});
        } else {
            std::cout << "Column " + column << std::endl;
            EXPECT_EQ(res->get(column), inserted.get(column));
        }
    }
}
