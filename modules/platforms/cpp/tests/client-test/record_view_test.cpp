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
 * Test table type mapping (@see ignite_runner_suite::TABLE_1).
 */
struct test_type {
    test_type() = default;
    explicit test_type(std::int64_t key) : key(key) {}
    explicit test_type(std::string val) : val(std::move(val)) {}
    explicit test_type(std::int64_t key, std::string val) : key(key), val(std::move(val)) {}

    std::int64_t key{0};
    std::string val;
};

namespace ignite {

template<>
ignite_tuple convert_to_tuple(test_type &&value) {
    ignite_tuple tuple;

    tuple.set("key", value.key);
    tuple.set("val", value.val);

    return tuple;
}

template<>
test_type convert_from_tuple(ignite_tuple&& value) {
    test_type res;

    res.key = value.get<std::int64_t>("key");
    res.val = value.get<std::string>("val");

    return res;
}

}

/**
 * Test suite.
 */
class record_view_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
        auto table = m_client.get_tables().get_table(TABLE_1);

        view = table->get_record_view<test_type>();
    }

    void TearDown() override {
        std::vector<test_type> work_range;
        work_range.reserve(200);
        for (std::int64_t i = -100; i < 100; ++i)
            work_range.emplace_back(i);

//        record_view.remove_all(nullptr, work_range);
    }

    /** Ignite client. */
    ignite_client m_client;

    /** Record binary view. */
    record_view<test_type> view;
};

TEST_F(record_view_test, upsert_get) {
    test_type key{1};
    test_type val{1, "foo"};

    view.upsert(nullptr, val);
    auto res = view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1L, res->key);
    EXPECT_EQ("foo", res->val);
}

TEST_F(record_view_test, upsert_get_async) {
    test_type key{1};
    test_type val{1, "foo"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.upsert_async(nullptr, val, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val.key, res->key);
    EXPECT_EQ(val.val, res->val);
}


TEST_F(record_view_test, upsert_overrides_value) {
    test_type key{1};
    test_type val{1, "foo"};

    view.upsert(nullptr, val);
    auto res = view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1L, res->key);
    EXPECT_EQ("foo", res->val);

    val.val = "bar";
    view.upsert(nullptr, val);
    res = view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(1L, res->key);
    EXPECT_EQ("bar", res->val);
}

TEST_F(record_view_test, get_all_empty) {
    auto res = view.get_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_view_test, get_all_nonexisting) {
    auto res = view.get_all(nullptr, {test_type(-42)});

    ASSERT_TRUE(res.empty());
}
//
//TEST_F(record_view_test, upsert_all_empty_no_throw) {
//    view.upsert_all(nullptr, {});
//}
//
//TEST_F(record_view_test, upsert_all_get_all) {
//    static constexpr std::size_t records_num = 10;
//
//    std::vector<ignite_tuple> records;
//    records.reserve(records_num);
//    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
//        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));
//
//    std::vector<ignite_tuple> keys;
//    for (std::int64_t i = 9; i < 13; ++i)
//        keys.emplace_back(get_tuple(i));
//
//    view.upsert_all(nullptr, records);
//    auto res = view.get_all(nullptr, keys);
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    EXPECT_EQ(res.size(), 2);
//
//    ASSERT_TRUE(res[0].has_value());
//    EXPECT_EQ(2, res[0]->column_count());
//    EXPECT_EQ(9, res[0]->get<int64_t>("key"));
//    EXPECT_EQ("Val9", res[0]->get<std::string>("val"));
//
//    ASSERT_TRUE(res[1].has_value());
//    EXPECT_EQ(2, res[1]->column_count());
//    EXPECT_EQ(10, res[1]->get<int64_t>("key"));
//    EXPECT_EQ("Val10", res[1]->get<std::string>("val"));
//}
//
//TEST_F(record_view_test, upsert_all_get_all_async) {
//    static constexpr std::size_t records_num = 10;
//
//    std::vector<ignite_tuple> records;
//    records.reserve(records_num);
//    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
//        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));
//
//    std::vector<ignite_tuple> keys;
//    for (std::int64_t i = 9; i < 13; ++i)
//        keys.emplace_back(get_tuple(i));
//
//    auto all_done = std::make_shared<std::promise<std::vector<std::optional<ignite_tuple>>>>();
//
//    view.upsert_all_async(nullptr, records, [&](ignite_result<void> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        // TODO: Key order should be preserved by the server (IGNITE-16004).
//        view.get_all_async(nullptr, keys, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//
//    EXPECT_EQ(res.size(), 2);
//
//    ASSERT_TRUE(res[0].has_value());
//    EXPECT_EQ(2, res[0]->column_count());
//    EXPECT_EQ(9, res[0]->get<int64_t>("key"));
//    EXPECT_EQ("Val9", res[0]->get<std::string>("val"));
//
//    ASSERT_TRUE(res[1].has_value());
//    EXPECT_EQ(2, res[1]->column_count());
//    EXPECT_EQ(10, res[1]->get<int64_t>("key"));
//    EXPECT_EQ("Val10", res[1]->get<std::string>("val"));
//}
//
//TEST_F(record_view_test, get_and_upsert_new_record) {
//    auto val = get_tuple(42, "foo");
//    auto res = view.get_and_upsert(nullptr, val);
//
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, get_and_upsert_existing_record) {
//    auto val1 = get_tuple(42, "foo");
//    auto res = view.get_and_upsert(nullptr, val1);
//
//    ASSERT_FALSE(res_tuple.has_value());
//
//    auto val2 = get_tuple(42, "bar");
//    res = view.get_and_upsert(nullptr, val2);
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//
//    res = view.get(nullptr, get_tuple(42));
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(record_view_test, get_and_upsert_existing_record_async) {
//    auto val1 = get_tuple(42, "foo");
//    auto val2 = get_tuple(42, "bar");
//
//    auto first = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.get_and_upsert_async(nullptr, val1, [&](auto res) {
//        if (!check_and_set_operation_error(*first, res))
//            return;
//
//        if (res.value().has_value())
//            first->set_exception(std::make_exception_ptr(ignite_error("Expected nullopt on first insertion")));
//
//        view.get_and_upsert_async(
//            nullptr, val2, [&](auto res) { result_set_promise(*first, std::move(res)); });
//    });
//
//    auto res = first->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val1.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val1.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val1.get<std::string>("val"), res->val);
//
//    auto second = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//    view.get_async(nullptr, get_tuple(42), [&](auto res) { result_set_promise(*second, std::move(res)); });
//
//    res = second->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val2.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val2.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val2.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, insert_new_record) {
//    auto val = get_tuple(42, "foo");
//    auto res = view.insert(nullptr, val);
//
//    ASSERT_TRUE(res);
//}
//
//TEST_F(record_view_test, insert_existing_record) {
//    auto val1 = get_tuple(42, "foo");
//    auto res = view.insert(nullptr, val1);
//    ASSERT_TRUE(res);
//
//    auto val2 = get_tuple(42, "bar");
//    res = view.insert(nullptr, val2);
//    ASSERT_FALSE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//}
//
//TEST_F(record_view_test, insert_existing_record_async) {
//    auto val1 = get_tuple(42, "foo");
//    auto val2 = get_tuple(42, "bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on first insertion")));
//
//        view.insert_async(nullptr, val2, [&](ignite_result<bool> &&res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second insertion")));
//
//            view.get_async(
//                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val1.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val1.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val1.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, insert_empty_tuple_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                view.insert(nullptr, ignite_tuple());
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, insert_all_new) {
//    static constexpr std::size_t records_num = 10;
//
//    std::vector<ignite_tuple> keys;
//    std::vector<ignite_tuple> records;
//    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i) {
//        keys.emplace_back(get_tuple(i));
//        records.emplace_back(get_tuple(i, "Val" + std::to_string(i)));
//    }
//
//    auto insert_res = view.insert_all(nullptr, records);
//
//    ASSERT_TRUE(insert_res.empty());
//
//    auto tuples_res = view.get_all(nullptr, keys);
//
//    EXPECT_EQ(tuples_res.size(), 10);
//    ASSERT_TRUE(std::all_of(tuples_res.begin(), tuples_res.end(), [](const auto &elem) { return elem.has_value(); }));
//}
//
//TEST_F(record_view_test, insert_all_overlapped) {
//    auto res = view.insert_all(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});
//
//    ASSERT_TRUE(res.empty());
//
//    res = view.insert_all(nullptr, {get_tuple(2, "baz"), get_tuple(3, "bar")});
//
//    EXPECT_EQ(res.size(), 1);
//    EXPECT_EQ(2, res.front().column_count());
//    EXPECT_EQ(2, res.front().get<int64_t>("key"));
//    EXPECT_EQ("baz", res.front().get<std::string>("val"));
//
//    auto tuple2 = view.get(nullptr, get_tuple(2));
//
//    ASSERT_TRUE(tuple2.has_value());
//    EXPECT_EQ(2, tuple2->column_count());
//    EXPECT_EQ(2, tuple2->get<int64_t>("key"));
//    EXPECT_EQ("bar", tuple2->get<std::string>("val"));
//}
//
//TEST_F(record_view_test, insert_all_overlapped_async) {
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_all_async(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")}, [&](auto res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value().empty())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));
//
//        view.insert_all_async(
//            nullptr, {get_tuple(1, "foo"), get_tuple(2, "baz"), get_tuple(3, "bar")}, [&](auto res) {
//                if (!check_and_set_operation_error(*all_done, res))
//                    return;
//
//                if (res.value().size() != 2)
//                    all_done->set_exception(std::make_exception_ptr(
//                        ignite_error("Expected 2 on second insertion but got " + std::to_string(res.value().size()))));
//
//                view.get_async(
//                    nullptr, get_tuple(2), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//            });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(record_view_test, insert_all_empty) {
//    auto res = view.insert_all(nullptr, {});
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(record_view_test, replace_nonexisting) {
//    auto val = get_tuple(42, "foo");
//    auto res = view.replace(nullptr, val);
//
//    ASSERT_FALSE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, replace_existing) {
//    auto val1 = get_tuple(42, "foo");
//    auto res = view.insert(nullptr, val1);
//    ASSERT_TRUE(res);
//
//    auto val2 = get_tuple(42, "bar");
//    res = view.replace(nullptr, val2);
//    ASSERT_TRUE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(record_view_test, replace_existing_async) {
//    auto val1 = get_tuple(42, "foo");
//    auto val2 = get_tuple(42, "bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        view.replace_async(nullptr, val2, [&](ignite_result<bool> &&res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (!res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));
//
//            view.get_async(
//                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val2.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val2.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val2.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, replace_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                view.replace(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, replace_exact_nonexisting) {
//    auto res = view.replace(nullptr, get_tuple(42, "foo"), get_tuple(42, "bar"));
//
//    ASSERT_FALSE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, replace_exact_existing_wrong) {
//    auto res = view.insert(nullptr, get_tuple(42, "foo"));
//    ASSERT_TRUE(res);
//
//    res = view.replace(nullptr, get_tuple(42, "bar"), get_tuple(42, "baz"));
//    ASSERT_FALSE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//}
//
//TEST_F(record_view_test, replace_exact_existing_right) {
//    auto res = view.insert(nullptr, get_tuple(42, "foo"));
//    ASSERT_TRUE(res);
//
//    res = view.replace(nullptr, get_tuple(42, "foo"), get_tuple(42, "baz"));
//    ASSERT_TRUE(res);
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("baz", res->val);
//}
//
//TEST_F(record_view_test, replace_exact_existing_right_async) {
//    auto val1 = get_tuple(42, "foo");
//    auto val2 = get_tuple(42, "bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        view.replace_async(nullptr, val1, val2, [&](ignite_result<bool> &&res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (!res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));
//
//            view.get_async(
//                nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val2.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val2.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val2.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, replace_exact_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                view.replace(nullptr, ignite_tuple{}, get_tuple(1, "foo"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//
//    EXPECT_THROW(
//        {
//            try {
//                view.replace(nullptr, get_tuple(1, "foo"), ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, get_and_replace_nonexisting) {
//    auto val = get_tuple(42, "foo");
//    auto res = view.get_and_replace(nullptr, val);
//
//    ASSERT_FALSE(res.has_value());
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, get_and_replace_existing) {
//    auto val1 = get_tuple(42, "foo");
//    auto res = view.insert(nullptr, val1);
//    ASSERT_TRUE(res);
//
//    auto val2 = get_tuple(42, "bar");
//    auto res = view.get_and_replace(nullptr, val2);
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//
//    res = view.get(nullptr, get_tuple(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(record_view_test, get_and_replace_existing_async) {
//    auto val1 = get_tuple(42, "foo");
//    auto val2 = get_tuple(42, "bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        view.get_and_replace_async(
//            nullptr, val2, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val1.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val1.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val1.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, get_and_replace_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                (void) view.get_and_replace(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, remove_nonexisting) {
//    auto res = view.remove(nullptr, get_tuple(1));
//    ASSERT_FALSE(res);
//
//    auto res = view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, remove_existing) {
//    auto res = view.insert(nullptr, get_tuple(1, "foo"));
//    ASSERT_TRUE(res);
//
//    res = view.remove(nullptr, get_tuple(1));
//    ASSERT_TRUE(res);
//
//    auto res = view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, remove_existing_async) {
//    auto all_done = std::make_shared<std::promise<bool>>();
//
//    view.insert_async(nullptr, get_tuple(42, "foo"), [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        view.remove_async(
//            nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res_tuple);
//}
//
//TEST_F(record_view_test, remove_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                view.remove(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, remove_exact_nonexisting) {
//    auto res = view.remove_exact(nullptr, get_tuple(1, "foo"));
//    ASSERT_FALSE(res);
//}
//
//TEST_F(record_view_test, remove_exact_existing) {
//    auto res = view.insert(nullptr, get_tuple(1, "foo"));
//    ASSERT_TRUE(res);
//
//    res = view.remove_exact(nullptr, get_tuple(1));
//    ASSERT_FALSE(res);
//
//    res = view.remove_exact(nullptr, get_tuple(1, "bar"));
//    ASSERT_FALSE(res);
//
//    res = view.remove_exact(nullptr, get_tuple(1, "foo"));
//    ASSERT_TRUE(res);
//
//    auto res = view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, remove_exact_existing_async) {
//    auto val = get_tuple(42, "foo");
//
//    auto all_done = std::make_shared<std::promise<bool>>();
//
//    auto res = view.insert(nullptr, val);
//    ASSERT_TRUE(res);
//
//    view.remove_exact_async(nullptr, get_tuple(42), [&](auto res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on first remove")));
//
//        view.remove_exact_async(nullptr, get_tuple(42, "bar"), [&](auto res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second remove")));
//
//            view.remove_exact_async(
//                nullptr, val, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res_tuple);
//}
//
//TEST_F(record_view_test, remove_exact_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                view.remove_exact(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, get_and_remove_nonexisting) {
//    auto res = view.get_and_replace(nullptr, get_tuple(42, "foo"));
//    ASSERT_FALSE(res.has_value());
//
//    auto res = view.get(nullptr, get_tuple(42));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, get_and_remove_existing) {
//    auto res = view.insert(nullptr, get_tuple(42, "foo"));
//    ASSERT_TRUE(res);
//
//    auto res = view.get_and_remove(nullptr, get_tuple(42));
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//
//    res = view.get(nullptr, get_tuple(42));
//    ASSERT_FALSE(res_tuple.has_value());
//}
//
//TEST_F(record_view_test, get_and_remove_existing_async) {
//    auto val1 = get_tuple(42, "foo");
//
//    auto all_done = std::make_shared<std::promise<std::optional<ignite_tuple>>>();
//
//    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        view.get_and_remove_async(
//            nullptr, get_tuple(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(val1.column_count(), res_tuple->column_count());
//    EXPECT_EQ(val1.get<int64_t>("key"), res->key);
//    EXPECT_EQ(val1.get<std::string>("val"), res->val);
//}
//
//TEST_F(record_view_test, get_and_remove_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                (void) view.get_and_remove(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(record_view_test, remove_all_nonexisting_keys_return_all) {
//    std::vector<ignite_tuple> non_existing = {get_tuple(1), get_tuple(2)};
//    auto res = view.remove_all(nullptr, non_existing);
//
//    EXPECT_EQ(res.size(), 2);
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    EXPECT_EQ(1, res[0].column_count());
//    EXPECT_EQ(2, res[0].get<int64_t>("key"));
//
//    EXPECT_EQ(1, res[1].column_count());
//    EXPECT_EQ(1, res[1].get<int64_t>("key"));
//}
//
//TEST_F(record_view_test, remove_all_only_existing) {
//    std::vector<ignite_tuple> to_insert = {get_tuple(1, "foo"), get_tuple(2, "bar")};
//    view.upsert_all(nullptr, to_insert);
//
//    auto res = view.remove_all(nullptr, {get_tuple(1), get_tuple(2)});
//
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(record_view_test, remove_all_overlapped) {
//    static constexpr std::size_t records_num = 10;
//
//    std::vector<ignite_tuple> to_insert;
//    to_insert.reserve(records_num);
//    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
//        to_insert.emplace_back(get_tuple(i, "Val" + std::to_string(i)));
//
//    view.upsert_all(nullptr, to_insert);
//
//    std::vector<ignite_tuple> to_remove;
//    for (std::int64_t i = 9; i < 13; ++i)
//        to_remove.emplace_back(get_tuple(i));
//
//    auto res = view.remove_all(nullptr, to_remove);
//
//    EXPECT_EQ(res.size(), 2);
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    EXPECT_EQ(1, res[0].column_count());
//    EXPECT_EQ(12, res[0].get<int64_t>("key"));
//
//    EXPECT_EQ(1, res[1].column_count());
//    EXPECT_EQ(11, res[1].get<int64_t>("key"));
//}
//
//TEST_F(record_view_test, remove_all_empty) {
//    auto res = view.remove_all(nullptr, {});
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(record_view_test, remove_all_exact_nonexisting) {
//    auto res = view.remove_all_exact(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    ASSERT_EQ(2, res.size());
//}
//
//TEST_F(record_view_test, remove_all_exact_overlapped) {
//    auto res = view.insert_all(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")});
//
//    ASSERT_TRUE(res.empty());
//
//    res = view.remove_all_exact(nullptr, {get_tuple(1, "baz"), get_tuple(2, "bar")});
//
//    EXPECT_EQ(res.size(), 1);
//    EXPECT_EQ(2, res.front().column_count());
//    EXPECT_EQ(1, res.front().get<int64_t>("key"));
//    EXPECT_EQ("baz", res.front().get<std::string>("val"));
//
//    auto tuple2 = view.get(nullptr, get_tuple(2));
//
//    ASSERT_FALSE(tuple2.has_value());
//}
//
//TEST_F(record_view_test, remove_all_exact_overlapped_async) {
//    auto all_done = std::make_shared<std::promise<std::vector<ignite_tuple>>>();
//
//    view.insert_all_async(nullptr, {get_tuple(1, "foo"), get_tuple(2, "bar")}, [&](auto res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value().empty())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));
//
//        view.remove_all_exact_async(nullptr, {get_tuple(1, "baz"), get_tuple(2, "bar")},
//            [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    EXPECT_EQ(res_tuple.size(), 1);
//    EXPECT_EQ(2, res_tuple.front().column_count());
//    EXPECT_EQ(1, res_tuple.front().get<int64_t>("key"));
//    EXPECT_EQ("baz", res_tuple.front().get<std::string>("val"));
//}
//
//TEST_F(record_view_test, remove_all_exact_empty) {
//    auto res = view.remove_all_exact(nullptr, {});
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(record_view_test, types_test) {
//    auto table = m_client.get_tables().get_table(TABLE_NAME_ALL_COLUMNS);
//    view = table->get_record_binary_view();
//
//    ignite_tuple inserted{
//        {"key", std::int64_t(42)},
//        {"str", "test"},
//        {"int8", std::int8_t(1)},
//        {"int16", std::int16_t(2)},
//        {"int32", std::int32_t(3)},
//        {"int64", std::int64_t(4)},
//        {"float", .5f},
//        {"double", .6},
//        {"uuid", uuid(0x123e4567e89b12d3, 0x7456426614174000)},
//        {"date", ignite_date(2023, 2, 7)},
//        {"bitmask", bit_array(16, true)},
//        {"time", ignite_time(17, 4, 12, 3543634)},
//        {"time2", ignite_time(17, 4, 12, 3543634)},
//        {"datetime", ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879})},
//        {"datetime2", ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879})},
//        {"timestamp", ignite_timestamp(3875238472, 248760634)},
//        {"timestamp2", ignite_timestamp(3875238472, 248760634)},
//        {"blob", std::vector<std::byte>{std::byte(1), std::byte(2), std::byte(42)}},
//        {"decimal", big_decimal(123456789098765)},
//    };
//
//    view.upsert(nullptr, inserted);
//    auto res = view.get(nullptr, {{"key", std::int64_t(42)}});
//
//    ASSERT_TRUE(res.has_value());
//    ASSERT_EQ(res->column_count(), inserted.column_count());
//
//    for (int i = 0; i < res->column_count(); ++i) {
//        const auto &column = inserted.column_name(i);
//
//        if (column == "time2") {
//            EXPECT_EQ(res->get(column), primitive{ignite_time(17, 4, 12)});
//        } else if (column == "datetime2") {
//            EXPECT_EQ(res->get(column), primitive{ignite_date_time({2020, 7, 28}, {2, 15, 52, 6000000})});
//        } else if (column == "timestamp2") {
//            EXPECT_EQ(res->get(column), primitive{ignite_timestamp(3875238472, 248700000)});
//        } else {
//            EXPECT_EQ(res->get(column), inserted.get(column));
//        }
//    }
//}
//
