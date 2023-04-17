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
 * Test key type mapping (@see ignite_runner_suite::TABLE_1).
 */
struct test_key_type {
    test_key_type() = default;
    explicit test_key_type(std::int64_t key) : key(key) {}

    std::int64_t key{0};
};

/**
 * Test value type mapping (@see ignite_runner_suite::TABLE_1).
 */
struct test_value_type {
    test_value_type() = default;
    explicit test_value_type(std::string val) : val(std::move(val)) {}

    std::string val;
};


namespace ignite {

template<>
ignite_tuple convert_to_tuple(test_key_type &&value) {
    ignite_tuple tuple;

    tuple.set("key", value.key);

    return tuple;
}

template<>
test_key_type convert_from_tuple(ignite_tuple&& value) {
    test_key_type res;

    res.key = value.get<std::int64_t>("key");

    return res;
}

template<>
ignite_tuple convert_to_tuple(test_value_type &&value) {
    ignite_tuple tuple;

    tuple.set("val", value.val);

    return tuple;
}

template<>
test_value_type convert_from_tuple(ignite_tuple&& value) {
    test_value_type res;

    res.val = value.get<std::string>("val");

    return res;
}

}

/**
 * Test suite.
 */
class key_value_view_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
        auto table = m_client.get_tables().get_table(TABLE_1);

        kv_view = table->get_key_value_view<test_key_type, test_value_type>();
    }

    void TearDown() override {
        std::vector<test_key_type> work_range;
        work_range.reserve(200);
        for (int i = -100; i < 100; ++i)
            work_range.emplace_back(i);

//        kv_view.remove_all(nullptr, work_range);
    }

    /** Ignite client. */
    ignite_client m_client;

    /** Key-Value view. */
    key_value_view<test_key_type, test_value_type> kv_view;
};

TEST_F(key_value_view_test, put_get) {
    auto key = test_key_type(1);
    auto val = test_value_type("foo");

    kv_view.put(nullptr, key, val);
    auto res = kv_view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("foo", res->val);
}

TEST_F(key_value_view_test, put_get_async) {
    auto key = test_key_type(1);
    auto val = test_value_type("foo");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_async(nullptr, key, val, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("foo", res->val);
}

TEST_F(key_value_view_test, put_overrides_value) {
    auto key = test_key_type(1);
    auto val = test_value_type("foo");

    kv_view.put(nullptr, key, val);
    auto res = kv_view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("foo", res->val);

    val.val = "bar";
    kv_view.put(nullptr, key, val);
    res = kv_view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("bar", res->val);
}

TEST_F(key_value_view_test, get_all_empty) {
    auto res = kv_view.get_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(key_value_view_test, get_all_nonexisting) {
    auto res = kv_view.get_all(nullptr, {test_key_type(-42)});

    ASSERT_TRUE(res.empty());
}

TEST_F(key_value_view_test, put_all_empty_no_throw) {
    kv_view.put_all(nullptr, {});
}

TEST_F(key_value_view_test, put_all_get_all) {
    static constexpr std::size_t records_num = 10;

    std::vector<std::pair<test_key_type, test_value_type>> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_key_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    kv_view.put_all(nullptr, records);
    auto res = kv_view.get_all(nullptr, keys);

    // TODO: Key order should be preserved by the server (IGNITE-16004).
    EXPECT_EQ(res.size(), 2);

    ASSERT_TRUE(res[0].has_value());
    EXPECT_EQ("Val9", res[0]->val);

    ASSERT_TRUE(res[1].has_value());
    EXPECT_EQ("Val10", res[1]->val);
}

TEST_F(key_value_view_test, put_all_get_all_async) {
    static constexpr std::size_t records_num = 10;

    std::vector<std::pair<test_key_type, test_value_type>> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_key_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    auto all_done = std::make_shared<std::promise<std::vector<std::optional<test_value_type>>>>();

    kv_view.put_all_async(nullptr, records, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        // TODO: Key order should be preserved by the server (IGNITE-16004).
        kv_view.get_all_async(nullptr, keys, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();

    EXPECT_EQ(res.size(), 2);

    ASSERT_TRUE(res[0].has_value());
    EXPECT_EQ("Val9", res[0]->val);

    ASSERT_TRUE(res[1].has_value());
    EXPECT_EQ("Val10", res[1]->val);
}

TEST_F(key_value_view_test, get_and_put_new_record) {
    auto key = test_key_type(42);
    auto val = test_value_type("foo");
    auto res = kv_view.get_and_put(nullptr, key, val);

    ASSERT_FALSE(res.has_value());
}

TEST_F(key_value_view_test, get_and_put_existing_record) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto res = kv_view.get_and_put(nullptr, key, val1);

    ASSERT_FALSE(res.has_value());

    auto val2 = test_value_type("bar");
    res = kv_view.get_and_put(nullptr, key, val2);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("foo", res->val);

    res = kv_view.get(nullptr, key);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("bar", res->val);
}

TEST_F(key_value_view_test, get_and_put_existing_record_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto val2 = test_value_type("bar");

    auto first = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.get_and_put_async(nullptr, key, val1, [&](auto res) {
        if (!check_and_set_operation_error(*first, res))
            return;

        if (res.value().has_value())
            first->set_exception(std::make_exception_ptr(ignite_error("Expected nullopt on first insertion")));

        kv_view.get_and_put_async(
            nullptr, key, val2, [&](auto res) { result_set_promise(*first, std::move(res)); });
    });

    auto res = first->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.val, res->val);

    auto second = std::make_shared<std::promise<std::optional<test_value_type>>>();
    kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*second, std::move(res)); });

    res = second->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(key_value_view_test, insert_new_record) {
    auto key = test_key_type(42);
    auto val = test_value_type("foo");
    auto res = kv_view.put_if_absent(nullptr, key, val);

    ASSERT_TRUE(res);
}

TEST_F(key_value_view_test, put_if_absent_existing_record) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto put_res = kv_view.put_if_absent(nullptr, key, val1);
    ASSERT_TRUE(put_res);

    auto val2 = test_value_type("bar");
    put_res = kv_view.put_if_absent(nullptr, key, val2);
    ASSERT_FALSE(put_res);

    auto res = kv_view.get(nullptr, key);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ("foo", res->val);
}

TEST_F(key_value_view_test, put_if_absent_existing_record_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto val2 = test_value_type("bar");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on first insertion")));

        kv_view.put_if_absent_async(nullptr, key, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second insertion")));

            kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.val, res->val);
}

//TEST_F(key_value_view_test, replace_nonexisting) {
//    auto key = test_key_type(42);
//    auto val = test_value_type("foo");
//
//    auto res = kv_view.replace(nullptr, key, val);
//    ASSERT_FALSE(res);
//
//    auto res = kv_view.get(nullptr, key);
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, replace_existing) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//
//    auto res = kv_view.put_if_absent(nullptr, key, val1);
//    ASSERT_TRUE(res);
//
//    auto val2 = test_value_type("bar");
//    res = kv_view.replace(nullptr, key, val2);
//    ASSERT_TRUE(res);
//
//    auto res = kv_view.get(nullptr, key);
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(key_value_view_test, replace_existing_async) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//    auto val2 = test_value_type("bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();
//
//    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        kv_view.replace_async(nullptr, key, val2, [&](ignite_result<bool> &&res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (!res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));
//
//            kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ(val2.val, res->val);
//}
//
//TEST_F(key_value_view_test, replace_empty_key_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.replace(nullptr, {}, get_tuple("foo"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, replace_empty_value_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.replace(nullptr, test_key_type(1), {});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Value tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, replace_exact_nonexisting) {
//    auto res = kv_view.replace(nullptr, test_key_type(42), test_value_type("foo"), test_value_type("bar"));
//
//    ASSERT_FALSE(res);
//
//    auto res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, replace_exact_existing_wrong) {
//    auto res = kv_view.put_if_absent(nullptr, test_key_type(42), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    res = kv_view.replace(nullptr, test_key_type(42), test_value_type("bar"), get_tuple("baz"));
//    ASSERT_FALSE(res);
//
//    auto res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//}
//
//TEST_F(key_value_view_test, replace_exact_existing_right) {
//    auto res = kv_view.put_if_absent(nullptr, test_key_type(42), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    res = kv_view.replace(nullptr, test_key_type(42), test_value_type("foo"), get_tuple("baz"));
//    ASSERT_TRUE(res);
//
//    auto res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("baz", res->val);
//}
//
//TEST_F(key_value_view_test, replace_exact_existing_right_async) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//    auto val2 = test_value_type("bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();
//
//    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        kv_view.replace_async(nullptr, key, val1, val2, [&](ignite_result<bool> &&res) {
//            if (!check_and_set_operation_error(*all_done, res))
//                return;
//
//            if (!res.value())
//                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));
//
//            kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//        });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ(val2.val, res->val);
//}
//
//TEST_F(key_value_view_test, replace_exact_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.replace(nullptr, {}, test_value_type("foo"), test_value_type("bar"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.replace(nullptr, test_key_type(1), {}, test_value_type("bar"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Value tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.replace(nullptr, test_key_type(1), test_value_type("foo"), {});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Value tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, get_and_replace_nonexisting) {
//    auto key = test_key_type(42);
//    auto val = test_value_type("foo");
//    auto res = kv_view.get_and_replace(nullptr, key, val);
//
//    ASSERT_FALSE(res.has_value());
//
//    auto res = kv_view.get(nullptr, key);
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, get_and_replace_existing) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//    auto res = kv_view.put_if_absent(nullptr, key, val1);
//    ASSERT_TRUE(res);
//
//    auto val2 = test_value_type("bar");
//    auto res = kv_view.get_and_replace(nullptr, key, val2);
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//
//    res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("bar", res->val);
//}
//
//TEST_F(key_value_view_test, get_and_replace_existing_async) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//    auto val2 = test_value_type("bar");
//
//    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();
//
//    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        kv_view.get_and_replace_async(
//            nullptr, key, val2, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ(val1.val, res->val);
//}
//
//TEST_F(key_value_view_test, get_and_replace_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                (void) kv_view.get_and_replace(nullptr, test_key_type(1), {});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Value tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//
//    EXPECT_THROW(
//        {
//            try {
//                (void) kv_view.get_and_replace(nullptr, {}, get_tuple("foo"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, remove_nonexisting) {
//    auto res = kv_view.remove(nullptr, get_tuple(1));
//    ASSERT_FALSE(res);
//
//    auto res = kv_view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, remove_existing) {
//    auto res = kv_view.put_if_absent(nullptr, test_key_type(1), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    res = kv_view.remove(nullptr, get_tuple(1));
//    ASSERT_TRUE(res);
//
//    auto res = kv_view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, remove_existing_async) {
//    auto all_done = std::make_shared<std::promise<bool>>();
//
//    kv_view.put_if_absent_async(nullptr, test_key_type(42), test_value_type("foo"), [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        kv_view.remove_async(nullptr, test_key_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res);
//}
//
//TEST_F(key_value_view_test, remove_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.remove(nullptr, ignite_tuple{});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, remove_exact_nonexisting) {
//    auto res = kv_view.remove(nullptr, test_key_type(1), get_tuple("foo"));
//    ASSERT_FALSE(res);
//}
//
//TEST_F(key_value_view_test, remove_exact_existing) {
//    auto res = kv_view.put_if_absent(nullptr, test_key_type(1), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    res = kv_view.remove(nullptr, test_key_type(1), test_value_type("bar"));
//    ASSERT_FALSE(res);
//
//    res = kv_view.remove(nullptr, test_key_type(1), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    auto res = kv_view.get(nullptr, get_tuple(1));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, remove_exact_existing_async) {
//    auto key = test_key_type(42);
//    auto val = test_value_type("foo");
//
//    auto all_done = std::make_shared<std::promise<bool>>();
//
//    auto res = kv_view.put_if_absent(nullptr, key, val);
//    ASSERT_TRUE(res);
//
//    kv_view.remove_async(nullptr, key, test_value_type("bar"), [&](auto res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second remove")));
//
//        kv_view.remove_async(
//            nullptr, key, val, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res);
//}
//
//TEST_F(key_value_view_test, remove_exact_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.remove(nullptr, {}, get_tuple("foo"));
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//
//    EXPECT_THROW(
//        {
//            try {
//                kv_view.remove(nullptr, test_key_type(1), {});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Value tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, get_and_remove_nonexisting) {
//    auto res = kv_view.get_and_replace(nullptr, test_key_type(42), get_tuple("foo"));
//    ASSERT_FALSE(res.has_value());
//
//    auto res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, get_and_remove_existing) {
//    auto res = kv_view.put_if_absent(nullptr, test_key_type(42), get_tuple("foo"));
//    ASSERT_TRUE(res);
//
//    auto res = kv_view.get_and_remove(nullptr, test_key_type(42));
//
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ("foo", res->val);
//
//    res = kv_view.get(nullptr, test_key_type(42));
//    ASSERT_FALSE(res.has_value());
//}
//
//TEST_F(key_value_view_test, get_and_remove_existing_async) {
//    auto key = test_key_type(42);
//    auto val1 = test_value_type("foo");
//
//    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();
//
//    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        if (!res.value())
//            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));
//
//        kv_view.get_and_remove_async(
//            nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    ASSERT_TRUE(res.has_value());
//    EXPECT_EQ(2, res->column_count());
//    EXPECT_EQ(42, res->key);
//    EXPECT_EQ(val1.val, res->val);
//}
//
//TEST_F(key_value_view_test, get_and_remove_empty_throws) {
//    EXPECT_THROW(
//        {
//            try {
//                (void) kv_view.get_and_remove(nullptr, {});
//            } catch (const ignite_error &e) {
//                EXPECT_STREQ("Key tuple can not be empty", e.what());
//                throw;
//            }
//        },
//        ignite_error);
//}
//
//TEST_F(key_value_view_test, remove_all_nonexisting_keys_return_all) {
//    std::vector<ignite_tuple> non_existing = {test_key_type(1), get_tuple(2)};
//    auto res = kv_view.remove_all(nullptr, non_existing);
//
//    EXPECT_EQ(res.size(), 2);
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    EXPECT_EQ(1, res[0].column_count());
//    EXPECT_EQ(2, res[0].key);
//
//    EXPECT_EQ(1, res[1].column_count());
//    EXPECT_EQ(1, res[1].key);
//}
//
//TEST_F(key_value_view_test, remove_all_only_existing) {
//    std::vector<std::pair<ignite_tuple, ignite_tuple>> to_insert = {
//        {test_key_type(1), test_value_type("foo")}, {get_tuple(2), test_value_type("bar")}};
//    kv_view.put_all(nullptr, to_insert);
//
//    auto res = kv_view.remove_all(nullptr, {test_key_type(1), get_tuple(2)});
//
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(key_value_view_test, remove_all_overlapped) {
//    static constexpr std::size_t records_num = 10;
//
//    std::vector<std::pair<ignite_tuple, ignite_tuple>> to_insert;
//    to_insert.reserve(records_num);
//    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
//        to_insert.emplace_back(get_tuple(i), get_tuple("Val" + std::to_string(i)));
//
//    kv_view.put_all(nullptr, to_insert);
//
//    std::vector<ignite_tuple> to_remove;
//    for (std::int64_t i = 9; i < 13; ++i)
//        to_remove.emplace_back(get_tuple(i));
//
//    auto res = kv_view.remove_all(nullptr, to_remove);
//
//    EXPECT_EQ(res.size(), 2);
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    EXPECT_EQ(1, res[0].column_count());
//    EXPECT_EQ(12, res[0].key);
//
//    EXPECT_EQ(1, res[1].column_count());
//    EXPECT_EQ(11, res[1].key);
//}
//
//TEST_F(key_value_view_test, remove_all_empty) {
//    auto res = kv_view.remove_all(nullptr, std::vector<ignite_tuple>{});
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(key_value_view_test, remove_all_exact_nonexisting) {
//    auto res = kv_view.remove_all(nullptr, {{test_key_type(1), test_value_type("foo")}, {get_tuple(2), test_value_type("bar")}});
//
//    // TODO: Key order should be preserved by the server (IGNITE-16004).
//    ASSERT_EQ(2, res.size());
//}
//
//TEST_F(key_value_view_test, remove_all_exact_overlapped) {
//    kv_view.put_all(nullptr, {{test_key_type(1), test_value_type("foo")}, {get_tuple(2), test_value_type("bar")}});
//
//    auto res = kv_view.remove_all(nullptr, {{test_key_type(1), get_tuple("baz")}, {get_tuple(2), test_value_type("bar")}});
//
//    EXPECT_EQ(res.size(), 1);
//    EXPECT_EQ(2, res.front().column_count());
//    EXPECT_EQ(1, res.front().key);
//    EXPECT_EQ("baz", res.front().val);
//
//    auto tuple2 = kv_view.get(nullptr, get_tuple(2));
//
//    ASSERT_FALSE(tuple2.has_value());
//}
//
//TEST_F(key_value_view_test, remove_all_exact_overlapped_async) {
//    auto all_done = std::make_shared<std::promise<std::vector<ignite_tuple>>>();
//
//    kv_view.put_all_async(nullptr, {{test_key_type(1), test_value_type("foo")}, {get_tuple(2), test_value_type("bar")}}, [&](auto res) {
//        if (!check_and_set_operation_error(*all_done, res))
//            return;
//
//        kv_view.remove_all_async(nullptr, {{test_key_type(1), get_tuple("baz")}, {get_tuple(2), test_value_type("bar")}},
//            [&](auto res) { result_set_promise(*all_done, std::move(res)); });
//    });
//
//    auto res = all_done->get_future().get();
//    EXPECT_EQ(res.size(), 1);
//    EXPECT_EQ(2, res.front().column_count());
//    EXPECT_EQ(1, res.front().key);
//    EXPECT_EQ("baz", res.front().val);
//}
//
//TEST_F(key_value_view_test, remove_all_exact_empty) {
//    auto res = kv_view.remove_all(nullptr, std::vector<std::pair<ignite_tuple, ignite_tuple>>{});
//    EXPECT_TRUE(res.empty());
//}
//
//TEST_F(key_value_view_test, types_test) {
//    auto table = m_client.get_tables().get_table(TABLE_NAME_ALL_COLUMNS);
//    kv_view = table->get_key_value_binary_view();
//
//    ignite_tuple inserted{
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
//    kv_view.put(nullptr, test_key_type(42), inserted);
//    auto res = kv_view.get(nullptr, test_key_type(42));
//
//    ASSERT_TRUE(res.has_value());
//    ASSERT_EQ(res->column_count(), inserted.column_count() + 1);
//
//    EXPECT_EQ(42, res->get<std::int64_t>("key"));
//
//    for (int i = 0; i < inserted.column_count(); ++i) {
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
