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

#include "all_fields_type.h"
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

    explicit test_key_type(std::int64_t key)
        : key(key) {}

    std::int64_t key{0};
};

/**
 * Test value type mapping (@see ignite_runner_suite::TABLE_1).
 */
struct test_value_type {
    test_value_type() = default;

    explicit test_value_type(std::string val)
        : val(std::move(val)) {}

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
test_key_type convert_from_tuple(ignite_tuple &&value) {
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
test_value_type convert_from_tuple(ignite_tuple &&value) {
    test_value_type res;

    res.val = value.get<std::string>("val");

    return res;
}

} // namespace ignite

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
        work_range.reserve(100);
        for (int i = -50; i < 50; ++i)
            work_range.emplace_back(i);

        kv_view.remove_all(nullptr, work_range);
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

    ASSERT_FALSE(res.empty());

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res.front(), std::nullopt);
}

TEST_F(key_value_view_test, put_all_empty_no_throw) {
    kv_view.put_all(nullptr, {});
}

TEST_F(key_value_view_test, put_all_get_all) {
    static constexpr std::int64_t records_num = 10;

    std::vector<std::pair<test_key_type, test_value_type>> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_key_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    kv_view.put_all(nullptr, records);
    auto res = kv_view.get_all(nullptr, keys);

    ASSERT_EQ(res.size(), keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        auto val = res[i];

        if (key.key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key.key;
            EXPECT_EQ("Val" + std::to_string(key.key), val->val);
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key.key << ", Res = " << val->val;
        }
    }
}

TEST_F(key_value_view_test, put_all_get_all_async) {
    static constexpr std::int64_t records_num = 10;

    std::vector<std::pair<test_key_type, test_value_type>> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_key_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    auto all_done = std::make_shared<std::promise<std::vector<std::optional<test_value_type>>>>();

    kv_view.put_all_async(nullptr, records, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        kv_view.get_all_async(nullptr, keys, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();

    ASSERT_EQ(res.size(), keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        auto val = res[i];

        if (key.key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key.key;
            EXPECT_EQ("Val" + std::to_string(key.key), val->val);
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key.key << ", Res = " << val->val;
        }
    }
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

        kv_view.get_and_put_async(nullptr, key, val2, [&](auto res) { result_set_promise(*first, std::move(res)); });
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

TEST_F(key_value_view_test, replace_nonexisting) {
    auto key = test_key_type(42);
    auto val = test_value_type("foo");

    auto res1 = kv_view.replace(nullptr, key, val);
    ASSERT_FALSE(res1);

    auto res2 = kv_view.get(nullptr, key);
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, replace_existing) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");

    auto res = kv_view.put_if_absent(nullptr, key, val1);
    ASSERT_TRUE(res);

    auto val2 = test_value_type("bar");
    res = kv_view.replace(nullptr, key, val2);
    ASSERT_TRUE(res);

    auto res2 = kv_view.get(nullptr, key);
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("bar", res2->val);
}

TEST_F(key_value_view_test, replace_existing_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto val2 = test_value_type("bar");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        kv_view.replace_async(nullptr, key, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(key_value_view_test, replace_exact_nonexisting) {
    auto res1 = kv_view.replace(nullptr, test_key_type(42), test_value_type("foo"), test_value_type("bar"));
    ASSERT_FALSE(res1);

    auto res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, replace_exact_existing_wrong) {
    auto res1 = kv_view.put_if_absent(nullptr, test_key_type(42), test_value_type("foo"));
    ASSERT_TRUE(res1);

    res1 = kv_view.replace(nullptr, test_key_type(42), test_value_type("bar"), test_value_type("baz"));
    ASSERT_FALSE(res1);

    auto res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("foo", res2->val);
}

TEST_F(key_value_view_test, replace_exact_existing_right) {
    auto res1 = kv_view.put_if_absent(nullptr, test_key_type(42), test_value_type("foo"));
    ASSERT_TRUE(res1);

    res1 = kv_view.replace(nullptr, test_key_type(42), test_value_type("foo"), test_value_type("baz"));
    ASSERT_TRUE(res1);

    auto res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("baz", res2->val);
}

TEST_F(key_value_view_test, replace_exact_existing_right_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto val2 = test_value_type("bar");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        kv_view.replace_async(nullptr, key, val1, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            kv_view.get_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(key_value_view_test, get_and_replace_nonexisting) {
    auto key = test_key_type(42);
    auto val = test_value_type("foo");
    auto res1 = kv_view.get_and_replace(nullptr, key, val);
    ASSERT_FALSE(res1.has_value());

    auto res2 = kv_view.get(nullptr, key);
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, get_and_replace_existing) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto res1 = kv_view.put_if_absent(nullptr, key, val1);
    ASSERT_TRUE(res1);

    auto val2 = test_value_type("bar");
    auto res2 = kv_view.get_and_replace(nullptr, key, val2);

    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("foo", res2->val);

    res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("bar", res2->val);
}

TEST_F(key_value_view_test, get_and_replace_existing_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");
    auto val2 = test_value_type("bar");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        kv_view.get_and_replace_async(
            nullptr, key, val2, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.val, res->val);
}

TEST_F(key_value_view_test, remove_nonexisting) {
    auto res1 = kv_view.remove(nullptr, test_key_type(1));
    ASSERT_FALSE(res1);

    auto res2 = kv_view.get(nullptr, test_key_type(1));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, remove_existing) {
    auto res = kv_view.put_if_absent(nullptr, test_key_type(1), test_value_type("foo"));
    ASSERT_TRUE(res);

    res = kv_view.remove(nullptr, test_key_type(1));
    ASSERT_TRUE(res);

    auto res2 = kv_view.get(nullptr, test_key_type(1));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, remove_existing_async) {
    auto all_done = std::make_shared<std::promise<bool>>();

    kv_view.put_if_absent_async(nullptr, test_key_type(42), test_value_type("foo"), [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        kv_view.remove_async(
            nullptr, test_key_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res);
}

TEST_F(key_value_view_test, remove_exact_nonexisting) {
    auto res = kv_view.remove(nullptr, test_key_type(1), test_value_type("foo"));
    ASSERT_FALSE(res);
}

TEST_F(key_value_view_test, remove_exact_existing) {
    auto res = kv_view.put_if_absent(nullptr, test_key_type(1), test_value_type("foo"));
    ASSERT_TRUE(res);

    res = kv_view.remove(nullptr, test_key_type(1), test_value_type("bar"));
    ASSERT_FALSE(res);

    res = kv_view.remove(nullptr, test_key_type(1), test_value_type("foo"));
    ASSERT_TRUE(res);

    auto res2 = kv_view.get(nullptr, test_key_type(1));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, remove_exact_existing_async) {
    auto key = test_key_type(42);
    auto val = test_value_type("foo");

    auto all_done = std::make_shared<std::promise<bool>>();

    auto res = kv_view.put_if_absent(nullptr, key, val);
    ASSERT_TRUE(res);

    kv_view.remove_async(nullptr, key, test_value_type("bar"), [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second remove")));

        kv_view.remove_async(nullptr, key, val, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res2 = all_done->get_future().get();
    ASSERT_TRUE(res2);
}

TEST_F(key_value_view_test, get_and_remove_nonexisting) {
    auto res1 = kv_view.get_and_remove(nullptr, test_key_type(42));
    ASSERT_FALSE(res1.has_value());

    auto res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, get_and_remove_existing) {
    auto res1 = kv_view.put_if_absent(nullptr, test_key_type(42), test_value_type("foo"));
    ASSERT_TRUE(res1);

    auto res2 = kv_view.get_and_remove(nullptr, test_key_type(42));

    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ("foo", res2->val);

    res2 = kv_view.get(nullptr, test_key_type(42));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(key_value_view_test, get_and_remove_existing_async) {
    auto key = test_key_type(42);
    auto val1 = test_value_type("foo");

    auto all_done = std::make_shared<std::promise<std::optional<test_value_type>>>();

    kv_view.put_if_absent_async(nullptr, key, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        kv_view.get_and_remove_async(nullptr, key, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.val, res->val);
}

TEST_F(key_value_view_test, contains) {
    auto res = kv_view.contains(nullptr, test_key_type(1));
    ASSERT_FALSE(res);

    res = kv_view.put_if_absent(nullptr, test_key_type(1), test_value_type("foo"));
    ASSERT_TRUE(res);

    res = kv_view.contains(nullptr, test_key_type(1));
    ASSERT_TRUE(res);
}

TEST_F(key_value_view_test, contains_async) {
    auto all_done = std::make_shared<std::promise<bool>>();

    kv_view.contains_async(nullptr, test_key_type(1), [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected to not contain")));

        kv_view.put_if_absent_async(nullptr, test_key_type(1), test_value_type("foo"), [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insert")));

            kv_view.contains_async(nullptr, test_key_type(1),
                [&](ignite_result<bool> &&res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res);
}

TEST_F(key_value_view_test, remove_all_nonexisting_keys_return_all) {
    std::vector<test_key_type> non_existing = {test_key_type(1), test_key_type(2)};
    auto res = kv_view.remove_all(nullptr, non_existing);

    EXPECT_EQ(res.size(), 2);

    EXPECT_EQ(1, res[0].key);
    EXPECT_EQ(2, res[1].key);
}

TEST_F(key_value_view_test, remove_all_only_existing) {
    std::vector<std::pair<test_key_type, test_value_type>> to_insert = {
        {test_key_type(1), test_value_type("foo")}, {test_key_type(2), test_value_type("bar")}};
    kv_view.put_all(nullptr, to_insert);

    auto res = kv_view.remove_all(nullptr, {test_key_type(1), test_key_type(2)});

    EXPECT_TRUE(res.empty());
}

TEST_F(key_value_view_test, remove_all_overlapped) {
    static constexpr std::size_t records_num = 10;

    std::vector<std::pair<test_key_type, test_value_type>> to_insert;
    to_insert.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        to_insert.emplace_back(i, "Val" + std::to_string(i));

    kv_view.put_all(nullptr, to_insert);

    std::vector<test_key_type> to_remove;
    for (std::int64_t i = 9; i < 13; ++i)
        to_remove.emplace_back(i);

    auto res = kv_view.remove_all(nullptr, to_remove);

    EXPECT_EQ(res.size(), 2);

    EXPECT_EQ(11, res[0].key);
    EXPECT_EQ(12, res[1].key);
}

TEST_F(key_value_view_test, remove_all_empty) {
    auto res = kv_view.remove_all(nullptr, std::vector<test_key_type>{});
    EXPECT_TRUE(res.empty());
}

TEST_F(key_value_view_test, remove_all_exact_nonexisting) {
    auto res = kv_view.remove_all(
        nullptr, {{test_key_type(1), test_value_type("foo")}, {test_key_type(2), test_value_type("bar")}});

    ASSERT_EQ(2, res.size());

    EXPECT_EQ(1, res[0].key);
    EXPECT_EQ(2, res[1].key);
}

TEST_F(key_value_view_test, remove_all_exact_overlapped) {
    kv_view.put_all(nullptr, {{test_key_type(1), test_value_type("foo")}, {test_key_type(2), test_value_type("bar")}});

    auto res = kv_view.remove_all(
        nullptr, {{test_key_type(1), test_value_type("baz")}, {test_key_type(2), test_value_type("bar")}});

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(1, res.front().key);

    auto tuple2 = kv_view.get(nullptr, test_key_type(2));

    ASSERT_FALSE(tuple2.has_value());
}

TEST_F(key_value_view_test, remove_all_exact_overlapped_async) {
    auto all_done = std::make_shared<std::promise<std::vector<test_key_type>>>();

    kv_view.put_all_async(nullptr,
        {{test_key_type(1), test_value_type("foo")}, {test_key_type(2), test_value_type("bar")}}, [&](auto res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            kv_view.remove_all_async(nullptr,
                {{test_key_type(1), test_value_type("baz")}, {test_key_type(2), test_value_type("bar")}},
                [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });

    auto res = all_done->get_future().get();
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(1, res.front().key);
}

TEST_F(key_value_view_test, remove_all_exact_empty) {
    auto res = kv_view.remove_all(nullptr, std::vector<std::pair<test_key_type, test_value_type>>{});
    EXPECT_TRUE(res.empty());
}

TEST_F(key_value_view_test, types_test) {
    auto table = m_client.get_tables().get_table(TABLE_NAME_ALL_COLUMNS);
    auto kv_view = table->get_key_value_view<test_key_type, all_fields_type>();

    all_fields_type inserted;
    inserted.m_key = 42;
    inserted.m_str = "test";
    inserted.m_int8 = 1;
    inserted.m_int16 = 2;
    inserted.m_int32 = 3;
    inserted.m_int64 = 4;
    inserted.m_float = .5f;
    inserted.m_double = .6f;
    inserted.m_uuid = {0x123e4567e89b12d3, 0x7456426614174000};
    inserted.m_date = {2023, 2, 7};
    inserted.m_bitmask = bit_array{16, true};
    // TODO: IGNITE-22504 Add nanoseconds to time/datetime/timestamp values
    inserted.m_time = {17, 4, 12, 354000000};
    inserted.m_time2 = {17, 4, 12, 354000000};
    inserted.m_datetime = {{2020, 7, 28}, {2, 15, 52, 634000000}};
    inserted.m_datetime2 = {{2020, 7, 28}, {2, 15, 52, 634000000}};
    inserted.m_timestamp = {3875238472, 248000000};
    inserted.m_timestamp2 = {3875238472, 248000000};
    inserted.m_blob = {std::byte(1), std::byte(2), std::byte(42)};
    inserted.m_decimal = big_decimal{123456789098765};
    inserted.m_boolean = true;

    kv_view.put(nullptr, test_key_type(42), inserted);
    auto res = kv_view.get(nullptr, test_key_type(42));

    ASSERT_TRUE(res.has_value());

    EXPECT_EQ(inserted.m_key, res->m_key);
    EXPECT_EQ(inserted.m_str, res->m_str);
    EXPECT_EQ(inserted.m_int8, res->m_int8);
    EXPECT_EQ(inserted.m_int16, res->m_int16);
    EXPECT_EQ(inserted.m_int32, res->m_int32);
    EXPECT_EQ(inserted.m_int64, res->m_int64);
    EXPECT_EQ(inserted.m_float, res->m_float);
    EXPECT_EQ(inserted.m_double, res->m_double);
    EXPECT_EQ(inserted.m_uuid, res->m_uuid);
    EXPECT_EQ(inserted.m_date, res->m_date);
    EXPECT_EQ(inserted.m_bitmask, res->m_bitmask);
    EXPECT_EQ(inserted.m_time, res->m_time);
    EXPECT_EQ(inserted.m_datetime, res->m_datetime);
    EXPECT_EQ(inserted.m_timestamp, res->m_timestamp);
    EXPECT_EQ(inserted.m_blob, res->m_blob);
    EXPECT_EQ(inserted.m_decimal, res->m_decimal);
    EXPECT_EQ(inserted.m_boolean, res->m_boolean);

    EXPECT_EQ(ignite_time(17, 4, 12), res->m_time2);
    EXPECT_EQ(ignite_date_time({2020, 7, 28}, {2, 15, 52, 6000000}), res->m_datetime2);
    EXPECT_EQ(ignite_timestamp(3875238472, 248000000), res->m_timestamp2);
}
