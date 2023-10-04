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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

/**
 * Test table type mapping (@see ignite_runner_suite::TABLE_1).
 */
struct test_type {
    test_type() = default;

    explicit test_type(std::int64_t key)
        : key(key) {}

    explicit test_type(std::string val)
        : val(std::move(val)) {}

    explicit test_type(std::int64_t key, std::string val)
        : key(key)
        , val(std::move(val)) {}

    std::int64_t key{0};
    std::string val;
};

struct missing_mapping_key {
    missing_mapping_key() = default;

    explicit missing_mapping_key(std::int64_t key)
        : key(key) {}

    explicit missing_mapping_key(std::string val)
        : val(std::move(val)) {}

    explicit missing_mapping_key(std::int64_t key, std::string val)
        : key(key)
        , val(std::move(val)) {}

    std::int64_t key{0};
    std::string val;
};

struct missing_mapping_value {
    missing_mapping_value() = default;

    explicit missing_mapping_value(std::int64_t key)
        : key(key) {}

    explicit missing_mapping_value(std::string val)
        : val(std::move(val)) {}

    explicit missing_mapping_value(std::int64_t key, std::string val)
        : key(key)
        , val(std::move(val)) {}

    std::int64_t key{0};
    std::string val;
};

struct extra_mapping_value {
    extra_mapping_value() = default;

    explicit extra_mapping_value(std::int64_t key)
        : key(key) {}

    explicit extra_mapping_value(std::string val)
        : val(std::move(val)) {}

    explicit extra_mapping_value(std::int64_t key, std::string val)
        : key(key)
        , val(std::move(val)) {}

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
test_type convert_from_tuple(ignite_tuple &&value) {
    test_type res;

    res.key = value.get<std::int64_t>("key");

    if (value.column_count() > 1)
        res.val = value.get<std::string>("val");

    return res;
}

template<>
ignite_tuple convert_to_tuple(missing_mapping_key &&value) {
    ignite_tuple tuple;

    tuple.set("val", value.val);

    return tuple;
}

template<>
missing_mapping_key convert_from_tuple(ignite_tuple &&value) {
    missing_mapping_key res;

    res.val = value.get<std::string>("val");

    return res;
}

template<>
ignite_tuple convert_to_tuple(missing_mapping_value &&value) {
    ignite_tuple tuple;

    tuple.set("key", value.key);

    return tuple;
}

template<>
missing_mapping_value convert_from_tuple(ignite_tuple &&value) {
    missing_mapping_value res;

    res.key = value.get<std::int64_t>("key");

    return res;
}

template<>
ignite_tuple convert_to_tuple(extra_mapping_value &&value) {
    ignite_tuple tuple;

    tuple.set("key", value.key);
    tuple.set("val", value.val);
    tuple.set("extra", std::string("extra"));

    return tuple;
}

template<>
extra_mapping_value convert_from_tuple(ignite_tuple &&value) {
    extra_mapping_value res;

    res.key = value.get<std::int64_t>("key");

    if (value.column_count() > 1) {
        res.val = value.get<std::string>("val");
        (void) value.get<std::string>("extra");
    }

    return res;
}

} // namespace ignite

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

        view.remove_all(nullptr, work_range);
    }

    /** Ignite client. */
    ignite_client m_client;

    /** Record binary view. */
    record_view<test_type> view;
};

TEST_F(record_view_test, missing_mapping_key_throws) {
    {
        test_type val{1, "foo"};
        view.upsert(nullptr, val);
    }

    auto table = m_client.get_tables().get_table(TABLE_1);
    auto wrong_view = table->get_record_view<missing_mapping_key>();

    missing_mapping_key key{1};
    missing_mapping_key val{1, "foo"};

    EXPECT_THROW(
        {
            try {
                wrong_view.upsert(nullptr, val);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Missed key column: KEY"));
                throw;
            }
        },
        ignite_error);

    EXPECT_THROW(
        {
            try {
                (void) wrong_view.get(nullptr, key);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Missed key column: KEY"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_view_test, extra_mapping_value_throws) {
    auto table = m_client.get_tables().get_table(TABLE_1);
    auto wrong_view = table->get_record_view<extra_mapping_value>();

    extra_mapping_value key{1};
    extra_mapping_value val{1, "foo"};

    EXPECT_THROW(
        {
            try {
                wrong_view.upsert(nullptr, val);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::MatchesRegex(
                    "Key tuple doesn't match schema: schemaVersion=.+, extraColumns=extra"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(record_view_test, missing_mapped_value_dont_throw) {
    {
        test_type val{1, "foo"};
        view.upsert(nullptr, val);
    }

    auto table = m_client.get_tables().get_table(TABLE_1);
    auto wrong_view = table->get_record_view<missing_mapping_value>();

    missing_mapping_value key{1};
    missing_mapping_value val{1, "foo"};

    // Should work just fine. Having additional field and not providing value field is not an error.
    wrong_view.upsert(nullptr, val);

    auto actual = wrong_view.get(nullptr, key);
    EXPECT_TRUE(actual->val.empty());
}

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

    ASSERT_FALSE(res.empty());

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(res.front(), std::nullopt);
}

TEST_F(record_view_test, upsert_all_empty_no_throw) {
    view.upsert_all(nullptr, {});
}

TEST_F(record_view_test, upsert_all_get_all) {
    static constexpr std::int64_t records_num = 10;

    std::vector<test_type> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    view.upsert_all(nullptr, records);
    auto res = view.get_all(nullptr, keys);

    ASSERT_EQ(res.size(), keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        auto val = res[i];

        if (key.key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key.key;
            EXPECT_EQ(key.key, val->key);
            EXPECT_EQ("Val" + std::to_string(key.key), val->val);
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key.key << ", Res = " << val->val;
        }
    }
}

TEST_F(record_view_test, upsert_all_get_all_async) {
    static constexpr std::int64_t records_num = 10;

    std::vector<test_type> records;
    records.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + records_num; ++i)
        records.emplace_back(i, "Val" + std::to_string(i));

    std::vector<test_type> keys;
    for (std::int64_t i = 9; i < 13; ++i)
        keys.emplace_back(i);

    auto all_done = std::make_shared<std::promise<std::vector<std::optional<test_type>>>>();

    view.upsert_all_async(nullptr, records, [&](ignite_result<void> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        view.get_all_async(nullptr, keys, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();

    ASSERT_EQ(res.size(), keys.size());

    for (std::size_t i = 0; i < keys.size(); ++i) {
        auto key = keys[i];
        auto val = res[i];

        if (key.key <= records_num) {
            ASSERT_TRUE(val.has_value()) << "Key = " << key.key;
            EXPECT_EQ(key.key, val->key);
            EXPECT_EQ("Val" + std::to_string(key.key), val->val);
        } else {
            ASSERT_FALSE(val.has_value()) << "Key = " << key.key << ", Res = " << val->val;
        }
    }
}

TEST_F(record_view_test, get_and_upsert_new_record) {
    test_type val{42, "foo"};
    auto res = view.get_and_upsert(nullptr, val);

    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, get_and_upsert_existing_record) {
    test_type val1{42, "foo"};
    auto res = view.get_and_upsert(nullptr, val1);

    ASSERT_FALSE(res.has_value());

    test_type val2{42, "bar"};
    res = view.get_and_upsert(nullptr, val2);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("foo", res->val);

    res = view.get(nullptr, test_type(42));

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("bar", res->val);
}

TEST_F(record_view_test, get_and_upsert_existing_record_async) {
    test_type val1{42, "foo"};
    test_type val2{42, "bar"};

    auto first = std::make_shared<std::promise<std::optional<test_type>>>();

    view.get_and_upsert_async(nullptr, val1, [&](auto res) {
        if (!check_and_set_operation_error(*first, res))
            return;

        if (res.value().has_value())
            first->set_exception(std::make_exception_ptr(ignite_error("Expected nullopt on first insertion")));

        view.get_and_upsert_async(nullptr, val2, [&](auto res) { result_set_promise(*first, std::move(res)); });
    });

    auto res = first->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.key, res->key);
    EXPECT_EQ(val1.val, res->val);

    auto second = std::make_shared<std::promise<std::optional<test_type>>>();
    view.get_async(nullptr, test_type(42), [&](auto res) { result_set_promise(*second, std::move(res)); });

    res = second->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.key, res->key);
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(record_view_test, insert_new_record) {
    test_type val{42, "foo"};
    auto res = view.insert(nullptr, val);

    ASSERT_TRUE(res);
}

TEST_F(record_view_test, insert_existing_record) {
    test_type val1{42, "foo"};
    auto inserted = view.insert(nullptr, val1);
    ASSERT_TRUE(inserted);

    test_type val2{42, "bar"};
    inserted = view.insert(nullptr, val2);
    ASSERT_FALSE(inserted);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("foo", res->val);
}

TEST_F(record_view_test, insert_existing_record_async) {
    test_type val1{42, "foo"};
    test_type val2{42, "bar"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on first insertion")));

        view.insert_async(nullptr, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second insertion")));

            view.get_async(nullptr, test_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.key, res->key);
    EXPECT_EQ(val1.val, res->val);
}

TEST_F(record_view_test, insert_all_new) {
    static constexpr std::size_t records_num = 10;

    std::vector<test_type> keys;
    std::vector<test_type> records;
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i) {
        keys.emplace_back(i);
        records.emplace_back(i, "Val" + std::to_string(i));
    }

    auto insert_res = view.insert_all(nullptr, records);

    ASSERT_TRUE(insert_res.empty());

    auto res = view.get_all(nullptr, keys);

    EXPECT_EQ(res.size(), 10);
    ASSERT_TRUE(std::all_of(res.begin(), res.end(), [](const auto &elem) { return elem.has_value(); }));
}

TEST_F(record_view_test, insert_all_overlapped) {
    auto res = view.insert_all(nullptr, {test_type(1, "foo"), test_type(2, "bar")});

    ASSERT_TRUE(res.empty());

    res = view.insert_all(nullptr, {test_type(2, "baz"), test_type(3, "bar")});

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(2, res.front().key);
    EXPECT_EQ("baz", res.front().val);

    auto res2 = view.get(nullptr, test_type(2));

    ASSERT_TRUE(res2.has_value());
    EXPECT_EQ(2, res2->key);
    EXPECT_EQ("bar", res2->val);
}

TEST_F(record_view_test, insert_all_overlapped_async) {
    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_all_async(nullptr, {test_type(1, "foo"), test_type(2, "bar")}, [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value().empty())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));

        view.insert_all_async(nullptr, {test_type(1, "foo"), test_type(2, "baz"), test_type(3, "bar")}, [&](auto res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value().size() != 2)
                all_done->set_exception(std::make_exception_ptr(
                    ignite_error("Expected 2 on second insertion but got " + std::to_string(res.value().size()))));

            view.get_async(nullptr, test_type(2), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(2, res->key);
    EXPECT_EQ("bar", res->val);
}

TEST_F(record_view_test, insert_all_empty) {
    auto res = view.insert_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_view_test, replace_nonexisting) {
    test_type val{42, "foo"};
    auto replaced = view.replace(nullptr, val);

    ASSERT_FALSE(replaced);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, replace_existing) {
    test_type val1{42, "foo"};
    auto replaced = view.insert(nullptr, val1);
    ASSERT_TRUE(replaced);

    test_type val2{42, "bar"};
    replaced = view.replace(nullptr, val2);
    ASSERT_TRUE(replaced);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("bar", res->val);
}

TEST_F(record_view_test, replace_existing_async) {
    test_type val1{42, "foo"};
    test_type val2{42, "bar"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        view.replace_async(nullptr, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            view.get_async(nullptr, test_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.key, res->key);
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(record_view_test, replace_exact_nonexisting) {
    auto replaced = view.replace(nullptr, test_type(42, "foo"), test_type(42, "bar"));

    ASSERT_FALSE(replaced);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, replace_exact_existing_wrong) {
    auto inserted = view.insert(nullptr, test_type(42, "foo"));
    ASSERT_TRUE(inserted);

    auto replaced = view.replace(nullptr, test_type(42, "bar"), test_type(42, "baz"));
    ASSERT_FALSE(replaced);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("foo", res->val);
}

TEST_F(record_view_test, replace_exact_existing_right) {
    auto inserted = view.insert(nullptr, test_type(42, "foo"));
    ASSERT_TRUE(inserted);

    auto replaced = view.replace(nullptr, test_type(42, "foo"), test_type(42, "baz"));
    ASSERT_TRUE(replaced);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("baz", res->val);
}

TEST_F(record_view_test, replace_exact_existing_right_async) {
    test_type val1{42, "foo"};
    test_type val2{42, "bar"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        view.replace_async(nullptr, val1, val2, [&](ignite_result<bool> &&res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (!res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on replace")));

            view.get_async(nullptr, test_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val2.key, res->key);
    EXPECT_EQ(val2.val, res->val);
}

TEST_F(record_view_test, get_and_replace_nonexisting) {
    test_type val{42, "foo"};
    auto res1 = view.get_and_replace(nullptr, val);

    ASSERT_FALSE(res1.has_value());

    auto res2 = view.get(nullptr, test_type(42));
    ASSERT_FALSE(res2.has_value());
}

TEST_F(record_view_test, get_and_replace_existing) {
    test_type val1{42, "foo"};
    auto inserted = view.insert(nullptr, val1);
    ASSERT_TRUE(inserted);

    test_type val2{42, "bar"};
    auto res = view.get_and_replace(nullptr, val2);

    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("foo", res->val);

    res = view.get(nullptr, test_type(42));
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(42, res->key);
    EXPECT_EQ("bar", res->val);
}

TEST_F(record_view_test, get_and_replace_existing_async) {
    test_type val1{42, "foo"};
    test_type val2{42, "bar"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        view.get_and_replace_async(nullptr, val2, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.key, res->key);
    EXPECT_EQ(val1.val, res->val);
}

TEST_F(record_view_test, remove_nonexisting) {
    auto removed = view.remove(nullptr, test_type(1));
    ASSERT_FALSE(removed);

    auto res = view.get(nullptr, test_type(1));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, remove_existing) {
    auto inserted = view.insert(nullptr, test_type(1, "foo"));
    ASSERT_TRUE(inserted);

    auto removed = view.remove(nullptr, test_type(1));
    ASSERT_TRUE(removed);

    auto res = view.get(nullptr, test_type(1));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, remove_existing_async) {
    auto all_done = std::make_shared<std::promise<bool>>();

    view.insert_async(nullptr, test_type(42, "foo"), [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        view.remove_async(nullptr, test_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res);
}

TEST_F(record_view_test, remove_exact_nonexisting) {
    auto res = view.remove_exact(nullptr, test_type(1, "foo"));
    ASSERT_FALSE(res);
}

TEST_F(record_view_test, remove_exact_existing) {
    auto inserted = view.insert(nullptr, test_type(1, "foo"));
    ASSERT_TRUE(inserted);

    auto removed = view.remove_exact(nullptr, test_type(1));
    ASSERT_FALSE(removed);

    removed = view.remove_exact(nullptr, test_type(1, "bar"));
    ASSERT_FALSE(removed);

    removed = view.remove_exact(nullptr, test_type(1, "foo"));
    ASSERT_TRUE(removed);

    auto res = view.get(nullptr, test_type(1));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, remove_exact_existing_async) {
    test_type val{42, "foo"};

    auto all_done = std::make_shared<std::promise<bool>>();

    auto inserted = view.insert(nullptr, val);
    ASSERT_TRUE(inserted);

    view.remove_exact_async(nullptr, test_type(42), [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on first remove")));

        view.remove_exact_async(nullptr, test_type(42, "bar"), [&](auto res) {
            if (!check_and_set_operation_error(*all_done, res))
                return;

            if (res.value())
                all_done->set_exception(std::make_exception_ptr(ignite_error("Expected false on second remove")));

            view.remove_exact_async(nullptr, val, [&](auto res) { result_set_promise(*all_done, std::move(res)); });
        });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res);
}

TEST_F(record_view_test, get_and_remove_nonexisting) {
    auto removed = view.get_and_remove(nullptr, test_type(42, "foo"));
    ASSERT_FALSE(removed.has_value());

    auto res = view.get(nullptr, test_type(42));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, get_and_remove_existing) {
    auto inserted = view.insert(nullptr, test_type(42, "foo"));
    ASSERT_TRUE(inserted);

    auto removed = view.get_and_remove(nullptr, test_type(42));

    ASSERT_TRUE(removed.has_value());
    EXPECT_EQ(42, removed->key);
    EXPECT_EQ("foo", removed->val);

    auto res = view.get(nullptr, test_type(42));
    ASSERT_FALSE(res.has_value());
}

TEST_F(record_view_test, get_and_remove_existing_async) {
    test_type val1{42, "foo"};

    auto all_done = std::make_shared<std::promise<std::optional<test_type>>>();

    view.insert_async(nullptr, val1, [&](ignite_result<bool> &&res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected true on insertion")));

        view.get_and_remove_async(
            nullptr, test_type(42), [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(val1.key, res->key);
    EXPECT_EQ(val1.val, res->val);
}

TEST_F(record_view_test, remove_all_nonexisting_keys_return_all) {
    std::vector<test_type> non_existing = {test_type(1), test_type(2)};
    auto res = view.remove_all(nullptr, non_existing);

    EXPECT_EQ(res.size(), 2);

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    EXPECT_EQ(2, res[0].key);
    EXPECT_EQ(1, res[1].key);
}

TEST_F(record_view_test, remove_all_only_existing) {
    std::vector<test_type> to_insert = {test_type(1, "foo"), test_type(2, "bar")};
    view.upsert_all(nullptr, to_insert);

    auto res = view.remove_all(nullptr, {test_type(1), test_type(2)});

    EXPECT_TRUE(res.empty());
}

TEST_F(record_view_test, remove_all_overlapped) {
    static constexpr std::size_t records_num = 10;

    std::vector<test_type> to_insert;
    to_insert.reserve(records_num);
    for (std::int64_t i = 1; i < 1 + std::int64_t(records_num); ++i)
        to_insert.emplace_back(i, "Val" + std::to_string(i));

    view.upsert_all(nullptr, to_insert);

    std::vector<test_type> to_remove;
    for (std::int64_t i = 9; i < 13; ++i)
        to_remove.emplace_back(i);

    auto res = view.remove_all(nullptr, to_remove);

    EXPECT_EQ(res.size(), 2);

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    EXPECT_EQ(12, res[0].key);
    EXPECT_EQ(11, res[1].key);
}

TEST_F(record_view_test, remove_all_empty) {
    auto res = view.remove_all(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_view_test, remove_all_exact_nonexisting) {
    auto res = view.remove_all_exact(nullptr, {test_type(1, "foo"), test_type(2, "bar")});

    // TODO: Key order should be preserved by the server (IGNITE-20435).
    ASSERT_EQ(2, res.size());
}

TEST_F(record_view_test, remove_all_exact_overlapped) {
    auto res = view.insert_all(nullptr, {test_type(1, "foo"), test_type(2, "bar")});

    ASSERT_TRUE(res.empty());

    res = view.remove_all_exact(nullptr, {test_type(1, "baz"), test_type(2, "bar")});

    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(1, res.front().key);
    EXPECT_EQ("baz", res.front().val);

    auto res2 = view.get(nullptr, test_type(2));

    ASSERT_FALSE(res2.has_value());
}

TEST_F(record_view_test, remove_all_exact_overlapped_async) {
    auto all_done = std::make_shared<std::promise<std::vector<test_type>>>();

    view.insert_all_async(nullptr, {test_type(1, "foo"), test_type(2, "bar")}, [&](auto res) {
        if (!check_and_set_operation_error(*all_done, res))
            return;

        if (!res.value().empty())
            all_done->set_exception(std::make_exception_ptr(ignite_error("Expected empty return on first insertion")));

        view.remove_all_exact_async(nullptr, {test_type(1, "baz"), test_type(2, "bar")},
            [&](auto res) { result_set_promise(*all_done, std::move(res)); });
    });

    auto res = all_done->get_future().get();
    EXPECT_EQ(res.size(), 1);
    EXPECT_EQ(1, res.front().key);
    EXPECT_EQ("baz", res.front().val);
}

TEST_F(record_view_test, remove_all_exact_empty) {
    auto res = view.remove_all_exact(nullptr, {});
    EXPECT_TRUE(res.empty());
}

TEST_F(record_view_test, types_test) {
    auto table = m_client.get_tables().get_table(TABLE_NAME_ALL_COLUMNS);
    auto view2 = table->get_record_view<all_fields_type>();

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
    inserted.m_time = {17, 4, 12, 3543634};
    inserted.m_time2 = {17, 4, 12, 3543634};
    inserted.m_datetime = {{2020, 7, 28}, {2, 15, 52, 6349879}};
    inserted.m_datetime2 = {{2020, 7, 28}, {2, 15, 52, 6349879}};
    inserted.m_timestamp = {3875238472, 248760634};
    inserted.m_timestamp2 = {3875238472, 248760634};
    inserted.m_blob = {std::byte(1), std::byte(2), std::byte(42)};
    inserted.m_decimal = big_decimal{123456789098765};
    inserted.m_boolean = true;

    view2.upsert(nullptr, inserted);
    auto res = view2.get(nullptr, all_fields_type{42});

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
    EXPECT_EQ(ignite_timestamp(3875238472, 248700000), res->m_timestamp2);
}
