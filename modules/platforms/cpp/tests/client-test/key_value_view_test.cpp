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
