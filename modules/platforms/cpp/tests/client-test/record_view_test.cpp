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

//TEST_F(record_binary_view_test, upsert_get) {
//    auto key_tuple = get_tuple(1);
//    auto val_tuple = get_tuple(1, "foo");
//
//    tuple_view.upsert(nullptr, val_tuple);
//    auto res_tuple = tuple_view.get(nullptr, key_tuple);
//
//    ASSERT_TRUE(res_tuple.has_value());
//    EXPECT_EQ(2, res_tuple->column_count());
//    EXPECT_EQ(1L, res_tuple->get<int64_t>("key"));
//    EXPECT_EQ("foo", res_tuple->get<std::string>("val"));
//}
