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
class sql_test : public ignite_runner_suite {

protected:
    void SetUp() override {
        ignite_client_configuration cfg{NODE_ADDRS};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
    }

    void TearDown() override {
        // remove all
    }

    /** Ignite client. */
    ignite_client m_client;
};

TEST_F(sql_test, sql_simple_select) {
    auto sql = m_client.get_sql();
    auto result_set = sql.execute(nullptr, {"select 42, 'Lorem'"}, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    auto &meta = result_set.metadata();

    ASSERT_EQ(2, meta.columns().size());

    EXPECT_EQ(0, meta.index_of("42"));
    EXPECT_EQ(1, meta.index_of("'Lorem'"));

    EXPECT_EQ(column_type::INT32, meta.columns()[0].type());
    EXPECT_EQ("42", meta.columns()[0].name());

    EXPECT_EQ(column_type::STRING, meta.columns()[1].type());
    EXPECT_EQ("'Lorem'", meta.columns()[1].name());

    auto page = result_set.current_page();

    EXPECT_EQ(1, page.size());
    EXPECT_EQ(42, page.front().get(0).get<std::int32_t>());
    EXPECT_EQ("Lorem", page.front().get(1).get<std::string>());
}
