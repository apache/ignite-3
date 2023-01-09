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

#include <gtest/gtest.h>
#include <gmock/gmock-matchers.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class transactions_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        clear_table1();

        ignite_client_configuration cfg{NODE_ADDRS};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
    }

    void TearDown() override {
        clear_table1();
    }

    /** Ignite client. */
    ignite_client m_client;
};

TEST_F(transactions_test, empty_transaction_rollback) {
    auto api = m_client.get_transactions();

    auto tx = api.begin();
    tx.rollback();
}

TEST_F(transactions_test, empty_transaction_commit) {
    auto api = m_client.get_transactions();

    auto tx = api.begin();
    tx.rollback();
}

TEST_F(transactions_test, commit_updates_data) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto expected = get_tuple(42, "Lorem ipsum");
    record_view.upsert(&tx, expected);

    tx.commit();

    auto actual = record_view.get(nullptr, get_tuple(42));

    ASSERT_TRUE(actual.has_value());
    EXPECT_EQ(2, actual->column_count());
    EXPECT_EQ(expected.get<int64_t>("key"), actual->get<int64_t>("key"));
    EXPECT_EQ(expected.get<std::string>("val"), actual->get<std::string>("val"));
}

