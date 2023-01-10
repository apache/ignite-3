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

TEST_F(transactions_test, rollback_does_not_update_data) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.upsert(&tx, value0);

    tx.rollback();

    auto actual = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(actual.has_value());
}

TEST_F(transactions_test, destruction_does_not_update_data) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    {
        auto tx = m_client.get_transactions().begin();

        auto value0 = get_tuple(42, "Lorem ipsum");
        record_view.upsert(&tx, value0);
    }

    auto actual = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(actual.has_value());
}

TEST_F(transactions_test, non_committed_data_visible_for_tx) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.upsert(&tx, value0);

    auto value_in = record_view.get(&tx, get_tuple(42));

    ASSERT_TRUE(value_in.has_value());
    EXPECT_EQ(2, value_in->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value_in->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value_in->get<std::string>("val"));
}

TEST_F(transactions_test, sql_commit) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    auto tx = m_client.get_transactions().begin();

    m_client.get_sql().execute(&tx,
        {"INSERT INTO " + std::string(TABLE_1) + " VALUES (?, ?)"}, {42LL, std::string("Lorem ipsum")});

    tx.commit();

    auto value = record_view.get(nullptr, get_tuple(42));

    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(2, value->column_count());
    EXPECT_EQ(42, value->get<int64_t>("key"));
    EXPECT_EQ("Lorem ipsum", value->get<std::string>("val"));
}

TEST_F(transactions_test, sql_rollback) {
    auto record_view = m_client.get_tables().get_table("tbl1")->record_binary_view();

    auto tx = m_client.get_transactions().begin();

    m_client.get_sql().execute(&tx, {"INSERT INTO " + std::string(TABLE_1) + " VALUES (?, ?)"}, {42LL, "Lorem ipsum"});

    tx.rollback();

    auto value = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(value.has_value());
}

TEST_F(transactions_test, rollback_after_commit_throws) {
    auto tx = m_client.get_transactions().begin();

    tx.commit();

    EXPECT_THROW(
    {
        try {
            tx.rollback();
        } catch (const ignite_error &e) {
            EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Transaction is already committed"));
            throw;
        }
    },
    ignite_error);
}

TEST_F(transactions_test, commit_after_commit_throws) {
    auto tx = m_client.get_transactions().begin();

    tx.commit();

    EXPECT_THROW(
    {
        try {
            tx.commit();
        } catch (const ignite_error &e) {
            EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Transaction is already committed"));
            throw;
        }
    },
    ignite_error);
}

TEST_F(transactions_test, commit_after_rollback_throws) {
    auto tx = m_client.get_transactions().begin();

    tx.rollback();

    EXPECT_THROW(
    {
        try {
            tx.commit();
        } catch (const ignite_error &e) {
            EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Transaction is already rolled back"));
            throw;
        }
    },
    ignite_error);
}

TEST_F(transactions_test, rollback_after_rollback_throws) {
    auto tx = m_client.get_transactions().begin();

    tx.rollback();

    EXPECT_THROW(
    {
        try {
            tx.rollback();
        } catch (const ignite_error &e) {
            EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Transaction is already rolled back"));
            throw;
        }
    },
    ignite_error);
}

