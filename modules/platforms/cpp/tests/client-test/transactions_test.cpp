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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class transactions_test : public ignite_runner_suite {
protected:
    /**
     * Set up for every test.
     */
    void SetUp() override {
        clear_table1();

        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
    }

    /**
     * Tear down for every test.
     */
    void TearDown() override { clear_table1(); }

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
    tx.commit();
}

TEST_F(transactions_test, commit_updates_data) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

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
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.upsert(&tx, value0);

    tx.rollback();

    auto actual = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(actual.has_value());
}

// TODO https://issues.apache.org/jira/browse/IGNITE-22057
TEST_F(transactions_test, DISABLED_destruction_does_not_update_data) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    {
        auto tx = m_client.get_transactions().begin();

        auto value0 = get_tuple(42, "Lorem ipsum");
        record_view.upsert(&tx, value0);
    }

    auto actual = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(actual.has_value());
}

TEST_F(transactions_test, non_committed_data_visible_for_tx) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

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
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    m_client.get_sql().execute(&tx, {"INSERT INTO " + std::string(TABLE_1) + " VALUES (?, ?)"},
        {std::int64_t(42), std::string("Lorem ipsum")});

    tx.commit();

    auto value = record_view.get(nullptr, get_tuple(42));

    ASSERT_TRUE(value.has_value());
    EXPECT_EQ(2, value->column_count());
    EXPECT_EQ(42, value->get<int64_t>("key"));
    EXPECT_EQ("Lorem ipsum", value->get<std::string>("val"));
}

TEST_F(transactions_test, sql_rollback) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    m_client.get_sql().execute(&tx, {"INSERT INTO " + std::string(TABLE_1) + " VALUES (?, ?)"},
        {std::int64_t(42), std::string("Lorem ipsum")});

    tx.rollback();

    auto value = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(value.has_value());
}

TEST_F(transactions_test, rollback_after_commit_works) {
    auto tx = m_client.get_transactions().begin();

    tx.commit();
    tx.rollback();
}

TEST_F(transactions_test, commit_after_commit_works) {
    auto tx = m_client.get_transactions().begin();

    tx.commit();
    tx.commit();
}

TEST_F(transactions_test, commit_after_rollback_works) {
    auto tx = m_client.get_transactions().begin();

    tx.rollback();
    tx.commit();
}

TEST_F(transactions_test, rollback_after_rollback_works) {
    auto tx = m_client.get_transactions().begin();

    tx.rollback();
    tx.rollback();
}

TEST_F(transactions_test, record_view_upsert_all) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.upsert_all(&tx, {value0});

    auto values1 = record_view.get_all(&tx, {get_tuple(42)});

    ASSERT_EQ(1, values1.size());
    EXPECT_EQ(2, values1.front()->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), values1.front()->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), values1.front()->get<std::string>("val"));

    tx.rollback();

    auto values2 = record_view.get_all(nullptr, {get_tuple(42)});

    ASSERT_EQ(1, values2.size());
    EXPECT_FALSE(values2.front().has_value());
}

TEST_F(transactions_test, record_view_get_and_upsert) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    auto value1 = record_view.get_and_upsert(&tx, value0);

    ASSERT_FALSE(value1.has_value());

    auto value2 = record_view.get_and_upsert(&tx, get_tuple(42, "12"));

    ASSERT_TRUE(value2.has_value());
    EXPECT_EQ(2, value2->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value2->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value2->get<std::string>("val"));

    tx.rollback();

    auto value3 = record_view.get_and_upsert(nullptr, value0);

    ASSERT_FALSE(value3.has_value());
}

TEST_F(transactions_test, record_view_insert) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(&tx, value0);

    auto value1 = record_view.get(&tx, get_tuple(42));

    ASSERT_TRUE(value1.has_value());
    EXPECT_EQ(2, value1->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value1->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value1->get<std::string>("val"));

    tx.rollback();

    auto value2 = record_view.get(nullptr, get_tuple(42));

    ASSERT_FALSE(value2.has_value());
}

TEST_F(transactions_test, record_view_insert_all) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert_all(&tx, {value0});

    auto values1 = record_view.get_all(&tx, {get_tuple(42)});

    ASSERT_EQ(1, values1.size());
    EXPECT_EQ(2, values1.front()->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), values1.front()->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), values1.front()->get<std::string>("val"));

    tx.rollback();

    auto values2 = record_view.get_all(nullptr, {get_tuple(42)});

    ASSERT_EQ(1, values2.size());
    EXPECT_FALSE(values2.front().has_value());
}

TEST_F(transactions_test, record_view_replace) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(&tx, value0);

    auto success1 = record_view.replace(&tx, get_tuple(42, "13"));

    ASSERT_TRUE(success1);

    tx.rollback();

    auto success2 = record_view.replace(nullptr, get_tuple(42, "13"));

    ASSERT_FALSE(success2);
}

TEST_F(transactions_test, record_view_replace_exact) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(&tx, value0);

    auto success1 = record_view.replace(&tx, value0, get_tuple(42, "13"));

    ASSERT_TRUE(success1);

    tx.rollback();

    auto success2 = record_view.replace(nullptr, value0, get_tuple(42, "13"));

    ASSERT_FALSE(success2);
}

TEST_F(transactions_test, record_view_get_and_replace) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto tx = m_client.get_transactions().begin();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(&tx, value0);

    auto value1 = record_view.get_and_replace(&tx, get_tuple(42, "12"));

    ASSERT_TRUE(value1.has_value());
    EXPECT_EQ(2, value1->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value1->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value1->get<std::string>("val"));

    tx.rollback();

    auto value2 = record_view.get_and_replace(nullptr, value0);

    ASSERT_FALSE(value2.has_value());
}

TEST_F(transactions_test, record_view_remove) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(nullptr, value0);

    auto tx = m_client.get_transactions().begin();
    auto success1 = record_view.remove(&tx, get_tuple(42));

    ASSERT_TRUE(success1);
    tx.rollback();

    auto success2 = record_view.remove(nullptr, get_tuple(42));
    ASSERT_TRUE(success2);
}

TEST_F(transactions_test, record_view_remove_exact) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(nullptr, value0);

    auto tx = m_client.get_transactions().begin();
    auto success1 = record_view.remove(&tx, value0);

    ASSERT_TRUE(success1);
    tx.rollback();

    auto success2 = record_view.remove(nullptr, value0);
    ASSERT_TRUE(success2);
}

TEST_F(transactions_test, record_view_get_and_remove) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(nullptr, value0);

    auto tx = m_client.get_transactions().begin();
    auto value1 = record_view.get_and_remove(&tx, get_tuple(42));

    ASSERT_TRUE(value1.has_value());
    EXPECT_EQ(2, value1->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value1->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value1->get<std::string>("val"));

    tx.rollback();

    auto value2 = record_view.get_and_remove(nullptr, get_tuple(42));

    ASSERT_TRUE(value2.has_value());
    EXPECT_EQ(2, value2->column_count());
    EXPECT_EQ(value0.get<int64_t>("key"), value2->get<int64_t>("key"));
    EXPECT_EQ(value0.get<std::string>("val"), value2->get<std::string>("val"));
}

TEST_F(transactions_test, record_view_remove_all) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(nullptr, value0);

    auto tx = m_client.get_transactions().begin();
    auto values1 = record_view.remove_all(&tx, {get_tuple(42)});

    ASSERT_TRUE(values1.empty());
    tx.rollback();

    auto values2 = record_view.remove_all(nullptr, {get_tuple(42)});
    ASSERT_TRUE(values2.empty());
}

TEST_F(transactions_test, record_view_remove_all_exact) {
    auto record_view = m_client.get_tables().get_table("tbl1")->get_record_binary_view();

    auto value0 = get_tuple(42, "Lorem ipsum");
    record_view.insert(nullptr, value0);

    auto tx = m_client.get_transactions().begin();
    auto values1 = record_view.remove_all(&tx, {value0});

    ASSERT_TRUE(values1.empty());
    tx.rollback();

    auto values2 = record_view.remove_all(nullptr, {value0});
    ASSERT_TRUE(values2.empty());
}
