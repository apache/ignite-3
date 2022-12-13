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

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class sql_test : public ignite_runner_suite {
protected:
    static void SetUpTestSuite() {
        ignite_client_configuration cfg{NODE_ADDRS};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        auto res = client.get_sql().execute(nullptr,
            {"CREATE TABLE IF NOT EXISTS TEST(ID INT PRIMARY KEY, VAL VARCHAR)"}, {});

        if (!res.was_applied()) {
            client.get_sql().execute(nullptr, {"DELETE FROM TEST"}, {});
        }

        for (std::int32_t i = 0; i < 10; ++i) {
            client.get_sql().execute(nullptr, {"INSERT INTO TEST VALUES (?, ?)"}, {i, "s-" + std::to_string(i)});
        }
    }

    static void TearDownTestSuite() {
        ignite_client_configuration cfg{NODE_ADDRS};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"DROP TABLE TEST"}, {});
        client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS TestDdlDml"}, {});
    }

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

void check_columns(const result_set_metadata& meta,
    std::initializer_list<std::tuple<std::string, column_type>> columns) {

    ASSERT_EQ(columns.size(), meta.columns().size());
    size_t i = 0;
    for (auto &column : columns) {
        EXPECT_EQ(i, meta.index_of(std::get<0>(column)));
        EXPECT_EQ(meta.columns()[i].name(), std::get<0>(column));
        EXPECT_EQ(meta.columns()[i].type(), std::get<1>(column));
        ++i;
    }
}

TEST_F(sql_test, sql_simple_select) {
    auto result_set = m_client.get_sql().execute(nullptr, {"select 42, 'Lorem'"}, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    check_columns(result_set.metadata(), {{"42", column_type::INT32}, {"'Lorem'", column_type::STRING}});

    auto page = result_set.current_page();

    EXPECT_EQ(1, page.size());
    EXPECT_EQ(42, page.front().get(0).get<std::int32_t>());
    EXPECT_EQ("Lorem", page.front().get(1).get<std::string>());

    EXPECT_FALSE(result_set.has_more_pages());
    EXPECT_EQ(0, result_set.current_page().size());
}

TEST_F(sql_test, sql_table_select) {
    auto result_set = m_client.get_sql().execute(nullptr, {"select id, val from TEST order by id"}, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    check_columns(result_set.metadata(), {{"ID", column_type::INT32}, {"VAL", column_type::STRING}});

    auto page = result_set.current_page();

    EXPECT_EQ(10, page.size());

    for (std::int32_t i = 0; i < page.size(); ++i) {
        EXPECT_EQ(i, page[i].get(0).get<std::int32_t>());
        EXPECT_EQ("s-" + std::to_string(i), page[i].get(1).get<std::string>());
    }

    EXPECT_FALSE(result_set.has_more_pages());
    EXPECT_EQ(0, result_set.current_page().size());
}

TEST_F(sql_test, sql_select_multiple_pages) {
    sql_statement statement{"select id, val from TEST order by id"};
    statement.page_size(1);

    auto result_set = m_client.get_sql().execute(nullptr, statement, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    check_columns(result_set.metadata(), {{"ID", column_type::INT32}, {"VAL", column_type::STRING}});

    for (std::int32_t i = 0; i < 10; ++i) {
        auto page = result_set.current_page();

        EXPECT_EQ(1, page.size()) << "i=" << i;
        EXPECT_EQ(i, page.front().get(0).get<std::int32_t>());
        EXPECT_EQ("s-" + std::to_string(i), page.front().get(1).get<std::string>());

        if (i < 9) {
            ASSERT_TRUE(result_set.has_more_pages());
            result_set.fetch_next_page();
        }
    }

    EXPECT_FALSE(result_set.has_more_pages());
    EXPECT_EQ(0, result_set.current_page().size());
}

TEST_F(sql_test, sql_close_non_empty_cursor) {
    sql_statement statement{"select id, val from TEST order by id"};
    statement.page_size(3);

    auto result_set = m_client.get_sql().execute(nullptr, statement, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    auto page = result_set.current_page();
    ASSERT_TRUE(result_set.has_more_pages());

    result_set.close();
}

TEST_F(sql_test, sql_ddl_dml) {
    auto result_set = m_client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS SQL_DDL_DML_TEST"}, {});

    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr,
        {"CREATE TABLE SQL_DDL_DML_TEST(ID BIGINT PRIMARY KEY, VAL VARCHAR)"}, {});

    EXPECT_TRUE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"INSERT INTO SQL_DDL_DML_TEST VALUES (?, ?)"}, {13LL, "Hello"});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"INSERT INTO SQL_DDL_DML_TEST VALUES (?, ?)"}, {14LL, "World"});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());


    result_set = m_client.get_sql().execute(nullptr, {"UPDATE SQL_DDL_DML_TEST SET VAL = ?"}, {"Test"});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(2, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"DROP TABLE SQL_DDL_DML_TEST"}, {});

    EXPECT_TRUE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());
}

TEST_F(sql_test, sql_insert_null) {
    auto result_set = m_client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS SQL_INSERT_NULL_TEST"}, {});
    result_set = m_client.get_sql().execute(nullptr,
        {"CREATE TABLE SQL_INSERT_NULL_TEST(ID INT PRIMARY KEY, VAL VARCHAR)"}, {});

    ASSERT_TRUE(result_set.was_applied());

    result_set = m_client.get_sql().execute(nullptr, {"INSERT INTO SQL_INSERT_NULL_TEST VALUES (13, NULL)"}, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"DROP TABLE SQL_INSERT_NULL_TEST"}, {});
    EXPECT_TRUE(result_set.was_applied());
}
