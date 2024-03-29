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
class sql_test : public ignite_runner_suite {
protected:
    static void SetUpTestSuite() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS TEST"}, {});
        client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS execute_script_success"}, {});
        client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS execute_script_fail"}, {});

        client.get_sql().execute(nullptr, {"CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)"}, {});

        for (std::int32_t i = 0; i < 10; ++i) {
            client.get_sql().execute(nullptr, {"INSERT INTO TEST VALUES (?, ?)"}, {i, "s-" + std::to_string(i)});
        }

        client.get_sql().execute(nullptr,
            {"INSERT INTO TBL_ALL_COLUMNS_SQL VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"},
            {std::int64_t(42), std::string("test"), std::int8_t(1), std::int16_t(2), std::int32_t(3), std::int64_t(4),
                .5f, .6, uuid(0x123e4567e89b12d3, 0x7456426614174000), ignite_date(2023, 2, 7),
                ignite_time(17, 4, 12, 3543634), ignite_time(17, 4, 12, 3543634),
                ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879}),
                ignite_date_time({2020, 7, 28}, {2, 15, 52, 6349879}), ignite_timestamp(3875238472, 248760634),
                ignite_timestamp(3875238472, 248760634),
                std::vector<std::byte>{std::byte(1), std::byte(2), std::byte(42)}, big_decimal(123456789098765), true});
    }

    static void TearDownTestSuite() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        auto client = ignite_client::start(cfg, std::chrono::seconds(30));

        client.get_sql().execute(nullptr, {"DELETE FROM TBL_ALL_COLUMNS_SQL"}, {});
        client.get_sql().execute(nullptr, {"DROP TABLE TEST"}, {});
        client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS execute_script_success"}, {});
    }

    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
    }

    void TearDown() override {
        // remove all
    }

    /** Ignite client. */
    ignite_client m_client;
};

static void check_columns(
    const result_set_metadata &meta, std::initializer_list<std::tuple<std::string, ignite_type>> columns) {

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

    check_columns(result_set.metadata(), {{"42", ignite_type::INT32}, {"'Lorem'", ignite_type::STRING}});

    auto page = std::move(result_set).current_page();

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

    check_columns(result_set.metadata(), {{"ID", ignite_type::INT32}, {"VAL", ignite_type::STRING}});

    auto page = result_set.current_page();

    EXPECT_EQ(10, page.size());

    for (std::int32_t i = 0; i < std::int32_t(page.size()); ++i) {
        EXPECT_EQ(i, page[i].get(0).get<std::int32_t>());
        EXPECT_EQ("s-" + std::to_string(i), page[i].get(1).get<std::string>());
    }

    EXPECT_FALSE(result_set.has_more_pages());
    EXPECT_EQ(10, result_set.current_page().size());
}

TEST_F(sql_test, sql_select_multiple_pages) {
    sql_statement statement{"select id, val from TEST order by id"};
    statement.page_size(1);

    auto result_set = m_client.get_sql().execute(nullptr, statement, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());

    check_columns(result_set.metadata(), {{"ID", ignite_type::INT32}, {"VAL", ignite_type::STRING}});

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

    result_set =
        m_client.get_sql().execute(nullptr, {"CREATE TABLE SQL_DDL_DML_TEST(ID BIGINT PRIMARY KEY, VAL VARCHAR)"}, {});

    EXPECT_TRUE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(-1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(
        nullptr, {"INSERT INTO SQL_DDL_DML_TEST VALUES (?, ?)"}, {std::int64_t(13), std::string("Hello")});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(
        nullptr, {"INSERT INTO SQL_DDL_DML_TEST VALUES (?, ?)"}, {std::int64_t(14), std::string("World")});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"UPDATE SQL_DDL_DML_TEST SET VAL = ?"}, {std::string("Test")});

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
    result_set =
        m_client.get_sql().execute(nullptr, {"CREATE TABLE SQL_INSERT_NULL_TEST(ID INT PRIMARY KEY, VAL VARCHAR)"}, {});

    ASSERT_TRUE(result_set.was_applied());

    result_set = m_client.get_sql().execute(nullptr, {"INSERT INTO SQL_INSERT_NULL_TEST VALUES (13, NULL)"}, {});

    EXPECT_FALSE(result_set.was_applied());
    EXPECT_FALSE(result_set.has_rowset());
    EXPECT_EQ(1, result_set.affected_rows());
    EXPECT_TRUE(result_set.metadata().columns().empty());

    result_set = m_client.get_sql().execute(nullptr, {"DROP TABLE SQL_INSERT_NULL_TEST"}, {});
    EXPECT_TRUE(result_set.was_applied());
}

TEST_F(sql_test, sql_invalid_query) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"not a query"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Failed to parse query: Non-query expression"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_unknown_table) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"select id from unknown_table"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Object 'UNKNOWN_TABLE' not found"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_unknown_column) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"select unknown_column from test"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Column 'UNKNOWN_COLUMN' not found in any table"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_create_existing_table) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"CREATE TABLE TEST(ID INT PRIMARY KEY, VAL VARCHAR)"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Table with name 'PUBLIC.TEST' already exists"));
                // TODO: IGNITE-21217 Check STMT_VALIDATION error code usage
                EXPECT_THAT(e.get_status_code(), ignite::error::code::STMT_VALIDATION);
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_add_existing_column) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"ALTER TABLE TEST ADD COLUMN ID INT"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Column with name 'ID' already exists"));
                // TODO: IGNITE-21217 Check STMT_VALIDATION error code usage
                EXPECT_THAT(e.get_status_code(), ignite::error::code::STMT_VALIDATION);
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_alter_nonexisting_table) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute(nullptr, {"ALTER TABLE UNKNOWN_TABLE ADD COLUMN ID INT"}, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Table with name 'PUBLIC.UNKNOWN_TABLE' not found"));
                // TODO: IGNITE-21217 Check STMT_VALIDATION error code usage
                EXPECT_THAT(e.get_status_code(), ignite::error::code::STMT_VALIDATION);
                throw;
            }
        },
        ignite_error);
}

TEST_F(sql_test, sql_statement_defaults) {
    sql_statement statement;

    EXPECT_EQ(statement.page_size(), sql_statement::DEFAULT_PAGE_SIZE);
    EXPECT_EQ(statement.schema(), sql_statement::DEFAULT_SCHEMA);
    EXPECT_EQ(statement.timeout(), sql_statement::DEFAULT_TIMEOUT);
}

TEST_F(sql_test, decimal_literal) {
    auto result_set = m_client.get_sql().execute(nullptr, {"SELECT CAST('12345.6789' AS DECIMAL(9, 4))"}, {});

    EXPECT_TRUE(result_set.has_rowset());

    auto value = result_set.current_page().front().get(0).get<big_decimal>();

    EXPECT_EQ(4, value.get_scale());
    EXPECT_EQ(9, value.get_precision());

    std::stringstream ss;
    ss << value;
    auto value_str = ss.str();

    EXPECT_EQ("12345.6789", value_str);
}

TEST_F(sql_test, all_type_arguments) {
    auto result_set = m_client.get_sql().execute(nullptr,
        {"select str,int8,int16,int32,int64,\"FLOAT\",\"DOUBLE\",\"UUID\",\"DATE\",\"TIME\",time2,"
         "\"DATETIME\",datetime2,\"TIMESTAMP\",timestamp2,\"BLOB\",\"DECIMAL\",\"BOOLEAN\" from TBL_ALL_COLUMNS_SQL"},
        {});

    EXPECT_TRUE(result_set.has_rowset());

    auto row = result_set.current_page().front();
    EXPECT_EQ(row.get(0).get<std::string>(), "test");
    EXPECT_EQ(row.get(1).get<std::int8_t>(), 1);
    EXPECT_EQ(row.get(2).get<std::int16_t>(), 2);
    EXPECT_EQ(row.get(3).get<std::int32_t>(), 3);
    EXPECT_EQ(row.get(4).get<std::int64_t>(), 4);
    EXPECT_FLOAT_EQ(row.get(5).get<float>(), 0.5f);
    EXPECT_DOUBLE_EQ(row.get(6).get<double>(), 0.6);
    EXPECT_EQ(row.get(7).get<uuid>(), uuid(0x123e4567e89b12d3, 0x7456426614174000));
    EXPECT_EQ(row.get(8).get<ignite_date>(), ignite_date(2023, 2, 7));
    EXPECT_EQ(row.get(9).get<ignite_time>(), ignite_time(17, 4, 12, 3000000));
    EXPECT_EQ(row.get(10).get<ignite_time>(), ignite_time(17, 4, 12, 3000000));
    EXPECT_EQ(row.get(11).get<ignite_date_time>(), ignite_date_time({2020, 7, 28}, {2, 15, 52, 6000000}));
    EXPECT_EQ(row.get(12).get<ignite_date_time>(), ignite_date_time({2020, 7, 28}, {2, 15, 52, 6000000}));
    EXPECT_EQ(row.get(13).get<ignite_timestamp>(), ignite_timestamp(3875238472, 248000000));
    EXPECT_EQ(row.get(14).get<ignite_timestamp>(), ignite_timestamp(3875238472, 248000000));
    EXPECT_EQ(row.get(16).get<big_decimal>(), big_decimal(123456789098765));
    EXPECT_EQ(row.get(17).get<bool>(), true);

    auto blob = row.get(15).get<std::vector<std::byte>>();
    EXPECT_EQ(blob[0], std::byte(1));
    EXPECT_EQ(blob[1], std::byte(2));
    EXPECT_EQ(blob[2], std::byte(42));
}

TEST_F(sql_test, uuid_literal) {
    auto result_set =
        m_client.get_sql().execute(nullptr, {"SELECT CAST('df6bbb13-f2d2-42b4-bef9-7ab8524a0704' AS UUID)"}, {});

    EXPECT_TRUE(result_set.has_rowset());

    auto value = result_set.current_page().front().get(0).get<uuid>();

    std::stringstream ss;
    ss << value;
    auto value_str = ss.str();

    EXPECT_EQ("df6bbb13-f2d2-42b4-bef9-7ab8524a0704", value_str);
}

TEST_F(sql_test, uuid_argument) {
    uuid req{0x123e4567e89b12d3, 0x7456426614174000};
    auto result_set =
        m_client.get_sql().execute(nullptr, {R"(select MAX("UUID") from TBL_ALL_COLUMNS_SQL WHERE "UUID" = ?)"}, {req});

    EXPECT_TRUE(result_set.has_rowset());

    auto value = result_set.current_page().front().get(0).get<uuid>();
    EXPECT_EQ(req, value);
}

TEST_F(sql_test, null_column) {
    auto result_set = m_client.get_sql().execute(nullptr, {"select NULL"}, {});

    EXPECT_TRUE(result_set.has_rowset());

    auto &columns = result_set.metadata().columns();
    EXPECT_EQ(1, columns.size());
    EXPECT_EQ(ignite_type::NIL, columns.at(0).type());

    auto value = result_set.current_page().front().get(0);
    EXPECT_EQ(ignite_type::NIL, value.get_type());
    EXPECT_TRUE(value.is_null());
}

TEST_F(sql_test, execute_script_success) {
    m_client.get_sql().execute_script({"CREATE TABLE execute_script_success (id INT PRIMARY KEY, step INTEGER); "
                                       "INSERT INTO execute_script_success VALUES(1, 0); "
                                       "UPDATE execute_script_success SET step = 1; "
                                       "UPDATE execute_script_success SET step = 2; "},
        {});

    auto result_set = m_client.get_sql().execute(nullptr, {"SELECT step FROM execute_script_success"}, {});
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_FALSE(result_set.has_more_pages());

    auto &columns = result_set.metadata().columns();
    EXPECT_EQ(1, columns.size());
    EXPECT_EQ(ignite_type::INT32, columns.at(0).type());

    ASSERT_EQ(1, result_set.current_page().size());
    auto value = result_set.current_page().front().get(0);
    EXPECT_EQ(ignite_type::INT32, value.get_type());
    EXPECT_EQ(2, value.get<std::int32_t>());
}

TEST_F(sql_test, execute_script_fail) {
    EXPECT_THROW(
        {
            try {
                m_client.get_sql().execute_script(
                    {"CREATE TABLE execute_script_fail (id INT PRIMARY KEY, step INTEGER); "
                     "INSERT INTO execute_script_fail VALUES(1, 0); "
                     "UPDATE execute_script_fail SET step = 1; "
                     "UPDATE execute_script_fail SET step = 3 WHERE step > 1/0; "
                     "UPDATE execute_script_fail SET step = 2; "},
                    {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Division by zero"));
                throw;
            }
        },
        ignite_error);

    auto result_set = m_client.get_sql().execute(nullptr, {"SELECT step FROM execute_script_fail"}, {});
    EXPECT_TRUE(result_set.has_rowset());
    EXPECT_FALSE(result_set.has_more_pages());

    auto &columns = result_set.metadata().columns();
    EXPECT_EQ(1, columns.size());
    EXPECT_EQ(ignite_type::INT32, columns.at(0).type());

    ASSERT_EQ(1, result_set.current_page().size());
    auto value = result_set.current_page().front().get(0);
    EXPECT_EQ(ignite_type::INT32, value.get_type());
    EXPECT_EQ(1, value.get<std::int32_t>());
}
