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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class schema_synchronization_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
        m_client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS SCHEMA_SYN_TEST"}, {});
        m_client.get_sql().execute(nullptr, {"CREATE TABLE SCHEMA_SYN_TEST(ID INT PRIMARY KEY, VAL1 INT)"}, {});

        auto table = m_client.get_tables().get_table("SCHEMA_SYN_TEST");
        tuple_view = table->get_record_binary_view();
    }

    void TearDown() override { m_client.get_sql().execute(nullptr, {"DROP TABLE IF EXISTS SCHEMA_SYN_TEST"}, {}); }

    /** Ignite client. */
    ignite_client m_client;

    /** Record binary view. */
    record_view<ignite_tuple> tuple_view;
};

TEST_F(schema_synchronization_test, upsert_unmapped_columns) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    auto val2 = ignite_tuple{{"ID", std::int32_t(2)}, {"VAL1", std::int32_t(3)}, {"VAL2", std::int32_t(4)}};

    EXPECT_THROW(
        {
            try {
                tuple_view.upsert(nullptr, val2);
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Tuple doesn't match schema"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(schema_synchronization_test, upsert_add_column_upsert) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});

    auto val2 = ignite_tuple{{"ID", std::int32_t(2)}, {"VAL1", std::int32_t(3)}, {"VAL2", std::int32_t(4)}};
    tuple_view.upsert(nullptr, val2);
}

TEST_F(schema_synchronization_test, upsert_add_column_upsert_old) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});

    auto val2 = ignite_tuple{{"ID", std::int32_t(2)}, {"VAL1", std::int32_t(3)}};
    tuple_view.upsert(nullptr, val2);
}

TEST_F(schema_synchronization_test, upsert_add_column_compute) {
    std::pair key{"ID", std::int32_t(1)};
    auto val1 = ignite_tuple{key, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});
    auto descriptor = job_descriptor::builder(NODE_NAME_JOB).build();

    m_client.get_compute().submit_colocated("SCHEMA_SYN_TEST", {key}, descriptor, {}).get_result();
}

TEST_F(schema_synchronization_test, upsert_add_column_upsert_all) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});

    auto val2 = ignite_tuple{{"ID", std::int32_t(2)}, {"VAL1", std::int32_t(3)}, {"VAL2", std::int32_t(4)}};
    tuple_view.upsert_all(nullptr, {val2, val2, val2});
}

TEST_F(schema_synchronization_test, upsert_add_column_get) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});

    auto val2 = tuple_view.get(nullptr, {{"ID", std::int32_t(1)}});
}

TEST_F(schema_synchronization_test, upsert_add_column_get_all) {
    auto val1 = ignite_tuple{{"ID", std::int32_t(1)}, {"VAL1", std::int32_t(2)}};
    tuple_view.upsert(nullptr, val1);

    m_client.get_sql().execute(nullptr, {"ALTER TABLE SCHEMA_SYN_TEST ADD COLUMN VAL2 INT"}, {});

    auto val2 = tuple_view.get_all(nullptr, {{{"ID", std::int32_t(1)}}, {{"ID", std::int32_t(2)}}});
}
