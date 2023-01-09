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

TEST_F(transactions_test, transactions_start) {
    auto api = m_client.get_transactions();
}

