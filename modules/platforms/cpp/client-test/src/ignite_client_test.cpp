/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <chrono>
#include <string_view>

#include <gtest/gtest.h>

#include "ignite/ignite_client_configuration.h"
#include "ignite/ignite_client.h"

#include "test_logger.h"

using namespace ignite;

static const std::initializer_list<std::string_view> NODE_ADDRS = {"127.0.0.1:10942", "127.0.0.1:10943"};

/**
 * Test suite.
 */
class ClientTest : public ::testing::Test {
protected:
    ClientTest() = default;
    ~ClientTest() override = default;

    /**
     * Get logger.
     *
     * @return Logger for tests.
     */
    static std::shared_ptr<TestLogger> getLogger()
    {
        return std::make_shared<TestLogger>(false, true);
    }
};

TEST_F(ClientTest, GetConfiguration)
{
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(getLogger());
    cfg.setConnectionLimit(42);

    auto client = IgniteClient::start(cfg, std::chrono::seconds(5));

    const auto& cfg2 = client.getConfiguration();

    EXPECT_EQ(cfg.getEndpoints(), cfg2.getEndpoints());
    EXPECT_EQ(cfg.getConnectionLimit(), cfg2.getConnectionLimit());
}

TEST_F(ClientTest, TablesGetTable)
{
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(getLogger());

    auto client = IgniteClient::startAsync(cfg, std::chrono::seconds(5)).get();

    auto tables = client.getTables();
    auto tableUnknown = tables.getTableAsync("PUB.some_unknown").get();

    EXPECT_FALSE(tableUnknown.has_value());

    auto table = tables.getTableAsync("PUB.tbl1").get();

    ASSERT_TRUE(table.has_value());
    EXPECT_EQ(table->getName(), "PUB.tbl1");
}
