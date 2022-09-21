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

//static const std::initializer_list<std::string_view> NODE_ADDRS = {"127.0.0.1:10942", "127.0.0.1:10943"};
static const std::initializer_list<std::string_view> NODE_ADDRS = {"127.0.0.1:10942"};

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
        return std::make_shared<TestLogger>(true, true);
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

TEST_F(ClientTest, TablesGetTablePromises)
{
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(getLogger());

    auto clientPromise = std::make_shared<std::promise<IgniteClient>>();
    IgniteClient::startAsync(cfg, std::chrono::seconds(5), IgniteResult<IgniteClient>::promiseSetter(clientPromise));

    auto client = clientPromise->get_future().get();

    auto tables = client.getTables();

    auto tablePromise = std::make_shared<std::promise<std::optional<Table>>>();
    tables.getTableAsync("PUB.some_unknown", IgniteResult<std::optional<Table>>::promiseSetter(tablePromise));

    auto tableUnknown = tablePromise->get_future().get();
    EXPECT_FALSE(tableUnknown.has_value());

    tablePromise = std::make_shared<std::promise<std::optional<Table>>>();
    tables.getTableAsync("PUB.tbl1", IgniteResult<std::optional<Table>>::promiseSetter(tablePromise));

    auto table = tablePromise->get_future().get();
    ASSERT_TRUE(table.has_value());
    EXPECT_EQ(table->getName(), "PUB.tbl1");
}
