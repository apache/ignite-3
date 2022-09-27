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

#include "gtest_logger.h"

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
    static std::shared_ptr<GtestLogger> getLogger() {
        return std::make_shared<GtestLogger>(true, true);
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

template<typename T>
bool checkAndSetOperationError(std::promise<void>& operation, const IgniteResult<T>& res) {
    if (res.hasError()) {
        operation.set_exception(std::make_exception_ptr(res.getError()));
        return false;
    }
    if (!res.hasValue()) {
        operation.set_exception(std::make_exception_ptr(IgniteError("There is no value in client result")));
        return false;
    }
    return true;
}

TEST_F(ClientTest, TablesGetTableCallbacks)
{
    auto operation0 = std::make_shared<std::promise<void>>();
    auto operation1 = std::make_shared<std::promise<void>>();
    auto operation2 = std::make_shared<std::promise<void>>();

    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(getLogger());

    IgniteClient client;

    IgniteClient::startAsync(cfg, std::chrono::seconds(5), [&] (IgniteResult<IgniteClient> clientRes) {
        if (!checkAndSetOperationError(*operation0, clientRes))
            return;

        client = std::move(clientRes.getValue());
        auto tables = client.getTables();

        operation0->set_value();
        tables.getTableAsync("PUB.some_unknown", [&] (auto tableRes) {
            if (!checkAndSetOperationError(*operation1, tableRes))
                return;

            auto tableUnknown = tableRes.getValue();
            if (tableUnknown.has_value()) {
                operation1->set_exception(std::make_exception_ptr(IgniteError("Table should be null")));
                return;
            }

            operation1->set_value();
        });

        tables.getTableAsync("PUB.tbl1", [&] (auto tableRes) {
            if (!checkAndSetOperationError(*operation2, tableRes))
                return;

            auto table = tableRes.getValue();
            if (!table.has_value()) {
                operation2->set_exception(std::make_exception_ptr(IgniteError("Table should not be null")));
                return;
            }
            if (table->getName() != "PUB.tbl1") {
                operation2->set_exception(std::make_exception_ptr(IgniteError("Table has unexpected name: " + table->getName())));
                return;
            }

            operation2->set_value();
        });
    });

    // Waiting for all operations to complete
    operation0->get_future().get();
    operation1->get_future().get();
    operation2->get_future().get();
}
