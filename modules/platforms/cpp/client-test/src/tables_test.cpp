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

#include "ignite/ignite_client.h"
#include "ignite/ignite_client_configuration.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <chrono>
#include <thread>

using namespace ignite;

/**
 * Test suite.
 */
class tables_test : public ignite_runner_suite {};

TEST_F(tables_test, tables_get_table_async_promises) {
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(get_logger());

    auto clientPromise = std::make_shared<std::promise<IgniteClient>>();
    IgniteClient::startAsync(cfg, std::chrono::seconds(5), result_promise_setter(clientPromise));

    auto client = clientPromise->get_future().get();

    auto tables = client.getTables();

    auto tablePromise = std::make_shared<std::promise<std::optional<Table>>>();
    tables.getTableAsync("PUB.some_unknown", result_promise_setter(tablePromise));

    auto tableUnknown = tablePromise->get_future().get();
    EXPECT_FALSE(tableUnknown.has_value());

    tablePromise = std::make_shared<std::promise<std::optional<Table>>>();
    tables.getTableAsync("PUB.tbl1", result_promise_setter(tablePromise));

    auto table = tablePromise->get_future().get();
    ASSERT_TRUE(table.has_value());
    EXPECT_EQ(table->getName(), "PUB.tbl1");
}

template <typename T>
bool check_and_set_operation_error(std::promise<void> &operation, const ignite_result<T> &res) {
    if (res.has_error()) {
        operation.set_exception(std::make_exception_ptr(res.error()));
        return false;
    }
    if (!res.has_value()) {
        operation.set_exception(std::make_exception_ptr(ignite_error("There is no value in client result")));
        return false;
    }
    return true;
}

TEST_F(tables_test, tables_get_table_async_callbacks) {
    auto operation0 = std::make_shared<std::promise<void>>();
    auto operation1 = std::make_shared<std::promise<void>>();
    auto operation2 = std::make_shared<std::promise<void>>();

    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(get_logger());

    IgniteClient client;

    IgniteClient::startAsync(cfg, std::chrono::seconds(5), [&](ignite_result<IgniteClient> clientRes) {
        if (!check_and_set_operation_error(*operation0, clientRes))
            return;

        client = std::move(clientRes).value();
        auto tables = client.getTables();

        operation0->set_value();
        tables.getTableAsync("PUB.some_unknown", [&](auto tableRes) {
            if (!check_and_set_operation_error(*operation1, tableRes))
                return;

            auto tableUnknown = std::move(tableRes).value();
            if (tableUnknown.has_value()) {
                operation1->set_exception(std::make_exception_ptr(ignite_error("Table should be null")));
                return;
            }

            operation1->set_value();
        });

        tables.getTableAsync("PUB.tbl1", [&](auto tableRes) {
            if (!check_and_set_operation_error(*operation2, tableRes))
                return;

            auto table = std::move(tableRes).value();
            if (!table.has_value()) {
                operation2->set_exception(std::make_exception_ptr(ignite_error("Table should not be null")));
                return;
            }
            if (table->getName() != "PUB.tbl1") {
                operation2->set_exception(
                    std::make_exception_ptr(ignite_error("Table has unexpected name: " + table->getName())));
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

TEST_F(tables_test, tables_get_tables_async_promises) {
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(get_logger());

    auto client = IgniteClient::start(cfg, std::chrono::seconds(5));

    auto tablesApi = client.getTables();

    auto tablesPromise = std::make_shared<std::promise<std::vector<Table>>>();
    tablesApi.getTablesAsync(result_promise_setter(tablesPromise));

    auto tables = tablesPromise->get_future().get();
    ASSERT_GT(tables.size(), 0);

    auto it = std::find_if(tables.begin(), tables.end(), [] (auto& table) {
        return table.getName() == "PUB.TBL1";
    });

    ASSERT_NE(it, tables.end());
}
