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

#include <gtest/gtest.h>

#include "ignite/ignite_client_configuration.h"
#include "ignite/ignite_client.h"

#include "ignite_node.h"

using namespace ignite;

class ClientTest : public ::testing::Test {
protected:
    ClientTest() = default;
    ~ClientTest() override = default;

    void SetUp() override
    {
        // Code here will be called immediately after the constructor (right
        // before each test).
    }

    void TearDown() override
    {
        // Code here will be called immediately after each test (right
        // before the destructor).
    }
};

class TestLogger : public IgniteLogger
{
public:
    void logError(std::string_view message) override
    {
        std::cout << "ERROR:   " << message << std::endl;
    }

    void logWarning(std::string_view message) override
    {
        std::cout << "WARNING: " << message << std::endl;
    }

    void logInfo(std::string_view message) override
    {
        std::cout << "INFO:    " << message << std::endl;
    }

    void logDebug(std::string_view message) override
    {
        std::cout << "DEBUG:   " << message << std::endl;
    }
};

TEST_F(ClientTest, TestTest)
{
    IgniteNode node;

    node.start();

    // TODO: Implement node startup await
    std::this_thread::sleep_for(std::chrono::seconds(20));

    IgniteClientConfiguration cfg{"127.0.0.1:10942"};
    cfg.setLogger(std::make_shared<TestLogger>());

    std::cout << "Connecting..." << std::endl;

    auto client = IgniteClient::startAsync(cfg).get();

    node.stop();
}