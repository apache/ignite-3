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
};

// TODO: implement test logger
class TestLogger : public IgniteLogger
{
public:
    void logError(std::string_view message) override
    {
        std::cout << getTimestamp() << " [ERROR]   " << message << std::endl;
    }

    void logWarning(std::string_view message) override
    {
        std::cout << getTimestamp() << " [WARNING] " << message << std::endl;
    }

    void logInfo(std::string_view message) override
    {
        std::cout << getTimestamp() << " [INFO]    " << message << std::endl;
    }

    void logDebug(std::string_view message) override
    {
        std::cout << getTimestamp() << " [DEBUG]  " << message << std::endl;
    }

private:
    static std::string getTimestamp()
    {
        using clock = std::chrono::system_clock;

        auto now = clock::now();
        auto cTime = clock::to_time_t(now);

        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());

        std::stringstream ss;
        ss << std::put_time(std::localtime(&cTime), "%H:%M:%S.") << std::setw(3) << std::setfill('0') << (ms.count() % 1000);
        return ss.str();
    }
};

TEST_F(ClientTest, GetConfiguration)
{
    IgniteClientConfiguration cfg{"127.0.0.1:10942"};
    cfg.setLogger(std::make_shared<TestLogger>());

    auto client = IgniteClient::start(cfg, std::chrono::seconds(5));

    const auto& cfg2 = client.getConfiguration();

    EXPECT_EQ(cfg.getEndpoints(), cfg2.getEndpoints());
}

TEST_F(ClientTest, TableGetUpsert)
{
    IgniteClientConfiguration cfg{"127.0.0.1:10942"};
    cfg.setLogger(std::make_shared<TestLogger>());

    auto client = IgniteClient::startAsync(cfg, std::chrono::seconds(5)).get();

    auto tables = client.getTables();
    auto tableUnknown = tables.getTableAsync("PUB.some_unknown").get();

    EXPECT_FALSE(tableUnknown.has_value());

    auto table = tables.getTableAsync("PUB.tbl1").get();

    ASSERT_TRUE(table.has_value());
    EXPECT_EQ(table->getName(), "PUB.tbl1");
}
