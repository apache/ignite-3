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

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class client_test : public ignite_runner_suite { };

TEST_F(client_test, get_configuration) {
    IgniteClientConfiguration cfg{NODE_ADDRS};
    cfg.setLogger(get_logger());
    cfg.setConnectionLimit(42);

    auto client = IgniteClient::start(cfg, std::chrono::seconds(5));

    const auto &cfg2 = client.getConfiguration();

    EXPECT_EQ(cfg.getEndpoints(), cfg2.getEndpoints());
    EXPECT_EQ(cfg.getConnectionLimit(), cfg2.getConnectionLimit());
}
