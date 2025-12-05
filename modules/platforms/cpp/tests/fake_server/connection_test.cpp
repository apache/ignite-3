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

#include "tests/client-test/ignite_runner_suite.h"
#include "ignite/client/ignite_client.h"
#include "fake_server.h"

#include <gtest/gtest.h>
#include <thread>

using namespace ignite;

class connection_test : public ignite_runner_suite {

};


TEST_F(connection_test, handshake_with_fake_server) {
    using namespace std::chrono_literals;
    fake_server fs{};

    fs.start();

    ignite_client_configuration cfg;
    cfg.set_logger(get_logger());
    cfg.set_endpoints({"127.0.0.1:10800"});

    auto cl = ignite_client::start(cfg, 5s);
}

TEST_F(connection_test, request_timeout) {
    using namespace std::chrono_literals;
    fake_server fs{
        10800,
        [](protocol::client_operation op) -> std::unique_ptr<response_action> {
            switch (op) {
                case protocol::client_operation::CLUSTER_GET_NODES:
                    return std::make_unique<drop_action>();
                default:
                    return nullptr;
            }
        }
    };

    fs.start();

    ignite_client_configuration cfg;
    cfg.set_logger(get_logger());
    cfg.set_endpoints({"127.0.0.1:10800"});
    cfg.set_operation_timeout(std::chrono::milliseconds{100});

    auto cl = ignite_client::start(cfg, 5s);

    try {
        auto cluster_nodes = cl.get_cluster_nodes();
    } catch (ignite_error& err) {
        EXPECT_EQ(error::code::CLIENT_OPERATION_TIMEOUT, err.get_status_code());
    }
}