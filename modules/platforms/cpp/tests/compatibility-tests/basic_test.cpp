// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#include "tests/client-test/ignite_runner_suite.h"

using namespace ignite;

class basic_test_ign_version : public ignite::ignite_runner_suite {
private:
    static ignite_client ConnectToCluster() {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());
        return ignite_client::start(cfg, std::chrono::seconds(30));
    }

protected:
    void SetUp() override {
        m_client = ConnectToCluster();

        std::cout << "CompatibilityServer version" << ignite_runner::COMPATIBILITY_VERSION << "\n";
    }

    ignite_client m_client;
};


TEST_F(basic_test_ign_version, get_cluster_nodes_successful) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    ASSERT_GE(cluster_nodes.size(), 1);
}