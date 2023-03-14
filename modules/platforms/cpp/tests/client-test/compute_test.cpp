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

#include <gmock/gmock-matchers.h>
#include <gtest/gtest.h>

#include <chrono>

using namespace ignite;

/**
 * Test suite.
 */
class compute_test : public ignite_runner_suite {
protected:
    void SetUp() override {
        ignite_client_configuration cfg{get_node_addrs()};
        cfg.set_logger(get_logger());

        m_client = ignite_client::start(cfg, std::chrono::seconds(30));
    }

    void TearDown() override {
        // remove all
    }

    /**
     * Get specific node.
     * @param id Node id.
     * @return Node.
     */
    cluster_node get_node(size_t id) {
        auto nodes = m_client.get_cluster_nodes();
        std::sort(nodes.begin(), nodes.end(), [] (const auto &n1, const auto &n2) {
            return n1.get_name() < n2.get_name();
        });
        return nodes[id];
    }

    /**
     * Get nodes as set.
     * @return Nodes in set.
     */
    std::set<cluster_node> get_node_set() {
        auto nodes = m_client.get_cluster_nodes();
        return {nodes.begin(), nodes.end()};
    }

    /** Ignite client. */
    ignite_client m_client;
};

TEST_F(compute_test, get_cluster_nodes) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    std::sort(cluster_nodes.begin(), cluster_nodes.end(), [] (const auto &n1, const auto &n2) {
        return n1.get_name() < n2.get_name();
    });

    ASSERT_EQ(2, cluster_nodes.size());

    EXPECT_FALSE(cluster_nodes[0].get_id().empty());
    EXPECT_FALSE(cluster_nodes[1].get_id().empty());

    EXPECT_EQ(3344, cluster_nodes[0].get_address().port);
    EXPECT_EQ(3345, cluster_nodes[1].get_address().port);

    EXPECT_FALSE(cluster_nodes[0].get_address().host.empty());
    EXPECT_FALSE(cluster_nodes[1].get_address().host.empty());

    EXPECT_EQ(cluster_nodes[0].get_address().host, cluster_nodes[1].get_address().host);
}

TEST_F(compute_test, execute_on_random_node) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    auto result = m_client.get_compute().execute(cluster_nodes, NODE_NAME_JOB, {});

    ASSERT_TRUE(result.has_value());
    EXPECT_THAT(result.value().get<std::string>(), ::testing::StartsWith(PLATFORM_TEST_NODE_RUNNER));
}

TEST_F(compute_test, execute_on_specific_node) {
    auto res1 = m_client.get_compute().execute({get_node(0)}, NODE_NAME_JOB, {"-", 11});
    auto res2 = m_client.get_compute().execute({get_node(1)}, NODE_NAME_JOB, {":", 22});

    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res2.has_value());

    EXPECT_EQ(res1.value().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "-_11");
    EXPECT_EQ(res2.value().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "_2:_22");
}

TEST_F(compute_test, execute_broadcast_one_node) {
    auto res = m_client.get_compute().broadcast({get_node(1)}, NODE_NAME_JOB, {"42"});

    ASSERT_EQ(res.size(), 1);

    EXPECT_EQ(res.begin()->first, get_node(1));

    ASSERT_TRUE(res.begin()->second.has_value());
    EXPECT_EQ(res.begin()->second.value(), PLATFORM_TEST_NODE_RUNNER + "_242");
}

TEST_F(compute_test, execute_broadcast_all_nodes) {
    auto res = m_client.get_compute().broadcast(get_node_set(), NODE_NAME_JOB, {"42"});

    ASSERT_EQ(res.size(), 2);

    EXPECT_EQ(res[get_node(0)].value(), get_node(0).get_name() + "42");
    EXPECT_EQ(res[get_node(1)].value(), get_node(1).get_name() + "42");
}

TEST_F(compute_test, execute_with_args) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    auto result = m_client.get_compute().execute(cluster_nodes, CONCAT_JOB, {5.3, uuid(), "42", nullptr});

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().get<std::string>(), "5.3_00000000-0000-0000-0000-000000000000_42_null");
}





