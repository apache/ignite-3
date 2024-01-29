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
#include <limits>

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

    /**
     * Get specific node.
     * @param id Node id.
     * @return Node.
     */
    cluster_node get_node(size_t id) {
        auto nodes = m_client.get_cluster_nodes();
        std::sort(
            nodes.begin(), nodes.end(), [](const auto &n1, const auto &n2) { return n1.get_name() < n2.get_name(); });
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

    /**
     * Check that passed argument returned in a specific string form.
     *
     * @tparam T Type of the argument.
     * @param value Argument.
     * @param expected_str Expected string form.
     */
    template<typename T>
    void check_argument(T value, const std::string &expected_str) {
        auto cluster_nodes = m_client.get_cluster_nodes();
        auto result = m_client.get_compute().execute(cluster_nodes, {}, ECHO_JOB, {value, expected_str});

        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().template get<T>(), value);
    }

    /**
     * Check that passed argument returned in an expected string form.
     *
     * @tparam T Type of the argument.
     * @param value Argument.
     */
    template<typename T>
    void check_argument(T value) {
        check_argument(std::move(value), std::to_string(value));
    }

    /** Ignite client. */
    ignite_client m_client;
};

TEST_F(compute_test, get_cluster_nodes) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    std::sort(cluster_nodes.begin(), cluster_nodes.end(),
        [](const auto &n1, const auto &n2) { return n1.get_name() < n2.get_name(); });

    ASSERT_EQ(4, cluster_nodes.size());

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

    auto result = m_client.get_compute().execute(cluster_nodes, {}, NODE_NAME_JOB, {});

    ASSERT_TRUE(result.has_value());
    EXPECT_THAT(result.value().get<std::string>(), ::testing::StartsWith(PLATFORM_TEST_NODE_RUNNER));
}

TEST_F(compute_test, execute_on_specific_node) {
    auto res1 = m_client.get_compute().execute({get_node(0)}, {}, NODE_NAME_JOB, {"-", 11});
    auto res2 = m_client.get_compute().execute({get_node(1)}, {}, NODE_NAME_JOB, {":", 22});

    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res2.has_value());

    EXPECT_EQ(res1.value().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "-_11");
    EXPECT_EQ(res2.value().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "_2:_22");
}

TEST_F(compute_test, execute_broadcast_one_node) {
    auto res = m_client.get_compute().broadcast({get_node(1)}, {}, NODE_NAME_JOB, {"42"});

    ASSERT_EQ(res.size(), 1);

    EXPECT_EQ(res.begin()->first, get_node(1));

    ASSERT_TRUE(res.begin()->second.has_value());
    EXPECT_EQ(res.begin()->second.value(), PLATFORM_TEST_NODE_RUNNER + "_242");
}

TEST_F(compute_test, execute_broadcast_all_nodes) {
    auto res = m_client.get_compute().broadcast(get_node_set(), {}, NODE_NAME_JOB, {"42"});

    ASSERT_EQ(res.size(), 4);

    EXPECT_EQ(res[get_node(0)].value(), get_node(0).get_name() + "42");
    EXPECT_EQ(res[get_node(1)].value(), get_node(1).get_name() + "42");
    EXPECT_EQ(res[get_node(2)].value(), get_node(2).get_name() + "42");
    EXPECT_EQ(res[get_node(3)].value(), get_node(3).get_name() + "42");
}

TEST_F(compute_test, execute_with_args) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    auto result = m_client.get_compute().execute(cluster_nodes, {}, CONCAT_JOB, {5.3, uuid(), "42", nullptr});

    ASSERT_TRUE(result.has_value());
    EXPECT_EQ(result.value().get<std::string>(), "5.3_00000000-0000-0000-0000-000000000000_42_null");
}

TEST_F(compute_test, job_error_propagates_to_client) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    EXPECT_THROW(
        {
            try {
                m_client.get_compute().execute(cluster_nodes, {}, ERROR_JOB, {"unused"});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Custom job error"));
                // TODO https://issues.apache.org/jira/browse/IGNITE-19603
                // EXPECT_THAT(e.what_str(),
                //     testing::HasSubstr(
                //         "org.apache.ignite.internal.runner.app.client.ItThinClientComputeTest$CustomException"));
                // EXPECT_THAT(e.what_str(), testing::HasSubstr("IGN-TBL-3"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, unknown_node_throws) {
    auto unknown_node = cluster_node("some", "random", {"127.0.0.1", 1234});

    EXPECT_THROW(
        {
            try {
                m_client.get_compute().execute({unknown_node}, {}, ECHO_JOB, {"unused"});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Specified node is not present in the cluster: random"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, all_arg_types) {
    check_argument<std::int8_t>(42);
    check_argument<std::int8_t>(std::numeric_limits<std::int8_t>::max());
    check_argument<std::int8_t>(std::numeric_limits<std::int8_t>::min());

    check_argument<std::int16_t>(4242);
    check_argument<std::int16_t>(std::numeric_limits<std::int16_t>::max());
    check_argument<std::int16_t>(std::numeric_limits<std::int16_t>::min());

    check_argument<std::int32_t>(424242);
    check_argument<std::int32_t>(std::numeric_limits<std::int32_t>::max());
    check_argument<std::int32_t>(std::numeric_limits<std::int32_t>::min());

    check_argument<std::int64_t>(424242424242);
    check_argument<std::int64_t>(std::numeric_limits<std::int64_t>::max());
    check_argument<std::int64_t>(std::numeric_limits<std::int64_t>::min());

    check_argument<float>(0.123456f);
    check_argument<float>(std::numeric_limits<float>::max(), "3.4028235E38");
    check_argument<float>(std::numeric_limits<float>::min(), "1.17549435E-38");

    check_argument<double>(0.987654);
    check_argument<double>(std::numeric_limits<double>::max(), "1.7976931348623157E308");
    check_argument<double>(std::numeric_limits<double>::min(), "2.2250738585072014E-308");

    check_argument<big_decimal>({123456, 3}, "123.456");
    check_argument<big_decimal>({}, "0");
    check_argument<big_decimal>({1, 0}, "1");

    auto str_dec = "12345678909876543211234567890.987654321";
    check_argument<big_decimal>(big_decimal(str_dec), str_dec);

    check_argument<ignite_date>({2021, 11, 18}, "2021-11-18");
    check_argument<ignite_time>({13, 8, 55, 266574889}, "13:08:55.266574889");
    check_argument<ignite_date_time>({{2021, 11, 18}, {13, 8, 55, 266574889}}, "2021-11-18T13:08:55.266574889");

    check_argument<uuid>({0, 0}, "00000000-0000-0000-0000-000000000000");
    check_argument<uuid>({0x123e4567e89b12d3, 0x7456426614174000}, "123e4567-e89b-12d3-7456-426614174000");
}

TEST_F(compute_test, execute_colocated) {
    std::map<std::int32_t, std::string> nodes_for_values = {{1, "_2"}, {5, "_4"}, {9, ""}, {10, "_2"}, {11, "_4"}};

    for (const auto &var : nodes_for_values) {
        SCOPED_TRACE("key=" + std::to_string(var.first) + ", node=" + var.second);
        auto key = get_tuple(var.first);

        auto res_node_name = m_client.get_compute().execute_colocated(TABLE_1, key, {}, NODE_NAME_JOB, {});
        auto expected_node_name = PLATFORM_TEST_NODE_RUNNER + var.second;

        EXPECT_EQ(expected_node_name, res_node_name.value().get<std::string>());
    }
}

TEST_F(compute_test, execute_colocated_throws_when_table_does_not_exist) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute_colocated("unknownTable", get_tuple(42), {}, ECHO_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Table does not exist: 'unknownTable'", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_throws_when_key_column_is_missing) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute_colocated(TABLE_1, get_tuple("some"), {}, ECHO_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Missed key column: KEY"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_throws_when_key_is_empty) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute_colocated(TABLE_1, {}, {}, ECHO_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Key tuple can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, exception_in_server_job_propogates_to_client) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute_colocated(TABLE_1, {}, {}, ECHO_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Key tuple can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, unknown_unit) {
    EXPECT_THROW(
        {
            try {
                auto cluster_nodes = m_client.get_cluster_nodes();
                (void) m_client.get_compute().execute(cluster_nodes, {{"unknown"}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Deployment unit unknown:latest doesn't exist"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_unknown_unit_and_version) {
    EXPECT_THROW(
        {
            try {
                auto cluster_nodes = m_client.get_cluster_nodes();
                (void) m_client.get_compute().execute(cluster_nodes, {{"unknown", "1.2.3"}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Deployment unit unknown:1.2.3 doesn't exist"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_unknown_unit_and_version) {
    EXPECT_THROW(
        {
            try {
                auto comp = m_client.get_compute();
                (void) comp.execute_colocated(TABLE_1, get_tuple(1), {{"unknown", "1.2.3"}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Deployment unit unknown:1.2.3 doesn't exist"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, broadcast_unknown_unit_and_version) {
    auto res = m_client.get_compute().broadcast({get_node(1)}, {{"unknown", "1.2.3"}}, NODE_NAME_JOB, {});

    ASSERT_EQ(res.size(), 1);

    auto &res1 = res[get_node(1)];
    ASSERT_TRUE(res1.has_error());
    EXPECT_THAT(res1.error().what_str(), ::testing::HasSubstr("Deployment unit unknown:1.2.3 doesn't exist"));
}

TEST_F(compute_test, execute_empty_unit_name) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute({get_node(1)}, {{""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit name can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_empty_unit_version) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute({get_node(1)}, {{"some", ""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit version can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, broadcast_empty_unit_name) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().broadcast({get_node(1)}, {{""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit name can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, broadcast_empty_unit_version) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().broadcast({get_node(1)}, {{"some", ""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit version can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_empty_unit_name) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().execute_colocated(TABLE_1, get_tuple(1), {{""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit name can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_empty_unit_version) {
    EXPECT_THROW(
        {
            try {
                auto comp = m_client.get_compute();
                comp.execute_colocated(TABLE_1, get_tuple(1), {{"some", ""}}, NODE_NAME_JOB, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit version can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}
