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
        auto execution = m_client.get_compute().submit(job_target::any_node(cluster_nodes), m_echo_job, {value});
        auto result = execution.get_result();

        ASSERT_TRUE(result.has_value());
        EXPECT_EQ(result.value().get_primitive().template get<T>(), value);

        execution = m_client.get_compute().submit(job_target::any_node(cluster_nodes), m_to_string_job, {value});
        result = execution.get_result();

        ASSERT_TRUE(result.has_value());
        auto res_str = result.value().get_primitive().template get<std::string>();
        if (res_str != expected_str)
            throw ignite_error("Expected equality of these values: '" + res_str + "' and '" + expected_str + "'");
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

    /** Node name job. */
    std::shared_ptr<job_descriptor> m_node_name_job{job_descriptor::builder(NODE_NAME_JOB).build()};

    /** Echo job. */
    std::shared_ptr<job_descriptor> m_echo_job{job_descriptor::builder(ECHO_JOB).build()};

    /** Concat job. */
    std::shared_ptr<job_descriptor> m_concat_job{job_descriptor::builder(CONCAT_JOB).build()};

    /** Error job. */
    std::shared_ptr<job_descriptor> m_error_job{job_descriptor::builder(ERROR_JOB).build()};

    /** Sleep job. */
    std::shared_ptr<job_descriptor> m_sleep_job{job_descriptor::builder(SLEEP_JOB).build()};

    /** ToString job. */
    std::shared_ptr<job_descriptor> m_to_string_job{job_descriptor::builder(TO_STRING_JOB).build()};

    /** Return null job. */
    std::shared_ptr<job_descriptor> m_return_null_job{job_descriptor::builder(RETURN_NULL_JOB).build()};
};

TEST_F(compute_test, get_cluster_nodes) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    std::sort(cluster_nodes.begin(), cluster_nodes.end(),
        [](const auto &n1, const auto &n2) { return n1.get_name() < n2.get_name(); });

    ASSERT_EQ(4, cluster_nodes.size());

    EXPECT_EQ(3344, cluster_nodes[0].get_address().port);
    EXPECT_EQ(3345, cluster_nodes[1].get_address().port);

    EXPECT_FALSE(cluster_nodes[0].get_address().host.empty());
    EXPECT_FALSE(cluster_nodes[1].get_address().host.empty());

    EXPECT_EQ(cluster_nodes[0].get_address().host, cluster_nodes[1].get_address().host);
}

TEST_F(compute_test, execute_on_random_node) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    auto execution = m_client.get_compute().submit(job_target::any_node(cluster_nodes), m_node_name_job, {});
    auto result = execution.get_result();

    ASSERT_TRUE(result.has_value());
    EXPECT_THAT(result.value().get_primitive().get<std::string>(), ::testing::StartsWith(PLATFORM_TEST_NODE_RUNNER));
}

TEST_F(compute_test, execute_on_specific_node) {
    auto execution1 = m_client.get_compute().submit(job_target::node(get_node(0)), m_node_name_job, {"-11"});
    auto execution2 = m_client.get_compute().submit(job_target::node(get_node(1)), m_node_name_job, {42});

    auto res1 = execution1.get_result();
    auto res2 = execution2.get_result();

    ASSERT_TRUE(res1.has_value());
    ASSERT_TRUE(res2.has_value());

    EXPECT_EQ(res1.value().get_primitive().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "-11");
    EXPECT_EQ(res2.value().get_primitive().get<std::string>(), PLATFORM_TEST_NODE_RUNNER + "_242");
}

TEST_F(compute_test, execute_broadcast_one_node) {
    auto res = m_client.get_compute().submit_broadcast(broadcast_job_target::node(get_node(1)), m_node_name_job, {"42"});
    auto execs = res.get_job_executions();

    ASSERT_EQ(execs.size(), 1);

    ASSERT_TRUE(execs.front().has_value());
    EXPECT_EQ(execs.front().value().get_result()->get_primitive(), PLATFORM_TEST_NODE_RUNNER + "_242");
}

TEST_F(compute_test, execute_broadcast_all_nodes) {
    auto res = m_client.get_compute().submit_broadcast(broadcast_job_target::nodes(get_node_set()), m_node_name_job, {"42"});
    auto execs = res.get_job_executions();

    ASSERT_EQ(execs.size(), 4);

    std::sort(execs.begin(), execs.end(), [] (auto &n1, auto &n2) {
        return n1.value().get_node().get_name() < n2.value().get_node().get_name();
    });

    EXPECT_EQ(execs[0].value().get_result()->get_primitive().get<std::string>(), get_node(0).get_name() + "42");
    EXPECT_EQ(execs[1].value().get_result()->get_primitive().get<std::string>(), get_node(1).get_name() + "42");
    EXPECT_EQ(execs[2].value().get_result()->get_primitive().get<std::string>(), get_node(2).get_name() + "42");
    EXPECT_EQ(execs[3].value().get_result()->get_primitive().get<std::string>(), get_node(3).get_name() + "42");
}

TEST_F(compute_test, job_error_propagates_to_client) {
    auto cluster_nodes = m_client.get_cluster_nodes();

    EXPECT_THROW(
        {
            try {
                m_client.get_compute().submit(job_target::any_node(cluster_nodes), m_error_job, {"unused"}).get_result();
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

TEST_F(compute_test, unknown_node_execute_throws) {
    auto unknown_node = cluster_node(uuid(1, 2), "random", {"127.0.0.1", 1234});

    EXPECT_THROW(
        {
            try {
                m_client.get_compute().submit(job_target::node(unknown_node), m_echo_job, {"unused"});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(),
                    testing::HasSubstr("None of the specified nodes are present in the cluster: [random]"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, unknown_node_broadcast_throws) {
    auto unknown_node = cluster_node(uuid(1, 2), "random", {"127.0.0.1", 1234});

    auto results =
        m_client.get_compute().submit_broadcast(broadcast_job_target::node(unknown_node), m_echo_job, {"unused"});

    EXPECT_TRUE(results.get_job_executions()[0].has_error());

    auto& e = results.get_job_executions()[0].error();
    EXPECT_THAT(e.what_str(), testing::HasSubstr("None of the specified nodes are present in the cluster: [random]"));
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
    try {
        check_argument<float>(std::numeric_limits<float>::min(), "1.17549435E-38");
    } catch (ignite_error &) {
        check_argument<float>(std::numeric_limits<float>::min(), "1.1754944E-38");
    }

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

TEST_F(compute_test, submit_colocated) {
    std::map<std::int32_t, std::string> nodes_for_values = {{1, "_2"}, {5, "_4"}, {9, ""}, {10, "_2"}, {11, "_4"}};

    for (const auto &var : nodes_for_values) {
        SCOPED_TRACE("key=" + std::to_string(var.first) + ", node=" + var.second);
        auto key = get_tuple(var.first);

        auto execution = m_client.get_compute().submit(job_target::colocated(TABLE_1, key), m_node_name_job, {});
        auto res_node_name = execution.get_result();
        auto expected_node_name = PLATFORM_TEST_NODE_RUNNER + var.second;

        EXPECT_EQ(expected_node_name, res_node_name.value().get_primitive().get<std::string>());
    }
}

TEST_F(compute_test, execute_colocated_throws_when_table_does_not_exist) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().submit(job_target::colocated("UNKNOWN_TABLE", get_tuple(42)), m_echo_job, {});
            } catch (const ignite_error &e) {
                EXPECT_STREQ("Table does not exist: 'PUBLIC.UNKNOWN_TABLE'", e.what());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, execute_colocated_throws_when_key_column_is_missing) {
    EXPECT_THROW(
        {
            try {
                (void) m_client.get_compute().submit(job_target::colocated(TABLE_1, get_tuple("some")), m_echo_job, {});
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
                (void) m_client.get_compute().submit(job_target::colocated(TABLE_1, {}), m_echo_job, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"unknown"}})
                    .build();

                (void) m_client.get_compute().submit(job_target::any_node(cluster_nodes), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"unknown", "1.2.3"}})
                    .build();

                (void) m_client.get_compute().submit(job_target::any_node(cluster_nodes), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"unknown", "1.2.3"}})
                    .build();

                (void) comp.submit(job_target::colocated(TABLE_1, get_tuple(1)), job_desc, {});
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), ::testing::HasSubstr("Deployment unit unknown:1.2.3 doesn't exist"));
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, broadcast_unknown_unit_and_version) {
    auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
        .deployment_units({{"unknown", "1.2.3"}})
        .build();

    auto res = m_client.get_compute().submit_broadcast(broadcast_job_target::node(get_node(1)), job_desc, {});
    auto execs = res.get_job_executions();

    ASSERT_EQ(execs.size(), 1);

    auto &exec1 = execs.front();
    ASSERT_TRUE(exec1.has_error());
    EXPECT_THAT(exec1.error().what_str(), ::testing::HasSubstr("Deployment unit unknown:1.2.3 doesn't exist"));
}

TEST_F(compute_test, execute_empty_unit_name) {
    EXPECT_THROW(
        {
            try {
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{""}})
                    .build();

                (void) m_client.get_compute().submit(job_target::node(get_node(1)), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"some", ""}})
                    .build();

                (void) m_client.get_compute().submit(job_target::node(get_node(1)), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{""}})
                    .build();

                (void) m_client.get_compute().submit_broadcast(broadcast_job_target::node(get_node(1)), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"some", ""}})
                    .build();

                (void) m_client.get_compute().submit_broadcast(broadcast_job_target::node(get_node(1)), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{""}})
                    .build();

                (void) m_client.get_compute().submit(job_target::colocated(TABLE_1, get_tuple(1)), job_desc, {});
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
                auto job_desc = job_descriptor::builder(NODE_NAME_JOB)
                    .deployment_units({{"some", ""}})
                    .build();

                comp.submit(job_target::colocated(TABLE_1, get_tuple(1)), job_desc, {});
            } catch (const ignite_error &e) {
                EXPECT_EQ("Deployment unit version can not be empty", e.what_str());
                throw;
            }
        },
        ignite_error);
}

TEST_F(compute_test, job_execution_status_executing) {
    const std::int32_t sleep_ms = 3000;

    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_sleep_job, {sleep_ms});

    auto state = execution.get_state();

    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(job_status::EXECUTING, state->status);
}

TEST_F(compute_test, job_execution_status_completed) {
    const std::int32_t sleep_ms = 1;

    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_sleep_job, {sleep_ms});
    execution.get_result();

    auto state = execution.get_state();

    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(job_status::COMPLETED, state->status);
}

TEST_F(compute_test, job_execution_status_failed) {
    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_error_job, {"unused"});

    EXPECT_THROW(
        {
            try {
                execution.get_result();
            } catch (const ignite_error &e) {
                EXPECT_THAT(e.what_str(), testing::HasSubstr("Custom job error"));
                throw;
            }
        },
        ignite_error);

    auto state = execution.get_state();

    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(job_status::FAILED, state->status);
}

TEST_F(compute_test, job_execution_cancel) {
    constexpr std::int32_t sleep_ms = 5000;

    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_sleep_job, {sleep_ms});
    execution.cancel();

    auto state = execution.get_state();

    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(job_status::CANCELED, state->status);
}

TEST_F(compute_test, job_execution_change_priority) {
    constexpr std::int32_t sleep_ms = 5000;

    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_sleep_job, {sleep_ms});
    auto res = execution.change_priority(123);

    EXPECT_EQ(res, job_execution::operation_result::INVALID_STATE);
}

TEST_F(compute_test, job_execution_return_null) {

    auto execution = m_client.get_compute().submit(job_target::node(get_node(1)), m_return_null_job, {});
    execution.get_result();

    auto state = execution.get_state();

    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(job_status::COMPLETED, state->status);
}
