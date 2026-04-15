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

#include "detail/string_extensions.h"
#include "ignite_runner_suite.h"
#include "tests/test-common/test_utils.h"

#include "ignite/client/detail/utils.h"
#include "ignite/client/ignite_client.h"
#include "ignite/client/ignite_client_configuration.h"

#include "proxy/asio_proxy.h"
#include "proxy/message_listener.h"

#include <gtest/gtest.h>

namespace ignite {

using namespace std::chrono_literals;

using part_id_type = std::size_t;
using id_type = std::int64_t;

constexpr id_type MIN_ID = 0;
constexpr id_type MAX_ID = 1000;

static const std::map<std::string, std::string> NODES_TO_ADDR = {
    {"org.apache.ignite.internal.runner.app.PlatformTestNodeRunner", "127.0.0.1:10942"},
    {"org.apache.ignite.internal.runner.app.PlatformTestNodeRunner_2", "127.0.0.1:10943"}
};

/**
 * Parses string-encoded partition distribution.
 * Example of string:
 * ff979603-fb56-49e9-bc79-7c4487bbbafd=[4];2e14bb82-5cb5-4283-aafd-f2d8552a842a=[5, 8, 9];
 * @param encoded String-encoded distribution
 * @return Conveniently structured distribution.
 */
std::map<int64_t, uuid> parse_partition_distribution(const std::string& encoded) {
    std::map<int64_t, uuid> res;

    size_t lhs = 0;
    size_t rhs = std::string::npos;
    while ((rhs = encoded.find(';', lhs)) != std::string::npos) {
        std::string chunk = encoded.substr(lhs, rhs - lhs);

        auto eq_pos = chunk.find('=');

        if (eq_pos == std::string::npos) {
            throw std::runtime_error("Incorrect partition distribution text:" + encoded);
        }

        auto node_id = uuid::from_string(chunk.substr(0, eq_pos));

        if (!node_id.has_value()) {
            throw std::runtime_error("can't parse partition distribution, incorrect input:" + encoded);
        }

        std::string num_list = chunk.substr(eq_pos + 1); // e.g. [1,2,3]

        size_t lo = 1;
        size_t hi = std::string::npos;
        while ((hi = num_list.find_first_of(",]", lo)) != std::string::npos) {
            int64_t part_id = std::stoll(num_list.substr(lo, hi - lo));
            res[part_id] = *node_id;
            lo = hi + 1;
        }
        lhs = rhs + 1;
    }

    return res;
}

template<typename T>
class partition_awareness_test : public ignite_runner_suite {
private:
    T tab_info{};
protected:
    void SetUp() override {
        ignite_client_configuration client_cfg;
        client_cfg.set_endpoints(get_node_addrs());

        m_direct_client = ignite_client::start(client_cfg, 5s);

        auto nodes = m_direct_client.get_cluster_nodes();

        for (auto& node : nodes) {
            if (NODES_TO_ADDR.count(node.get_name())) {
                m_endpoint_to_node_id[NODES_TO_ADDR.at(node.get_name())] = node.get_id();
            }
        }

        ASSERT_EQ(m_endpoint_to_node_id.size(), get_node_addrs().size())
            << "Incorrect node names or address!"
            << "This test should be able to connect to all non-ssl nodes";

        drop_table_if_exists();

        create_table();

        collect_partition_distribution();

        setup_proxy();
    }

    void TearDown() override {
        drop_table_if_exists();
    }

    ignite_tuple key_tup(id_type key) {
        return get_tuple(tab_info.key_column_names, tab_info.get_pk_vals(key));
    }

    ignite_tuple main_val_tup(id_type key) {
        return get_tuple(tab_info.non_key_column_names, tab_info.get_main_vals(key));
    }

    ignite_tuple alt_val_tup(id_type key) {
        return get_tuple(tab_info.non_key_column_names, tab_info.get_alt_vals(key));
    }

    ignite_tuple main_rec(id_type key) {
        return get_tuple(tab_info.all_column_names, tab_info.get_main_rec(key));
    }

    ignite_tuple alt_rec(id_type key) {
        return get_tuple(tab_info.all_column_names, tab_info.get_alt_rec(key));
    }

    void create_table() {
        m_direct_client.get_sql().execute(nullptr, nullptr, {tab_info.create_table_ddl()}, {});
    }

    void drop_table_if_exists() {
        m_direct_client.get_sql().execute(nullptr, nullptr, {"drop table if exists " + tab_info.table_name}, {});
    }

    void collect_partition_distribution() {
        std::map<id_type, int64_t> key_to_part;

        populate_table();

        std::stringstream sql;
        sql << "select ID " << ", " << PART_PSEUDOCOLUMN << " from " << tab_info.table_name << " order by ID";

        auto rs = m_direct_client.get_sql().execute(nullptr, nullptr, {sql.str()}, {});
        do {
            auto page = rs.current_page();

            for (auto &rec : page) {
                auto key = rec.get("ID").get<id_type>();
                auto part_id = rec.get(PART_PSEUDOCOLUMN).get<int64_t>();

                key_to_part[key] = part_id;
            }

            if (rs.has_more_pages())
                rs.fetch_next_page();
            else
                break;
        } while (true);

        auto job_exec = m_direct_client.get_compute().submit(job_target::any_node(m_direct_client.get_cluster_nodes()),
            job_descriptor::builder{GET_PART_DISTRIBUTION_JOB}.build(), {tab_info.table_name});

        auto res = job_exec.get_result()->get_primitive().template get<std::string>();

        auto part_to_node = parse_partition_distribution(res);

        for (auto [key, part_id] : key_to_part) {
            m_key_distribution[key] = part_to_node[part_id];
        }

        clear_table();
    }

    void populate_table() {
        auto view = m_direct_client.get_tables().get_table(tab_info.table_name)->get_key_value_binary_view();

        for (int64_t key = MIN_ID; key < MAX_ID; ++key) {
            view.put(nullptr, this->key_tup(key), this->main_val_tup(key));
        }
    }

    void clear_table() {
        std::stringstream sql;
        sql << "delete from " << tab_info.table_name;

        m_direct_client.get_sql().execute(nullptr, nullptr, {sql.str()}, {});
    }

    void setup_proxy() {
        auto srv_endpoints = get_node_addrs();

        std::vector<proxy::configuration> proxy_cfg;
        proxy_cfg.reserve(srv_endpoints.size());

        std::vector<std::string> proxy_endpoints;
        proxy_endpoints.reserve(srv_endpoints.size());

        std::uint16_t proxy_port = 50000;
        for (auto& srv_endpoint: srv_endpoints) {
            auto node_id = m_endpoint_to_node_id.at(srv_endpoint);

            auto in_listener = std::make_shared<proxy::message_listener>();
            auto out_listener = std::make_shared<proxy::message_listener>();
            m_in_listeners[node_id] = in_listener;
            m_out_listeners[node_id] = out_listener;

            proxy_cfg.emplace_back(proxy_port, srv_endpoint, in_listener, out_listener);
            proxy_endpoints.push_back("127.0.0.1:" + std::to_string(proxy_port));

            proxy_port++;
        }

        proxy = std::make_unique<proxy::asio_proxy>(proxy_cfg, get_logger());

        ignite_client_configuration client_cfg;
        client_cfg.set_endpoints(proxy_endpoints);
        client_cfg.set_logger(get_logger());

        m_client = ignite_client::start(client_cfg, 5s);

        m_kv_view = m_client.get_tables().get_table(tab_info.table_name)->get_key_value_binary_view();
        m_rec_view = m_client.get_tables().get_table(tab_info.table_name)->get_record_binary_view();

        {
            // we initiate initial partition assignment load to reduce tests flakiness on first records
            auto res = m_kv_view.get(nullptr, this->key_tup(42));

            auto start = std::chrono::steady_clock::now();

            int total = 0;
            while (total == 0) {
                for (auto &[_, node_id] : m_endpoint_to_node_id) {
                    total += count_op_by_node_id(node_id, protocol::client_operation::PARTITION_ASSIGNMENT_GET);
                }

                if (std::chrono::steady_clock::now() - start < 3s) {
                    std::this_thread::yield();
                } else {
                    FAIL() << "Test didn't wait for partition assignment";
                }
            }
        }
    }

    void toggle_message_registration(bool enable) {
        for (auto &[_, listener] : m_in_listeners) {
            listener->toggle_message_registration(enable);
        }

        for (auto &[_, listener] : m_out_listeners) {
            listener->toggle_message_registration(enable);
        }
    }

    /**
     * Count how many operations was sent directly to desired node.
     * @param node_id Node id.
     * @return Number of operations.
     */
    int count_op_by_node_id(uuid node_id, protocol::client_operation op) {
        auto in_listener = m_in_listeners.at(node_id);

        int found = 0;
        while (true) {
            auto msgs = in_listener->get_next<proxy::client_message>();

            if (msgs.empty()) {
                break;
            }

            for (auto &msg : msgs) {
                get_logger()->log_debug("Processed: node_id=" + detail::to_string(node_id)
                    + " req_id=" + std::to_string(msg.get_req_id()) + " op=" + std::to_string(int(msg.get_op())));

                if (msg.get_op() == op) {
                    found++;
                }
            }
        }

        return found;
    }

    void check_messages_sent_to_correct_nodes(id_type key, ignite::protocol::client_operation op) {
        auto this_node_id = m_key_distribution.at(key);

        // check if client connected to this node
        bool connected_to_this_node = m_in_listeners.count(this_node_id);

        int total = 0;
        for (auto &[ep, node_id] : m_endpoint_to_node_id) {
            auto cnt = count_op_by_node_id(node_id, op);

            total += cnt;
            if (connected_to_this_node) {
                if (this_node_id == node_id) {
                    EXPECT_EQ(1, cnt) << "Key " << key << " was not found among messages to correct node";
                } else if (this_node_id != node_id) {
                    EXPECT_EQ(0, cnt) << "Key " << key << " was found among messages to incorrect node";
                }
            }
        }

        if (!connected_to_this_node) {
            EXPECT_EQ(1, total) << "Key " << key
                                << " was not found among messages to connected nodes or message was duplicated";
        }
    }

    /**
     * Client which connected directly.
     */
    ignite_client m_direct_client;

    std::map<std::string, uuid> m_endpoint_to_node_id;

    std::map<id_type, uuid> m_key_distribution;

    /**
     * Client which connect through the proxy.
     */
    ignite_client m_client;

    key_value_view<ignite_tuple, ignite_tuple> m_kv_view;
    record_view<ignite_tuple> m_rec_view;

    std::unique_ptr<proxy::asio_proxy> proxy;

    std::map<uuid, std::shared_ptr<proxy::message_listener>> m_in_listeners;
    std::map<uuid, std::shared_ptr<proxy::message_listener>> m_out_listeners;
};

struct SimpleType {
    std::string table_name = "simple_table";
    std::vector<std::string> key_column_names = {"ID"};
    std::vector<std::string> collocation_column_names = {"ID"};
    std::vector<std::string> non_key_column_names = {"TEXT"};
    std::vector<std::string> all_column_names{};

    SimpleType() {
        all_column_names.reserve(key_column_names.size() + non_key_column_names.size());
        all_column_names.insert(all_column_names.end(), key_column_names.begin(), key_column_names.end());
        all_column_names.insert(all_column_names.end(), non_key_column_names.begin(), non_key_column_names.end());
    }

    static std::string create_table_ddl() {
        return "create table simple_table (id bigint primary key, text varchar)";
    }

    static std::vector<primitive> get_pk_vals(id_type key) {
        return {key};
    }

    /**
     * Values of columns which is part of collocation key.
     */
    static std::vector<primitive> get_ck_values(id_type key) {
        return {key};
    }

    static std::vector<primitive> get_main_vals(id_type key) {
        return {std::to_string(key)};
    }

    static std::vector<primitive> get_alt_vals(id_type key) {
        return {std::to_string(key + MAX_ID)};
    }

    static std::vector<primitive> get_main_rec(id_type key) {
        std::vector<primitive> res;

        auto key_part = get_pk_vals(key);
        auto non_key_part = get_main_vals(key);

        res.reserve(key_part.size() + non_key_part.size());

        res.insert(res.end(), std::make_move_iterator(key_part.begin()), std::make_move_iterator(key_part.end()));
        res.insert(res.end(), std::make_move_iterator(non_key_part.begin()), std::make_move_iterator(non_key_part.end()));

        return res;
    }

    static std::vector<primitive> get_alt_rec(id_type key) {
        std::vector<primitive> res;

        auto key_part = get_pk_vals(key);
        auto non_key_part = get_alt_vals(key);

        res.reserve(key_part.size() + non_key_part.size());

        res.insert(res.end(), std::make_move_iterator(key_part.begin()), std::make_move_iterator(key_part.end()));
        res.insert(res.end(), std::make_move_iterator(non_key_part.begin()), std::make_move_iterator(non_key_part.end()));

        return res;
    }
};

struct TypeWithCollocationKey {
    std::string table_name = "table_with_collocation_key";
    std::vector<std::string> key_column_names = {"ID", "DEPT_ID"};
    std::vector<std::string> collocation_column_names = {"DEPT_ID"};
    std::vector<std::string> non_key_column_names = {"TEXT"};
    std::vector<std::string> all_column_names{};

    TypeWithCollocationKey() {
        all_column_names.reserve(key_column_names.size() + non_key_column_names.size());
        all_column_names.insert(all_column_names.end(), key_column_names.begin(), key_column_names.end());
        all_column_names.insert(all_column_names.end(), non_key_column_names.begin(), non_key_column_names.end());
    }

    static std::string create_table_ddl() {
        return "create table table_with_collocation_key "
               "(id bigint, dept_id bigint, text varchar, primary key (id, dept_id)) "
               "colocate by (dept_id)";
    }

    static std::vector<primitive> get_pk_vals(id_type key) {
        return {key, -key};
    }

    /**
     * Values of columns which is part of collocation key.
     */
    static std::vector<primitive> get_ck_values(id_type key) {
        return {-key};
    }

    static std::vector<primitive> get_main_vals(id_type key) {
        return {std::to_string(key)};
    }

    static std::vector<primitive> get_alt_vals(id_type key) {
        return {std::to_string(key + MAX_ID)};
    }

    static std::vector<primitive> get_main_rec(id_type key) {
        std::vector<primitive> res;

        auto key_part = get_pk_vals(key);
        auto non_key_part = get_main_vals(key);

        res.reserve(key_part.size() + non_key_part.size());

        res.insert(res.end(), std::make_move_iterator(key_part.begin()), std::make_move_iterator(key_part.end()));
        res.insert(res.end(), std::make_move_iterator(non_key_part.begin()), std::make_move_iterator(non_key_part.end()));

        return res;
    }

    static std::vector<primitive> get_alt_rec(id_type key) {
        std::vector<primitive> res;

        auto key_part = get_pk_vals(key);
        auto non_key_part = get_alt_vals(key);

        res.reserve(key_part.size() + non_key_part.size());

        res.insert(res.end(), std::make_move_iterator(key_part.begin()), std::make_move_iterator(key_part.end()));
        res.insert(res.end(), std::make_move_iterator(non_key_part.begin()), std::make_move_iterator(non_key_part.end()));

        return res;
    }
};

using TestTypes = ::testing::Types<SimpleType, TypeWithCollocationKey>;
TYPED_TEST_SUITE(partition_awareness_test, TestTypes);

TYPED_TEST(partition_awareness_test, kv_contains) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val = this->m_kv_view.contains(nullptr, this->key_tup(key));

        EXPECT_TRUE(val);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_CONTAINS_KEY);
    }
}

TYPED_TEST(partition_awareness_test, kv_get) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val = this->m_kv_view.get(nullptr, this->key_tup(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET);
    }
}

TYPED_TEST(partition_awareness_test, kv_get_and_put) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val = this->m_kv_view.get_and_put(nullptr, this->key_tup(key), this->alt_val_tup(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_UPSERT);
    }
}

TYPED_TEST(partition_awareness_test, kv_get_and_remove) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val = this->m_kv_view.get_and_remove(nullptr, this->key_tup(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_DELETE);
    }
}

TYPED_TEST(partition_awareness_test, kv_get_and_replace) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val = this->m_kv_view.get_and_replace(nullptr, this->key_tup(key), this->alt_val_tup(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_REPLACE);
    }
}

TYPED_TEST(partition_awareness_test, kv_replace) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        bool success = this->m_kv_view.replace(nullptr, this->key_tup(key), this->alt_val_tup(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_REPLACE);
    }
}

TYPED_TEST(partition_awareness_test, kv_replace_exact) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        bool success = this->m_kv_view.replace(nullptr, this->key_tup(key), this->main_val_tup(key), this->alt_val_tup(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_REPLACE_EXACT);
    }
}

TYPED_TEST(partition_awareness_test, kv_remove) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        bool success = this->m_kv_view.remove(nullptr, this->key_tup(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_DELETE);
    }
}

TYPED_TEST(partition_awareness_test, kv_remove_exact) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        bool success = this->m_kv_view.remove(nullptr, this->key_tup(key), this->main_val_tup(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_DELETE_EXACT);
    }
}

TYPED_TEST(partition_awareness_test, kv_put) {
    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        this->m_kv_view.put(nullptr, this->key_tup(key), this->main_val_tup(key));

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_UPSERT);
    }
}

TYPED_TEST(partition_awareness_test, rec_get) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val =  this->m_rec_view.get(nullptr, this->key_tup(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET);
    }
}

TYPED_TEST(partition_awareness_test, rec_get_and_upsert) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val =  this->m_rec_view.get_and_upsert(nullptr, this->alt_rec(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_UPSERT);
    }
}

TYPED_TEST(partition_awareness_test, rec_get_and_remove) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val =  this->m_rec_view.get_and_remove(nullptr, this->alt_rec(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_DELETE);
    }
}

TYPED_TEST(partition_awareness_test, rec_get_and_replace) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto val =  this->m_rec_view.get_and_replace(nullptr, this->alt_rec(key));

        EXPECT_TRUE(val.has_value());

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_GET_AND_REPLACE);
    }
}

TYPED_TEST(partition_awareness_test, rec_replace) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto success =  this->m_rec_view.replace(nullptr, this->alt_rec(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_REPLACE);
    }
}

TYPED_TEST(partition_awareness_test, rec_replace_exact) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto success =  this->m_rec_view.replace(nullptr, this->main_rec(key), this->alt_rec(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_REPLACE_EXACT);
    }
}

TYPED_TEST(partition_awareness_test, rec_remove) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto success =  this->m_rec_view.remove(nullptr, this->key_tup(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_DELETE);
    }
}

TYPED_TEST(partition_awareness_test, rec_remove_exact) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto success =  this->m_rec_view.remove_exact(nullptr, this->main_rec(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_DELETE_EXACT);
    }
}

TYPED_TEST(partition_awareness_test, rec_upsert) {
    this->populate_table();

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

         this->m_rec_view.upsert(nullptr, this->main_rec(key));

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_UPSERT);
    }
}

TYPED_TEST(partition_awareness_test, rec_insert) {

    for (id_type key = MIN_ID; key < MAX_ID; ++key) {

        auto success =  this->m_rec_view.insert(nullptr, this->main_rec(key));

        EXPECT_TRUE(success);

        this->check_messages_sent_to_correct_nodes(key, protocol::client_operation::TUPLE_INSERT);
    }
}

} // namespace ignite
