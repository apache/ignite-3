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

#include "ignite/client/compute/compute.h"
#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/compute/compute_impl.h"

namespace ignite {

void compute::submit_async(const std::vector<cluster_node> &nodes, std::shared_ptr<job_descriptor> descriptor,
    const binary_object &arg, ignite_callback<job_execution> callback) {
    detail::arg_check::container_non_empty(nodes, "Nodes container");
    detail::arg_check::container_non_empty(descriptor->get_job_class_name(), "Job class name");

    m_impl->submit_to_nodes(nodes, descriptor, arg, std::move(callback));
}

void compute::submit_broadcast_async(const std::set<cluster_node> &nodes, std::shared_ptr<job_descriptor> descriptor,
    const binary_object &arg,
    ignite_callback<std::map<cluster_node, ignite_result<job_execution>>> callback) {
    typedef std::map<cluster_node, ignite_result<job_execution>> result_type;

    detail::arg_check::container_non_empty(nodes, "Nodes set");
    detail::arg_check::container_non_empty(descriptor->get_job_class_name(), "Job class name");

    struct result_group {
        explicit result_group(std::int32_t cnt, ignite_callback<result_type> &&cb)
            : m_cnt(cnt)
            , m_callback(cb) {}

        std::mutex m_mutex;
        result_type m_res_map;
        std::int32_t m_cnt{0};
        ignite_callback<result_type> m_callback;
    };

    auto shared_res = std::make_shared<result_group>(std::int32_t(nodes.size()), std::move(callback));

    for (const auto &node : nodes) {
        std::vector<cluster_node> candidates = {node};
        m_impl->submit_to_nodes(candidates, descriptor, arg, [node, shared_res](auto &&res) {
            auto &val = *shared_res;

            std::lock_guard<std::mutex> lock(val.m_mutex);
            val.m_res_map.emplace(node, res);
            --val.m_cnt;
            if (val.m_cnt == 0)
                val.m_callback(std::move(val.m_res_map));
        });
    }
}

void compute::submit_colocated_async(std::string_view table_name, const ignite_tuple &key,
    std::shared_ptr<job_descriptor> descriptor, const binary_object &arg, ignite_callback<job_execution> callback) {
    detail::arg_check::container_non_empty(table_name, "Table name");
    detail::arg_check::tuple_non_empty(key, "Key tuple");
    detail::arg_check::container_non_empty(descriptor->get_job_class_name(), "Job class name");

    m_impl->submit_colocated_async(
        std::string(table_name), key, descriptor, arg, std::move(callback));
}

} // namespace ignite
