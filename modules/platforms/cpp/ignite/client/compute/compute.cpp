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

#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/compute/nodes_broadcast_job_target.h"
#include <ignite/client/detail/compute/any_node_job_target.h>
#include <ignite/client/detail/compute/colocated_job_target.h>
#include "ignite/client/detail/compute/compute_impl.h"

#include "ignite/client/compute/compute.h"

namespace ignite {

void compute::submit_async(std::shared_ptr<job_target> target, std::shared_ptr<job_descriptor> descriptor,
    const binary_object &arg, ignite_callback<job_execution> callback) {
    detail::arg_check::pointer_valid(target, "Target");
    detail::arg_check::container_non_empty(descriptor->get_job_class_name(), "Job class name");

    switch (target->get_type()) {
        case detail::job_target_type::ANY_NODE: {
            auto any_node_target = static_cast<detail::any_node_job_target*>(target.get());
            m_impl->submit_to_nodes(any_node_target->get_nodes(), descriptor, arg, std::move(callback));
            break;
        }

        case detail::job_target_type::COLOCATED: {
            auto colocated_target = static_cast<detail::colocated_job_target*>(target.get());
            m_impl->submit_colocated_async(*colocated_target, descriptor, arg, std::move(callback));
            break;
        }

        default: {
            assert(false);
        }
    }

}

void compute::submit_broadcast_async(std::shared_ptr<broadcast_job_target> target,
    std::shared_ptr<job_descriptor> descriptor, const binary_object &arg,
    ignite_callback<broadcast_execution> callback) {

    detail::arg_check::pointer_valid(target, "Target pointer");
    detail::arg_check::container_non_empty(descriptor->get_job_class_name(), "Job class name");

    struct result_group {
        explicit result_group(std::int32_t cnt, ignite_callback<broadcast_execution> &&cb)
            : m_cnt(cnt)
            , m_callback(cb) {}

        std::mutex m_mutex;
        std::vector<ignite_result<job_execution>> m_res_vector;
        std::int32_t m_cnt{0};
        ignite_callback<broadcast_execution> m_callback;
    };

    auto nodes = static_cast<detail::nodes_broadcast_job_target&>(*target).get_nodes();
    auto shared_res = std::make_shared<result_group>(std::int32_t(nodes.size()), std::move(callback));

    for (const auto &node : nodes) {
        std::set<cluster_node> candidates = {node};
        m_impl->submit_to_nodes(candidates, descriptor, arg, [shared_res](auto &&res) {
            auto &val = *shared_res;

            std::lock_guard<std::mutex> lock(val.m_mutex);
            val.m_res_vector.emplace_back(std::move(res));
            --val.m_cnt;
            if (val.m_cnt == 0)
                val.m_callback(broadcast_execution(std::move(val.m_res_vector)));
        });
    }
}

} // namespace ignite