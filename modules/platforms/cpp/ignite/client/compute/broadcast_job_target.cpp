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

#include "ignite/client/compute/broadcast_job_target.h"
#include "ignite/client/table/ignite_tuple.h"

#include "ignite/client/detail/argument_check_utils.h"
#include "ignite/client/detail/compute/nodes_broadcast_job_target.h"

namespace ignite {

std::shared_ptr<broadcast_job_target> broadcast_job_target::node(cluster_node val) {
    return std::shared_ptr<broadcast_job_target>{new detail::nodes_broadcast_job_target{{std::move(val)}}};
}

std::shared_ptr<broadcast_job_target> broadcast_job_target::nodes(std::set<cluster_node> vals) {
    detail::arg_check::container_non_empty(vals, "Nodes set");

    return std::shared_ptr<broadcast_job_target>{new detail::nodes_broadcast_job_target{std::move(vals)}};
}

std::shared_ptr<broadcast_job_target> broadcast_job_target::nodes(const std::vector<cluster_node> &vals) {
    detail::arg_check::container_non_empty(vals, "Nodes set");

    std::set<cluster_node> node_set(vals.begin(), vals.end());
    return nodes(node_set);
}

} // namespace ignite
