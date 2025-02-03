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

#include "ignite/client/detail/ignite_client_impl.h"
#include "ignite/client/detail/utils.h"

namespace ignite::detail {


void ignite_client_impl::get_cluster_nodes_async(ignite_callback<std::vector<cluster_node>> callback) {
    auto reader_func = [](protocol::reader &reader) -> std::vector<cluster_node> {
        std::vector<cluster_node> nodes;
        auto size = reader.read_int32();
        nodes.reserve(std::size_t(size));

        for (std::int32_t node_idx = 0; node_idx < size; ++node_idx) {
            nodes.emplace_back(read_cluster_node(reader));
        }

        return nodes;
    };

    m_connection->perform_request_rd<std::vector<cluster_node>>(
        protocol::client_operation::CLUSTER_GET_NODES, std::move(reader_func), std::move(callback));
}

} // namespace ignite::detail
