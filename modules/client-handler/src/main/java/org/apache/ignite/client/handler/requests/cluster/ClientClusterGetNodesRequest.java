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

package org.apache.ignite.client.handler.requests.cluster;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.network.ClusterNode;

/**
 * Cluster nodes request.
 */
public class ClientClusterGetNodesRequest {
    /**
     * Processes the request.
     *
     * @param out            Packer.
     * @param clusterService Cluster.
     * @return Future.
     */
    public static CompletableFuture<Void> process(
            ClientMessagePacker out,
            ClusterService clusterService) {
        Collection<ClusterNode> nodes = clusterService.topologyService().allMembers();

        out.packInt(nodes.size());

        for (ClusterNode node : nodes) {
            out.packInt(4);

            out.packString(node.id());
            out.packString(node.name());
            out.packString(node.address().host());
            out.packInt(node.address().port());
        }

        // Null future indicates synchronous completion.
        return null;
    }
}
