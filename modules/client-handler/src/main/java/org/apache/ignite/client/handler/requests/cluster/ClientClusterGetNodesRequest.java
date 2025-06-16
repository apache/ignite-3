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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.handler.ResponseWriter;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
import org.apache.ignite.network.NetworkAddress;

/**
 * Cluster nodes request.
 */
public class ClientClusterGetNodesRequest {
    /**
     * Processes the request.
     *
     * @param cluster Cluster.
     * @return Future.
     */
    public static CompletableFuture<ResponseWriter> process(IgniteCluster cluster) {
        return cluster.nodesAsync().thenApply(nodes -> out -> {
            out.packInt(nodes.size());

            for (ClusterNode node : nodes) {
                packClusterNode(node, out);
            }
        });
    }

    /**
     * Pack {@link ClusterNode} instance to client message.
     *
     * @param clusterNode Cluster node.
     * @param out Client message packer.
     */
    public static void packClusterNode(ClusterNode clusterNode, ClientMessagePacker out) {
        out.packInt(4);

        out.packUuid(clusterNode.id());
        out.packString(clusterNode.name());

        NetworkAddress address = clusterNode.address();
        out.packString(address.host());
        out.packInt(address.port());
    }
}
