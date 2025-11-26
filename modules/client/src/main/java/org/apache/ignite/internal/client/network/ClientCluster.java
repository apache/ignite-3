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

package org.apache.ignite.internal.client.network;

import static org.apache.ignite.internal.client.TcpIgniteClient.unpackClusterNode;
import static org.apache.ignite.internal.util.ViewUtils.sync;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.client.ReliableChannel;
import org.apache.ignite.internal.client.proto.ClientOp;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.IgniteCluster;
import org.jetbrains.annotations.Nullable;

/**
 * Client-side implementation of {@link IgniteCluster}.
 */
public class ClientCluster implements IgniteCluster {
    private final ReliableChannel ch;

    public ClientCluster(ReliableChannel ch) {
        this.ch = ch;
    }

    @Override
    public UUID id() {
        return ch.clusterId();
    }

    /** {@inheritDoc} */
    @Override
    public Collection<ClusterNode> nodes() {
        return sync(nodesAsync());
    }

    /** {@inheritDoc} */
    @Override
    public CompletableFuture<Collection<ClusterNode>> nodesAsync() {
        return ch.serviceAsync(ClientOp.CLUSTER_GET_NODES, r -> {
            int cnt = r.in().unpackInt();
            List<ClusterNode> res = new ArrayList<>(cnt);

            for (int i = 0; i < cnt; i++) {
                // TODO IGNITE-25660 Propagate node metadata to client.
                ClusterNode clusterNode = unpackClusterNode(r);

                res.add(clusterNode);
            }

            return res;
        });
    }

    @Override
    public @Nullable ClusterNode localNode() {
        return null;
    }
}
