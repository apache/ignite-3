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

package org.apache.ignite.network;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Ignite cluster. Provides access to cluster nodes and the local node.
 */
public interface IgniteCluster {
    /**
     * Gets the cluster nodes that are members of the logical topology, including the local node (if applicable).
     *
     * @return Collection of cluster nodes.
     */
    Collection<ClusterNode> nodes();

    /**
     * Gets the cluster nodes that are members of the logical topology, including the local node (if applicable).
     *
     * @return Collection of cluster nodes.
     */
    CompletableFuture<Collection<ClusterNode>> nodesAsync();

    /**
     * Gets the local cluster node, if applicable.
     *
     * @return Local cluster node (embedded mode), or {@code null} if not applicable (client side).
     */
    @Nullable ClusterNode localNode();
}
