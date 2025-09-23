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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.jetbrains.annotations.Nullable;

/**
 * Represents an Ignite cluster, providing access to the cluster nodes and the local node.
 */
public interface IgniteCluster {
    /**
     * Returns the current cluster ID.
     *
     * @return Cluster's current ID.
     */
    UUID id();

    /**
     * Returns all cluster nodes that are part of the logical topology, including the local node if applicable.
     *
     * @return A collection of all cluster nodes.
     */
    Collection<ClusterNode> nodes();

    /**
     * Asynchronously retrieves all cluster nodes that are part of the logical topology, including the local node if applicable.
     *
     * @return A future that will complete with a collection of all cluster nodes.
     */
    CompletableFuture<Collection<ClusterNode>> nodesAsync();

    /**
     * Returns the local cluster node if available.
     *
     * @return The local cluster node (in embedded mode), or {@code null} if not applicable (e.g., on the client side).
     */
    @Nullable ClusterNode localNode();
}
