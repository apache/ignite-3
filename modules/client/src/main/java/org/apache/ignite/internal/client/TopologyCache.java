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

package org.apache.ignite.internal.client;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;

/**
 * Cache for server nodes topology.
 */
public class TopologyCache {
    private final Map<String, ClusterNode> topology = new ConcurrentHashMap<>();

    private Instant aliveUntil;

    private final Supplier<Collection<ClusterNode>> updateFunction;

    /**
     * Creates new topology cache.
     *
     * @param updateFunction New server nodes topology provider.
     */
    public TopologyCache(Supplier<Collection<ClusterNode>> updateFunction) {
        this.updateFunction = updateFunction;
    }

    /**
     * Refreshes cache with new given topology.
     *
     * @param newTopology New server nodes topology.
     */
    public void refresh(Collection<ClusterNode> newTopology) {
        topology.clear();
        for (ClusterNode clusterNode : newTopology) {
            topology.put(clusterNode.name(), clusterNode);
        }
        aliveUntil = Instant.now().plus(1, ChronoUnit.MINUTES);
    }

    /**
     * Returns {@link ClusterNode} for given node name.
     *
     * @param name Node name.
     * @return Cluster node for given node name or {@code null} if node name is not presented in topology.
     */
    public @Nullable ClusterNode get(String name) {
        if (aliveUntil == null || Instant.now().isAfter(aliveUntil)) {
            refresh(updateFunction.get());
        }
        return topology.get(name);
    }
}
