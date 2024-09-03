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

package org.apache.ignite.internal.cluster.management;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.internal.cluster.management.network.messages.CmgMessageGroup;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.annotations.Transferable;
import org.apache.ignite.internal.properties.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

/**
 * Represents the state of the CMG. It contains:
 * <ol>
 *     <li>Names of the nodes that host Meta storage.</li>
 *     <li>Names of the nodes that host the CMG.</li>
 *     <li>Ignite version of the nodes that comprise the cluster.</li>
 *     <li>Cluster tag, unique for a particular Ignite cluster.</li>
 * </ol>
 */
@Transferable(CmgMessageGroup.Commands.CLUSTER_STATE)
public interface ClusterState extends NetworkMessage, Serializable {
    /**
     * Returns node names that host the CMG.
     */
    Set<String> cmgNodes();

    /**
     * Returns node names that host Meta storage.
     */
    Set<String> metaStorageNodes();

    /**
     * Returns a version of Ignite nodes that comprise this cluster.
     */
    String version();

    /**
     * Returns a cluster tag.
     */
    ClusterTag clusterTag();

    /**
     * Returns a version of Ignite nodes that comprise this cluster.
     */
    default IgniteProductVersion igniteVersion() {
        return IgniteProductVersion.fromString(version());
    }

    /**
     * Returns initial cluster configuration.
     */
    @Nullable
    String initialClusterConfiguration();

    /**
     * Returns IDs this cluster had before ({@code null} if this is the first incarnation).
     */
    @Nullable
    List<UUID> formerClusterIds();

    /**
     * Returns history of cluster IDs (oldest to newest). Last element is the current ID. Contains at least one element.
     */
    default List<UUID> clusterIdHistory() {
        List<UUID> formerClusterIds = formerClusterIds();
        UUID currentClusterId = clusterTag().clusterId();

        if (formerClusterIds == null) {
            return List.of(currentClusterId);
        }

        List<UUID> history = new ArrayList<>(formerClusterIds);
        history.add(currentClusterId);

        return history;
    }
}
