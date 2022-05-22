/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import org.apache.ignite.internal.properties.IgniteProductVersion;

/**
 * Represents the state of the CMG. It contains:
 * <ol>
 *     <li>Names of the nodes that host the Meta Storage.</li>
 *     <li>Names of the nodes that host the CMG.</li>
 *     <li>Ignite version of the nodes that comprise the cluster.</li>
 *     <li>Cluster tag, unique for a particular Ignite cluster.</li>
 * </ol>
 */
public final class ClusterState implements Serializable {
    private final Set<String> cmgNodes;

    private final Set<String> msNodes;

    private final IgniteProductVersion igniteVersion;

    private final ClusterTag clusterTag;

    /**
     * Creates a new cluster state.
     *
     * @param cmgNodes Node names that host the CMG.
     * @param metaStorageNodes Node names that host the Meta Storage.
     * @param igniteVersion Version of Ignite nodes that comprise this cluster.
     * @param clusterTag Cluster tag.
     */
    public ClusterState(
            Collection<String> cmgNodes,
            Collection<String> metaStorageNodes,
            IgniteProductVersion igniteVersion,
            ClusterTag clusterTag
    ) {
        this.cmgNodes = Set.copyOf(cmgNodes);
        this.msNodes = Set.copyOf(metaStorageNodes);
        this.igniteVersion = igniteVersion;
        this.clusterTag = clusterTag;
    }

    public Set<String> cmgNodes() {
        return cmgNodes;
    }

    public Set<String> metaStorageNodes() {
        return msNodes;
    }

    public IgniteProductVersion igniteVersion() {
        return igniteVersion;
    }

    public ClusterTag clusterTag() {
        return clusterTag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ClusterState state = (ClusterState) o;
        return cmgNodes.equals(state.cmgNodes) && msNodes.equals(state.msNodes) && igniteVersion.equals(state.igniteVersion)
                && clusterTag.equals(state.clusterTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmgNodes, msNodes, igniteVersion, clusterTag);
    }

    @Override
    public String toString() {
        return "ClusterState{"
                + "cmgNodes=" + cmgNodes
                + ", msNodes=" + msNodes
                + ", igniteVersion=" + igniteVersion
                + ", clusterTag=" + clusterTag
                + '}';
    }
}
