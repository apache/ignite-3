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

package org.apache.ignite.internal.cluster.management.rest;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.Objects;

/**
 * REST representation on {@link org.apache.ignite.internal.cluster.management.ClusterState}.
 */
class ClusterState {
    private final Collection<String> cmgNodes;

    private final Collection<String> msNodes;

    private final org.apache.ignite.internal.cluster.management.rest.IgniteProductVersion igniteVersion;

    private final org.apache.ignite.internal.cluster.management.rest.ClusterTag clusterTag;

    /**
     * Creates a new cluster state.
     *
     * @param cmgNodes Node names that host the CMG.
     * @param metaStorageNodes Node names that host the Meta Storage.
     * @param igniteVersion Version of Ignite nodes that comprise this cluster.
     * @param clusterTag Cluster tag.
     */
    @JsonCreator
    public ClusterState(
            @JsonProperty("cmgNodes") Collection<String> cmgNodes,
            @JsonProperty("metaStorageNodes") Collection<String> metaStorageNodes,
            @JsonProperty("igniteVersion") org.apache.ignite.internal.cluster.management.rest.IgniteProductVersion igniteVersion,
            @JsonProperty("clusterTag") org.apache.ignite.internal.cluster.management.rest.ClusterTag clusterTag
    ) {
        this.cmgNodes = cmgNodes;
        this.msNodes = metaStorageNodes;
        this.igniteVersion = igniteVersion;
        this.clusterTag = clusterTag;
    }

    @JsonProperty
    public Collection<String> cmgNodes() {
        return cmgNodes;
    }

    @JsonProperty
    public Collection<String> metaStorageNodes() {
        return msNodes;
    }

    @JsonProperty
    public org.apache.ignite.internal.cluster.management.rest.IgniteProductVersion igniteVersion() {
        return igniteVersion;
    }

    @JsonProperty
    public org.apache.ignite.internal.cluster.management.rest.ClusterTag clusterTag() {
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
