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

package org.apache.ignite.internal.rest.api.cluster;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.Objects;

/**
 * REST representation of internal ClusterState.
 */
@Schema(description = "Information about current cluster state.")
public class ClusterState {
    @Schema(description = "List of cluster management group nodes. These nodes are responsible for maintaining RAFT cluster topology.")
    private final Collection<String> cmgNodes;

    @Schema(description = "List of metastorage nodes. These nodes are responsible for storing RAFT cluster metadata.")
    private final Collection<String> msNodes;

    @Schema(description = "Version of Apache Ignite that the cluster was created on.")
    private final String igniteVersion;

    @Schema(description = "Unique tag that identifies the cluster.")
    private final ClusterTag clusterTag;

    /**
     * Creates a new cluster state.
     *
     * @param cmgNodes Node names that host the CMG.
     * @param msNodes Node names that host the Meta Storage.
     * @param igniteVersion Version of Ignite nodes that comprise this cluster.
     * @param clusterTag Cluster tag.
     */
    @JsonCreator
    public ClusterState(
            @JsonProperty("cmgNodes") Collection<String> cmgNodes,
            @JsonProperty("msNodes") Collection<String> msNodes,
            @JsonProperty("igniteVersion") String igniteVersion,
            @JsonProperty("clusterTag") ClusterTag clusterTag
    ) {
        this.cmgNodes = cmgNodes;
        this.msNodes = msNodes;
        this.igniteVersion = igniteVersion;
        this.clusterTag = clusterTag;
    }

    @JsonGetter("cmgNodes")
    public Collection<String> cmgNodes() {
        return cmgNodes;
    }

    @JsonGetter("msNodes")
    public Collection<String> msNodes() {
        return msNodes;
    }

    @JsonGetter("igniteVersion")
    public String igniteVersion() {
        return igniteVersion;
    }

    @JsonGetter("clusterTag")
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
