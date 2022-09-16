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
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.Objects;

/**
 * REST representation of internal ClusterState.
 */
@Schema(name = "ClusterState")
public class ClusterStateDto {
    private final Collection<String> cmgNodes;

    private final Collection<String> msNodes;

    private final IgniteProductVersionDto igniteVersion;

    private final ClusterTagDto clusterTag;

    /**
     * Creates a new cluster state.
     *
     * @param cmgNodes Node names that host the CMG.
     * @param msNodes Node names that host the Meta Storage.
     * @param igniteVersion Version of Ignite nodes that comprise this cluster.
     * @param clusterTag Cluster tag.
     */
    @JsonCreator
    public ClusterStateDto(
            @JsonProperty("cmgNodes") Collection<String> cmgNodes,
            @JsonProperty("msNodes") Collection<String> msNodes,
            @JsonProperty("igniteVersion") IgniteProductVersionDto igniteVersion,
            @JsonProperty("clusterTag") ClusterTagDto clusterTag
    ) {
        this.cmgNodes = cmgNodes;
        this.msNodes = msNodes;
        this.igniteVersion = igniteVersion;
        this.clusterTag = clusterTag;
    }

    @JsonProperty
    public Collection<String> cmgNodes() {
        return cmgNodes;
    }

    @JsonProperty
    public Collection<String> msNodes() {
        return msNodes;
    }

    @JsonProperty
    public IgniteProductVersionDto igniteVersion() {
        return igniteVersion;
    }

    @JsonProperty
    public ClusterTagDto clusterTag() {
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
        ClusterStateDto state = (ClusterStateDto) o;
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
