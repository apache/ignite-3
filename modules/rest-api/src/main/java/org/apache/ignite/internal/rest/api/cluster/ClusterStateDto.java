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
import io.swagger.v3.oas.annotations.media.Schema.RequiredMode;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/**
 * REST representation of internal ClusterState.
 */
@Schema(description = "Information about current cluster state.")
public class ClusterStateDto {
    @Schema(description = "List of cluster management group nodes. These nodes are responsible for maintaining RAFT cluster topology.",
            requiredMode = RequiredMode.REQUIRED)
    @IgniteToStringInclude
    private final GroupState cmgStatus;

    @Schema(description = "List of metastorage nodes. These nodes are responsible for storing RAFT cluster metadata.",
            requiredMode = RequiredMode.REQUIRED)
    @IgniteToStringInclude
    private final GroupState metastoreStatus;

    @Schema(description = "Version of Apache Ignite that the cluster was created on.", requiredMode = RequiredMode.REQUIRED)
    private final String igniteVersion;

    @Schema(description = "Unique tag that identifies the cluster.",
            requiredMode = RequiredMode.REQUIRED)
    private final ClusterTag clusterTag;

    @Schema(description = "IDs the cluster had before.")
    @IgniteToStringInclude
    private final @Nullable List<UUID> formerClusterIds;

    /**
     * Creates a new cluster state.
     *
     * @param cmgStatus Node names that host the CMG.
     * @param metastoreStatus Node names that host the Meta Storage.
     * @param igniteVersion Version of Ignite nodes that comprise this cluster.
     * @param clusterTag Cluster tag.
     * @param formerClusterIds Former cluster IDs.
     */
    @JsonCreator
    public ClusterStateDto(
            @JsonProperty("cmgStatus") GroupState cmgStatus,
            @JsonProperty("metastoreStatus") GroupState metastoreStatus,
            @JsonProperty("igniteVersion") String igniteVersion,
            @JsonProperty("clusterTag") ClusterTag clusterTag,
            @JsonProperty("formerClusterIds") @Nullable List<UUID> formerClusterIds
    ) {
        this.cmgStatus = cmgStatus;
        this.metastoreStatus = metastoreStatus;
        this.igniteVersion = igniteVersion;
        this.clusterTag = clusterTag;
        this.formerClusterIds = formerClusterIds == null ? null : List.copyOf(formerClusterIds);
    }

    @JsonGetter("igniteVersion")
    public String igniteVersion() {
        return igniteVersion;
    }

    @JsonGetter("clusterTag")
    public ClusterTag clusterTag() {
        return clusterTag;
    }

    @JsonGetter("formerClusterIds")
    public @Nullable List<UUID> formerClusterIds() {
        return formerClusterIds;
    }

    @JsonGetter("cmgStatus")
    public GroupState cmgStatus() {
        return cmgStatus;
    }

    @JsonGetter("metastoreStatus")
    public GroupState metastoreStatus() {
        return metastoreStatus;
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
        return cmgStatus.equals(state.cmgStatus)
                && metastoreStatus.equals(state.metastoreStatus)
                && igniteVersion.equals(state.igniteVersion)
                && clusterTag.equals(state.clusterTag);
    }

    @Override
    public int hashCode() {
        return Objects.hash(cmgStatus, metastoreStatus, igniteVersion, clusterTag);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
