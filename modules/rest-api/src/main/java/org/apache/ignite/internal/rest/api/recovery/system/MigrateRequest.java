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

package org.apache.ignite.internal.rest.api.recovery.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import org.apache.ignite.internal.tostring.IgniteToStringInclude;
import org.apache.ignite.internal.tostring.S;
import org.jetbrains.annotations.Nullable;

/** Request to migrate nodes from old cluster to new (repaired). */
@Schema(description = "Migrate nodes to new cluster.")
public class MigrateRequest {
    @Schema(description = "Names of the CMG node names.")
    @IgniteToStringInclude
    private final List<String> cmgNodes;

    @Schema(description = "Names of the Metastorage node names.")
    @IgniteToStringInclude
    private final List<String> metaStorageNodes;

    @Schema(description = "Ignite version.")
    private final String version;

    @Schema(description = "ID of the cluster.")
    private final UUID clusterId;

    @Schema(description = "Name of the cluster.")
    private final String clusterName;

    @Schema(description = "IDs the cluster had before. If CMG/Metastorage group were never repaired, this is null.")
    private final @Nullable List<UUID> formerClusterIds;

    /** Constructor. */
    @JsonCreator
    public MigrateRequest(
            @JsonProperty("cmgNodes") List<String> cmgNodes,
            @JsonProperty("metaStorageNodes") List<String> metaStorageNodes,
            @JsonProperty("version") String version,
            @JsonProperty("clusterId") UUID clusterId,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("formerClusterIds") @Nullable List<UUID> formerClusterIds
    ) {
        Objects.requireNonNull(cmgNodes);
        Objects.requireNonNull(metaStorageNodes);
        Objects.requireNonNull(version);
        Objects.requireNonNull(clusterId);
        Objects.requireNonNull(clusterName);

        this.cmgNodes = List.copyOf(cmgNodes);
        this.metaStorageNodes = List.copyOf(metaStorageNodes);
        this.version = version;
        this.clusterId = clusterId;
        this.clusterName = clusterName;
        this.formerClusterIds = formerClusterIds == null ? null : List.copyOf(formerClusterIds);
    }

    /** Returns names of the CMG node names. */
    @JsonGetter("cmgNodes")
    public List<String> cmgNodes() {
        return cmgNodes;
    }

    /** Returns names of the Metastorage node names. */
    @JsonGetter("metaStorageNodes")
    public List<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    /** Returns Ignite version. */
    @JsonGetter("version")
    public String version() {
        return version;
    }

    /** Returns ID of the cluster. */
    @JsonGetter("clusterId")
    public UUID clusterId() {
        return clusterId;
    }

    /** Returns name of the cluster. */
    @JsonGetter("clusterName")
    public String clusterName() {
        return clusterName;
    }

    /** Returns IDs the cluster had before ({@code null} if CMG/Metastorage group were never repaired. */
    @JsonGetter("formerClusterIds")
    public @Nullable List<UUID> formerClusterIds() {
        return formerClusterIds;
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
