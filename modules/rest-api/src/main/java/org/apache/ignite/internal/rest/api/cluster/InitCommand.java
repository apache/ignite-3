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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * REST command for initializing a cluster.
 */
@Schema(description = "Cluster initialization configuration.")
public class InitCommand {
    @Schema(description = "A list of RAFT metastorage nodes.")
    private final Collection<String> metaStorageNodes;

    @Schema(description = "A list of RAFT cluster management nodes.")
    private final Collection<String> cmgNodes;

    @Schema(description = "The name of the cluster.")
    private final String clusterName;

    @Schema(description = "Cluster configuration in HOCON format.")
    private final String clusterConfiguration;

    /**
     * Constructor.
     */
    @JsonCreator
    public InitCommand(
            @JsonProperty("metaStorageNodes") Collection<String> metaStorageNodes,
            @JsonProperty("cmgNodes") @Nullable Collection<String> cmgNodes,
            @JsonProperty("clusterName") String clusterName,
            @JsonProperty("clusterConfiguration") String clusterConfiguration
    ) {
        Objects.requireNonNull(metaStorageNodes);
        Objects.requireNonNull(clusterName);

        if (metaStorageNodes.isEmpty()) {
            throw new IllegalArgumentException("Meta Storage node names list must not be empty.");
        }

        if (metaStorageNodes.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("Meta Storage node names must not contain blank strings: " + metaStorageNodes);
        }

        if (!nullOrEmpty(cmgNodes) && cmgNodes.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("CMG node names must not contain blank strings: " + cmgNodes);
        }

        if (clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name must not be empty.");
        }

        this.metaStorageNodes = List.copyOf(metaStorageNodes);
        this.cmgNodes = cmgNodes == null ? List.of() : List.copyOf(cmgNodes);
        this.clusterName = clusterName;
        this.clusterConfiguration = clusterConfiguration;
    }

    @JsonGetter("metaStorageNodes")
    public Collection<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    @JsonGetter("cmgNodes")
    public Collection<String> cmgNodes() {
        return cmgNodes;
    }

    @JsonGetter("clusterName")
    public String clusterName() {
        return clusterName;
    }

    @JsonGetter("clusterConfiguration")
    public String clusterConfiguration() {
        return clusterConfiguration;
    }

    @Override
    public String toString() {
        return "InitCommand{"
                + "metaStorageNodes=" + metaStorageNodes
                + ", cmgNodes=" + cmgNodes
                + ", clusterName='" + clusterName + '\''
                + ", clusterConfiguration='" + clusterConfiguration + '\''
                + '}';
    }
}
