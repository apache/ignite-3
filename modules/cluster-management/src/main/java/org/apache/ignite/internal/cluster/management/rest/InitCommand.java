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

import static org.apache.ignite.internal.util.CollectionUtils.nullOrEmpty;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import org.apache.ignite.internal.util.StringUtils;
import org.jetbrains.annotations.Nullable;

/**
 * REST command for initializing a cluster.
 */
public class InitCommand {
    private final Collection<String> metaStorageNodes;

    private final Collection<String> cmgNodes;

    private final String clusterName;

    /**
     * Constructor.
     */
    @JsonCreator
    public InitCommand(
            @JsonProperty("metaStorageNodes") Collection<String> metaStorageNodes,
            @JsonProperty("cmgNodes") @Nullable Collection<String> cmgNodes,
            @JsonProperty("clusterName") String clusterName
    ) {
        Objects.requireNonNull(metaStorageNodes);
        Objects.requireNonNull(clusterName);

        if (metaStorageNodes.isEmpty()) {
            throw new IllegalArgumentException("Meta Storage node names list must not be empty");
        }

        if (metaStorageNodes.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("Meta Storage node names must not contain blank strings: " + metaStorageNodes);
        }

        if (!nullOrEmpty(cmgNodes) && cmgNodes.stream().anyMatch(StringUtils::nullOrBlank)) {
            throw new IllegalArgumentException("CMG node names must not contain blank strings: " + cmgNodes);
        }

        if (clusterName.isBlank()) {
            throw new IllegalArgumentException("Cluster name must not be empty");
        }

        this.metaStorageNodes = List.copyOf(metaStorageNodes);
        this.cmgNodes = cmgNodes == null ? List.of() : List.copyOf(cmgNodes);
        this.clusterName = clusterName;
    }

    @JsonProperty
    public Collection<String> metaStorageNodes() {
        return metaStorageNodes;
    }

    @JsonProperty
    public Collection<String> cmgNodes() {
        return cmgNodes;
    }

    @JsonProperty
    public String clusterName() {
        return clusterName;
    }

    @Override
    public String toString() {
        return "InitCommand{"
                + "metaStorageNodes=" + metaStorageNodes
                + ", cmgNodes=" + cmgNodes
                + ", clusterName='" + clusterName + '\''
                + '}';
    }
}
