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

package org.apache.ignite.internal.cli.call.recovery;

import java.util.List;
import org.apache.ignite.internal.cli.commands.recovery.partitions.states.PartitionStatesMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.jetbrains.annotations.Nullable;

/** Input for the {@link PartitionStatesCall} call. */
public class PartitionStatesCallInput implements CallInput {
    private final String clusterUrl;

    private final boolean local;

    private final List<String> nodeNames;

    private final List<String> zoneNames;

    private final List<Integer> partitionIds;

    /** Cluster url. */
    public String clusterUrl() {
        return clusterUrl;
    }

    /** If local partition states should be returned. */
    public boolean local() {
        return local;
    }

    /** Returns node names to get local partition states from. */
    public List<String> nodeNames() {
        return nodeNames;
    }

    /** Names of zones to get partition states of. */
    public List<String> zoneNames() {
        return zoneNames;
    }

    /** IDs of partitions to get states of. */
    public List<Integer> partitionIds() {
        return partitionIds;
    }

    private PartitionStatesCallInput(
            String clusterUrl,
            boolean local,
            @Nullable List<String> nodeNames,
            @Nullable List<String> zoneNames,
            @Nullable List<Integer> partitionIds
    ) {
        this.clusterUrl = clusterUrl;
        this.local = local;
        this.nodeNames = nodeNames == null ? List.of() : List.copyOf(nodeNames);
        this.zoneNames = zoneNames == null ? List.of() : List.copyOf(zoneNames);
        this.partitionIds = partitionIds == null ? List.of() : List.copyOf(partitionIds);
    }

    public static PartitionStatesCallInput of(PartitionStatesMixin statesArgs) {
        return of(statesArgs, statesArgs.clusterUrl());
    }

    /** Returns {@link PartitionStatesCallInput} with specified arguments. */
    public static PartitionStatesCallInput of(PartitionStatesMixin statesArgs, String clusterUrl) {
        return builder()
                .local(statesArgs.local())
                .nodeNames(statesArgs.nodeNames())
                .zoneNames(statesArgs.zoneNames())
                .partitionIds(statesArgs.partitionIds())
                .clusterUrl(clusterUrl)
                .build();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link PartitionStatesCallInputBuilder}.
     */
    private static PartitionStatesCallInputBuilder builder() {
        return new PartitionStatesCallInputBuilder();
    }

    /** Builder for {@link PartitionStatesCallInput}. */
    private static class PartitionStatesCallInputBuilder {
        private String clusterUrl;

        private boolean local;

        @Nullable
        private List<String> nodeNames;

        @Nullable
        private List<String> zoneNames;

        @Nullable
        private List<Integer> partitionIds;

        /** Set cluster URL. */
        PartitionStatesCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /** Set flag to get local partition states. */
        PartitionStatesCallInputBuilder local(boolean local) {
            this.local = local;
            return this;
        }

        /** Set names of zones to get partition states of. All if empty or null. */
        PartitionStatesCallInputBuilder nodeNames(@Nullable List<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        /** Set names of zones to get partition states of. All if empty or null. */
        PartitionStatesCallInputBuilder zoneNames(@Nullable List<String> zoneNames) {
            this.zoneNames = zoneNames;
            return this;
        }

        /** Names of zones to get partition states of. All if empty or null. */
        PartitionStatesCallInputBuilder partitionIds(@Nullable List<Integer> partitionIds) {
            this.partitionIds = partitionIds;
            return this;
        }

        /** Build {@link PartitionStatesCallInput}. */
        PartitionStatesCallInput build() {
            return new PartitionStatesCallInput(clusterUrl, local, nodeNames, zoneNames, partitionIds);
        }
    }
}
