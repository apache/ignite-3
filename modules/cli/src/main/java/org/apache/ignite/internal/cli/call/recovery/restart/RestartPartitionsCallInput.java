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

package org.apache.ignite.internal.cli.call.recovery.restart;

import java.util.List;
import org.apache.ignite.internal.cli.commands.recovery.partitions.restart.RestartPartitionsMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.jetbrains.annotations.Nullable;

/** Input for the {@link RestartPartitionsCall} call. */
public class RestartPartitionsCallInput implements CallInput {
    private final String clusterUrl;

    private final String zoneName;

    private final String tableName;

    private final List<String> nodeNames;

    private final List<Integer> partitionIds;

    /** Cluster url. */
    public String clusterUrl() {
        return clusterUrl;
    }

    /** Returns zone name to restart partitions of. */
    public String zoneName() {
        return zoneName;
    }

    /** Returns table name to restart partitions of. */
    public String tableName() {
        return tableName;
    }

    /** IDs of partitions to restart. Empty means "all partitions". */
    public List<Integer> partitionIds() {
        return partitionIds;
    }

    /** Names specifying nodes to restart partitions. Empty means "all nodes". */
    public List<String> nodeNames() {
        return nodeNames;
    }

    private RestartPartitionsCallInput(
            String clusterUrl,
            String zoneName,
            String tableName,
            @Nullable List<Integer> partitionIds,
            @Nullable List<String> nodeNames
    ) {
        this.clusterUrl = clusterUrl;
        this.zoneName = zoneName;
        this.tableName = tableName;
        this.partitionIds = partitionIds == null ? List.of() : List.copyOf(partitionIds);
        this.nodeNames = nodeNames == null ? List.of() : List.copyOf(nodeNames);
    }

    /** Returns {@link RestartPartitionsCallInput} with specified arguments. */
    public static RestartPartitionsCallInput of(RestartPartitionsMixin restartArgs, String clusterUrl) {
        return builder()
                .zoneName(restartArgs.zoneName())
                .tableName(restartArgs.tableName())
                .partitionIds(restartArgs.partitionIds())
                .nodeNames(restartArgs.nodeNames())
                .clusterUrl(clusterUrl)
                .build();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link RestartPartitionsCallInput}.
     */
    private static RestartPartitionsCallInputBuilder builder() {
        return new RestartPartitionsCallInputBuilder();
    }

    /** Builder for {@link RestartPartitionsCallInput}. */
    private static class RestartPartitionsCallInputBuilder {
        private String clusterUrl;

        private String zoneName;

        private String tableName;

        @Nullable
        private List<Integer> partitionIds;

        @Nullable
        private List<String> nodeNames;

        /** Set cluster URL. */
        RestartPartitionsCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /** Set name of zone to restart partitions of. */
        RestartPartitionsCallInputBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        /** Set name of table to restart partitions of. */
        RestartPartitionsCallInputBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** Names of zones to restart partitions of. Empty / null means "all partitions". */
        RestartPartitionsCallInputBuilder partitionIds(@Nullable List<Integer> partitionIds) {
            this.partitionIds = partitionIds;
            return this;
        }

        /** Names specifying nodes to restart partitions. Case-sensitive, empty / null means "all nodes". */
        RestartPartitionsCallInputBuilder nodeNames(@Nullable List<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        /** Build {@link RestartPartitionsCallInput}. */
        RestartPartitionsCallInput build() {
            return new RestartPartitionsCallInput(clusterUrl, zoneName, tableName, partitionIds, nodeNames);
        }
    }
}
