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

package org.apache.ignite.internal.cli.call.recovery.reset;

import java.util.List;
import org.apache.ignite.internal.cli.commands.recovery.reset.ResetPartitionsMixin;
import org.apache.ignite.internal.cli.core.call.CallInput;
import org.jetbrains.annotations.Nullable;

/** Input for the {@link ResetPartitionsCall} call. */
public class ResetPartitionsCallInput implements CallInput {
    private final String clusterUrl;

    private final String zoneName;

    private final String tableName;

    private final List<Integer> partitionIds;

    /** Cluster url. */
    public String clusterUrl() {
        return clusterUrl;
    }

    /** Returns zone name to reset partitions of. */
    public String zoneName() {
        return zoneName;
    }

    /** Returns table name to reset partitions of. */
    public String tableName() {
        return tableName;
    }

    /** IDs of partitions to get states of. */
    public List<Integer> partitionIds() {
        return partitionIds;
    }

    private ResetPartitionsCallInput(
            String clusterUrl,
            String zoneName,
            String tableName,
            @Nullable List<Integer> partitionIds
    ) {
        this.clusterUrl = clusterUrl;
        this.zoneName = zoneName;
        this.tableName = tableName;
        this.partitionIds = partitionIds == null ? List.of() : List.copyOf(partitionIds);
    }

    public static ResetPartitionsCallInput of(ResetPartitionsMixin statesArgs) {
        return of(statesArgs, statesArgs.clusterUrl());
    }

    /** Returns {@link ResetPartitionsCallInput} with specified arguments. */
    public static ResetPartitionsCallInput of(ResetPartitionsMixin statesArgs, String clusterUrl) {
        return builder()
                .zoneName(statesArgs.zoneName())
                .tableName(statesArgs.tableName())
                .partitionIds(statesArgs.partitionIds())
                .clusterUrl(clusterUrl)
                .build();
    }

    /**
     * Builder method provider.
     *
     * @return new instance of {@link ResetPartitionsCallInput}.
     */
    private static ResetPartitionsCallInputBuilder builder() {
        return new ResetPartitionsCallInputBuilder();
    }

    /** Builder for {@link ResetPartitionsCallInput}. */
    private static class ResetPartitionsCallInputBuilder {
        private String clusterUrl;

        private String zoneName;

        private String tableName;

        @Nullable
        private List<Integer> partitionIds;

        /** Set cluster URL. */
        ResetPartitionsCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        /** Set name of zone to reset partitions of. */
        ResetPartitionsCallInputBuilder zoneName(String zoneName) {
            this.zoneName = zoneName;
            return this;
        }

        /** Set name of table to reset partitions of. */
        ResetPartitionsCallInputBuilder tableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        /** Names of zones to get partition states of. */
        ResetPartitionsCallInputBuilder partitionIds(@Nullable List<Integer> partitionIds) {
            this.partitionIds = partitionIds;
            return this;
        }

        /** Set IDs of partitions to get states of. */
        ResetPartitionsCallInput build() {
            return new ResetPartitionsCallInput(clusterUrl, zoneName, tableName, partitionIds);
        }
    }
}
