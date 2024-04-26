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
import org.apache.ignite.internal.cli.core.call.CallInput;

public class PartitionStatesCallInput implements CallInput {

    /** Cluster url. */
    private final String clusterUrl;

    private final boolean local;

    private final boolean global;

    private final List<String> nodeNames;

    private final List<String> zoneNames;

    private final List<Integer> partitionIds;

    public String clusterUrl() {
        return clusterUrl;
    }

    public boolean local() {
        return local;
    }

    public boolean global() {
        return global;
    }

    public List<String> nodeNames() {
        return nodeNames;
    }

    public List<String> zoneNames() {
        return zoneNames;
    }

    public List<Integer> partitionIds() {
        return partitionIds;
    }

    PartitionStatesCallInput(
            String clusterUrl,
            boolean local,
            boolean global,
            List<String> nodeNames,
            List<String> zoneNames,
            List<Integer> partitionIds
    ) {
        this.clusterUrl = clusterUrl;
        this.local = local;
        this.global = global;
        this.nodeNames = nodeNames;
        this.zoneNames = zoneNames;
        this.partitionIds = partitionIds;
    }

    public static PartitionStatesCallInputBuilder builder() {
        return new PartitionStatesCallInputBuilder();
    }

    public static class PartitionStatesCallInputBuilder {
        private String clusterUrl;

        private boolean local;

        private boolean global;

        private List<String> nodeNames;

        private List<String> zoneNames;

        private List<Integer> partitionIds;

        public PartitionStatesCallInputBuilder clusterUrl(String clusterUrl) {
            this.clusterUrl = clusterUrl;
            return this;
        }

        public PartitionStatesCallInputBuilder local(boolean local) {
            this.local = local;
            return this;
        }

        public PartitionStatesCallInputBuilder global(boolean global) {
            this.global = global;
            return this;
        }

        public PartitionStatesCallInputBuilder nodeNames(List<String> nodeNames) {
            this.nodeNames = nodeNames;
            return this;
        }

        public PartitionStatesCallInputBuilder zoneNames(List<String> zoneNames) {
            this.zoneNames = zoneNames;
            return this;
        }

        public PartitionStatesCallInputBuilder partitionIds(List<Integer> partitionIds) {
            this.partitionIds = partitionIds;
            return this;
        }

        public PartitionStatesCallInput build() {
            return new PartitionStatesCallInput(clusterUrl, local, global, nodeNames, zoneNames, partitionIds);
        }
    }
}
