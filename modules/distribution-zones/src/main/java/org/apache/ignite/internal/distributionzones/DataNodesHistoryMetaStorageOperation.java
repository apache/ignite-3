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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.unmodifiableList;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.nodeNames;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;

import java.util.List;
import java.util.Set;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.Operations;
import org.jetbrains.annotations.Nullable;

class DataNodesHistoryMetaStorageOperation {
    private final Condition condition;
    private final List<Operation> operations;
    private final String operationName;
    private final int zoneId;
    private final DataNodesHistory currentDataNodesHistory;
    private final HybridTimestamp currentTimestamp;
    private final HybridTimestamp historyEntryTimestamp;
    private final Set<NodeWithAttributes> historyEntryNodes;
    private final boolean addMandatoryEntry;

    @Nullable
    private final DistributionZoneTimer scaleUpTimer;

    @Nullable
    private final DistributionZoneTimer scaleDownTimer;

    private DataNodesHistoryMetaStorageOperation(
            Condition condition,
            List<Operation> operations,
            String operationName,
            int zoneId,
            DataNodesHistory currentDataNodesHistory,
            HybridTimestamp currentTimestamp,
            HybridTimestamp historyEntryTimestamp,
            Set<NodeWithAttributes> historyEntryNodes,
            boolean addMandatoryEntry,
            @Nullable DistributionZoneTimer scaleUpTimer,
            @Nullable DistributionZoneTimer scaleDownTimer
    ) {
        this.condition = condition;
        this.operations = operations;
        this.operationName = operationName;
        this.zoneId = zoneId;
        this.currentDataNodesHistory = currentDataNodesHistory;
        this.currentTimestamp = currentTimestamp;
        this.historyEntryTimestamp = historyEntryTimestamp;
        this.historyEntryNodes = historyEntryNodes;
        this.addMandatoryEntry = addMandatoryEntry;
        this.scaleUpTimer = scaleUpTimer;
        this.scaleDownTimer = scaleDownTimer;
    }

    static Builder builder() {
        return new Builder();
    }

    /**
     * Forms a message for the log record about the history entry update. It also specifies whether the new history entry was
     * actually added; it is added if the history is empty, the nodes are different from the latest history entry or the
     * {@code addMandatoryEntry} parameter is true.
     *
     * @return Log message.
     */
    String successLogMessage() {
        Set<NodeWithAttributes> latestNodesWritten = currentDataNodesHistory.dataNodesForTimestamp(HybridTimestamp.MAX_VALUE).dataNodes();
        Set<NodeWithAttributes> nodes = historyEntryNodes;
        boolean historyEntryAdded = addMandatoryEntry || currentDataNodesHistory.isEmpty() || !nodes.equals(latestNodesWritten);

        return "Updated data nodes on " + operationName + ", "
                + (historyEntryAdded ? "added history entry" : "history entry not added")
                + " [zoneId=" + zoneId + ", currentTimestamp=" + currentTimestamp
                + (historyEntryAdded ? ", historyEntryTimestamp=" + historyEntryTimestamp + ", nodes=" + nodeNames(nodes) : "")
                + (historyEntryAdded ? "" : ", latestNodesWritten=" + nodeNames(latestNodesWritten))
                + "]"
                + ", dataNodesHistorySize=" + currentDataNodesHistory.size()
                + (scaleUpTimer == null ? "" : ", scaleUpTimer=" + scaleUpTimer)
                + (scaleDownTimer == null ? "" : ", scaleDownTimer=" + scaleDownTimer)
                + "].";
    }

    String failureLogMessage() {
        return "Failed to update data nodes history and timers on " + operationName + " [timestamp="
                + currentTimestamp + "].";
    }

    public Iif operation() {
        return iif(condition, new Operations(operations).yield(true), ops().yield(false));
    }

    static class Builder {
        private Condition condition;
        private List<Operation> operations;
        private String operationName;
        private int zoneId = -1;
        private DataNodesHistory currentDataNodesHistory;
        private HybridTimestamp currentTimestamp;
        private HybridTimestamp historyEntryTimestamp;
        private Set<NodeWithAttributes> historyEntryNodes;
        private boolean addMandatoryEntry;
        private DistributionZoneTimer scaleUpTimer;
        private DistributionZoneTimer scaleDownTimer;

        Builder condition(Condition condition) {
            this.condition = condition;
            return this;
        }

        Builder operations(List<Operation> operations) {
            this.operations = operations;
            return this;
        }

        Builder operationName(String operationName) {
            this.operationName = operationName;
            return this;
        }

        Builder zoneId(int zoneId) {
            this.zoneId = zoneId;
            return this;
        }

        Builder currentDataNodesHistory(DataNodesHistory currentDataNodesHistory) {
            this.currentDataNodesHistory = currentDataNodesHistory;
            return this;
        }

        Builder currentTimestamp(HybridTimestamp currentTimestamp) {
            this.currentTimestamp = currentTimestamp;
            return this;
        }

        Builder historyEntryTimestamp(HybridTimestamp historyEntryTimestamp) {
            this.historyEntryTimestamp = historyEntryTimestamp;
            return this;
        }

        Builder historyEntryNodes(Set<NodeWithAttributes> historyEntryNodes) {
            this.historyEntryNodes = historyEntryNodes;
            return this;
        }

        Builder scaleUpTimer(@Nullable DistributionZoneTimer scaleUpTimer) {
            this.scaleUpTimer = scaleUpTimer;
            return this;
        }

        Builder scaleDownTimer(@Nullable DistributionZoneTimer scaleDownTimer) {
            this.scaleDownTimer = scaleDownTimer;
            return this;
        }

        Builder addMandatoryEntry(boolean addMandatoryEntry) {
            this.addMandatoryEntry = addMandatoryEntry;
            return this;
        }

        DataNodesHistoryMetaStorageOperation build() {
            assert zoneId >= 0 : "Zone id is not set.";
            assert condition != null : "Condition is not set, zoneId=" + zoneId;
            assert operations != null : "Meta storage operations are not set, zoneId=" + zoneId;
            assert operationName != null : "Operation name is not set, zoneId=" + zoneId;
            assert currentDataNodesHistory != null : "Current data nodes history is not set, zoneId=" + zoneId;
            assert currentTimestamp != null : "Current timestamp is not set, zoneId=" + zoneId;
            assert historyEntryTimestamp != null : "History entry timestamp is not set, zoneId=" + zoneId;
            assert historyEntryNodes != null : "History entry nodes are not set, zoneId=" + zoneId;

            return new DataNodesHistoryMetaStorageOperation(
                    condition,
                    unmodifiableList(operations),
                    operationName,
                    zoneId,
                    currentDataNodesHistory,
                    currentTimestamp,
                    historyEntryTimestamp,
                    historyEntryNodes,
                    addMandatoryEntry,
                    scaleUpTimer,
                    scaleDownTimer
            );
        }
    }
}
