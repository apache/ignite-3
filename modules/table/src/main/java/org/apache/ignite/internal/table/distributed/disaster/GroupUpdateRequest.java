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

package org.apache.ignite.internal.table.distributed.disaster;

import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.SINGLE_NODE;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.tostring.S;

/**
 * Executes partition reset request to restore partition assignments when majority is not available.
 *
 * <p>The reset is executed in two stages - first we switch to a single node having the most up-to-date data,
 * then we switch to other available nodes up to the configured replica factor, in the case of manual reset, and to the available nodes from
 * the original group, in the case of the automatic reset.
 */
class GroupUpdateRequest implements DisasterRecoveryRequest {

    private final UUID operationId;

    /**
     * Catalog version at the moment of operation creation. Must match catalog version at the moment of operation execution.
     */
    private final int catalogVersion;

    private final int zoneId;

    // TODO remove
    private final boolean colocationEnabled;

    /**
     * Map of (tableId -> setOfPartitions) to reset if colocation is disabled
     * or map of (zoneId -> setOfPartitions) to reset if colocation is enabled.
     */
    private final Map<Integer, Set<Integer>> partitionIds;

    private final boolean manualUpdate;

    private GroupUpdateRequest(
            UUID operationId,
            int catalogVersion,
            int zoneId,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate,
            boolean colocationEnabled
    ) {
        this.operationId = operationId;
        this.catalogVersion = catalogVersion;
        this.zoneId = zoneId;
        this.partitionIds = Map.copyOf(partitionIds);
        this.manualUpdate = manualUpdate;
        this.colocationEnabled = colocationEnabled;
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public int zoneId() {
        return zoneId;
    }

    @Override
    public DisasterRecoveryRequestType type() {
        return SINGLE_NODE;
    }

    public int catalogVersion() {
        return catalogVersion;
    }

    public Map<Integer, Set<Integer>> partitionIds() {
        return partitionIds;
    }

    public boolean manualUpdate() {
        return manualUpdate;
    }

    /**
     * Returns {@code true} if this request is a zone request with enabled colocation and {@code false} if this is a table request with
     * colocation disabled.
     */
    boolean colocationEnabled() {
        return colocationEnabled;
    }

    public static GroupUpdateRequest create(
            UUID operationId,
            int catalogVersion,
            int zoneId,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate,
            boolean colocationEnabled
    ) {
        return new GroupUpdateRequest(operationId, catalogVersion, zoneId, partitionIds, manualUpdate, colocationEnabled);
    }

    @Override
    public CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long msRevision, HybridTimestamp msTimestamp) {
        return GroupUpdateRequestHandler.handler(this).handle(disasterRecoveryManager, msRevision, msTimestamp);
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
