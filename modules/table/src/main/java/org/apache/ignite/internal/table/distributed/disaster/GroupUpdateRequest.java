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

    private static final int COLOCATION_BIT = 1 << 31;

    private final UUID operationId;

    /**
     * Catalog version at the moment of operation creation. Must match catalog version at the moment of operation execution.
     */
    private final int catalogVersion;

    private final int internalZoneId;

    /**
     * Map of (tableId -> setOfPartitions) to reset.
     */
    private final Map<Integer, Set<Integer>> partitionIds;

    private final boolean manualUpdate;

    private GroupUpdateRequest(
            UUID operationId,
            int catalogVersion,
            int internalZoneId,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate
    ) {
        this.operationId = operationId;
        this.catalogVersion = catalogVersion;
        this.internalZoneId = internalZoneId;
        this.partitionIds = Map.copyOf(partitionIds);
        this.manualUpdate = manualUpdate;
    }

    @Override
    public UUID operationId() {
        return operationId;
    }

    @Override
    public int zoneId() {
        return zoneIdFromInternalZoneId(internalZoneId);
    }

    public int internalZoneId() {
        return internalZoneId;
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
        return (internalZoneId & COLOCATION_BIT) == 0;
    }

    private static int zoneIdFromInternalZoneId(int zoneId) {
        return zoneId & ~COLOCATION_BIT;
    }

    private static int toInternalZoneId(int zoneId, boolean colocationEnabled) {
        return (colocationEnabled ? 0 : COLOCATION_BIT) | zoneId;
    }

    public static GroupUpdateRequest createColocationAware(
            UUID operationId,
            int catalogVersion,
            int zoneId,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate,
            boolean colocationEnabled
    ) {
        int internalZoneId = toInternalZoneId(zoneId, colocationEnabled);

        return new GroupUpdateRequest(operationId, catalogVersion, internalZoneId, partitionIds, manualUpdate);
    }

    public static GroupUpdateRequest create(
            UUID operationId,
            int catalogVersion,
            int zoneId,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate
    ) {
        return new GroupUpdateRequest(operationId, catalogVersion, zoneId, partitionIds, manualUpdate);
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
