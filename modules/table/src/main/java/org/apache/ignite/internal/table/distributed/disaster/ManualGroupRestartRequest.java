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

import static java.util.concurrent.CompletableFuture.allOf;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoveryRequestType.MULTI_NODE;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;

import java.util.ArrayList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.tostring.S;

class ManualGroupRestartRequest implements DisasterRecoveryRequest {
    private static final long serialVersionUID = 0L;

    private final UUID operationId;

    private final int zoneId;

    private final int tableId;

    private final Set<Integer> partitionIds;

    private final Set<String> nodeNames;

    private final int catalogVersion;

    ManualGroupRestartRequest(
            UUID operationId,
            int zoneId,
            int tableId,
            Set<Integer> partitionIds,
            Set<String> nodeNames,
            int catalogVersion
    ) {
        this.operationId = operationId;
        this.zoneId = zoneId;
        this.tableId = tableId;
        this.partitionIds = Set.copyOf(partitionIds);
        this.nodeNames = Set.copyOf(nodeNames);
        this.catalogVersion = catalogVersion;
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
        return MULTI_NODE;
    }

    public int tableId() {
        return tableId;
    }

    public Set<Integer> partitionIds() {
        return partitionIds;
    }

    public Set<String> nodeNames() {
        return nodeNames;
    }

    @Override
    public CompletableFuture<Void> handle(DisasterRecoveryManager disasterRecoveryManager, long revision) {
        if (!nodeNames.isEmpty() && !nodeNames.contains(disasterRecoveryManager.localNode().name())) {
            return nullCompletedFuture();
        }

        var restartFutures = new ArrayList<CompletableFuture<?>>();

        disasterRecoveryManager.raftManager.forEach((raftNodeId, raftGroupService) -> {
            if (raftNodeId.groupId() instanceof TablePartitionId) {
                TablePartitionId groupId = (TablePartitionId) raftNodeId.groupId();

                if (groupId.tableId() == tableId && partitionIds.contains(groupId.partitionId())) {
                    restartFutures.add(disasterRecoveryManager.tableManager.restartPartition(groupId, revision, catalogVersion));
                }
            }
        });

        return restartFutures.isEmpty() ? nullCompletedFuture() : allOf(restartFutures.toArray(CompletableFuture[]::new));
    }

    @Override
    public String toString() {
        return S.toString(this);
    }
}
