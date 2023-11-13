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

package org.apache.ignite.client.handler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.apache.ignite.internal.utils.PrimaryReplica;
import org.jetbrains.annotations.Nullable;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 * Tracks primary replica for every partition.
 */
public class ClientPrimaryReplicaTracker {
    private static final PrimaryReplicaEvent EVENT = PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;

    private final ConcurrentHashMap<Integer, CompletableFuture<List<String>>> primaryReplicas = new ConcurrentHashMap<>();

    private final AtomicLong updateCount = new AtomicLong();

    private final PlacementDriver placementDriver;

    private final IgniteTablesInternal igniteTables;

    private final EventListener<PrimaryReplicaEventParameters> listener = this::onEvent;

    public ClientPrimaryReplicaTracker(PlacementDriver placementDriver, IgniteTablesInternal igniteTables) {
        this.placementDriver = placementDriver;
        this.igniteTables = igniteTables;
    }

    private CompletableFuture<Boolean> onEvent(PrimaryReplicaEventParameters eventParameters, @Nullable Throwable err) {
        if (err != null || !(eventParameters.groupId() instanceof TablePartitionId)) {
            return CompletableFuture.completedFuture(null);
        }

        TablePartitionId tablePartitionId = (TablePartitionId) eventParameters.groupId();
        var fut = primaryReplicas.get(tablePartitionId.tableId());

        if (fut != null) {
            fut.thenAccept(replicas -> {
                replicas.set(tablePartitionId.partitionId(), eventParameters.leaseholder());
                updateCount.incrementAndGet();
            });
        }

        return CompletableFuture.completedFuture(false); // false: don't remove listener.
    }

    /**
     * Gets primary replicas by partition for the table.
     *
     * @param tableId Table ID.
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<List<String>> primaryReplicasAsync(int tableId) {
        return primaryReplicas.computeIfAbsent(tableId, this::init);
    }

    long updateCount() {
        return updateCount.get();
    }

    void start() {
        placementDriver.listen(EVENT, listener);
    }

    void stop() {
        placementDriver.removeListener(EVENT, listener);
    }

    private CompletableFuture<List<String>> init(Integer tableId) {
        try {
            // Initially, request all primary replicas for the table.
            // Then keep them updated via PRIMARY_REPLICA_ELECTED events.
            return igniteTables
                    .tableAsync(tableId)
                    .thenCompose(t -> t.internalTable().primaryReplicas())
                    .thenApply(replicas -> {
                        List<String> replicaNames = new ArrayList<>(replicas.size());
                        for (PrimaryReplica replica : replicas) {
                            replicaNames.add(replica.node().name());
                        }

                        return replicaNames;
                    });
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }
}
