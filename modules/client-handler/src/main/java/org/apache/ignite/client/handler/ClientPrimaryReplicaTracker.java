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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.IgniteTablesInternal;
import org.jetbrains.annotations.Nullable;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 * Tracks primary replicas by partition for every table.
 */
public class ClientPrimaryReplicaTracker {
    private static final PrimaryReplicaEvent EVENT = PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED;

    private static final int AWAIT_PRIMARY_REPLICA_TIMEOUT = 30;

    private final ConcurrentHashMap<Integer, CompletableFuture<List<String>>> primaryReplicas = new ConcurrentHashMap<>();

    private final AtomicLong updateCount = new AtomicLong();

    private final PlacementDriver placementDriver;

    private final IgniteTablesInternal igniteTables;

    private final EventListener<PrimaryReplicaEventParameters> listener = this::onEvent;

    private final HybridClock clock;

    public ClientPrimaryReplicaTracker(
            PlacementDriver placementDriver,
            IgniteTablesInternal igniteTables,
            HybridClock clock) {
        this.placementDriver = placementDriver;
        this.igniteTables = igniteTables;
        this.clock = clock;
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
        return primaryReplicas.computeIfAbsent(tableId, this::initTable);
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

    private CompletableFuture<List<String>> initTable(Integer tableId) {
        try {
            // Initially, request all primary replicas for the table.
            // Then keep them updated via PRIMARY_REPLICA_ELECTED events.
            return igniteTables
                    .tableAsync(tableId)
                    .thenCompose(t -> primaryReplicas(t.tableId(), t.internalTable().partitions()))
                    .handle((replicas, err) -> {
                        if (err != null) {
                            // Remove failed future from the map to allow retry.
                            primaryReplicas.remove(tableId);

                            throw new RuntimeException(err);
                        }

                        return replicas;
                    });
        } catch (NodeStoppingException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    private CompletableFuture<List<String>> primaryReplicas(int tableId, int partitions) {
        CompletableFuture<ReplicaMeta>[] futs = (CompletableFuture<ReplicaMeta>[]) new CompletableFuture[partitions];

        for (int partition = 0; partition < partitions; partition++) {
            futs[partition] = placementDriver.awaitPrimaryReplica(
                    new TablePartitionId(tableId, partition),
                    clock.now(),
                    AWAIT_PRIMARY_REPLICA_TIMEOUT,
                    SECONDS
            );
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(futs);

        return all.thenApply(v -> {
            List<String> replicaNames = new ArrayList<>(partitions);

            for (int partition = 0; partition < partitions; partition++) {
                ReplicaMeta replicaMeta = futs[partition].join();
                assert replicaMeta != null : "Primary replica is null for partition " + partition;

                replicaNames.add(replicaMeta.getLeaseholder());
            }

            return replicaNames;
        });
    }
}
