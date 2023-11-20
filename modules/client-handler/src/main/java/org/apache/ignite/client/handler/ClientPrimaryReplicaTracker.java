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

import static org.apache.ignite.lang.ErrorGroups.Table.TABLE_NOT_FOUND_ERR;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 *
 * <p>Keeps up-to-date lists of primary replicas by partition for every table, avoiding expensive placement driver calls in most cases.
 */
public class ClientPrimaryReplicaTracker implements EventListener<EventParameters> {
    private final ConcurrentHashMap<TablePartitionId, ReplicaHolder> primaryReplicas = new ConcurrentHashMap<>();

    private final AtomicLong maxStartTime = new AtomicLong();

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final CatalogService catalogService;

    private final SchemaSyncService schemaSyncService;

    /**
     * Constructor.
     *
     * @param placementDriver Placement driver.
     * @param catalogService Catalog.
     * @param clock Hybrid clock.
     * @param schemaSyncService Schema synchronization service.
     */
    public ClientPrimaryReplicaTracker(
            PlacementDriver placementDriver,
            CatalogService catalogService,
            HybridClock clock,
            SchemaSyncService schemaSyncService) {
        this.placementDriver = placementDriver;
        this.catalogService = catalogService;
        this.clock = clock;
        this.schemaSyncService = schemaSyncService;
    }

    /**
     * Gets primary replicas by partition for the table.
     *
     * @param tableId Table ID.
     * @param maxStartTime Timestamp.
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<PrimaryReplicasResult> primaryReplicasAsync(int tableId, @Nullable Long maxStartTime) {
        HybridTimestamp timestamp = clock.now();

        if (maxStartTime == null) {
            maxStartTime = this.maxStartTime.get();
        }
        else {
            // If the client provides an old maxStartTime, ignore it and use the current one.
            maxStartTime = Math.max(maxStartTime, this.maxStartTime.get());
        }

        // Check happy path: if we already have all replicas, and maxStartTime > timestamp, return synchronously.
        var fastRes = primaryReplicasNoWait(tableId, maxStartTime, timestamp);
        if (fastRes != null) {
            return CompletableFuture.completedFuture(fastRes);
        }

        // Request primary for all partitions.
        var partitionsFut = partitionsAsync(tableId, timestamp).thenCompose(partitions -> {
            CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

            for (int partition = 0; partition < partitions; partition++) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);

                futures[partition] = placementDriver.getPrimaryReplica(tablePartitionId, timestamp).thenAccept(replicaMeta -> {
                    if (replicaMeta != null && replicaMeta.getLeaseholder() != null) {
                        updatePrimaryReplica(tablePartitionId, replicaMeta.getStartTime(), replicaMeta.getLeaseholder());
                    }
                });
            }

            return CompletableFuture.allOf(futures);
        });

        // Wait for all futures, check condition again.
        // Give up (return null) if we don't have replicas with specified maxStartTime - the client will retry later.
        long maxStartTime0 = maxStartTime;
        return partitionsFut.thenApply(v -> primaryReplicasNoWait(tableId, maxStartTime0, timestamp));
    }

    @Nullable
    private PrimaryReplicasResult primaryReplicasNoWait(int tableId, long maxStartTime, HybridTimestamp timestamp) {
        long currentMaxStartTime = this.maxStartTime.get();
        if (currentMaxStartTime < maxStartTime) {
            return null;
        }

        int partitions;

        try {
            partitions = partitionsNoWait(tableId, timestamp);
        }
        catch (IllegalStateException | TableNotFoundException e) {
            // Table or schema not found for because we did not wait.
            return null;
        }

        List<String> res = new ArrayList<>(partitions);

        for (int partition = 0; partition < partitions; partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);
            ReplicaHolder holder = primaryReplicas.get(tablePartitionId);

            if (holder == null || holder.nodeName == null || holder.leaseStartTime == null) {
                return null;
            }

            res.add(holder.nodeName);
        }

        return new PrimaryReplicasResult(res, currentMaxStartTime);
    }

    private CompletableFuture<Integer> partitionsAsync(int tableId, HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp).thenApply(v -> partitionsNoWait(tableId, timestamp));
    }

    private int partitionsNoWait(int tableId, HybridTimestamp timestamp) {
        CatalogTableDescriptor table = catalogService.table(tableId, timestamp.longValue());

        if (table == null) {
            throw tableNotFoundException(tableId);
        }

        var zone = catalogService.zone(table.zoneId(), timestamp.longValue());

        if (zone == null) {
            throw tableNotFoundException(tableId);
        }

        return zone.partitions();
    }

    long maxStartTime() {
        return maxStartTime.get();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void start() {
        // This could be newer than the actual max start time, but we are on the safe side here.
        maxStartTime.set(clock.nowLong());

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (EventListener) this);
        catalogService.listen(CatalogEvent.TABLE_DROP, (EventListener) this);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void stop() {
        catalogService.removeListener(CatalogEvent.TABLE_DROP, (EventListener) this);
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (EventListener) this);
        primaryReplicas.clear();
    }

    @Override
    public CompletableFuture<Boolean> notify(EventParameters parameters, @Nullable Throwable exception) {
        if (exception != null) {
            return CompletableFuture.completedFuture(false);
        }

        if (parameters instanceof DropTableEventParameters) {
            removeTable((DropTableEventParameters) parameters);

            return CompletableFuture.completedFuture(false);
        }

        if (!(parameters instanceof PrimaryReplicaEventParameters)) {
            assert false : "Unexpected event parameters: " + parameters.getClass();

            return CompletableFuture.completedFuture(false);
        }

        PrimaryReplicaEventParameters primaryReplicaEvent = (PrimaryReplicaEventParameters) parameters;
        if (!(primaryReplicaEvent.groupId() instanceof TablePartitionId)) {
            return CompletableFuture.completedFuture(false);
        }

        TablePartitionId tablePartitionId = (TablePartitionId) primaryReplicaEvent.groupId();
        updatePrimaryReplica(tablePartitionId, primaryReplicaEvent.startTime(), primaryReplicaEvent.leaseholder());

        return CompletableFuture.completedFuture(false); // false: don't remove listener.
    }

    private void removeTable(DropTableEventParameters dropTableEvent) {
        CatalogTableDescriptor table = catalogService.table(dropTableEvent.tableId(), dropTableEvent.catalogVersion());
        assert table != null : "Table from DropTableEventParameters not found: " + dropTableEvent.tableId();

        var zone = catalogService.zone(table.zoneId(), dropTableEvent.catalogVersion());
        assert zone != null : "Zone from DropTableEventParameters not found: " + table.zoneId();

        for (int partition = 0; partition < zone.partitions(); partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(dropTableEvent.tableId(), partition);
            primaryReplicas.remove(tablePartitionId);
        }
    }

    private void updatePrimaryReplica(TablePartitionId tablePartitionId, HybridTimestamp startTime, String nodeName) {
        long startTimeLong = startTime.longValue();

        primaryReplicas.compute(tablePartitionId, (key, existingVal) -> {
            if (existingVal != null && existingVal.leaseStartTime != null
                    && existingVal.leaseStartTime.longValue() >= startTimeLong) {
                return existingVal;
            }

            return new ReplicaHolder(nodeName, startTime);
        });

        while (true) {
            long maxStartTime0 = maxStartTime.get();

            if (startTimeLong <= maxStartTime0) {
                break;
            }

            if (maxStartTime.compareAndSet(maxStartTime0, startTimeLong)) {
                break;
            }
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private static TableNotFoundException tableNotFoundException(Integer tableId) {
        return new TableNotFoundException(UUID.randomUUID(), TABLE_NOT_FOUND_ERR, "Table not found: " + tableId, null);
    }

    private static class ReplicaHolder {
        final String nodeName;

        final HybridTimestamp leaseStartTime;

        ReplicaHolder(String nodeName, HybridTimestamp leaseStartTime) {
            this.nodeName = nodeName;
            this.leaseStartTime = leaseStartTime;
        }
    }

    public static class PrimaryReplicasResult {
        private final List<String> nodeNames;

        private final long timestamp;

        PrimaryReplicasResult(List<String> nodeNames, long timestamp) {
            this.nodeNames = nodeNames;
            this.timestamp = timestamp;
        }

        public List<String> nodeNames() {
            return nodeNames;
        }

        public long timestamp() {
            return timestamp;
        }
    }
}
