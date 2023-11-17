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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
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

    private final AtomicReference<Long> maxStartTime = new AtomicReference<>();

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
     * @param observableTs
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<List<ReplicaHolder>> primaryReplicasAsync(int tableId, HybridTimestamp observableTs) {
        schemaSyncService.waitForMetadataCompleteness(observableTs).thenCompose(v -> {
            CatalogTableDescriptor table = catalogService.table(tableId, observableTs.longValue());

            if (table == null) {
                return CompletableFuture.failedFuture(tableNotFoundException(tableId));
            }

            var zone = catalogService.zone(table.zoneId(), observableTs.longValue());

            if (zone == null) {
                return CompletableFuture.failedFuture(tableNotFoundException(tableId));
            }

            List<CompletableFuture<ReplicaHolder>> futs = new ArrayList<>(zone.partitions());

            for (int partition = 0; partition < zone.partitions(); partition++) {
                TablePartitionId key = new TablePartitionId(tableId, partition);
                var holder = primaryReplicas.get(key);
            }
        };
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

    private CompletableFuture<List<ReplicaHolder>> initReplicasForTableAsync(Integer tableId, HybridTimestamp timestamp) {
        try {
            // Initially, request all primary replicas for the table.
            // Then keep them updated via PRIMARY_REPLICA_ELECTED events.

            // TODO: Use observable timestamp from client
            // TODO: Use SchemaSyncService to wait for tables using client timestamp.
            CatalogTableDescriptor tableDesc = catalogService.table(tableId, timestamp.longValue());

            if (tableDesc == null) {
                return CompletableFuture.failedFuture(tableNotFoundException(tableId));
            }

            CatalogZoneDescriptor zoneDesc = catalogService.zone(tableDesc.zoneId(), timestamp.longValue());

            if (zoneDesc == null) {
                return CompletableFuture.failedFuture(tableNotFoundException(tableId));
            }

            return primaryReplicasAsyncInternal(tableId, zoneDesc.partitions(), timestamp.longValue());
        } catch (Throwable t) {
            return CompletableFuture.failedFuture(t);
        }
    }

    private CompletableFuture<List<ReplicaHolder>> primaryReplicasAsyncInternal(int tableId, int partitions, long timestamp) {
        CompletableFuture<ReplicaMeta>[] futs = (CompletableFuture<ReplicaMeta>[]) new CompletableFuture[partitions];

        for (int partition = 0; partition < partitions; partition++) {
            futs[partition] = placementDriver.getPrimaryReplica(
                    new TablePartitionId(tableId, partition), HybridTimestamp.hybridTimestamp(timestamp));
        }

        CompletableFuture<Void> all = CompletableFuture.allOf(futs);

        return all.thenApply(v -> {
            List<ReplicaHolder> replicaHolders = new ArrayList<>(partitions);

            for (int partition = 0; partition < partitions; partition++) {
                ReplicaMeta replicaMeta = futs[partition].join();

                // Returning null is fine - the client will use default channel for this partition.
                replicaHolders.add(replicaMeta == null
                        ? new ReplicaHolder(null, HybridTimestamp.MIN_VALUE)
                        : new ReplicaHolder(replicaMeta.getLeaseholder(), replicaMeta.getStartTime()));
            }

            return replicaHolders;
        });
    }

    @Override
    public CompletableFuture<Boolean> notify(EventParameters parameters, @Nullable Throwable exception) {
        if (exception != null) {
            return CompletableFuture.completedFuture(false);
        }

        if (parameters instanceof DropTableEventParameters) {
            DropTableEventParameters dropTableEvent = (DropTableEventParameters) parameters;
            primaryReplicas.remove(dropTableEvent.tableId());

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
        var hld = primaryReplicas.get(tablePartitionId.tableId());

        // TODO: Race condition. Use a single map with TablePartitionId as a key to simplify and fix.
        if (hld != null) {
            hld.replicas.thenAccept(replicas ->
                    replicas.get(tablePartitionId.partitionId())
                            .update(primaryReplicaEvent.leaseholder(), primaryReplicaEvent.startTime()));
        }

        // Update always, even if the table is not tracked. Client could retrieve the table from another node.
        // TODO: Update when calling getPrimaryReplica too.
        updateMaxStartTime(primaryReplicaEvent);

        // LRU check: remove entries that were not accessed for a long time to reduce memory usage.
        // Check on every event, but not more often than LRU_CHECK_FREQ.
        long lruCheckTime0 = lruCheckTime.get();
        long time = System.currentTimeMillis();
        if (time - lruCheckTime0 > LRU_CHECK_FREQ_MILLIS && lruCheckTime.compareAndSet(lruCheckTime0, time)) {
            primaryReplicas.entrySet().removeIf(e -> time - e.getValue().lastAccessTime > LRU_CHECK_FREQ_MILLIS * 2);
        }

        return CompletableFuture.completedFuture(false); // false: don't remove listener.
    }

    private void updateMaxStartTime(PrimaryReplicaEventParameters primaryReplicaEvent) {
        long startTime = primaryReplicaEvent.startTime().longValue();

        while (true) {
            long maxStartTime0 = maxStartTime.get();

            if (startTime <= maxStartTime0) {
                break;
            }

            if (maxStartTime.compareAndSet(maxStartTime0, startTime)) {
                break;
            }
        }
    }

    @SuppressWarnings("DataFlowIssue")
    private static TableNotFoundException tableNotFoundException(Integer tableId) {
        return new TableNotFoundException(UUID.randomUUID(), TABLE_NOT_FOUND_ERR, "Table not found: " + tableId, null);
    }

    /**
     * Replica holder.
     */
    public static class ReplicaHolder {
        @Nullable
        private volatile String nodeName;

        private HybridTimestamp leaseStartTime;

        private ReplicaHolder(@Nullable String nodeName, HybridTimestamp leaseStartTime) {
            this.nodeName = nodeName;
            this.leaseStartTime = leaseStartTime;
        }

        public @Nullable String nodeName() {
            return nodeName;
        }

        private synchronized void update(String nodeName, HybridTimestamp leaseStartTime) {
            // Ignore old updates.
            if (leaseStartTime.compareTo(this.leaseStartTime) > 0) {
                this.nodeName = nodeName;
                this.leaseStartTime = leaseStartTime;
            }
        }
    }
}
