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
import org.jetbrains.annotations.NotNull;
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
     * @param timestamp Timestamp.
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<List<String>> primaryReplicasAsync(int tableId, HybridTimestamp timestamp) {
        // 0. Check happy path: if we already have all replicas, and maxStartTime > timestamp, return synchronously.
        var fastRes = primaryReplicasNoWait(tableId, timestamp);
        if (fastRes != null) {
            return CompletableFuture.completedFuture(fastRes);
        }

        // 1. Make sure all partitions for the current table are initialized
        var initPartitionsFut = partitionsAsync(tableId, timestamp).thenCompose(partitions -> {
            CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

            for (int partition = 0; partition < partitions; partition++) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);

                ReplicaHolder holder = primaryReplicas.compute(tablePartitionId, (id, old) -> {
                    if (old != null) {
                        if (old.nodeName != null || !old.fut.isDone()) {
                            return old;
                        }
                    }

                    // Old does not exist, or completed with null.
                    CompletableFuture<ReplicaMeta> fut = placementDriver.getPrimaryReplica(tablePartitionId, timestamp);
                    var newHolder = new ReplicaHolder(null, null, fut);

                    fut.thenAccept(replicaMeta -> {
                        if (replicaMeta != null && replicaMeta.getLeaseholder() != null) {
                            newHolder.update(replicaMeta.getLeaseholder(), replicaMeta.getStartTime());
                        }
                    });

                    return newHolder;
                });

                futures[partition] = holder.fut();
            }

            return CompletableFuture.allOf(futures);
        });

        // 2. Wait for all futures to complete, check if replicas are now new enough.
        return initPartitionsFut.thenCompose(v -> {
            var res = primaryReplicasNoWait(tableId, timestamp);
            if (res != null) {
                return CompletableFuture.completedFuture(res);
            }

            // Still not new enough, wait for events to come.
            // TODO
            return CompletableFuture.completedFuture(null);
        });
    }

    @Nullable
    private List<String> primaryReplicasNoWait(int tableId, HybridTimestamp timestamp) {
        int partitions;

        try {
            partitions = partitionsNoWait(tableId, timestamp);
        }
        catch (IllegalStateException e) {
            // No valid schema found for given timestamp (because we did not wait).
            return null;
        }

        List<String> res = new ArrayList<>(partitions);
        long maxStartTime = 0;

        for (int partition = 0; partition < partitions; partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);

            ReplicaHolder holder = primaryReplicas.get(tablePartitionId);

            if (holder == null) {
                return null;
            }

            res.add(holder.nodeName());
            maxStartTime = Math.max(maxStartTime, holder.leaseStartTime().longValue());
        }

        return maxStartTime >= timestamp.longValue() ? res : null;
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

    private CompletableFuture<Integer> partitionsAsync(int tableId, HybridTimestamp timestamp) {
        return schemaSyncService.waitForMetadataCompleteness(timestamp).thenApply(v -> partitionsNoWait(tableId, timestamp));
    }

    @Override
    public CompletableFuture<Boolean> notify(EventParameters parameters, @Nullable Throwable exception) {
        if (exception != null) {
            return CompletableFuture.completedFuture(false);
        }

        if (parameters instanceof DropTableEventParameters) {
            DropTableEventParameters dropTableEvent = (DropTableEventParameters) parameters;

            // TODO: Remove one by one.
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
        primaryReplicas.compute(tablePartitionId, (ignore, old) -> {
            if (old != null && old.leaseStartTime != null && old.leaseStartTime.longValue() >= primaryReplicaEvent.startTime().longValue()) {
                // We already have a newer replica.
                return old;
            }

            // The event is newer.
            return new ReplicaHolder(
                    primaryReplicaEvent.leaseholder(),
                    primaryReplicaEvent.startTime(),
                    CompletableFuture.completedFuture(null));
        });


        // Update always, even if the table is not tracked. Client could retrieve the table from another node.
        updateMaxStartTime(primaryReplicaEvent.startTime().longValue());

        return CompletableFuture.completedFuture(false); // false: don't remove listener.
    }

    private void updateMaxStartTime(long startTime) {
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
    static class ReplicaHolder {
        /** Current future that will populate name and time. */
        private final CompletableFuture<?> fut;

        /** Node name. */
        @Nullable
        private volatile String nodeName;

        /** Lease start time. */
        @Nullable
        private volatile HybridTimestamp leaseStartTime;

        ReplicaHolder(@Nullable String nodeName, @Nullable HybridTimestamp leaseStartTime, CompletableFuture<?> fut) {
            this.nodeName = nodeName;
            this.leaseStartTime = leaseStartTime;
            this.fut = fut;
        }

        public CompletableFuture<?> fut() {
            return fut;
        }

        public String nodeName() {
            return nodeName;
        }

        public HybridTimestamp leaseStartTime() {
            return leaseStartTime;
        }

        synchronized void update(String nodeName, HybridTimestamp leaseStartTime) {
            if (this.leaseStartTime == null || this.leaseStartTime.longValue() < leaseStartTime.longValue()) {
                this.nodeName = nodeName;
                this.leaseStartTime = leaseStartTime;
            }
        }
    }
}
