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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.client.handler.requests.table.ClientTableCommon.tableIdNotFoundException;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.catalog.CatalogService;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEvent;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.event.EventListener;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.lowwatermark.LowWatermark;
import org.apache.ignite.internal.lowwatermark.event.ChangeLowWatermarkEventParameters;
import org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.table.LongPriorityQueue;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.TableNotFoundException;
import org.jetbrains.annotations.Nullable;

/**
 * Primary partition replica tracker. Shared by all instances of {@link ClientInboundMessageHandler}.
 *
 * <p>Keeps up-to-date lists of primary replicas by partition for every table, avoiding expensive placement driver calls in most cases.
 *
 * <p>Every "assignment" (set of primary replicas per partition) is identified by a maxStartTime - latest known lease start time.
 *
 * <p>Assumptions:
 *     - Primary replicas are not changed often.
 *     - We do "best effort" partition awareness - it is ok if we don't have the latest primary replicas at some point or don't have them
 *       at all. What matters is that we have the correct assignment eventually.
 *     - It is allowed to return incomplete assignment (null for some partitions) - better than nothing.
 *     - We don't know which tables the client is going to use, so we track a common maxStartTime for all tables.
 *
 * <p>Tracking logic:
 *     - Listen to election events from placement driver, update primary replicas. This is the main source of information.
 *     - When we have not yet received events for all partitions of a certain table, and the client requests the assignment,
 *       load it from the placement driver. Wait for a limited amount of time (in getPrimaryReplica) and return what we have.
 *       Don't block the client for too long, it is better to miss the primary than to delay the request.
 */
public class ClientPrimaryReplicaTracker {
    private final ConcurrentHashMap<TablePartitionId, ReplicaHolder> primaryReplicas = new ConcurrentHashMap<>();

    private final AtomicLong maxStartTime = new AtomicLong();

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final CatalogService catalogService;

    private final SchemaSyncService schemaSyncService;

    private final LowWatermark lowWatermark;

    private final EventListener<ChangeLowWatermarkEventParameters> lwmListener = fromConsumer(this::onLwmChanged);
    private final EventListener<DropTableEventParameters> dropTableEventListener = fromConsumer(this::onTableDrop);
    private final EventListener<PrimaryReplicaEventParameters> primaryReplicaEventListener = fromConsumer(this::onPrimaryReplicaChanged);

    /** A queue for deferred table destruction events. */
    private final LongPriorityQueue<DestroyTableEvent> destructionEventsQueue =
            new LongPriorityQueue<>(DestroyTableEvent::catalogVersion);

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /**
     * Constructor.
     *
     * @param placementDriver Placement driver.
     * @param catalogService Catalog.
     * @param clockService Clock service.
     * @param schemaSyncService Schema synchronization service.
     * @param lowWatermark Low watermark.
     */
    public ClientPrimaryReplicaTracker(
            PlacementDriver placementDriver,
            CatalogService catalogService,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            LowWatermark lowWatermark
    ) {
        this.placementDriver = placementDriver;
        this.catalogService = catalogService;
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.lowWatermark = lowWatermark;
    }

    /**
     * Gets primary replicas by partition for the table.
     *
     * @param tableId Table ID.
     * @param maxStartTime Timestamp.
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<PrimaryReplicasResult> primaryReplicasAsync(int tableId, @Nullable Long maxStartTime) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            return primaryReplicasAsyncInternal(tableId, maxStartTime);
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets primary replicas by partition for the table.
     *
     * @param tableId Table ID.
     * @param maxStartTime Timestamp.
     * @return Primary replicas for the table, or null when not yet known.
     */
    private CompletableFuture<PrimaryReplicasResult> primaryReplicasAsyncInternal(int tableId, @Nullable Long maxStartTime) {
        HybridTimestamp timestamp = clockService.now();

        if (maxStartTime == null) {
            maxStartTime = this.maxStartTime.get();
        } else {
            // If the client provides an old maxStartTime, ignore it and use the current one.
            maxStartTime = Math.max(maxStartTime, this.maxStartTime.get());
        }

        // Check happy path: if we already have all replicas, and this.maxStartTime >= maxStartTime, return synchronously.
        PrimaryReplicasResult fastRes = primaryReplicasNoWait(tableId, maxStartTime, timestamp, false);
        if (fastRes != null) {
            return completedFuture(fastRes);
        }

        // Request primary for all partitions.
        CompletableFuture<Integer> partitionsFut = partitionsAsync(tableId, timestamp).thenCompose(partitions -> {
            CompletableFuture<?>[] futures = new CompletableFuture<?>[partitions];

            for (int partition = 0; partition < partitions; partition++) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);

                futures[partition] = placementDriver.getPrimaryReplica(tablePartitionId, timestamp).thenAccept(replicaMeta -> {
                    if (replicaMeta != null && replicaMeta.getLeaseholder() != null) {
                        updatePrimaryReplica(tablePartitionId, replicaMeta.getStartTime(), replicaMeta.getLeaseholder());
                    }
                });
            }

            return CompletableFuture.allOf(futures).thenApply(v -> partitions);
        });

        // Wait for all futures, check condition again.
        // Give up (return null) if we don't have replicas with specified maxStartTime - the client will retry later.
        long maxStartTime0 = maxStartTime;
        return partitionsFut.handle((partitions, err) -> {
            if (err != null) {
                var cause = ExceptionUtils.unwrapCause(err);

                if (cause instanceof TableNotFoundException) {
                    throw new CompletionException(cause);
                }

                assert false : "Unexpected error: " + err;
            }

            PrimaryReplicasResult res = primaryReplicasNoWait(tableId, maxStartTime0, timestamp, true);
            if (res != null) {
                return res;
            }

            // Return partition count to the client so that batching can be initialized.
            return new PrimaryReplicasResult(partitions);
        });
    }

    @Nullable
    private PrimaryReplicasResult primaryReplicasNoWait(
            int tableId, long maxStartTime, HybridTimestamp timestamp, boolean allowUnknownReplicas) {
        long currentMaxStartTime = this.maxStartTime.get();
        if (currentMaxStartTime < maxStartTime) {
            return null;
        }

        int partitions;

        try {
            partitions = partitionsNoWait(tableId, timestamp);
        } catch (IllegalStateException | TableNotFoundException e) {
            // Table or schema not found for because we did not wait.
            return null;
        }

        List<String> res = new ArrayList<>(partitions);

        for (int partition = 0; partition < partitions; partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);
            ReplicaHolder holder = primaryReplicas.get(tablePartitionId);

            if (holder == null || holder.nodeName == null || holder.leaseStartTime == null) {
                if (allowUnknownReplicas) {
                    res.add(null);
                    continue;
                } else {
                    return null;
                }
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
            throw tableIdNotFoundException(tableId);
        }

        CatalogZoneDescriptor zone = catalogService.zone(table.zoneId(), timestamp.longValue());

        if (zone == null) {
            throw tableIdNotFoundException(tableId);
        }

        return zone.partitions();
    }

    long maxStartTime() {
        return maxStartTime.get();
    }

    void start() {
        // This could be newer than the actual max start time, but we are on the safe side here.
        maxStartTime.set(clockService.nowLong());

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaEventListener);
        catalogService.listen(CatalogEvent.TABLE_DROP, dropTableEventListener);
        lowWatermark.listen(LowWatermarkEvent.LOW_WATERMARK_CHANGED, lwmListener);
    }

    void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        lowWatermark.removeListener(LowWatermarkEvent.LOW_WATERMARK_CHANGED, lwmListener);
        catalogService.removeListener(CatalogEvent.TABLE_DROP, dropTableEventListener);
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, primaryReplicaEventListener);
        primaryReplicas.clear();
    }

    private void onPrimaryReplicaChanged(PrimaryReplicaEventParameters primaryReplicaEvent) {
        inBusyLock(busyLock, () -> {
            if (!(primaryReplicaEvent.groupId() instanceof TablePartitionId)) {
                return;
            }

            TablePartitionId tablePartitionId = (TablePartitionId) primaryReplicaEvent.groupId();

            updatePrimaryReplica(tablePartitionId, primaryReplicaEvent.startTime(), primaryReplicaEvent.leaseholder());
        });
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int tableId = parameters.tableId();
            int catalogVersion = parameters.catalogVersion();
            int previousVersion = catalogVersion - 1;

            // Retrieve descriptor during synchronous call, before the previous catalog version could be concurrently compacted.
            int partitions = getTablePartitionsFromCatalog(catalogService, previousVersion, tableId);

            destructionEventsQueue.enqueue(new DestroyTableEvent(catalogVersion, tableId, partitions));
        });
    }

    private void onLwmChanged(ChangeLowWatermarkEventParameters parameters) {
        inBusyLock(busyLock, () -> {
            int earliestVersion = catalogService.activeCatalogVersion(parameters.newLowWatermark().longValue());

            List<DestroyTableEvent> events = destructionEventsQueue.drainUpTo(earliestVersion);

            events.forEach(event -> removeTable(event.tableId(), event.partitions()));
        });
    }

    private void removeTable(int tableId, int partitions) {
        for (int partition = 0; partition < partitions; partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);
            primaryReplicas.remove(tablePartitionId);
        }
    }

    private void updatePrimaryReplica(TablePartitionId tablePartitionId, HybridTimestamp startTime, String nodeName) {
        long startTimeLong = startTime.longValue();

        primaryReplicas.compute(tablePartitionId, (key, existingVal) -> {
            if (existingVal != null
                    && existingVal.leaseStartTime != null
                    && existingVal.leaseStartTime.longValue() >= startTimeLong) {
                return existingVal;
            }

            return new ReplicaHolder(nodeName, startTime);
        });

        maxStartTime.updateAndGet(value -> Math.max(value, startTimeLong));
    }

    private static int getTablePartitionsFromCatalog(CatalogService catalogService, int catalogVersion, int tableId) {
        CatalogTableDescriptor tableDescriptor = catalogService.table(tableId, catalogVersion);
        assert tableDescriptor != null : "tableId=" + tableId + ", catalogVersion=" + catalogVersion;

        int zoneId = tableDescriptor.zoneId();

        CatalogZoneDescriptor zoneDescriptor = catalogService.zone(zoneId, catalogVersion);
        assert zoneDescriptor != null : "zoneId=" + zoneId + ", catalogVersion=" + catalogVersion;

        return zoneDescriptor.partitions();
    }

    private static class ReplicaHolder {
        final String nodeName;

        final HybridTimestamp leaseStartTime;

        ReplicaHolder(String nodeName, HybridTimestamp leaseStartTime) {
            this.nodeName = nodeName;
            this.leaseStartTime = leaseStartTime;
        }
    }

    /**
     * Primary replicas per partition with timestamp.
     */
    public static class PrimaryReplicasResult {
        private final int partitions;

        @Nullable
        private final List<String> nodeNames;

        private final long timestamp;

        PrimaryReplicasResult(int partitions) {
            this.partitions = partitions;
            this.nodeNames = null;
            this.timestamp = 0;
        }

        PrimaryReplicasResult(List<String> nodeNames, long timestamp) {
            this.partitions = nodeNames.size();
            this.nodeNames = nodeNames;
            this.timestamp = timestamp;
        }

        public @Nullable List<String> nodeNames() {
            return nodeNames;
        }

        public long timestamp() {
            return timestamp;
        }

        public int partitions() {
            return partitions;
        }
    }

    /** Internal event. */
    private static class DestroyTableEvent {
        final int catalogVersion;
        final int tableId;
        final int partitions;

        DestroyTableEvent(int catalogVersion, int tableId, int partitions) {
            this.catalogVersion = catalogVersion;
            this.tableId = tableId;
            this.partitions = partitions;
        }

        public int catalogVersion() {
            return catalogVersion;
        }

        public int tableId() {
            return tableId;
        }

        public int partitions() {
            return partitions;
        }
    }
}
