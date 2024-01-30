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
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;

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
import org.apache.ignite.internal.event.EventParameters;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEvent;
import org.apache.ignite.internal.placementdriver.event.PrimaryReplicaEventParameters;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
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
public class ClientPrimaryReplicaTracker implements EventListener<EventParameters> {
    private final ConcurrentHashMap<TablePartitionId, ReplicaHolder> primaryReplicas = new ConcurrentHashMap<>();

    private final AtomicLong maxStartTime = new AtomicLong();

    private final PlacementDriver placementDriver;

    private final HybridClock clock;

    private final CatalogService catalogService;

    private final SchemaSyncService schemaSyncService;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

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
            SchemaSyncService schemaSyncService
    ) {
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
        HybridTimestamp timestamp = clock.now();

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
        CompletableFuture<Void> partitionsFut = partitionsAsync(tableId, timestamp).thenCompose(partitions -> {
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
        return partitionsFut.handle((v, err) -> {
            if (err != null) {
                var cause = ExceptionUtils.unwrapCause(err);

                if (cause instanceof TableNotFoundException) {
                    throw new CompletionException(cause);
                }

                assert false : "Unexpected error: " + err;
            }

            return primaryReplicasNoWait(tableId, maxStartTime0, timestamp, true);
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

    @SuppressWarnings({"rawtypes", "unchecked"})
    void start() {
        // This could be newer than the actual max start time, but we are on the safe side here.
        maxStartTime.set(clock.nowLong());

        placementDriver.listen(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (EventListener) this);
        catalogService.listen(CatalogEvent.TABLE_DROP, (EventListener) this);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    void stop() {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        catalogService.removeListener(CatalogEvent.TABLE_DROP, (EventListener) this);
        placementDriver.removeListener(PrimaryReplicaEvent.PRIMARY_REPLICA_ELECTED, (EventListener) this);
        primaryReplicas.clear();
    }

    @Override
    public CompletableFuture<Boolean> notify(EventParameters parameters, @Nullable Throwable exception) {
        if (!busyLock.enterBusy()) {
            return CompletableFuture.failedFuture(new NodeStoppingException());
        }

        try {
            if (exception != null) {
                return falseCompletedFuture();
            }

            return notifyInternal(parameters);
        } finally {
            busyLock.leaveBusy();
        }
    }

    private CompletableFuture<Boolean> notifyInternal(EventParameters parameters) {
        if (parameters instanceof DropTableEventParameters) {
            removeTable((DropTableEventParameters) parameters);

            return falseCompletedFuture();
        }

        if (!(parameters instanceof PrimaryReplicaEventParameters)) {
            assert false : "Unexpected event parameters: " + parameters.getClass();

            return falseCompletedFuture();
        }

        PrimaryReplicaEventParameters primaryReplicaEvent = (PrimaryReplicaEventParameters) parameters;
        if (!(primaryReplicaEvent.groupId() instanceof TablePartitionId)) {
            return falseCompletedFuture();
        }

        TablePartitionId tablePartitionId = (TablePartitionId) primaryReplicaEvent.groupId();

        updatePrimaryReplica(tablePartitionId, primaryReplicaEvent.startTime(), primaryReplicaEvent.leaseholder());

        return falseCompletedFuture(); // false: don't remove listener.
    }

    private void removeTable(DropTableEventParameters dropTableEvent) {
        // Use previous version of the catalog to get the dropped table.
        int prevCatalogVersion = dropTableEvent.catalogVersion() - 1;

        CatalogTableDescriptor table = catalogService.table(dropTableEvent.tableId(), prevCatalogVersion);
        assert table != null : "Table from DropTableEventParameters not found: " + dropTableEvent.tableId();

        CatalogZoneDescriptor zone = catalogService.zone(table.zoneId(), prevCatalogVersion);
        assert zone != null : "Zone from DropTableEventParameters not found: " + table.zoneId();

        for (int partition = 0; partition < zone.partitions(); partition++) {
            TablePartitionId tablePartitionId = new TablePartitionId(dropTableEvent.tableId(), partition);
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
