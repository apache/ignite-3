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
     * @param timestamp Timestamp.
     * @return Primary replicas for the table, or null when not yet known.
     */
    public CompletableFuture<List<String>> primaryReplicasAsync(int tableId, HybridTimestamp timestamp) {
        // 1. Make sure all partitions for the current table are initialized
        partitionsAsync(tableId, timestamp).thenCompose(partitions -> {
            List<CompletableFuture<?>> futures = new ArrayList<>();

            for (int partition = 0; partition < partitions; partition++) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableId, partition);

                primaryReplicas.computeIfAbsent(tablePartitionId, id -> {
                    CompletableFuture<ReplicaMeta> fut = placementDriver.getPrimaryReplica(tablePartitionId, timestamp)
                            .thenAccept(meta -> {
                                if (meta != null) {
                                    updateMaxStartTime(meta.getStartTime().longValue());

                                    return new ReplicaHolder(meta.nodeName(), meta.leaseStartTime(),
                                            CompletableFuture.completedFuture(null));
                                }
                            });

                    return new ReplicaHolder(null, null, fut);
                });
            }
        });

        // 2. Wait for all futures to complete
        // 3. Check if current max timestamp for one of the partitions is >= specified timestamp. If yes, return current results.
        // 4. If no, wait for the event.
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
        return schemaSyncService.waitForMetadataCompleteness(timestamp).thenApply(v -> {
            CatalogTableDescriptor table = catalogService.table(tableId, timestamp.longValue());

            if (table == null) {
                throw tableNotFoundException(tableId);
            }

            var zone = catalogService.zone(table.zoneId(), timestamp.longValue());

            if (zone == null) {
                throw tableNotFoundException(tableId);
            }

            return zone.partitions();
        });
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
        final CompletableFuture<?> fut;

        /** Node name. */
        @Nullable
        final String nodeName;

        /** Lease start time. */
        @Nullable
        final HybridTimestamp leaseStartTime;

        ReplicaHolder(@Nullable String nodeName, @Nullable HybridTimestamp leaseStartTime, CompletableFuture<?> fut) {
            this.nodeName = nodeName;
            this.leaseStartTime = leaseStartTime;
            this.fut = fut;
        }
    }
}
