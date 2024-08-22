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

package org.apache.ignite.internal.catalog.compaction;

import static java.util.function.Predicate.not;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessageGroup;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessagesFactory;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesResponse;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionPrepareUpdateTxBeginTimeMessage;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.table.distributed.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.ActiveLocalTxMinimumBeginTimeProvider;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Catalog compaction runner.
 *
 * <p>The main goal of this runner is to determine the minimum required catalog version for the cluster
 * and the moment when the catalog history up to this version can be safely deleted.
 *
 * <p>Overall process consists of the following steps:
 * <ol>
 *     <li>Routine is triggered after receiving local notification that the low watermark
 *     has been updated on catalog compaction coordinator (metastorage group leader).</li>
 *     <li>Coordinator calculates the minimum required time in the cluster
 *     by sending {@link CatalogCompactionMinimumTimesRequest} to all cluster members.</li>
 *     <li>If it is considered safe to trim the history up to calculated catalog version
 *     (at least, all partition owners are present in the logical topology), then the catalog is compacted.</li>
 * </ol>
 */
public class CatalogCompactionRunner implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(CatalogCompactionRunner.class);

    private static final CatalogCompactionMessagesFactory COMPACTION_MESSAGES_FACTORY = new CatalogCompactionMessagesFactory();

    private static final PartitionReplicationMessagesFactory REPLICATION_MESSAGES_FACTORY = new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final long ANSWER_TIMEOUT = 10_000;

    private final CatalogManagerCompactionFacade catalogManagerFacade;

    private final MessagingService messagingService;

    private final LogicalTopologyService logicalTopologyService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final MinimumRequiredTimeProvider localMinTimeProvider;

    private final String localNodeName;

    private final ActiveLocalTxMinimumBeginTimeProvider activeLocalTxMinimumBeginTimeProvider;

    private final ReplicaService replicaService;

    private final SchemaSyncService schemaSyncService;

    private final TopologyService topologyService;

    private CompletableFuture<Void> lastRunFuture = CompletableFutures.nullCompletedFuture();

    /**
     * Node that is considered to be a coordinator of compaction process.
     *
     * <p>May be not set. Node should act as coordinator only in case this field is set and value is equal to name of the local node.
     */
    private volatile @Nullable String compactionCoordinatorNodeName;

    private volatile HybridTimestamp lowWatermark;

    private volatile String localNodeId;

    /**
     * Constructs catalog compaction runner.
     */
    public CatalogCompactionRunner(
            String localNodeName,
            CatalogManagerImpl catalogManager,
            MessagingService messagingService,
            LogicalTopologyService logicalTopologyService,
            PlacementDriver placementDriver,
            ReplicaService replicaService,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            TopologyService topologyService,
            Executor executor,
            ActiveLocalTxMinimumBeginTimeProvider activeLocalTxMinimumBeginTimeProvider
    ) {
        this(localNodeName, catalogManager, messagingService, logicalTopologyService, placementDriver, replicaService, clockService,
                schemaSyncService, topologyService, executor, activeLocalTxMinimumBeginTimeProvider, null);
    }

    /**
     * Constructs catalog compaction runner with custom minimum required time provider.
     */
    CatalogCompactionRunner(
            String localNodeName,
            CatalogManagerImpl catalogManager,
            MessagingService messagingService,
            LogicalTopologyService logicalTopologyService,
            PlacementDriver placementDriver,
            ReplicaService replicaService,
            ClockService clockService,
            SchemaSyncService schemaSyncService,
            TopologyService topologyService,
            Executor executor,
            ActiveLocalTxMinimumBeginTimeProvider activeLocalTxMinimumBeginTimeProvider,
            @Nullable CatalogCompactionRunner.MinimumRequiredTimeProvider localMinTimeProvider
    ) {
        this.localNodeName = localNodeName;
        this.messagingService = messagingService;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManagerFacade = new CatalogManagerCompactionFacade(catalogManager);
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.topologyService = topologyService;
        this.placementDriver = placementDriver;
        this.replicaService = replicaService;
        this.executor = executor;
        this.activeLocalTxMinimumBeginTimeProvider = activeLocalTxMinimumBeginTimeProvider;
        this.localMinTimeProvider = localMinTimeProvider == null ? this::determineLocalMinimumRequiredTime : localMinTimeProvider;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(CatalogCompactionMessageGroup.class, new CatalogCompactionMessageHandler());

        localNodeId = topologyService.localMember().id();

        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        return CompletableFutures.nullCompletedFuture();
    }

    /** Updates the local view of the node with new compaction coordinator. */
    public void updateCoordinator(ClusterNode newCoordinator) {
        compactionCoordinatorNodeName = newCoordinator.name();

        triggerCompaction(lowWatermark);
    }

    /** Returns local view of the node on who is currently compaction coordinator. For test purposes only.*/
    @TestOnly
    public @Nullable String coordinator() {
        return compactionCoordinatorNodeName;
    }

    /** Called when the low watermark has been changed. */
    public CompletableFuture<Boolean> onLowWatermarkChanged(HybridTimestamp newLowWatermark) {
        lowWatermark = newLowWatermark;

        triggerCompaction(newLowWatermark);

        return CompletableFutures.falseCompletedFuture();
    }

    @TestOnly
    synchronized CompletableFuture<Void> lastRunFuture() {
        return lastRunFuture;
    }

    /** Starts the catalog compaction routine. */
    void triggerCompaction(@Nullable HybridTimestamp lwm) {
        if (lwm == null || !localNodeName.equals(compactionCoordinatorNodeName)) {
            return;
        }

        inBusyLock(busyLock, () -> {
            synchronized (this) {
                CompletableFuture<Void> fut = lastRunFuture;

                if (!fut.isDone()) {
                    LOG.info("Catalog compaction is already in progress, skipping [timestamp={}].", lwm.longValue());

                    return;
                }

                lastRunFuture = startCompaction(logicalTopologyService.localLogicalTopology());
            }
        });
    }

    private CompletableFuture<Void> startCompaction(LogicalTopologySnapshot topologySnapshot) {
        long localMinimum = localMinTimeProvider.time();

        if (catalogManagerFacade.catalogByTsNullable(localMinimum) == null) {
            LOG.info("Catalog compaction skipped, nothing to compact [timestamp={}].", localMinimum);

            return CompletableFutures.nullCompletedFuture();
        }

        return determineGlobalMinimumRequiredTime(topologySnapshot.nodes(), localMinimum)
                .thenComposeAsync(timeHolder -> {
                    long minRequiredTime = timeHolder.minRequiredTime;
                    long minActiveTxBeginTime = timeHolder.minActiveTxBeginTime;
                    Catalog catalog = catalogManagerFacade.catalogByTsNullable(minRequiredTime);

                    CompletableFuture<Boolean> catalogCompactionFut;

                    if (catalog == null) {
                        LOG.info("Catalog compaction skipped, nothing to compact [timestamp={}].", minRequiredTime);

                        catalogCompactionFut = CompletableFutures.falseCompletedFuture();
                    } else {
                        catalogCompactionFut = tryCompactCatalog(catalog, topologySnapshot).whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Catalog compaction has failed [timestamp={}].", ex, minRequiredTime);
                            } else {
                                if (res) {
                                    LOG.info("Catalog compaction completed successfully [timestamp={}].", minRequiredTime);
                                } else {
                                    LOG.info("Catalog compaction skipped [timestamp={}].", minRequiredTime);
                                }
                            }
                        });
                    }

                    CompletableFuture<Void> propagateToReplicasFut =
                            propagateTimeToNodes(minActiveTxBeginTime, topologySnapshot.nodes())
                                    .whenComplete((ignore, ex) -> {
                                        if (ex != null) {
                                            LOG.warn("Failed to propagate minimum required time to replicas.", ex);
                                        }
                                    });

                    return CompletableFuture.allOf(
                            catalogCompactionFut,
                            propagateToReplicasFut
                    );
                }, executor);
    }

    CompletableFuture<TimeHolder> determineGlobalMinimumRequiredTime(
            Collection<? extends ClusterNode> nodes,
            long localMinimumRequiredTime
    ) {
        CatalogCompactionMinimumTimesRequest request = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesRequest().build();
        List<CompletableFuture<CatalogCompactionMinimumTimesResponse>> responseFutures = new ArrayList<>(nodes.size() - 1);

        for (ClusterNode node : nodes) {
            if (localNodeName.equals(node.name())) {
                continue;
            }

            CompletableFuture<CatalogCompactionMinimumTimesResponse> fut = messagingService.invoke(node, request, ANSWER_TIMEOUT)
                    .thenApply(CatalogCompactionMinimumTimesResponse.class::cast);

            responseFutures.add(fut);
        }

        return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
                .thenApply(ignore -> {
                    long globalMinimumRequiredTime = localMinimumRequiredTime;
                    long globalMinimumActiveTxTime = activeLocalTxMinimumBeginTimeProvider.minimumBeginTime().longValue();

                    for (CompletableFuture<CatalogCompactionMinimumTimesResponse> fut : responseFutures) {
                        CatalogCompactionMinimumTimesResponse response = fut.join();

                        if (response.minimumRequiredTime() < globalMinimumRequiredTime) {
                            globalMinimumRequiredTime = response.minimumRequiredTime();
                        }

                        if (response.minimumActiveTxTime() < globalMinimumActiveTxTime) {
                            globalMinimumActiveTxTime = response.minimumActiveTxTime();
                        }
                    }

                    return new TimeHolder(globalMinimumRequiredTime, globalMinimumActiveTxTime);
                });
    }

    CompletableFuture<Void> propagateTimeToNodes(long timestamp, Collection<? extends ClusterNode> nodes) {
        CatalogCompactionPrepareUpdateTxBeginTimeMessage request = COMPACTION_MESSAGES_FACTORY
                .catalogCompactionPrepareUpdateTxBeginTimeMessage()
                .timestamp(timestamp)
                .build();

        List<CompletableFuture<?>> sendFutures = new ArrayList<>(nodes.size());

        for (ClusterNode node : nodes) {
            sendFutures.add(messagingService.send(node, request));
        }

        return CompletableFutures.allOf(sendFutures);
    }

    CompletableFuture<Void> propagateTimeToLocalReplicas(long txBeginTime) {
        HybridTimestamp nowTs = clockService.now();

        return schemaSyncService.waitForMetadataCompleteness(nowTs)
                .thenComposeAsync(ignore -> {
                    Int2IntMap tablesWithPartitions =
                            catalogManagerFacade.collectTablesWithPartitionsBetween(txBeginTime, nowTs.longValue());

                    ObjectIterator<Entry> itr = tablesWithPartitions.int2IntEntrySet().iterator();

                    return invokeOnLocalReplicas(txBeginTime, localNodeId, itr);
                }, executor);
    }

    private long determineLocalMinimumRequiredTime() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-22637 Provide actual minimum required time.
        return HybridTimestamp.MIN_VALUE.longValue();
    }

    private CompletableFuture<Boolean> tryCompactCatalog(Catalog catalog, LogicalTopologySnapshot topologySnapshot) {
        return requiredNodes(catalog)
                .thenCompose(requiredNodes -> {
                    List<String> missingNodes = missingNodes(requiredNodes, topologySnapshot.nodes());

                    if (!missingNodes.isEmpty()) {
                        LOG.info("Catalog compaction aborted due to missing cluster members [nodes={}].", missingNodes);

                        return CompletableFutures.falseCompletedFuture();
                    }

                    return catalogManagerFacade.compactCatalog(catalog.version());
                });
    }

    private CompletableFuture<Set<String>> requiredNodes(Catalog catalog) {
        HybridTimestamp nowTs = clockService.now();
        Set<String> required = Collections.newSetFromMap(new ConcurrentHashMap<>());

        return CompletableFutures.allOf(catalog.tables().stream()
                .map(table -> collectRequiredNodes(catalog, table, required, nowTs))
                .collect(Collectors.toList())
        ).thenApply(ignore -> required);
    }

    private CompletableFuture<Void> collectRequiredNodes(
            Catalog catalog,
            CatalogTableDescriptor table,
            Set<String> required,
            HybridTimestamp nowTs
    ) {
        CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

        assert zone != null : table.zoneId();

        int partitions = zone.partitions();

        List<TablePartitionId> replicationGroupIds = new ArrayList<>(partitions);

        for (int p = 0; p < partitions; p++) {
            replicationGroupIds.add(new TablePartitionId(table.id(), p));
        }

        return placementDriver.getAssignments(replicationGroupIds, nowTs)
                .thenAccept(tokenizedAssignments -> {
                    assert tokenizedAssignments.size() == replicationGroupIds.size();

                    for (int p = 0; p < partitions; p++) {
                        TokenizedAssignments assignment = tokenizedAssignments.get(p);
                        if (assignment == null) {
                            throw new IllegalStateException("Cannot get assignments for table "
                                    + "[group=" + replicationGroupIds.get(p) + ']');
                        }

                        assignment.nodes().forEach(a -> required.add(a.consistentId()));
                    }
                });
    }

    private static List<String> missingNodes(Set<String> requiredNodes, Collection<LogicalNode> logicalTopologyNodes) {
        Set<String> logicalNodeIds = logicalTopologyNodes
                .stream()
                .map(ClusterNode::name)
                .collect(Collectors.toSet());

        return requiredNodes.stream().filter(not(logicalNodeIds::contains)).collect(Collectors.toList());
    }

    private CompletableFuture<Void> invokeOnLocalReplicas(long txBeginTime, String localNodeId, ObjectIterator<Entry> tabTtr) {
        if (!tabTtr.hasNext()) {
            return CompletableFutures.nullCompletedFuture();
        }

        Entry tableWithPartitions = tabTtr.next();
        int tableId = tableWithPartitions.getIntKey();
        int partitions = tableWithPartitions.getIntValue();
        List<CompletableFuture<?>> partFutures = new ArrayList<>(partitions);
        HybridTimestamp nowTs = clockService.now();

        for (int p = 0; p < partitions; p++) {
            TablePartitionId tablePartitionId = new TablePartitionId(tableId, p);

            CompletableFuture<?> fut = placementDriver
                    .getPrimaryReplica(tablePartitionId, nowTs)
                    .thenCompose(meta -> {
                        // If primary is not elected yet - we'll update replication groups on next iteration.
                        if (meta == null || meta.getLeaseholderId() == null) {
                            return CompletableFutures.nullCompletedFuture();
                        }

                        // We need to compare leaseHolderId instead of leaseHolder, because after node restart the
                        // leaseHolder may contain the name of the local node even though the local node is not the primary.
                        if (!localNodeId.equals(meta.getLeaseholderId())) {
                            return CompletableFutures.nullCompletedFuture();
                        }

                        TablePartitionIdMessage partIdMessage = ReplicaMessageUtils.toTablePartitionIdMessage(
                                REPLICA_MESSAGES_FACTORY,
                                tablePartitionId
                        );

                        UpdateMinimumActiveTxBeginTimeReplicaRequest msg = REPLICATION_MESSAGES_FACTORY
                                .updateMinimumActiveTxBeginTimeReplicaRequest()
                                .groupId(partIdMessage)
                                .timestamp(txBeginTime)
                                .build();

                        return replicaService.invoke(localNodeName, msg);
                    });

            partFutures.add(fut);
        }

        return CompletableFutures.allOf(partFutures)
                .thenComposeAsync(ignore -> invokeOnLocalReplicas(txBeginTime, localNodeId, tabTtr), executor);
    }

    class CatalogCompactionMessageHandler implements NetworkMessageHandler {
        @Override
        public void onReceived(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
            assert message.groupType() == CatalogCompactionMessageGroup.GROUP_TYPE : message.groupType();

            switch (message.messageType()) {
                case CatalogCompactionMessageGroup.MINIMUM_TIMES_REQUEST:
                    assert correlationId != null;

                    handleMinimumTimesRequest(sender, correlationId);

                    break;

                case CatalogCompactionMessageGroup.PREPARE_TO_UPDATE_TIME_ON_REPLICAS_MESSAGE:
                    handlePrepareToUpdateTimeOnReplicasMessage(message);

                    break;

                default:
                    throw new UnsupportedOperationException("Not supported message type: " + message.messageType());
            }
        }

        private void handleMinimumTimesRequest(ClusterNode sender, Long correlationId) {
            CatalogCompactionMinimumTimesResponse response = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesResponse()
                    .minimumRequiredTime(localMinTimeProvider.time())
                    .minimumActiveTxTime(activeLocalTxMinimumBeginTimeProvider.minimumBeginTime().longValue())
                    .build();

            messagingService.respond(sender, response, correlationId);
        }

        private void handlePrepareToUpdateTimeOnReplicasMessage(NetworkMessage message) {
            long txBeginTime = ((CatalogCompactionPrepareUpdateTxBeginTimeMessage) message).timestamp();

            propagateTimeToLocalReplicas(txBeginTime)
                    .exceptionally(ex -> {
                        LOG.warn("Failed to propagate minimum required time to replicas.", ex);

                        return null;
                    });
        }
    }

    /** Minimum required time provider. */
    @FunctionalInterface
    interface MinimumRequiredTimeProvider {
        /** Returns minimum required timestamp. */
        long time();
    }

    static class TimeHolder {
        final long minRequiredTime;
        final long minActiveTxBeginTime;

        private TimeHolder(long minRequiredTime, long minActiveTxBeginTime) {
            this.minRequiredTime = minRequiredTime;
            this.minActiveTxBeginTime = minActiveTxBeginTime;
        }
    }
}
