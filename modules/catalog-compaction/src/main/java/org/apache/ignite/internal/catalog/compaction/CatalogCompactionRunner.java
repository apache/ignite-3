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
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogIndexStatus;
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
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.tx.ActiveLocalTxMinimumBeginTimeProvider;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.Pair;
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

    private volatile boolean enabled;

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
            ActiveLocalTxMinimumBeginTimeProvider activeLocalTxMinimumBeginTimeProvider,
            MinimumRequiredTimeProvider localMinTimeProvider
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
        this.localMinTimeProvider = localMinTimeProvider;
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

    /** Returns {@link MinimumRequiredTimeProvider}. For test purposes only. */
    @TestOnly
    public MinimumRequiredTimeProvider localMinTimeProvider() {
        return this.localMinTimeProvider;
    }

    /** Enables or disables the compaction process. */
    @TestOnly
    public void enable(boolean value) {
        this.enabled = value;
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
        if (lwm == null || !localNodeName.equals(compactionCoordinatorNodeName) || !enabled) {
            return;
        }

        inBusyLock(busyLock, () -> {
            synchronized (this) {
                CompletableFuture<Void> fut = lastRunFuture;

                if (!fut.isDone()) {
                    LOG.info("Catalog compaction is already in progress, skipping [timestamp={}].", lwm.longValue());

                    return;
                }

                lastRunFuture = startCompaction(lwm, logicalTopologyService.localLogicalTopology());
            }
        });
    }

    private @Nullable LocalMinTime getMinLocalTime(HybridTimestamp lwm) {
        Map<TablePartitionId, @Nullable Long> partitionStates = localMinTimeProvider.minTimePerPartition();

        // Find the minimum time among all partitions.
        long partitionMinTime = Long.MAX_VALUE;

        for (Map.Entry<TablePartitionId, Long> e : partitionStates.entrySet()) {
            Long state = e.getValue();

            if (state == null) {
                LOG.debug("Partition state is missing [partition={}].", e.getKey());
                return null;
            }

            partitionMinTime = Math.min(partitionMinTime, state);
        }

        // Choose the minimum time between the low watermark and the minimum time among all partitions.
        long chosenMinTime = Math.min(lwm.longValue(), partitionMinTime);

        LOG.debug("Minimum required time was chosen [partitionMinTime={}, lowWatermark={}, chosen={}].",
                partitionMinTime,
                lwm,
                chosenMinTime
        );

        Map<Integer, BitSet> tableBitSet = buildTablePartitions(partitionStates);

        return new LocalMinTime(chosenMinTime, tableBitSet);
    }

    private CompletableFuture<Void> startCompaction(HybridTimestamp lwm, LogicalTopologySnapshot topologySnapshot) {
        LOG.info("Catalog compaction started [lowWaterMark={}].", lwm);

        LocalMinTime localMinRequiredTime = getMinLocalTime(lwm);
        Long minTime = localMinRequiredTime != null ? localMinRequiredTime.time : null;

        return determineGlobalMinimumRequiredTime(topologySnapshot.nodes(), minTime)
                .thenComposeAsync(timeHolder -> {

                    long minRequiredTime = timeHolder.minRequiredTime;
                    long minActiveTxBeginTime = timeHolder.minActiveTxBeginTime;

                    CompletableFuture<Boolean> catalogCompactionFut = tryCompactCatalog(minRequiredTime,
                            topologySnapshot,
                            timeHolder.remotePartitions
                    );

                    LOG.debug("Propagate minimum active tx begin time to replicas [timestamp={}].", minActiveTxBeginTime);

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
            @Nullable Long localMinimumRequiredTime
    ) {
        CatalogCompactionMinimumTimesRequest request = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesRequest().build();
        List<CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>>> responseFutures = new ArrayList<>(nodes.size() - 1);

        for (ClusterNode node : nodes) {
            if (localNodeName.equals(node.name())) {
                continue;
            }

            CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>> fut = messagingService.invoke(
                            node, request, ANSWER_TIMEOUT).thenApply(CatalogCompactionMinimumTimesResponse.class::cast)
                    .thenApply(r -> new Pair<>(node.name(), r));

            responseFutures.add(fut);
        }

        return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
                .thenApply(ignore -> {
                    Long globalMinimumRequiredTime = localMinimumRequiredTime;
                    long globalMinimumActiveTxTime = activeLocalTxMinimumBeginTimeProvider.minimumBeginTime().longValue();
                    Map<String, Map<Integer, BitSet>> remotePartitions = new HashMap<>();

                    for (CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>> fut : responseFutures) {
                        Pair<String, CatalogCompactionMinimumTimesResponse> p = fut.join();

                        String nodeId = p.getFirst();
                        CatalogCompactionMinimumTimesResponse response = p.getSecond();

                        if (globalMinimumRequiredTime == null || response.minimumRequiredTime() < globalMinimumRequiredTime) {
                            globalMinimumRequiredTime = response.minimumRequiredTime();
                        }

                        if (response.minimumActiveTxTime() < globalMinimumActiveTxTime) {
                            globalMinimumActiveTxTime = response.minimumActiveTxTime();
                        }

                        remotePartitions.put(nodeId, response.partitions());
                    }

                    if (globalMinimumRequiredTime == null) {
                        globalMinimumRequiredTime = HybridTimestamp.MIN_VALUE.longValue();
                    }

                    return new TimeHolder(globalMinimumRequiredTime, globalMinimumActiveTxTime, remotePartitions);
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


    private CompletableFuture<Boolean> tryCompactCatalog(
            long minRequiredTime,
            LogicalTopologySnapshot topologySnapshot,
            Map<String, Map<Integer, BitSet>> remotePartitions
    ) {
        Catalog catalog = catalogManagerFacade.catalogByTsNullable(minRequiredTime);

        if (catalog == null) {
            LOG.info("Catalog compaction skipped, nothing to compact [timestamp={}].", minRequiredTime);

            return CompletableFutures.falseCompletedFuture();
        }

        for (CatalogIndexDescriptor index : catalog.indexes()) {
            if (index.status() == CatalogIndexStatus.BUILDING || index.status() == CatalogIndexStatus.REGISTERED) {
                LOG.info("Catalog compaction aborted, index construction is taking place.");

                return CompletableFutures.falseCompletedFuture();
            }
        }

        return validatePartitions(catalog, remotePartitions)
                .thenCompose(result -> {
                    if (!result.getFirst()) {
                        LOG.info("Catalog compaction aborted due to mismatching table partitions.");

                        return CompletableFutures.falseCompletedFuture();
                    }

                    Set<String> requiredNodes = result.getSecond();
                    List<String> missingNodes = missingNodes(requiredNodes, topologySnapshot.nodes());

                    if (!missingNodes.isEmpty()) {
                        LOG.info("Catalog compaction aborted due to missing cluster members [nodes={}].", missingNodes);

                        return CompletableFutures.falseCompletedFuture();
                    }

                    return catalogManagerFacade.compactCatalog(catalog.version());
                }).whenComplete((res, ex) -> {
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

    private CompletableFuture<Pair<Boolean, Set<String>>> validatePartitions(
            Catalog catalog,
            Map<String, Map<Integer, BitSet>> remotePartitions
    ) {
        HybridTimestamp nowTs = clockService.now();
        Set<String> required = Collections.newSetFromMap(new ConcurrentHashMap<>());
        Map<String, Map<Integer, BitSet>> actualPartitions = new ConcurrentHashMap<>();

        return CompletableFutures.allOf(catalog.tables().stream()
                .map(table -> collectRequiredNodes(catalog, table, required, nowTs, actualPartitions))
                .collect(Collectors.toList())
        ).thenApply(ignore -> {

            // Compare partitions received from nodes (remotePartitions) with partitions retrieve
            // from the placement driver.

            for (Map.Entry<String, Map<Integer, BitSet>> e : remotePartitions.entrySet()) {
                String remoteId = e.getKey();
                Map<Integer, BitSet> remoteParts = e.getValue();
                Map<Integer, BitSet> actualParts = actualPartitions.get(remoteId);

                if (!remoteParts.equals(actualParts)) {
                    return new Pair<>(false, required);
                }
            }

            return new Pair<>(true, required);
        });
    }

    private CompletableFuture<Void> collectRequiredNodes(
            Catalog catalog,
            CatalogTableDescriptor table,
            Set<String> required,
            HybridTimestamp nowTs,
            Map<String, Map<Integer, BitSet>> actualPartitions
    ) {
        CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

        assert zone != null : table.zoneId();

        int partitions = zone.partitions();

        List<TablePartitionId> replicationGroupIds = new ArrayList<>(partitions);
        BitSet bitSet = new BitSet();

        for (int p = 0; p < partitions; p++) {
            replicationGroupIds.add(new TablePartitionId(table.id(), p));
            bitSet.set(p);
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

                        assignment.nodes().forEach(a -> {
                            String nodeId = a.consistentId();
                            required.add(nodeId);

                            Map<Integer, BitSet> nodeTables = actualPartitions.computeIfAbsent(nodeId, (k) -> new ConcurrentHashMap<>());
                            nodeTables.put(table.id(), bitSet);
                        });
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

    private class CatalogCompactionMessageHandler implements NetworkMessageHandler {
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
            HybridTimestamp lwm = lowWatermark;
            Long minRequiredTime;
            Map<Integer, BitSet> availablePartitions;
            if (lwm != null) {
                LocalMinTime minLocalTime = getMinLocalTime(lwm);
                if (minLocalTime != null) {
                    minRequiredTime = minLocalTime.time;
                    availablePartitions = minLocalTime.availablePartitions;
                } else {
                    minRequiredTime = null;
                    availablePartitions = Collections.emptyMap();
                }
            } else {
                minRequiredTime = null;
                availablePartitions = Collections.emptyMap();
            }

            // We do not have local min time yet. Reply with the absolute min time.
            if (minRequiredTime == null) {
                minRequiredTime = HybridTimestamp.MIN_VALUE.longValue();
            }

            CatalogCompactionMinimumTimesResponse response = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesResponse()
                    .minimumRequiredTime(minRequiredTime)
                    .minimumActiveTxTime(activeLocalTxMinimumBeginTimeProvider.minimumBeginTime().longValue())
                    .partitions(availablePartitions)
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

    private static Map<Integer, BitSet> buildTablePartitions(Map<TablePartitionId, @Nullable Long> tablePartitionMap) {
        Map<Integer, BitSet> tableIdBitSet = new HashMap<>();

        for (var e : tablePartitionMap.entrySet()) {
            TablePartitionId tp = e.getKey();
            Long time = e.getValue();

            tableIdBitSet.compute(tp.tableId(), (k, v) -> {
                int partition = tp.partitionId();
                if (v == null) {
                    v = new BitSet();
                }
                if (time != null) {
                    v.set(partition);
                }
                return v;
            });
        }
        return tableIdBitSet;
    }

    private static class LocalMinTime {
        final long time;
        final Map<Integer, BitSet> availablePartitions;

        LocalMinTime(long time, Map<Integer, BitSet> availablePartitions) {
            this.time = time;
            this.availablePartitions = availablePartitions;
        }
    }

    static class TimeHolder {
        final long minRequiredTime;
        final long minActiveTxBeginTime;
        final Map<String, Map<Integer, BitSet>> remotePartitions;

        private TimeHolder(long minRequiredTime, long minActiveTxBeginTime, Map<String, Map<Integer, BitSet>> remotePartitions) {
            this.minRequiredTime = minRequiredTime;
            this.minActiveTxBeginTime = minActiveTxBeginTime;
            this.remotePartitions = remotePartitions;
        }
    }
}
