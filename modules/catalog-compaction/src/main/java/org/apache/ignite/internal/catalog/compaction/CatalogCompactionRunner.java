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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap.Entry;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMaps;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.message.AvailablePartitionsMessage;
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
import org.apache.ignite.internal.components.NodeProperties;
import org.apache.ignite.internal.distributionzones.rebalance.RebalanceMinimumRequiredTimeProvider;
import org.apache.ignite.internal.hlc.ClockService;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.NetworkMessageHandler;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxBeginTimeReplicaRequest;
import org.apache.ignite.internal.partitiondistribution.TokenizedAssignments;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.ReplicationGroupIdMessage;
import org.apache.ignite.internal.schema.SchemaSyncService;
import org.apache.ignite.internal.table.distributed.raft.MinimumRequiredTimeCollectorService;
import org.apache.ignite.internal.thread.IgniteThreadFactory;
import org.apache.ignite.internal.tx.ActiveLocalTxMinimumRequiredTimeProvider;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.internal.util.ExceptionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.Pair;
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

    private final ExecutorService executor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final AtomicBoolean stopGuard = new AtomicBoolean();

    private final MinimumRequiredTimeCollectorService localMinTimeCollectorService;

    private final String localNodeName;

    private final ActiveLocalTxMinimumRequiredTimeProvider activeLocalTxMinimumRequiredTimeProvider;

    private final ReplicaService replicaService;

    private final SchemaSyncService schemaSyncService;

    private final TopologyService topologyService;

    private final NodeProperties nodeProperties;

    private final RebalanceMinimumRequiredTimeProvider rebalanceMinimumRequiredTimeProvider;

    private CompletableFuture<Void> lastRunFuture = CompletableFutures.nullCompletedFuture();

    /**
     * Node that is considered to be a coordinator of compaction process.
     *
     * <p>May be not set. Node should act as coordinator only in case this field is set and value is equal to name of the local node.
     */
    private volatile @Nullable String compactionCoordinatorNodeName;

    private volatile HybridTimestamp lowWatermark;

    private volatile UUID localNodeId;

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
            NodeProperties nodeProperties,
            ActiveLocalTxMinimumRequiredTimeProvider activeLocalTxMinimumRequiredTimeProvider,
            MinimumRequiredTimeCollectorService minimumRequiredTimeCollectorService,
            RebalanceMinimumRequiredTimeProvider rebalanceMinimumRequiredTimeProvider
    ) {
        this.localNodeName = localNodeName;
        this.messagingService = messagingService;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManagerFacade = new CatalogManagerCompactionFacade(catalogManager);
        this.clockService = clockService;
        this.schemaSyncService = schemaSyncService;
        this.topologyService = topologyService;
        this.nodeProperties = nodeProperties;
        this.placementDriver = placementDriver;
        this.replicaService = replicaService;
        this.activeLocalTxMinimumRequiredTimeProvider = activeLocalTxMinimumRequiredTimeProvider;
        this.localMinTimeCollectorService = minimumRequiredTimeCollectorService;
        this.rebalanceMinimumRequiredTimeProvider = rebalanceMinimumRequiredTimeProvider;
        this.executor = createExecutor(localNodeName);
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(CatalogCompactionMessageGroup.class, new CatalogCompactionMessageHandler());

        localNodeId = topologyService.localMember().id();

        return CompletableFutures.nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return CompletableFutures.nullCompletedFuture();
        }

        busyLock.block();

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        return CompletableFutures.nullCompletedFuture();
    }

    /** Updates the local view of the node with new compaction coordinator. */
    public void updateCoordinator(InternalClusterNode newCoordinator) {
        compactionCoordinatorNodeName = newCoordinator.name();

        triggerCompaction(lowWatermark);
    }

    /** Returns local view of the node on who is currently compaction coordinator. For test purposes only. */
    @TestOnly
    public @Nullable String coordinator() {
        return compactionCoordinatorNodeName;
    }

    @TestOnly
    public ActiveLocalTxMinimumRequiredTimeProvider activeLocalTxMinimumRequiredTimeProvider() {
        return activeLocalTxMinimumRequiredTimeProvider;
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

                lastRunFuture = startCompaction(lwm, logicalTopologyService.localLogicalTopology());
            }
        });
    }

    private LocalMinTime getMinLocalTime(HybridTimestamp lwm) {
        Map<TablePartitionId, Long> partitionStates = localMinTimeCollectorService.minTimestampPerPartition();

        // Find the minimum time among all partitions.
        long partitionMinTime = Long.MAX_VALUE;

        for (Map.Entry<TablePartitionId, Long> e : partitionStates.entrySet()) {
            Long state = e.getValue();

            if (state == MinimumRequiredTimeCollectorService.UNDEFINED_MIN_TIME) {
                LOG.debug("Partition state is missing [partition={}].", e.getKey());
                return LocalMinTime.NOT_AVAILABLE;
            }

            partitionMinTime = Math.min(partitionMinTime, state);
        }

        long rebalanceMinTime = rebalanceMinimumRequiredTimeProvider.minimumRequiredTime();

        // Choose the minimum time between the low watermark, minimum
        // required rebalance time and the minimum time among all partitions.
        long chosenMinTime = Math.min(Math.min(lwm.longValue(), partitionMinTime), rebalanceMinTime);

        LOG.debug("Minimum required time was chosen [partitionMinTime={}, rebalanceMinTime={}, lowWatermark={}, chosen={}].",
                partitionMinTime,
                rebalanceMinTime,
                lwm,
                chosenMinTime
        );

        Int2ObjectMap<BitSet> tableBitSet = buildTablePartitions(partitionStates);

        return new LocalMinTime(chosenMinTime, tableBitSet);
    }

    private CompletableFuture<Void> startCompaction(HybridTimestamp lwm, LogicalTopologySnapshot topologySnapshot) {
        return CompletableFuture.supplyAsync(() -> {
            LOG.info("Catalog compaction started [lowWaterMark={}].", lwm);

            return getMinLocalTime(lwm);
        }, executor).thenCompose(localMinRequiredTime -> {
            long localMinTime = localMinRequiredTime.time;
            Int2ObjectMap<BitSet> localPartitions = localMinRequiredTime.availablePartitions;

            return determineGlobalMinimumRequiredTime(topologySnapshot.nodes(), localMinTime, localPartitions)
                    .thenCompose(timeHolder -> {

                        long minRequiredTime = timeHolder.minRequiredTime;
                        long txMinRequiredTime = timeHolder.txMinRequiredTime;
                        Map<String, Int2ObjectMap<BitSet>> allPartitions = timeHolder.allPartitions;

                        CompletableFuture<Boolean> catalogCompactionFut = tryCompactCatalog(
                                minRequiredTime,
                                topologySnapshot,
                                lwm,
                                allPartitions
                        );

                        LOG.debug("Propagate minimum required tx time to replicas [timestamp={}].", txMinRequiredTime);

                        CompletableFuture<Void> propagateToReplicasFut =
                                propagateTimeToNodes(txMinRequiredTime, topologySnapshot.nodes())
                                        .exceptionally((ex) -> {
                                            throw new CompletionException("Failed to propagate minimum required tx time to replicas.", ex);
                                        });

                        return CompletableFuture.allOf(
                                catalogCompactionFut,
                                propagateToReplicasFut
                        ).exceptionally(ex -> {
                            if (catalogCompactionFut.isCompletedExceptionally() && propagateToReplicasFut.isCompletedExceptionally()) {
                                ex.addSuppressed(propagateToReplicasFut.handle((r, t) -> t).join());
                            }

                            throw new CompletionException(ex);
                        });
                    });
        }).whenComplete((ignore, ex) -> {
            if (ex != null) {
                if (ExceptionUtils.isOrCausedBy(NodeStoppingException.class, ex)) {
                    LOG.debug("Catalog compaction iteration has failed [lwm={}].", ex, lwm);
                } else {
                    LOG.warn("Catalog compaction iteration has failed [lwm={}].", ex, lwm);
                }
            }
        });
    }

    @TestOnly
    CompletableFuture<TimeHolder> determineGlobalMinimumRequiredTime(
            Collection<? extends InternalClusterNode> nodes,
            long localMinimumRequiredTime) {

        return determineGlobalMinimumRequiredTime(nodes, localMinimumRequiredTime, Int2ObjectMaps.emptyMap());
    }

    private CompletableFuture<TimeHolder> determineGlobalMinimumRequiredTime(
            Collection<? extends InternalClusterNode> nodes,
            long localMinimumRequiredTime,
            Int2ObjectMap<BitSet> localPartitions
    ) {
        CatalogCompactionMinimumTimesRequest request = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesRequest().build();
        List<CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>>> responseFutures = new ArrayList<>(nodes.size() - 1);

        for (InternalClusterNode node : nodes) {
            if (localNodeName.equals(node.name())) {
                continue;
            }

            CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>> fut = messagingService.invoke(
                            node, request, ANSWER_TIMEOUT).thenApply(CatalogCompactionMinimumTimesResponse.class::cast)
                    .thenApply(r -> new Pair<>(node.name(), r));

            responseFutures.add(fut);
        }

        return CompletableFuture.allOf(responseFutures.toArray(new CompletableFuture[0]))
                .thenApplyAsync(ignore -> {
                    long globalMinimumRequiredTime = localMinimumRequiredTime;
                    long globalMinimumTxRequiredTime = activeLocalTxMinimumRequiredTimeProvider.minimumRequiredTime();

                    Map<String, Int2ObjectMap<BitSet>> allPartitions = new HashMap<>();
                    allPartitions.put(localNodeName, localPartitions);

                    for (CompletableFuture<Pair<String, CatalogCompactionMinimumTimesResponse>> fut : responseFutures) {
                        Pair<String, CatalogCompactionMinimumTimesResponse> p = fut.join();

                        String nodeId = p.getFirst();
                        CatalogCompactionMinimumTimesResponse response = p.getSecond();

                        if (response.minimumRequiredTime() < globalMinimumRequiredTime) {
                            globalMinimumRequiredTime = response.minimumRequiredTime();
                        }

                        if (response.activeTxMinimumRequiredTime() < globalMinimumTxRequiredTime) {
                            globalMinimumTxRequiredTime = response.activeTxMinimumRequiredTime();
                        }

                        allPartitions.put(nodeId, availablePartitionListToMap(response.partitions()));
                    }

                    return new TimeHolder(globalMinimumRequiredTime, globalMinimumTxRequiredTime, allPartitions);
                }, executor);
    }

    CompletableFuture<Void> propagateTimeToNodes(long timestamp, Collection<? extends InternalClusterNode> nodes) {
        CatalogCompactionPrepareUpdateTxBeginTimeMessage request = COMPACTION_MESSAGES_FACTORY
                .catalogCompactionPrepareUpdateTxBeginTimeMessage()
                .timestamp(timestamp)
                .build();

        List<CompletableFuture<?>> sendFutures = new ArrayList<>(nodes.size());

        for (InternalClusterNode node : nodes) {
            sendFutures.add(messagingService.send(node, request));
        }

        return CompletableFutures.allOf(sendFutures);
    }

    CompletableFuture<Void> propagateTimeToLocalReplicas(long txBeginTime) {
        HybridTimestamp nowTs = clockService.now();

        return schemaSyncService.waitForMetadataCompleteness(nowTs)
                .thenComposeAsync(ignore -> {
                    Int2IntMap idsWithPartitions = nodeProperties.colocationEnabled()
                            ? catalogManagerFacade.collectZonesWithPartitionsBetween(txBeginTime, nowTs.longValue())
                            : catalogManagerFacade.collectTablesWithPartitionsBetween(txBeginTime, nowTs.longValue());

                    ObjectIterator<Entry> itr = idsWithPartitions.int2IntEntrySet().iterator();

                    return invokeOnLocalReplicas(txBeginTime, localNodeId, itr);
                }, executor);
    }

    private CompletableFuture<Boolean> tryCompactCatalog(
            long minRequiredTime,
            LogicalTopologySnapshot topologySnapshot,
            HybridTimestamp lwm,
            Map<String, Int2ObjectMap<BitSet>> allPartitions
    ) {
        Catalog catalog = catalogManagerFacade.catalogPriorToVersionAtTsNullable(minRequiredTime);

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

        return validatePartitions(catalog, lwm, allPartitions)
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
                    if (ex == null) {
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
            HybridTimestamp lwm,
            Map<String, Int2ObjectMap<BitSet>> allPartitions
    ) {
        HybridTimestamp nowTs = clockService.now();
        ConcurrentHashMap<String, RequiredPartitions> requiredPartitionsPerNode = new ConcurrentHashMap<>();
        // Used as a set.
        ConcurrentHashMap<Integer, Boolean> deletedTables = new ConcurrentHashMap<>();

        Catalog currentCatalog = catalogManagerFacade.catalogAtTsNullable(nowTs.longValue());
        assert currentCatalog != null;

        return CompletableFutures.allOf(catalog.tables().stream()
                .map(table -> collectRequiredNodes(catalog, table, nowTs, requiredPartitionsPerNode, currentCatalog, deletedTables))
                .collect(Collectors.toList())
        ).thenApply(ignore -> {

            Set<String> requiredNodeNames = requiredPartitionsPerNode.keySet();

            for (Map.Entry<String, RequiredPartitions> entry : requiredPartitionsPerNode.entrySet()) {
                RequiredPartitions partitionsPerNode = entry.getValue();
                String nodeId = entry.getKey();
                Int2ObjectMap<BitSet> actualPartitions = allPartitions.get(nodeId);

                if (actualPartitions == null) {
                    return new Pair<>(false, requiredNodeNames);
                }

                Int2ObjectMap<BitSet> requiredPartitions = partitionsPerNode.data();
                if (!actualPartitions.keySet().containsAll(requiredPartitions.keySet())) {
                    return new Pair<>(false, requiredNodeNames);
                }

                for (Int2ObjectMap.Entry<BitSet> tableParts : requiredPartitions.int2ObjectEntrySet()) {
                    BitSet actual = actualPartitions.get(tableParts.getIntKey());
                    BitSet expected = tableParts.getValue();

                    BitSet cmp = (BitSet) actual.clone();
                    cmp.and(expected);

                    if (!cmp.equals(expected)) {
                        return new Pair<>(false, requiredNodeNames);
                    }
                }
            }

            Catalog catalogAtLwm = catalogManagerFacade.catalogAtTsNullable(lwm.longValue());
            assert catalogAtLwm != null;

            for (int tableId : deletedTables.keySet()) {
                if (catalogAtLwm.table(tableId) != null) {
                    // Table existed in a revision at the low watermark, abort.
                    return new Pair<>(false, requiredNodeNames);
                }
            }

            return new Pair<>(true, requiredNodeNames);
        });
    }

    private CompletableFuture<Void> collectRequiredNodes(
            Catalog catalog,
            CatalogTableDescriptor table,
            HybridTimestamp nowTs,
            ConcurrentHashMap<String, RequiredPartitions> requiredPartitionsPerNode,
            Catalog currentCatalog,
            ConcurrentHashMap<Integer, Boolean> deletedTables
    ) {
        CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

        assert zone != null : table.zoneId();

        int partitions = zone.partitions();

        List<ReplicationGroupId> replicationGroupIds = new ArrayList<>(partitions);

        for (int p = 0; p < partitions; p++) {
            replicationGroupIds.add(nodeProperties.colocationEnabled() ? new ZonePartitionId(table.zoneId(), p)
                    : new TablePartitionId(table.id(), p));
        }

        return placementDriver.getAssignments(replicationGroupIds, nowTs)
                .thenAccept(tokenizedAssignments -> {
                    assert tokenizedAssignments.size() == replicationGroupIds.size();

                    if (nodeProperties.colocationEnabled() && currentCatalog.table(table.id()) == null) {
                        // Table no longer exists
                        deletedTables.put(table.id(), true);

                        return;
                    }

                    for (int p = 0; p < partitions; p++) {
                        TokenizedAssignments assignment = tokenizedAssignments.get(p);

                        if (assignment == null) {
                            if (currentCatalog.table(table.id()) == null) {
                                // Table no longer exists
                                deletedTables.put(table.id(), true);
                                continue;
                            } else {
                                throw new IllegalStateException("Cannot get assignments for replication group "
                                        + "[group=" + replicationGroupIds.get(p) + ']');
                            }
                        }

                        int partitionId = p;

                        assignment.nodes().forEach(a -> {
                            String nodeId = a.consistentId();
                            RequiredPartitions partitionsAtNode = requiredPartitionsPerNode.computeIfAbsent(nodeId,
                                    (k) -> new RequiredPartitions()
                            );
                            partitionsAtNode.update(table.id(), partitionId);
                        });
                    }
                });
    }

    private static ExecutorService createExecutor(String localNodeName) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(
                1,
                1,
                10,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(),
                IgniteThreadFactory.create(localNodeName, "catalog-compaction", LOG)
        );

        executor.allowCoreThreadTimeOut(true);

        return executor;
    }

    private static List<String> missingNodes(Set<String> requiredNodes, Collection<LogicalNode> logicalTopologyNodes) {
        Set<String> logicalNodeIds = logicalTopologyNodes
                .stream()
                .map(InternalClusterNode::name)
                .collect(Collectors.toSet());

        return requiredNodes.stream().filter(not(logicalNodeIds::contains)).collect(Collectors.toList());
    }

    private CompletableFuture<Void> invokeOnLocalReplicas(long txBeginTime, UUID localNodeId, ObjectIterator<Entry> entryIterator) {
        if (!entryIterator.hasNext()) {
            return CompletableFutures.nullCompletedFuture();
        }

        Entry idWithPartitions = entryIterator.next();
        int id = idWithPartitions.getIntKey();
        int partitions = idWithPartitions.getIntValue();
        List<CompletableFuture<?>> partFutures = new ArrayList<>(partitions);
        HybridTimestamp nowTs = clockService.now();

        for (int p = 0; p < partitions; p++) {
            ReplicationGroupId groupReplicationId = nodeProperties.colocationEnabled()
                    ? new ZonePartitionId(id, p) : new TablePartitionId(id, p);

            CompletableFuture<?> fut = placementDriver
                    .getPrimaryReplica(groupReplicationId, nowTs)
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

                        ReplicationGroupIdMessage groupIdMessage = nodeProperties.colocationEnabled()
                                ? toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, (ZonePartitionId) groupReplicationId)
                                : toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, (TablePartitionId) groupReplicationId);

                        UpdateMinimumActiveTxBeginTimeReplicaRequest msg = REPLICATION_MESSAGES_FACTORY
                                .updateMinimumActiveTxBeginTimeReplicaRequest()
                                .groupId(groupIdMessage)
                                .timestamp(txBeginTime)
                                .build();

                        return replicaService.invoke(localNodeName, msg);
                    });

            partFutures.add(fut);
        }

        return CompletableFutures.allOf(partFutures)
                .thenComposeAsync(ignore -> invokeOnLocalReplicas(txBeginTime, localNodeId, entryIterator), executor);
    }

    private class CatalogCompactionMessageHandler implements NetworkMessageHandler {
        @Override
        public void onReceived(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
            assert message.groupType() == CatalogCompactionMessageGroup.GROUP_TYPE : message.groupType();

            switch (message.messageType()) {
                case CatalogCompactionMessageGroup.MINIMUM_TIMES_REQUEST:
                    assert correlationId != null;

                    executor.execute(() -> handleMinimumTimesRequest(sender, correlationId));

                    break;

                case CatalogCompactionMessageGroup.PREPARE_TO_UPDATE_TIME_ON_REPLICAS_MESSAGE:
                    executor.execute(() -> handlePrepareToUpdateTimeOnReplicasMessage(message));

                    break;

                default:
                    throw new UnsupportedOperationException("Not supported message type: " + message.messageType());
            }
        }

        private void handleMinimumTimesRequest(InternalClusterNode sender, Long correlationId) {
            HybridTimestamp lwm = lowWatermark;
            LocalMinTime minLocalTime;

            if (lwm != null) {
                minLocalTime = getMinLocalTime(lwm);
            } else {
                // We do not have local min time yet. Reply with the absolute min time.
                minLocalTime = LocalMinTime.NOT_AVAILABLE;
            }

            long minRequiredTime = minLocalTime.time;
            Int2ObjectMap<BitSet> availablePartitions = minLocalTime.availablePartitions;

            CatalogCompactionMinimumTimesResponse response = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesResponse()
                    .minimumRequiredTime(minRequiredTime)
                    .activeTxMinimumRequiredTime(activeLocalTxMinimumRequiredTimeProvider.minimumRequiredTime())
                    .partitions(availablePartitionsMessages(availablePartitions))
                    .build();

            messagingService.respond(sender, response, correlationId);
        }

        private void handlePrepareToUpdateTimeOnReplicasMessage(NetworkMessage message) {
            long txBeginTime = ((CatalogCompactionPrepareUpdateTxBeginTimeMessage) message).timestamp();

            propagateTimeToLocalReplicas(txBeginTime)
                    .exceptionally(ex -> {
                        if (!hasCause(ex, NodeStoppingException.class)) {
                            LOG.warn("Failed to propagate minimum required time to replicas.", ex);
                        }

                        return null;
                    });
        }
    }

    private static Int2ObjectMap<BitSet> buildTablePartitions(Map<TablePartitionId, @Nullable Long> tablePartitionMap) {
        Int2ObjectMap<BitSet> tableIdBitSet = new Int2ObjectOpenHashMap<>();

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
        private static final LocalMinTime NOT_AVAILABLE = new LocalMinTime(HybridTimestamp.MIN_VALUE.longValue(),
                Int2ObjectMaps.emptyMap());

        final long time;
        // TableId to partition number(s).
        final Int2ObjectMap<BitSet> availablePartitions;

        LocalMinTime(long time, Int2ObjectMap<BitSet> availablePartitions) {
            this.time = time;
            this.availablePartitions = availablePartitions;
        }
    }

    static class TimeHolder {
        final long minRequiredTime;
        final long txMinRequiredTime;
        final Map<String, Int2ObjectMap<BitSet>> allPartitions;

        private TimeHolder(long minRequiredTime, long txMinRequiredTime, Map<String, Int2ObjectMap<BitSet>> allPartitions) {
            this.minRequiredTime = minRequiredTime;
            this.txMinRequiredTime = txMinRequiredTime;
            this.allPartitions = allPartitions;
        }
    }

    private static class RequiredPartitions {
        final Int2ObjectMap<BitSet> partitions = new Int2ObjectOpenHashMap<>();

        synchronized void update(int tableId, int p) {
            partitions.compute(tableId, (k, v) -> {
                if (v == null) {
                    v = new BitSet();
                }
                v.set(p);
                return v;
            });
        }

        synchronized Int2ObjectMap<BitSet> data() {
            return partitions;
        }
    }

    private static List<AvailablePartitionsMessage> availablePartitionsMessages(Int2ObjectMap<BitSet> availablePartitions) {
        return availablePartitions.int2ObjectEntrySet().stream()
                .map(e -> COMPACTION_MESSAGES_FACTORY.availablePartitionsMessage()
                        .tableId(e.getIntKey())
                        .partitions(e.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    private static Int2ObjectMap<BitSet> availablePartitionListToMap(List<AvailablePartitionsMessage> availablePartitions) {
        return availablePartitions.stream()
                .collect(CollectionUtils.toIntMapCollector(AvailablePartitionsMessage::tableId, AvailablePartitionsMessage::partitions));
    }
}
