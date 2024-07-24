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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessageGroup;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessagesFactory;
import org.apache.ignite.internal.catalog.compaction.message.CatalogMinimumRequiredTimeRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogMinimumRequiredTimeResponse;
import org.apache.ignite.internal.catalog.compaction.message.CatalogMinimumTxTimeRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogMinimumTxTimeResponse;
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
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimalPendingTxStartTimeReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.tx.LocalPendingTxMinimalBeginTimeProvider;
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
 *     by sending {@link CatalogMinimumRequiredTimeRequest} to all cluster members.</li>
 *     <li>If it is considered safe to trim the history up to calculated catalog version
 *     (at least, all partition owners are present in the logical topology), then the catalog is compacted.</li>
 * </ol>
 */
public class CatalogCompactionRunner implements IgniteComponent {
    private static final IgniteLogger LOG = Loggers.forClass(CatalogCompactionRunner.class);

    private static final CatalogCompactionMessagesFactory MESSAGES_FACTORY = new CatalogCompactionMessagesFactory();

    private static final PartitionReplicationMessagesFactory PART_REPL_MESSAGE_FACTORY = new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    private static final long ANSWER_TIMEOUT = 5_000;

    private final CatalogManagerImpl catalogManager;

    private final MessagingService messagingService;

    private final LogicalTopologyService logicalTopologyService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final MinimumRequiredTimeProvider localMinTimeProvider;

    private final String localNodeName;

    private final LocalPendingTxMinimalBeginTimeProvider pendingRwTxMinBeginTimeProvider;

    private final ReplicaService replicaService;

    /**
     * Node that is considered to be a coordinator of compaction process.
     *
     * <p>May be not set. Node should act as coordinator only in case this field is set and value is equal to name of the local node.
     */
    private volatile @Nullable String compactionCoordinatorNodeName;

    private volatile CompletableFuture<Boolean> lastRunFuture = CompletableFutures.nullCompletedFuture();

    private volatile HybridTimestamp lowWatermark;

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
            Executor executor,
            LocalPendingTxMinimalBeginTimeProvider pendingRwTxMinBeginTimeProvider
    ) {
        this(localNodeName, catalogManager, messagingService, logicalTopologyService, placementDriver, replicaService, clockService,
                executor, pendingRwTxMinBeginTimeProvider, null);
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
            Executor executor,
            LocalPendingTxMinimalBeginTimeProvider pendingRwTxMinBeginTimeProvider,
            @Nullable CatalogCompactionRunner.MinimumRequiredTimeProvider localMinTimeProvider
    ) {
        this.localNodeName = localNodeName;
        this.messagingService = messagingService;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManager = catalogManager;
        this.clockService = clockService;
        this.placementDriver = placementDriver;
        this.replicaService = replicaService;
        this.executor = executor;
        this.pendingRwTxMinBeginTimeProvider = pendingRwTxMinBeginTimeProvider;
        this.localMinTimeProvider = localMinTimeProvider == null ? this::determineLocalMinimumRequiredTime : localMinTimeProvider;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(CatalogCompactionMessageGroup.class, (message, sender, correlationId) -> {
            assert message.groupType() == CatalogCompactionMessageGroup.GROUP_TYPE : message.groupType();
            assert correlationId != null;

            if (message.messageType() == CatalogCompactionMessageGroup.MINIMUM_REQUIRED_TIME_REQUEST) {
                CatalogMinimumRequiredTimeResponse response = MESSAGES_FACTORY.catalogMinimumRequiredTimeResponse()
                        .timestamp(localMinTimeProvider.time())
                        .minimumActiveTxTime(pendingRwTxMinBeginTimeProvider.minimalStartTime())
                        .build();

                messagingService.respond(sender, response, correlationId);

                return;
            }

            if (message.messageType() == CatalogCompactionMessageGroup.MINIMUM_TX_BEGIN_TIME_REQUEST) {
                CatalogMinimumTxTimeResponse response = MESSAGES_FACTORY.catalogMinimumTxTimeResponse()
                        .timestamp(pendingRwTxMinBeginTimeProvider.minimalStartTime())
                        .build();

                messagingService.respond(sender, response, correlationId);

                return;
            }

            throw new UnsupportedOperationException("Not supported message type: " + message.messageType());
        });

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
    CompletableFuture<Boolean> lastRunFuture() {
        return lastRunFuture;
    }

    /** Starts the catalog compaction routine. */
    CompletableFuture<Boolean> triggerCompaction(@Nullable HybridTimestamp lwm) {
        if (lwm == null || !localNodeName.equals(compactionCoordinatorNodeName)) {
            return CompletableFutures.falseCompletedFuture();
        }

        return inBusyLock(busyLock, () -> {
            CompletableFuture<Boolean> fut = lastRunFuture;

            if (!fut.isDone()) {
                LOG.info("Catalog compaction is already in progress, skipping (timestamp={})", lwm.longValue());

                return CompletableFutures.falseCompletedFuture();
            }

            fut = startCompaction(logicalTopologyService.localLogicalTopology())
                    .whenComplete((res, ex) -> {
                        if (ex != null) {
                            LOG.warn("Catalog compaction has failed (timestamp={})", ex, lwm.longValue());
                        } else if (LOG.isDebugEnabled()) {
                            if (res) {
                                LOG.debug("Catalog compaction completed successfully (timestamp={})", lwm.longValue());
                            } else {
                                LOG.debug("Catalog compaction skipped (timestamp={})", lwm.longValue());
                            }
                        }
                    });

            lastRunFuture = fut;

            return fut;
        });
    }

    private CompletableFuture<Boolean> startCompaction(LogicalTopologySnapshot topologySnapshot) {
        long localMinimum = localMinTimeProvider.time();

        if (catalogByTsNullable(localMinimum) == null) {
            return CompletableFutures.falseCompletedFuture();
        }

        return determineGlobalMinimumRequiredTime(topologySnapshot.nodes(), localMinimum)
                .thenComposeAsync(ts -> {
                    Catalog catalog = catalogByTsNullable(ts);

                    if (catalog == null) {
                        return CompletableFutures.falseCompletedFuture();
                    }

                    return requiredNodes(catalog)
                            .thenCompose(requiredNodes -> {
                                List<String> missingNodes = missingNodes(requiredNodes, topologySnapshot.nodes());

                                if (!missingNodes.isEmpty()) {
                                    if (LOG.isDebugEnabled()) {
                                        LOG.debug("Catalog compaction aborted due to missing cluster members (nodes={})", missingNodes);
                                    }

                                    return CompletableFutures.falseCompletedFuture();
                                }

                                return catalogManager.compactCatalog(catalog.time());
                            });
                }, executor);
    }

    private @Nullable Catalog catalogByTsNullable(long ts) {
        try {
            int catalogVer = catalogManager.activeCatalogVersion(ts);
            return catalogManager.catalog(catalogVer - 1);
        } catch (IllegalStateException e) {
            return null;
        }
    }

    CompletableFuture<HybridTimestamp> determineGlobalMinimumTxStartTime(Collection<? extends ClusterNode> logicalTopNodes) {
        CatalogMinimumTxTimeRequest request = MESSAGES_FACTORY.catalogMinimumTxTimeRequest().build();
        HybridTimestamp localMinTxBeginTime = pendingRwTxMinBeginTimeProvider.minimalStartTime();

        return collectRemoteValues(request, logicalTopNodes, localMinTxBeginTime, (msg) -> ((CatalogMinimumTxTimeResponse) msg).timestamp())
                .thenApply(values -> values.stream().min((o1, o2) -> {
                    if (o1 == o2) {
                        return 0;
                    } else if (o1 == null) {
                        return 1;
                    } else if (o2 == null) {
                        return -1;
                    } else {
                        return o1.compareTo(o2);
                    }
                }).orElse(null));
    }

    private CompletableFuture<Long> determineGlobalMinimumRequiredTime(Collection<? extends ClusterNode> logicalTopNodes, long localMinTs) {
        CatalogMinimumRequiredTimeRequest request = MESSAGES_FACTORY.catalogMinimumRequiredTimeRequest().build();

        return collectRemoteValues(request, logicalTopNodes, localMinTs, (msg) -> ((CatalogMinimumRequiredTimeResponse) msg).timestamp())
                .thenApply(values -> values.stream().min(Long::compare).orElse(null));
    }

    private <T> CompletableFuture<List<T>> collectRemoteValues(
            NetworkMessage msg,
            Collection<? extends ClusterNode> nodes,
            @Nullable T localValue,
            Function<NetworkMessage, T> msgValueExtractor
    ) {
        List<CompletableFuture<T>> ackFutures = new ArrayList<>();

        for (ClusterNode node : nodes) {
            CompletableFuture<T> fut;

            if (localNodeName.equals(node.name())) {
                fut = CompletableFuture.completedFuture(localValue);
            } else {
                fut = messagingService.invoke(node, msg, ANSWER_TIMEOUT)
                        .thenApply(msgValueExtractor);
            }

            ackFutures.add(fut);
        }

        return CompletableFuture.allOf(ackFutures.toArray(new CompletableFuture[0]))
                .thenApply(ignore -> {
                    List<T> values = ackFutures.stream().map(CompletableFuture::join).collect(Collectors.toList());

                    values.add(localValue);

                    return values;
                });
    }

    private long determineLocalMinimumRequiredTime() {
        // TODO https://issues.apache.org/jira/browse/IGNITE-22637 Provide actual minimum required time.
        return HybridTimestamp.MIN_VALUE.longValue();
    }

    private CompletableFuture<Set<String>> requiredNodes(Catalog catalog) {
        HybridTimestamp nowTs = clockService.now();
        Set<String> required = Collections.newSetFromMap(new ConcurrentHashMap<>());

        // TODO https://issues.apache.org/jira/browse/IGNITE-22722 This method needs to be simplified.
        return collectRequiredNodes(catalog, new ArrayList<>(catalog.tables()).iterator(), required, nowTs);
    }

    private CompletableFuture<Set<String>> collectRequiredNodes(
            Catalog catalog,
            Iterator<CatalogTableDescriptor> tabItr,
            Set<String> required,
            HybridTimestamp nowTs
    ) {
        if (!tabItr.hasNext()) {
            return CompletableFuture.completedFuture(required);
        }

        CatalogTableDescriptor table = tabItr.next();
        CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

        assert zone != null : table.zoneId();

        List<CompletableFuture<?>> partitionFutures = new ArrayList<>(zone.partitions());

        for (int p = 0; p < zone.partitions(); p++) {
            ReplicationGroupId replicationGroupId = new TablePartitionId(table.id(), p);

            CompletableFuture<TokenizedAssignments> assignmentsFut = placementDriver.getAssignments(replicationGroupId, nowTs)
                    .whenComplete((tokenizedAssignments, ex) -> {
                        if (ex != null) {
                            return;
                        }

                        if (tokenizedAssignments == null) {
                            throw new IllegalStateException("Cannot get assignments for table " + table.name()
                                    + " (replication group=" + replicationGroupId + ").");
                        }

                        List<String> assignments = tokenizedAssignments.nodes().stream()
                                .map(Assignment::consistentId)
                                .collect(Collectors.toList());

                        required.addAll(assignments);
                    });

            partitionFutures.add(assignmentsFut);
        }

        return CompletableFutures.allOf(partitionFutures)
                .thenCompose(ignore -> collectRequiredNodes(catalog, tabItr, required, nowTs));
    }

    private static List<String> missingNodes(Set<String> requiredNodes, Collection<LogicalNode> logicalTopologyNodes) {
        Set<String> logicalNodeIds = logicalTopologyNodes
                .stream()
                .map(ClusterNode::name)
                .collect(Collectors.toSet());

        return requiredNodes.stream().filter(not(logicalNodeIds::contains)).collect(Collectors.toList());
    }

    CompletableFuture<Void> updateAllReplicasWithMinimalTime(HybridTimestamp minimalPendingTxStartTime) {
        Map<Integer, Integer> tablesWithPartitions = findAllTablesSince(minimalPendingTxStartTime.longValue());

        return updateAllReplicasWithMinimalTime0(
                tablesWithPartitions.entrySet().iterator(),
                minimalPendingTxStartTime.longValue(),
                clockService.now()
        );
    }

    private Map<Integer, Integer> findAllTablesSince(long beginTs) {
        Map<Integer, Integer> tablesWithPartitions = new HashMap<>();
        int ver = catalogManager.activeCatalogVersion(beginTs);
        int lastVer = catalogManager.activeCatalogVersion(clockService.nowLong());

        do {
            Catalog catalog = catalogManager.catalog(ver);

            assert catalog != null : "ver=" + ver + ", last=" + lastVer;

            for (CatalogTableDescriptor table : catalog.tables()) {
                CatalogZoneDescriptor zone = catalog.zone(table.zoneId());

                assert zone != null : table.zoneId();

                tablesWithPartitions.put(table.id(), zone.partitions());
            }
        } while (++ver <= lastVer);

        return tablesWithPartitions;
    }

    private CompletableFuture<Void> updateAllReplicasWithMinimalTime0(
            Iterator<Map.Entry<Integer, Integer>> tabItr,
            long txBeginTime,
            HybridTimestamp nowTs
    ) {
        if (!tabItr.hasNext()) {
            return CompletableFutures.nullCompletedFuture();
        }

        Entry<Integer, Integer> tableWithPartsCount = tabItr.next();
        int parts = tableWithPartsCount.getValue();

        List<CompletableFuture<?>> partitionFutures = new ArrayList<>();

        for (int p = 0; p < parts; p++) {
            TablePartitionId replicationGroupId = new TablePartitionId(tableWithPartsCount.getKey(), p);

            CompletableFuture<Object> fut = placementDriver.getAssignments(replicationGroupId, nowTs)
                    .thenCompose(tokenizedAssignments -> {
                        if (tokenizedAssignments == null || tokenizedAssignments.nodes().isEmpty()) {
                            throw new IllegalStateException("Assignments are not available");
                        }

                        Assignment assignment = tokenizedAssignments.nodes().iterator().next();

                        TablePartitionIdMessage partIdMessage = ReplicaMessageUtils.toTablePartitionIdMessage(
                                REPLICA_MESSAGES_FACTORY,
                                replicationGroupId
                        );

                        UpdateMinimalPendingTxStartTimeReplicaRequest msg = PART_REPL_MESSAGE_FACTORY
                                .updateMinimalPendingTxStartTimeReplicaRequest()
                                .groupId(partIdMessage)
                                .timestamp(txBeginTime)
                                .build();

                        return replicaService.invoke(assignment.consistentId(), msg);
                    });

            partitionFutures.add(fut);
        }

        return CompletableFutures.allOf(partitionFutures)
                .thenCompose(ignore -> updateAllReplicasWithMinimalTime0(tabItr, txBeginTime, nowTs));
    }

    /** Minimum required time supplier. */
    @FunctionalInterface
    interface MinimumRequiredTimeProvider {
        /** Returns minimum required timestamp. */
        long time();
    }
}
