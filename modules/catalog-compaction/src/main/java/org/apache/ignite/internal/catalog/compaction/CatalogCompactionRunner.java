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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.affinity.TokenizedAssignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManagerImpl;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessageGroup;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMessagesFactory;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesRequest;
import org.apache.ignite.internal.catalog.compaction.message.CatalogCompactionMinimumTimesResponse;
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
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.replication.UpdateMinimumActiveTxStartTimeReplicaRequest;
import org.apache.ignite.internal.placementdriver.PlacementDriver;
import org.apache.ignite.internal.replicator.ReplicaService;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessageUtils;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.tx.ActiveTxMinimumBeginTimeProvider;
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

    private static final long ANSWER_TIMEOUT = 5_000;

    private final CatalogManagerCompactionFacade catalogManagerHelper;

    private final MessagingService messagingService;

    private final LogicalTopologyService logicalTopologyService;

    private final PlacementDriver placementDriver;

    private final ClockService clockService;

    private final Executor executor;

    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private final MinimumRequiredTimeProvider localMinTimeProvider;

    private final String localNodeName;

    private final ActiveTxMinimumBeginTimeProvider localActiveRwTxMinBeginTimeProvider;

    private final ReplicaService replicaService;

    private CompletableFuture<Void> lastRunFuture = CompletableFutures.nullCompletedFuture();

    /**
     * Node that is considered to be a coordinator of compaction process.
     *
     * <p>May be not set. Node should act as coordinator only in case this field is set and value is equal to name of the local node.
     */
    private volatile @Nullable String compactionCoordinatorNodeName;

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
            ActiveTxMinimumBeginTimeProvider localActiveRwTxMinBeginTimeProvider
    ) {
        this(localNodeName, catalogManager, messagingService, logicalTopologyService, placementDriver, replicaService, clockService,
                executor, localActiveRwTxMinBeginTimeProvider, null);
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
            ActiveTxMinimumBeginTimeProvider localActiveRwTxMinBeginTimeProvider,
            @Nullable CatalogCompactionRunner.MinimumRequiredTimeProvider localMinTimeProvider
    ) {
        this.localNodeName = localNodeName;
        this.messagingService = messagingService;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManagerHelper = new CatalogManagerCompactionFacade(catalogManager);
        this.clockService = clockService;
        this.placementDriver = placementDriver;
        this.replicaService = replicaService;
        this.executor = executor;
        this.localActiveRwTxMinBeginTimeProvider = localActiveRwTxMinBeginTimeProvider;
        this.localMinTimeProvider = localMinTimeProvider == null ? this::determineLocalMinimumRequiredTime : localMinTimeProvider;
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(CatalogCompactionMessageGroup.class, (message, sender, correlationId) -> {
            assert message.groupType() == CatalogCompactionMessageGroup.GROUP_TYPE : message.groupType();
            assert correlationId != null;

            if (message.messageType() == CatalogCompactionMessageGroup.MINIMUM_REQUIRED_TIME_REQUEST) {
                CatalogCompactionMinimumTimesResponse response = COMPACTION_MESSAGES_FACTORY.catalogCompactionMinimumTimesResponse()
                        .minimumRequiredTime(localMinTimeProvider.time())
                        .minimumActiveTxTime(localActiveRwTxMinBeginTimeProvider.minimumBeginTime())
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
                    LOG.info("Catalog compaction is already in progress, skipping (timestamp={})", lwm.longValue());
                }

                lastRunFuture = startCompaction(logicalTopologyService.localLogicalTopology());
            }
        });
    }

    private CompletableFuture<Void> startCompaction(LogicalTopologySnapshot topologySnapshot) {
        long localMinimum = localMinTimeProvider.time();

        if (catalogManagerHelper.catalogByTsNullable(localMinimum) == null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Catalog compaction skipped, nothing to compact (ts={})", localMinimum);
            }

            return CompletableFutures.nullCompletedFuture();
        }

        return determineGlobalMinimumRequiredTime(topologySnapshot.nodes(), localMinimum)
                .thenComposeAsync(timeHolder -> {
                    long minRequiredTime = timeHolder.minRequiredTime;
                    HybridTimestamp minActiveTxStartTime = timeHolder.minActiveTxStartTime;
                    Catalog catalog = catalogManagerHelper.catalogByTsNullable(minRequiredTime);

                    CompletableFuture<Boolean> catalogCompactionFut;

                    if (catalog == null) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Catalog compaction skipped, nothing to compact (ts={})", minRequiredTime);
                        }

                        catalogCompactionFut = CompletableFutures.falseCompletedFuture();
                    } else {
                        catalogCompactionFut = tryCompactCatalog(catalog, topologySnapshot).whenComplete((res, ex) -> {
                            if (ex != null) {
                                LOG.warn("Catalog compaction has failed (timestamp={})", ex, minRequiredTime);
                            } else {
                                if (res && LOG.isDebugEnabled()) {
                                    LOG.debug("Catalog compaction completed successfully (timestamp={})", minRequiredTime);
                                } else {
                                    LOG.debug("Catalog compaction skipped (timestamp={})", minRequiredTime);
                                }
                            }
                        });
                    }

                    if (minActiveTxStartTime == null) {
                        return catalogCompactionFut.thenApply(ignore -> null);
                    }

                    CompletableFuture<Void> propagateToReplicasFut = propagateTimeToReplicas(minActiveTxStartTime)
                            .whenComplete((res, ex) -> {
                                if (ex != null) {
                                    LOG.warn("Failed to propagate minimum active tx start time to replicas", ex);
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
                    HybridTimestamp globalMinimumActiveTxTime = localActiveRwTxMinBeginTimeProvider.minimumBeginTime();

                    for (CompletableFuture<CatalogCompactionMinimumTimesResponse> fut : responseFutures) {
                        CatalogCompactionMinimumTimesResponse response = fut.join();

                        if (response.minimumRequiredTime() < globalMinimumRequiredTime) {
                            globalMinimumRequiredTime = response.minimumRequiredTime();
                        }

                        HybridTimestamp responseMinActiveTxTime = response.minimumActiveTxTime();

                        if (responseMinActiveTxTime != null) {
                            if (globalMinimumActiveTxTime == null || responseMinActiveTxTime.compareTo(globalMinimumActiveTxTime) < 0) {
                                globalMinimumActiveTxTime = response.minimumActiveTxTime();
                            }
                        }
                    }

                    return new TimeHolder(globalMinimumRequiredTime, globalMinimumActiveTxTime);
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
            TablePartitionId replicationGroupId = new TablePartitionId(table.id(), p);

            CompletableFuture<TokenizedAssignments> assignmentsFut = placementDriver.getAssignments(replicationGroupId, nowTs)
                    .whenComplete((tokenizedAssignments, ex) -> {
                        if (ex != null) {
                            return;
                        }

                        if (tokenizedAssignments == null) {
                            throwAssignmentsNotReadyException(replicationGroupId);
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

    CompletableFuture<Void> propagateTimeToReplicas(HybridTimestamp minimumRequiredTime) {
        Map<Integer, Integer> tablesWithPartitions = catalogManagerHelper.collectTablesWithPartitionsBetween(
                minimumRequiredTime.longValue(),
                clockService.nowLong()
        );

        return invokeOnReplicas(
                tablesWithPartitions.entrySet().iterator(),
                minimumRequiredTime.longValue(),
                clockService.now()
        );
    }

    private CompletableFuture<Void> invokeOnReplicas(
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
                        if (tokenizedAssignments == null) {
                            throwAssignmentsNotReadyException(replicationGroupId);
                        }

                        Assignment assignment = tokenizedAssignments.nodes().iterator().next();

                        TablePartitionIdMessage partIdMessage = ReplicaMessageUtils.toTablePartitionIdMessage(
                                REPLICA_MESSAGES_FACTORY,
                                replicationGroupId
                        );

                        UpdateMinimumActiveTxStartTimeReplicaRequest msg = REPLICATION_MESSAGES_FACTORY
                                .updateMinimumActiveTxStartTimeReplicaRequest()
                                .groupId(partIdMessage)
                                .timestamp(txBeginTime)
                                .build();

                        return replicaService.invoke(assignment.consistentId(), msg);
                    });

            partitionFutures.add(fut);
        }

        return CompletableFutures.allOf(partitionFutures)
                .thenCompose(ignore -> invokeOnReplicas(tabItr, txBeginTime, nowTs));
    }

    private CompletableFuture<Boolean> tryCompactCatalog(Catalog catalog, LogicalTopologySnapshot topologySnapshot) {
        return requiredNodes(catalog)
                .thenCompose(requiredNodes -> {
                    List<String> missingNodes = missingNodes(requiredNodes, topologySnapshot.nodes());

                    if (!missingNodes.isEmpty()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Catalog compaction aborted due to missing cluster members (nodes={})", missingNodes);
                        }

                        return CompletableFutures.falseCompletedFuture();
                    }

                    return catalogManagerHelper.compactCatalog(catalog);
                });
    }

    private static void throwAssignmentsNotReadyException(TablePartitionId replicationGroupId) {
        throw new IllegalStateException("Cannot get assignments for table "
                + "(replication group=" + replicationGroupId + ").");
    }

    /** Minimum required time supplier. */
    @FunctionalInterface
    interface MinimumRequiredTimeProvider {
        /** Returns minimum required timestamp. */
        long time();
    }

    static class TimeHolder {
        final long minRequiredTime;
        final @Nullable HybridTimestamp minActiveTxStartTime;

        private TimeHolder(long minRequiredTime, @Nullable HybridTimestamp minActiveTxStartTime) {
            this.minRequiredTime = minRequiredTime;
            this.minActiveTxStartTime = minActiveTxStartTime;
        }
    }
}
