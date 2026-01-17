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

package org.apache.ignite.internal.table.distributed.disaster;

import static java.util.Collections.emptyList;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_DROP;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.CATCHING_UP;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createGlobalZonePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createLocalZonePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.DEGRADED;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.READ_ONLY;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.Constants.DISASTER_RECOVERY_TIMEOUT_MILLIS;
import static org.apache.ignite.internal.util.ExceptionUtils.hasCause;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.PARTITION_STATE_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEvent;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEventParams;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil;
import org.apache.ignite.internal.failure.FailureContext;
import org.apache.ignite.internal.failure.FailureManager;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metrics.MetricManager;
import org.apache.ignite.internal.network.ChannelType;
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.disaster.DisasterRecoveryRequestMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.DisasterRecoveryResponseMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesRequest;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesResponse;
import org.apache.ignite.internal.partition.replicator.network.disaster.OperationCompletedMessage;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryRequestForwardException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalNodesException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalPartitionIdException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodeLeftException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NotEnoughAliveNodesException;
import org.apache.ignite.internal.table.distributed.storage.NullMvTableStorage;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations,
 * such as {@link #resetPartitions(String, Set)}.
 * More details are in the <a href="https://issues.apache.org/jira/browse/IGNITE-21140">epic</a>.
 */
public class DisasterRecoveryManager implements IgniteComponent, SystemViewProvider {
    /** Logger. */
    static final IgniteLogger LOG = Loggers.forClass(DisasterRecoveryManager.class);

    /** Single key for writing disaster recovery requests into meta-storage. */
    static final ByteArray RECOVERY_TRIGGER_KEY = new ByteArray("disaster.recovery.trigger");

    /**
     * Metastorage key prefix to store the per zone revision of logical event, which start the recovery process.
     * It's needed to skip the stale recovery triggers.
     */
    private static final String RECOVERY_TRIGGER_REVISION_KEY_PREFIX = "disaster.recovery.trigger.revision.";

    private static final PartitionReplicationMessagesFactory PARTITION_REPLICATION_MESSAGES_FACTORY =
            new PartitionReplicationMessagesFactory();

    private static final ReplicaMessagesFactory REPLICA_MESSAGES_FACTORY = new ReplicaMessagesFactory();

    /**
     * Maximal allowed difference between committed index on the leader and on the follower, that differentiates
     * {@link LocalPartitionStateEnum#HEALTHY} from {@link LocalPartitionStateEnum#CATCHING_UP}.
     */
    private static final int CATCH_UP_THRESHOLD = 100;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Thread pool executor for async parts. */
    private final ExecutorService threadPool;

    /** Messaging service. */
    private final MessagingService messagingService;

    /** Meta-storage manager. */
    final MetaStorageManager metaStorageManager;

    /** Catalog manager. */
    final CatalogManager catalogManager;

    /** Distribution zone manager. */
    final DistributionZoneManager dzManager;

    /** Raft manager. */
    final Loza raftManager;

    /** Cluster physical topology service.  */
    private final TopologyService topologyService;

    private final LogicalTopologyService logicalTopology;

    /** Watch listener for {@link #RECOVERY_TRIGGER_KEY}. */
    private final WatchListener watchListener;

    private final LogicalTopologyEventListener nodeLeftListener;

    /** Table manager. */
    private final TableManager tableManager;

    final PartitionReplicaLifecycleManager partitionReplicaLifecycleManager;

    /** Metric manager. */
    private final MetricManager metricManager;

    private final FailureManager failureManager;

    private final SystemViewManager systemViewManager;

    /**
     * Map of operations, triggered by local node, that have not yet been processed by {@link #watchListener}. Values in the map are the
     * futures, returned from the {@link #processNewRequest(DisasterRecoveryRequest)}, they are completed by
     * {@link #handleTriggerKeyUpdate(WatchEvent)} when node receives corresponding events from the metastorage (or if it doesn't receive
     * this event within a 30 seconds window).
     */
    private final Map<UUID, CompletableFuture<Void>> ongoingOperationsById = new ConcurrentHashMap<>();

    private final Map<Integer, PartitionStatesMetricSource> metricSourceByTableId = new ConcurrentHashMap<>();

    private final Map<UUID, MultiNodeOperations> operationsByNodeId = new ConcurrentHashMap<>();

    /** Constructor. */
    public DisasterRecoveryManager(
            ExecutorService threadPool,
            MessagingService messagingService,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            DistributionZoneManager dzManager,
            Loza raftManager,
            TopologyService topologyService,
            LogicalTopologyService logicalTopology,
            TableManager tableManager,
            MetricManager metricManager,
            FailureManager failureManager,
            PartitionReplicaLifecycleManager partitionReplicaLifecycleManager,
            SystemViewManager systemViewManager
    ) {
        this.threadPool = threadPool;
        this.messagingService = messagingService;
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.dzManager = dzManager;
        this.raftManager = raftManager;
        this.topologyService = topologyService;
        this.logicalTopology = logicalTopology;
        this.tableManager = tableManager;
        this.metricManager = metricManager;
        this.failureManager = failureManager;
        this.partitionReplicaLifecycleManager = partitionReplicaLifecycleManager;
        this.systemViewManager = systemViewManager;

        watchListener = event -> inBusyLock(busyLock, () -> {
            handleTriggerKeyUpdate(event);

            // There is no need to block a watch thread any longer.
            return nullCompletedFuture();
        });

        nodeLeftListener = new LogicalTopologyEventListener() {
            @Override
            public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
                operationsByNodeId.compute(leftNode.id(), (node, operations) -> {
                    if (operations != null) {
                        operations.completeAllExceptionally(leftNode.name(), new NodeLeftException(leftNode.name(), leftNode.id()));
                    }

                    return null;
                });
            }
        };
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLock(busyLock, () -> {
            systemViewManager.register(this);

            messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

            metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

            dzManager.listen(HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED, this::onHaZonePartitionTopologyReduce);

            catalogManager.listen(TABLE_CREATE, fromConsumer(this::onTableCreate));

            catalogManager.listen(TABLE_DROP, fromConsumer(this::onTableDrop));

            registerMetricSources();

            logicalTopology.addEventListener(nodeLeftListener);

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        metaStorageManager.unregisterWatch(watchListener);

        logicalTopology.removeEventListener(nodeLeftListener);

        for (CompletableFuture<Void> future : ongoingOperationsById.values()) {
            future.completeExceptionally(new NodeStoppingException());
        }

        return nullCompletedFuture();
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return List.of(
                createGlobalZonePartitionStatesSystemView(this),
                createLocalZonePartitionStatesSystemView(this)
        );
    }

    @TestOnly
    public Map<UUID, CompletableFuture<Void>> ongoingOperationsById() {
        return ongoingOperationsById;
    }

    public IgniteSpinBusyLock busyLock() {
        return busyLock;
    }

    private Set<Assignment> stableAssignmentsWithOnlyAliveNodes(ReplicationGroupId partitionId, long revision) {
        Set<Assignment> stableAssignments;

        stableAssignments = ZoneRebalanceUtil.zoneStableAssignmentsGetLocally(
                metaStorageManager,
                (ZonePartitionId) partitionId,
                revision
        ).nodes();

        Set<String> logicalTopology = dzManager.logicalTopology(revision)
                .stream().map(NodeWithAttributes::nodeName).collect(Collectors.toUnmodifiableSet());

        return stableAssignments
                .stream().filter(a -> logicalTopology.contains(a.consistentId())).collect(Collectors.toUnmodifiableSet());
    }

    private CompletableFuture<Boolean> onHaZonePartitionTopologyReduce(HaZoneTopologyUpdateEventParams params) {
        return inBusyLock(busyLock, () -> {
            int zoneId = params.zoneId();
            long revision = params.causalityToken();
            long timestamp = metaStorageManager.timestampByRevisionLocally(revision).longValue();

            Catalog catalog = catalogManager.activeCatalog(timestamp);
            CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

            Set<Integer> partitionsToReset = new HashSet<>();

            for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
                ZonePartitionId partitionId = new ZonePartitionId(zoneId, partId);

                if (stableAssignmentsWithOnlyAliveNodes(partitionId, revision).size() < calculateQuorum(zoneDescriptor.replicas())) {
                    partitionsToReset.add(partId);
                }
            }

            if (!partitionsToReset.isEmpty()) {
                return resetPartitions(zoneDescriptor.name(), Map.of(zoneId, partitionsToReset), false, revision).thenApply(
                        r -> false);
            } else {
                return falseCompletedFuture();
            }
        });
    }

    /**
     * Updates assignments of the zone in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to reset. If empty, reset all zone's partitions.
     * @return Future that completes when partitions are reset.
     */
    public CompletableFuture<Void> resetPartitions(String zoneName, Set<Integer> partitionIds) {
        return inBusyLock(busyLock, () -> {
            int zoneId = zoneDescriptor(catalogLatestVersion(), zoneName).id();

            return resetPartitions(zoneName, Map.of(zoneId, partitionIds), true, -1);
        });
    }

    /**
     * Updates assignments of the zone in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to reset. If empty, reset all zone's partitions.
     * @param manualUpdate Whether the update is triggered manually by user or automatically by core logic.
     * @param triggerRevision Revision of the event, which produce this reset. -1 for manual reset.
     * @return Future that completes when partitions are reset.
     */
    public CompletableFuture<Void> resetPartitions(
            String zoneName,
            Set<Integer> partitionIds,
            boolean manualUpdate,
            long triggerRevision
    ) {
        return inBusyLock(busyLock, () -> {
            int zoneId = zoneDescriptor(catalogLatestVersion(), zoneName).id();

            return resetPartitions(zoneName, Map.of(zoneId, partitionIds), manualUpdate, triggerRevision);
        });
    }

    /**
     * Updates assignments of the zone in a forced manner, allowing for the recovery of raft group with lost majorities. It is
     * achieved via triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required.
     * New pending assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via
     * "resetPeers" so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds Map of per zone or table partitions' sets to reset. If empty, reset all zone's partitions.
     * @param manualUpdate Whether the update is triggered manually by user or automatically by core logic.
     * @param triggerRevision Revision of the event, which produce this reset. -1 for manual reset.
     * @return Future that completes when partitions are reset.
     */
    private CompletableFuture<Void> resetPartitions(
            String zoneName,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate,
            long triggerRevision
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                Catalog catalog = catalogLatestVersion();

                CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

                partitionIds.values().forEach(ids -> checkPartitionsRange(ids, Set.of(zone)));

                return processNewRequest(
                        GroupUpdateRequest.create(
                                UUID.randomUUID(),
                                catalog.version(),
                                zone.id(),
                                partitionIds,
                                manualUpdate
                        ),
                        triggerRevision
                );
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Restarts replica service and raft group of passed partitions.
     *
     * @param nodeNames Names specifying nodes to restart partitions. Case-sensitive, empty set means "all nodes".
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to restart. If empty, restart all zone's partitions.
     * @return Future that completes when partitions are restarted.
     */
    public CompletableFuture<Void> restartPartitions(
            Set<String> nodeNames,
            String zoneName,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                // Validates passed node names.
                getNodes(nodeNames);

                Catalog catalog = catalogLatestVersion();

                CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

                checkPartitionsRange(partitionIds, Set.of(zone));

                return processNewRequest(new ManualGroupRestartRequest(
                        UUID.randomUUID(),
                        zone.id(),
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        false,
                        localNode().name()
                ));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Restart partitions of a zone with cleanup. This method destroys partition storage during restart.
     *
     * @param nodeNames Names of nodes to restart partitions on. Only one node is allowed.
     * @param zoneName Zone name. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to restart. If empty, restart all zone's partitions.
     * @return Future that completes when partitions are restarted.
     */
    public CompletableFuture<Void> restartPartitionsWithCleanup(
            Set<String> nodeNames,
            String zoneName,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                // Validates passed node names.
                getNodes(nodeNames);

                Catalog catalog = catalogLatestVersion();

                CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

                checkPartitionsRange(partitionIds, Set.of(zone));

                checkOnlyOneNodeSpecified(nodeNames);

                return processNewRequest(new ManualGroupRestartRequest(
                        UUID.randomUUID(),
                        zone.id(),
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        true,
                        localNode().name()
                ));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link ZonePartitionId} to the mapping between a node name and a
     * partition state.
     *
     * @param zoneNames Names specifying zones to get partition states from. Case-sensitive, empty set means "all zones".
     * @param nodeNames Names specifying nodes to get partition states from. Case-sensitive, empty set means "all nodes".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<ZonePartitionId, LocalPartitionStateByNode>> localPartitionStates(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                Catalog catalog = catalogLatestVersion();

                return localPartitionStatesInternal(
                        zoneNames,
                        nodeNames,
                        partitionIds,
                        catalog,
                        zoneState()
                ).thenApply(res -> normalizeLocal(res, catalog));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link ZonePartitionId} to the global partition state value.
     *
     * @param zoneNames Names specifying zones to get partition states. Case-sensitive, empty set means "all zones".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<ZonePartitionId, GlobalPartitionState>> globalPartitionStates(
            Set<String> zoneNames,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                Catalog catalog = catalogLatestVersion();

                return localPartitionStatesInternal(
                        zoneNames,
                        Set.of(),
                        partitionIds,
                        catalog,
                        zoneState()
                )
                        .thenApply(res -> normalizeLocal(res, catalog))
                        .thenApply(res -> assembleGlobal(res, partitionIds, catalog));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    static Function<LocalPartitionStateMessage, ZonePartitionId> zoneState() {
        return state -> state.zonePartitionId().asZonePartitionId();
    }

    <T extends ReplicationGroupId> CompletableFuture<Map<T, LocalPartitionStateMessageByNode>> localPartitionStatesInternal(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds,
            Catalog catalog,
            Function<LocalPartitionStateMessage, T> keyExtractor
    ) {
        return inBusyLock(busyLock, () -> {
            Collection<CatalogZoneDescriptor> zones = DistributionZonesUtil.filterZonesForOperations(zoneNames, catalog.zones());

            checkPartitionsRange(partitionIds, zones);

            Set<NodeWithAttributes> nodes = getNodes(nodeNames);

            Set<Integer> zoneIds = zones.stream().map(CatalogObjectDescriptor::id).collect(toSet());

            LocalPartitionStatesRequest localPartitionStatesRequest = PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStatesRequest()
                    .zoneIds(zoneIds)
                    .partitionIds(partitionIds)
                    .catalogVersion(catalog.version())
                    .build();

            Map<T, LocalPartitionStateMessageByNode> result = new ConcurrentHashMap<>();
            CompletableFuture<?>[] futures = new CompletableFuture[nodes.size()];

            int i = 0;
            for (NodeWithAttributes node : nodes) {
                CompletableFuture<NetworkMessage> invokeFuture = messagingService.invoke(
                        node.nodeName(),
                        localPartitionStatesRequest,
                        DISASTER_RECOVERY_TIMEOUT_MILLIS
                );

                futures[i++] = invokeFuture.thenAccept(networkMessage -> {
                    inBusyLock(busyLock, () -> {
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Received local partition states response [networkMessage={}]", networkMessage);
                        }

                        assert networkMessage instanceof LocalPartitionStatesResponse : networkMessage;

                        var response = (LocalPartitionStatesResponse) networkMessage;

                        for (LocalPartitionStateMessage state : response.states()) {
                            result.compute(keyExtractor.apply(state), (partitionId, messageByNode) -> {
                                if (messageByNode == null) {
                                    return new LocalPartitionStateMessageByNode(Map.of(node.nodeName(), state));
                                }

                                messageByNode = new LocalPartitionStateMessageByNode(messageByNode);
                                messageByNode.put(node.nodeName(), state);
                                return messageByNode;
                            });
                        }

                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Combined partition state result for node [node={}, result={}]", node, result);
                        }
                    });
                });
            }

            return allOf(futures).handle((unused, err) -> {
                if (err != null) {
                    throw new DisasterRecoveryException(PARTITION_STATE_ERR, err);
                }

                if (LOG.isInfoEnabled()) {
                    LOG.debug("Returning total result for local partition states [result={}]", result);
                }

                return result;
            });
        });
    }

    private static void checkPartitionsRange(Set<Integer> partitionIds, Collection<CatalogZoneDescriptor> zones) {
        if (partitionIds.isEmpty()) {
            return;
        }

        int minPartition = partitionIds.stream().min(Integer::compare).get();

        if (minPartition < 0) {
            throw new IllegalPartitionIdException(minPartition);
        }

        int maxPartition = partitionIds.stream().max(Integer::compare).get();

        zones.forEach(zone -> {
            if (maxPartition >= zone.partitions()) {
                throw new IllegalPartitionIdException(maxPartition, zone.partitions(), zone.name());
            }
        });
    }

    private static void checkOnlyOneNodeSpecified(Set<String> nodeNames) {
        if (nodeNames.size() != 1) {
            throw new IllegalNodesException();
        }
    }

    private Set<NodeWithAttributes> getNodes(Set<String> nodeNames) throws NodesNotFoundException {
        if (nodeNames.isEmpty()) {
            return dzManager.logicalTopology();
        }

        Set<NodeWithAttributes> nodes = dzManager.logicalTopology().stream()
                .filter(node -> nodeNames.contains(node.nodeName()))
                .collect(toSet());

        Set<String> foundNodeNames = nodes.stream()
                .map(NodeWithAttributes::nodeName)
                .collect(toSet());

        if (!nodeNames.equals(foundNodeNames)) {
            Set<String> missingNodeNames = CollectionUtils.difference(nodeNames, foundNodeNames);

            throw new NodesNotFoundException(missingNodeNames);
        }

        return nodes;
    }

    /**
     * Short version of {@link DisasterRecoveryManager#processNewRequest(DisasterRecoveryRequest, long)} without revision.
     *
     * @param request Request.
     * @return Operation future.
     */
    private CompletableFuture<Void> processNewRequest(DisasterRecoveryRequest request) {
        return processNewRequest(request, -1);
    }

    /**
     * Creates new operation future, associated with the request, and writes it into meta-storage.
     *
     * @param request Request.
     * @param revision Revision of event, which produce this recovery request.
     * @return Operation future.
     */
    private CompletableFuture<Void> processNewRequest(DisasterRecoveryRequest request, long revision) {
        // Check if this is a manual restart request with specific nodes, and forward to the first node if needed.
        if (request instanceof MultiNodeDisasterRecoveryRequest) {
            MultiNodeDisasterRecoveryRequest multiNodeRequest = (MultiNodeDisasterRecoveryRequest) request;

            Set<String> nodeNames = multiNodeRequest.nodeNames();

            if (!nodeNames.isEmpty()) {
                String firstNode = nodeNames.stream()
                        .sorted()
                        .findFirst()
                        .orElseThrow();

                String localNodeName = topologyService.localMember().name();

                // If this is not the target node, forward the request and return its response.
                if (!firstNode.equals(localNodeName)) {
                    return forwardDisasterRecoveryRequest(multiNodeRequest, revision, firstNode);
                }
            }
        }

        UUID operationId = request.operationId();

        CompletableFuture<Void> operationFuture = new CompletableFuture<Void>().orTimeout(DISASTER_RECOVERY_TIMEOUT_MILLIS, MILLISECONDS);

        CompletableFuture<Void> remoteProcessingFuture = remoteProcessingFuture(request);

        operationFuture.whenComplete((v, throwable) -> ongoingOperationsById.remove(operationId));

        byte[] serializedRequest = VersionedSerialization.toBytes(request, DisasterRecoveryRequestSerializer.INSTANCE);

        ongoingOperationsById.put(operationId, operationFuture);

        if (revision != -1) {
            putRecoveryTriggerIfRevisionIsNotProcessed(
                    request.zoneId(),
                    longToBytesKeepingOrder(revision),
                    serializedRequest,
                    operationId
            );
        } else {
            // In case of manual request - it is ok to bypass the revision check,
            // because we have no any trigger with revision for this call.
            metaStorageManager.put(RECOVERY_TRIGGER_KEY, serializedRequest);
        }

        return operationFuture.thenCompose(v -> remoteProcessingFuture);
    }

    private CompletableFuture<Void> remoteProcessingFuture(DisasterRecoveryRequest request) {
        if (request.type() != DisasterRecoveryRequestType.MULTI_NODE) {
            return nullCompletedFuture();
        }

        UUID operationId = request.operationId();

        MultiNodeDisasterRecoveryRequest multiNodeRequest = (MultiNodeDisasterRecoveryRequest) request;

        Set<NodeWithAttributes> nodes = getRequestNodes(multiNodeRequest.nodeNames());

        CompletableFuture<?>[] remoteProcessingFutures = nodes
                .stream()
                .map(node -> addMultiNodeOperation(node, operationId))
                .toArray(CompletableFuture[]::new);

        return allOf(remoteProcessingFutures);
    }

    /** If request node names is empty, returns all nodes in the logical topology. */
    private Set<NodeWithAttributes> getRequestNodes(Set<String> requestNodeNames) {
        return dzManager.logicalTopology().stream()
                .filter(node -> requestNodeNames.isEmpty() || requestNodeNames.contains(node.nodeName()))
                .collect(toSet());
    }

    private CompletableFuture<Void> addMultiNodeOperation(NodeWithAttributes node, UUID operationId) {
        CompletableFuture<Void> result = new CompletableFuture<Void>().orTimeout(DISASTER_RECOVERY_TIMEOUT_MILLIS, MILLISECONDS);

        operationsByNodeId.compute(node.nodeId(), (nodeId, operations) -> {
            Set<UUID> nodes = dzManager.logicalTopology().stream()
                    .map(NodeWithAttributes::nodeId)
                    .collect(toSet());

            if (!nodes.contains(nodeId)) {
                result.completeExceptionally(new NodeLeftException(node.nodeName(), nodeId));

                return operations;
            }

            if (operations == null) {
                operations = new MultiNodeOperations();
            }

            operations.add(operationId, result);

            return operations;
        });

        // Cleanup operation on completion.
        return result.whenComplete((v, e) ->
                operationsByNodeId.compute(node.nodeId(), (nodeId, operations) -> {
                    if (operations != null) {
                        operations.remove(operationId);

                        return operations.isEmpty() ? null : operations;
                    }

                    return null;
                }));
    }

    /**
     * Put the {@link DisasterRecoveryManager#RECOVERY_TRIGGER_KEY}
     * if the revision of the trigger event is not processed for this zone yet.
     *
     * @param zoneId Zone id.
     * @param revisionBytes Trigger event revision as bytes.
     * @param recoveryTriggerValue Recovery trigger as bytes.
     * @param operationId Operation ID.
     */
    private void putRecoveryTriggerIfRevisionIsNotProcessed(
            int zoneId,
            byte[] revisionBytes,
            byte[] recoveryTriggerValue,
            UUID operationId
    ) {
        ByteArray zoneTriggerRevisionKey = zoneRecoveryTriggerRevisionKey(zoneId);

        metaStorageManager.invoke(
                        notExists(zoneTriggerRevisionKey).or(value(zoneTriggerRevisionKey).lt(revisionBytes)),
                        List.of(
                                put(RECOVERY_TRIGGER_KEY, recoveryTriggerValue),
                                put(zoneTriggerRevisionKey, revisionBytes)
                        ),
                        List.of()
                ).thenAccept(wasWrite -> {
                    if (!wasWrite) {
                        ongoingOperationsById.remove(operationId).complete(null);
                    }
                });
    }

    /**
     * Forwards a disaster recovery request to a specific node.
     *
     * @param request The disaster recovery request to forward.
     * @param revision Revision of event, which produce this recovery request, or -1 for manual requests.
     * @param targetNodeName Name of the target node to forward the request to.
     * @return Future that completes when the forwarded request is processed.
     */
    private CompletableFuture<Void> forwardDisasterRecoveryRequest(
            MultiNodeDisasterRecoveryRequest request,
            long revision,
            String targetNodeName
    ) {
        DisasterRecoveryRequest updatedCoordinator = request.updateCoordinator(targetNodeName);

        byte[] serializedRequest = VersionedSerialization.toBytes(updatedCoordinator, DisasterRecoveryRequestSerializer.INSTANCE);

        DisasterRecoveryRequestMessage message = PARTITION_REPLICATION_MESSAGES_FACTORY.disasterRecoveryRequestMessage()
                .requestBytes(serializedRequest)
                .revision(revision)
                .build();

        return messagingService.invoke(targetNodeName, message, DISASTER_RECOVERY_TIMEOUT_MILLIS)
                .thenApply(responseMsg -> {
                    assert responseMsg instanceof DisasterRecoveryResponseMessage : responseMsg;

                    DisasterRecoveryResponseMessage response = (DisasterRecoveryResponseMessage) responseMsg;

                    if (response.errorMessage() != null) {
                        String errorMessage = response.errorMessage();
                        throw new DisasterRecoveryRequestForwardException(targetNodeName, errorMessage == null ? "" : errorMessage);
                    }
                    return null;
                });
    }

    /**
     * Handler for {@link #RECOVERY_TRIGGER_KEY} update event. Deserializes the request and delegates the execution to
     * {@link DisasterRecoveryRequest#handle(DisasterRecoveryManager, long, HybridTimestamp)}.
     */
    private void handleTriggerKeyUpdate(WatchEvent watchEvent) {
        Entry newEntry = watchEvent.entryEvent().newEntry();

        byte[] requestBytes = newEntry.value();
        assert requestBytes != null;

        DisasterRecoveryRequest request;
        try {
            request = VersionedSerialization.fromBytes(requestBytes, DisasterRecoveryRequestSerializer.INSTANCE);
        } catch (Exception e) {
            failureManager.process(new FailureContext(e, "Unable to deserialize disaster recovery request."));

            return;
        }

        CompletableFuture<Void> operationFuture = ongoingOperationsById.remove(request.operationId());

        switch (request.type()) {
            case SINGLE_NODE:
                if (operationFuture == null) {
                    // We're not the initiator, or timeout has passed. Just ignore it.
                    return;
                }

                request.handle(this, watchEvent.revision(), watchEvent.timestamp())
                        .handle((res, ex) -> inBusyLock(busyLock, () -> {
                            copyStateTo(operationFuture).accept(res, ex);

                            if (ex != null) {
                                if (!hasCause(
                                        ex,
                                        NodeStoppingException.class,
                                        UnresolvableConsistentIdException.class,
                                        RecipientLeftException.class
                                )) {
                                    failureManager.process(new FailureContext(ex, "Unable to handle disaster recovery request."));
                                }
                            }

                            return null;
                        }));
                break;
            case MULTI_NODE:
                request.handle(this, watchEvent.revision(), watchEvent.timestamp())
                        .handle((res, ex) -> inBusyLock(busyLock, () -> {
                            if (operationFuture != null) {
                                copyStateTo(operationFuture).accept(res, ex);
                            }

                            MultiNodeDisasterRecoveryRequest multiNodeRequest = (MultiNodeDisasterRecoveryRequest) request;

                            if (multiNodeRequest.coordinator() != null) {
                                messagingService.send(
                                        multiNodeRequest.coordinator(),
                                        ChannelType.DEFAULT,
                                        PARTITION_REPLICATION_MESSAGES_FACTORY.operationCompletedMessage()
                                                .operationId(request.operationId())
                                                .exceptionMessage(ex == null ? null : ex.getMessage())
                                                .build()
                                );
                            }

                            if (ex != null) {
                                if (!hasCause(
                                        ex,
                                        NodeStoppingException.class,
                                        UnresolvableConsistentIdException.class,
                                        RecipientLeftException.class,
                                        NotEnoughAliveNodesException.class
                                )) {
                                    failureManager.process(new FailureContext(ex, "Unable to handle disaster recovery request."));
                                }
                            }

                            return null;
                        }));
                break;
            default:
                var error = new AssertionError("Unexpected request type [type=" + request.type() + ", request=" + request + ']');

                if (operationFuture != null) {
                    operationFuture.completeExceptionally(error);
                }
        }
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof LocalPartitionStatesRequest) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received local partition states request [request={}]", message);
            }

            handleLocalPartitionStatesRequest((LocalPartitionStatesRequest) message, sender, correlationId);
        } else if (message instanceof DisasterRecoveryRequestMessage) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Received disaster recovery request [request={}]", message);
            }

            handleDisasterRecoveryRequest((DisasterRecoveryRequestMessage) message, sender, correlationId);
        } else if (message instanceof OperationCompletedMessage) {
            handleOperationCompletedMessage((OperationCompletedMessage) message, sender);
        }
    }

    private void handleDisasterRecoveryRequest(
            DisasterRecoveryRequestMessage message,
            InternalClusterNode sender,
            @Nullable Long correlationId
    ) {
        assert correlationId != null : "request=" + message + ", sender=" + sender;

        DisasterRecoveryRequest request;
        try {
            request = VersionedSerialization.fromBytes(message.requestBytes(), DisasterRecoveryRequestSerializer.INSTANCE);
        } catch (Exception e) {
            LOG.error("Failed to deserialize disaster recovery request.", e);

            DisasterRecoveryResponseMessage response = PARTITION_REPLICATION_MESSAGES_FACTORY.disasterRecoveryResponseMessage()
                    .errorMessage("Failed to deserialize request: " + e.getMessage())
                    .build();

            messagingService.respond(sender, response, correlationId);
            return;
        }

        // Process the request.
        processNewRequest(request, message.revision())
                .whenComplete((result, throwable) -> {
                    DisasterRecoveryResponseMessage response;

                    if (throwable != null) {
                        response = PARTITION_REPLICATION_MESSAGES_FACTORY.disasterRecoveryResponseMessage()
                                .errorMessage(throwable.getMessage())
                                .build();
                    } else {
                        response = PARTITION_REPLICATION_MESSAGES_FACTORY.disasterRecoveryResponseMessage()
                                .errorMessage(null)
                                .build();
                    }

                    messagingService.respond(sender, response, correlationId);
                });
    }

    private void handleLocalPartitionStatesRequest(
            LocalPartitionStatesRequest request,
            InternalClusterNode sender,
            @Nullable Long correlationId
    ) {
        assert correlationId != null : "request=" + request + ", sender=" + sender;

        int catalogVersion = request.catalogVersion();

        catalogManager.catalogReadyFuture(catalogVersion).thenRunAsync(() -> {
            List<LocalPartitionStateMessage> statesList = new ArrayList<>();

            raftManager.forEach((raftNodeId, raftGroupService) -> {
                if (raftNodeId.groupId() instanceof ZonePartitionId) {
                    LocalPartitionStateMessage message = handleStateRequestForZone(
                            request,
                            raftGroupService,
                            (ZonePartitionId) raftNodeId.groupId(),
                            catalogVersion
                    );

                    if (message != null) {
                        statesList.add(message);
                    }
                }
            });

            LocalPartitionStatesResponse response = PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStatesResponse()
                    .states(statesList)
                    .build();

            if (LOG.isDebugEnabled()) {
                LOG.debug("Responding with state for local partitions [response={}]", response);
            }

            messagingService.respond(sender, response, correlationId);
        }, threadPool);
    }

    private void handleOperationCompletedMessage(
            OperationCompletedMessage message,
            InternalClusterNode sender
    ) {
        MultiNodeOperations multiNodeOperations = operationsByNodeId.get(sender.id());
        if (multiNodeOperations != null) {
            multiNodeOperations.complete(message.operationId(), sender.name(), message.exceptionMessage());
        }
    }

    private @Nullable LocalPartitionStateMessage handleStateRequestForZone(
            LocalPartitionStatesRequest request,
            RaftGroupService raftGroupService,
            ZonePartitionId zonePartitionId,
            int catalogVersion
    ) {
        if (!containsOrEmpty(zonePartitionId.partitionId(), request.partitionIds())) {
            return null;
        }

        Catalog catalog = catalogManager.catalog(catalogVersion);
        assert catalog != null : "Catalog is not found for version: " + catalogVersion;

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zonePartitionId.zoneId());
        // Only zones that belong to a specific catalog version will be returned.
        if (zoneDescriptor == null || !containsOrEmpty(zoneDescriptor.id(), request.zoneIds())) {
            return null;
        }

        Node raftNode = raftGroupService.getRaftNode();

        LocalPartitionStateEnumWithLogIndex localPartitionStateWithLogIndex =
                LocalPartitionStateEnumWithLogIndex.of(raftNode);

        return PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStateMessage()
                .zonePartitionId(toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, zonePartitionId))
                .state(localPartitionStateWithLogIndex.state)
                .logIndex(localPartitionStateWithLogIndex.logIndex)
                .estimatedRows(calculateEstimatedSize(zonePartitionId))
                .isLearner(raftNode.isLearner())
                .build();
    }

    private long calculateEstimatedSize(ZonePartitionId zonePartitionId) {
        return tableManager.zoneTables(zonePartitionId.zoneId()).stream()
                .map(tableImpl -> tableImpl.internalTable().storage())
                .filter(storage -> !(storage instanceof NullMvTableStorage))
                .map(storage -> storage.getMvPartition(zonePartitionId.partitionId()))
                .filter(Objects::nonNull)
                .mapToLong(MvPartitionStorage::estimatedSize)
                .sum();
    }

    private static <T> boolean containsOrEmpty(T item, Collection<T> collection) {
        return collection.isEmpty() || collection.contains(item);
    }

    /**
     * Replaces some healthy states with a {@link LocalPartitionStateEnum#CATCHING_UP}, it can only be done once the state of all peers is
     * known.
     */
    private static Map<ZonePartitionId, LocalPartitionStateByNode> normalizeLocal(
            Map<ZonePartitionId, LocalPartitionStateMessageByNode> result,
            Catalog catalog
    ) {
        Map<ZonePartitionId, LocalPartitionStateByNode> map = new HashMap<>();

        for (Map.Entry<ZonePartitionId, LocalPartitionStateMessageByNode> entry : result.entrySet()) {
            ZonePartitionId zonePartitionId = entry.getKey();
            LocalPartitionStateMessageByNode messageByNode = entry.getValue();

            // noinspection OptionalGetWithoutIsPresent
            long maxLogIndex = messageByNode.values().stream()
                    .mapToLong(LocalPartitionStateMessage::logIndex)
                    .max()
                    .getAsLong();

            Map<String, LocalPartitionState> nodeToStateMap = messageByNode.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, nodeToState ->
                            toLocalPartitionState(nodeToState.getValue(), maxLogIndex, zonePartitionId, catalog))
                    );

            map.put(zonePartitionId, new LocalPartitionStateByNode(nodeToStateMap));
        }

        return map;
    }

    private static LocalPartitionState toLocalPartitionState(
            LocalPartitionStateMessage stateMsg,
            long maxLogIndex,
            ZonePartitionId zonePartitionId,
            Catalog catalog
    ) {
        LocalPartitionStateEnum stateEnum = calculateState(stateMsg, maxLogIndex);

        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zonePartitionId.zoneId());

        String zoneName = zoneDescriptor.name();

        return new LocalPartitionState(
                zonePartitionId.zoneId(),
                zoneName,
                zonePartitionId.partitionId(),
                stateEnum,
                stateMsg.estimatedRows()
        );
    }

    private static LocalPartitionStateEnum calculateState(LocalPartitionStateMessage stateMsg, long maxLogIndex) {
        LocalPartitionStateEnum stateEnum = stateMsg.state();

        if (stateEnum == HEALTHY && maxLogIndex - stateMsg.logIndex() >= CATCH_UP_THRESHOLD) {
            return CATCHING_UP;
        }

        return stateEnum;
    }

    private static Map<ZonePartitionId, GlobalPartitionState> assembleGlobal(
            Map<ZonePartitionId, LocalPartitionStateByNode> localResult,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Map<ZonePartitionId, GlobalPartitionState> result = localResult.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> {
                    ZonePartitionId zonePartitionId = entry.getKey();
                    LocalPartitionStateByNode map = entry.getValue();

                    return assembleGlobalStateFromLocal(catalog, zonePartitionId, map);
                }));

        makeMissingPartitionsUnavailable(localResult, catalog, result, partitionIds);

        return result;
    }

    private static void makeMissingPartitionsUnavailable(
            Map<ZonePartitionId, LocalPartitionStateByNode> localResult,
            Catalog catalog,
            Map<ZonePartitionId, GlobalPartitionState> result,
            Set<Integer> partitionIds
    ) {
        localResult.keySet().stream()
                .map(ZonePartitionId::zoneId)
                .distinct()
                .forEach(zoneId -> {
                    CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

                    if (partitionIds.isEmpty()) {
                        int partitions = zoneDescriptor.partitions();

                        for (int partitionId = 0; partitionId < partitions; partitionId++) {
                            putUnavailableStateIfAbsent(result, partitionId, zoneDescriptor);
                        }
                    } else {
                        partitionIds.forEach(partitionId -> {
                            putUnavailableStateIfAbsent(result, partitionId, zoneDescriptor);
                        });
                    }
                });
    }

    private static void putUnavailableStateIfAbsent(
            Map<ZonePartitionId, GlobalPartitionState> states,
            int partitionId,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        ZonePartitionId zonePartitionId = new ZonePartitionId(zoneDescriptor.id(), partitionId);

        states.computeIfAbsent(zonePartitionId, key ->
                new GlobalPartitionState(
                        zoneDescriptor.id(),
                        zoneDescriptor.name(),
                        key.partitionId(),
                        GlobalPartitionStateEnum.UNAVAILABLE
                )
        );
    }

    private static GlobalPartitionState assembleGlobalStateFromLocal(
            Catalog catalog,
            ZonePartitionId zonePartitionId,
            LocalPartitionStateByNode map
    ) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zonePartitionId.zoneId());

        int replicas = zoneDescriptor.replicas();
        int quorum = calculateQuorum(replicas);

        Map<LocalPartitionStateEnum, List<LocalPartitionState>> groupedStates = map.values().stream()
                .collect(groupingBy(localPartitionState -> localPartitionState.state));

        int healthyReplicas = groupedStates.getOrDefault(HEALTHY, emptyList()).size();

        GlobalPartitionStateEnum globalStateEnum = calculateGlobalState(replicas, healthyReplicas, quorum);

        return new GlobalPartitionState(
                zoneDescriptor.id(),
                zoneDescriptor.name(),
                zonePartitionId.partitionId(),
                globalStateEnum
        );
    }

    private static GlobalPartitionStateEnum calculateGlobalState(int replicas, int healthyReplicas, int quorum) {
        if (healthyReplicas == replicas) {
            return AVAILABLE;
        } else if (healthyReplicas >= quorum) {
            return DEGRADED;
        } else if (healthyReplicas > 0) {
            return READ_ONLY;
        } else {
            return GlobalPartitionStateEnum.UNAVAILABLE;
        }
    }

    private static int calculateQuorum(int replicas) {
        return replicas / 2 + 1;
    }

    private Catalog catalogLatestVersion() {
        return catalogManager.latestCatalog();
    }

    private static CatalogZoneDescriptor zoneDescriptor(Catalog catalog, String zoneName) {
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneName);

        if (zoneDescriptor == null) {
            throw new DistributionZoneNotFoundException(zoneName);
        }

        return zoneDescriptor;
    }

    private static ByteArray zoneRecoveryTriggerRevisionKey(int zoneId) {
        return new ByteArray(RECOVERY_TRIGGER_REVISION_KEY_PREFIX + zoneId);
    }

    InternalClusterNode localNode() {
        return topologyService.localMember();
    }

    private void onTableCreate(CreateTableEventParameters parameters) {
        registerPartitionStatesMetricSource(parameters.tableDescriptor());
    }

    private void onTableDrop(DropTableEventParameters parameters) {
        unregisterPartitionStatesMetricSource(parameters.tableId());
    }

    private void registerMetricSources() {
        catalogLatestVersion().tables().forEach(this::registerPartitionStatesMetricSource);
    }

    private void registerPartitionStatesMetricSource(CatalogTableDescriptor tableDescriptor) {
        var metricSource = new PartitionStatesMetricSource(tableDescriptor, this);

        PartitionStatesMetricSource previous = metricSourceByTableId.putIfAbsent(tableDescriptor.id(), metricSource);

        assert previous == null : "tableId=" + tableDescriptor.id();

        metricManager.registerSource(metricSource);
        metricManager.enable(metricSource);
    }

    private void unregisterPartitionStatesMetricSource(int tableId) {
        PartitionStatesMetricSource metricSource = metricSourceByTableId.get(tableId);

        assert metricSource != null : "tableId=" + tableId;

        metricManager.unregisterSource(metricSource);
    }
}
