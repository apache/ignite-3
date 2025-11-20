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
import static java.util.Collections.emptyMap;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
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
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toZonePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createGlobalTablePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createGlobalZonePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createLocalTablePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createLocalZonePartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.DEGRADED;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.READ_ONLY;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogSchemaDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CreateTableEventParameters;
import org.apache.ignite.internal.catalog.events.DropTableEventParameters;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
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
import org.apache.ignite.internal.network.InternalClusterNode;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.RecipientLeftException;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.network.UnresolvableConsistentIdException;
import org.apache.ignite.internal.partition.replicator.PartitionReplicaLifecycleManager;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesRequest;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesResponse;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalTablePartitionStateMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalTablePartitionStateRequest;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalTablePartitionStateResponse;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.replicator.message.TablePartitionIdMessage;
import org.apache.ignite.internal.replicator.message.ZonePartitionIdMessage;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewManager;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalNodesException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalPartitionIdException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NotEnoughAliveNodesException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.ZonesNotFoundException;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.table.QualifiedNameHelper;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations,
 * such as {@link #resetTablePartitions(String, String, String, Set, boolean, long)}.
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

    /** Disaster recovery operations timeout in seconds. */
    private static final int TIMEOUT_SECONDS = 30;

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

    /** Watch listener for {@link #RECOVERY_TRIGGER_KEY}. */
    private final WatchListener watchListener;

    /** Table manager. */
    final TableManager tableManager;

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

    /** Constructor. */
    public DisasterRecoveryManager(
            ExecutorService threadPool,
            MessagingService messagingService,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            DistributionZoneManager dzManager,
            Loza raftManager,
            TopologyService topologyService,
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
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLock(busyLock, () -> {
            systemViewManager.register(this);

            messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

            metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

            dzManager.listen(HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED, this::onHaZoneTablePartitionTopologyReduce);

            catalogManager.listen(TABLE_CREATE, fromConsumer(this::onTableCreate));

            catalogManager.listen(TABLE_DROP, fromConsumer(this::onTableDrop));

            registerMetricSources();

            return nullCompletedFuture();
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        busyLock.block();

        metaStorageManager.unregisterWatch(watchListener);

        for (CompletableFuture<Void> future : ongoingOperationsById.values()) {
            future.completeExceptionally(new NodeStoppingException());
        }

        return nullCompletedFuture();
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return List.of(
                createGlobalZonePartitionStatesSystemView(this),
                createLocalZonePartitionStatesSystemView(this),
                createGlobalTablePartitionStatesSystemView(this),
                createLocalTablePartitionStatesSystemView(this)
        );
    }

    @TestOnly
    public Map<UUID, CompletableFuture<Void>> ongoingOperationsById() {
        return ongoingOperationsById;
    }

    public IgniteSpinBusyLock busyLock() {
        return busyLock;
    }

    private CompletableFuture<Boolean> onHaZoneTablePartitionTopologyReduce(HaZoneTopologyUpdateEventParams params) {
        return inBusyLock(busyLock, () -> {
            int zoneId = params.zoneId();
            long revision = params.causalityToken();
            long timestamp = metaStorageManager.timestampByRevisionLocally(revision).longValue();

            Catalog catalog = catalogManager.activeCatalog(timestamp);
            CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

            Map<Integer, Set<Integer>> tablePartitionsToReset = new HashMap<>();
            for (CatalogTableDescriptor table : catalog.tables(zoneId)) {
                Set<Integer> partitionsToReset = new HashSet<>();
                for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
                    TablePartitionId partitionId = new TablePartitionId(table.id(), partId);

                    if (stableAssignmentsWithOnlyAliveNodes(partitionId, revision).size() < calculateQuorum(
                            zoneDescriptor.replicas())) {
                        partitionsToReset.add(partId);
                    }
                }

                if (!partitionsToReset.isEmpty()) {
                    tablePartitionsToReset.put(table.id(), partitionsToReset);
                }
            }

            if (!tablePartitionsToReset.isEmpty()) {
                return resetPartitions(zoneDescriptor.name(), tablePartitionsToReset, false, revision, false).thenApply(r -> false);
            } else {
                return falseCompletedFuture();
            }
        });
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

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param schemaName Schema name. Case-sensitive, without quotes.
     * @param tableName Table name. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to reset. If empty, reset all zone's partitions.
     * @return Future that completes when partitions are reset.
     */
    public CompletableFuture<Void> resetTablePartitions(String zoneName, String schemaName, String tableName, Set<Integer> partitionIds) {
        return inBusyLock(busyLock, () -> {
            int tableId = tableDescriptor(catalogLatestVersion(), schemaName, tableName).id();

            return resetPartitions(zoneName, Map.of(tableId, partitionIds), true, -1, false);
        });
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param schemaName Schema name. Case-sensitive, without quotes.
     * @param tableName Table name. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to reset. If empty, reset all zone's partitions.
     * @param manualUpdate Whether the update is triggered manually by user or automatically by core logic.
     * @param triggerRevision Revision of the event, which produce this reset. -1 for manual reset.
     * @return Future that completes when partitions are reset.
     */
    public CompletableFuture<Void> resetTablePartitions(
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds,
            boolean manualUpdate,
            long triggerRevision
    ) {
        return inBusyLock(busyLock, () -> {
            int tableId = tableDescriptor(catalogLatestVersion(), schemaName, tableName).id();

            return resetPartitions(zoneName, Map.of(tableId, partitionIds), manualUpdate, triggerRevision, false);
        });
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
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

            return resetPartitions(zoneName, Map.of(zoneId, partitionIds), true, -1, true);
        });
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
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

            return resetPartitions(zoneName, Map.of(zoneId, partitionIds), manualUpdate, triggerRevision, true);
        });
    }

    /**
     * Updates assignments of the table or zone in a forced manner, allowing for the recovery of raft group with lost majorities. It is
     * achieved via triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required.
     * New pending assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via
     * "resetPeers" so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds Map of per zone or table partitions' sets to reset. If empty, reset all zone's partitions.
     * @param manualUpdate Whether the update is triggered manually by user or automatically by core logic.
     * @param triggerRevision Revision of the event, which produce this reset. -1 for manual reset.
     * @param colocationEnabled Whether the update is a zone request (enabled colocation) or a table request (colocation disabled).
     * @return Future that completes when partitions are reset.
     */
    private CompletableFuture<Void> resetPartitions(
            String zoneName,
            Map<Integer, Set<Integer>> partitionIds,
            boolean manualUpdate,
            long triggerRevision,
            boolean colocationEnabled
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
                                manualUpdate,
                                colocationEnabled
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
     * @param schemaName Schema name. Case-sensitive, without quotes.
     * @param tableName Table name. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to restart. If empty, restart all zone's partitions.
     * @return Future that completes when partitions are restarted.
     */
    public CompletableFuture<Void> restartTablePartitions(
            Set<String> nodeNames,
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                // Validates passed node names.
                getNodes(nodeNames);

                Catalog catalog = catalogLatestVersion();

                CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

                CatalogTableDescriptor table = tableDescriptor(catalog, schemaName, tableName);

                checkPartitionsRange(partitionIds, Set.of(zone));

                return processNewRequest(new ManualGroupRestartRequest(
                        UUID.randomUUID(),
                        zone.id(),
                        table.id(),
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        false
                ));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Restarts replica service and raft group of passed partitions with cleaning up partition storages.
     *
     * @param nodeNames Names specifying nodes to restart partitions. Only one node is allowed.
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param schemaName Schema name. Case-sensitive, without quotes.
     * @param tableName Table name. Case-sensitive, without quotes.
     * @param partitionIds IDs of partitions to restart. If empty, restart all zone's partitions.
     * @return Future that completes when partitions are restarted.
     */
    public CompletableFuture<Void> restartTablePartitionsWithCleanup(
            Set<String> nodeNames,
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds
    ) {
        return inBusyLock(busyLock, () -> {
            try {
                // Validates passed node names.
                getNodes(nodeNames);

                Catalog catalog = catalogLatestVersion();

                CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

                CatalogTableDescriptor table = tableDescriptor(catalog, schemaName, tableName);

                checkPartitionsRange(partitionIds, Set.of(zone));

                checkOnlyOneNodeSpecified(nodeNames);

                return processNewRequest(new ManualGroupRestartRequest(
                        UUID.randomUUID(),
                        zone.id(),
                        table.id(),
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        true
                ));
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
                        // We pass here -1 as table id because it is not used for zone-based partitions.
                        // We expect that the field will be removed once colocation track is finished.
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522
                        -1,
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        false
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
                        // We pass here -1 as table id because it is not used for zone-based partitions.
                        // We expect that the field will be removed once colocation track is finished.
                        // TODO: https://issues.apache.org/jira/browse/IGNITE-22522
                        -1,
                        partitionIds,
                        nodeNames,
                        catalog.time(),
                        true
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
            Collection<CatalogZoneDescriptor> zones = filterZones(zoneNames, catalog.zones());

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
                        TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
                );

                futures[i++] = invokeFuture.thenAccept(networkMessage -> {
                    inBusyLock(busyLock, () -> {
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
                    });
                });
            }

            return allOf(futures).handle((unused, err) -> {
                if (err != null) {
                    throw new DisasterRecoveryException(PARTITION_STATE_ERR, err);
                }

                return result;
            });
        });
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link TablePartitionId} to the mapping
     * between a node name and a partition state.
     *
     * @param zoneNames Names specifying zones to get partition states from. Case-sensitive, empty set means "all zones".
     * @param nodeNames Names specifying nodes to get partition states from. Case-sensitive, empty set means "all nodes".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<TablePartitionId, LocalTablePartitionStateByNode>> localTablePartitionStates(
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
                )
                        .thenCompose(res -> tableStateForZone(toZonesOnNodes(res), catalog.version())
                                .thenApply(tableState -> zoneStateToTableState(res, tableState, catalog))
                        )
                        .thenApply(res -> normalizeTableLocal(res, catalog));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link TablePartitionId} to the global partition state value.
     *
     * @param zoneNames Names specifying zones to get partition states. Case-sensitive, empty set means "all zones".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<TablePartitionId, GlobalTablePartitionState>> globalTablePartitionStates(
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
                        .thenCompose(res -> tableStateForZone(toZonesOnNodes(res), catalog.version())
                                .thenApply(tableState -> zoneStateToTableState(res, tableState, catalog))
                        )
                        .thenApply(res -> normalizeTableLocal(res, catalog))
                        .thenApply(res -> assembleTableGlobal(res, partitionIds, catalog));
            } catch (Throwable t) {
                return failedFuture(t);
            }
        });
    }

    /**
     * Converts {@link LocalPartitionStateMessageByNode} to a mapping of zone names to the set of zone partitions.
     *
     * @param partitionStateMap Partition state map.
     * @return Mapping of zone names to the set of zone partitions.
     */
    private static Map<String, Set<ZonePartitionId>> toZonesOnNodes(
            Map<ZonePartitionId, LocalPartitionStateMessageByNode> partitionStateMap
    ) {
        Map<String, Set<ZonePartitionId>> res = new HashMap<>();

        for (Map.Entry<ZonePartitionId, LocalPartitionStateMessageByNode> entry : partitionStateMap.entrySet()) {
            ZonePartitionId zonePartitionId = entry.getKey();

            LocalPartitionStateMessageByNode zoneLocalPartitionStateMessageByNode = entry.getValue();

            for (String nodeName : zoneLocalPartitionStateMessageByNode.nodes()) {
                res.computeIfAbsent(nodeName, k -> new HashSet<>()).add(zonePartitionId);
            }
        }

        return res;
    }

    /**
     * Returns estimated number of rows for each table having a partition in the specified zones.
     *
     * <p>The result is returned from the nodes specified in the {@code zonesOnNodes.keySet()} -
     * these are the nodes we previously received partition states from.
     *
     * @param zonesOnNodes Mapping of node names to the set of zone partitions.
     * @param catalogVersion Catalog version.
     * @return Future with the mapping.
     */
    private CompletableFuture<Map<String, Map<TablePartitionIdMessage, Long>>> tableStateForZone(
            Map<String, Set<ZonePartitionId>> zonesOnNodes,
            int catalogVersion
    ) {
        Map<String, Map<TablePartitionIdMessage, Long>> result = new ConcurrentHashMap<>();

        CompletableFuture<?>[] futures = zonesOnNodes.entrySet().stream()
                .map(entry ->
                        tableStateForZoneOnNode(catalogVersion, entry.getKey(), entry.getValue())
                                .thenAccept(response ->
                                        response.states().forEach(state -> {
                                            result.computeIfAbsent(entry.getKey(), k -> new ConcurrentHashMap<>())
                                                    .putAll(state.tablePartitionIdToEstimatedRowsMap());
                                        })
                                )
                ).toArray(CompletableFuture[]::new);

        return allOf(futures).handle((unused, err) -> {
            if (err != null) {
                throw new DisasterRecoveryException(PARTITION_STATE_ERR, err);
            }

            return result;
        });
    }

    /**
     * Returns estimated number of rows for each table having a partition in the specified zones.
     *
     * @param catalogVersion Catalog version.
     * @param node Node we get table partition states from.
     * @param zones Set of zone partitions.
     * @return Future with the mapping.
     */
    private CompletableFuture<LocalTablePartitionStateResponse> tableStateForZoneOnNode(
            int catalogVersion,
            String node,
            Set<ZonePartitionId> zones
    ) {
        Set<ZonePartitionIdMessage> zoneMessage = zones.stream()
                .map(zonePartitionId -> toZonePartitionIdMessage(REPLICA_MESSAGES_FACTORY, zonePartitionId))
                .collect(toSet());
        LocalTablePartitionStateRequest request = PARTITION_REPLICATION_MESSAGES_FACTORY.localTablePartitionStateRequest()
                .zonePartitionIds(zoneMessage)
                .catalogVersion(catalogVersion)
                .build();

        return messagingService.invoke(node, request, TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS))
                .thenApply(networkMessage -> {
                    assert networkMessage instanceof LocalTablePartitionStateResponse : networkMessage;

                    return (LocalTablePartitionStateResponse) networkMessage;
                });
    }

    private static Map<TablePartitionId, LocalPartitionStateMessageByNode> zoneStateToTableState(
            Map<ZonePartitionId, LocalPartitionStateMessageByNode> partitionStateMap,
            Map<String, Map<TablePartitionIdMessage, Long>> tableState,
            Catalog catalog
    ) {
        Map<TablePartitionId, LocalPartitionStateMessageByNode> res = new HashMap<>();

        for (Map.Entry<ZonePartitionId, LocalPartitionStateMessageByNode> entry : partitionStateMap.entrySet()) {
            ZonePartitionId zonePartitionId = entry.getKey();

            int zoneId = zonePartitionId.zoneId();

            int partitionId = zonePartitionId.partitionId();

            LocalPartitionStateMessageByNode zoneLocalPartitionStateMessageByNode = entry.getValue();

            LocalPartitionStateMessageByNode tableLocalPartitionStateMessageByNode = new LocalPartitionStateMessageByNode(new HashMap<>());

            for (CatalogTableDescriptor tableDescriptor : catalog.tables(zoneId)) {
                TablePartitionId tablePartitionId = new TablePartitionId(tableDescriptor.id(), partitionId);

                TablePartitionIdMessage tablePartitionIdMessage =
                        toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartitionId);

                for (Map.Entry<String, LocalPartitionStateMessage> nodeEntry : zoneLocalPartitionStateMessageByNode.entrySet()) {
                    String nodeName = nodeEntry.getKey();

                    Long estimatedRows = tableState.getOrDefault(nodeName, emptyMap())
                            .get(tablePartitionIdMessage);

                    if (estimatedRows == null) {
                        continue;
                    }

                    LocalPartitionStateMessage localPartitionStateMessage = nodeEntry.getValue();

                    LocalPartitionStateMessage tableLocalPartitionStateMessage =
                            PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStateMessage()
                                    .partitionId(tablePartitionIdMessage)
                                    .state(localPartitionStateMessage.state())
                                    .logIndex(localPartitionStateMessage.logIndex())
                                    .estimatedRows(estimatedRows)
                                    .isLearner(localPartitionStateMessage.isLearner())
                                    .build();

                    tableLocalPartitionStateMessageByNode.put(nodeName, tableLocalPartitionStateMessage);
                }

                if (!tableLocalPartitionStateMessageByNode.values().isEmpty()) {
                    res.put(tablePartitionId, tableLocalPartitionStateMessageByNode);
                }
            }
        }

        return res;
    }

    static Function<LocalPartitionStateMessage, TablePartitionId> tableState() {
        return state -> state.partitionId().asTablePartitionId();
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

    private static Collection<CatalogZoneDescriptor> filterZones(Set<String> zoneNames, Collection<CatalogZoneDescriptor> zones)
            throws ZonesNotFoundException {
        if (zoneNames.isEmpty()) {
            return zones;
        }

        List<CatalogZoneDescriptor> zoneDescriptors = zones.stream()
                .filter(catalogZoneDescriptor -> zoneNames.contains(catalogZoneDescriptor.name()))
                .collect(toList());

        Set<String> foundZoneNames = zoneDescriptors.stream()
                .map(CatalogObjectDescriptor::name)
                .collect(toSet());

        if (!zoneNames.equals(foundZoneNames)) {
            Set<String> missingZoneNames = CollectionUtils.difference(zoneNames, foundZoneNames);

            throw new ZonesNotFoundException(missingZoneNames);
        }

        return zoneDescriptors;
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
        UUID operationId = request.operationId();

        CompletableFuture<Void> operationFuture = new CompletableFuture<Void>().orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);

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

        return operationFuture;
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
                var error = new AssertionError("Unexpected request type: " + request.getClass());

                if (operationFuture != null) {
                    operationFuture.completeExceptionally(error);
                }
        }
    }

    private void handleMessage(NetworkMessage message, InternalClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof LocalPartitionStatesRequest) {
            handleLocalPartitionStatesRequest((LocalPartitionStatesRequest) message, sender, correlationId);
        } else if (message instanceof LocalTablePartitionStateRequest) {
            handleLocalTableStateRequest((LocalTablePartitionStateRequest) message, sender, correlationId);
        }
    }

    private void handleLocalTableStateRequest(
            LocalTablePartitionStateRequest request,
            InternalClusterNode sender,
            @Nullable Long correlationId
    ) {
        assert correlationId != null : "request=" + request + ", sender=" + sender;

        int catalogVersion = request.catalogVersion();

        Set<ZonePartitionId> requestedPartitions = request.zonePartitionIds().stream()
                .map(ZonePartitionIdMessage::asZonePartitionId)
                .collect(toSet());

        catalogManager.catalogReadyFuture(catalogVersion).thenRunAsync(() -> {
            Set<LocalTablePartitionStateMessage> statesList = new HashSet<>();

            raftManager.forEach((raftNodeId, raftGroupService) -> {
                if (raftNodeId.groupId() instanceof ZonePartitionId) {

                    LocalTablePartitionStateMessage message = handleSizeRequestForTablesInZone(
                            requestedPartitions,
                            (ZonePartitionId) raftNodeId.groupId()
                    );

                    if (message != null) {
                        statesList.add(message);
                    }
                }
            });

            LocalTablePartitionStateResponse response = PARTITION_REPLICATION_MESSAGES_FACTORY.localTablePartitionStateResponse()
                    .states(statesList)
                    .build();

            messagingService.respond(sender, response, correlationId);
        }, threadPool);
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
                if (raftNodeId.groupId() instanceof TablePartitionId) {
                    LocalPartitionStateMessage message = handleStateRequestForTable(
                            request,
                            raftGroupService,
                            (TablePartitionId) raftNodeId.groupId(),
                            catalogVersion
                    );

                    if (message != null) {
                        statesList.add(message);
                    }
                } else if (raftNodeId.groupId() instanceof ZonePartitionId) {
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

            messagingService.respond(sender, response, correlationId);
        }, threadPool);
    }

    private @Nullable LocalTablePartitionStateMessage handleSizeRequestForTablesInZone(
            Set<ZonePartitionId> requestedPartitions,
            ZonePartitionId zonePartitionId
    ) {
        if (!containsOrEmpty(zonePartitionId, requestedPartitions)) {
            return null;
        }

        return PARTITION_REPLICATION_MESSAGES_FACTORY.localTablePartitionStateMessage()
                .tablePartitionIdToEstimatedRowsMap(estimatedSizeMap(zonePartitionId))
                .build();
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
                .map(tableImpl -> tableImpl.internalTable().storage().getMvPartition(zonePartitionId.partitionId()))
                .filter(Objects::nonNull)
                .mapToLong(MvPartitionStorage::estimatedSize)
                .sum();
    }

    private Map<TablePartitionIdMessage, Long> estimatedSizeMap(ZonePartitionId zonePartitionId) {
        Map<TablePartitionIdMessage, Long> partitionIdToEstimatedRowsMap = new HashMap<>();

        for (TableViewInternal tableImpl : tableManager.zoneTables(zonePartitionId.zoneId())) {
            MvPartitionStorage mvPartitionStorage = tableImpl.internalTable().storage().getMvPartition(zonePartitionId.partitionId());

            if (mvPartitionStorage != null) {
                partitionIdToEstimatedRowsMap.put(
                        toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY,
                                new TablePartitionId(tableImpl.tableId(), zonePartitionId.partitionId())),
                        mvPartitionStorage.estimatedSize()
                );
            }
        }

        return partitionIdToEstimatedRowsMap;
    }

    private @Nullable LocalPartitionStateMessage handleStateRequestForTable(
            LocalPartitionStatesRequest request,
            RaftGroupService raftGroupService,
            TablePartitionId tablePartitionId,
            int catalogVersion
    ) {
        if (!containsOrEmpty(tablePartitionId.partitionId(), request.partitionIds())) {
            return null;
        }

        Catalog catalog = catalogManager.catalog(catalogVersion);
        assert catalog != null : "Catalog is not found for version: " + catalogVersion;

        CatalogTableDescriptor tableDescriptor = catalog.table(tablePartitionId.tableId());
        // Only tables that belong to a specific catalog version will be returned.
        if (tableDescriptor == null || !containsOrEmpty(tableDescriptor.zoneId(), request.zoneIds())) {
            return null;
        }

        // Since the raft service starts after registering a new table, we don't need to wait or write additional asynchronous
        // code.
        TableViewInternal tableViewInternal = tableManager.cachedTable(tablePartitionId.tableId());
        // Perhaps the table began to be stopped or destroyed.
        if (tableViewInternal == null) {
            return null;
        }

        MvPartitionStorage partitionStorage = tableViewInternal.internalTable().storage()
                .getMvPartition(tablePartitionId.partitionId());
        // Perhaps the partition began to be stopped or destroyed.
        if (partitionStorage == null) {
            return null;
        }

        Node raftNode = raftGroupService.getRaftNode();

        LocalPartitionStateEnumWithLogIndex localPartitionStateWithLogIndex =
                LocalPartitionStateEnumWithLogIndex.of(raftNode);

        return PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStateMessage()
                .partitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartitionId))
                .state(localPartitionStateWithLogIndex.state)
                .logIndex(localPartitionStateWithLogIndex.logIndex)
                .estimatedRows(partitionStorage.estimatedSize())
                .isLearner(raftNode.isLearner())
                .build();
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

    /**
     * Replaces some healthy states with a {@link LocalPartitionStateEnum#CATCHING_UP}, it can only be done once the state of all peers is
     * known.
     */
    private static Map<TablePartitionId, LocalTablePartitionStateByNode> normalizeTableLocal(
            Map<TablePartitionId, LocalPartitionStateMessageByNode> result,
            Catalog catalog
    ) {
        Map<TablePartitionId, LocalTablePartitionStateByNode> map = new HashMap<>();

        for (Map.Entry<TablePartitionId, LocalPartitionStateMessageByNode> entry : result.entrySet()) {
            TablePartitionId tablePartitionId = entry.getKey();
            LocalPartitionStateMessageByNode messageByNode = entry.getValue();

            // noinspection OptionalGetWithoutIsPresent
            long maxLogIndex = messageByNode.values().stream()
                    .mapToLong(LocalPartitionStateMessage::logIndex)
                    .max()
                    .getAsLong();

            Map<String, LocalTablePartitionState> nodeToStateMap = messageByNode.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, nodeToState ->
                            toLocalTablePartitionState(nodeToState, maxLogIndex, tablePartitionId, catalog))
                    );

            map.put(tablePartitionId, new LocalTablePartitionStateByNode(nodeToStateMap));
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

    private static LocalTablePartitionState toLocalTablePartitionState(
            Map.Entry<String, LocalPartitionStateMessage> nodeToMessage,
            long maxLogIndex,
            TablePartitionId tablePartitionId,
            Catalog catalog
    ) {
        LocalPartitionStateMessage stateMsg = nodeToMessage.getValue();

        LocalPartitionStateEnum stateEnum = calculateState(stateMsg, maxLogIndex);

        // Tables, returned from local states request, are always present in the required version of the catalog.
        CatalogTableDescriptor tableDescriptor = catalog.table(tablePartitionId.tableId());

        String zoneName = catalog.zone(tableDescriptor.zoneId()).name();
        String schemaName = catalog.schema(tableDescriptor.schemaId()).name();

        return new LocalTablePartitionState(
                tableDescriptor.zoneId(),
                zoneName,
                tableDescriptor.schemaId(),
                schemaName,
                tableDescriptor.id(),
                tableDescriptor.name(),
                tablePartitionId.partitionId(),
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

    private static Map<TablePartitionId, GlobalTablePartitionState> assembleTableGlobal(
            Map<TablePartitionId, LocalTablePartitionStateByNode> localResult,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Map<TablePartitionId, GlobalTablePartitionState> result = localResult.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> {
                    TablePartitionId tablePartitionId = entry.getKey();
                    LocalTablePartitionStateByNode map = entry.getValue();

                    return assembleTableGlobalStateFromLocal(catalog, tablePartitionId, map);
                }));

        makeMissingTablePartitionsUnavailable(localResult, catalog, result, partitionIds);

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

    private static void makeMissingTablePartitionsUnavailable(
            Map<TablePartitionId, LocalTablePartitionStateByNode> localResult,
            Catalog catalog,
            Map<TablePartitionId, GlobalTablePartitionState> result,
            Set<Integer> partitionIds
    ) {
        localResult.keySet().stream()
                .map(TablePartitionId::tableId)
                .distinct()
                .forEach(tableId -> {
                    CatalogTableDescriptor table = catalog.table(tableId);

                    CatalogZoneDescriptor zoneDescriptor = catalog.zone(table.zoneId());
                    CatalogSchemaDescriptor schemaDescriptor = catalog.schema(table.schemaId());

                    if (partitionIds.isEmpty()) {
                        int partitions = zoneDescriptor.partitions();

                        for (int partitionId = 0; partitionId < partitions; partitionId++) {
                            putUnavailableTableStateIfAbsent(catalog, result, tableId, partitionId, schemaDescriptor, zoneDescriptor);
                        }
                    } else {
                        partitionIds.forEach(partitionId -> {
                            putUnavailableTableStateIfAbsent(catalog, result, tableId, partitionId, schemaDescriptor, zoneDescriptor);
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

    private static void putUnavailableTableStateIfAbsent(
            Catalog catalog,
            Map<TablePartitionId, GlobalTablePartitionState> states,
            Integer tableId,
            int partitionId,
            CatalogSchemaDescriptor schemaDescriptor,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        states.computeIfAbsent(tablePartitionId, key ->
                new GlobalTablePartitionState(
                        zoneDescriptor.id(),
                        zoneDescriptor.name(),
                        schemaDescriptor.id(), schemaDescriptor.name(),
                        key.tableId(),
                        catalog.table(key.tableId()).name(),
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

    private static GlobalTablePartitionState assembleTableGlobalStateFromLocal(
            Catalog catalog,
            TablePartitionId tablePartitionId,
            LocalTablePartitionStateByNode map
    ) {
        // Tables, returned from local states request, are always present in the required version of the catalog.
        CatalogTableDescriptor table = catalog.table(tablePartitionId.tableId());

        CatalogSchemaDescriptor schemaDescriptor = catalog.schema(table.schemaId());
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(table.zoneId());

        int replicas = zoneDescriptor.replicas();
        int quorum = calculateQuorum(replicas);

        Map<LocalPartitionStateEnum, List<LocalTablePartitionState>> groupedStates = map.values().stream()
                .collect(groupingBy(localPartitionState -> localPartitionState.state));

        int healthyReplicas = groupedStates.getOrDefault(HEALTHY, emptyList()).size();

        GlobalPartitionStateEnum globalStateEnum = calculateGlobalState(replicas, healthyReplicas, quorum);

        LocalTablePartitionState anyLocalState = map.values().iterator().next();

        return new GlobalTablePartitionState(
                zoneDescriptor.id(),
                zoneDescriptor.name(),
                schemaDescriptor.id(),
                schemaDescriptor.name(),
                anyLocalState.tableId,
                anyLocalState.tableName,
                tablePartitionId.partitionId(),
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
        int catalogVersion = catalogManager.latestCatalogVersion();

        Catalog catalog = catalogManager.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog;
    }

    private static CatalogTableDescriptor tableDescriptor(Catalog catalog, String schemaName, String tableName) {
        CatalogTableDescriptor tableDescriptor = catalog.table(schemaName, tableName);

        if (tableDescriptor == null) {
            throw new TableNotFoundException(QualifiedNameHelper.fromNormalized(schemaName, tableName));
        }

        return tableDescriptor;
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
        int catalogVersion = catalogManager.latestCatalogVersion();
        Catalog catalog = catalogManager.catalog(catalogVersion);

        assert catalog != null : "Catalog is not found for version: " + catalogVersion;

        catalog.tables().forEach(this::registerPartitionStatesMetricSource);
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
