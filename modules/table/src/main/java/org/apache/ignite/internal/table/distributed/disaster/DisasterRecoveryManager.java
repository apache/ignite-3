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
import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.TABLE_DROP;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.findTablesByZoneId;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.stablePartAssignmentsKey;
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.CATCHING_UP;
import static org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.replicator.message.ReplicaMessageUtils.toTablePartitionIdMessage;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createGlobalPartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.DisasterRecoverySystemViews.createLocalPartitionStatesSystemView;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.DEGRADED;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.READ_ONLY;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.CompletableFutures.copyStateTo;
import static org.apache.ignite.internal.util.CompletableFutures.falseCompletedFuture;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.PARTITION_STATE_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.network.TopologyService;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessageGroup;
import org.apache.ignite.internal.partition.replicator.network.PartitionReplicationMessagesFactory;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateEnum;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStateMessage;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesRequest;
import org.apache.ignite.internal.partition.replicator.network.disaster.LocalPartitionStatesResponse;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.partitiondistribution.Assignments;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.message.ReplicaMessagesFactory;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.systemview.api.SystemView;
import org.apache.ignite.internal.systemview.api.SystemViewProvider;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalPartitionIdException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.ZonesNotFoundException;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.internal.versioned.VersionedSerialization;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.table.QualifiedNameHelper;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations,
 * such as {@link #resetPartitions(String, String, String, Set, boolean, long)}.
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

    /** Metric manager. */
    private final MetricManager metricManager;

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
            MetricManager metricManager
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

        watchListener = event -> {
            handleTriggerKeyUpdate(event);

            // There is no need to block a watch thread any longer.
            return nullCompletedFuture();
        };
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        messagingService.addMessageHandler(PartitionReplicationMessageGroup.class, this::handleMessage);

        metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

        dzManager.listen(HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED, this::onHaZoneTopologyReduce);

        catalogManager.listen(TABLE_CREATE, fromConsumer(this::onTableCreate));

        catalogManager.listen(TABLE_DROP, fromConsumer(this::onTableDrop));

        registerMetricSources();

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        metaStorageManager.unregisterWatch(watchListener);

        for (CompletableFuture<Void> future : ongoingOperationsById.values()) {
            future.completeExceptionally(new NodeStoppingException());
        }

        return nullCompletedFuture();
    }

    @Override
    public List<SystemView<?>> systemViews() {
        return List.of(
                createGlobalPartitionStatesSystemView(this),
                createLocalPartitionStatesSystemView(this)
        );
    }

    @TestOnly
    public Map<UUID, CompletableFuture<Void>> ongoingOperationsById() {
        return ongoingOperationsById;
    }

    private CompletableFuture<Boolean> onHaZoneTopologyReduce(HaZoneTopologyUpdateEventParams params) {
        int zoneId = params.zoneId();
        long revision = params.causalityToken();
        long timestamp = metaStorageManager.timestampByRevisionLocally(revision).longValue();

        CatalogZoneDescriptor zoneDescriptor = catalogManager.zone(zoneId, timestamp);
        int catalogVersion = catalogManager.activeCatalogVersion(timestamp);

        List<CatalogTableDescriptor> tables = findTablesByZoneId(zoneId, catalogVersion, catalogManager);
        Map<Integer, Set<Integer>> tablePartitionsToReset = new HashMap<>();
        for (CatalogTableDescriptor table : tables) {
            Set<Integer> partitionsToReset = new HashSet<>();
            for (int partId = 0; partId < zoneDescriptor.partitions(); partId++) {
                TablePartitionId partitionId = new TablePartitionId(table.id(), partId);

                if (stableAssignmentsWithOnlyAliveNodes(partitionId, revision).size() < (zoneDescriptor.replicas() / 2 + 1)) {
                    partitionsToReset.add(partId);
                }
            }

            if (!partitionsToReset.isEmpty()) {
                tablePartitionsToReset.put(table.id(), partitionsToReset);
            }
        }

        if (!tablePartitionsToReset.isEmpty()) {
            return resetPartitions(zoneDescriptor.name(), tablePartitionsToReset, false, revision).thenApply(r -> false);
        } else {
            return falseCompletedFuture();
        }
    }

    private Set<Assignment> stableAssignmentsWithOnlyAliveNodes(TablePartitionId partitionId, long revision) {
        Set<Assignment> stableAssignments = Assignments.fromBytes(
                metaStorageManager.getLocally(stablePartAssignmentsKey(partitionId), revision).value()).nodes();

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
     * @param manualUpdate Whether the update is triggered manually by user or automatically by core logic.
     * @param triggerRevision Revision of the event, which produce this reset. -1 for manual reset.
     * @return Future that completes when partitions are reset.
     */
    public CompletableFuture<Void> resetAllPartitions(
            String zoneName,
            String schemaName,
            String tableName,
            boolean manualUpdate,
            long triggerRevision
    ) {
        return resetPartitions(zoneName, schemaName, tableName, emptySet(), manualUpdate, triggerRevision);
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
    public CompletableFuture<Void> resetPartitions(String zoneName, String schemaName, String tableName, Set<Integer> partitionIds) {
        int tableId = tableDescriptor(catalogLatestVersion(), schemaName, tableName).id();

        return resetPartitions(zoneName, Map.of(tableId, partitionIds), true, -1);
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
    private CompletableFuture<Void> resetPartitions(
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds,
            boolean manualUpdate,
            long triggerRevision
    ) {
        int tableId = tableDescriptor(catalogLatestVersion(), schemaName, tableName).id();

        return resetPartitions(zoneName, Map.of(tableId, partitionIds), manualUpdate, triggerRevision);
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param partitionIds Map of per table partitions' sets to reset. If empty, reset all zone's partitions.
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
        try {
            Catalog catalog = catalogLatestVersion();

            CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

            partitionIds.values().forEach(ids -> checkPartitionsRange(ids, Set.of(zone)));

            return processNewRequest(
                    new GroupUpdateRequest(UUID.randomUUID(), catalog.version(), zone.id(), partitionIds, manualUpdate),
                    triggerRevision
            );
        } catch (Throwable t) {
            return failedFuture(t);
        }
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
    public CompletableFuture<Void> restartPartitions(
            Set<String> nodeNames,
            String zoneName,
            String schemaName,
            String tableName,
            Set<Integer> partitionIds
    ) {
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
                    catalog.time()
            ));
        } catch (Throwable t) {
            return failedFuture(t);
        }
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
    public CompletableFuture<Map<TablePartitionId, LocalPartitionStateByNode>> localPartitionStates(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds
    ) {
        try {
            Catalog catalog = catalogLatestVersion();

            return localPartitionStatesInternal(zoneNames, nodeNames, partitionIds, catalog)
                    .thenApply(res -> normalizeLocal(res, catalog));
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link TablePartitionId} to the global
     * partition state enum value.
     *
     * @param zoneNames Names specifying zones to get partition states. Case-sensitive, empty set means "all zones".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<TablePartitionId, GlobalPartitionState>> globalPartitionStates(
            Set<String> zoneNames,
            Set<Integer> partitionIds
    ) {
        try {
            Catalog catalog = catalogLatestVersion();

            return localPartitionStatesInternal(zoneNames, Set.of(), partitionIds, catalog)
                    .thenApply(res -> normalizeLocal(res, catalog))
                    .thenApply(res -> assembleGlobal(res, partitionIds, catalog));
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    CompletableFuture<Map<TablePartitionId, LocalPartitionStateMessageByNode>> localPartitionStatesInternal(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Collection<CatalogZoneDescriptor> zones = filterZones(zoneNames, catalog.zones());

        checkPartitionsRange(partitionIds, zones);

        Set<NodeWithAttributes> nodes = getNodes(nodeNames);

        Set<Integer> zoneIds = zones.stream().map(CatalogObjectDescriptor::id).collect(toSet());

        LocalPartitionStatesRequest localPartitionStatesRequest = PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStatesRequest()
                .zoneIds(zoneIds)
                .partitionIds(partitionIds)
                .catalogVersion(catalog.version())
                .build();

        Map<TablePartitionId, LocalPartitionStateMessageByNode> result = new ConcurrentHashMap<>();
        CompletableFuture<?>[] futures = new CompletableFuture[nodes.size()];

        int i = 0;
        for (NodeWithAttributes node : nodes) {
            CompletableFuture<NetworkMessage> invokeFuture = messagingService.invoke(
                    node.nodeName(),
                    localPartitionStatesRequest,
                    TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS)
            );

            futures[i++] = invokeFuture.thenAccept(networkMessage -> {
                assert networkMessage instanceof LocalPartitionStatesResponse : networkMessage;

                var response = (LocalPartitionStatesResponse) networkMessage;

                for (LocalPartitionStateMessage state : response.states()) {
                    result.compute(state.partitionId().asTablePartitionId(), (tablePartitionId, messageByNode) -> {
                        if (messageByNode == null) {
                            return new LocalPartitionStateMessageByNode(Map.of(node.nodeName(), state));
                        }

                        messageByNode = new LocalPartitionStateMessageByNode(messageByNode);
                        messageByNode.put(node.nodeName(), state);
                        return messageByNode;
                    });
                }
            });
        }

        return allOf(futures).handle((unused, err) -> {
            if (err != null) {
                throw new DisasterRecoveryException(PARTITION_STATE_ERR, err);
            }

            return result;
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

        CompletableFuture<Void> operationFuture = new CompletableFuture<Void>()
                .whenComplete((v, throwable) -> ongoingOperationsById.remove(operationId))
                .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);

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
            LOG.warn("Unable to deserialize disaster recovery request.", e);

            return;
        }

        CompletableFuture<Void> operationFuture = ongoingOperationsById.remove(request.operationId());

        switch (request.type()) {
            case SINGLE_NODE:
                if (operationFuture == null) {
                    // We're not the initiator, or timeout has passed. Just ignore it.
                    return;
                }

                request.handle(this, watchEvent.revision(), watchEvent.timestamp()).whenComplete(copyStateTo(operationFuture));

                break;
            case MULTI_NODE:
                CompletableFuture<Void> handleFuture = request.handle(this, watchEvent.revision(), watchEvent.timestamp());

                if (operationFuture == null) {
                    // We're not the initiator, or timeout has passed.
                    return;
                }

                handleFuture.whenComplete(copyStateTo(operationFuture));

                break;
            default:
                var error = new AssertionError("Unexpected request type: " + request.getClass());

                if (operationFuture != null) {
                    operationFuture.completeExceptionally(error);
                }
        }
    }

    private void handleMessage(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof LocalPartitionStatesRequest) {
            handleLocalPartitionStatesRequest((LocalPartitionStatesRequest) message, sender, correlationId);
        }
    }

    private void handleLocalPartitionStatesRequest(LocalPartitionStatesRequest request, ClusterNode sender, @Nullable Long correlationId) {
        assert correlationId != null : "request=" + request + ", sender=" + sender;

        int catalogVersion = request.catalogVersion();

        catalogManager.catalogReadyFuture(catalogVersion).thenRunAsync(() -> {
            List<LocalPartitionStateMessage> statesList = new ArrayList<>();

            raftManager.forEach((raftNodeId, raftGroupService) -> {
                if (raftNodeId.groupId() instanceof TablePartitionId) {
                    var tablePartitionId = (TablePartitionId) raftNodeId.groupId();

                    if (!containsOrEmpty(tablePartitionId.partitionId(), request.partitionIds())) {
                        return;
                    }

                    CatalogTableDescriptor tableDescriptor = catalogManager.table(tablePartitionId.tableId(), catalogVersion);
                    // Only tables that belong to a specific catalog version will be returned.
                    if (tableDescriptor == null || !containsOrEmpty(tableDescriptor.zoneId(), request.zoneIds())) {
                        return;
                    }

                    // Since the raft service starts after registering a new table, we don't need to wait or write additional asynchronous
                    // code.
                    TableViewInternal tableViewInternal = tableManager.cachedTable(tablePartitionId.tableId());
                    // Perhaps the table began to be stopped or destroyed.
                    if (tableViewInternal == null) {
                        return;
                    }

                    MvPartitionStorage partitionStorage = tableViewInternal.internalTable().storage()
                            .getMvPartition(tablePartitionId.partitionId());
                    // Perhaps the partition began to be stopped or destroyed.
                    if (partitionStorage == null) {
                        return;
                    }

                    LocalPartitionStateEnumWithLogIndex localPartitionStateWithLogIndex =
                            LocalPartitionStateEnumWithLogIndex.of(raftGroupService.getRaftNode());

                    statesList.add(PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStateMessage()
                            .partitionId(toTablePartitionIdMessage(REPLICA_MESSAGES_FACTORY, tablePartitionId))
                            .state(localPartitionStateWithLogIndex.state)
                            .logIndex(localPartitionStateWithLogIndex.logIndex)
                            .estimatedRows(partitionStorage.estimatedSize())
                            .build()
                    );
                }
            });

            LocalPartitionStatesResponse response = PARTITION_REPLICATION_MESSAGES_FACTORY.localPartitionStatesResponse()
                    .states(statesList)
                    .build();

            messagingService.respond(sender, response, correlationId);
        }, threadPool);
    }

    private static <T> boolean containsOrEmpty(T item, Collection<T> collection) {
        return collection.isEmpty() || collection.contains(item);
    }

    /**
     * Replaces some healthy states with a {@link LocalPartitionStateEnum#CATCHING_UP}, it can only be done once the state of all peers is
     * known.
     */
    private static Map<TablePartitionId, LocalPartitionStateByNode> normalizeLocal(
            Map<TablePartitionId, LocalPartitionStateMessageByNode> result,
            Catalog catalog
    ) {
        Map<TablePartitionId, LocalPartitionStateByNode> map = new HashMap<>();

        for (Map.Entry<TablePartitionId, LocalPartitionStateMessageByNode> entry : result.entrySet()) {
            TablePartitionId tablePartitionId = entry.getKey();
            LocalPartitionStateMessageByNode messageByNode = entry.getValue();

            // noinspection OptionalGetWithoutIsPresent
            long maxLogIndex = messageByNode.values().stream()
                    .mapToLong(LocalPartitionStateMessage::logIndex)
                    .max()
                    .getAsLong();

            Map<String, LocalPartitionState> nodeToStateMap = messageByNode.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, nodeToState ->
                            toLocalPartitionState(nodeToState, maxLogIndex, tablePartitionId, catalog))
                    );

            map.put(tablePartitionId, new LocalPartitionStateByNode(nodeToStateMap));
        }

        return map;
    }

    private static LocalPartitionState toLocalPartitionState(
            Map.Entry<String, LocalPartitionStateMessage> nodeToMessage,
            long maxLogIndex,
            TablePartitionId tablePartitionId,
            Catalog catalog
    ) {
        LocalPartitionStateMessage stateMsg = nodeToMessage.getValue();

        LocalPartitionStateEnum stateEnum = stateMsg.state();

        if (stateEnum == HEALTHY && maxLogIndex - stateMsg.logIndex() >= CATCH_UP_THRESHOLD) {
            stateEnum = CATCHING_UP;
        }

        // Tables, returned from local states request, are always present in the required version of the catalog.
        CatalogTableDescriptor tableDescriptor = catalog.table(tablePartitionId.tableId());

        String zoneName = catalog.zone(tableDescriptor.zoneId()).name();
        String schemaName = catalog.schema(tableDescriptor.schemaId()).name();

        return new LocalPartitionState(
                zoneName,
                schemaName,
                tableDescriptor.id(),
                tableDescriptor.name(),
                tablePartitionId.partitionId(),
                stateEnum,
                stateMsg.estimatedRows()
        );
    }

    private static Map<TablePartitionId, GlobalPartitionState> assembleGlobal(
            Map<TablePartitionId, LocalPartitionStateByNode> localResult,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Map<TablePartitionId, GlobalPartitionState> result = localResult.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> {
                    TablePartitionId tablePartitionId = entry.getKey();
                    LocalPartitionStateByNode map = entry.getValue();

                    return assembleGlobalStateFromLocal(catalog, tablePartitionId, map);
                }));

        makeMissingPartitionsUnavailable(localResult, catalog, result, partitionIds);

        return result;
    }

    private static void makeMissingPartitionsUnavailable(Map<TablePartitionId, LocalPartitionStateByNode> localResult, Catalog catalog,
            Map<TablePartitionId, GlobalPartitionState> result, Set<Integer> partitionIds) {
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
                            putUnavailableStateIfAbsent(catalog, result, tableId, partitionId, schemaDescriptor, zoneDescriptor);
                        }
                    } else {
                        partitionIds.forEach(partitionId -> {
                            putUnavailableStateIfAbsent(catalog, result, tableId, partitionId, schemaDescriptor, zoneDescriptor);
                        });
                    }
                });
    }

    private static void putUnavailableStateIfAbsent(
            Catalog catalog,
            Map<TablePartitionId, GlobalPartitionState> states,
            Integer tableId,
            int partitionId,
            CatalogSchemaDescriptor schemaDescriptor,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        states.computeIfAbsent(tablePartitionId, key ->
                new GlobalPartitionState(
                        zoneDescriptor.name(),
                        schemaDescriptor.name(),
                        key.tableId(),
                        catalog.table(key.tableId()).name(),
                        key.partitionId(),
                        GlobalPartitionStateEnum.UNAVAILABLE
                )
        );
    }

    private static GlobalPartitionState assembleGlobalStateFromLocal(
            Catalog catalog,
            TablePartitionId tablePartitionId,
            LocalPartitionStateByNode map
    ) {
        // Tables, returned from local states request, are always present in the required version of the catalog.
        CatalogTableDescriptor table = catalog.table(tablePartitionId.tableId());

        CatalogSchemaDescriptor schemaDescriptor = catalog.schema(table.schemaId());
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(table.zoneId());

        int replicas = zoneDescriptor.replicas();
        int quorum = replicas / 2 + 1;

        Map<LocalPartitionStateEnum, List<LocalPartitionState>> groupedStates = map.values().stream()
                .collect(groupingBy(localPartitionState -> localPartitionState.state));

        GlobalPartitionStateEnum globalStateEnum;

        int healthyReplicas = groupedStates.getOrDefault(HEALTHY, emptyList()).size();

        if (healthyReplicas == replicas) {
            globalStateEnum = AVAILABLE;
        } else if (healthyReplicas >= quorum) {
            globalStateEnum = DEGRADED;
        } else if (healthyReplicas > 0) {
            globalStateEnum = READ_ONLY;
        } else {
            globalStateEnum = GlobalPartitionStateEnum.UNAVAILABLE;
        }

        LocalPartitionState anyLocalState = map.values().iterator().next();
        return new GlobalPartitionState(
                zoneDescriptor.name(),
                schemaDescriptor.name(),
                anyLocalState.tableId,
                anyLocalState.tableName,
                tablePartitionId.partitionId(),
                globalStateEnum
        );
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

    ClusterNode localNode() {
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

        catalogManager.tables(catalogVersion).forEach(this::registerPartitionStatesMetricSource);
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
