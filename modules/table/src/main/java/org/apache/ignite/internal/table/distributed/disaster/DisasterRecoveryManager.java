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
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.AVAILABLE;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.DEGRADED;
import static org.apache.ignite.internal.table.distributed.disaster.GlobalPartitionStateEnum.READ_ONLY;
import static org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateEnum.BROKEN;
import static org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateEnum.CATCHING_UP;
import static org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateEnum.HEALTHY;
import static org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateEnum.INITIALIZING;
import static org.apache.ignite.internal.table.distributed.disaster.LocalPartitionStateEnum.INSTALLING_SNAPSHOT;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.copyStateTo;
import static org.apache.ignite.lang.ErrorGroups.DisasterRecovery.PARTITION_STATE_ERR;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.internal.affinity.Assignments;
import org.apache.ignite.internal.catalog.Catalog;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogObjectDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogTableDescriptor;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.network.MessagingService;
import org.apache.ignite.internal.network.NetworkMessage;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.table.distributed.TableManager;
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.DisasterRecoveryException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.IllegalPartitionIdException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.ZonesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStateMessage;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStatesRequest;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStatesResponse;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.lang.TableNotFoundException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.TopologyService;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.core.State;
import org.jetbrains.annotations.Nullable;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations, such as {@link #resetPartitions(String, String, Set)}.
 * More details are in the <a href="https://issues.apache.org/jira/browse/IGNITE-21140">epic</a>.
 */
public class DisasterRecoveryManager implements IgniteComponent {
    /** Logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DisasterRecoveryManager.class);

    /** Single key for writing disaster recovery requests into meta-storage. */
    static final ByteArray RECOVERY_TRIGGER_KEY = new ByteArray("disaster.recovery.trigger");

    private static final TableMessagesFactory MSG_FACTORY = new TableMessagesFactory();

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

    /**
     * Map of operations, triggered by local node, that have not yet been processed by {@link #watchListener}. Values in the map are the
     * futures, returned from the {@link #processNewRequest(DisasterRecoveryRequest)}, they are completed by
     * {@link #handleTriggerKeyUpdate(WatchEvent)} when node receives corresponding events from the metastorage (or if it doesn't receive
     * this event within a 30 seconds window).
     */
    private final Map<UUID, CompletableFuture<Void>> ongoingOperationsById = new ConcurrentHashMap<>();

    /** Constructor. */
    public DisasterRecoveryManager(
            ExecutorService threadPool,
            MessagingService messagingService,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            DistributionZoneManager dzManager,
            Loza raftManager,
            TopologyService topologyService,
            TableManager tableManager
    ) {
        this.threadPool = threadPool;
        this.messagingService = messagingService;
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.dzManager = dzManager;
        this.raftManager = raftManager;
        this.topologyService = topologyService;
        this.tableManager = tableManager;

        watchListener = new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                return handleTriggerKeyUpdate(event);
            }

            @Override
            public void onError(Throwable e) {
                // No-op.
            }
        };
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        messagingService.addMessageHandler(TableMessageGroup.class, this::handleMessage);

        metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

        return nullCompletedFuture();
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        metaStorageManager.unregisterWatch(watchListener);

        for (CompletableFuture<Void> future : ongoingOperationsById.values()) {
            future.completeExceptionally(new NodeStoppingException());
        }

        return nullCompletedFuture();
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param tableName Fully-qualified table name. Case-sensitive, without quotes. Example: "PUBLIC.Foo".
     * @param partitionIds IDs of partitions to reset. If empty, reset all zone's partitions.
     * @return Operation future.
     */
    public CompletableFuture<Void> resetPartitions(String zoneName, String tableName, Set<Integer> partitionIds) {
        try {
            Catalog catalog = catalogLatestVersion();

            int tableId = tableDescriptor(catalog, tableName).id();

            CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

            checkPartitionsRange(partitionIds, Set.of(zone));

            return processNewRequest(new ManualGroupUpdateRequest(UUID.randomUUID(), zone.id(), tableId, partitionIds));
        } catch (Throwable t) {
            return failedFuture(t);
        }
    }

    /**
     * Restarts replica service and raft group of passed partitions.
     *
     * @param nodeNames Names specifying nodes to restart partitions. Case-sensitive, empty set means "all nodes".
     * @param zoneName Name of the distribution zone. Case-sensitive, without quotes.
     * @param tableName Fully-qualified table name. Case-sensitive, without quotes. Example: "PUBLIC.Foo".
     * @param partitionIds IDs of partitions to restart. If empty, restart all zone's partitions.
     * @return Operation future.
     */
    public CompletableFuture<Void> restartPartitions(Set<String> nodeNames, String zoneName, String tableName, Set<Integer> partitionIds) {
        try {
            // Validates passed node names.
            getNodes(nodeNames);

            Catalog catalog = catalogLatestVersion();

            CatalogZoneDescriptor zone = zoneDescriptor(catalog, zoneName);

            CatalogTableDescriptor table = tableDescriptor(catalog, tableName);

            checkPartitionsRange(partitionIds, Set.of(zone));

            return processNewRequest(new ManualGroupRestartRequest(UUID.randomUUID(), zone.id(), table.id(), partitionIds, nodeNames));
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

    private CompletableFuture<Map<TablePartitionId, LocalPartitionStateMessageByNode>> localPartitionStatesInternal(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Collection<CatalogZoneDescriptor> zones = filterZones(zoneNames, catalog.zones());

        checkPartitionsRange(partitionIds, zones);

        Set<NodeWithAttributes> nodes = getNodes(nodeNames);

        Set<Integer> zoneIds = zones.stream().map(CatalogObjectDescriptor::id).collect(toSet());

        LocalPartitionStatesRequest localPartitionStatesRequest = MSG_FACTORY.localPartitionStatesRequest()
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
     * Creates new operation future, associated with the request, and writes it into meta-storage.
     *
     * @param request Request.
     * @return Operation future.
     */
    private CompletableFuture<Void> processNewRequest(DisasterRecoveryRequest request) {
        UUID operationId = request.operationId();

        CompletableFuture<Void> operationFuture = new CompletableFuture<Void>()
                .whenComplete((v, throwable) -> ongoingOperationsById.remove(operationId))
                .orTimeout(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        ongoingOperationsById.put(operationId, operationFuture);

        metaStorageManager.put(RECOVERY_TRIGGER_KEY, ByteUtils.toBytes(request));

        return operationFuture;
    }

    /**
     * Handler for {@link #RECOVERY_TRIGGER_KEY} update event. Deserializes the request and delegates the execution to
     * {@link DisasterRecoveryRequest#handle(DisasterRecoveryManager, long)}.
     */
    private CompletableFuture<Void> handleTriggerKeyUpdate(WatchEvent watchEvent) {
        Entry newEntry = watchEvent.entryEvent().newEntry();

        byte[] requestBytes = newEntry.value();
        assert requestBytes != null;

        DisasterRecoveryRequest request;
        try {
            request = ByteUtils.fromBytes(requestBytes);
        } catch (Exception e) {
            LOG.warn("Unable to deserialize disaster recovery request.", e);

            return nullCompletedFuture();
        }

        CompletableFuture<Void> operationFuture = ongoingOperationsById.remove(request.operationId());

        switch (request.type()) {
            case SINGLE_NODE:
                if (operationFuture == null) {
                    // We're not the initiator, or timeout has passed. Just ignore it.
                    return nullCompletedFuture();
                }

                return request.handle(this, watchEvent.revision()).whenComplete(copyStateTo(operationFuture));
            case MULTI_NODE:
                CompletableFuture<Void> handleFuture = request.handle(this, watchEvent.revision());

                if (operationFuture == null) {
                    // We're not the initiator, or timeout has passed.
                    return handleFuture;
                }

                return handleFuture.whenComplete(copyStateTo(operationFuture));
            default:
                var error = new AssertionError("Unexpected request type: " + request.getClass());

                if (operationFuture != null) {
                    operationFuture.completeExceptionally(error);
                }

                return failedFuture(error);
        }
    }

    private void handleMessage(NetworkMessage message, ClusterNode sender, @Nullable Long correlationId) {
        if (message instanceof LocalPartitionStatesRequest) {
            handleLocalPartitionStatesRequest((LocalPartitionStatesRequest) message, sender, correlationId);
        }
    }

    private void handleLocalPartitionStatesRequest(LocalPartitionStatesRequest request, ClusterNode sender, @Nullable Long correlationId) {
        assert correlationId != null;

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
                    if (tableDescriptor == null || !containsOrEmpty(tableDescriptor.zoneId(), request.zoneIds())
                    ) {
                        return;
                    }

                    Node raftNode = raftGroupService.getRaftNode();
                    State nodeState = raftNode.getNodeState();

                    LocalPartitionStateEnum localState = convertState(nodeState);
                    long lastLogIndex = raftNode.lastLogIndex();

                    if (localState == HEALTHY) {
                        // Node without log didn't process anything yet, it's not really "healthy" before it accepts leader's configuration.
                        if (lastLogIndex == 0) {
                            localState = INITIALIZING;
                        }

                        if (raftNode.isInstallingSnapshot()) {
                            localState = INSTALLING_SNAPSHOT;
                        }
                    }

                    statesList.add(MSG_FACTORY.localPartitionStateMessage()
                            .partitionId(MSG_FACTORY.tablePartitionIdMessage()
                                    .tableId(tablePartitionId.tableId())
                                    .partitionId(tablePartitionId.partitionId())
                                    .build())
                            .state(localState)
                            .logIndex(lastLogIndex)
                            .build()
                    );
                }
            });

            LocalPartitionStatesResponse response = MSG_FACTORY.localPartitionStatesResponse().states(statesList).build();

            messagingService.respond(sender, response, correlationId);
        }, threadPool);
    }

    private static <T> boolean containsOrEmpty(T item, Collection<T> collection) {
        return collection.isEmpty() || collection.contains(item);
    }

    /**
     * Converts internal raft node state into public local partition state.
     */
    private static LocalPartitionStateEnum convertState(State nodeState) {
        switch (nodeState) {
            case STATE_LEADER:
            case STATE_TRANSFERRING:
            case STATE_CANDIDATE:
            case STATE_FOLLOWER:
                return HEALTHY;

            case STATE_ERROR:
                return BROKEN;

            case STATE_UNINITIALIZED:
                return INITIALIZING;

            case STATE_SHUTTING:
            case STATE_SHUTDOWN:
            case STATE_END:
                return LocalPartitionStateEnum.UNAVAILABLE;

            default:
                // Unrecognized state, better safe than sorry.
                return BROKEN;
        }
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

        if (stateMsg.state() == HEALTHY && maxLogIndex - stateMsg.logIndex() >= CATCH_UP_THRESHOLD) {
            stateEnum = CATCHING_UP;
        }

        // Tables, returned from local states request, are always present in the required version of the catalog.
        CatalogTableDescriptor tableDescriptor = catalog.table(tablePartitionId.tableId());

        String zoneName = catalog.zone(tableDescriptor.zoneId()).name();

        return new LocalPartitionState(tableDescriptor.name(), zoneName, tablePartitionId.partitionId(), stateEnum);
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
                    int zoneId = catalog.table(tableId).zoneId();
                    CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

                    if (partitionIds.isEmpty()) {
                        int partitions = zoneDescriptor.partitions();

                        for (int partitionId = 0; partitionId < partitions; partitionId++) {
                            putUnavailableStateIfAbsent(catalog, result, tableId, partitionId, zoneDescriptor);
                        }
                    } else {
                        partitionIds.forEach(id -> {
                            putUnavailableStateIfAbsent(catalog, result, tableId, id, zoneDescriptor);
                        });
                    }
                });
    }

    private static void putUnavailableStateIfAbsent(
            Catalog catalog,
            Map<TablePartitionId, GlobalPartitionState> states,
            Integer tableId,
            int partitionId,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

        states.computeIfAbsent(tablePartitionId, key ->
                new GlobalPartitionState(catalog.table(key.tableId()).name(), zoneDescriptor.name(), key.partitionId(),
                        GlobalPartitionStateEnum.UNAVAILABLE)
        );
    }

    private static GlobalPartitionState assembleGlobalStateFromLocal(
            Catalog catalog,
            TablePartitionId tablePartitionId,
            LocalPartitionStateByNode map
    ) {
        // Tables, returned from local states request, are always present in the required version of the catalog.
        int zoneId = catalog.table(tablePartitionId.tableId()).zoneId();
        CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);

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
        return new GlobalPartitionState(anyLocalState.tableName, zoneDescriptor.name(), tablePartitionId.partitionId(), globalStateEnum);
    }

    private Catalog catalogLatestVersion() {
        int catalogVersion = catalogManager.latestCatalogVersion();

        Catalog catalog = catalogManager.catalog(catalogVersion);

        assert catalog != null : catalogVersion;

        return catalog;
    }

    private static CatalogTableDescriptor tableDescriptor(Catalog catalog, String tableName) {
        CatalogTableDescriptor tableDescriptor = catalog.table(tableName);

        if (tableDescriptor == null) {
            throw new TableNotFoundException(tableName);
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

    ClusterNode localNode() {
        return topologyService.localMember();
    }
}
