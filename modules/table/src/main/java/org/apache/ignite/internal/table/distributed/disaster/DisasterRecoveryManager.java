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

import java.util.ArrayList;
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
import org.apache.ignite.internal.table.distributed.TableMessageGroup;
import org.apache.ignite.internal.table.distributed.TableMessagesFactory;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.NodesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.PartitionsNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.exceptions.ZonesNotFoundException;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStateMessage;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStatesRequest;
import org.apache.ignite.internal.table.distributed.disaster.messages.LocalPartitionStatesResponse;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.CollectionUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.core.State;
import org.jetbrains.annotations.Nullable;

/**
 * Manager, responsible for "disaster recovery" operations.
 * Internally it triggers meta-storage updates, in order to acquire unique causality token.
 * As a reaction to these updates, manager performs actual recovery operations, such as {@link #resetPartitions(int, int)}.
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
    private final Loza raftManager;

    /** Watch listener for {@link #RECOVERY_TRIGGER_KEY}. */
    private final WatchListener watchListener;

    /**
     * Map of operations, triggered by local node, that have not yet been processed by {@link #watchListener}. Values in the map are the
     * futures, returned from the {@link #processNewRequest(ManualGroupUpdateRequest)}, they are completed by
     * {@link #handleTriggerKeyUpdate(WatchEvent)} when node receives corresponding events from the metastorage (or if it doesn't receive
     * this event within a 30 seconds window).
     */
    private final Map<UUID, CompletableFuture<Void>> ongoingOperationsById = new ConcurrentHashMap<>();

    /**
     * Constructor.
     */
    public DisasterRecoveryManager(
            ExecutorService threadPool,
            MessagingService messagingService,
            MetaStorageManager metaStorageManager,
            CatalogManager catalogManager,
            DistributionZoneManager dzManager,
            Loza raftManager
    ) {
        this.threadPool = threadPool;
        this.messagingService = messagingService;
        this.metaStorageManager = metaStorageManager;
        this.catalogManager = catalogManager;
        this.dzManager = dzManager;
        this.raftManager = raftManager;

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
    public CompletableFuture<Void> start() {
        messagingService.addMessageHandler(TableMessageGroup.class, this::handleMessage);

        metaStorageManager.registerExactWatch(RECOVERY_TRIGGER_KEY, watchListener);

        return nullCompletedFuture();
    }

    @Override
    public void stop() throws Exception {
        metaStorageManager.unregisterWatch(watchListener);

        for (CompletableFuture<Void> future : ongoingOperationsById.values()) {
            future.completeExceptionally(new NodeStoppingException());
        }
    }

    /**
     * Updates assignments of the table in a forced manner, allowing for the recovery of raft group with lost majorities. It is achieved via
     * triggering a new rebalance with {@code force} flag enabled in {@link Assignments} for partitions where it's required. New pending
     * assignments with {@code force} flag remove old stable nodes from the distribution, and force new Raft configuration via "resetPeers"
     * so that a new leader could be elected.
     *
     * @param zoneId Distribution zone ID.
     * @param tableId Table ID.
     * @return Operation future.
     */
    public CompletableFuture<Void> resetPartitions(int zoneId, int tableId) {
        return processNewRequest(new ManualGroupUpdateRequest(UUID.randomUUID(), zoneId, tableId));
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link TablePartitionId} to the mapping
     * between a node name and a partition state.
     *
     * @param zoneNames Names specifying zones to get partition states from. Empty set means "all zones".
     * @param nodeNames Names specifying nodes to get partition states from. Empty set means "all nodes".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<TablePartitionId, Map<String, LocalPartitionState>>> localPartitionStates(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds
    ) {
        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        return localPartitionStatesInternal(zoneNames, nodeNames, partitionIds, catalog)
                .thenApply(res -> normalizeLocal(res, catalog));
    }

    /**
     * Returns states of partitions in the cluster. Result is a mapping of {@link TablePartitionId} to the global
     * partition state enum value.
     *
     * @param zoneNames Names specifying zones to get partition states. Empty set means "all zones".
     * @param partitionIds IDs of partitions to get states of. Empty set means "all partitions".
     * @return Future with the mapping.
     */
    public CompletableFuture<Map<TablePartitionId, GlobalPartitionState>> globalPartitionStates(
            Set<String> zoneNames,
            Set<Integer> partitionIds
    ) {
        Catalog catalog = catalogManager.catalog(catalogManager.latestCatalogVersion());

        return localPartitionStatesInternal(zoneNames, Set.of(), partitionIds, catalog)
                .thenApply(res -> normalizeLocal(res, catalog))
                .thenApply(res -> assembleGlobal(res, catalog));
    }

    private CompletableFuture<Map<TablePartitionId, Map<String, LocalPartitionStateMessage>>> localPartitionStatesInternal(
            Set<String> zoneNames,
            Set<String> nodeNames,
            Set<Integer> partitionIds,
            Catalog catalog
    ) {
        Set<Integer> zoneIds = getZoneIds(zoneNames, catalog);

        Set<NodeWithAttributes> nodes = getNodes(nodeNames);

        LocalPartitionStatesRequest localPartitionStatesRequest = MSG_FACTORY.localPartitionStatesRequest()
                .zoneIds(zoneIds)
                .partitionIds(partitionIds)
                .catalogVersion(catalog.version())
                .build();

        Map<TablePartitionId, Map<String, LocalPartitionStateMessage>> result = new ConcurrentHashMap<>();
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
                    result.compute(state.partitionId().asTablePartitionId(), (tablePartitionId, map) -> {
                        if (map == null) {
                            return Map.of(node.nodeName(), state);
                        }

                        map = new HashMap<>(map);
                        map.put(node.nodeName(), state);
                        return map;
                    });
                }
            });
        }

        if (!partitionIds.isEmpty()) {
            return allOf(futures).thenApply(ignored -> {
                Set<Integer> foundPartitionIds = result.keySet().stream()
                        .map(TablePartitionId::partitionId)
                        .collect(toSet());

                checkPartitions(foundPartitionIds, partitionIds);

                return result;
            });
        }

        return allOf(futures).handle((unused, throwable) -> result);
    }

    private Set<NodeWithAttributes> getNodes(Set<String> nodeNames) throws NodesNotFoundException{
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

    private static Set<Integer> getZoneIds(Set<String> zoneNames, Catalog catalog) throws ZonesNotFoundException {
        if (zoneNames.isEmpty()) {
            return Set.of();
        }

        List<CatalogZoneDescriptor> zoneDescriptors = catalog.zones().stream()
                .filter(catalogZoneDescriptor -> zoneNames.contains(catalogZoneDescriptor.name()))
                .collect(toList());

        if (zoneDescriptors.size() != zoneNames.size()) {
            Set<String> foundZoneNames = zoneDescriptors.stream()
                    .map(CatalogObjectDescriptor::name)
                    .collect(toSet());

            Set<String> missingZoneNames = CollectionUtils.difference(zoneNames, foundZoneNames);

            throw new ZonesNotFoundException(missingZoneNames);
        }

        return zoneDescriptors.stream()
                .map(CatalogObjectDescriptor::id)
                .collect(toSet());
    }

    /**
     * Creates new operation future, associated with the request, and writes it into meta-storage.
     *
     * @param request Request.
     * @return Operation future.
     */
    private CompletableFuture<Void> processNewRequest(ManualGroupUpdateRequest request) {
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
     * {@link DisasterRecoveryRequest#handle(DisasterRecoveryManager, long, CompletableFuture)}.
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

        if (operationFuture == null) {
            // We're not the initiator, or timeout has passed. Just ignore it.
            return nullCompletedFuture();
        }

        return request.handle(this, watchEvent.revision(), operationFuture);
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

                    if (!request.partitionIds().isEmpty() && !request.partitionIds().contains(tablePartitionId.partitionId())) {
                        return;
                    }

                    CatalogTableDescriptor tableDescriptor = catalogManager.table(tablePartitionId.tableId(), catalogVersion);
                    // Only tables that belong to a specific catalog version will be returned.
                    if (tableDescriptor == null
                            || !request.zoneIds().isEmpty()
                            && !request.zoneIds().contains(tableDescriptor.zoneId())
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
     * Checks that resulting states contain all partitions IDs from the request.
     *
     * @param foundPartitionIds Found partition IDs.
     * @param requestedPartitionIds Requested partition IDs.
     * @throws PartitionsNotFoundException if some IDs are missing.
     */
    private static void checkPartitions(
            Set<Integer> foundPartitionIds,
            Set<Integer> requestedPartitionIds
    ) throws PartitionsNotFoundException {
        if (!requestedPartitionIds.equals(foundPartitionIds)) {
            Set<Integer> missingPartitionIds = CollectionUtils.difference(requestedPartitionIds, foundPartitionIds);

            throw new PartitionsNotFoundException(missingPartitionIds);
        }
    }

    /**
     * Replaces some healthy states with a {@link LocalPartitionStateEnum#CATCHING_UP}, it can only be done once the state of all peers is
     * known.
     */
    private static Map<TablePartitionId, Map<String, LocalPartitionState>> normalizeLocal(
            Map<TablePartitionId, Map<String, LocalPartitionStateMessage>> result,
            Catalog catalog
    ) {
        return result.entrySet().stream().collect(toMap(Map.Entry::getKey, entry -> {
            TablePartitionId tablePartitionId = entry.getKey();
            Map<String, LocalPartitionStateMessage> map = entry.getValue();

            // noinspection OptionalGetWithoutIsPresent
            long maxLogIndex = map.values().stream().mapToLong(LocalPartitionStateMessage::logIndex).max().getAsLong();

            return map.entrySet().stream().collect(toMap(Map.Entry::getKey, entry2 -> {
                LocalPartitionStateMessage stateMsg = entry2.getValue();

                LocalPartitionStateEnum stateEnum = stateMsg.state();

                if (stateMsg.state() == HEALTHY && maxLogIndex - stateMsg.logIndex() >= CATCH_UP_THRESHOLD) {
                    stateEnum = CATCHING_UP;
                }

                // Tables, returned from local states request, are always present in the required version of the catalog.
                CatalogTableDescriptor tableDescriptor = catalog.table(tablePartitionId.tableId());

                String zoneName = catalog.zone(tableDescriptor.zoneId()).name();

                return new LocalPartitionState(tableDescriptor.name(), zoneName, tablePartitionId.partitionId(), stateEnum);
            }));
        }));
    }

    private static Map<TablePartitionId, GlobalPartitionState> assembleGlobal(
            Map<TablePartitionId, Map<String, LocalPartitionState>> localResult,
            Catalog catalog
    ) {
        Map<TablePartitionId, GlobalPartitionState> result = localResult.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, entry -> {
                    TablePartitionId tablePartitionId = entry.getKey();
                    Map<String, LocalPartitionState> map = entry.getValue();

                    return assembleGlobalStateFromLocal(catalog, tablePartitionId, map);
                }));

        localResult.keySet().stream()
                .map(TablePartitionId::tableId)
                .distinct()
                .forEach(tableId -> {
                    int zoneId = catalog.table(tableId).zoneId();
                    CatalogZoneDescriptor zoneDescriptor = catalog.zone(zoneId);
                    int partitions = zoneDescriptor.partitions();

                    // Make missing partitions explicitly unavailable.
                    for (int partitionId = 0; partitionId < partitions; partitionId++) {
                        TablePartitionId tablePartitionId = new TablePartitionId(tableId, partitionId);

                        result.computeIfAbsent(tablePartitionId, key ->
                                new GlobalPartitionState(catalog.table(key.tableId()).name(), zoneDescriptor.name(),  key.partitionId(),
                                        GlobalPartitionStateEnum.UNAVAILABLE)
                        );
                    }
                });

        return result;
    }

    private static GlobalPartitionState assembleGlobalStateFromLocal(
            Catalog catalog,
            TablePartitionId tablePartitionId,
            Map<String, LocalPartitionState> map
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
}
