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

package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForRecoverableStateChanges;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForZoneCreation;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForZoneRemoval;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesAndTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractChangeTriggerRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.toDataNodesMap;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerScaleUpScaleDownKeysCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndScaleDownTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndScaleUpTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneTopologyAugmentation;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLastHandledTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesRecoverableStateRevision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.causalitydatanodes.CausalityDataNodesEngine;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.SimpleCondition;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard = new AtomicBoolean();

    /** Logical topology service to track topology changes. */
    private final LogicalTopologyService logicalTopologyService;

    /** Executor for scheduling tasks for scale up and scale down processes. */
    private final StripedScheduledThreadPoolExecutor executor;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState = new ConcurrentHashMap<>();

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new LogicalTopologyEventListener() {
        @Override
        public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology);
        }
    };

    /**
     * The logical topology on the last watch event.
     * It's enough to mark this field by volatile because we don't update the collection after it is assigned to the field.
     */
    private volatile Set<NodeWithAttributes> logicalTopology = emptySet();

    /**
     * Local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-19491 properly clean up this map
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter documentation</a>
     */
    private Map<String, NodeWithAttributes> nodesAttributes = new ConcurrentHashMap<>();

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Rebalance engine. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /** Causality data nodes engine. */
    private final CausalityDataNodesEngine causalityDataNodesEngine;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /** Executor for scheduling rebalances. */
    private final ScheduledExecutorService rebalanceScheduler;

    /**
     * Creates a new distribution zone manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param catalogManager Catalog manager.
     */
    public DistributionZoneManager(
            String nodeName,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            CatalogManager catalogManager,
            ScheduledExecutorService rebalanceScheduler
    ) {
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.catalogManager = catalogManager;

        this.topologyWatchListener = createMetastorageTopologyListener();

        this.rebalanceScheduler = rebalanceScheduler;

        executor = createZoneManagerExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                NamedThreadFactory.create(nodeName, "dst-zones-scheduler", LOG)
        );

        // It's safe to leak with partially initialised object here, because rebalanceEngine is only accessible through this or by
        // meta storage notification thread that won't start before all components start.
        //noinspection ThisEscapedInObjectConstruction
        rebalanceEngine = new DistributionZoneRebalanceEngine(
                busyLock,
                metaStorageManager,
                this,
                catalogManager,
                rebalanceScheduler
        );

        //noinspection ThisEscapedInObjectConstruction
        causalityDataNodesEngine = new CausalityDataNodesEngine(
                busyLock,
                registry,
                metaStorageManager,
                zonesState,
                this,
                catalogManager
        );
    }

    @Override
    public CompletableFuture<Void> startAsync() {
        return inBusyLockAsync(busyLock, () -> {
            registerCatalogEventListenersOnStartManagerBusy();

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), topologyWatchListener);

            CompletableFuture<Long> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            restoreGlobalStateFromLocalMetastorage(recoveryRevision);

            return allOf(
                    createOrRestoreZonesStates(recoveryRevision),
                    restoreLogicalTopologyChangeEventAndStartTimers(recoveryRevision)
            ).thenCompose((notUsed) -> rebalanceEngine.start());
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync() {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        rebalanceEngine.stop();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);

        shutdownAndAwaitTermination(executor, 10, SECONDS);
        shutdownAndAwaitTermination(rebalanceScheduler, 10, SECONDS);

        return nullCompletedFuture();
    }

    /**
     * Gets data nodes of the zone using causality token and catalog version. {@code causalityToken} must be agreed
     * with the {@code catalogVersion}, meaning that for the provided {@code causalityToken} actual {@code catalogVersion} must be provided.
     * For example, if you are in the meta storage watch thread and {@code causalityToken} is the revision of the watch event, it is
     * safe to take {@link CatalogManager#latestCatalogVersion()} as a {@code catalogVersion},
     * because {@link CatalogManager#latestCatalogVersion()} won't be updated in a watch thread.
     * The same is applied for {@link CatalogEventParameters}, it is safe to take {@link CatalogEventParameters#causalityToken()}
     * as a {@code causalityToken} and {@link CatalogEventParameters#catalogVersion()} as a {@code catalogVersion}.
     *
     * <p>Return data nodes or throw the exception:
     * {@link IllegalArgumentException} if causalityToken or zoneId is not valid.
     * {@link DistributionZoneNotFoundException} if the zone with the provided zoneId does not exist.
     *
     * @param causalityToken Causality token.
     * @param catalogVersion Catalog version.
     * @param zoneId Zone id.
     * @return The future with data nodes for the zoneId.
     */
    public CompletableFuture<Set<String>> dataNodes(long causalityToken, int catalogVersion, int zoneId) {
        return causalityDataNodesEngine.dataNodes(causalityToken, catalogVersion, zoneId);
    }

    private CompletableFuture<Void> onUpdateScaleUpBusy(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        int newScaleUp = parameters.zoneDescriptor().dataNodesAutoAdjustScaleUp();

        long causalityToken = parameters.causalityToken();

        if (newScaleUp == IMMEDIATE_TIMER_VALUE) {
            return saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken);
        }

        // It is safe to zonesTimers.get(zoneId) in term of NPE because meta storage notifications are one-threaded
        // and this map will be initialized on a manager start or with catalog notification
        ZoneState zoneState = zonesState.get(zoneId);

        if (newScaleUp != INFINITE_TIMER_VALUE) {
            Optional<Long> highestRevision = zoneState.highestRevision(true);

            assert highestRevision.isEmpty() || causalityToken >= highestRevision.get() : IgniteStringFormatter.format(
                    "Expected causalityToken that is greater or equal to already seen meta storage events: highestRevision={}, "
                            + "causalityToken={}",
                    highestRevision.orElse(null), causalityToken
            );

            zoneState.rescheduleScaleUp(
                    newScaleUp,
                    () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken),
                    zoneId
            );
        } else {
            zoneState.stopScaleUp();
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> onUpdateScaleDownBusy(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        int newScaleDown = parameters.zoneDescriptor().dataNodesAutoAdjustScaleDown();

        long causalityToken = parameters.causalityToken();

        if (newScaleDown == IMMEDIATE_TIMER_VALUE) {
            return saveDataNodesToMetaStorageOnScaleDown(zoneId, causalityToken);
        }

        // It is safe to zonesTimers.get(zoneId) in term of NPE because meta storage notifications are one-threaded
        // and this map will be initialized on a manager start or with catalog notification
        ZoneState zoneState = zonesState.get(zoneId);

        if (newScaleDown != INFINITE_TIMER_VALUE) {
            Optional<Long> highestRevision = zoneState.highestRevision(false);

            assert highestRevision.isEmpty() || causalityToken >= highestRevision.get() : IgniteStringFormatter.format(
                    "Expected causalityToken that is greater or equal to already seen meta storage events: highestRevision={}, "
                            + "causalityToken={}",
                    highestRevision.orElse(null), causalityToken
            );

            zoneState.rescheduleScaleDown(
                    newScaleDown,
                    () -> saveDataNodesToMetaStorageOnScaleDown(zoneId, causalityToken),
                    zoneId
            );
        } else {
            zoneState.stopScaleDown();
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> onUpdateFilter(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        long causalityToken = parameters.causalityToken();

        return saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken);
    }

    /**
     * Restores zones' states.
     *
     * @param zone Zone descriptor.
     * @param causalityToken Causality token.
     * @return Future reflecting the completion of creation or restoring a zone.
     */
    private CompletableFuture<Void> restoreZoneStateBusy(CatalogZoneDescriptor zone, long causalityToken) {
        int zoneId = zone.id();

        Entry zoneDataNodesLocalMetaStorage = metaStorageManager.getLocally(zoneDataNodesKey(zoneId), causalityToken);

        if (zoneDataNodesLocalMetaStorage.value() == null) {
            // In this case, creation of a zone was interrupted during restart.
            return onCreateZone(zone, causalityToken);
        } else {
            Entry topologyAugmentationMapLocalMetaStorage = metaStorageManager.getLocally(zoneTopologyAugmentation(zoneId), causalityToken);

            ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap;

            if (topologyAugmentationMapLocalMetaStorage.value() == null) {
                // This case means that there won't any logical topology updates before restart.
                topologyAugmentationMap = new ConcurrentSkipListMap<>();
            } else {
                topologyAugmentationMap = fromBytes(topologyAugmentationMapLocalMetaStorage.value());
            }

            ZoneState zoneState = new ZoneState(executor, topologyAugmentationMap);

            ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

            assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> onCreateZone(CatalogZoneDescriptor zone, long causalityToken) {
        int zoneId = zone.id();

        ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap = new ConcurrentSkipListMap<>();

        ZoneState zoneState = new ZoneState(executor, topologyAugmentationMap);

        ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

        assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';

        Set<Node> dataNodes = logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet());

        causalityDataNodesEngine.onCreateZoneState(causalityToken, zone);

        return initDataNodesAndTriggerKeysInMetaStorage(zoneId, causalityToken, dataNodes);
    }

    /**
     * Restores timers that were scheduled before a node's restart. Take the highest revision from the
     * {@link ZoneState#topologyAugmentationMap()}, schedule scale up/scale down timers.
     *
     * @param catalogVersion Catalog version.
     * @return Future that represents the pending completion of the operation.
     *         For the immediate timers it will be completed when data nodes will be updated in Meta Storage.
     */
    private CompletableFuture<Void> restoreTimers(int catalogVersion) {
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
            ZoneState zoneState = zonesState.get(zone.id());

            // Max revision from the {@link ZoneState#topologyAugmentationMap()} for node joins.
            Optional<Long> maxScaleUpRevisionOptional = zoneState.highestRevision(true);

            // Max revision from the {@link ZoneState#topologyAugmentationMap()} for node removals.
            Optional<Long> maxScaleDownRevisionOptional = zoneState.highestRevision(false);

            maxScaleUpRevisionOptional.ifPresent(
                    maxScaleUpRevision -> {
                        // Take the highest revision from the topologyAugmentationMap and schedule scale up/scale down,
                        // meaning that all augmentations of nodes will be taken into account in newly created timers.
                        // If augmentations have already been proposed to data nodes in the metastorage before restart,
                        // that means we have updated corresponding trigger key and it's value will be greater or equal to
                        // the highest revision from the topologyAugmentationMap, and current timer won't affect data nodes.

                        futures.add(scheduleTimers(zone, true, false, maxScaleUpRevision));
                    }
            );

            maxScaleDownRevisionOptional.ifPresent(
                    maxScaleDownRevision -> futures.add(scheduleTimers(zone, false, true, maxScaleDownRevision))
            );
        }

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Method initialise data nodes value for the specified zone, also sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} and {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)}
     * if it passes the condition. It is called on the first creation of a zone.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     * @param dataNodes Data nodes.
     * @return Future reflecting the completion of initialisation of zone's keys in meta storage.
     */
    private CompletableFuture<Void> initDataNodesAndTriggerKeysInMetaStorage(
            int zoneId,
            long revision,
            Set<Node> dataNodes
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the corresponding data nodes keys weren't initialised in ms yet.
            CompoundCondition triggerKeyCondition = conditionForZoneCreation(zoneId);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKeys(zoneId, revision, toBytes(toDataNodesMap(dataNodes)));

            Iif iif = iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]",
                                    e,
                                    zoneId,
                                    dataNodes,
                                    revision
                            );
                        } else if (invokeResult) {
                            LOG.info("Update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]",
                                    zoneId,
                                    dataNodes,
                                    revision
                            );
                        } else {
                            LOG.debug(
                                    "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]",
                                    zoneId,
                                    dataNodes,
                                    revision
                            );
                        }
                    }).thenCompose((ignored) -> nullCompletedFuture());
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method deletes data nodes value for the specified zone.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private CompletableFuture<Void> removeTriggerKeysAndDataNodes(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            SimpleCondition triggerKeyCondition = conditionForZoneRemoval(zoneId);

            Update removeKeysUpd = deleteDataNodesAndTriggerKeys(zoneId, revision);

            Iif iif = iif(triggerKeyCondition, removeKeysUpd, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Failed to delete zone's dataNodes keys [zoneId = {}, revision = {}]",
                                    e,
                                    zoneId,
                                    revision
                            );
                        } else if (invokeResult) {
                            LOG.info("Delete zone's dataNodes keys [zoneId = {}, revision = {}]", zoneId, revision);
                        } else {
                            LOG.debug("Failed to delete zone's dataNodes keys [zoneId = {}, revision = {}]", zoneId, revision);
                        }
                    })
                    .thenCompose(ignored -> nullCompletedFuture());
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Updates {@link DistributionZonesUtil#zonesLogicalTopologyKey()} and {@link DistributionZonesUtil#zonesLogicalTopologyVersionKey()}
     * in meta storage.
     *
     * @param newTopology Logical topology snapshot.
     */
    private void updateLogicalTopologyInMetaStorage(LogicalTopologySnapshot newTopology) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            Set<LogicalNode> logicalTopology = newTopology.nodes();

            Condition updateCondition;

            if (newTopology.version() == 1) {
                // Very first start of the cluster, so we just initialize zonesLogicalTopologyVersionKey
                updateCondition = notExists(zonesLogicalTopologyVersionKey());
            } else {
                updateCondition = value(zonesLogicalTopologyVersionKey()).lt(longToBytes(newTopology.version()));
            }

            Iif iff = iif(
                    updateCondition,
                    updateLogicalTopologyAndVersion(logicalTopology, newTopology.version()),
                    ops().yield(false)
            );

            metaStorageManager.invoke(iff).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error(
                            "Failed to update distribution zones' logical topology and version keys [topology = {}, version = {}]",
                            e,
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                } else if (res.getAsBoolean()) {
                    LOG.info(
                            "Distribution zones' logical topology and version keys were updated [topology = {}, version = {}]",
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                } else {
                    LOG.info(
                            "Failed to update distribution zones' logical topology and version keys [topology = {}, version = {}]",
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Restores from local Meta Storage logical topology and nodes' attributes fields in {@link DistributionZoneManager} after restart.
     *
     * @param recoveryRevision Revision of the Meta Storage after its recovery.
     */
    private void restoreGlobalStateFromLocalMetastorage(long recoveryRevision) {
        Entry lastHandledTopologyEntry = metaStorageManager.getLocally(zonesLastHandledTopology(), recoveryRevision);

        Entry nodeAttributesEntry = metaStorageManager.getLocally(zonesNodesAttributes(), recoveryRevision);

        if (lastHandledTopologyEntry.value() != null) {
            // We save zonesLastHandledTopology and zonesNodesAttributes in Meta Storage in a one batch, so it is impossible
            // that one value is not null, but other is null.
            assert nodeAttributesEntry.value() != null;

            logicalTopology = fromBytes(lastHandledTopologyEntry.value());

            nodesAttributes = fromBytes(nodeAttributesEntry.value());
        }

        assert lastHandledTopologyEntry.value() == null || logicalTopology.equals(fromBytes(lastHandledTopologyEntry.value()))
                : "Initial value of logical topology was changed after initialization from the Meta Storage manager.";

        assert nodeAttributesEntry.value() == null
                || nodesAttributes.equals(fromBytes(nodeAttributesEntry.value()))
                : "Initial value of nodes' attributes was changed after initialization from the Meta Storage manager.";
    }

    /**
     * Creates watch listener which listens logical topology and logical topology version.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    assert evt.entryEvents().size() == 2 :
                            "Expected an event with logical topology and logical topology version entries but was events with keys: "
                            + evt.entryEvents().stream().map(entry -> entry.newEntry() == null ? "null" : entry.newEntry().key())
                            .collect(toList());

                    byte[] newLogicalTopologyBytes;

                    Set<NodeWithAttributes> newLogicalTopology = null;

                    long revision = 0;

                    for (EntryEvent event : evt.entryEvents()) {
                        Entry e = event.newEntry();

                        if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                            revision = e.revision();
                        } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                            newLogicalTopologyBytes = e.value();

                            newLogicalTopology = fromBytes(newLogicalTopologyBytes);
                        }
                    }

                    assert newLogicalTopology != null : "The event doesn't contain logical topology";
                    assert revision > 0 : "The event doesn't contain logical topology version";

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogManager.latestCatalogVersion();

                    return onLogicalTopologyUpdate(newLogicalTopology, revision, catalogVersion);
                } finally {
                    busyLock.leaveBusy();
                }
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process logical topology event", e);
            }
        };
    }

    /**
     * Reaction on an update of logical topology. In this method {@link DistributionZoneManager#logicalTopology},
     * {@link DistributionZoneManager#nodesAttributes}, {@link ZoneState#topologyAugmentationMap} are updated.
     * This fields are saved to Meta Storage, also timers are scheduled.
     * Note that all futures of Meta Storage updates that happen in this method are returned from this method.
     *
     * @param newLogicalTopology New logical topology.
     * @param revision Revision of the logical topology update.
     * @param catalogVersion Actual version of the Catalog.
     * @return Future reflecting the completion of the actions needed when logical topology was updated.
     */
    private CompletableFuture<Void> onLogicalTopologyUpdate(Set<NodeWithAttributes> newLogicalTopology, long revision, int catalogVersion) {
        Set<Node> removedNodes =
                logicalTopology.stream()
                        .filter(node -> !newLogicalTopology.contains(node))
                        .map(NodeWithAttributes::node)
                        .collect(toSet());

        Set<Node> addedNodes =
                newLogicalTopology.stream()
                        .filter(node -> !logicalTopology.contains(node))
                        .map(NodeWithAttributes::node)
                        .collect(toSet());

        Set<Integer> zoneIds = new HashSet<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
            int zoneId = zone.id();

            updateLocalTopologyAugmentationMap(addedNodes, removedNodes, revision, zoneId);

            futures.add(scheduleTimers(zone, !addedNodes.isEmpty(), !removedNodes.isEmpty(), revision));

            zoneIds.add(zone.id());
        }

        newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n));

        logicalTopology = newLogicalTopology;

        futures.add(saveRecoverableStateToMetastorage(zoneIds, revision, newLogicalTopology));

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Update local topology augmentation map with newly added and removed nodes.
     *
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision of the event that triggered this method.
     * @param zoneId Zone's id.
     */
    private void updateLocalTopologyAugmentationMap(Set<Node> addedNodes, Set<Node> removedNodes, long revision, int zoneId) {
        if (!addedNodes.isEmpty()) {
            zonesState.get(zoneId).nodesToAddToDataNodes(addedNodes, revision);
        }

        if (!removedNodes.isEmpty()) {
            zonesState.get(zoneId).nodesToRemoveFromDataNodes(removedNodes, revision);
        }
    }

    /**
     * Saves recoverable state of the Distribution Zone Manager to Meta Storage atomically in one batch.
     * After restart it could be used to restore these fields.
     *
     * @param zoneIds Set of zone id's, whose states will be saved in the Meta Storage.
     * @param revision Revision of the event.
     * @param newLogicalTopology New logical topology.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<Void> saveRecoverableStateToMetastorage(
            Set<Integer> zoneIds,
            long revision,
            Set<NodeWithAttributes> newLogicalTopology
    ) {
        Operation[] puts = new Operation[3 + zoneIds.size()];

        puts[0] = put(zonesNodesAttributes(), toBytes(nodesAttributes()));

        puts[1] = put(zonesRecoverableStateRevision(), longToBytes(revision));

        puts[2] = put(zonesLastHandledTopology(), toBytes(newLogicalTopology));

        int i = 3;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19491 Properly utilise topology augmentation map. Also this map
        // TODO: can be saved only once for all zones.
        for (Integer zoneId : zoneIds) {
            puts[i++] = put(zoneTopologyAugmentation(zoneId), toBytes(zonesState.get(zoneId).topologyAugmentationMap()));
        }

        Iif iif = iif(
                conditionForRecoverableStateChanges(revision),
                ops(puts).yield(true),
                ops().yield(false)
        );

        return metaStorageManager.invoke(iif)
                .thenApply(StatementResult::getAsBoolean)
                .whenComplete((invokeResult, e) -> {
                    if (e != null) {
                        LOG.error("Failed to update recoverable state for distribution zone manager [revision = {}]", e, revision);
                    } else if (invokeResult) {
                        LOG.info("Update recoverable state for distribution zone manager [revision = {}]", revision);
                    } else {
                        LOG.debug("Failed to update recoverable states for distribution zone manager [revision = {}]", revision);
                    }
                }).thenCompose((ignored) -> nullCompletedFuture());
    }

    /**
     * Schedules scale up and scale down timers.
     *
     * @param zone Zone descriptor.
     * @param nodesAdded Flag indicating that nodes was added to a topology and should be added to zones data nodes.
     * @param nodesRemoved Flag indicating that nodes was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision that triggered that event.
     * @return Future that represents the pending completion of the operation.
     *         For the immediate timers it will be completed when data nodes will be updated in Meta Storage.
     */
    private CompletableFuture<Void> scheduleTimers(CatalogZoneDescriptor zone, boolean nodesAdded, boolean nodesRemoved, long revision) {
        int autoAdjust = zone.dataNodesAutoAdjust();
        int autoAdjustScaleDown = zone.dataNodesAutoAdjustScaleDown();
        int autoAdjustScaleUp = zone.dataNodesAutoAdjustScaleUp();

        int zoneId = zone.id();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if ((nodesAdded || nodesRemoved) && autoAdjust != INFINITE_TIMER_VALUE) {
            // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
        } else {
            if (nodesAdded) {
                if (autoAdjustScaleUp == IMMEDIATE_TIMER_VALUE) {
                    futures.add(saveDataNodesToMetaStorageOnScaleUp(zoneId, revision));
                }

                if (autoAdjustScaleUp != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleUp(
                            autoAdjustScaleUp,
                            () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, revision),
                            zoneId
                    );
                }
            }

            if (nodesRemoved) {
                if (zone.dataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE) {
                    futures.add(saveDataNodesToMetaStorageOnScaleDown(zoneId, revision));
                }

                if (autoAdjustScaleDown != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleDown(
                            autoAdjustScaleDown,
                            () -> saveDataNodesToMetaStorageOnScaleDown(zoneId, revision),
                            zoneId
                    );
                }
            }
        }

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Method updates data nodes value for the specified zone after scale up timer timeout, sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    CompletableFuture<Void> saveDataNodesToMetaStorageOnScaleUp(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            ZoneState zoneState = zonesState.get(zoneId);

            if (zoneState == null) {
                // Zone was deleted
                return nullCompletedFuture();
            }

            Set<ByteArray> keysToGetFromMs = Set.of(
                    zoneDataNodesKey(zoneId),
                    zoneScaleUpChangeTriggerKey(zoneId),
                    zoneScaleDownChangeTriggerKey(zoneId)
            );

            return metaStorageManager.getAll(keysToGetFromMs).thenCompose(values -> inBusyLock(busyLock, () -> {
                if (values.containsValue(null)) {
                    // Zone was deleted
                    return nullCompletedFuture();
                }

                Map<Node, Integer> dataNodesFromMetaStorage = extractDataNodes(values.get(zoneDataNodesKey(zoneId)));

                long scaleUpTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleUpChangeTriggerKey(zoneId)));

                long scaleDownTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleDownChangeTriggerKey(zoneId)));

                if (revision <= scaleUpTriggerRevision) {
                    LOG.debug(
                            "Revision of the event is less than the scale up revision from the metastorage "
                                    + "[zoneId = {}, revision = {}, scaleUpTriggerRevision = {}]",
                            zoneId,
                            revision,
                            scaleUpTriggerRevision
                    );

                    return nullCompletedFuture();
                }

                List<Node> deltaToAdd = zoneState.nodesToBeAddedToDataNodes(scaleUpTriggerRevision, revision);

                Map<Node, Integer> newDataNodes = new HashMap<>(dataNodesFromMetaStorage);

                deltaToAdd.forEach(n -> newDataNodes.merge(n, 1, Integer::sum));

                // Update dataNodes, so nodeId will be updated with the latest seen data on the node.
                // For example, node could be restarted with new nodeId, we need to update it in the data nodes.
                deltaToAdd.forEach(n -> newDataNodes.put(n, newDataNodes.remove(n)));

                // Remove redundant nodes that are not presented in the data nodes.
                newDataNodes.entrySet().removeIf(e -> e.getValue() == 0);

                Update dataNodesAndTriggerKeyUpd = updateDataNodesAndScaleUpTriggerKey(zoneId, revision, toBytes(newDataNodes));

                Iif iif = iif(
                        triggerScaleUpScaleDownKeysCondition(scaleUpTriggerRevision, scaleDownTriggerRevision, zoneId),
                        dataNodesAndTriggerKeyUpd,
                        ops().yield(false)
                );

                return metaStorageManager.invoke(iif)
                        .thenApply(StatementResult::getAsBoolean)
                        .thenCompose(invokeResult -> inBusyLock(busyLock, () -> {
                            if (invokeResult) {
                                // TODO: https://issues.apache.org/jira/browse/IGNITE-19491 Properly utilise this map
                                // Currently we call clean up only on a node that successfully writes data nodes.
                                LOG.info(
                                        "Updating data nodes for a zone after scale up has succeeded "
                                                + "[zoneId = {}, dataNodes = {}, revision = {}]",
                                        zoneId,
                                        newDataNodes,
                                        revision
                                );

                                zoneState.cleanUp(Math.min(scaleDownTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone after scale up has not succeeded "
                                                + "[zoneId = {}, dataNodes = {}, revision = {}]",
                                        zoneId,
                                        newDataNodes,
                                        revision
                                );

                                return saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
                            }

                            return nullCompletedFuture();
                        }));
            })).whenComplete((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value after scale up [zoneId = {}, revision = {}]",
                            e, zoneId, revision);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method updates data nodes value for the specified zone after scale down timer timeout, sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    CompletableFuture<Void> saveDataNodesToMetaStorageOnScaleDown(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            ZoneState zoneState = zonesState.get(zoneId);

            if (zoneState == null) {
                // Zone was deleted
                return nullCompletedFuture();
            }

            Set<ByteArray> keysToGetFromMs = Set.of(
                    zoneDataNodesKey(zoneId),
                    zoneScaleUpChangeTriggerKey(zoneId),
                    zoneScaleDownChangeTriggerKey(zoneId)
            );

            return metaStorageManager.getAll(keysToGetFromMs).thenCompose(values -> inBusyLock(busyLock, () -> {
                if (values.containsValue(null)) {
                    // Zone was deleted
                    return nullCompletedFuture();
                }

                Map<Node, Integer> dataNodesFromMetaStorage = extractDataNodes(values.get(zoneDataNodesKey(zoneId)));

                long scaleUpTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleUpChangeTriggerKey(zoneId)));

                long scaleDownTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleDownChangeTriggerKey(zoneId)));

                if (revision <= scaleDownTriggerRevision) {
                    LOG.debug(
                            "Revision of the event is less than the scale down revision from the metastorage "
                                    + "[zoneId = {}, revision = {}, scaleUpTriggerRevision = {}]",
                            zoneId,
                            revision,
                            scaleDownTriggerRevision
                    );

                    return nullCompletedFuture();
                }

                List<Node> deltaToRemove = zoneState.nodesToBeRemovedFromDataNodes(scaleDownTriggerRevision, revision);

                Map<Node, Integer> newDataNodes = new HashMap<>(dataNodesFromMetaStorage);

                deltaToRemove.forEach(n -> newDataNodes.merge(n, -1, Integer::sum));

                // Remove redundant nodes that are not presented in the data nodes.
                newDataNodes.entrySet().removeIf(e -> e.getValue() == 0);

                Update dataNodesAndTriggerKeyUpd = updateDataNodesAndScaleDownTriggerKey(zoneId, revision, toBytes(newDataNodes));

                Iif iif = iif(
                        triggerScaleUpScaleDownKeysCondition(scaleUpTriggerRevision, scaleDownTriggerRevision, zoneId),
                        dataNodesAndTriggerKeyUpd,
                        ops().yield(false)
                );

                return metaStorageManager.invoke(iif)
                        .thenApply(StatementResult::getAsBoolean)
                        .thenCompose(invokeResult -> inBusyLock(busyLock, () -> {
                            if (invokeResult) {
                                LOG.info(
                                        "Updating data nodes for a zone after scale down has succeeded "
                                                + "[zoneId = {}, dataNodes = {}, revision = {}]",
                                        zoneId,
                                        newDataNodes,
                                        revision
                                );

                                // TODO: https://issues.apache.org/jira/browse/IGNITE-19491 Properly utilise this map
                                // Currently we call clean up only on a node that successfully writes data nodes.
                                zoneState.cleanUp(Math.min(scaleUpTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone after scale down has not succeeded "
                                                + "[zoneId = {}, dataNodes = {}, revision = {}]",
                                        zoneId,
                                        newDataNodes,
                                        revision
                                );

                                return saveDataNodesToMetaStorageOnScaleDown(zoneId, revision);
                            }

                            return nullCompletedFuture();
                        }));
            })).whenComplete((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value after scale down [zoneId = {}]", e, zoneId);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Class responsible for storing state for a distribution zone.
     * States are needed to track nodes that we want to add or remove from the data nodes,
     * to schedule and stop scale up and scale down processes.
     */
    public static class ZoneState {
        /** Schedule task for a scale up process. */
        private ScheduledFuture<?> scaleUpTask;

        /** Schedule task for a scale down process. */
        private ScheduledFuture<?> scaleDownTask;

        /** The delay for the scale up task. */
        private long scaleUpTaskDelay;

        /** The delay for the scale down task. */
        private long scaleDownTaskDelay;

        /**
         * Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can track which nodes
         * should be added or removed in the processes of scale up or scale down. Revision helps to track visibility of the events
         * of adding or removing nodes because any process of scale up or scale down has a revision that triggered this process.
         */
        private final ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap;

        /** Executor for scheduling tasks for scale up and scale down processes. */
        private final StripedScheduledThreadPoolExecutor executor;

        /**
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         */
        ZoneState(StripedScheduledThreadPoolExecutor executor) {
            this.executor = executor;
            topologyAugmentationMap = new ConcurrentSkipListMap<>();
        }

        /**
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         * @param topologyAugmentationMap Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can
         *         track which nodes should be added or removed in the processes of scale up or scale down. Revision helps to track
         *         visibility of the events of adding or removing nodes because any process of scale up or scale down has a revision that
         *         triggered this process.
         */
        ZoneState(StripedScheduledThreadPoolExecutor executor, ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap) {
            this.executor = executor;
            this.topologyAugmentationMap = topologyAugmentationMap;
        }

        /**
         * Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can track which nodes
         * should be added or removed in the processes of scale up or scale down. Revision helps to track visibility of the events
         * of adding or removing nodes because any process of scale up or scale down has a revision that triggered this process.
         */
        public ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap() {
            return topologyAugmentationMap;
        }

        /**
         * Reschedules existing scale up task, if it is not started yet and the delay of this task is not immediate,
         * or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         * @param zoneId Unique id of a zone to determine the executor of the task.
         */
        public synchronized void rescheduleScaleUp(long delay, Runnable runnable, int zoneId) {
            stopScaleUp();

            scaleUpTask = executor.schedule(runnable, delay, SECONDS, zoneId);

            scaleUpTaskDelay = delay;
        }

        /**
         * Reschedules existing scale down task, if it is not started yet and the delay of this task is not immediate,
         * or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         * @param zoneId Unique id of a zone to determine the executor of the task.
         */
        public synchronized void rescheduleScaleDown(long delay, Runnable runnable, int zoneId) {
            stopScaleDown();

            scaleDownTask = executor.schedule(runnable, delay, SECONDS, zoneId);

            scaleDownTaskDelay = delay;
        }

        /**
         * Cancels task for scale up and scale down. Used on {@link #onDropZoneBusy(DropZoneEventParameters)}.
         * Not need to check {@code scaleUpTaskDelay} and {@code scaleDownTaskDelay} because after timer stopping on zone delete event
         * the data nodes value will be updated.
         */
        synchronized void stopTimers() {
            if (scaleUpTask != null) {
                scaleUpTask.cancel(false);
            }

            if (scaleDownTask != null) {
                scaleDownTask.cancel(false);
            }
        }

        /**
         * Cancels task for scale up if it is not started yet and the delay of this task is not immediate.
         */
        synchronized void stopScaleUp() {
            if (scaleUpTask != null && scaleUpTaskDelay > 0) {
                scaleUpTask.cancel(false);
            }
        }

        /**
         * Cancels task for scale down if it is not started yet and the delay of this task is not immediate.
         */
        synchronized void stopScaleDown() {
            if (scaleDownTask != null && scaleDownTaskDelay > 0) {
                scaleDownTask.cancel(false);
            }
        }

        /**
         * Returns a set of nodes that should be added to zone's data nodes.
         *
         * @param scaleUpRevision Last revision of the scale up event. Nodes that were associated with this event
         *                        or with the lower revisions, won't be included in the accumulation.
         * @param revision Revision of the event for which this data nodes is needed.
         *                 Nodes that were associated with this event will be included.
         * @return List of nodes that should be added to zone's data nodes.
         */
        List<Node> nodesToBeAddedToDataNodes(long scaleUpRevision, long revision) {
            return accumulateNodes(scaleUpRevision, revision, true);
        }

        /**
         * Returns a set of nodes that should be removed from zone's data nodes.
         *
         * @param scaleDownRevision Last revision of the scale down event. Nodes that were associated with this event
         *                          or with the lower revisions, won't be included in the accumulation.
         * @param revision Revision of the event for which this data nodes is needed.
         *                 Nodes that were associated with this event will be included.
         * @return List of nodes that should be removed from zone's data nodes.
         */
        List<Node> nodesToBeRemovedFromDataNodes(long scaleDownRevision, long revision) {
            return accumulateNodes(scaleDownRevision, revision, false);
        }

        /**
         * Add nodes to the map where nodes that must be added to the zone's data nodes are accumulated.
         *
         * @param nodes Nodes to add to zone's data nodes.
         * @param revision Revision of the event that triggered this addition.
         */
        void nodesToAddToDataNodes(Set<Node> nodes, long revision) {
            topologyAugmentationMap.put(revision, new Augmentation(nodes, true));
        }

        /**
         * Add nodes to the map where nodes that must be removed from the zone's data nodes are accumulated.
         *
         * @param nodes Nodes to remove from zone's data nodes.
         * @param revision Revision of the event that triggered this addition.
         */
        void nodesToRemoveFromDataNodes(Set<Node> nodes, long revision) {
            topologyAugmentationMap.put(revision, new Augmentation(nodes, false));
        }

        /**
         * Accumulate nodes from the {@link ZoneState#topologyAugmentationMap} starting from the {@code fromKey} (excluding)
         * to the {@code toKey} (including), where flag {@code addition} indicates whether we should accumulate nodes that should be
         * added to data nodes, or removed.
         *
         * @param fromKey Starting key (excluding).
         * @param toKey Ending key (including).
         * @param addition Indicates whether we should accumulate nodes that should be added to data nodes, or removed.
         * @return Accumulated nodes.
         */
        private List<Node> accumulateNodes(long fromKey, long toKey, boolean addition) {
            return topologyAugmentationMap.subMap(fromKey, false, toKey, true).values()
                    .stream()
                    .filter(a -> a.addition == addition)
                    .flatMap(a -> a.nodes.stream())
                    .collect(toList());
        }

        /**
         * Cleans {@code topologyAugmentationMap} to the key, to which is safe to delete.
         * Safe means that this key was handled both by scale up and scale down schedulers.
         *
         * @param toKey Key in map to which is safe to delete data from {@code topologyAugmentationMap}.
         */
        private void cleanUp(long toKey) {
            topologyAugmentationMap.headMap(toKey, true).clear();
        }

        /**
         * Returns the highest revision which is presented in the {@link ZoneState#topologyAugmentationMap()} taking into account
         * the {@code addition} flag.
         *
         * @param addition Flag indicating the type of the nodes for which we want to find the highest revision.
         * @return The highest revision which is presented in the {@link ZoneState#topologyAugmentationMap()} taking into account
         *         the {@code addition} flag.
         */
        Optional<Long> highestRevision(boolean addition) {
            return topologyAugmentationMap().entrySet()
                    .stream()
                    .filter(e -> e.getValue().addition == addition)
                    .max(Map.Entry.comparingByKey())
                    .map(Map.Entry::getKey);
        }

        @TestOnly
        public synchronized ScheduledFuture<?> scaleUpTask() {
            return scaleUpTask;
        }

        @TestOnly
        public synchronized ScheduledFuture<?> scaleDownTask() {
            return scaleDownTask;
        }
    }

    /**
     * Class stores the info about nodes that should be added or removed from the data nodes of a zone.
     * With flag {@code addition} we can track whether {@code nodeNames} should be added or removed.
     */
    public static class Augmentation implements Serializable {
        private static final long serialVersionUID = -7957428671075739621L;

        /** Names of the node. */
        private final Set<Node> nodes;

        /** Flag that indicates whether {@code nodeNames} should be added or removed. */
        private final boolean addition;

        Augmentation(Set<Node> nodes, boolean addition) {
            this.nodes = unmodifiableSet(nodes);
            this.addition = addition;
        }

        public boolean addition() {
            return addition;
        }

        public Set<Node> nodes() {
            return nodes;
        }
    }

    /**
     * Returns local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     *
     * @return Mapping {@code nodeId} -> node's attributes.
     */
    public Map<String, NodeWithAttributes> nodesAttributes() {
        return nodesAttributes;
    }

    @TestOnly
    public Map<Integer, ZoneState> zonesState() {
        return zonesState;
    }

    public Set<NodeWithAttributes> logicalTopology() {
        return logicalTopology;
    }

    private void registerCatalogEventListenersOnStartManagerBusy() {
        catalogManager.listen(ZONE_CREATE, (CreateZoneEventParameters parameters) -> inBusyLock(busyLock, () -> {
            return onCreateZone(parameters.zoneDescriptor(), parameters.causalityToken()).thenApply((ignored) -> false);
        }));

        catalogManager.listen(ZONE_DROP, (DropZoneEventParameters parameters) -> inBusyLock(busyLock, () -> {
            return onDropZoneBusy(parameters).thenApply((ignored) -> false);
        }));

        catalogManager.listen(ZONE_ALTER, new ManagerCatalogAlterZoneEventListener());
    }

    private CompletableFuture<Void> createOrRestoreZonesStates(long recoveryRevision) {
        int catalogVersion = catalogManager.latestCatalogVersion();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        // TODO: IGNITE-20287 Clean up abandoned resources for dropped tables from vault and metastore
        for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
            futures.add(restoreZoneStateBusy(zone, recoveryRevision));
        }

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Restore the event of the updating the logical topology from Meta Storage, that has not been completed before restart.
     * Also start scale up/scale down timers.
     *
     * @param recoveryRevision Revision of the Meta Storage after its recovery.
     * @return Future that represents the pending completion of the operations.
     */
    private CompletableFuture<Void> restoreLogicalTopologyChangeEventAndStartTimers(long recoveryRevision) {
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), recoveryRevision);

        if (topologyEntry.value() != null) {
            Set<NodeWithAttributes> newLogicalTopology = fromBytes(topologyEntry.value());

            long topologyRevision = topologyEntry.revision();

            // It is safe to get the latest version of the catalog as we are in the starting process.
            int catalogVersion = catalogManager.latestCatalogVersion();

            Entry lastUpdateRevisionEntry = metaStorageManager.getLocally(zonesRecoverableStateRevision(), recoveryRevision);

            if (lastUpdateRevisionEntry.value() == null || topologyRevision > bytesToLong(lastUpdateRevisionEntry.value())) {
                return onLogicalTopologyUpdate(newLogicalTopology, recoveryRevision, catalogVersion);
            } else {
                return restoreTimers(catalogVersion);
            }
        }

        return nullCompletedFuture();
    }

    private class ManagerCatalogAlterZoneEventListener extends CatalogAlterZoneEventListener {
        private ManagerCatalogAlterZoneEventListener() {
            super(catalogManager);
        }

        @Override
        protected CompletableFuture<Void> onAutoAdjustScaleUpUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleUp) {
            return inBusyLock(busyLock, () -> onUpdateScaleUpBusy(parameters));
        }

        @Override
        protected CompletableFuture<Void> onAutoAdjustScaleDownUpdate(AlterZoneEventParameters parameters, int oldAutoAdjustScaleDown) {
            return inBusyLock(busyLock, () -> onUpdateScaleDownBusy(parameters));
        }

        @Override
        protected CompletableFuture<Void> onFilterUpdate(AlterZoneEventParameters parameters, String oldFilter) {
            return inBusyLock(busyLock, () -> onUpdateFilter(parameters));
        }
    }

    private CompletableFuture<Void> onDropZoneBusy(DropZoneEventParameters parameters) {
        int zoneId = parameters.zoneId();

        long causalityToken = parameters.causalityToken();

        ZoneState zoneState = zonesState.get(zoneId);

        zoneState.stopTimers();

        return removeTriggerKeysAndDataNodes(zoneId, causalityToken).thenRun(() -> {
            causalityDataNodesEngine.onDelete(causalityToken, zoneId);

            zonesState.remove(zoneId);
        });
    }
}
