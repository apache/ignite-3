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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForGlobalStatesChanges;
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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesFilterUpdateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
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
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

    /** Meta Storage manager. */
    private final MetaStorageManager metaStorageManager;

    /** Vault manager. */
    private final VaultManager vaultMgr;

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
    private Map<String, Map<String, String>> nodesAttributes = new ConcurrentHashMap<>();

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Rebalance engine. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /** Causality data nodes engine. */
    private final CausalityDataNodesEngine causalityDataNodesEngine;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /**
     * Creates a new distribution zone manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param vaultMgr Vault manager.
     * @param catalogManager Catalog manager.
     */
    public DistributionZoneManager(
            String nodeName,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            VaultManager vaultMgr,
            CatalogManager catalogManager
    ) {
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.vaultMgr = vaultMgr;
        this.catalogManager = catalogManager;

        this.topologyWatchListener = createMetastorageTopologyListener();

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
                catalogManager
        );

        //noinspection ThisEscapedInObjectConstruction
        causalityDataNodesEngine = new CausalityDataNodesEngine(
                busyLock,
                registry,
                metaStorageManager,
                vaultMgr,
                zonesState,
                this
        );
    }

    @Override
    public void start() {
        inBusyLock(busyLock, () -> {
            registerCatalogEventListenersOnStartManagerBusy();

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), topologyWatchListener);

            CompletableFuture<Long> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join();

            restoreGlobalStateFromLocalMetastorage(recoveryRevision);

            startZonesOnStartManagerBusy(recoveryRevision);

            rebalanceEngine.start();
        });
    }

    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        rebalanceEngine.stop();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);

        shutdownAndAwaitTermination(executor, 10, SECONDS);
    }

    /**
     * Returns the data nodes of the specified zone.
     * See {@link CausalityDataNodesEngine#dataNodes(long, int)}.
     *
     * @param causalityToken Causality token.
     * @param zoneId Zone id.
     * @return The future which will be completed with data nodes for the zoneId or with exception.
     */
    public CompletableFuture<Set<String>> dataNodes(long causalityToken, int zoneId) {
        return causalityDataNodesEngine.dataNodes(causalityToken, zoneId);
    }

    private CompletableFuture<Void> onUpdateScaleUpBusy(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        int newScaleUp = parameters.zoneDescriptor().dataNodesAutoAdjustScaleUp();

        long causalityToken = parameters.causalityToken();

        if (newScaleUp == IMMEDIATE_TIMER_VALUE) {
            return saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken).thenRun(() -> {
                // TODO: causalityOnUpdateScaleUp will be removed https://issues.apache.org/jira/browse/IGNITE-20604,
                // catalog must be used instead
                causalityDataNodesEngine.causalityOnUpdateScaleUp(causalityToken, zoneId, IMMEDIATE_TIMER_VALUE);
            });
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

        causalityDataNodesEngine.causalityOnUpdateScaleUp(causalityToken, zoneId, newScaleUp);

        return completedFuture(null);
    }

    private CompletableFuture<Void> onUpdateScaleDownBusy(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        int newScaleDown = parameters.zoneDescriptor().dataNodesAutoAdjustScaleDown();

        long causalityToken = parameters.causalityToken();

        if (newScaleDown == IMMEDIATE_TIMER_VALUE) {
            return saveDataNodesToMetaStorageOnScaleDown(zoneId, causalityToken).thenRun(() -> {
                // TODO: causalityOnUpdateScaleDown will be removed https://issues.apache.org/jira/browse/IGNITE-20604,
                // catalog must be used instead
                causalityDataNodesEngine.causalityOnUpdateScaleDown(causalityToken, zoneId, IMMEDIATE_TIMER_VALUE);
            });
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

        causalityDataNodesEngine.causalityOnUpdateScaleDown(causalityToken, zoneId, newScaleDown);

        return completedFuture(null);
    }

    private CompletableFuture<Void> onUpdateFilter(AlterZoneEventParameters parameters) {
        int zoneId = parameters.zoneDescriptor().id();

        String newFilter = parameters.zoneDescriptor().filter();

        long causalityToken = parameters.causalityToken();

        VaultEntry filterUpdateRevision = vaultMgr.get(zonesFilterUpdateRevision()).join();

        if (filterUpdateRevision != null) {
            // This means that we have already handled event with this causalityToken.
            // It is possible when node was restarted after this listener completed,
            // but applied causalityToken didn't have time to be propagated to the Vault.
            if (bytesToLong(filterUpdateRevision.value()) >= causalityToken) {
                return completedFuture(null);
            }
        }

        vaultMgr.put(zonesFilterUpdateRevision(), longToBytes(causalityToken)).join();

        causalityDataNodesEngine.onUpdateFilter(causalityToken, zoneId, newFilter);

        return saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken);
    }

    /**
     * Creates or restores zone's state depending on the {@link ZoneState#topologyAugmentationMap()} existence in the Vault.
     * We save {@link ZoneState#topologyAugmentationMap()} in the Vault every time we receive logical topology changes from the metastore.
     *
     * @param zone Zone descriptor.
     * @param causalityToken Causality token.
     * @return Future reflecting the completion of creation or restoring a zone.
     */
    private CompletableFuture<Void> createOrRestoreZoneStateBusy(CatalogZoneDescriptor zone, long causalityToken) {
        int zoneId = zone.id();

        Entry topologyAugmentationMapLocalMetaStorage =
                metaStorageManager.getLocally(zoneTopologyAugmentation(zoneId), causalityToken);

        // First creation of a zone, or first call on the manager start for the default zone.
        if (topologyAugmentationMapLocalMetaStorage.value() == null) {
            ZoneState zoneState = new ZoneState(executor);

            ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

            assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';

            Set<Node> dataNodes = logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet());

            causalityDataNodesEngine.onCreateOrRestoreZoneState(causalityToken, zone);

            return initDataNodesAndTriggerKeysInMetaStorage(zoneId, causalityToken, dataNodes);
        } else {
            // Restart case, when topologyAugmentationMap has already been saved during a cluster work.
            ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap = fromBytes(topologyAugmentationMapLocalMetaStorage.value());

            ZoneState zoneState = new ZoneState(executor, topologyAugmentationMap);

            ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

            assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';

            Optional<Long> maxScaleUpRevision = zoneState.highestRevision(true);

            Optional<Long> maxScaleDownRevision = zoneState.highestRevision(false);

            VaultEntry filterUpdateRevision = vaultMgr.get(zonesFilterUpdateRevision()).join();

            restoreTimers(zone, zoneState, maxScaleUpRevision, maxScaleDownRevision, filterUpdateRevision);
        }

        causalityDataNodesEngine.onCreateOrRestoreZoneState(causalityToken, zone);

        return completedFuture(null);
    }

    /**
     * Restores timers that were scheduled before a node's restart.
     * Take the highest revision from the {@link ZoneState#topologyAugmentationMap()}, compare it with the revision
     * of the last update of the zone's filter and schedule scale up/scale down timers. Filter revision is taken into account because
     * any filter update triggers immediate scale up.
     *
     * @param zone Zone descriptor.
     * @param zoneState Zone's state from Distribution Zone Manager
     * @param maxScaleUpRevisionOptional Max revision from the {@link ZoneState#topologyAugmentationMap()} for node joins.
     * @param maxScaleDownRevisionOptional Max revision from the {@link ZoneState#topologyAugmentationMap()} for node removals.
     * @param filterUpdateRevisionVaultEntry Revision of the last update of the zone's filter.
     */
    private void restoreTimers(
            CatalogZoneDescriptor zone,
            ZoneState zoneState,
            Optional<Long> maxScaleUpRevisionOptional,
            Optional<Long> maxScaleDownRevisionOptional,
            VaultEntry filterUpdateRevisionVaultEntry
    ) {
        int zoneId = zone.id();

        maxScaleUpRevisionOptional.ifPresent(
                maxScaleUpRevision -> {
                    if (filterUpdateRevisionVaultEntry != null) {
                        long filterUpdateRevision = bytesToLong(filterUpdateRevisionVaultEntry.value());

                        // Immediately trigger scale up, that was planned to be invoked before restart.
                        // If this invoke was successful before restart, then current call just will be skipped.
                        saveDataNodesToMetaStorageOnScaleUp(zoneId, filterUpdateRevision);

                        if (maxScaleUpRevision < filterUpdateRevision) {
                            // Don't need to trigger additional scale up for the scenario, when filter update event happened after the last
                            // node join event.
                            return;
                        }
                    }

                    // Take the highest revision from the topologyAugmentationMap and schedule scale up/scale down,
                    // meaning that all augmentations of nodes will be taken into account in newly created timers.
                    // If augmentations have already been proposed to data nodes in the metastorage before restart,
                    // that means we have updated corresponding trigger key and it's value will be greater than
                    // the highest revision from the topologyAugmentationMap, and current timer won't affect data nodes.
                    zoneState.rescheduleScaleUp(
                            zone.dataNodesAutoAdjustScaleUp(),
                            () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, maxScaleUpRevision),
                            zoneId
                    );
                }
        );

        maxScaleDownRevisionOptional.ifPresent(
                maxScaleDownRevision -> zoneState.rescheduleScaleDown(
                        zone.dataNodesAutoAdjustScaleDown(),
                        () -> saveDataNodesToMetaStorageOnScaleDown(zoneId, maxScaleDownRevision),
                        zoneId
                )
        );
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
                    }).thenCompose((ignored) -> completedFuture(null));
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
                    .thenCompose(ignored -> completedFuture(null));
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
                    LOG.debug(
                            "Distribution zones' logical topology and version keys were updated [topology = {}, version = {}]",
                            Arrays.toString(logicalTopology.toArray()),
                            newTopology.version()
                    );
                } else {
                    LOG.debug(
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
     */
    private void restoreGlobalStateFromLocalMetastorage(long revision) {
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), revision);

        Entry nodeAttributesEntry = metaStorageManager.getLocally(zonesNodesAttributes(), revision);

        if (topologyEntry != null && topologyEntry.value() != null) {
            logicalTopology = fromBytes(topologyEntry.value());

            if (nodeAttributesEntry.value() != null) {
                nodesAttributes = fromBytes(nodeAttributesEntry.value());
            } else {
                // This possible, if after CMG has notified about new topology, but cluster restarted in the middle of
                // DistributionZoneManager.createMetastorageTopologyListener, so nodeAttributes has not been saved to MS.
                // In that case, we can initialise it with the current logical topology.
                // TODO: https://issues.apache.org/jira/browse/IGNITE-20603 restore nodeAttributes as well
                logicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));
            }
        }

        assert topologyEntry == null || topologyEntry.value() == null || logicalTopology.equals(fromBytes(topologyEntry.value()))
                : "Initial value of logical topology was changed after initialization from the Meta Storage manager.";

        assert nodeAttributesEntry == null
                || nodeAttributesEntry.value() == null
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

                    Set<NodeWithAttributes> newLogicalTopology0 = newLogicalTopology;

                    Set<Node> removedNodes =
                            logicalTopology.stream()
                                    .filter(node -> !newLogicalTopology0.contains(node))
                                    .map(NodeWithAttributes::node)
                                    .collect(toSet());

                    Set<Node> addedNodes =
                            newLogicalTopology.stream()
                                    .filter(node -> !logicalTopology.contains(node))
                                    .map(NodeWithAttributes::node)
                                    .collect(toSet());

                    // It is safe to get the latest version of the catalog as we are in the metastore thread.
                    int catalogVersion = catalogManager.latestCatalogVersion();

                    Set<Integer> zoneIds = new HashSet<>();

                    List<CompletableFuture<Void>> futures = new ArrayList<>();

                    for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
                        int zoneId = zone.id();

                        updateTopologyAugmentationMap(addedNodes, removedNodes, revision, zoneId);

                        futures.add(scheduleTimers(zone, addedNodes, removedNodes, revision));

                        zoneIds.add(zone.id());
                    }

                    newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));

                    logicalTopology = newLogicalTopology;

                    futures.add(saveGlobalStatesToMetastorage(zoneIds, revision));

                    return allOf(futures.toArray(CompletableFuture[]::new));
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
     * Update topology augmentation map with newly added and removed nodes.
     *
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision of the event that triggered this method.
     * @param zoneId Zone's id.
     */
    private void updateTopologyAugmentationMap(Set<Node> addedNodes, Set<Node> removedNodes, long revision, int zoneId) {
        if (!addedNodes.isEmpty()) {
            zonesState.get(zoneId).nodesToAddToDataNodes(addedNodes, revision);
        }

        if (!removedNodes.isEmpty()) {
            zonesState.get(zoneId).nodesToRemoveFromDataNodes(removedNodes, revision);
        }
    }

    /**
     * Saves global states of the Distribution Zone Manager to Meta Storage atomically in one batch.
     * After restart it could be used to restore these fields.
     *
     * @param zoneIds Set of zone id's, whose states will be saved in the Meta Storage.
     * @param revision Revision of the event.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<Void> saveGlobalStatesToMetastorage(Set<Integer> zoneIds, long revision) {
        Operation[] puts = new Operation[2 + zoneIds.size()];

        puts[0] = put(zonesNodesAttributes(), toBytes(nodesAttributes()));

        puts[1] = put(zonesGlobalStateRevision(), longToBytes(revision));

        int i = 2;

        // TODO: https://issues.apache.org/jira/browse/IGNITE-19491 Properly utilise topology augmentation map. Also this map
        // TODO: can be saved only once for all zones.
        for (Integer zoneId : zoneIds) {
            puts[i++] = put(zoneTopologyAugmentation(zoneId), toBytes(zonesState.get(zoneId).topologyAugmentationMap));
        }

        Iif iif = iif(
                conditionForGlobalStatesChanges(revision),
                ops(puts).yield(true),
                ops().yield(false)
        );

        return metaStorageManager.invoke(iif)
                .thenApply(StatementResult::getAsBoolean)
                .whenComplete((invokeResult, e) -> {
                    if (e != null) {
                        LOG.error("Failed to update global states for distribution zone manager [revision = {}]", e, revision);
                    } else if (invokeResult) {
                        LOG.info("Update global states for distribution zone manager [revision = {}]", revision);
                    } else {
                        LOG.debug("Failed to update global states for distribution zone manager [revision = {}]", revision);
                    }
                }).thenCompose((ignored) -> completedFuture(null));
    }

    /**
     * Schedules scale up and scale down timers.
     *
     * @param zone Zone descriptor.
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision that triggered that event.
     * @return Future that represents the pending completion of the operation.
     *         For the immediate timers it will be completed when data nodes will be updated in Meta Storage.
     */
    private CompletableFuture<Void> scheduleTimers(
            CatalogZoneDescriptor zone,
            Set<Node> addedNodes,
            Set<Node> removedNodes,
            long revision
    ) {
        return scheduleTimers(
                zone,
                addedNodes,
                removedNodes,
                revision,
                this::saveDataNodesToMetaStorageOnScaleUp,
                this::saveDataNodesToMetaStorageOnScaleDown
        );
    }

    /**
     * Schedules scale up and scale down timers. This method is needed also for test purposes.
     *
     * @param zone Zone descriptor.
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision that triggered that event.
     * @param saveDataNodesOnScaleUp Function that saves nodes to a zone's data nodes in case of scale up was triggered.
     * @param saveDataNodesOnScaleDown Function that saves nodes to a zone's data nodes in case of scale down was triggered.
     * @return Future that represents the pending completion of the operation.
     *         For the immediate timers it will be completed when data nodes will be updated in Meta Storage.
     */
    private CompletableFuture<Void> scheduleTimers(
            CatalogZoneDescriptor zone,
            Set<Node> addedNodes,
            Set<Node> removedNodes,
            long revision,
            BiFunction<Integer, Long, CompletableFuture<Void>> saveDataNodesOnScaleUp,
            BiFunction<Integer, Long, CompletableFuture<Void>> saveDataNodesOnScaleDown
    ) {
        int autoAdjust = zone.dataNodesAutoAdjust();
        int autoAdjustScaleDown = zone.dataNodesAutoAdjustScaleDown();
        int autoAdjustScaleUp = zone.dataNodesAutoAdjustScaleUp();

        int zoneId = zone.id();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if ((!addedNodes.isEmpty() || !removedNodes.isEmpty()) && autoAdjust != INFINITE_TIMER_VALUE) {
            //TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
        } else {
            if (!addedNodes.isEmpty()) {
                if (autoAdjustScaleUp == IMMEDIATE_TIMER_VALUE) {
                    futures.add(saveDataNodesToMetaStorageOnScaleUp(zoneId, revision));
                }

                if (autoAdjustScaleUp != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleUp(
                            autoAdjustScaleUp,
                            () -> saveDataNodesOnScaleUp.apply(zoneId, revision),
                            zoneId
                    );
                }
            }

            if (!removedNodes.isEmpty()) {
                if (zone.dataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE) {
                    futures.add(saveDataNodesToMetaStorageOnScaleDown(zoneId, revision));
                }

                if (autoAdjustScaleDown != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleDown(
                            autoAdjustScaleDown,
                            () -> saveDataNodesOnScaleDown.apply(zoneId, revision),
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
                return completedFuture(null);
            }

            Set<ByteArray> keysToGetFromMs = Set.of(
                    zoneDataNodesKey(zoneId),
                    zoneScaleUpChangeTriggerKey(zoneId),
                    zoneScaleDownChangeTriggerKey(zoneId)
            );

            return metaStorageManager.getAll(keysToGetFromMs).thenCompose(values -> inBusyLock(busyLock, () -> {
                if (values.containsValue(null)) {
                    // Zone was deleted
                    return completedFuture(null);
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

                    return completedFuture(null);
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

                            return completedFuture(null);
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
                return completedFuture(null);
            }

            Set<ByteArray> keysToGetFromMs = Set.of(
                    zoneDataNodesKey(zoneId),
                    zoneScaleUpChangeTriggerKey(zoneId),
                    zoneScaleDownChangeTriggerKey(zoneId)
            );

            return metaStorageManager.getAll(keysToGetFromMs).thenCompose(values -> inBusyLock(busyLock, () -> {
                if (values.containsValue(null)) {
                    // Zone was deleted
                    return completedFuture(null);
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

                    return completedFuture(null);
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

                            return completedFuture(null);
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
    public Map<String, Map<String, String>> nodesAttributes() {
        return nodesAttributes;
    }

    @TestOnly
    public Map<Integer, ZoneState> zonesState() {
        return zonesState;
    }

    @TestOnly
    public Set<NodeWithAttributes> logicalTopology() {
        return logicalTopology;
    }

    private void registerCatalogEventListenersOnStartManagerBusy() {
        catalogManager.listen(ZONE_CREATE, (parameters, exception) -> inBusyLock(busyLock, () -> {
            assert exception == null : parameters;

            CreateZoneEventParameters params = (CreateZoneEventParameters) parameters;

            return createOrRestoreZoneStateBusy(params.zoneDescriptor(), params.causalityToken())
                    .thenCompose((ignored) -> completedFuture(false));
        }));

        catalogManager.listen(ZONE_DROP, (parameters, exception) -> inBusyLock(busyLock, () -> {
            assert exception == null : parameters;

            return onDropZoneBusy((DropZoneEventParameters) parameters).thenCompose((ignored) -> completedFuture(false));
        }));

        catalogManager.listen(ZONE_ALTER, new ManagerCatalogAlterZoneEventListener());
    }

    private void startZonesOnStartManagerBusy(long revision) {
        int catalogVersion = catalogManager.latestCatalogVersion();

        // TODO: IGNITE-20287 Clean up abandoned resources for dropped zones from volt and metastore
        for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
            createOrRestoreZoneStateBusy(zone, revision);
        }
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
