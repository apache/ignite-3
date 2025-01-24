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

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;
import static java.util.Collections.unmodifiableSet;
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.INFINITE_TIMER_VALUE;
import static org.apache.ignite.internal.catalog.descriptors.ConsistencyMode.HIGH_AVAILABILITY;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_ALTER;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_CREATE;
import static org.apache.ignite.internal.catalog.events.CatalogEvent.ZONE_DROP;
import static org.apache.ignite.internal.distributionzones.DataNodesManager.addNewEntryToDataNodesHistory;
import static org.apache.ignite.internal.distributionzones.DataNodesManager.clearTimer;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.conditionForRecoverableStateChanges;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deserializeLogicalTopologySet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersionAndClusterId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesHistoryKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonePartitionResetTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpTimerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLastHandledTopology;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyClusterIdKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesRecoverableStateRevision;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.and;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.exists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notTombstone;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.metastorage.dsl.Operations.remove;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.longToBytesKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.uuidToBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLockAsync;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.AlterZoneEventParameters;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.catalog.events.CreateZoneEventParameters;
import org.apache.ignite.internal.catalog.events.DropZoneEventParameters;
import org.apache.ignite.internal.causality.RevisionListenerRegistry;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.SystemDistributedConfiguration;
import org.apache.ignite.internal.configuration.utils.SystemDistributedConfigurationPropertyHolder;
import org.apache.ignite.internal.distributionzones.DataNodesManager.DataNodesHistory;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEvent;
import org.apache.ignite.internal.distributionzones.events.HaZoneTopologyUpdateEventParams;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.distributionzones.utils.CatalogAlterZoneEventListener;
import org.apache.ignite.internal.event.AbstractEventProducer;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.Revisions;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.Iif;
import org.apache.ignite.internal.metastorage.dsl.Operation;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.exceptions.CompactedException;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.thread.StripedScheduledThreadPoolExecutor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager extends
        AbstractEventProducer<HaZoneTopologyUpdateEvent, HaZoneTopologyUpdateEventParams> implements IgniteComponent {
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

    private DataNodesManager dataNodesManager;

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new DistributionZoneManagerLogicalTopologyEventListener();

    /**
     * The logical topology mapped to the MS revision.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-23561 get rid of this map (or properly clean up it).
     */
    private final ConcurrentSkipListMap<Long, Set<NodeWithAttributes>> logicalTopologyByRevision = new ConcurrentSkipListMap<>();

    /**
     * Local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-19491 properly clean up this map
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter documentation</a>
     */
    private Map<UUID, NodeWithAttributes> nodesAttributes = new ConcurrentHashMap<>();

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Rebalance engine. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /** Configuration of HA mode. */
    private final SystemDistributedConfigurationPropertyHolder<Integer> partitionDistributionResetTimeoutConfiguration;

    /**
     * Creates a new distribution zone manager.
     *
     * @param nodeName Node name.
     * @param registry Registry for versioned values.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param catalogManager Catalog manager.
     * @param systemDistributedConfiguration System distributed configuration.
     */
    public DistributionZoneManager(
            String nodeName,
            RevisionListenerRegistry registry,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            CatalogManager catalogManager,
            SystemDistributedConfiguration systemDistributedConfiguration
    ) {
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
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

        partitionDistributionResetTimeoutConfiguration = new SystemDistributedConfigurationPropertyHolder<>(
                systemDistributedConfiguration,
                this::onUpdatePartitionDistributionResetBusy,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT,
                PARTITION_DISTRIBUTION_RESET_TIMEOUT_DEFAULT_VALUE,
                Integer::parseInt
        );
    }

    @Override
    public CompletableFuture<Void> startAsync(ComponentContext componentContext) {
        return inBusyLockAsync(busyLock, () -> {
            partitionDistributionResetTimeoutConfiguration.init();

            registerCatalogEventListenersOnStartManagerBusy();

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), topologyWatchListener);

            CompletableFuture<Revisions> recoveryFinishFuture = metaStorageManager.recoveryFinishedFuture();

            // At the moment of the start of this manager, it is guaranteed that Meta Storage has been recovered.
            assert recoveryFinishFuture.isDone();

            long recoveryRevision = recoveryFinishFuture.join().revision();

            restoreGlobalStateFromLocalMetastorage(recoveryRevision);

            // If Catalog manager is empty, it gets initialized asynchronously and at this moment the initialization might not complete,
            // nevertheless everything works correctly.
            // All components execute the synchronous part of startAsync sequentially and only when they all complete,
            // we enable metastorage listeners (see IgniteImpl.joinClusterAsync: metaStorageMgr.deployWatches()).
            // Once the metstorage watches are deployed, all components start to receive callbacks, this chain of callbacks eventually
            // fires CatalogManager's ZONE_CREATE event, and the state of DistributionZoneManager becomes consistent.
            int catalogVersion = catalogManager.latestCatalogVersion();

            return allOf(
                    restoreLogicalTopologyChangeEventAndStartTimers(recoveryRevision, catalogVersion)
            ).thenComposeAsync((notUsed) -> rebalanceEngine.startAsync(catalogVersion), componentContext.executor());
        });
    }

    @Override
    public CompletableFuture<Void> stopAsync(ComponentContext componentContext) {
        if (!stopGuard.compareAndSet(false, true)) {
            return nullCompletedFuture();
        }

        busyLock.block();

        dataNodesManager.stop();

        rebalanceEngine.stop();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);

        shutdownAndAwaitTermination(executor, 10, SECONDS);

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
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(causalityToken);

        return dataNodesManager.dataNodes(zoneId, timestamp);
    }

    private CompletableFuture<Void> onUpdateScaleUpBusy(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), parameters.causalityToken());

        if (topologyEntry != null) {
            Set<NodeWithAttributes> logicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            return dataNodesManager.onAutoAdjustAlteration(
                    parameters.zoneDescriptor(),
                    timestamp,
                    parameters.previousDescriptor().dataNodesAutoAdjustScaleUp(),
                    parameters.previousDescriptor().dataNodesAutoAdjustScaleDown(),
                    logicalTopology
            );
        }

        // TODO ???
        return nullCompletedFuture();

        /*int zoneId = parameters.zoneDescriptor().id();

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

        return nullCompletedFuture();*/
    }

    private void onUpdatePartitionDistributionResetBusy(
            int partitionDistributionResetTimeoutSeconds,
            long causalityToken
    ) {
        CompletableFuture<Revisions> recoveryFuture = metaStorageManager.recoveryFinishedFuture();

        // At the moment of the first call to this method from configuration notifications,
        // it is guaranteed that Meta Storage has been recovered.
        assert recoveryFuture.isDone();

        if (recoveryFuture.join().revision() >= causalityToken) {
            // So, configuration already has the right value on configuration init
            // and all timers started with the right configuration timeouts on recovery.
            return;
        }

        long updateTimestamp = timestampByRevision(causalityToken);

        if (updateTimestamp == -1) {
            return;
        }

        int catalogVersion = catalogManager.activeCatalogVersion(updateTimestamp);

        // It is safe to zoneState.entrySet in term of ConcurrentModification and etc. because meta storage notifications are one-threaded
        // and this map will be initialized on a manager start or with catalog notification or with distribution configuration changes.
        for (CatalogZoneDescriptor zoneDescriptor : catalogManager.zones(catalogVersion)) {
            int zoneId = zoneDescriptor.id();

            if (zoneDescriptor.consistencyMode() != HIGH_AVAILABILITY) {
                continue;
            }

            dataNodesManager.onUpdatePartitionDistributionReset(
                    zoneId,
                    partitionDistributionResetTimeoutSeconds,
                    () -> fireTopologyReduceLocalEvent(causalityToken, zoneId)
            );
        }
    }

    private CompletableFuture<Void> onUpdateScaleDownBusy(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), parameters.causalityToken());

        if (topologyEntry != null) {
            Set<NodeWithAttributes> logicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            return dataNodesManager.onAutoAdjustAlteration(
                    parameters.zoneDescriptor(),
                    timestamp,
                    parameters.previousDescriptor().dataNodesAutoAdjustScaleUp(),
                    parameters.previousDescriptor().dataNodesAutoAdjustScaleDown(),
                    logicalTopology
            );
        }

        // TODO ???
        return nullCompletedFuture();


        /*int zoneId = parameters.zoneDescriptor().id();

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

        return nullCompletedFuture();*/
    }

    private CompletableFuture<Void> onUpdateFilter(AlterZoneEventParameters parameters) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(parameters.causalityToken());

        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), parameters.causalityToken());

        if (topologyEntry != null) {
            Set<NodeWithAttributes> logicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            dataNodesManager.onZoneFilterChange(parameters.zoneDescriptor(), timestamp, logicalTopology);
        }
        /*int zoneId = parameters.zoneDescriptor().id();

        long causalityToken = parameters.causalityToken();

        return saveDataNodesToMetaStorageOnScaleUp(zoneId, causalityToken);*/
    }

    private CompletableFuture<Void> onCreateZone(CatalogZoneDescriptor zone, long causalityToken) {
        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(causalityToken);

        return initDataNodesKeysInMetaStorage(zone.id(), timestamp, filterDataNodes(logicalTopology(causalityToken), zone));
    }

    /**
     * Method initialise data nodes value for the specified zone, also sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} and {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)}
     * if it passes the condition. It is called on the first creation of a zone.
     *
     * @param zoneId Unique id of a zone
     * @param timestamp Timestamp of an event that has triggered this method.
     * @param dataNodes Data nodes.
     * @return Future reflecting the completion of initialisation of zone's keys in meta storage.
     */
    private CompletableFuture<Void> initDataNodesKeysInMetaStorage(
            int zoneId,
            HybridTimestamp timestamp,
            Set<NodeWithAttributes> dataNodes
    ) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the corresponding data nodes keys weren't initialised in ms yet.
            Condition condition = and(
                    notExists(zoneDataNodesHistoryKey(zoneId)),
                    notTombstone(zoneDataNodesHistoryKey(zoneId))
            );

            Update update = ops(
                    addNewEntryToDataNodesHistory(zoneId, new DataNodesHistory(), timestamp, dataNodes),
                    clearTimer(zoneScaleUpTimerKey(zoneId)),
                    clearTimer(zoneScaleDownTimerKey(zoneId)),
                    clearTimer(zonePartitionResetTimerKey(zoneId))
            ).yield(true);

            Iif iif = iif(condition, update, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, timestamp = {}]",
                                    e,
                                    zoneId,
                                    dataNodes,
                                    timestamp
                            );
                        } else if (invokeResult) {
                            LOG.info("Update zones' dataNodes value [zoneId = {}, dataNodes = {}, timestamp = {}]",
                                    zoneId,
                                    dataNodes,
                                    timestamp
                            );
                        } else {
                            LOG.debug(
                                    "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, timestamp = {}]",
                                    zoneId,
                                    dataNodes,
                                    timestamp
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
     * @param timestamp Timestamp of an event that has triggered this method.
     */
    private CompletableFuture<Void> removeDataNodesKeys(int zoneId, HybridTimestamp timestamp) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            Condition condition = exists(zoneDataNodesHistoryKey(zoneId));

            Update removeKeysUpd = ops(
                    remove(zoneDataNodesHistoryKey(zoneId)),
                    remove(zoneScaleUpTimerKey(zoneId)),
                    remove(zoneScaleDownTimerKey(zoneId)),
                    remove(zonePartitionResetTimerKey(zoneId))
            ).yield(true);

            Iif iif = iif(condition, removeKeysUpd, ops().yield(false));

            return metaStorageManager.invoke(iif)
                    .thenApply(StatementResult::getAsBoolean)
                    .whenComplete((invokeResult, e) -> {
                        if (e != null) {
                            LOG.error(
                                    "Failed to delete zone's dataNodes keys [zoneId = {}, timestamp = {}]",
                                    e,
                                    zoneId,
                                    timestamp
                            );
                        } else if (invokeResult) {
                            LOG.info("Delete zone's dataNodes keys [zoneId = {}, timestamp = {}]", zoneId, timestamp);
                        } else {
                            LOG.debug("Failed to delete zone's dataNodes keys [zoneId = {}, timestamp = {}]", zoneId, timestamp);
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

            Condition condition;
            Update update;

            if (newTopology.version() == LogicalTopologySnapshot.FIRST_VERSION) {
                // Very first start of the cluster, OR first topology version after a cluster reset, so we just
                // initialize zonesLogicalTopologyVersionKey.
                // We don't need to check whether clusterId is 'newer' as it's guaranteed that after a newer clusterId
                // gets written to the Metastorage, we cannot send a Metastorage update switching it back to older clusterId.
                condition = notExists(zonesLogicalTopologyVersionKey())
                        .or(value(zonesLogicalTopologyClusterIdKey()).ne(uuidToBytes(newTopology.clusterId())));
                update = updateLogicalTopologyAndVersionAndClusterId(newTopology);
            } else {
                condition = value(zonesLogicalTopologyVersionKey()).lt(longToBytesKeepingOrder(newTopology.version()));
                update = updateLogicalTopologyAndVersion(newTopology);
            }

            Iif iff = iif(condition, update, ops().yield(false));

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

            logicalTopologyByRevision.put(recoveryRevision, deserializeLogicalTopologySet(lastHandledTopologyEntry.value()));

            nodesAttributes = DistributionZonesUtil.deserializeNodesAttributes(nodeAttributesEntry.value());
        }

        assert lastHandledTopologyEntry.value() == null
                || logicalTopology(recoveryRevision).equals(deserializeLogicalTopologySet(lastHandledTopologyEntry.value()))
                : "Initial value of logical topology was changed after initialization from the Meta Storage manager.";

        assert nodeAttributesEntry.value() == null
                || nodesAttributes.equals(DistributionZonesUtil.deserializeNodesAttributes(nodeAttributesEntry.value()))
                : "Initial value of nodes' attributes was changed after initialization from the Meta Storage manager.";
    }

    /**
     * Creates watch listener which listens logical topology and logical topology version.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageTopologyListener() {
        return evt -> {
            if (!busyLock.enterBusy()) {
                return failedFuture(new NodeStoppingException());
            }

            try {
                assert evt.entryEvents().size() == 2 || evt.entryEvents().size() == 3 :
                        "Expected an event with logical topology, its version and maybe clusterId entries but was events with keys: "
                        + evt.entryEvents().stream().map(DistributionZoneManager::entryKeyAsString)
                        .collect(toList());

                byte[] newLogicalTopologyBytes;

                Set<NodeWithAttributes> newLogicalTopology = null;
                Set<NodeWithAttributes> oldLogicalTopology = emptySet();

                HybridTimestamp timestamp = evt.timestamp();

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopologyBytes = e.value();

                        assert newLogicalTopologyBytes != null : "New topology is null.";

                        newLogicalTopology = deserializeLogicalTopologySet(newLogicalTopologyBytes);

                        Entry oldEntry = event.oldEntry();
                        if (oldEntry != null && oldEntry.value() != null && !oldEntry.empty() && !oldEntry.tombstone()) {
                            oldLogicalTopology = deserializeLogicalTopologySet(oldEntry.value());
                        }
                    }
                }

                assert newLogicalTopology != null : "The event doesn't contain logical topology";

                return onLogicalTopologyUpdate(newLogicalTopology, oldLogicalTopology, timestamp);
            } finally {
                busyLock.leaveBusy();
            }
        };
    }

    private static String entryKeyAsString(EntryEvent entry) {
        return entry.newEntry() == null ? "null" : new String(entry.newEntry().key(), UTF_8);
    }

    /**
     * Reaction on an update of logical topology. In this method {@link DistributionZoneManager#logicalTopology},
     * {@link DistributionZoneManager#nodesAttributes}, {@link ZoneState#topologyAugmentationMap} are updated.
     * This fields are saved to Meta Storage, also timers are scheduled.
     * Note that all futures of Meta Storage updates that happen in this method are returned from this method.
     *
     * @param newLogicalTopology New logical topology.
     * @param oldLogicalTopology Old logical topology.
     * @param timestamp Event timestamp.
     * @return Future reflecting the completion of the actions needed when logical topology was updated.
     */
    private CompletableFuture<Void> onLogicalTopologyUpdate(
            Set<NodeWithAttributes> newLogicalTopology,
            Set<NodeWithAttributes> oldLogicalTopology,
            HybridTimestamp timestamp
    ) {
        Set<Node> removedNodes =
                oldLogicalTopology.stream()
                        .filter(node -> !newLogicalTopology.contains(node))
                        .map(NodeWithAttributes::node)
                        .collect(toSet());

        Set<Node> addedNodes =
                newLogicalTopology.stream()
                        .filter(node -> !oldLogicalTopology.contains(node))
                        .map(NodeWithAttributes::node)
                        .collect(toSet());

        Set<Integer> zoneIds = new HashSet<>();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        int catalogVersion = catalogManager.activeCatalogVersion(timestamp.longValue());

        for (CatalogZoneDescriptor zone : catalogManager.zones(catalogVersion)) {
            CompletableFuture<Void> f = dataNodesManager.onTopologyChangeZoneHandler(
                    zone,
                    timestamp,
                    oldLogicalTopology,
                    newLogicalTopology
            );

            futures.add(f);

            zoneIds.add(zone.id());
        }

        newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n));

        //futures.add(saveRecoverableStateToMetastorage(zoneIds, revision, newLogicalTopology));

        return allOf(futures.toArray(CompletableFuture[]::new));
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
        // TODO
        Operation[] puts = new Operation[3 + zoneIds.size()];

        puts[0] = put(zonesNodesAttributes(), NodesAttributesSerializer.serialize(nodesAttributes()));

        puts[1] = put(zonesRecoverableStateRevision(), longToBytesKeepingOrder(revision));

        puts[2] = put(
                zonesLastHandledTopology(),
                LogicalTopologySetSerializer.serialize(newLogicalTopology)
        );

        int i = 3;

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
     * @param timestamp Timestamp.
     * @return Future that represents the pending completion of the operation.
     *         For the immediate timers it will be completed when data nodes will be updated in Meta Storage.
     */
    private CompletableFuture<Void> scheduleTimers(
            CatalogZoneDescriptor zone,
            boolean nodesAdded,
            boolean nodesRemoved,
            HybridTimestamp timestamp
    ) {
        int autoAdjust = zone.dataNodesAutoAdjust();
        int autoAdjustScaleDown = zone.dataNodesAutoAdjustScaleDown();
        int autoAdjustScaleUp = zone.dataNodesAutoAdjustScaleUp();
        int partitionResetDelay = partitionDistributionResetTimeoutConfiguration.currentValue();

        int zoneId = zone.id();

        List<CompletableFuture<Void>> futures = new ArrayList<>();

        if ((nodesAdded || nodesRemoved) && autoAdjust != INFINITE_TIMER_VALUE) {
            // TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
        } else {
            if (nodesAdded) {
                if (autoAdjustScaleUp != INFINITE_TIMER_VALUE) {

                }
            }

            if (nodesRemoved) {
                if (zone.consistencyMode() == HIGH_AVAILABILITY) {
                    if (partitionResetDelay != INFINITE_TIMER_VALUE) {

                    }
                }

                if (autoAdjustScaleDown != INFINITE_TIMER_VALUE) {

                }
            }
        }

        return allOf(futures.toArray(CompletableFuture[]::new));
    }

    /**
     * Returns metastore long view of {@link org.apache.ignite.internal.hlc.HybridTimestamp} by revision.
     *
     * @param revision Metastore revision.
     * @return Appropriate metastore timestamp or -1 if revision is already compacted.
     */
    private long timestampByRevision(long revision) {
        try {
            return metaStorageManager.timestampByRevisionLocally(revision).longValue();
        } catch (CompactedException e) {
            if (revision > 1) {
                LOG.warn("Unable to retrieve timestamp by revision because of meta storage compaction, [revision={}].", revision);
            }

            return -1;
        }

    }

    private void fireTopologyReduceLocalEvent(long revision, int zoneId) {
        fireEvent(
                HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED,
                new HaZoneTopologyUpdateEventParams(zoneId, revision)
        ).exceptionally(th -> {
            LOG.error("Error during the local " + HaZoneTopologyUpdateEvent.TOPOLOGY_REDUCED.name()
                    + " event processing", th);

            return null;
        });
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

        /** Schedule task for a partition distribution reset process. */
        private ScheduledFuture<?> partitionDistributionResetTask;

        /** The delay for the scale up task. */
        private long scaleUpTaskDelay;

        /** The delay for the scale down task. */
        private long scaleDownTaskDelay;

        /** The delay for the partition distribution reset task. */
        private long partitionDistributionResetTaskDelay;

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
         * Reschedules existing partition distribution reset task, if it is not started yet and the delay of this task is not immediate,
         * or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         * @param zoneId Unique id of a zone to determine the executor of the task.
         */
        public synchronized void reschedulePartitionDistributionReset(long delay, Runnable runnable, int zoneId) {
            stopPartitionDistributionReset();

            partitionDistributionResetTask = executor.schedule(runnable, delay, SECONDS, zoneId);

            partitionDistributionResetTaskDelay = delay;
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

            if (partitionDistributionResetTask != null) {
                partitionDistributionResetTask.cancel(false);
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
         * Cancels task for partition distribution reset if it is not started yet and the delay of this task is not immediate.
         */
        synchronized void stopPartitionDistributionReset() {
            if (partitionDistributionResetTask != null && partitionDistributionResetTaskDelay > 0) {
                partitionDistributionResetTask.cancel(false);
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

        /**
         * Returns the highest revision which is presented in the {@link ZoneState#topologyAugmentationMap()}.
         *
         * @return The highest revision which is presented in the {@link ZoneState#topologyAugmentationMap()}.
         */
        Optional<Long> highestRevision() {
            return topologyAugmentationMap().keySet()
                    .stream()
                    .max(Comparator.naturalOrder());
        }

        @TestOnly
        public synchronized ScheduledFuture<?> scaleUpTask() {
            return scaleUpTask;
        }

        @TestOnly
        public synchronized ScheduledFuture<?> scaleDownTask() {
            return scaleDownTask;
        }

        @TestOnly
        public synchronized ScheduledFuture<?> partitionDistributionResetTask() {
            return partitionDistributionResetTask;
        }
    }

    /**
     * Class stores the info about nodes that should be added or removed from the data nodes of a zone.
     * With flag {@code addition} we can track whether {@code nodeNames} should be added or removed.
     */
    public static class Augmentation {
        /** Nodes. */
        private final Set<Node> nodes;

        /** Flag that indicates whether {@code nodeNames} should be added or removed. */
        private final boolean addition;

        public Augmentation(Set<Node> nodes, boolean addition) {
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
    public Map<UUID, NodeWithAttributes> nodesAttributes() {
        return nodesAttributes;
    }

    @TestOnly
    public Map<Integer, ZoneState> zonesState() {
        // TODO remove this
    }

    public Set<NodeWithAttributes> logicalTopology() {
        return logicalTopology(Long.MAX_VALUE);
    }

    /**
     * Get logical topology for the given revision.
     * If there is no data for revision i, return topology for the maximum revision smaller than i.
     *
     * @param revision metastore revision.
     * @return logical topology.
     */
    public Set<NodeWithAttributes> logicalTopology(long revision) {
        assert revision >= 0 : revision;

        Map.Entry<Long, Set<NodeWithAttributes>> entry = logicalTopologyByRevision.floorEntry(revision);

        return entry != null ? entry.getValue() : emptySet();
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

    /**
     * Restore the event of the updating the logical topology from Meta Storage, that has not been completed before restart.
     * Also start scale up/scale down timers.
     *
     * @param recoveryRevision Revision of the Meta Storage after its recovery.
     * @return Future that represents the pending completion of the operations.
     */
    private CompletableFuture<Void> restoreLogicalTopologyChangeEventAndStartTimers(long recoveryRevision, int catalogVersion) {
        Entry topologyEntry = metaStorageManager.getLocally(zonesLogicalTopologyKey(), recoveryRevision);

        if (topologyEntry.value() != null) {
            Set<NodeWithAttributes> newLogicalTopology = deserializeLogicalTopologySet(topologyEntry.value());

            long topologyRevision = topologyEntry.revision();

            Entry lastUpdateRevisionEntry = metaStorageManager.getLocally(zonesRecoverableStateRevision(), recoveryRevision);

            if (lastUpdateRevisionEntry.value() == null || topologyRevision > bytesToLongKeepingOrder(lastUpdateRevisionEntry.value())) {
                HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(recoveryRevision);

                return onLogicalTopologyUpdate(newLogicalTopology, timestamp);
            }

            dataNodesManager.start(catalogManager.zones(catalogVersion), newLogicalTopology);
        }

        return nullCompletedFuture();
    }

    private CompletableFuture<Void> onDropZoneBusy(DropZoneEventParameters parameters) {
        long causalityToken = parameters.causalityToken();

        HybridTimestamp timestamp = metaStorageManager.timestampByRevisionLocally(causalityToken);

        return removeDataNodesKeys(parameters.zoneId(), timestamp)
                .thenRun(() -> dataNodesManager.onZoneDrop(parameters.zoneId(), timestamp));
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

    private class DistributionZoneManagerLogicalTopologyEventListener implements LogicalTopologyEventListener {
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
    }
}
