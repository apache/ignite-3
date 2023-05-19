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
import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesAndUpdateTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractChangeTriggerRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.isZoneExist;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.toDataNodesMap;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerKeyConditionForZonesChanges;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerScaleUpScaleDownKeysCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndScaleDownTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndScaleUpTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationNodeAlreadyExistException;
import org.apache.ignite.configuration.ConfigurationNodeDoesNotExistException;
import org.apache.ignite.configuration.ConfigurationNodeRemovedException;
import org.apache.ignite.configuration.ConfigurationProperty;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfigurationSchema;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneBindTableException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneWasRemovedException;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
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
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** Name of the default distribution zone. */
    public static final String DEFAULT_ZONE_NAME = "Default";

    private static final String DISTRIBUTION_ZONE_MANAGER_POOL_NAME = "dst-zones-scheduler";

    /** Id of the default distribution zone. */
    public static final int DEFAULT_ZONE_ID = 0;

    /**
     * Default filter value for a distribution zone,
     * which is a {@link com.jayway.jsonpath.JsonPath} expression for including all attributes of nodes.
     */
    public static final String DEFAULT_FILTER = "$..*";

    /** Default number of zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 1;

    /** Default number of zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;

    /**
     * Value for the distribution zones' timers which means that data nodes changing for distribution zone
     * will be started without waiting.
     */
    public static final int IMMEDIATE_TIMER_VALUE = 0;

    /** Default infinite value for the distribution zones' timers. */
    public static final int INFINITE_TIMER_VALUE = Integer.MAX_VALUE;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

    /**
     * If this property is set to {@code true} then an attempt to get the configuration property directly from Meta storage will be skipped,
     * and the local property will be returned.
     * TODO: IGNITE-16774 This property and overall approach, access configuration directly through Meta storage,
     * TODO: will be removed after fix of the issue.
     */
    private final boolean getMetadataLocallyOnly = IgniteSystemProperties.getBoolean("IGNITE_GET_METADATA_LOCALLY_ONLY");

    /** Distribution zone configuration. */
    private final DistributionZonesConfiguration zonesConfiguration;

    /** Tables configuration. */
    private final TablesConfiguration tablesConfiguration;

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
    private final ScheduledExecutorService executor;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState;

    /** The tracker for the last topology version which was observed by distribution zone manager. */
    private final PendingComparableValuesTracker<Long, Void> topVerTracker;

    /** The last meta storage revision on which scale up timer was started. */
    private volatile long lastScaleUpRevision;

    /** The last meta storage revision on which scale down timer was started. */
    private volatile long lastScaleDownRevision;

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new LogicalTopologyEventListener() {
        @Override
        public void onNodeJoined(LogicalNode joinedNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology, false);
        }

        @Override
        public void onNodeLeft(LogicalNode leftNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology, false);
        }

        @Override
        public void onTopologyLeap(LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology, true);
        }
    };

    /**
     * The logical topology on the last watch event.
     * It's enough to mark this field by volatile because we don't update the collection after it is assigned to the field.
     */
    private volatile Set<NodeWithAttributes> logicalTopology;

    /**
     * Local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-19491 properly clean up this map
     */
    private final Map<String, Map<String, String>> nodesAttributes;

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Watch listener for data nodes keys. */
    private final WatchListener dataNodesWatchListener;

    /** Watch listener for data nodes keys. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     * @param tablesConfiguration Tables configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param vaultMgr Vault manager.
     * @param nodeName Node name.
     */
    public DistributionZoneManager(
            DistributionZonesConfiguration zonesConfiguration,
            TablesConfiguration tablesConfiguration,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            VaultManager vaultMgr,
            String nodeName
    ) {
        this.zonesConfiguration = zonesConfiguration;
        this.tablesConfiguration = tablesConfiguration;
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.vaultMgr = vaultMgr;

        this.topologyWatchListener = createMetastorageTopologyListener();

        this.dataNodesWatchListener = createMetastorageDataNodesListener();

        zonesState = new ConcurrentHashMap<>();

        logicalTopology = emptySet();

        nodesAttributes = new ConcurrentHashMap<>();

        executor = new ScheduledThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, DISTRIBUTION_ZONE_MANAGER_POOL_NAME), LOG),
                new ThreadPoolExecutor.DiscardPolicy()
        );

        topVerTracker = new PendingComparableValuesTracker<>(0L);

        // It's safe to leak with partially initialised object here, because rebalanceEngine is only accessible through this or by
        // meta storage notification thread that won't start before all components start.
        //noinspection ThisEscapedInObjectConstruction
        rebalanceEngine = new DistributionZoneRebalanceEngine(
                stopGuard,
                busyLock,
                zonesConfiguration,
                tablesConfiguration,
                metaStorageManager,
                this
        );
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            ZonesConfigurationListener zonesConfigurationListener = new ZonesConfigurationListener();

            zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
            zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
            zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());

            zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
            zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
            zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());

            rebalanceEngine.start();

            // Init timers after restart.
            zonesState.putIfAbsent(DEFAULT_ZONE_ID, new ZoneState(executor));

            zonesConfiguration.distributionZones().value().forEach(zone -> {
                int zoneId = zone.zoneId();

                zonesState.putIfAbsent(zoneId, new ZoneState(executor));
            });

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zoneLogicalTopologyPrefix(), topologyWatchListener);
            metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), dataNodesWatchListener);

            initDataNodesFromVaultManager();

            initLogicalTopologyAndVersionInMetaStorageOnStart();
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        rebalanceEngine.stop();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);
        metaStorageManager.unregisterWatch(dataNodesWatchListener);

        //Need to update trackers with max possible value to complete all futures that are waiting for trackers.
        topVerTracker.update(Long.MAX_VALUE, null);

        zonesState.values().forEach(zoneState -> {
            zoneState.scaleUpRevisionTracker().update(Long.MAX_VALUE, null);
            zoneState.scaleDownRevisionTracker().update(Long.MAX_VALUE, null);
        });

        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    /**
     * Creates a new distribution zone with the given {@code name} asynchronously.
     *
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future with the id of the zone. Future can be completed with:
     *      {@link DistributionZoneAlreadyExistsException} if a zone with the given name already exists,
     *      {@link ConfigurationValidationException} if {@code distributionZoneCfg} is broken,
     *      {@link IllegalArgumentException} if distribution zone configuration is null
     *      or distribution zone name is {@code DEFAULT_ZONE_NAME},
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Integer> createZone(DistributionZoneConfigurationParameters distributionZoneCfg) {
        if (distributionZoneCfg == null) {
            return failedFuture(new IllegalArgumentException("Distribution zone configuration is null"));
        }

        if (DEFAULT_ZONE_NAME.equals(distributionZoneCfg.name())) {
            return failedFuture(
                    new IllegalArgumentException("It's not possible to create distribution zone with [name= " + DEFAULT_ZONE_NAME + ']')
            );
        }

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<Integer> fut = new CompletableFuture<>();

            int[] zoneIdContainer = new int[1];

            zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                try {
                    zonesListChange.create(distributionZoneCfg.name(), zoneChange -> {
                        if (distributionZoneCfg.partitions() != null) {
                            zoneChange.changePartitions(distributionZoneCfg.partitions());
                        }

                        if (distributionZoneCfg.dataStorageChangeConsumer() == null) {
                            zoneChange.changeDataStorage(ch -> ch.convert(zonesConfiguration.defaultDataStorage().value()));
                        } else {
                            zoneChange.changeDataStorage(distributionZoneCfg.dataStorageChangeConsumer());
                        }

                        if (distributionZoneCfg.replicas() != null) {
                            zoneChange.changeReplicas(distributionZoneCfg.replicas());
                        }

                        if (distributionZoneCfg.filter() != null) {
                            zoneChange.changeFilter(distributionZoneCfg.filter());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjust() != null) {
                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() == null) {
                            if (distributionZoneCfg.dataNodesAutoAdjust() != null) {
                                zoneChange.changeDataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE);
                            }
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() != null) {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                        }

                        int intZoneId = zonesChange.globalIdCounter() + 1;
                        zonesChange.changeGlobalIdCounter(intZoneId);

                        zoneChange.changeZoneId(intZoneId);
                        zoneIdContainer[0] = intZoneId;
                    });
                } catch (ConfigurationNodeAlreadyExistException e) {
                    throw new DistributionZoneAlreadyExistsException(distributionZoneCfg.name(), e);
                }
            })).whenComplete((res, e) -> {
                if (e != null) {
                    fut.completeExceptionally(
                            unwrapDistributionZoneException(
                                    e,
                                    DistributionZoneAlreadyExistsException.class,
                                    ConfigurationValidationException.class)
                    );
                } else {
                    fut.complete(zoneIdContainer[0]);
                }
            });

            return fut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Alters a distribution zone.
     *
     * @param name Distribution zone name.
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation. Future can be completed with:
     *      {@link DistributionZoneAlreadyExistsException} if a zone with the given name already exists.
     *      {@link DistributionZoneNotFoundException} if a zone with the given name doesn't exist,
     *      {@link ConfigurationValidationException} if {@code distributionZoneCfg} is broken,
     *      {@link IllegalArgumentException} if {@code name} or {@code distributionZoneCfg} is {@code null}
     *      or it is an attempt to rename default distribution zone,
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneConfigurationParameters distributionZoneCfg) {
        if (name == null || name.isEmpty()) {
            return failedFuture(new IllegalArgumentException("Distribution zone name is null or empty [name=" + name + ']'));
        }

        if (distributionZoneCfg == null) {
            return failedFuture(new IllegalArgumentException("Distribution zone configuration is null"));
        }

        if (DEFAULT_ZONE_NAME.equals(name) && !DEFAULT_ZONE_NAME.equals(distributionZoneCfg.name())) {
            return failedFuture(
                    new IllegalArgumentException("It's not possible to rename default distribution zone")
            );
        }

        if (!DEFAULT_ZONE_NAME.equals(name) && DEFAULT_ZONE_NAME.equals(distributionZoneCfg.name())) {
            return failedFuture(
                    new IllegalArgumentException("It's not possible to rename distribution zone to [name= " + DEFAULT_ZONE_NAME + ']')
            );
        }

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<Void> fut = new CompletableFuture<>();

            CompletableFuture<Void> change;

            if (DEFAULT_ZONE_NAME.equals(name)) {
                change = zonesConfiguration.change(
                        zonesChange -> zonesChange.changeDefaultDistributionZone(
                                zoneChange -> updateZoneChange(zoneChange, distributionZoneCfg)
                        )
                );
            } else {
                change = zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                    NamedListChange<DistributionZoneView, DistributionZoneChange> renameChange;

                    try {
                        renameChange = zonesListChange.rename(name, distributionZoneCfg.name());
                    } catch (ConfigurationNodeAlreadyExistException e) {
                        throw new DistributionZoneAlreadyExistsException(distributionZoneCfg.name(), e);
                    } catch (ConfigurationNodeDoesNotExistException | ConfigurationNodeRemovedException e) {
                        throw new DistributionZoneNotFoundException(distributionZoneCfg.name(), e);
                    }

                    try {
                        renameChange.update(distributionZoneCfg.name(), zoneChange -> updateZoneChange(zoneChange, distributionZoneCfg));
                    } catch (ConfigurationNodeDoesNotExistException | ConfigurationNodeRemovedException e) {
                        throw new DistributionZoneNotFoundException(distributionZoneCfg.name(), e);
                    }
                }));
            }

            change.whenComplete((res, e) -> {
                if (e != null) {
                    fut.completeExceptionally(
                            unwrapDistributionZoneException(
                                    e,
                                    DistributionZoneNotFoundException.class,
                                    DistributionZoneAlreadyExistsException.class,
                                    ConfigurationValidationException.class)
                    );
                } else {
                    fut.complete(null);
                }
            });

            return fut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops a distribution zone with the name specified.
     *
     * @param name Distribution zone name.
     * @return Future representing pending completion of the operation. Future can be completed with:
     *      {@link DistributionZoneNotFoundException} if a zone with the given name doesn't exist,
     *      {@link IllegalArgumentException} if {@code name} is {@code null} or distribution zone name is {@code DEFAULT_ZONE_NAME},
     *      {@link DistributionZoneBindTableException} if the zone is bound to table,
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> dropZone(String name) {
        if (name == null || name.isEmpty()) {
            return failedFuture(new IllegalArgumentException("Distribution zone name is null or empty [name=" + name + ']'));
        }

        if (DEFAULT_ZONE_NAME.equals(name)) {
            return failedFuture(new IllegalArgumentException("Default distribution zone cannot be dropped."));
        }

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<Void> fut = new CompletableFuture<>();

            zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                DistributionZoneView zoneView = zonesListChange.get(name);

                if (zoneView == null) {
                    throw new DistributionZoneNotFoundException(name);
                }

                NamedConfigurationTree<TableConfiguration, TableView, TableChange> tables = tablesConfiguration.tables();

                for (int i = 0; i < tables.value().size(); i++) {
                    TableView tableView = tables.value().get(i);
                    int tableZoneId = tableView.zoneId();

                    if (zoneView.zoneId() == tableZoneId) {
                        throw new DistributionZoneBindTableException(name, tableView.name());
                    }
                }

                zonesListChange.delete(name);
            }))
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            fut.completeExceptionally(
                                    unwrapDistributionZoneException(
                                            e,
                                            DistributionZoneNotFoundException.class,
                                            DistributionZoneBindTableException.class)
                            );
                        } else {
                            fut.complete(null);
                        }
                    });

            return fut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets zone id by zone name.
     *
     * @param name Distribution zone name.
     * @return The zone id.
     * @throws DistributionZoneNotFoundException If the zone is not exist..
     */
    public int getZoneId(String name) {
        if (DEFAULT_ZONE_NAME.equals(name)) {
            return DEFAULT_ZONE_ID;
        }

        DistributionZoneConfiguration zoneCfg = zonesConfiguration.distributionZones().get(name);

        if (zoneCfg != null) {
            return zoneCfg.zoneId().value();
        } else {
            throw new DistributionZoneNotFoundException(name);
        }
    }

    /**
     * The method for obtaining the data nodes of the specified zone.
     * If {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp} and
     * {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} are immediate then it waits that the data nodes
     * are up-to-date for the passed topology version.
     *
     * <p>If the values of auto adjust scale up and auto adjust scale down are zero, then on the cluster topology changes
     * the data nodes for the zone should be updated immediately. Therefore, this method must return the data nodes which is calculated
     * based on the topology with passed or greater version. Since the date nodes value is updated asynchronously, this method waits for
     * the date nodes to be updated with new nodes in the topology if the value of auto adjust scale up is 0, and also waits for
     * the date nodes to be updated with nodes that have left the topology if the value of auto adjust scale down is 0.
     * After the zone manager has observed the logical topology change and the data nodes value is updated according to cluster topology,
     * then this method completes the returned future with the current value of data nodes.
     *
     * <p>If the value of auto adjust scale up is greater than zero, then it is not necessary to wait for the data nodes update triggered
     * by new nodes in cluster topology. Similarly if the value of auto adjust scale down is greater than zero, then it is not necessary to
     * wait for the data nodes update triggered by new nodes that have left the topology in cluster topology.
     *
     * <p>The returned future can be completed with {@link DistributionZoneNotFoundException} if zone with the provided {@code zoneId}
     * cannot be found or {@link DistributionZoneWasRemovedException} in case when the distribution zone was removed during
     * method execution.
     *
     * @param zoneId Zone id.
     * @param topVer Topology version.
     * @return The data nodes future which will be completed with data nodes for the zoneId or with exception.
     */
    public CompletableFuture<Set<String>> topologyVersionedDataNodes(int zoneId, long topVer) {
        CompletableFuture<IgniteBiTuple<Boolean, Boolean>> timerValuesFut = awaitTopologyVersion(topVer)
                .thenCompose(ignored -> getImmediateTimers(zoneId));

        return allOf(
                timerValuesFut.thenCompose(timerValues -> scaleUpAwaiting(zoneId, timerValues.get1())),
                timerValuesFut.thenCompose(timerValues -> scaleDownAwaiting(zoneId, timerValues.get2()))
        ).thenApply(ignored -> dataNodes(zoneId));
    }

    /**
     * Waits for observing passed topology version or greater version in {@link DistributionZoneManager#topologyWatchListener}.
     *
     * @param topVer Topology version.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> awaitTopologyVersion(long topVer) {
        return inBusyLock(busyLock, () -> topVerTracker.waitFor(topVer));
    }

    /**
     * Transforms {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp}
     * and {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} values to boolean values.
     * True if it equals to zero and false if it greater than zero. Zero means that data nodes changing must be started immediately.
     *
     * <p>The returned future can be completed with {@link DistributionZoneNotFoundException}
     * in case when the distribution zone was removed.
     *
     * @param zoneId Zone id.
     * @return Future with the boolean values for immediate auto adjust scale up and immediate auto adjust scale down.
     */
    private CompletableFuture<IgniteBiTuple<Boolean, Boolean>> getImmediateTimers(int zoneId) {
        return inBusyLock(busyLock, () -> {
            DistributionZoneConfiguration zoneCfg = getZoneById(zonesConfiguration, zoneId);

            return completedFuture(new IgniteBiTuple<>(
                    zoneCfg.dataNodesAutoAdjustScaleUp().value() == IMMEDIATE_TIMER_VALUE,
                    zoneCfg.dataNodesAutoAdjustScaleDown().value() == IMMEDIATE_TIMER_VALUE
            ));
        });
    }

    /**
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp} equals to 0 then waits for the zone manager processes
     * the data nodes update triggered by started nodes with passed or greater revision.
     * Else does nothing.
     *
     * <p>The returned future can be completed with {@link DistributionZoneWasRemovedException}
     * in case when the distribution zone was removed during method execution.
     *
     * @param zoneId Zone id.
     * @param immediateScaleUp True in case of immediate scale up.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> scaleUpAwaiting(int zoneId, boolean immediateScaleUp) {
        return inBusyLock(busyLock, () -> {
            if (immediateScaleUp) {
                ZoneState zoneState = zonesState.get(zoneId);

                if (zoneState != null) {
                    return zoneState.scaleUpRevisionTracker().waitFor(lastScaleUpRevision);
                } else {
                    return failedFuture(new DistributionZoneWasRemovedException(zoneId));
                }
            } else {
                return completedFuture(null);
            }
        });
    }

    /**
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} equals to 0 then waits for the zone manager processes
     * the data nodes update triggered by stopped nodes with passed or greater revision.
     * Else does nothing.
     *
     * <p>The returned future can be completed with {@link DistributionZoneWasRemovedException}
     * in case when the distribution zone was removed during method execution.
     *
     * @param zoneId Zone id.
     * @param immediateScaleDown True in case of immediate scale down.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> scaleDownAwaiting(int zoneId, boolean immediateScaleDown) {
        return inBusyLock(busyLock, () -> {
            if (immediateScaleDown) {
                ZoneState zoneState = zonesState.get(zoneId);

                if (zoneState != null) {
                    return zoneState.scaleDownRevisionTracker().waitFor(lastScaleDownRevision);
                } else {
                    return failedFuture(new DistributionZoneWasRemovedException(zoneId));
                }
            } else {
                return completedFuture(null);
            }
        });
    }

    /**
     * Returns the data nodes of the specified zone.
     *
     * @param zoneId Zone id.
     * @return The latest data nodes.
     */
    // TODO: https://issues.apache.org/jira/browse/IGNITE-19425 Proper causality token based implementation is expected.
    public Set<String> dataNodes(int zoneId) {
        return inBusyLock(busyLock, () -> {
            ZoneState zoneState = zonesState.get(zoneId);

            if (zoneState != null) {
                return zonesState.get(zoneId).nodes();
            } else {
                throw new DistributionZoneWasRemovedException(zoneId);
            }
        });
    }

    /**
     * Creates configuration listener for updates of scale up value.
     *
     * @return Configuration listener for updates of scale up value.
     */
    private ConfigurationListener<Integer> onUpdateScaleUp() {
        return ctx -> {
            if (ctx.oldValue() == null) {
                // zone creation, already handled in a separate listener.
                return completedFuture(null);
            }

            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            int newScaleUp = ctx.newValue().intValue();

            // It is safe to zonesTimers.get(zoneId) in term of NPE because meta storage notifications are one-threaded
            // and this map will be initialized on a manager start or with onCreate configuration notification
            ZoneState zoneState = zonesState.get(zoneId);

            if (newScaleUp != INFINITE_TIMER_VALUE) {
                Optional<Long> highestRevision = zoneState.highestRevision(true);

                assert highestRevision.isEmpty() || ctx.storageRevision() >= highestRevision.get() : "Expected revision that "
                        + "is greater or equal to already seen meta storage events.";

                zoneState.rescheduleScaleUp(
                        newScaleUp,
                        () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, ctx.storageRevision())
                );
            } else {
                zoneState.stopScaleUp();
            }

            //Only wait for a scale up revision if the auto adjust scale up has a zero value.
            //So if the value of the auto adjust scale up has become non-zero, then need to complete all futures.
            if (newScaleUp > 0) {
                zoneState.scaleUpRevisionTracker().update(lastScaleUpRevision, null);
            }

            return completedFuture(null);
        };
    }

    /**
     * Creates configuration listener for updates of scale down value.
     *
     * @return Configuration listener for updates of scale down value.
     */
    private ConfigurationListener<Integer> onUpdateScaleDown() {
        return ctx -> {
            if (ctx.oldValue() == null) {
                // zone creation, already handled in a separate listener.
                return completedFuture(null);
            }

            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            int newScaleDown = ctx.newValue().intValue();

            // It is safe to zonesTimers.get(zoneId) in term of NPE because meta storage notifications are one-threaded
            // and this map will be initialized on a manager start or with onCreate configuration notification
            ZoneState zoneState = zonesState.get(zoneId);

            if (newScaleDown != INFINITE_TIMER_VALUE) {
                Optional<Long> highestRevision = zoneState.highestRevision(false);

                assert highestRevision.isEmpty() || ctx.storageRevision() >= highestRevision.get() : "Expected revision that "
                        + "is greater or equal to already seen meta storage events.";

                zoneState.rescheduleScaleDown(
                        newScaleDown,
                        () -> saveDataNodesToMetaStorageOnScaleDown(zoneId, ctx.storageRevision())
                );
            } else {
                zoneState.stopScaleDown();
            }

            //Only wait for a scale down revision if the auto adjust scale down has a zero value.
            //So if the value of the auto adjust scale down has become non-zero, then need to complete all futures.
            if (newScaleDown > 0) {
                zoneState.scaleDownRevisionTracker().update(lastScaleDownRevision, null);
            }

            return completedFuture(null);
        };
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            ZoneState zoneState = new ZoneState(executor);

            zonesState.putIfAbsent(zoneId, zoneState);

            saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                    zoneId,
                    ctx.storageRevision(),
                    logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet())
            );

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            zonesState.get(zoneId).stopTimers();

            removeTriggerKeysAndDataNodes(zoneId, ctx.storageRevision());

            ZoneState zoneState = zonesState.remove(zoneId);

            zoneState.scaleUpRevisionTracker.update(Long.MAX_VALUE, null);
            zoneState.scaleDownRevisionTracker.update(Long.MAX_VALUE, null);

            return completedFuture(null);
        }
    }

    /**
     * Method updates data nodes value for the specified zone, also sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}, {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)}
     * and {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     * @param dataNodes Data nodes.
     */
    private void saveDataNodesAndUpdateTriggerKeysInMetaStorage(int zoneId, long revision, Set<Node> dataNodes) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the revision of the event is newer than value in that trigger key,
            // so we do not react on a stale events
            CompoundCondition triggerKeyCondition = triggerKeyConditionForZonesChanges(revision, zoneId);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKeys(zoneId, revision, toBytes(toDataNodesMap(dataNodes)));

            Iif iif = iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

            metaStorageManager.invoke(iif).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);
                } else if (res.getAsBoolean()) {
                    LOG.debug("Update zones' dataNodes value [zoneId = {}, dataNodes = {}", zoneId, dataNodes);
                } else {
                    LOG.debug("Failed to update zones' dataNodes value [zoneId = {}]", zoneId);
                }
            });
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
    private void removeTriggerKeysAndDataNodes(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            CompoundCondition triggerKeyCondition = triggerKeyConditionForZonesChanges(revision, zoneId);

            Update removeKeysUpd = deleteDataNodesAndUpdateTriggerKeys(zoneId, revision);

            Iif iif = iif(triggerKeyCondition, removeKeysUpd, ops().yield(false));

            metaStorageManager.invoke(iif).thenAccept(res -> {
                if (res.getAsBoolean()) {
                    LOG.debug("Delete zones' dataNodes key [zoneId = {}", zoneId);
                } else {
                    LOG.debug("Failed to delete zones' dataNodes key [zoneId = {}]", zoneId);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Updates {@link DistributionZoneChange} according to distribution zone configuration.
     *
     * @param zoneChange Zone change.
     * @param distributionZoneCfg Distribution zone configuration.
     */
    private static void updateZoneChange(DistributionZoneChange zoneChange, DistributionZoneConfigurationParameters distributionZoneCfg) {
        if (distributionZoneCfg.replicas() != null) {
            zoneChange.changeReplicas(distributionZoneCfg.replicas());
        }

        if (distributionZoneCfg.partitions() != null) {
            zoneChange.changePartitions(distributionZoneCfg.partitions());
        }

        if (distributionZoneCfg.dataStorageChangeConsumer() != null) {
            zoneChange.changeDataStorage(
                    distributionZoneCfg.dataStorageChangeConsumer());
        }

        if (distributionZoneCfg.filter() != null) {
            zoneChange.changeFilter(distributionZoneCfg.filter());
        }

        if (distributionZoneCfg.dataNodesAutoAdjust() != null) {
            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
            zoneChange.changeDataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE);
            zoneChange.changeDataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE);
        }

        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() != null) {
            zoneChange.changeDataNodesAutoAdjustScaleUp(
                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
            zoneChange.changeDataNodesAutoAdjust(INFINITE_TIMER_VALUE);
        }

        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() != null) {
            zoneChange.changeDataNodesAutoAdjustScaleDown(
                    distributionZoneCfg.dataNodesAutoAdjustScaleDown());
            zoneChange.changeDataNodesAutoAdjust(INFINITE_TIMER_VALUE);
        }
    }

    /**
     * Updates {@link DistributionZonesUtil#zonesLogicalTopologyKey()} and {@link DistributionZonesUtil#zonesLogicalTopologyVersionKey()}
     * in meta storage.
     *
     * @param newTopology Logical topology snapshot.
     * @param topologyLeap Flag that indicates whether this updates was trigger by
     *                     {@link LogicalTopologyEventListener#onTopologyLeap(LogicalTopologySnapshot)} or not.
     */
    private void updateLogicalTopologyInMetaStorage(LogicalTopologySnapshot newTopology, boolean topologyLeap) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            Set<LogicalNode> logicalTopology = newTopology.nodes();

            Condition updateCondition;

            if (topologyLeap) {
                updateCondition = value(zonesLogicalTopologyVersionKey()).lt(ByteUtils.longToBytes(newTopology.version()));
            } else {
                // This condition may be stronger, as far as we receive topology events one by one.
                updateCondition = value(zonesLogicalTopologyVersionKey()).eq(ByteUtils.longToBytes(newTopology.version() - 1));
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
     * Initialises {@link DistributionZonesUtil#zonesLogicalTopologyKey()} and
     * {@link DistributionZonesUtil#zonesLogicalTopologyVersionKey()} from meta storage on the start of {@link DistributionZoneManager}.
     */
    private void initLogicalTopologyAndVersionInMetaStorageOnStart() {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            CompletableFuture<Entry> zonesTopologyVersionFuture = metaStorageManager.get(zonesLogicalTopologyVersionKey());

            CompletableFuture<LogicalTopologySnapshot> logicalTopologyFuture = logicalTopologyService.logicalTopologyOnLeader();

            logicalTopologyFuture.thenAcceptBoth(zonesTopologyVersionFuture, (snapshot, topVerEntry) -> {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                }

                try {
                    long topologyVersionFromCmg = snapshot.version();

                    byte[] topVerFromMetaStorage = topVerEntry.value();

                    if (topVerFromMetaStorage == null || bytesToLong(topVerFromMetaStorage) < topologyVersionFromCmg) {
                        Set<LogicalNode> topologyFromCmg = snapshot.nodes();

                        Condition topologyVersionCondition = topVerFromMetaStorage == null
                                ? notExists(zonesLogicalTopologyVersionKey()) :
                                value(zonesLogicalTopologyVersionKey()).eq(topVerFromMetaStorage);

                        Iif iff = iif(topologyVersionCondition,
                                updateLogicalTopologyAndVersion(topologyFromCmg, topologyVersionFromCmg),
                                ops().yield(false)
                        );

                        metaStorageManager.invoke(iff).whenComplete((res, e) -> {
                            if (e != null) {
                                LOG.error(
                                        "Failed to initialize distribution zones' logical topology "
                                                + "and version keys [topology = {}, version = {}]",
                                        e,
                                        Arrays.toString(topologyFromCmg.toArray()),
                                        topologyVersionFromCmg
                                );
                            } else if (res.getAsBoolean()) {
                                LOG.debug(
                                        "Distribution zones' logical topology and version keys were initialised "
                                                + "[topology = {}, version = {}]",
                                        Arrays.toString(topologyFromCmg.toArray()),
                                        topologyVersionFromCmg
                                );
                            } else {
                                LOG.debug(
                                        "Failed to initialize distribution zones' logical topology "
                                                + "and version keys [topology = {}, version = {}]",
                                        Arrays.toString(topologyFromCmg.toArray()),
                                        topologyVersionFromCmg
                                );
                            }
                        });
                    }
                } finally {
                    busyLock.leaveBusy();
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Initialises data nodes of distribution zones in meta storage
     * from {@link DistributionZonesUtil#zonesLogicalTopologyKey()} in vault.
     */
    private void initDataNodesFromVaultManager() {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            long appliedRevision = metaStorageManager.appliedRevision();

            lastScaleUpRevision = appliedRevision;

            lastScaleDownRevision = appliedRevision;

            VaultEntry topVerEntry = vaultMgr.get(zonesLogicalTopologyVersionKey()).join();

            if (topVerEntry != null && topVerEntry.value() != null) {
                topVerTracker.update(bytesToLong(topVerEntry.value()), null);
            }

            VaultEntry topologyEntry = vaultMgr.get(zonesLogicalTopologyKey()).join();

            if (topologyEntry != null && topologyEntry.value() != null) {
                assert  appliedRevision > 0 : "The meta storage last applied revision is 0 but the logical topology is not null.";

                logicalTopology = fromBytes(topologyEntry.value());

                logicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));

                // init keys and data nodes for default zone
                saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                        DEFAULT_ZONE_ID,
                        appliedRevision,
                        logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet())
                );

                zonesConfiguration.distributionZones().value().forEach(zone -> {
                    int zoneId = zone.zoneId();

                    saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                            zoneId,
                            appliedRevision,
                            logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet())
                    );
                });
            }

            zonesState.values().forEach(zoneState -> {
                zoneState.scaleUpRevisionTracker().update(lastScaleUpRevision, null);

                zoneState.scaleDownRevisionTracker().update(lastScaleDownRevision, null);

                zoneState.nodes(logicalTopology.stream().map(NodeWithAttributes::nodeName).collect(toSet()));
            });

            assert topologyEntry == null || topologyEntry.value() == null || logicalTopology.equals(fromBytes(topologyEntry.value()))
                    : "Initial value of logical topology was changed after initialization from the vault manager.";
        } finally {
            busyLock.leaveBusy();
        }
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

                    long topVer = 0;

                    byte[] newLogicalTopologyBytes = null;

                    Set<NodeWithAttributes> newLogicalTopology = null;

                    long revision = 0;

                    for (EntryEvent event : evt.entryEvents()) {
                        Entry e = event.newEntry();

                        if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                            topVer = bytesToLong(e.value());

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

                    //Firstly update lastScaleUpRevision and lastScaleDownRevision then update topVerTracker to ensure thread-safety.
                    if (!addedNodes.isEmpty()) {
                        lastScaleUpRevision = revision;
                    }

                    if (!removedNodes.isEmpty()) {
                        lastScaleDownRevision = revision;
                    }

                    topVerTracker.update(topVer, null);

                    newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));

                    logicalTopology = newLogicalTopology;

                    NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                            zonesConfiguration.distributionZones();

                    for (int i = 0; i < zones.value().size(); i++) {
                        DistributionZoneView zoneView = zones.value().get(i);

                        scheduleTimers(zoneView, addedNodes, removedNodes, revision);
                    }

                    DistributionZoneView defaultZoneView = zonesConfiguration.value().defaultDistributionZone();

                    scheduleTimers(defaultZoneView, addedNodes, removedNodes, revision);

                    return completedFuture(null);
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
     * Creates watch listener which listens data nodes, scale up revision and scale down revision.
     *
     * @return Watch listener.
     */
    private WatchListener createMetastorageDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    return failedFuture(new NodeStoppingException());
                }

                try {
                    int zoneId = 0;

                    Set<Node> newDataNodes = null;

                    long scaleUpRevision = 0;

                    long scaleDownRevision = 0;

                    for (EntryEvent event : evt.entryEvents()) {
                        Entry e = event.newEntry();

                        if (startsWith(e.key(), zoneDataNodesKey().bytes())) {
                            zoneId = extractZoneId(e.key());

                            byte[] dataNodesBytes = e.value();

                            if (dataNodesBytes != null) {
                                newDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesBytes));
                            } else {
                                newDataNodes = emptySet();
                            }
                        } else if (startsWith(e.key(), zoneScaleUpChangeTriggerKey().bytes())) {
                            if (e.value() != null) {
                                scaleUpRevision = bytesToLong(e.value());
                            }
                        } else if (startsWith(e.key(), zoneScaleDownChangeTriggerKey().bytes())) {
                            if (e.value() != null) {
                                scaleDownRevision = bytesToLong(e.value());
                            }
                        }
                    }

                    ZoneState zoneState = zonesState.get(zoneId);

                    if (zoneState == null) {
                        //The zone has been dropped so no need to update zoneState.
                        return completedFuture(null);
                    }

                    assert newDataNodes != null : "Data nodes was not initialized.";

                    String filter = getZoneById(zonesConfiguration, zoneId).filter().value();

                    zoneState.nodes(filterDataNodes(newDataNodes, filter, nodesAttributes()));

                    //Associates scale up meta storage revision and data nodes.
                    if (scaleUpRevision > 0) {
                        zoneState.scaleUpRevisionTracker.update(scaleUpRevision, null);
                    }

                    //Associates scale down meta storage revision and data nodes.
                    if (scaleDownRevision > 0) {
                        zoneState.scaleDownRevisionTracker.update(scaleDownRevision, null);
                    }
                } finally {
                    busyLock.leaveBusy();
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
                LOG.warn("Unable to process data nodes event", e);
            }
        };
    }

    /**
     * Schedules scale up and scale down timers.
     *
     * @param zoneCfg Zone's configuration.
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision that triggered that event.
     */
    private void scheduleTimers(
            DistributionZoneView zoneCfg,
            Set<Node> addedNodes,
            Set<Node> removedNodes,
            long revision
    ) {
        scheduleTimers(
                zoneCfg,
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
     * @param zoneCfg Zone's configuration.
     * @param addedNodes Nodes that was added to a topology and should be added to zones data nodes.
     * @param removedNodes Nodes that was removed from a topology and should be removed from zones data nodes.
     * @param revision Revision that triggered that event.
     * @param saveDataNodesOnScaleUp Function that saves nodes to a zone's data nodes in case of scale up was triggered.
     * @param saveDataNodesOnScaleDown Function that saves nodes to a zone's data nodes in case of scale down was triggered.
     */
    void scheduleTimers(
            DistributionZoneView zoneCfg,
            Set<Node> addedNodes,
            Set<Node> removedNodes,
            long revision,
            BiFunction<Integer, Long, CompletableFuture<Void>> saveDataNodesOnScaleUp,
            BiFunction<Integer, Long, CompletableFuture<Void>> saveDataNodesOnScaleDown
    ) {
        int autoAdjust = zoneCfg.dataNodesAutoAdjust();
        int autoAdjustScaleDown = zoneCfg.dataNodesAutoAdjustScaleDown();
        int autoAdjustScaleUp = zoneCfg.dataNodesAutoAdjustScaleUp();

        int zoneId = zoneCfg.zoneId();

        if ((!addedNodes.isEmpty() || !removedNodes.isEmpty()) && autoAdjust != INFINITE_TIMER_VALUE) {
            //TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
            throw new UnsupportedOperationException("Data nodes auto adjust is not supported.");
        } else {
            if (!addedNodes.isEmpty()) {
                zonesState.get(zoneId).nodesToAddToDataNodes(addedNodes, revision);

                if (autoAdjustScaleUp != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleUp(
                            autoAdjustScaleUp,
                            () -> saveDataNodesOnScaleUp.apply(zoneId, revision)
                    );
                }
            }

            if (!removedNodes.isEmpty()) {
                zonesState.get(zoneId).nodesToRemoveFromDataNodes(removedNodes, revision);

                if (autoAdjustScaleDown != INFINITE_TIMER_VALUE) {
                    zonesState.get(zoneId).rescheduleScaleDown(
                            autoAdjustScaleDown,
                            () -> saveDataNodesOnScaleDown.apply(zoneId, revision)
                    );
                }
            }
        }
    }

    /**
     * Unwraps distribution zone exception from {@link ConfigurationChangeException} if it is possible.
     *
     * @param e Exception.
     * @param expectedClz Expected exception classes to unwrap.
     * @return Unwrapped exception if it is expected or original if it is unexpected exception.
     */
    private static Throwable unwrapDistributionZoneException(Throwable e, Class<? extends Throwable>... expectedClz) {
        Throwable ret = unwrapDistributionZoneExceptionRecursively(e, expectedClz);

        return ret != null ? ret : e;
    }

    private static Throwable unwrapDistributionZoneExceptionRecursively(Throwable e, Class<? extends Throwable>... expectedClz) {
        if ((e instanceof CompletionException || e instanceof ConfigurationChangeException) && e.getCause() != null) {
            return unwrapDistributionZoneExceptionRecursively(e.getCause(), expectedClz);
        }

        for (Class<?> expected : expectedClz) {
            if (expected.isAssignableFrom(e.getClass())) {
                return e;
            }
        }

        return null;
    }

    /**
     * Method updates data nodes value for the specified zone after scale up timer timeout, sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} and
     * {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} if it passes the condition.
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
                                zoneState.cleanUp(Math.min(scaleDownTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone has not succeeded [zoneId = {}]", zoneId);

                                return saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
                            }

                            return completedFuture(null);
                        }));
            })).whenComplete((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method updates data nodes value for the specified zone after scale down timer timeout, sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} and
     * {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} if it passes the condition.
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
                                // TODO: https://issues.apache.org/jira/browse/IGNITE-19491 Properly utilise this map
                                // Currently we call clean up only on a node that successfully writes data nodes.
                                zoneState.cleanUp(Math.min(scaleUpTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone has not succeeded [zoneId = {}]", zoneId);

                                return saveDataNodesToMetaStorageOnScaleDown(zoneId, revision);
                            }

                            return completedFuture(null);
                        }));
            })).whenComplete((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets direct id of the distribution zone with {@code zoneName}.
     *
     * @param zoneName Name of the distribution zone.
     * @return Direct id of the distribution zone, or {@code null} if the zone with the {@code zoneName} has not been found.
     */
    public CompletableFuture<Integer> zoneIdAsyncInternal(String zoneName) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            if (DEFAULT_ZONE_NAME.equals(zoneName)) {
                return completedFuture(DEFAULT_ZONE_ID);
            }

            // TODO: IGNITE-16288 directZoneId should use async configuration API
            return supplyAsync(() -> directZoneIdInternal(zoneName), executor)
                    .thenCompose(zoneId -> {
                        if (zoneId == null) {
                            return completedFuture(null);
                        } else {
                            return waitZoneIdLocally(zoneId).thenCompose(ignored -> completedFuture(zoneId));
                        }
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    @Nullable
    private Integer directZoneIdInternal(String zoneName) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            DistributionZoneConfiguration zoneCfg = directProxy(zonesConfiguration.distributionZones()).get(zoneName);

            if (zoneCfg == null) {
                return null;
            } else {
                return zoneCfg.zoneId().value();
            }
        } catch (NoSuchElementException e) {
            return null;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Internal method for waiting that the zone is created locally.
     *
     * @param id Zone id.
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<Void> waitZoneIdLocally(int id) {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            if (isZoneExist(zonesConfiguration, id)) {
                return completedFuture(null);
            }

            CompletableFuture<Void> zoneExistFut = new CompletableFuture<>();

            ConfigurationNamedListListener<DistributionZoneView> awaitZoneListener = new ConfigurationNamedListListener<>() {
                @Override
                public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
                    if (!busyLock.enterBusy()) {
                        throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
                    }

                    try {
                        if (ctx.newValue().zoneId() == id) {
                            zoneExistFut.complete(null);

                            zonesConfiguration.distributionZones().stopListenElements(this);
                        }

                        return completedFuture(null);
                    } finally {
                        busyLock.leaveBusy();
                    }
                }
            };

            zonesConfiguration.distributionZones().listenElements(awaitZoneListener);

            // This check is needed for the case when we have registered awaitZoneListener, but the zone has already been created.
            if (isZoneExist(zonesConfiguration, id)) {
                zonesConfiguration.distributionZones().stopListenElements(awaitZoneListener);

                return completedFuture(null);
            }

            return zoneExistFut;
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Gets a direct accessor for the configuration distributed property. If the metadata access only locally configured the method will
     * return local property accessor.
     *
     * @param property Distributed configuration property to receive direct access.
     * @param <T> Type of the property accessor.
     * @return An accessor for distributive property.
     * @see #getMetadataLocallyOnly
     */
    private <T extends ConfigurationProperty<?>> T directProxy(T property) {
        return getMetadataLocallyOnly ? property : (T) property.directProxy();
    }

    /**
     * Class responsible for storing state for a distribution zone.
     * States are needed to track nodes that we want to add or remove from the data nodes,
     * to schedule and stop scale up and scale down processes.
     */
    static class ZoneState {
        /** Schedule task for a scale up process. */
        private ScheduledFuture<?> scaleUpTask;

        /** Schedule task for a scale down process. */
        private ScheduledFuture<?> scaleDownTask;

        /**
         * Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can track which nodes
         * should be added or removed in the processes of scale up or scale down. Revision helps to track visibility of the events
         * of adding or removing nodes because any process of scale up or scale down has a revision that triggered this process.
         */
        private final ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap;

        /** Executor for scheduling tasks for scale up and scale down processes. */
        private final ScheduledExecutorService executor;

        /** Data nodes. */
        private volatile Set<String> nodes;

        /** The tracker for scale up meta storage revision of current data nodes value. */
        private final PendingComparableValuesTracker<Long, Void> scaleUpRevisionTracker;

        /** The tracker for scale down meta storage revision of current data nodes value. */
        private final PendingComparableValuesTracker<Long, Void> scaleDownRevisionTracker;

        /**
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         */
        ZoneState(ScheduledExecutorService executor) {
            this.executor = executor;
            topologyAugmentationMap = new ConcurrentSkipListMap<>();
            nodes = emptySet();
            scaleUpRevisionTracker = new PendingComparableValuesTracker<>(0L);
            scaleDownRevisionTracker = new PendingComparableValuesTracker<>(0L);
        }

        /**
         * Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can track which nodes
         * should be added or removed in the processes of scale up or scale down. Revision helps to track visibility of the events
         * of adding or removing nodes because any process of scale up or scale down has a revision that triggered this process.
         */
        ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap() {
            return topologyAugmentationMap;
        }

        /**
         * Reschedules existing scale up task, if it is not started yet, or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         */
        synchronized void rescheduleScaleUp(long delay, Runnable runnable) {
            if (scaleUpTask != null) {
                scaleUpTask.cancel(false);
            }

            scaleUpTask = executor.schedule(runnable, delay, TimeUnit.SECONDS);
        }

        /**
         * Reschedules existing scale down task, if it is not started yet, or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         */
        synchronized void rescheduleScaleDown(long delay, Runnable runnable) {
            if (scaleDownTask != null) {
                scaleDownTask.cancel(false);
            }

            scaleDownTask = executor.schedule(runnable, delay, TimeUnit.SECONDS);
        }

        /**
         * Cancels task for scale up and scale down.
         */
        synchronized void stopTimers() {
            stopScaleUp();

            stopScaleDown();
        }

        /**
         * Cancels task for scale up.
         */
        synchronized void stopScaleUp() {
            if (scaleUpTask != null) {
                scaleUpTask.cancel(false);
            }
        }

        /**
         * Cancels task for scale down.
         */
        synchronized void stopScaleDown() {
            if (scaleDownTask != null) {
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

        /**
         * Get data nodes.
         *
         * @return Data nodes.
         */
        private Set<String> nodes() {
            return nodes;
        }

        /**
         * Set data nodes.
         *
         * @param nodes Data nodes.
         */
        private void nodes(Set<String> nodes) {
            this.nodes = nodes;
        }

        /**
         * The tracker for scale up meta storage revision of current data nodes value.
         *
         * @return The tracker.
         */
        private PendingComparableValuesTracker<Long, Void> scaleUpRevisionTracker() {
            return scaleUpRevisionTracker;
        }

        /**
         * The tracker for scale down meta storage revision of current data nodes value.
         *
         * @return The tracker.
         */
        private PendingComparableValuesTracker<Long, Void> scaleDownRevisionTracker() {
            return scaleDownRevisionTracker;
        }

        @TestOnly
        synchronized ScheduledFuture<?> scaleUpTask() {
            return scaleUpTask;
        }

        @TestOnly
        synchronized ScheduledFuture<?> scaleDownTask() {
            return scaleDownTask;
        }
    }

    /**
     * Class stores the info about nodes that should be added or removed from the data nodes of a zone.
     * With flag {@code addition} we can track whether {@code nodeNames} should be added or removed.
     */
    private static class Augmentation {
        /** Names of the node. */
        Set<Node> nodes;

        /** Flag that indicates whether {@code nodeNames} should be added or removed. */
        boolean addition;

        Augmentation(Set<Node> nodes, boolean addition) {
            this.nodes = nodes;
            this.addition = addition;
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
    Map<Integer, ZoneState> zonesTimers() {
        return zonesState;
    }
}
