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
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesAndUpdateTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractChangeTriggerRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.getZoneById;
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
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
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
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.ConfigurationNodeAlreadyExistException;
import org.apache.ignite.configuration.ConfigurationNodeDoesNotExistException;
import org.apache.ignite.configuration.ConfigurationNodeRemovedException;
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
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneBindTableException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
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
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
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

    /** Default number of zone replicas. */
    public static final int DEFAULT_REPLICA_COUNT = 1;

    /** Default number of zone partitions. */
    public static final int DEFAULT_PARTITION_COUNT = 25;

    /** Default infinite value for the distribution zones' timers. */
    public static final int INFINITE_TIMER_VALUE = Integer.MAX_VALUE;

    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

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

    /** Data nodes modification mutex. */
    private final Object dataNodesMutex = new Object();

    /** The last topology version which was observed by distribution zone manager. */
    private long lastTopVer;

    /**
     * The map contains futures which are completed when zone manager observe appropriate logical topology version.
     * Map (topology version -> future).
     */
    private final NavigableMap<Long, CompletableFuture<Void>> topVerFutures;

    /**
     * The last meta storage revision on which scale up timer was started.
     */
    private long lastScaleUpRevision;

    /**
     * The last meta storage revision on which scale down timer was started.
     */
    private long lastScaleDownRevision;

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
    private volatile Set<String> logicalTopology;

    /** Watch listener for logical topology keys. */
    private final WatchListener topologyWatchListener;

    /** Watch listener for data nodes keys. */
    private final WatchListener dataNodesWatchListener;

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     * @param tablesConfiguration Tables configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param vaultMgr Vault manager.
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

        executor = new ScheduledThreadPoolExecutor(
                Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
                new NamedThreadFactory(NamedThreadFactory.threadPrefix(nodeName, DISTRIBUTION_ZONE_MANAGER_POOL_NAME), LOG),
                new ThreadPoolExecutor.DiscardPolicy()
        );

        topVerFutures = new TreeMap<>();
    }

    @TestOnly
    Map<Integer, ZoneState> zonesTimers() {
        return zonesState;
    }

    /**
     * Creates a new distribution zone with the given {@code name} asynchronously.
     *
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation. Future can be completed with:
     *      {@link DistributionZoneAlreadyExistsException} if a zone with the given name already exists,
     *      {@link ConfigurationValidationException} if {@code distributionZoneCfg} is broken,
     *      {@link IllegalArgumentException} if distribution zone configuration is null
     *      or distribution zone name is {@code DEFAULT_ZONE_NAME},
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> createZone(DistributionZoneConfigurationParameters distributionZoneCfg) {
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
            CompletableFuture<Void> fut = new CompletableFuture<>();

            zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                try {
                    zonesListChange.create(distributionZoneCfg.name(), zoneChange -> {
                        if (distributionZoneCfg.partitions() == null) {
                            zoneChange.changePartitions(DEFAULT_PARTITION_COUNT);
                        } else {
                            zoneChange.changePartitions(distributionZoneCfg.partitions());
                        }

                        if (distributionZoneCfg.replicas() == null) {
                            zoneChange.changeReplicas(DEFAULT_REPLICA_COUNT);
                        } else {
                            zoneChange.changeReplicas(distributionZoneCfg.replicas());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjust() == null) {
                            zoneChange.changeDataNodesAutoAdjust(INFINITE_TIMER_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(INFINITE_TIMER_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(
                                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(INFINITE_TIMER_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                        }

                        int intZoneId = zonesChange.globalIdCounter() + 1;
                        zonesChange.changeGlobalIdCounter(intZoneId);

                        zoneChange.changeZoneId(intZoneId);
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
                    fut.complete(null);
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
     * The method for obtaining data nodes of the specified zone.
     * The flow for the future completion:
     * Waiting for DistributionZoneManager observe passed topology version or greater version in topologyWatchListener.
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp} equals to 0 than wait for writing data nodes triggered
     * by started nodes and corresponding to the passed topology version or greater topology version
     * to the data nodes into the meta storage.
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} equals to 0 than wait for writing data nodes
     * triggered by stopped nodes and corresponding to the passed topology version or greater topology version
     * to the data nodes into the meta storage.
     * After waiting it returns the future with data nodes of the specified zone.
     *
     * @param zoneId Zone id.
     * @param topVer Topology version.
     * @return The data nodes future.
     */
    public CompletableFuture<Set<String>> topologyVersionedDataNodes(int zoneId, long topVer) {
        CompletableFuture<Void> topVerFut = awaitTopologyVersion(topVer);

        CompletableFuture<IgniteBiTuple<Boolean, Boolean>> timerValuesFut = getImmediateTimers(zoneId, topVerFut);

        CompletableFuture<Void> topVerScaleUpFut = scaleUpAwaiting(zoneId, timerValuesFut);

        CompletableFuture<Void> topVerScaleDownFut = scaleDownAwaiting(zoneId, timerValuesFut);

        return getDataNodesFuture(zoneId, topVerScaleUpFut, topVerScaleDownFut);
    }

    /**
     * Waits for DistributionZoneManager observe passed topology version or greater version in topologyWatchListener.
     *
     * @param topVer Topology version.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> awaitTopologyVersion(long topVer) {
        synchronized (dataNodesMutex) {
            if (topVer > lastTopVer) {
                return topVerFutures.computeIfAbsent(topVer, key -> new CompletableFuture<>());
            } else {
                return completedFuture(null);
            }
        }
    }

    /**
     * Transforms {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp}
     * and {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} values to boolean values.
     * True if it equals to zero and false if it greater than zero. Zero means that data nodes changing must be scheduled immediate.
     *
     * @param zoneId Zone id.
     * @param topVerFut Future for chaining.
     * @return Future.
     */
    private CompletableFuture<IgniteBiTuple<Boolean, Boolean>> getImmediateTimers(int zoneId, CompletableFuture<Void> topVerFut) {
        CompletableFuture<IgniteBiTuple<Boolean, Boolean>> timerValuesFut = new CompletableFuture<>();

        topVerFut.thenAccept(ignored -> {
            DistributionZoneConfiguration zoneCfg = getZoneById(zonesConfiguration, zoneId);

            timerValuesFut.complete(new IgniteBiTuple<>(
                    zoneCfg.dataNodesAutoAdjustScaleUp().value() == 0,
                    zoneCfg.dataNodesAutoAdjustScaleDown().value() == 0
            ));
        });

        return timerValuesFut;
    }

    /**
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleUp} equals to 0 than waits for writing data nodes triggered
     * by started nodes and corresponding to the passed topology version or greater topology version
     * to the data nodes into the meta storage.
     *
     * @param zoneId Zone id.
     * @param timerValuesFut Future for chaining.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> scaleUpAwaiting(
            int zoneId,
            CompletableFuture<IgniteBiTuple<Boolean, Boolean>> timerValuesFut
    ) {
        CompletableFuture<Void> topVerScaleUpFut = new CompletableFuture<>();

        timerValuesFut.thenAccept(timerValues -> {
            boolean immediateScaleUp = timerValues.get1();

            synchronized (dataNodesMutex) {
                ZoneState zoneState = zonesState.get(zoneId);

                CompletableFuture<Void> topVerScaleUpFut0 = null;

                if (immediateScaleUp) {
                    if (zoneState.scaleUpRevision() < lastScaleUpRevision) {
                        Map.Entry<Long, CompletableFuture<Void>> ceilingEntry =
                                zoneState.revisionScaleUpFutures().ceilingEntry(lastScaleUpRevision);

                        if (ceilingEntry != null) {
                            topVerScaleUpFut0 = ceilingEntry.getValue();
                        }

                        if (topVerScaleUpFut0 == null) {
                            topVerScaleUpFut0 = new CompletableFuture<>();

                            zoneState.revisionScaleUpFutures().put(lastScaleUpRevision, topVerScaleUpFut0);
                        }

                        topVerScaleUpFut0.handle((ignored0, e) -> {
                            if (e == null) {
                                topVerScaleUpFut.complete(null);
                            } else {
                                topVerScaleUpFut.completeExceptionally(e);
                            }

                            return null;
                        });
                    } else {
                        topVerScaleUpFut.complete(null);
                    }
                } else {
                    topVerScaleUpFut.complete(null);
                }
            }
        });

        return topVerScaleUpFut;
    }

    /**
     * If the {@link DistributionZoneConfigurationSchema#dataNodesAutoAdjustScaleDown} equals to 0 than waits for writing data nodes
     * triggered by stopped nodes and corresponding to the passed topology version or greater topology version
     * to the data nodes into the meta storage.
     *
     * @param zoneId Zone id.
     * @param timerValuesFut Future for chaining.
     * @return Future for chaining.
     */
    private CompletableFuture<Void> scaleDownAwaiting(
            int zoneId,
            CompletableFuture<IgniteBiTuple<Boolean, Boolean>> timerValuesFut
    ) {
        CompletableFuture<Void> topVerScaleDownFut = new CompletableFuture<>();

        timerValuesFut.thenAccept(timerValues -> {
            boolean immediateScaleDown = timerValues.get2();

            synchronized (dataNodesMutex) {
                ZoneState zoneState = zonesState.get(zoneId);

                CompletableFuture<Void> topVerScaleDownFut0 = null;

                if (immediateScaleDown) {
                    if (zoneState.scaleDownRevision() < lastScaleDownRevision) {
                        Map.Entry<Long, CompletableFuture<Void>> ceilingEntry =
                                zoneState.revisionScaleDownFutures().ceilingEntry(lastScaleDownRevision);

                        if (ceilingEntry != null) {
                            topVerScaleDownFut0 = ceilingEntry.getValue();
                        }

                        if (topVerScaleDownFut0 == null) {
                            topVerScaleDownFut0 = new CompletableFuture<>();

                            zoneState.revisionScaleDownFutures().put(lastScaleDownRevision, topVerScaleDownFut0);
                        }

                        topVerScaleDownFut0.handle((ignored0, e) -> {
                            if (e == null) {
                                topVerScaleDownFut.complete(null);
                            } else {
                                topVerScaleDownFut.completeExceptionally(e);
                            }

                            return null;
                        });
                    } else {
                        topVerScaleDownFut.complete(null);
                    }
                } else {
                    topVerScaleDownFut.complete(null);
                }
            }
        });

        return topVerScaleDownFut;
    }

    /**
     * Returns the future with data nodes of the specified zone.
     *
     * @param zoneId Zone id.
     * @param topVerScaleUpFut Future for chaining.
     * @param topVerScaleDownFut Future for chaining.
     * @return future.
     */
    private CompletableFuture<Set<String>> getDataNodesFuture(
            int zoneId,
            CompletableFuture<Void> topVerScaleUpFut,
            CompletableFuture<Void> topVerScaleDownFut
    ) {
        CompletableFuture<Set<String>> dataNodesFut = new CompletableFuture<>();

        allOf(topVerScaleUpFut, topVerScaleDownFut).handle((ignored, e) -> {
            if (e != null) {
                dataNodesFut.completeExceptionally(e);
            } else {
                synchronized (dataNodesMutex) {
                    dataNodesFut.complete(zonesState.get(zoneId).nodes());
                }
            }

            return null;
        });

        return dataNodesFut;
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

    /**
     * Creates configuration listener for updates of scale up value.
     *
     * @return Configuration listener for updates of scale up value.
     */
    private ConfigurationListener<Integer> onUpdateScaleUp() {
        return ctx -> {
            int zoneId = ctx.config(DistributionZoneConfiguration.class).zoneId().value();

            if (ctx.oldValue() == null) {
                // zone creation, already handled in a separate listener.
                return completedFuture(null);
            }

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

            if (newScaleUp > 0) {
                synchronized (dataNodesMutex) {
                    zoneState.revisionScaleUpFutures().values()
                            .forEach(fut0 -> fut0.complete(null));

                    zoneState.revisionScaleUpFutures().clear();
                }
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
            int zoneId = ctx.config(DistributionZoneConfiguration.class).zoneId().value();

            if (ctx.oldValue() == null) {
                // zone creation, already handled in a separate listener.
                return completedFuture(null);
            }

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

            if (newScaleDown > 0) {
                synchronized (dataNodesMutex) {
                    zoneState.revisionScaleDownFutures().values()
                            .forEach(fut0 -> fut0.complete(null));

                    zoneState.revisionScaleDownFutures().clear();
                }
            }

            return completedFuture(null);
        };
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {
        if (!stopGuard.compareAndSet(false, true)) {
            return;
        }

        busyLock.block();

        logicalTopologyService.removeEventListener(topologyEventListener);

        metaStorageManager.unregisterWatch(topologyWatchListener);
        metaStorageManager.unregisterWatch(dataNodesWatchListener);

        shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            ZoneState zoneState = new ZoneState(executor);

            zonesState.putIfAbsent(zoneId, zoneState);

            saveDataNodesAndUpdateTriggerKeysInMetaStorage(zoneId, ctx.storageRevision(), logicalTopology);

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            zonesState.get(zoneId).stopTimers();

            removeTriggerKeysAndDataNodes(zoneId, ctx.storageRevision());

            ZoneState zoneState = zonesState.remove(zoneId);

            synchronized (dataNodesMutex) {
                zoneState.revisionScaleUpFutures().values()
                        .forEach(fut0 -> fut0.completeExceptionally(new DistributionZoneNotFoundException(zoneId)));

                zoneState.revisionScaleDownFutures().values()
                        .forEach(fut0 -> fut0.completeExceptionally(new DistributionZoneNotFoundException(zoneId)));
            }

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
    private void saveDataNodesAndUpdateTriggerKeysInMetaStorage(int zoneId, long revision, Set<String> dataNodes) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the revision of the event is newer than value in that trigger key,
            // so we do not react on a stale events
            CompoundCondition triggerKeyCondition = triggerKeyConditionForZonesChanges(revision, zoneId);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKeys(zoneId, revision, toBytes(toDataNodesMap(dataNodes)));

            Iif iif = iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

            metaStorageManager.invoke(iif).thenAccept(res -> {
                if (res.getAsBoolean()) {
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
            Set<String> topologyFromCmg = newTopology.nodes().stream().map(ClusterNode::name).collect(toSet());

            Condition updateCondition;

            if (topologyLeap) {
                updateCondition = value(zonesLogicalTopologyVersionKey()).lt(ByteUtils.longToBytes(newTopology.version()));
            } else {
                // This condition may be stronger, as far as we receive topology events one by one.
                updateCondition = value(zonesLogicalTopologyVersionKey()).eq(ByteUtils.longToBytes(newTopology.version() - 1));
            }

            Iif iff = iif(
                    updateCondition,
                    updateLogicalTopologyAndVersion(topologyFromCmg, newTopology.version()),
                    ops().yield(false)
            );

            metaStorageManager.invoke(iff).thenAccept(res -> {
                if (res.getAsBoolean()) {
                    LOG.debug(
                            "Distribution zones' logical topology and version keys were updated [topology = {}, version = {}]",
                            Arrays.toString(topologyFromCmg.toArray()),
                            newTopology.version()
                    );
                } else {
                    LOG.debug(
                            "Failed to update distribution zones' logical topology and version keys [topology = {}, version = {}]",
                            Arrays.toString(topologyFromCmg.toArray()),
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
            logicalTopologyService.logicalTopologyOnLeader().thenAccept(snapshot -> {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                }

                try {
                    metaStorageManager.get(zonesLogicalTopologyVersionKey()).thenAccept(topVerEntry -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            long topologyVersionFromCmg = snapshot.version();

                            byte[] topVerFromMetaStorage = topVerEntry.value();

                            if (topVerFromMetaStorage == null || bytesToLong(topVerFromMetaStorage) < topologyVersionFromCmg) {
                                Set<String> topologyFromCmg = snapshot.nodes().stream().map(ClusterNode::name).collect(toSet());

                                Condition topologyVersionCondition = topVerFromMetaStorage == null
                                        ? notExists(zonesLogicalTopologyVersionKey()) :
                                        value(zonesLogicalTopologyVersionKey()).eq(topVerFromMetaStorage);

                                Iif iff = iif(topologyVersionCondition,
                                        updateLogicalTopologyAndVersion(topologyFromCmg, topologyVersionFromCmg),
                                        ops().yield(false)
                                );

                                metaStorageManager.invoke(iff).thenAccept(res -> {
                                    if (res.getAsBoolean()) {
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
            CompletableFuture<VaultEntry> topologyFut = vaultMgr.get(zonesLogicalTopologyKey());

            CompletableFuture<VaultEntry> topVerFut = vaultMgr.get(zonesLogicalTopologyVersionKey());

            allOf(topologyFut, topVerFut)
                    .thenAccept(ignored -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            VaultEntry topVerEntry = topVerFut.join();

                            if (topVerEntry.value() != null) {
                                synchronized (dataNodesMutex) {
                                    lastTopVer = bytesToLong(topVerEntry.value());
                                }
                            }

                            VaultEntry topologyEntry = topologyFut.join();

                            long appliedRevision = metaStorageManager.appliedRevision();

                            if (topologyEntry.value() != null) {
                                logicalTopology = fromBytes(topologyEntry.value());

                                // init keys and data nodes for default zone
                                saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                                        DEFAULT_ZONE_ID,
                                        appliedRevision,
                                        logicalTopology
                                );

                                zonesConfiguration.distributionZones().value().forEach(zone -> {
                                    int zoneId = zone.zoneId();

                                    saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                                            zoneId,
                                            appliedRevision,
                                            logicalTopology
                                    );
                                });

                                synchronized (dataNodesMutex) {
                                    zonesState.values().forEach(zoneState -> {
                                        zoneState.scaleUpRevision(appliedRevision);

                                        zoneState.scaleDownRevision(appliedRevision);
                                    });

                                    lastScaleUpRevision = appliedRevision;

                                    lastScaleDownRevision = appliedRevision;

                                    zonesState.get(DEFAULT_ZONE_ID).nodes(logicalTopology);

                                    zonesConfiguration.distributionZones().value().namedListKeys()
                                            .forEach(zoneName -> {
                                                NamedConfigurationTree<DistributionZoneConfiguration,
                                                        DistributionZoneView,
                                                        DistributionZoneChange> zones = zonesConfiguration.distributionZones();

                                                for (int i = 0; i < zones.value().size(); i++) {
                                                    DistributionZoneView zoneView = zones.value().get(i);

                                                    zonesState.get(zoneView.zoneId()).nodes(logicalTopology);
                                                }
                                            });
                                }
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

                    Set<String> newLogicalTopology = null;

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

                    assert newLogicalTopology != null;
                    assert revision > 0;

                    Set<String> newLogicalTopology0 = newLogicalTopology;

                    Set<String> removedNodes =
                            logicalTopology.stream().filter(node -> !newLogicalTopology0.contains(node)).collect(toSet());

                    Set<String> addedNodes =
                            newLogicalTopology.stream().filter(node -> !logicalTopology.contains(node)).collect(toSet());

                    synchronized (dataNodesMutex) {
                        lastTopVer = topVer;

                        //Associates topology version and scale up meta storage revision.
                        if (!addedNodes.isEmpty()) {
                            lastScaleUpRevision = revision;
                        }

                        //Associates topology version and scale down meta storage revision.
                        if (!removedNodes.isEmpty()) {
                            lastScaleDownRevision = revision;
                        }

                        //Completes futures which await corresponding topology versions.
                        SortedMap<Long, CompletableFuture<Void>> topVerFuts = topVerFutures.headMap(topVer, true);

                        topVerFuts.values().forEach(v -> v.complete(null));

                        topVerFuts.clear();
                    }

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

                    Set<String> newDataNodes = null;

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

                    synchronized (dataNodesMutex) {
                        ZoneState zoneState = zonesState.get(zoneId);

                        if (zoneState != null) {
                            zoneState.nodes(newDataNodes);

                            //Associates scale up meta storage revision and data nodes.
                            if (scaleUpRevision > 0) {
                                zoneState.scaleUpRevision(scaleUpRevision);
                            }

                            //Associates scale down meta storage revision and data nodes.
                            if (scaleDownRevision > 0) {
                                zoneState.scaleDownRevision(scaleDownRevision);
                            }

                            //Completes futures which await corresponding scale up meta storage revision.
                            if (scaleUpRevision > 0) {
                                SortedMap<Long, CompletableFuture<Void>> revisionScaleUpFuts =
                                        zoneState.revisionScaleUpFutures().headMap(scaleUpRevision, true);

                                revisionScaleUpFuts.values().forEach(v -> v.complete(null));

                                revisionScaleUpFuts.clear();
                            }

                            //Completes futures which await corresponding scale down meta storage revision.
                            if (scaleDownRevision > 0) {
                                SortedMap<Long, CompletableFuture<Void>> revisionScaleDownFuts =
                                        zoneState.revisionScaleDownFutures().headMap(scaleDownRevision, true);

                                revisionScaleDownFuts.values().forEach(v -> v.complete(null));

                                revisionScaleDownFuts.clear();
                            }
                        }
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
            Set<String> addedNodes,
            Set<String> removedNodes,
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
            Set<String> addedNodes,
            Set<String> removedNodes,
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
            if (!addedNodes.isEmpty() && autoAdjustScaleUp != INFINITE_TIMER_VALUE) {
                zonesState.get(zoneId).nodesToAddToDataNodes(addedNodes, revision);

                zonesState.get(zoneId).rescheduleScaleUp(
                        autoAdjustScaleUp,
                        () -> saveDataNodesOnScaleUp.apply(zoneId, revision)
                );
            }

            if (!removedNodes.isEmpty() && autoAdjustScaleDown != INFINITE_TIMER_VALUE) {
                zonesState.get(zoneId).nodesToRemoveFromDataNodes(removedNodes, revision);

                zonesState.get(zoneId).rescheduleScaleDown(
                        autoAdjustScaleDown,
                        () -> saveDataNodesOnScaleDown.apply(zoneId, revision)
                );
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

                Map<String, Integer> dataNodesFromMetaStorage = extractDataNodes(values.get(zoneDataNodesKey(zoneId)));

                long scaleUpTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleUpChangeTriggerKey(zoneId)));

                long scaleDownTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleDownChangeTriggerKey(zoneId)));

                if (revision <= scaleUpTriggerRevision) {
                    return completedFuture(null);
                }

                List<String> deltaToAdd = zoneState.nodesToBeAddedToDataNodes(scaleUpTriggerRevision, revision);

                Map<String, Integer> newDataNodes = new HashMap<>(dataNodesFromMetaStorage);

                deltaToAdd.forEach(n -> newDataNodes.merge(n, 1, Integer::sum));

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
                        .thenApply(invokeResult -> inBusyLock(busyLock, () -> {
                            if (invokeResult) {
                                zoneState.cleanUp(Math.min(scaleDownTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone has not succeeded [zoneId = {}]", zoneId);

                                return saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
                            }

                            return completedFuture(null);
                        }));
            })).handle((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);

                    return CompletableFuture.<Void>failedFuture(e);
                }

                return CompletableFuture.<Void>completedFuture(null);
            }).thenCompose(Function.identity());
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

                Map<String, Integer> dataNodesFromMetaStorage = extractDataNodes(values.get(zoneDataNodesKey(zoneId)));

                long scaleUpTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleUpChangeTriggerKey(zoneId)));

                long scaleDownTriggerRevision = extractChangeTriggerRevision(values.get(zoneScaleDownChangeTriggerKey(zoneId)));

                if (revision <= scaleDownTriggerRevision) {
                    return completedFuture(null);
                }

                List<String> deltaToRemove = zoneState.nodesToBeRemovedFromDataNodes(scaleDownTriggerRevision, revision);

                Map<String, Integer> newDataNodes = new HashMap<>(dataNodesFromMetaStorage);

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
                        .thenApply(invokeResult -> inBusyLock(busyLock, () -> {
                            if (invokeResult) {
                                zoneState.cleanUp(Math.min(scaleUpTriggerRevision, revision));
                            } else {
                                LOG.debug("Updating data nodes for a zone has not succeeded [zoneId = {}]", zoneId);

                                return saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
                            }

                            return completedFuture(null);
                        }));
            })).handle((v, e) -> {
                if (e != null) {
                    LOG.warn("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);

                    return CompletableFuture.<Void>failedFuture(e);
                }

                return CompletableFuture.<Void>completedFuture(null);
            }).thenCompose(Function.identity());
        } finally {
            busyLock.leaveBusy();
        }
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
        private Set<String> nodes;

        /** Scale up meta storage revision of current nodes value. */
        private long scaleUpRevision;

        /** Scale down meta storage revision of current nodes value. */
        private long scaleDownRevision;

        /**
         * The map contains futures which are completed when zone manager observe data nodes bound to appropriate scale up revision.
         * Map (revision -> future).
         */
        private final NavigableMap<Long, CompletableFuture<Void>> revisionScaleUpFutures = new ConcurrentSkipListMap();

        /**
         * The map contains futures which are completed when zone manager observe data nodes bound to appropriate scale down revision.
         * Map (revision -> future).
         */
        private final NavigableMap<Long, CompletableFuture<Void>> revisionScaleDownFutures = new ConcurrentSkipListMap();

        /**
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         */
        ZoneState(ScheduledExecutorService executor) {
            this.executor = executor;
            topologyAugmentationMap = new ConcurrentSkipListMap<>();
            nodes = emptySet();
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
        List<String> nodesToBeAddedToDataNodes(long scaleUpRevision, long revision) {
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
        List<String> nodesToBeRemovedFromDataNodes(long scaleDownRevision, long revision) {
            return accumulateNodes(scaleDownRevision, revision, false);
        }

        /**
         * Add nodes to the map where nodes that must be added to the zone's data nodes are accumulated.
         *
         * @param nodes Nodes to add to zone's data nodes.
         * @param revision Revision of the event that triggered this addition.
         */
        void nodesToAddToDataNodes(Set<String> nodes, long revision) {
            topologyAugmentationMap.put(revision, new Augmentation(nodes, true));
        }

        /**
         * Add nodes to the map where nodes that must be removed from the zone's data nodes are accumulated.
         *
         * @param nodes Nodes to remove from zone's data nodes.
         * @param revision Revision of the event that triggered this addition.
         */
        void nodesToRemoveFromDataNodes(Set<String> nodes, long revision) {
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
        private List<String> accumulateNodes(long fromKey, long toKey, boolean addition) {
            return topologyAugmentationMap.subMap(fromKey, false, toKey, true).values()
                    .stream()
                    .filter(a -> a.addition == addition)
                    .flatMap(a -> a.nodeNames.stream())
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
         * Get scale up meta storage revision of current nodes value.
         *
         * @return Scale up meta storage revision.
         */
        private long scaleUpRevision() {
            return scaleUpRevision;
        }

        /**
         * Set scale up meta storage revision of current nodes value.
         *
         * @param scaleUpRevision Scale up meta storage revision.
         */
        private void scaleUpRevision(long scaleUpRevision) {
            this.scaleUpRevision = scaleUpRevision;
        }

        /**
         * Get scale down meta storage revision of current nodes value.
         *
         * @return Scale down meta storage revision.
         */
        private long scaleDownRevision() {
            return scaleDownRevision;
        }

        /**
         * Set scale down meta storage revision of current nodes value.
         *
         * @param scaleDownRevision Scale up meta storage revision.
         */
        private void scaleDownRevision(long scaleDownRevision) {
            this.scaleDownRevision = scaleDownRevision;
        }

        /**
         * The map contains futures which are completed when zone manager observe data nodes bound to appropriate scale up revision.
         * Map (revision -> future).
         *
         * @return The map of futures.
         */
        NavigableMap<Long, CompletableFuture<Void>> revisionScaleUpFutures() {
            return revisionScaleUpFutures;
        }

        /**
         * The map contains futures which are completed when zone manager observe data nodes bound to appropriate scale down revision.
         * Map (revision -> future).
         *
         * @return The map of futures.
         */
        NavigableMap<Long, CompletableFuture<Void>> revisionScaleDownFutures() {
            return revisionScaleDownFutures;
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
        Set<String> nodeNames;

        /** Flag that indicates whether {@code nodeNames} should be added or removed. */
        boolean addition;

        Augmentation(Set<String> nodeNames, boolean addition) {
            this.nodeNames = nodeNames;
            this.addition = addition;
        }
    }

    @TestOnly
    Object dataNodesMutex() {
        return dataNodesMutex;
    }

    @TestOnly
    Map<Integer, ZoneState> zonesState() {
        return zonesState;
    }

    @TestOnly
    NavigableMap<Long, CompletableFuture<Void>> topVerFutures() {
        return topVerFutures;
    }
}
