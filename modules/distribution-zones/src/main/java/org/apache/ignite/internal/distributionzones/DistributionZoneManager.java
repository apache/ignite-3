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
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesAndUpdateTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.startsWith;
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
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.configuration.tree.ConfigurationNodeAlreadyExistException;
import org.apache.ignite.internal.configuration.tree.ConfigurationNodeDoesNotExistException;
import org.apache.ignite.internal.configuration.tree.ConfigurationNodeRemovedException;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
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
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
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

    private final Object dataNodesMutex = new Object();

    private long lastTopVer;

    private final Map<Integer, DataNodes> dataNodes = new ConcurrentHashMap<>();

    private final NavigableMap<Long, CompletableFuture<Void>> topVerFutures = new ConcurrentSkipListMap();

    private final NavigableMap<Long, Long> topVerScaleUpAndRevision = new ConcurrentSkipListMap<>();

    private final NavigableMap<Long, Long> topVerScaleDownAndRevision = new ConcurrentSkipListMap<>();

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new LogicalTopologyEventListener() {
        @Override
        public void onNodeJoined(ClusterNode joinedNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology, false);
        }

        @Override
        public void onNodeLeft(ClusterNode leftNode, LogicalTopologySnapshot newTopology) {
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

    /** Watch listener. Needed to unregister it on {@link DistributionZoneManager#stop()}. */
    private final WatchListener topologyWatchListener;

    private final WatchListener dataNodesWatchListener;

    static class DataNodes {
        private Set<String> nodes;

        private long scaleUpRevision;

        private long scaleDownRevision;

        private final NavigableMap<Long, CompletableFuture<Void>> revisionScaleUpFutures = new ConcurrentSkipListMap();

        private final NavigableMap<Long, CompletableFuture<Void>> revisionScaleDownFutures = new ConcurrentSkipListMap();

        public DataNodes() {
            nodes = emptySet();
        }

        public DataNodes(Set<String> nodes, long scaleUpRevision, long scaleDownRevision) {
            this.nodes = nodes;
            this.scaleUpRevision = scaleUpRevision;
            this.scaleDownRevision = scaleDownRevision;
        }

        public Set<String> nodes() {
            return nodes;
        }

        public void nodes(Set<String> nodes) {
            this.nodes = nodes;
        }

        public long scaleUpRevision() {
            return scaleUpRevision;
        }

        public void scaleUpRevision(long scaleUpRevision) {
            this.scaleUpRevision = scaleUpRevision;
        }

        public long scaleDownRevision() {
            return scaleDownRevision;
        }

        public void scaleDownRevision(long scaleDownRevision) {
            this.scaleDownRevision = scaleDownRevision;
        }

        public NavigableMap<Long, CompletableFuture<Void>> getRevisionScaleUpFutures() {
            return revisionScaleUpFutures;
        }

        public NavigableMap<Long, CompletableFuture<Void>> getRevisionScaleDownFutures() {
            return revisionScaleDownFutures;
        }
    }

    @TestOnly
    public Map<Integer, DataNodes> dataNodes() {
        return dataNodes;
    }

    @TestOnly
    public NavigableMap<Long, CompletableFuture<Void>> topVerFutures() {
        return topVerFutures;
    }

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

        dataNodes.put(DEFAULT_ZONE_ID, new DataNodes());
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
                AtomicInteger zoneId = new AtomicInteger();

                try {
                    zonesListChange.create(distributionZoneCfg.name(), zoneChange -> {
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

                        zoneId.set(intZoneId);

                        synchronized (dataNodesMutex) {
                            dataNodes.put(intZoneId, new DataNodes());
                        }
                    });
                } catch (ConfigurationNodeAlreadyExistException e) {
                    synchronized (dataNodesMutex) {
                        dataNodes.remove(zoneId.get());
                    }

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

                synchronized (dataNodesMutex) {
                    DataNodes dataNodesMeta = dataNodes.remove(zoneView.zoneId());

                    dataNodesMeta.revisionScaleUpFutures.values()
                            .forEach(fut0 -> fut.completeExceptionally(new DistributionZoneNotFoundException(name)));

                    dataNodesMeta.revisionScaleDownFutures.values()
                            .forEach(fut0 -> fut.completeExceptionally(new DistributionZoneNotFoundException(name)));
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
     * Data nodes.
     *
     * @param zoneId Zone id.
     * @param topVer Topology version.
     * @return Data nodes future.
     */
    public CompletableFuture<Set<String>> getDataNodes(int zoneId, long topVer) {
        boolean immediateScaleUp0 = false;
        boolean immediateScaleDown0 = false;

        if (zoneId == DEFAULT_ZONE_ID) {
            immediateScaleUp0 = zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().value() == 0;
            immediateScaleDown0 = zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().value() == 0;
        } else {
            NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                    zonesConfiguration.distributionZones();

            for (int i = 0; i < zones.value().size(); i++) {
                DistributionZoneView zone = zones.value().get(i);

                immediateScaleUp0 = zone.dataNodesAutoAdjustScaleUp() == 0;
                immediateScaleDown0 = zone.dataNodesAutoAdjustScaleDown() == 0;
            }
        }

        boolean immediateScaleUp = immediateScaleUp0;
        boolean immediateScaleDown = immediateScaleDown0;

        if (!immediateScaleUp && !immediateScaleDown) {

            DataNodes dataNodes = this.dataNodes.get(zoneId);

            Set<String> nodes;

            if (dataNodes == null) {
                nodes = emptySet();
            } else {
                nodes = this.dataNodes.get(zoneId).nodes();
            }

            return completedFuture(nodes);
        }

        CompletableFuture<Set<String>> dataNodesFut = new CompletableFuture<>();

        CompletableFuture<Void> topVerFut;

        synchronized (dataNodesMutex) {
            if (topVer > lastTopVer) {
                topVerFut = topVerFutures.get(topVer);

                if (topVerFut == null) {
                    topVerFut = new CompletableFuture<>();

                    topVerFutures.put(topVer, topVerFut);
                }
            } else {
                topVerFut = completedFuture(null);
            }
        }

        topVerFut.thenAcceptAsync(ignored -> {
            synchronized (dataNodesMutex) {
                CompletableFuture<Void> topVerScaleUpFut = null;

                if (immediateScaleUp) {
                    Map.Entry<Long, Long> scaleUpRevisionEntry = topVerScaleUpAndRevision.ceilingEntry(topVer);

                    Long scaleUpRevision = null;

                    if (scaleUpRevisionEntry != null) {
                        scaleUpRevision = scaleUpRevisionEntry.getValue();
                    }

                    if (scaleUpRevision != null
                            && (dataNodes.get(zoneId) == null || dataNodes.get(zoneId).scaleUpRevision() < scaleUpRevision)) {
                        Map.Entry<Long, CompletableFuture<Void>> ceilingEntry =
                                dataNodes.get(zoneId).getRevisionScaleUpFutures().ceilingEntry(scaleUpRevision);

                        if (ceilingEntry != null) {
                            topVerScaleUpFut = ceilingEntry.getValue();
                        }

                        if (topVerScaleUpFut == null) {
                            topVerScaleUpFut = new CompletableFuture<>();

                            dataNodes.get(zoneId).getRevisionScaleUpFutures().put(scaleUpRevision, topVerScaleUpFut);
                        }
                    } else {
                        topVerScaleUpFut = completedFuture(null);
                    }
                } else {
                    topVerScaleUpFut = completedFuture(null);
                }

                topVerScaleUpFut.thenAcceptAsync(ignored0 -> {
                    CompletableFuture<Void> topVerScaleDownFut = null;

                    synchronized (dataNodesMutex) {
                        if (immediateScaleDown) {
                            Map.Entry<Long, Long> scaleDownRevisionEntry = topVerScaleDownAndRevision.ceilingEntry(topVer);

                            Long scaleDownRevision = null;

                            if (scaleDownRevisionEntry != null) {
                                scaleDownRevision = scaleDownRevisionEntry.getValue();
                            }

                            if (scaleDownRevision != null
                                    && (dataNodes.get(zoneId) == null || dataNodes.get(zoneId).scaleDownRevision() < scaleDownRevision)) {
                                Map.Entry<Long, CompletableFuture<Void>> ceilingEntry =
                                        dataNodes.get(zoneId).getRevisionScaleDownFutures().ceilingEntry(scaleDownRevision);

                                if (ceilingEntry != null) {
                                    topVerScaleDownFut = ceilingEntry.getValue();
                                }

                                if (topVerScaleDownFut == null) {
                                    topVerScaleDownFut = new CompletableFuture<>();

                                    dataNodes.get(zoneId).revisionScaleDownFutures.put(scaleDownRevision, topVerScaleDownFut);
                                }
                            } else {
                                topVerScaleDownFut = completedFuture(null);
                            }
                        } else {
                            topVerScaleDownFut = completedFuture(null);
                        }
                    }

                    topVerScaleDownFut.thenAcceptAsync(ignored1 -> {
                        dataNodesFut.complete(dataNodes.get(zoneId).nodes());
                    });
                });
            }
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

            zonesConfiguration.distributionZones().value().namedListKeys()
                    .forEach(zoneName -> {
                        int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId().value();

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

            zonesState.remove(zoneId);

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
            long appliedRevision = metaStorageManager.appliedRevision();

            vaultMgr.get(zonesLogicalTopologyKey())
                    .thenAccept(vaultEntry -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            if (vaultEntry != null && vaultEntry.value() != null) {
                                logicalTopology = fromBytes(vaultEntry.value());

                                // init keys and data nodes for default zone
                                saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                                        DEFAULT_ZONE_ID,
                                        appliedRevision,
                                        logicalTopology
                                );

                                zonesConfiguration.distributionZones().value().namedListKeys()
                                        .forEach(zoneName -> {
                                            int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId().value();

                                            saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                                                    zoneId,
                                                    appliedRevision,
                                                    logicalTopology
                                            );
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

    private WatchListener createMetastorageTopologyListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
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

                        SortedMap<Long, CompletableFuture<Void>> topVerFuts = topVerFutures.headMap(topVer, true);

                        topVerFuts.values().forEach(v -> v.complete(null));

                        topVerFuts.clear();

                        if (!addedNodes.isEmpty()) {
                            topVerScaleUpAndRevision.put(topVer, revision);
                        }

                        if (!removedNodes.isEmpty()) {
                            topVerScaleDownAndRevision.put(topVer, revision);
                        }

                        topVerScaleUpAndRevision.headMap(topVer).clear();
                        topVerScaleDownAndRevision.headMap(topVer).clear();
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

    private WatchListener createMetastorageDataNodesListener() {
        return new WatchListener() {
            @Override
            public void onUpdate(WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
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
                        DataNodes dataNodesMeta = dataNodes.get(zoneId);

                        if (dataNodesMeta != null) {
                            dataNodesMeta.nodes(newDataNodes);

                            if (scaleUpRevision > 0) {
                                dataNodesMeta.scaleUpRevision(scaleUpRevision);
                            }

                            if (scaleDownRevision > 0) {
                                dataNodesMeta.scaleDownRevision(scaleDownRevision);
                            }

                            if (scaleUpRevision > 0) {
                                SortedMap<Long, CompletableFuture<Void>> revisionScaleUpFuts =
                                        dataNodes.get(zoneId).getRevisionScaleUpFutures().headMap(scaleUpRevision, true);

                                revisionScaleUpFuts.values().forEach(v -> v.complete(null));

                                revisionScaleUpFuts.clear();
                            }

                            if (scaleDownRevision > 0) {
                                SortedMap<Long, CompletableFuture<Void>> revisionScaleDownFuts =
                                        dataNodes.get(zoneId).getRevisionScaleDownFutures().headMap(scaleDownRevision, true);

                                revisionScaleDownFuts.values().forEach(v -> v.complete(null));

                                revisionScaleDownFuts.clear();
                            }
                        }
                    }
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

                Map<String, Integer> dataNodesFromMetaStorage = fromBytes(values.get(zoneDataNodesKey(zoneId)).value());

                long scaleUpTriggerRevision = bytesToLong(values.get(zoneScaleUpChangeTriggerKey(zoneId)).value());

                long scaleDownTriggerRevision = bytesToLong(values.get(zoneScaleDownChangeTriggerKey(zoneId)).value());

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

                Map<String, Integer> dataNodesFromMetaStorage = fromBytes(values.get(zoneDataNodesKey(zoneId)).value());

                long scaleUpTriggerRevision = bytesToLong(values.get(zoneScaleUpChangeTriggerKey(zoneId)).value());

                long scaleDownTriggerRevision = bytesToLong(values.get(zoneScaleDownChangeTriggerKey(zoneId)).value());

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

        /**
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         */
        ZoneState(ScheduledExecutorService executor) {
            this.executor = executor;
            topologyAugmentationMap = new ConcurrentSkipListMap<>();
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
}
