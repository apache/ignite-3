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
import static java.util.concurrent.CompletableFuture.supplyAsync;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.createZoneManagerExecutor;
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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneTopologyAugmentationVault;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesFilterUpdateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesGlobalStateRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVault;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesNodesAttributesVault;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.metastorage.dsl.Statements.iif;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.longToBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;
import static org.apache.ignite.internal.util.IgniteUtils.shutdownAndAwaitTermination;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
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
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
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
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneAlreadyExistsException;
import org.apache.ignite.lang.DistributionZoneBindTableException;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteSystemProperties;
import org.apache.ignite.lang.NodeStoppingException;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.TestOnly;

/**
 * Distribution zones manager.
 */
// TODO: IGNITE-20114 избавиться от констант
public class DistributionZoneManager implements IgniteComponent {
    /** Name of the default distribution zone. */
    public static final String DEFAULT_ZONE_NAME = "Default";

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
    // TODO: IGNITE-20114 избавиться
    private final DistributionZonesConfiguration zonesConfiguration;

    /** Tables configuration. */
    // TODO: IGNITE-20114 избавиться
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
    private final Map<Integer, ZoneState> zonesState = new ConcurrentHashMap<>();

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

    /** Watch listener for data nodes keys. */
    private final WatchListener dataNodesWatchListener;

    /** Watch listener for data nodes keys. */
    private final DistributionZoneRebalanceEngine rebalanceEngine;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /**
     * Creates a new distribution zone manager.
     *
     * @param nodeName Node name.
     * @param zonesConfiguration Distribution zones configuration.
     * @param tablesConfiguration Tables configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param vaultMgr Vault manager.
     * @param catalogManager Catalog manager.
     */
    public DistributionZoneManager(
            String nodeName,
            DistributionZonesConfiguration zonesConfiguration,
            TablesConfiguration tablesConfiguration,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            VaultManager vaultMgr,
            CatalogManager catalogManager
    ) {
        this.zonesConfiguration = zonesConfiguration;
        this.tablesConfiguration = tablesConfiguration;
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.vaultMgr = vaultMgr;
        this.catalogManager = catalogManager;

        this.topologyWatchListener = createMetastorageTopologyListener();

        this.dataNodesWatchListener = createMetastorageDataNodesListener();

        executor = createZoneManagerExecutor(NamedThreadFactory.create(nodeName, "dst-zones-scheduler", LOG));

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
            zonesConfiguration.distributionZones().any().filter().listen(onUpdateFilter());

            zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
            zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
            zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());
            zonesConfiguration.defaultDistributionZone().filter().listen(onUpdateFilter());

            rebalanceEngine.start();

            logicalTopologyService.addEventListener(topologyEventListener);

            metaStorageManager.registerPrefixWatch(zonesLogicalTopologyPrefix(), topologyWatchListener);
            metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), dataNodesWatchListener);

            restoreGlobalStateFromVault();

            long appliedRevision = metaStorageManager.appliedRevision();

            // onCreate for default zone is not called, so we have to restore it's state on start
            createOrRestoreZoneState(zonesConfiguration.defaultDistributionZone().value(), appliedRevision);
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

        shutdownAndAwaitTermination(executor, 10, SECONDS);
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
                        throw new DistributionZoneNotFoundException(name, e);
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
     * Asynchronously gets data nodes of the zone using causality token.
     *
     * <p>The returned future can be completed with {@link DistributionZoneNotFoundException} if the zone with the provided {@code zoneId}
     * does not exist.
     *
     * @param causalityToken Causality token.
     * @param zoneId Zone id.
     * @return The future which will be completed with data nodes for the zoneId or with exception.
     */
    // TODO: Will be implemented in IGNITE-19506.
    public CompletableFuture<Set<String>> dataNodes(long causalityToken, int zoneId) {
        return null;
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

            return completedFuture(null);
        };
    }

    /**
     * Creates configuration listener for updates of zone's filter value.
     *
     * @return Configuration listener for updates of zone's filter value.
     */
    private ConfigurationListener<String> onUpdateFilter() {
        return ctx -> {
            if (ctx.oldValue() == null) {
                // zone creation, already handled in a separate listener.
                return completedFuture(null);
            }

            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            VaultEntry filterUpdateRevision = vaultMgr.get(zonesFilterUpdateRevision()).join();

            long eventRevision = ctx.storageRevision();

            if (filterUpdateRevision != null) {
                // This means that we have already handled event with this revision.
                // It is possible when node was restarted after this listener completed,
                // but applied revision didn't have time to be propagated to the Vault.
                if (bytesToLong(filterUpdateRevision.value()) >= eventRevision) {
                    return completedFuture(null);
                }
            }

            vaultMgr.put(zonesFilterUpdateRevision(), longToBytes(eventRevision)).join();

            saveDataNodesToMetaStorageOnScaleUp(zoneId, eventRevision);

            return completedFuture(null);
        };
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            DistributionZoneView zone = ctx.newValue();

            createOrRestoreZoneState(zone, ctx.storageRevision());

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
     * Creates or restores zone's state depending on the {@link ZoneState#topologyAugmentationMap()} existence in the Vault.
     * We save {@link ZoneState#topologyAugmentationMap()} in the Vault every time we receive logical topology changes from the metastore.
     *
     * @param zone Zone's view.
     * @param revision Revision for which we restore zone's state.
     */
    private void createOrRestoreZoneState(DistributionZoneView zone, long revision) {
        int zoneId = zone.zoneId();

        VaultEntry topologyAugmentationMapFromVault = vaultMgr.get(zoneTopologyAugmentationVault(zoneId)).join();

        // First creation of a zone, or first call on the manager start for the default zone.
        if (topologyAugmentationMapFromVault == null) {
            ZoneState zoneState = new ZoneState(executor);

            ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

            assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';

            Set<Node> dataNodes = logicalTopology.stream().map(NodeWithAttributes::node).collect(toSet());

            initDataNodesAndTriggerKeysInMetaStorage(zoneId, revision, dataNodes);
        } else {
            // Restart case, when topologyAugmentationMap has already been saved during a cluster work.
            ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap = fromBytes(topologyAugmentationMapFromVault.value());

            ZoneState zoneState = new ZoneState(executor, topologyAugmentationMap);

            Entry dataNodes = metaStorageManager.getLocally(zoneDataNodesKey(zoneId), revision);

            if (dataNodes != null) {
                String filter = zone.filter();

                zoneState.nodes(filterDataNodes(DistributionZonesUtil.dataNodes(fromBytes(dataNodes.value())), filter, nodesAttributes()));
            }

            ZoneState prevZoneState = zonesState.putIfAbsent(zoneId, zoneState);

            assert prevZoneState == null : "Zone's state was created twice [zoneId = " + zoneId + ']';

            Optional<Long> maxScaleUpRevision = zoneState.highestRevision(true);

            Optional<Long> maxScaleDownRevision = zoneState.highestRevision(false);

            VaultEntry filterUpdateRevision = vaultMgr.get(zonesFilterUpdateRevision()).join();

            restoreTimers(zone, zoneState, maxScaleUpRevision, maxScaleDownRevision, filterUpdateRevision);
        }
    }

    /**
     * Restores timers that were scheduled before a node's restart.
     * Take the highest revision from the {@link ZoneState#topologyAugmentationMap()}, compare it with the revision
     * of the last update of the zone's filter and schedule scale up/scale down timers. Filter revision is taken into account because
     * any filter update triggers immediate scale up.
     *
     * @param zone Zone's view.
     * @param zoneState Zone's state from Distribution Zone Manager
     * @param maxScaleUpRevisionOptional Max revision from the {@link ZoneState#topologyAugmentationMap()} for node joins.
     * @param maxScaleDownRevisionOptional Max revision from the {@link ZoneState#topologyAugmentationMap()} for node removals.
     * @param filterUpdateRevisionVaultEntry Revision of the last update of the zone's filter.
     */
    private void restoreTimers(
            DistributionZoneView zone,
            ZoneState zoneState,
            Optional<Long> maxScaleUpRevisionOptional,
            Optional<Long> maxScaleDownRevisionOptional,
            VaultEntry filterUpdateRevisionVaultEntry
    ) {
        int zoneId = zone.zoneId();

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

                            // TODO: IGNITE-19506 Think carefully for the scenario when scale up timer was immediate before restart and
                            // causality data nodes is implemented.
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
                            () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, maxScaleUpRevision)
                    );
                }
        );

        maxScaleDownRevisionOptional.ifPresent(
                maxScaleDownRevision -> zoneState.rescheduleScaleDown(
                        zone.dataNodesAutoAdjustScaleDown(),
                        () -> saveDataNodesToMetaStorageOnScaleDown(zoneId, maxScaleDownRevision)
                )
        );
    }

    /**
     * Method initialise data nodes value for the specified zone, also sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)}, {@link DistributionZonesUtil#zoneScaleDownChangeTriggerKey(int)} and
     * {@link DistributionZonesUtil#zonesChangeTriggerKey(int)} if it passes the condition. It is called on the first creation of a zone.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     * @param dataNodes Data nodes.
     */
    private void initDataNodesAndTriggerKeysInMetaStorage(
            int zoneId,
            long revision,
            Set<Node> dataNodes
    ) {
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
                    LOG.error(
                            "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]",
                            e,
                            zoneId,
                            dataNodes,
                            revision
                    );
                } else if (res.getAsBoolean()) {
                    LOG.debug("Update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]", zoneId, dataNodes, revision);
                } else {
                    LOG.debug(
                            "Failed to update zones' dataNodes value [zoneId = {}, dataNodes = {}, revision = {}]",
                            zoneId,
                            dataNodes,
                            revision
                    );
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

            metaStorageManager.invoke(iif).whenComplete((res, e) -> {
                if (e != null) {
                    LOG.error(
                            "Failed to delete zone's dataNodes keys [zoneId = {}, revision = {}]",
                            e,
                            zoneId,
                            revision
                    );
                } else if (res.getAsBoolean()) {
                    LOG.debug("Delete zone's dataNodes keys [zoneId = {}, revision = {}]", zoneId, revision);
                } else {
                    LOG.debug("Failed to delete zone's dataNodes keys [zoneId = {}, revision = {}]", zoneId, revision);
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

            if (newTopology.version() == 1) {
                // Very first start of the cluster, so we just initialize zonesLogicalTopologyVersionKey
                updateCondition = notExists(zonesLogicalTopologyVersionKey());
            } else {
                if (topologyLeap) {
                    updateCondition = value(zonesLogicalTopologyVersionKey()).lt(longToBytes(newTopology.version()));
                } else {
                    // This condition may be stronger, as far as we receive topology events one by one.
                    updateCondition = value(zonesLogicalTopologyVersionKey()).eq(longToBytes(newTopology.version() - 1));
                }
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
     * Restores from vault logical topology and nodes' attributes fields in {@link DistributionZoneManager} after restart.
     */
    private void restoreGlobalStateFromVault() {
        VaultEntry topologyEntry = vaultMgr.get(zonesLogicalTopologyVault()).join();

        VaultEntry nodeAttributesEntry = vaultMgr.get(zonesNodesAttributesVault()).join();

        if (topologyEntry != null && topologyEntry.value() != null) {
            assert nodeAttributesEntry != null : "Nodes' attributes cannot be null when logical topology is not null.";
            assert nodeAttributesEntry.value() != null : "Nodes' attributes cannot be null when logical topology is not null.";

            logicalTopology = fromBytes(topologyEntry.value());

            nodesAttributes = fromBytes(nodeAttributesEntry.value());
        }

        assert topologyEntry == null || topologyEntry.value() == null || logicalTopology.equals(fromBytes(topologyEntry.value()))
                : "Initial value of logical topology was changed after initialization from the vault manager.";

        assert nodeAttributesEntry == null
                || nodeAttributesEntry.value() == null
                || nodesAttributes.equals(fromBytes(nodeAttributesEntry.value()))
                : "Initial value of nodes' attributes was changed after initialization from the vault manager.";
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

                    VaultEntry globalStateRevision = vaultMgr.get(zonesGlobalStateRevision()).join();

                    if (globalStateRevision != null) {
                        // This means that we have already handled event with this revision.
                        // It is possible when node was restarted after this listener completed,
                        // but applied revision didn't have time to be propagated to the Vault.
                        if (bytesToLong(globalStateRevision.value()) >= revision) {
                            return completedFuture(null);
                        }
                    }

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

                    NamedConfigurationTree<DistributionZoneConfiguration, DistributionZoneView, DistributionZoneChange> zones =
                            zonesConfiguration.distributionZones();

                    Set<Integer> zoneIds = new HashSet<>();

                    for (int i = 0; i < zones.value().size(); i++) {
                        DistributionZoneView zoneView = zones.value().get(i);

                        scheduleTimers(zoneView, addedNodes, removedNodes, revision);

                        zoneIds.add(zoneView.zoneId());
                    }

                    DistributionZoneView defaultZoneView = zonesConfiguration.value().defaultDistributionZone();

                    scheduleTimers(defaultZoneView, addedNodes, removedNodes, revision);

                    zoneIds.add(defaultZoneView.zoneId());

                    newLogicalTopology.forEach(n -> nodesAttributes.put(n.nodeId(), n.nodeAttributes()));

                    logicalTopology = newLogicalTopology;

                    saveStatesToVault(newLogicalTopology, revision, zoneIds);

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
     * Saves states of the Distribution Zone Manager to vault atomically in one batch.
     * After restart it could be used to restore these fields.
     *
     * @param newLogicalTopology Logical topology.
     * @param revision Revision of the event, which triggers update.
     * @param zoneIds Set of zone id's, whose states will be staved in the Vault
     */
    private void saveStatesToVault(Set<NodeWithAttributes> newLogicalTopology, long revision, Set<Integer> zoneIds) {
        Map<ByteArray, byte[]> batch = IgniteUtils.newHashMap(3 + zoneIds.size());

        batch.put(zonesLogicalTopologyVault(), toBytes(newLogicalTopology));

        batch.put(zonesNodesAttributesVault(), toBytes(nodesAttributes()));

        for (Integer zoneId : zoneIds) {
            batch.put(zoneTopologyAugmentationVault(zoneId), toBytes(zonesState.get(zoneId).topologyAugmentationMap));
        }

        batch.put(zonesGlobalStateRevision(), longToBytes(revision));

        vaultMgr.putAll(batch).join();
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
                                LOG.debug(
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
                    LOG.warn("Failed to update zones' dataNodes value after scale up [zoneId = {}, revision = {}]", e, zoneId, revision);
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
                                LOG.debug(
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
        private final ScheduledExecutorService executor;

        /** Data nodes. */
        private volatile Set<String> nodes;

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
         * Constructor.
         *
         * @param executor Executor for scheduling tasks for scale up and scale down processes.
         * @param topologyAugmentationMap Map that stores pairs revision -> {@link Augmentation} for a zone. With this map we can
         *         track which nodes should be added or removed in the processes of scale up or scale down. Revision helps to track
         *         visibility of the events of adding or removing nodes because any process of scale up or scale down has a revision that
         *         triggered this process.
         */
        ZoneState(ScheduledExecutorService executor, ConcurrentSkipListMap<Long, Augmentation> topologyAugmentationMap) {
            this.executor = executor;
            this.topologyAugmentationMap = topologyAugmentationMap;
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
         * Reschedules existing scale up task, if it is not started yet and the delay of this task is not immediate,
         * or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         */
        synchronized void rescheduleScaleUp(long delay, Runnable runnable) {
            stopScaleUp();

            scaleUpTask = executor.schedule(runnable, delay, SECONDS);

            scaleUpTaskDelay = delay;
        }

        /**
         * Reschedules existing scale down task, if it is not started yet and the delay of this task is not immediate,
         * or schedules new one, if the current task cannot be canceled.
         *
         * @param delay Delay to start runnable in seconds.
         * @param runnable Custom logic to run.
         */
        synchronized void rescheduleScaleDown(long delay, Runnable runnable) {
            stopScaleDown();

            scaleDownTask = executor.schedule(runnable, delay, SECONDS);

            scaleDownTaskDelay = delay;
        }

        /**
         * Cancels task for scale up and scale down. Used on {@link ZonesConfigurationListener#onDelete(ConfigurationNotificationEvent)}.
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
    private static class Augmentation implements Serializable {
        private static final long serialVersionUID = -7957428671075739621L;

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
    public Map<Integer, ZoneState> zonesState() {
        return zonesState;
    }

    @TestOnly
    public Set<NodeWithAttributes> logicalTopology() {
        return logicalTopology;
    }
}
