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

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesAndUpdateTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerKeyConditionForZonesChanges;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerScaleUpScaleDownKeysCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndScaleUpTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKeys;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationChangeException;
import org.apache.ignite.configuration.NamedConfigurationTree;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.configuration.validation.ConfigurationValidationException;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyEventListener;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologyService;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalTopologySnapshot;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneChange;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneConfiguration;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneAlreadyExistsException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneBindTableException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneRenameException;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.dsl.CompoundCondition;
import org.apache.ignite.internal.metastorage.dsl.Condition;
import org.apache.ignite.internal.metastorage.dsl.StatementResult;
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.schema.configuration.TableChange;
import org.apache.ignite.internal.schema.configuration.TableConfiguration;
import org.apache.ignite.internal.schema.configuration.TableView;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** Name of the default distribution zone. */
    public static final String DEFAULT_ZONE_NAME = "Default";

    /** Id of the default distribution zone. */
    public static final int DEFAULT_ZONE_ID = 0;

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

    private final ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(
            Math.min(Runtime.getRuntime().availableProcessors() * 3, 20),
            new NamedThreadFactory("dst-zones-scheduler", LOG),
            new ThreadPoolExecutor.DiscardPolicy()
    );

    private final Map<Integer, ZoneState> zonesTimers = new ConcurrentHashMap<>();

    /** Listener for a topology events. */
    private final LogicalTopologyEventListener topologyEventListener = new LogicalTopologyEventListener() {
        @Override
        public void onAppeared(ClusterNode appearedNode, LogicalTopologySnapshot newTopology) {
            updateLogicalTopologyInMetaStorage(newTopology, false);
        }

        @Override
        public void onDisappeared(ClusterNode disappearedNode, LogicalTopologySnapshot newTopology) {
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

    /** Watch listener id to unregister the watch listener on {@link DistributionZoneManager#stop()}. */
    private volatile Long watchListenerId;

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
            VaultManager vaultMgr
    ) {
        this.zonesConfiguration = zonesConfiguration;
        this.tablesConfiguration = tablesConfiguration;
        this.metaStorageManager = metaStorageManager;
        this.logicalTopologyService = logicalTopologyService;
        this.vaultMgr = vaultMgr;

        logicalTopology = Collections.emptySet();
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
                        if (distributionZoneCfg.dataNodesAutoAdjust() == null) {
                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleUp(
                                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                        }

                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() == null) {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                        } else {
                            zoneChange.changeDataNodesAutoAdjustScaleDown(distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                        }

                        int intZoneId = zonesChange.globalIdCounter() + 1;
                        zonesChange.changeGlobalIdCounter(intZoneId);

                        zoneChange.changeZoneId(intZoneId);
                    });
                } catch (IllegalArgumentException e) {
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
     *      {@link DistributionZoneRenameException} if a zone with the given name already exists
     *      or zone with name for renaming already exists,
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

            zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                NamedListChange<DistributionZoneView, DistributionZoneChange> renameChange;

                try {
                    renameChange = zonesListChange.rename(name, distributionZoneCfg.name());
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneRenameException(name, distributionZoneCfg.name(), e);
                }

                try {
                    renameChange
                            .update(
                                    distributionZoneCfg.name(), zoneChange -> {
                                        if (distributionZoneCfg.dataNodesAutoAdjust() != null) {
                                            zoneChange.changeDataNodesAutoAdjust(distributionZoneCfg.dataNodesAutoAdjust());
                                            zoneChange.changeDataNodesAutoAdjustScaleUp(Integer.MAX_VALUE);
                                            zoneChange.changeDataNodesAutoAdjustScaleDown(Integer.MAX_VALUE);
                                        }

                                        if (distributionZoneCfg.dataNodesAutoAdjustScaleUp() != null) {
                                            zoneChange.changeDataNodesAutoAdjustScaleUp(
                                                    distributionZoneCfg.dataNodesAutoAdjustScaleUp());
                                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                        }

                                        if (distributionZoneCfg.dataNodesAutoAdjustScaleDown() != null) {
                                            zoneChange.changeDataNodesAutoAdjustScaleDown(
                                                    distributionZoneCfg.dataNodesAutoAdjustScaleDown());
                                            zoneChange.changeDataNodesAutoAdjust(Integer.MAX_VALUE);
                                        }
                                    });
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneNotFoundException(distributionZoneCfg.name(), e);
                }
            }))
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            fut.completeExceptionally(
                                    unwrapDistributionZoneException(
                                            e,
                                            DistributionZoneRenameException.class,
                                            DistributionZoneNotFoundException.class,
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

                //TODO: IGNITE-18516 Access to other configuration must be thread safe.
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

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            zonesConfiguration.distributionZones().listenElements(new ZonesConfigurationListener());

            // Init timers after restart.
            //zonesTimers.putIfAbsent(zonesConfiguration.defaultDistributionZone().zoneId().value(), new ZoneState());

            zonesConfiguration.distributionZones().value().namedListKeys()
                    .forEach(zoneName -> {
                        int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId().value();

                        zonesTimers.putIfAbsent(zoneId, new ZoneState());
                    });

            logicalTopologyService.addEventListener(topologyEventListener);

            registerMetaStorageWatchListener()
                    .thenAccept(ignore -> initDataNodesFromVaultManager())
                    .thenAccept(ignore -> initLogicalTopologyAndVersionInMetaStorageOnStart());
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

        logicalTopologyService.removeEventListener(topologyEventListener);

        if (watchListenerId != null) {
            metaStorageManager.unregisterWatch(watchListenerId);
        }
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            ZoneState zoneState = new ZoneState();

            // TODO default timer?
            zonesTimers.putIfAbsent(zoneId, zoneState);

            // logicalTopology can be updated concurrently by the watch listener.
            saveDataNodesAndUpdateTriggerKeysInMetaStorage(zoneId, ctx.storageRevision(), toBytes(logicalTopology));

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            zonesTimers.get(zoneId).stopTimers();

            removeTriggerKeysAndDataNodes(zoneId, ctx.storageRevision());

            zonesTimers.remove(zoneId);

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            int oldScaleUp = ctx.oldValue().dataNodesAutoAdjustScaleUp();

            int newScaleUp = ctx.newValue().dataNodesAutoAdjustScaleUp();

            if (newScaleUp != Integer.MAX_VALUE && oldScaleUp != newScaleUp) {
                // It is safe to zonesTimers.get(zoneId) in term of NPE because meta storage notifications are one-threaded
                zonesTimers.get(zoneId).rescheduleScaleUp(newScaleUp, null);
            }

            return completedFuture(null);
        }
    }

    /**
     * Method updates data nodes value for the specified zone, also sets {@code revision} to the
     * {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private void saveDataNodesAndUpdateTriggerKeysInMetaStorage(int zoneId, long revision, byte[] dataNodes) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the revision of the event is newer than value in that trigger key,
            // so we do not react on a stale events
            CompoundCondition triggerKeyCondition = triggerKeyConditionForZonesChanges(revision, zoneId);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKeys(zoneId, revision, dataNodes);

            If iif = If.iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

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

            If iif = If.iif(triggerKeyCondition, removeKeysUpd, ops().yield(false));

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
            Set<String> topologyFromCmg = newTopology.nodes().stream().map(ClusterNode::name).collect(Collectors.toSet());

            Condition updateCondition;

            if (topologyLeap) {
                updateCondition = value(zonesLogicalTopologyVersionKey()).lt(ByteUtils.longToBytes(newTopology.version()));
            } else {
                // This condition may be stronger, as far as we receive topology events one by one.
                updateCondition = value(zonesLogicalTopologyVersionKey()).eq(ByteUtils.longToBytes(newTopology.version() - 1));
            }

            If iff = If.iif(
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
                                Set<String> topologyFromCmg = snapshot.nodes().stream().map(ClusterNode::name).collect(Collectors.toSet());

                                Condition topologyVersionCondition = topVerFromMetaStorage == null
                                        ? notExists(zonesLogicalTopologyVersionKey()) :
                                        value(zonesLogicalTopologyVersionKey()).eq(topVerFromMetaStorage);

                                If iff = If.iif(topologyVersionCondition,
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
            // TODO: Remove this call as part of https://issues.apache.org/jira/browse/IGNITE-18397
            vaultMgr.get(MetaStorageManagerImpl.APPLIED_REV)
                    .thenApply(appliedRevision -> appliedRevision == null ? 0L : bytesToLong(appliedRevision.value()))
                    .thenAccept(vaultAppliedRevision -> {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            vaultMgr.get(zonesLogicalTopologyKey())
                                    .thenAccept(vaultEntry -> {
                                        if (!busyLock.enterBusy()) {
                                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                                        }

                                        try {
                                            if (vaultEntry != null && vaultEntry.value() != null) {
                                                logicalTopology = fromBytes(vaultEntry.value());

                                                // init keys and data nodes for default zone
//                                                initTriggerKeysAndDataNodesInMetaStorage(
//                                                        zonesConfiguration.defaultDistributionZone().zoneId().value(),
//                                                        vaultAppliedRevision,
//                                                        vaultEntry.value()
//                                                );

                                                zonesConfiguration.distributionZones().value().namedListKeys()
                                                        .forEach(zoneName -> {
                                                            int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId()
                                                                    .value();

                                                            saveDataNodesAndUpdateTriggerKeysInMetaStorage(
                                                                    zoneId,
                                                                    vaultAppliedRevision,
                                                                    vaultEntry.value()
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
                    });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Registers {@link WatchListener} which updates data nodes of distribution zones on logical topology changing event.
     *
     * @return Future representing pending completion of the operation.
     */
    private CompletableFuture<?> registerMetaStorageWatchListener() {
        return metaStorageManager.registerExactWatch(zonesLogicalTopologyKey(), new WatchListener() {
                    @Override
                    public boolean onUpdate(@NotNull WatchEvent evt) {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            assert evt.single() : "Expected an event with one entry but was an event with several entries with keys: "
                                    + evt.entryEvents().stream().map(entry -> entry.newEntry() == null ? "null" : entry.newEntry().key())
                                    .collect(toList());

                            Entry newEntry = evt.entryEvent().newEntry();

                            Set<String> newLogicalTopology = fromBytes(newEntry.value());

                            List<String> removedNodes =
                                    logicalTopology.stream().filter(node -> !newLogicalTopology.contains(node)).collect(toList());

                            List<String> addedNodes =
                                    newLogicalTopology.stream().filter(node -> !logicalTopology.contains(node)).collect(toList());

                            logicalTopology = newLogicalTopology;

                            zonesConfiguration.distributionZones().value().namedListKeys()
                                    .forEach(zoneName -> {
                                        DistributionZoneConfiguration zoneCfg = zonesConfiguration.distributionZones().get(zoneName);

                                        int autoAdjust = zoneCfg.dataNodesAutoAdjust().value();
                                        int autoAdjustScaleDown = zoneCfg.dataNodesAutoAdjustScaleDown().value();
                                        int autoAdjustScaleUp = zoneCfg.dataNodesAutoAdjustScaleUp().value();

                                        Integer zoneId = zoneCfg.zoneId().value();

                                        if ((!addedNodes.isEmpty() || !removedNodes.isEmpty()) && autoAdjust != Integer.MAX_VALUE) {
                                            //TODO: IGNITE-18134 Create scheduler with dataNodesAutoAdjust timer.
                                            throw new UnsupportedOperationException("Data nodes Auto Adjust is not supported.");
                                        } else {
                                            if (!addedNodes.isEmpty() && autoAdjustScaleUp != Integer.MAX_VALUE) {
                                                //TODO: IGNITE-18121 Create scale up scheduler with dataNodesAutoAdjustScaleUp timer.
                                                zonesTimers.get(zoneId).nodesToAdd(new HashSet<>(addedNodes));

                                                zonesTimers.get(zoneId).rescheduleScaleUp(
                                                        autoAdjustScaleUp,
                                                        () -> CompletableFuture.supplyAsync(
                                                                () -> saveDataNodesToMetaStorageOnScaleUp(zoneId, newEntry.revision()),
                                                                Runnable::run
                                                        )
                                                );
                                            }

                                            if (!removedNodes.isEmpty() && autoAdjustScaleDown != Integer.MAX_VALUE) {
                                                //TODO: IGNITE-18132 Create scale down scheduler with dataNodesAutoAdjustScaleDown timer.
                                                throw new UnsupportedOperationException("Data nodes Auto Adjust Scale Down is not supported.");
                                            }
                                        }
                                    });

                            return true;
                        } finally {
                            busyLock.leaveBusy();
                        }
                    }

                    @Override
                    public void onError(@NotNull Throwable e) {
                        LOG.warn("Unable to process logical topology event", e);
                    }
                })
                .thenAccept(id -> watchListenerId = id);
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
     * Method updates data nodes value for the specified zone after scale up timer timeout,
     * also sets {@code revision} to the {@link DistributionZonesUtil#zoneScaleUpChangeTriggerKey(int)} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private CompletableFuture<Void> saveDataNodesToMetaStorageOnScaleUp(int zoneId, long revision) {
        ZoneState zoneState = zonesTimers.get(zoneId);

        if (zoneState == null) {
            // Zone was deleted
            return completedFuture(null);
        }

        ReentrantLock lockForTimers = zoneState.lockForTimers();

        lockForTimers.lock();

        Set<ByteArray> keysToGetFromMs = Set.of(
                zoneDataNodesKey(zoneId),
                zoneScaleUpChangeTriggerKey(zoneId),
                zoneScaleDownChangeTriggerKey(zoneId)
        );

        return metaStorageManager.getAll(keysToGetFromMs).thenCompose(values -> {
            if (values.containsValue(null)) {
                // Zone was deleted
                return completedFuture(null);
            }

            Set<String> dataNodesFromMetaStorage = fromBytes(values.get(zoneDataNodesKey(zoneId)).value());

            long scaleUpTriggerRevision = bytesToLong(values.get(zoneScaleUpChangeTriggerKey(zoneId)).value());

            long scaleDownTriggerRevision = bytesToLong(values.get(zoneScaleDownChangeTriggerKey(zoneId)).value());

            if (revision <= scaleUpTriggerRevision) {
                return completedFuture(null);
            }

            ReentrantLock lockForZone = zoneState.lock();

            lockForZone.lock();

            Set<String> deltaToAdd;

            try {
                deltaToAdd = new HashSet<>(zoneState.nodesToAdd());
            } finally {
                lockForZone.unlock();
            }

            Set<String> newDataNodes = new HashSet<>(dataNodesFromMetaStorage);

            newDataNodes.addAll(deltaToAdd);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndScaleUpTriggerKey(zoneId, revision, toBytes(newDataNodes));

            If iif = If.iif(
                    triggerScaleUpScaleDownKeysCondition(scaleUpTriggerRevision, scaleDownTriggerRevision, zoneId),
                    dataNodesAndTriggerKeyUpd,
                    ops().yield(false)
            );

            return metaStorageManager.invoke(iif).thenApply(StatementResult::getAsBoolean).thenApply(invokeResult -> {
                if (invokeResult) {
                    lockForZone.lock();
                    try {
                        zoneState.nodesToAdd().removeAll(deltaToAdd);
                    } finally {
                        lockForZone.unlock();
                    }
                } else {
                    LOG.debug("Updating data nodes for a zone has not succeeded [zoneId = {}]", zoneId);

                    return saveDataNodesToMetaStorageOnScaleUp(zoneId, revision);
                }

                return completedFuture(null);
            });
        }).handle((v, e) -> {
            lockForTimers.unlock();

            if (e != null) {
                LOG.warn("Failed to update zones' dataNodes value [zoneId = {}]", e, zoneId);

                return CompletableFuture.<Void>failedFuture(e);
            }

            return CompletableFuture.<Void>completedFuture(null);
        }).thenCompose(Function.identity());
    }

    private class ZoneState {
        private Timer scaleUp;

        private Timer scaleDown;

        private final ReentrantLock lockDeltas;

        private final ReentrantLock lockForTimers;

        private final Set<String> nodesToAdd;

        private final Set<String> nodesToRemove;

        ZoneState() {
            this.lockDeltas = new ReentrantLock();
            this.lockForTimers = new ReentrantLock();
            this.nodesToAdd = new HashSet<>();
            this.nodesToRemove = new HashSet<>();
        }

        /**
         * Reschedules existing task, if it is not started yet, or schedules new one, if the current task cannot be canceled.
         * If the provided {@code runnable} is null, then just tries to reschedule existing task, if it is not started yet, or
         * do nothing otherwise.
         *
         * @param delay Delay to start runnable.
         * @param runnable Custom logic to run.
         */
        private synchronized void rescheduleScaleUp(long delay, @Nullable Runnable runnable) {
            if (scaleUp == null) {
                if (runnable != null) {
                    scaleUp = new Timer(runnable);
                } else {
                    return;
                }
            }

            scaleUp.reschedule(delay, runnable != null);
        }

        private synchronized void rescheduleScaleDown(long delay, @Nullable Runnable runnable) {
            if (scaleDown == null) {
                if (runnable != null) {
                    scaleDown = new Timer(runnable);
                } else {
                    return;
                }
            }

            scaleDown.reschedule(delay, runnable != null);
        }

        private synchronized void stopTimers() {
            if (scaleUp != null) {
                scaleUp.stop();
            }

            if (scaleDown != null) {
                scaleDown.stop();
            }
        }

        public ReentrantLock lock() {
            return lockDeltas;
        }

        public ReentrantLock lockForTimers() {
            return lockForTimers;
        }

        public Set<String> nodesToAdd() {
            return nodesToAdd;
        }

        public Set<String> nodesToRemove() {
            return nodesToRemove;
        }

        public void nodesToAdd(Set<String> nodes) {
            lockDeltas.lock();

            try {
                nodesToAdd.addAll(nodes);
            } finally {
                lockDeltas.unlock();
            }
        }

        public void nodesToRemove(Set<String> nodes) {
            lockDeltas.lock();

            try {
                nodesToRemove.addAll(nodes);
            } finally {
                lockDeltas.unlock();
            }
        }
    }

    private class Timer implements Runnable {
        private final Runnable runnable;

        ScheduledFuture<?> task;

        Timer(Runnable runnable) {
            this.runnable = runnable;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            runnable.run();
        }

        private void reschedule(long delay, boolean scheduleNewTask) {
            if (task != null) {
                if (task.cancel(false)) {
                    task = executor.schedule(this, delay, TimeUnit.MILLISECONDS);

                    return;
                }
            }

            if (scheduleNewTask) {
                task = executor.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        }

        private void stop() {
            task.cancel(false);
        }
    }
}
