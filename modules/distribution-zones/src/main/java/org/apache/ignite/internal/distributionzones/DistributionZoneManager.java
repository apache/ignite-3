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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesKeyAndUpdateTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerKeyCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateLogicalTopologyAndVersion;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.metastorage.MetaStorageManager.APPLIED_REV;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.value;
import static org.apache.ignite.internal.metastorage.dsl.Operations.ops;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.ConfigurationChangeException;
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
import org.apache.ignite.internal.metastorage.dsl.If;
import org.apache.ignite.internal.metastorage.dsl.Update;
import org.apache.ignite.internal.metastorage.impl.MetaStorageManagerImpl;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.jetbrains.annotations.NotNull;

/**
 * Distribution zones manager.
 */
public class DistributionZoneManager implements IgniteComponent {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneManager.class);

    /** Distribution zone configuration. */
    private final DistributionZonesConfiguration zonesConfiguration;

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

    /** The logical topology on the last watch event.
     *  It's enough to mark this field by volatile because we don't update the collection after it is assigned to the field.
     */
    private volatile Set<String> logicalTopology;

    /** Watch listener id to unregister the watch listener on {@link DistributionZoneManager#stop()}. */
    private volatile Long watchListenerId;

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param logicalTopologyService Logical topology service.
     * @param vaultMgr Vault manager.
     */
    public DistributionZoneManager(
            DistributionZonesConfiguration zonesConfiguration,
            MetaStorageManager metaStorageManager,
            LogicalTopologyService logicalTopologyService,
            VaultManager vaultMgr
    ) {
        this.zonesConfiguration = zonesConfiguration;
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
     *      {@link IllegalArgumentException} if distribution zone configuration is null,
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> createZone(DistributionZoneConfigurationParameters distributionZoneCfg) {
        if (distributionZoneCfg == null) {
            return failedFuture(new IllegalArgumentException("Distribution zone configuration is null"));
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
     *      {@link IllegalArgumentException} if {@code name} or {@code distributionZoneCfg} is {@code null},
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneConfigurationParameters distributionZoneCfg) {
        if (name == null || name.isEmpty()) {
            return failedFuture(new IllegalArgumentException("Distribution zone name is null or empty [name=" + name + ']'));
        }

        if (distributionZoneCfg == null) {
            return failedFuture(new IllegalArgumentException("Distribution zone configuration is null"));
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
     *      {@link IllegalArgumentException} if {@code name} is {@code null},
     *      {@link NodeStoppingException} if the node is stopping.
     */
    public CompletableFuture<Void> dropZone(String name) {
        if (name == null || name.isEmpty()) {
            return failedFuture(new IllegalArgumentException("Distribution zone name is null or empty [name=" + name + ']'));
        }

        if (!busyLock.enterBusy()) {
            return failedFuture(new NodeStoppingException());
        }

        try {
            CompletableFuture<Void> fut = new CompletableFuture<>();

            zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                DistributionZoneView view = zonesListChange.get(name);

                if (view == null) {
                    throw new DistributionZoneNotFoundException(name);
                }

                zonesListChange.delete(name);
            }))
                    .whenComplete((res, e) -> {
                        if (e != null) {
                            fut.completeExceptionally(
                                    unwrapDistributionZoneException(
                                            e,
                                            DistributionZoneNotFoundException.class)
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

    /** {@inheritDoc} */
    @Override
    public void start() {
        if (!busyLock.enterBusy()) {
            throw new IgniteException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            zonesConfiguration.distributionZones().listenElements(new ZonesConfigurationListener());

            logicalTopologyService.addEventListener(topologyEventListener);

            registerMetaStorageWatchListener()
                    .thenAccept(ignore -> initDataNodesFromVaultManager())
                    .thenAccept(ignore -> initMetaStorageKeysOnStart());
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
            updateMetaStorageOnZoneCreate(ctx.newValue().zoneId(), ctx.storageRevision());

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            updateMetaStorageOnZoneDelete(ctx.oldValue().zoneId(), ctx.storageRevision());

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onUpdate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            updateMetaStorageOnZoneUpdate(ctx.storageRevision());

            //TODO: Also add here rescheduling for the existing timers https://issues.apache.org/jira/browse/IGNITE-18121

            return completedFuture(null);
        }
    }

    /**
     * Method updates data nodes value for the specified zone,
     * also sets {@code revision} to the {@link DistributionZonesUtil#zonesChangeTriggerKey()} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private void updateMetaStorageOnZoneCreate(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            // Update data nodes for a zone only if the revision of the event is newer than value in that trigger key,
            // so we do not react on a stale events
            CompoundCondition triggerKeyCondition = triggerKeyCondition(revision);

            // logicalTopology can be updated concurrently by the watch listener.
            Set<String> logicalTopology0 = logicalTopology;

            byte[] logicalTopologyBytes = toBytes(logicalTopology0);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKey(zoneId, revision, logicalTopologyBytes);

            If iif = If.iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

            metaStorageManager.invoke(iif).thenAccept(res -> {
                if (res.getAsBoolean()) {
                    LOG.debug("Update zones' dataNodes value [zoneId = {}, dataNodes = {}", zoneId, logicalTopology0);
                } else {
                    LOG.debug("Failed to update zones' dataNodes value [zoneId = {}]", zoneId);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Sets {@code revision} to the {@link DistributionZonesUtil#zonesChangeTriggerKey()} if it passes the condition.
     *
     * @param revision Revision of an event that has triggered this method.
     */
    private void updateMetaStorageOnZoneUpdate(long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            CompoundCondition triggerKeyCondition = triggerKeyCondition(revision);

            Update triggerKeyUpd = updateTriggerKey(revision);

            If iif = If.iif(triggerKeyCondition, triggerKeyUpd, ops().yield(false));

            metaStorageManager.invoke(iif).thenAccept(res -> {
                if (res.getAsBoolean()) {
                    LOG.debug("Distribution zones' trigger key was updated with the revision [revision = {}]", revision);
                } else {
                    LOG.debug("Failed to update distribution zones' trigger key with the revision [revision = {}]", revision);
                }
            });
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Method deletes data nodes value for the specified zone,
     * also sets {@code revision} to the {@link DistributionZonesUtil#zonesChangeTriggerKey()} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private void updateMetaStorageOnZoneDelete(int zoneId, long revision) {
        if (!busyLock.enterBusy()) {
            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
        }

        try {
            CompoundCondition triggerKeyCondition = triggerKeyCondition(revision);

            Update dataNodesRemoveUpd = deleteDataNodesKeyAndUpdateTriggerKey(zoneId, revision);

            If iif = If.iif(triggerKeyCondition, dataNodesRemoveUpd, ops().yield(false));

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
    private void initMetaStorageKeysOnStart() {
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
                                                logicalTopology = ByteUtils.fromBytes(vaultEntry.value());

                                                zonesConfiguration.distributionZones().value().namedListKeys()
                                                        .forEach(zoneName -> {
                                                            int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId()
                                                                    .value();

                                                            saveDataNodesToMetaStorage(zoneId, vaultEntry.value(), vaultAppliedRevision);
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
        // TODO: Change to "registerExactWatch", see https://issues.apache.org/jira/browse/IGNITE-18397
        return metaStorageManager.registerWatchByPrefix(zonesLogicalTopologyKey(), new WatchListener() {
                    @Override
                    public boolean onUpdate(@NotNull WatchEvent evt) {
                        if (!busyLock.enterBusy()) {
                            throw new IgniteInternalException(NODE_STOPPING_ERR, new NodeStoppingException());
                        }

                        try {
                            assert evt.single();

                            Entry newEntry = evt.entryEvent().newEntry();

                            Set<String> newLogicalTopology = ByteUtils.fromBytes(newEntry.value());

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
                                            saveDataNodesToMetaStorage(
                                                    zoneId, newEntry.value(), newEntry.revision()
                                            );
                                        } else {
                                            if (!addedNodes.isEmpty() && autoAdjustScaleUp != Integer.MAX_VALUE) {
                                                //TODO: IGNITE-18121 Create scale up scheduler with dataNodesAutoAdjustScaleUp timer.
                                                saveDataNodesToMetaStorage(
                                                        zoneId, newEntry.value(), newEntry.revision()
                                                );
                                            }

                                            if (!removedNodes.isEmpty() && autoAdjustScaleDown != Integer.MAX_VALUE) {
                                                //TODO: IGNITE-18132 Create scale down scheduler with dataNodesAutoAdjustScaleDown timer.
                                                saveDataNodesToMetaStorage(
                                                        zoneId, newEntry.value(), newEntry.revision()
                                                );
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
     * Method updates data nodes value for the specified zone,
     * also sets {@code revision} to the {@link DistributionZonesUtil#zonesChangeTriggerKey()} if it passes the condition.
     *
     * @param zoneId Unique id of a zone
     * @param dataNodes Data nodes of a zone
     * @param revision Revision of an event that has triggered this method.
     */
    private void saveDataNodesToMetaStorage(int zoneId, byte[] dataNodes, long revision) {
        Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKey(zoneId, revision, dataNodes);

        var iif = If.iif(triggerKeyCondition(revision), dataNodesAndTriggerKeyUpd, ops().yield(false));

        metaStorageManager.invoke(iif).thenAccept(res -> {
            if (res.getAsBoolean()) {
                LOG.debug("Delete zones' dataNodes key [zoneId = {}", zoneId);
            } else {
                LOG.debug("Failed to delete zones' dataNodes key [zoneId = {}]", zoneId);
            }
        });
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
}
