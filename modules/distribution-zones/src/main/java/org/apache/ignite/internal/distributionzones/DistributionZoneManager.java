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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesKeyAndUpdateTriggerKey;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerKeyCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.metastorage.MetaStorageManager.APPLIED_REV;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.deleteDataNodesKeyAndUpdateTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.triggerKeyCondition;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateDataNodesAndTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.updateTriggerKey;
import static org.apache.ignite.internal.metastorage.client.Operations.ops;
import static org.apache.ignite.lang.ErrorGroups.Common.NODE_STOPPING_ERR;
import static org.apache.ignite.lang.ErrorGroups.Common.UNEXPECTED_ERR;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import org.apache.ignite.configuration.NamedListChange;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
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
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.CompoundCondition;
import org.apache.ignite.internal.metastorage.client.Entry;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.Update;
import org.apache.ignite.internal.metastorage.client.WatchEvent;
import org.apache.ignite.internal.metastorage.client.WatchListener;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.metastorage.client.CompoundCondition;
import org.apache.ignite.internal.metastorage.client.If;
import org.apache.ignite.internal.metastorage.client.Update;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.IgniteException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
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

    /* Cluster Management manager. */
    private final ClusterManagementGroupManager cmgManager;

    private final VaultManager vaultMgr;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock = new IgniteSpinBusyLock();

    private Set<String> logicalTopology = Collections.emptySet();

    /**
     * Creates a new distribution zone manager.
     *
     * @param zonesConfiguration Distribution zones configuration.
     * @param metaStorageManager Meta Storage manager.
     * @param cmgManager Cluster management group manager.
     */
    public DistributionZoneManager(
            DistributionZonesConfiguration zonesConfiguration,
            MetaStorageManager metaStorageManager,
            ClusterManagementGroupManager cmgManager,
            VaultManager vaultMgr
    ) {
        this.zonesConfiguration = zonesConfiguration;
        this.metaStorageManager = metaStorageManager;
        this.cmgManager = cmgManager;
        this.vaultMgr = vaultMgr;
    }

    /**
     * Creates a new distribution zone with the given {@code name} asynchronously.
     *
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> createZone(DistributionZoneConfigurationParameters distributionZoneCfg) {
        Objects.requireNonNull(distributionZoneCfg, "Distribution zone configuration is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
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
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
                }
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Alters a distribution zone.
     *
     * @param name Distribution zone name.
     * @param distributionZoneCfg Distribution zone configuration.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> alterZone(String name, DistributionZoneConfigurationParameters distributionZoneCfg) {
        Objects.requireNonNull(name, "Distribution zone name is null.");
        Objects.requireNonNull(distributionZoneCfg, "Distribution zone configuration is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                NamedListChange<DistributionZoneView, DistributionZoneChange> renameChange;

                try {
                    renameChange = zonesListChange.rename(name, distributionZoneCfg.name());
                } catch (IllegalArgumentException e) {
                    throw new DistributionZoneRenameException(name, distributionZoneCfg.name(), e);
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
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
                } catch (Exception e) {
                    throw new IgniteInternalException(UNEXPECTED_ERR, distributionZoneCfg.name(), e);
                }
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /**
     * Drops a distribution zone with the name specified.
     *
     * @param name Distribution zone name.
     * @return Future representing pending completion of the operation.
     */
    public CompletableFuture<Void> dropZone(String name) {
        Objects.requireNonNull(name, "Distribution zone name is null.");

        if (!busyLock.enterBusy()) {
            throw new IgniteException(new NodeStoppingException());
        }

        try {
            return zonesConfiguration.change(zonesChange -> zonesChange.changeDistributionZones(zonesListChange -> {
                DistributionZoneView view = zonesListChange.get(name);

                if (view == null) {
                    throw new DistributionZoneNotFoundException(name);
                }

                zonesListChange.delete(name);
            }));
        } finally {
            busyLock.leaveBusy();
        }
    }

    /** {@inheritDoc} */
    @Override
    public void start() {
        zonesConfiguration.distributionZones().listenElements(new ZonesConfigurationListener());

        long appliedRevision = appliedRevision();

        VaultEntry vaultEntry;

        try {
            vaultEntry = vaultMgr.get(zonesLogicalTopologyKey()).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IgniteInternalException(e);
        }

        if (vaultEntry != null) {
            byte[] newLogicalTopology = vaultEntry.value();

            logicalTopology = ByteUtils.fromBytes(newLogicalTopology);

            zonesConfiguration.distributionZones().value().namedListKeys()
                    .forEach(zoneName -> {
                        int zoneId = zonesConfiguration.distributionZones().get(zoneName).zoneId().value();

                        saveDataNodesToMetastorage(zoneId, newLogicalTopology, appliedRevision);
                    });
        }

        metaStorageManager.registerWatchByPrefix(zonesLogicalTopologyKey(), new WatchListener() {
            @Override
            public boolean onUpdate(@NotNull WatchEvent evt) {
                if (!busyLock.enterBusy()) {
                    throw new IgniteInternalException(new NodeStoppingException());
                }

                try {
                    assert evt.single();

                    Entry newLogicalTopology = evt.entryEvent().newEntry();

                    Set<String> newlogicalTopology = ByteUtils.fromBytes(newLogicalTopology.value());

                    List<String> removedNodes =
                            logicalTopology.stream().filter(node -> !newlogicalTopology.contains(node)).collect(toList());

                    List<String> addedNodes =
                            newlogicalTopology.stream().filter(node -> !logicalTopology.contains(node)).collect(toList());

                    logicalTopology = newlogicalTopology;

                    zonesConfiguration.distributionZones().value().namedListKeys()
                            .forEach(zoneName -> {
                                DistributionZoneConfiguration zoneCfg = zonesConfiguration.distributionZones().get(zoneName);

                                int autoAdjust = zoneCfg.dataNodesAutoAdjust().value();
                                int scaleDown = zoneCfg.dataNodesAutoAdjustScaleDown().value();
                                int scaleUp = zoneCfg.dataNodesAutoAdjustScaleUp().value();

                                Integer zoneId = zoneCfg.zoneId().value();

                                if (!removedNodes.isEmpty()) {
                                    if (autoAdjust != Integer.MAX_VALUE) {
                                        saveDataNodesToMetastorage(
                                                zoneId, newLogicalTopology.value(), newLogicalTopology.revision()
                                        );
                                    } else if (scaleDown != Integer.MAX_VALUE) {
                                        saveDataNodesToMetastorage(
                                                zoneId, newLogicalTopology.value(), newLogicalTopology.revision()
                                        );
                                    }
                                }

                                if (!addedNodes.isEmpty()) {
                                    if (autoAdjust != Integer.MAX_VALUE) {
                                        saveDataNodesToMetastorage(
                                                zoneId, newLogicalTopology.value(), newLogicalTopology.revision()
                                        );
                                    } else if (scaleUp != Integer.MAX_VALUE) {
                                        saveDataNodesToMetastorage(
                                                zoneId, newLogicalTopology.value(), newLogicalTopology.revision()
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
        });
    }

    /** {@inheritDoc} */
    @Override
    public void stop() throws Exception {

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
            Set<ClusterNode> clusterNodes;

            //TODO temporary code, will be removed in https://issues.apache.org/jira/browse/IGNITE-18087
            try {
                clusterNodes = cmgManager.logicalTopology().get().nodes();
            } catch (InterruptedException | ExecutionException e) {
                throw new IgniteInternalException(e);
            }

            // Update data nodes for a zone only if the revision of the event is newer than value in that trigger key,
            // so we do not react on a stale events
            CompoundCondition triggerKeyCondition = triggerKeyCondition(revision);

            Set<String> nodesConsistentIds = clusterNodes.stream().map(ClusterNode::name).collect(Collectors.toSet());

            byte[] logicalTopologyBytes = ByteUtils.toBytes(nodesConsistentIds);

            Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKey(zoneId, revision, logicalTopologyBytes);

            If iif = If.iif(triggerKeyCondition, dataNodesAndTriggerKeyUpd, ops().yield(false));

            metaStorageManager.invoke(iif).thenAccept(res -> {
                if (res.getAsBoolean()) {
                    LOG.debug("Update zones' dataNodes value [zoneId = {}, dataNodes = {}", zoneId, nodesConsistentIds);
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
     * Method deleted data nodes value for the specified zone,
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
     * Returns applied revision for {@link VaultManager#putAll} operation.
     */
    public long appliedRevision() {
        try {
            return vaultMgr.get(APPLIED_REV)
                    .thenApply(appliedRevision -> appliedRevision == null ? 0L : bytesToLong(appliedRevision.value()))
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IgniteInternalException(e);
        }
    }

    private void saveDataNodesToMetastorage(int zoneId, byte[] newLogicalTopology, long revision) {
        Update dataNodesAndTriggerKeyUpd = updateDataNodesAndTriggerKey(zoneId, revision, newLogicalTopology);

        var iif = If.iif(triggerKeyCondition(revision), dataNodesAndTriggerKeyUpd, ops().yield(false));

        metaStorageManager.invoke(iif).thenAccept(res -> {
            if (res.getAsBoolean()) {
                LOG.info("");
            } else {
                LOG.info("");
            }
        });
    }
}
