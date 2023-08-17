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

package org.apache.ignite.internal.distributionzones.causalitydatanodes;

import static java.lang.Math.max;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.catalog.commands.CatalogUtils.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneVersionedConfigurationKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.DistributionZoneNotFoundException;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;

/**
 * Causality data nodes engine. Contains logic for obtaining zone's data nodes with causality token.
 */
public class CausalityDataNodesEngine {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Meta Storage manager. */
    private final MetaStorageManager msManager;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState;

    /**
     * The map which contains configuration changes which trigger zone's data nodes recalculation.
     * zoneId -> (revision -> zoneConfiguration).
     * TODO IGNITE-20050 Clean up this map.
     */
    private final ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, ZoneConfiguration>> zonesVersionedCfg;

    /** Used to guarantee that the zone will be created before other components use the zone. */
    private final VersionedValue<Void> zonesVv;

    /**
     * The constructor.
     *
     * @param busyLock Busy lock to stop synchronously.
     * @param msManager Meta Storage manager.
     * @param vaultMgr Vault manager.
     * @param zonesState Map with states for distribution zones.
     * @param distributionZoneManager Distribution zones manager.
     */
    public CausalityDataNodesEngine(
            IgniteSpinBusyLock busyLock,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            MetaStorageManager msManager,
            VaultManager vaultMgr,
            Map<Integer, ZoneState> zonesState,
            DistributionZoneManager distributionZoneManager
    ) {
        this.busyLock = busyLock;
        this.msManager = msManager;
        this.vaultMgr = vaultMgr;
        this.zonesState = zonesState;
        this.distributionZoneManager = distributionZoneManager;

        zonesVersionedCfg = new ConcurrentHashMap<>();
        zonesVv = new IncrementalVersionedValue<>(registry);
    }

    /**
     * Gets data nodes of the zone using causality token.
     *
     * <p>Return data nodes or throw the exception:
     * {@link IllegalArgumentException} if causalityToken or zoneId is not valid.
     * {@link DistributionZoneNotFoundException} if the zone with the provided zoneId does not exist.
     *
     * @param causalityToken Causality token.
     * @param zoneId Zone id.
     * @return The future with data nodes for the zoneId.
     */
    public CompletableFuture<Set<String>> dataNodes(long causalityToken, int zoneId) {
        if (causalityToken < 1) {
            throw new IllegalArgumentException("causalityToken must be greater then zero [causalityToken=" + causalityToken + '"');
        }

        if (zoneId < DEFAULT_ZONE_ID) {
            throw new IllegalArgumentException("zoneId cannot be a negative number [zoneId=" + zoneId + '"');
        }

        return inBusyLock(busyLock, () -> {
            try {
                return zonesVv.get(causalityToken);
            } catch (OutdatedTokenException e) {
                // This exception means that the DistributionZoneManager has already processed event with the causalityToken.
                return CompletableFuture.completedFuture(null);
            }
        }).thenApply(ignored -> inBusyLock(busyLock, () -> {
            ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

            // Get the latest configuration and configuration revision for a given causality token
            Map.Entry<Long, ZoneConfiguration> zoneLastCfgEntry = versionedCfg.floorEntry(causalityToken);

            if (zoneLastCfgEntry == null) {
                // It means that the zone does not exist on a given causality token.
                throw new DistributionZoneNotFoundException(zoneId);
            }

            long lastCfgRevision = zoneLastCfgEntry.getKey();

            ZoneConfiguration zoneLastCfg = zoneLastCfgEntry.getValue();

            String filter = zoneLastCfg.getFilter();

            boolean isZoneRemoved = zoneLastCfg.getIsRemoved();

            if (isZoneRemoved) {
                // It means that the zone was removed on a given causality token.
                throw new DistributionZoneNotFoundException(zoneId);
            }

            // Get revisions of the last scale up and scale down event which triggered immediate data nodes recalculation.
            long lastScaleUpRevision = getRevisionsOfLastScaleUpEvent(causalityToken, zoneId);
            long lastScaleDownRevision = getRevisionsOfLastScaleDownEvent(causalityToken, zoneId);

            if (lastCfgRevision == versionedCfg.firstKey()
                    && lastCfgRevision >= lastScaleUpRevision
                    && lastCfgRevision >= lastScaleDownRevision
            ) {
                // It means that the zone was created but the data nodes value had not updated yet.
                // So the data nodes value will be equals to the logical topology on the lastCfgRevision.

                Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), zoneLastCfgEntry.getKey());

                assert topologyEntry.value() != null : "Logical topology must be initialized.";

                Set<NodeWithAttributes> logicalTopology = fromBytes(topologyEntry.value());

                Set<Node> logicalTopologyNodes = logicalTopology.stream().map(n -> n.node()).collect(toSet());

                Set<String> dataNodesNames = filterDataNodes(logicalTopologyNodes, filter,
                        distributionZoneManager.nodesAttributes());

                return dataNodesNames;
            }

            ZoneState zoneState = zonesState.get(zoneId);

            ConcurrentSkipListMap<Long, Augmentation> subAugmentationMap = null;

            // On the data nodes recalculation we write new data nodes to the meta storage then clear the augmentation map.
            // Therefore, first we need to read the augmentation map before it is cleared and then read the last data nodes value
            // from the meta storage.
            // The zoneState can be null if the zone was removed.
            if (zoneState != null) {
                subAugmentationMap = new ConcurrentSkipListMap<>(zoneState.topologyAugmentationMap()
                        .headMap(causalityToken, true));
            }

            // Search the revisions of zoneScaleUpChangeTriggerKey and zoneScaleDownChangeTriggerKey with value greater or equals
            // to expected one.
            long scaleUpDataNodesRevision = searchTriggerKey(lastScaleUpRevision, zoneId, zoneScaleUpChangeTriggerKey(zoneId));
            long scaleDownDataNodesRevision = searchTriggerKey(lastScaleDownRevision, zoneId,
                    zoneScaleDownChangeTriggerKey(zoneId));

            // Choose the highest revision.
            long dataNodesRevision = max(causalityToken, max(scaleUpDataNodesRevision, scaleDownDataNodesRevision));

            // Read data nodes value from the meta storage on dataNodesRevision and associated trigger keys.
            Entry dataNodesEntry = msManager.getLocally(zoneDataNodesKey(zoneId), dataNodesRevision);
            Entry scaleUpChangeTriggerKey = msManager.getLocally(zoneScaleUpChangeTriggerKey(zoneId), dataNodesRevision);
            Entry scaleDownChangeTriggerKey = msManager.getLocally(zoneScaleDownChangeTriggerKey(zoneId), dataNodesRevision);

            if (dataNodesEntry.value() == null) {
                // The zone was removed.
                // In this case it is impossible to find out the data nodes value idempotently.
                return emptySet();
            }

            Set<Node> baseDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesEntry.value()));
            long scaleUpTriggerRevision = bytesToLong(scaleUpChangeTriggerKey.value());
            long scaleDownTriggerRevision = bytesToLong(scaleDownChangeTriggerKey.value());

            Set<Node> finalDataNodes = new HashSet<>(baseDataNodes);

            // If the subAugmentationMap is null then it means that the zone was removed.
            // In this case all nodes from topologyAugmentationMap must be already written to the meta storage.
            if (subAugmentationMap != null) {
                // Update the data nodes set with pending data from augmentation map
                subAugmentationMap.forEach((rev, augmentation) -> {
                    if (augmentation.addition() && rev > scaleUpTriggerRevision && rev <= lastScaleUpRevision) {
                        for (Node node : augmentation.nodes()) {
                            finalDataNodes.add(node);
                        }
                    }

                    if (!augmentation.addition() && rev > scaleDownTriggerRevision && rev <= lastScaleDownRevision) {
                        for (Node node : augmentation.nodes()) {
                            finalDataNodes.remove(node);
                        }
                    }
                });
            }

            // Apply the filter to get the final data nodes set.
            return filterDataNodes(finalDataNodes, filter, distributionZoneManager.nodesAttributes());
        }));
    }

    /**
     * Return revision of the last event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getRevisionsOfLastScaleUpEvent(
            long causalityToken,
            int zoneId
    ) {
        return max(
                getLastScaleUpConfigRevision(causalityToken, zoneId),
                getLastScaleUpTopologyRevisions(causalityToken, zoneId)
        );
    }

    /**
     * Return revision of the last event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getRevisionsOfLastScaleDownEvent(
            long causalityToken,
            int zoneId
    ) {
        return max(
                getLastScaleDownConfigRevision(causalityToken, zoneId),
                getLastScaleDownTopologyRevisions(causalityToken, zoneId)
        );
    }

    /**
     * Return revision of the last configuration change event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getLastScaleUpConfigRevision(
            long causalityToken,
            int zoneId
    ) {
        return getLastConfigRevision(causalityToken, zoneId, true);
    }

    /**
     * Return revision of the last configuration change event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getLastScaleDownConfigRevision(
            long causalityToken,
            int zoneId
    ) {
        return getLastConfigRevision(causalityToken, zoneId, false);
    }

    /**
     * Get revision of the latest configuration change event which trigger immediate scale up or scale down recalculation
     * of the data nodes value.
     */
    private long getLastConfigRevision(
            long causalityToken,
            int zoneId,
            boolean isScaleUp
    ) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        Iterator<Map.Entry<Long, ZoneConfiguration>> reversedIterator = versionedCfg.headMap(causalityToken, true)
                .descendingMap().entrySet().iterator();

        Map.Entry<Long, ZoneConfiguration> entryNewerCfg = null;

        // Iterate over zone configurations from newest to oldest.
        while (reversedIterator.hasNext()) {
            Map.Entry<Long, ZoneConfiguration> entryOlderCfg = reversedIterator.next();

            ZoneConfiguration olderCfg = entryOlderCfg.getValue();

            if (entryNewerCfg != null) {
                ZoneConfiguration newerCfg = entryNewerCfg.getValue();

                if (isScaleUp) {
                    if (isScaleUpConfigRevision(olderCfg, newerCfg)) {
                        return entryNewerCfg.getKey();
                    }
                } else {
                    if (isScaleDownConfigRevision(olderCfg, newerCfg)) {
                        return entryNewerCfg.getKey();
                    }
                }
            }

            entryNewerCfg = entryOlderCfg;
        }

        assert entryNewerCfg != null : "At least one zone configuration must be present .";

        // The case when there is only one configuration in the history. This configuration corresponds to the zone creation.
        return entryNewerCfg.getKey();
    }

    /**
     * Check if newer configuration triggers immediate scale up recalculation of the data nodes value.
     * Return true if an older configuration has not immediate scale up and an newer configuration has immediate scale up
     * or older and newer configuration have different filter.
     */
    private static boolean isScaleUpConfigRevision(ZoneConfiguration olderCfg, ZoneConfiguration newerCfg) {
        return olderCfg.getDataNodesAutoAdjustScaleUp() != newerCfg.getDataNodesAutoAdjustScaleUp()
                    && newerCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE
                || !olderCfg.getFilter().equals(newerCfg.getFilter());
    }

    /**
     * Check if newer configuration triggers immediate scale down recalculation of the data nodes value.
     * Return true if an older configuration has not immediate scale down and an newer configuration has immediate scale down.
     */
    private static boolean isScaleDownConfigRevision(ZoneConfiguration olderCfg, ZoneConfiguration newerCfg) {
        return olderCfg.getDataNodesAutoAdjustScaleDown() != newerCfg.getDataNodesAutoAdjustScaleDown()
                && newerCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;
    }

    /**
     * Return revision of the last topology change event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getLastScaleUpTopologyRevisions(
            long causalityToken,
            int zoneId
    ) {
        return getLastTopologyRevisions(causalityToken, zoneId, true);
    }

    /**
     * Return revision of the last topology change event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getLastScaleDownTopologyRevisions(
            long causalityToken,
            int zoneId
    ) {
        return getLastTopologyRevisions(causalityToken, zoneId, false);
    }

    /**
     * Get revisions of the latest topology event with added nodes and with removed nodes when the zone have
     * immediate scale up and scale down timers.
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @return Revisions.
     */
    private long getLastTopologyRevisions(
            long causalityToken,
            int zoneId,
            boolean isScaleUp
    ) {
        Set<NodeWithAttributes> newerLogicalTopology;

        long newerTopologyRevision;

        Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), causalityToken);

        if (!topologyEntry.empty()) {
            byte[] newerLogicalTopologyBytes = topologyEntry.value();

            newerLogicalTopology = fromBytes(newerLogicalTopologyBytes);

            newerTopologyRevision = topologyEntry.revision();

            while (true) {
                topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), newerTopologyRevision - 1);

                Set<NodeWithAttributes> olderLogicalTopology;

                if (topologyEntry.empty()) {
                    // If older topology is empty then it means that each topology changes were iterated
                    // so use empty set to compare it with the first topology.
                    olderLogicalTopology = emptySet();
                } else {
                    byte[] olderLogicalTopologyBytes = topologyEntry.value();

                    olderLogicalTopology = fromBytes(olderLogicalTopologyBytes);
                }

                Map.Entry<Long, ZoneConfiguration> zoneConfigurationEntry = zonesVersionedCfg.get(zoneId)
                        .floorEntry(newerTopologyRevision);

                if (zoneConfigurationEntry == null) {
                    break;
                }

                ZoneConfiguration zoneCfg = zoneConfigurationEntry.getValue();

                if (isScaleUp) {
                    if (isScaleUpTopologyRevision(olderLogicalTopology, newerLogicalTopology, zoneCfg)) {
                        return newerTopologyRevision;
                    }
                } else {
                    if (isScaleDownTopologyRevision(olderLogicalTopology, newerLogicalTopology, zoneCfg)) {
                        return newerTopologyRevision;
                    }
                }

                newerLogicalTopology = olderLogicalTopology;

                newerTopologyRevision = topologyEntry.revision();

                if (topologyEntry.empty()) {
                    break;
                }
            }
        }

        return 0;
    }

    /**
     * Check if newer topology triggers immediate scale up recalculation of the data nodes value.
     * Return true if newer logical topology has added nodes and a zone configuration has immediate scale up.
     */
    private static boolean isScaleUpTopologyRevision(
            Set<NodeWithAttributes> olderLogicalTopology,
            Set<NodeWithAttributes> newerLogicalTopology,
            ZoneConfiguration zoneCfg
    ) {
        return newerLogicalTopology.stream().anyMatch(node -> !olderLogicalTopology.contains(node))
                && zoneCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE;
    }

    /**
     * Check if newer topology triggers immediate scale down recalculation of the data nodes value.
     * Return true if newer logical topology has removed nodes and a zone configuration has immediate scale down.
     */
    private static boolean isScaleDownTopologyRevision(
            Set<NodeWithAttributes> olderLogicalTopology,
            Set<NodeWithAttributes> newerLogicalTopology,
            ZoneConfiguration zoneCfg
    ) {
        return olderLogicalTopology.stream().anyMatch(node -> !newerLogicalTopology.contains(node))
                && zoneCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;
    }

    /**
     * Search a value of zoneScaleUpChangeTriggerKey/zoneScaleDownChangeTriggerKey which equals or greater than scaleRevision.
     * It iterates over the entries in the local meta storage. If there is an entry with a value equals to or greater than
     * the scaleRevision, then it returns the revision of this entry. If there is no such entry then it returns zero.
     *
     * @param scaleRevision Scale revision.
     * @param zoneId Zone id.
     * @param triggerKey Trigger key.
     * @return Revision.
     */
    private long searchTriggerKey(Long scaleRevision, int zoneId, ByteArray triggerKey) {
        Entry lastEntry = msManager.getLocally(triggerKey, Long.MAX_VALUE);

        long upperRevision = max(lastEntry.revision(), scaleRevision);

        // Gets old entries from storage to check if the expected value was handled before watch listener was registered.
        List<Entry> entryList = msManager.getLocally(triggerKey.bytes(), scaleRevision, upperRevision);

        for (Entry entry : entryList) {
            // scaleRevision is null if the zone was removed.
            if (entry.value() == null) {
                return entry.revision();
            } else {
                long entryScaleRevision = bytesToLong(entry.value());

                if (entryScaleRevision >= scaleRevision) {
                    return entry.revision();
                }
            }
        }

        return 0;
    }

    /**
     * Update zone configuration on a scale up update.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param newScaleUp New scale up value.
     */
    public void causalityOnUpdateScaleUp(long revision, int zoneId, int newScaleUp) {
        updateZoneConfiguration(revision, zoneId, zoneCfg -> zoneCfg.setDataNodesAutoAdjustScaleUp(newScaleUp));
    }

    /**
     * Update zone configuration on a scale down update.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param newScaleDown New scale down value.
     */
    public void causalityOnUpdateScaleDown(long revision, int zoneId, int newScaleDown) {
        updateZoneConfiguration(revision, zoneId, zoneCfg -> zoneCfg.setDataNodesAutoAdjustScaleDown(newScaleDown));
    }

    /**
     * Update zone configuration on a scale down update.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param filter Filter.
     */
    public void onUpdateFilter(long revision, int zoneId, String filter) {
        updateZoneConfiguration(revision, zoneId, zoneCfg -> zoneCfg.setFilter(filter));
    }

    /**
     * Update zone configuration.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     * @param updater Closure to update a configuration.
     */
    private void updateZoneConfiguration(long revision, int zoneId, Consumer<ZoneConfiguration> updater) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg);

        updater.accept(newCfg);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     * Creates or restores zone's versioned configuration from the vault the Vault.
     * We save versioned configuration in the Vault every time we receive event which triggers the data nodes recalculation.
     *
     * @param revision Revision.
     * @param zone Zone's view.
     */
    public void onCreateOrRestoreZoneState(long revision, DistributionZoneView zone) {
        int zoneId = zone.zoneId();

        VaultEntry versionedCfgEntry = vaultMgr.get(zoneVersionedConfigurationKey(zoneId)).join();

        if (versionedCfgEntry == null) {
            ZoneConfiguration zoneConfiguration = new ZoneConfiguration(
                    false,
                    zone.dataNodesAutoAdjustScaleUp(),
                    zone.dataNodesAutoAdjustScaleDown(),
                    zone.filter()
            );

            ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = new ConcurrentSkipListMap<>();

            versionedCfg.put(revision, zoneConfiguration);

            zonesVersionedCfg.put(zoneId, versionedCfg);

            vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();

        } else {
            zonesVersionedCfg.put(zoneId, fromBytes(versionedCfgEntry.value()));
        }
    }

    /**
     * Update zone configuration on a zone dropping.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     */
    public void onDelete(long revision, int zoneId) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setIsRemoved(true);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     * Class stores zone configuration parameters. Changing of these parameters can trigger a data nodes recalculation.
     */
    private static class ZoneConfiguration implements Serializable {
        /** Serial version UID. */
        private static final long serialVersionUID = -7212835078222590440L;

        /**
         * The flag indicates whether this zone has been deleted or not.
         */
        private boolean isRemoved;

        /**
         * Scale up timer value.
         */
        private int dataNodesAutoAdjustScaleUp;

        /**
         * Scale down timer value.
         */
        private int dataNodesAutoAdjustScaleDown;

        /**
         * Filter.
         */
        private String filter;

        /**
         * Constructor.
         *
         * @param isRemoved Is this zone removed.
         * @param dataNodesAutoAdjustScaleUp Data nodes auto adjust scale up timeout.
         * @param dataNodesAutoAdjustScaleDown Data nodes auto adjust scale down timeout.
         * @param filter Data nodes filter.
         */
        ZoneConfiguration(boolean isRemoved, int dataNodesAutoAdjustScaleUp, int dataNodesAutoAdjustScaleDown, String filter) {
            this.isRemoved = isRemoved;
            this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
            this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
            this.filter = filter;
        }

        /**
         * Constructor.
         *
         * @param cfg Zone configuration.
         */
        ZoneConfiguration(ZoneConfiguration cfg) {
            this.isRemoved = cfg.getIsRemoved();
            this.dataNodesAutoAdjustScaleUp = cfg.getDataNodesAutoAdjustScaleUp();
            this.dataNodesAutoAdjustScaleDown = cfg.getDataNodesAutoAdjustScaleDown();
            this.filter = cfg.getFilter();
        }

        boolean getIsRemoved() {
            return isRemoved;
        }

        ZoneConfiguration setIsRemoved(boolean isRemoved) {
            this.isRemoved = isRemoved;

            return this;
        }

        int getDataNodesAutoAdjustScaleUp() {
            return dataNodesAutoAdjustScaleUp;
        }

        ZoneConfiguration setDataNodesAutoAdjustScaleUp(int dataNodesAutoAdjustScaleUp) {
            this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;

            return this;
        }

        int getDataNodesAutoAdjustScaleDown() {
            return dataNodesAutoAdjustScaleDown;
        }

        ZoneConfiguration setDataNodesAutoAdjustScaleDown(int dataNodesAutoAdjustScaleDown) {
            this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;

            return this;
        }

        String getFilter() {
            return filter;
        }

        ZoneConfiguration setFilter(String filter) {
            this.filter = filter;

            return this;
        }
    }
}
