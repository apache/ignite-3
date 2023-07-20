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
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneVersionedConfigurationKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLong;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.ByteUtils.toBytes;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.vault.VaultEntry;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.DistributionZoneNotFoundException;
import org.apache.ignite.lang.IgniteBiTuple;

/**
 * Causality data nodes manager.
 */
public class CausalityDataNodesEngine {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(DistributionZoneRebalanceEngine.class);

    /** Meta Storage manager. */
    private final MetaStorageManager msManager;

    /** Vault manager. */
    private final VaultManager vaultMgr;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState;

    /**
     * zoneId -> (revision -> zoneConfiguration).
     */
    private final ConcurrentHashMap<Integer, ConcurrentSkipListMap<Long, ZoneConfiguration>> zonesVersionedCfg;

    /**
     * Local mapping of {@code nodeId} -> node's attributes, where {@code nodeId} is a node id, that changes between restarts.
     * This map is updated every time we receive a topology event in a {@code topologyWatchListener}.
     * TODO: https://issues.apache.org/jira/browse/IGNITE-19491 properly clean up this map
     *
     * @see <a href="https://github.com/apache/ignite-3/blob/main/modules/distribution-zones/tech-notes/filters.md">Filter documentation</a>
     */
    private Map<String, Map<String, String>> nodesAttributes;

    /**
     * The constructor.
     *
     * @param msManager msManager.
     * @param vaultMgr vaultMgr.
     * @param zonesState zonesState.
     * @param nodesAttributes nodesAttributes.
     */
    public CausalityDataNodesEngine(
            MetaStorageManager msManager,
            VaultManager vaultMgr,
            Map<Integer, ZoneState> zonesState,
            Map<String, Map<String, String>> nodesAttributes
    ) {
        this.msManager = msManager;
        this.vaultMgr = vaultMgr;
        this.zonesState = zonesState;
        this.nodesAttributes = nodesAttributes;

        zonesVersionedCfg = new ConcurrentHashMap<>();
    }

    /**
     * Gets data nodes of the zone using causality token.
     *
     * <p>Return data nodes or throw the {@link DistributionZoneNotFoundException} if the zone with the provided {@code zoneId}
     * does not exist.
     *
     * @param causalityToken Causality token.
     * @param zoneId Zone id.
     * @return The future which will be completed with data nodes for the zoneId or with exception.
     */
    public Set<String> dataNodes(long causalityToken, int zoneId) {
        LOG.info("+++++++ dataNodes " + causalityToken + " " + zoneId);

        if (causalityToken < 1) {
            throw new IllegalArgumentException("causalityToken must be greater then zero [causalityToken=" + causalityToken + '"');
        }

        if (zoneId < DEFAULT_ZONE_ID) {
            throw new IllegalArgumentException("zoneId cannot be a negative number [zoneId=" + zoneId + '"');
        }

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
        IgniteBiTuple<Long, Long> revisions = getRevisionsOfLastScaleUpAndScaleDownEvents(causalityToken, zoneId);
        long lastScaleUpRevision = revisions.get1();
        long lastScaleDownRevision = revisions.get2();

        if (lastCfgRevision == versionedCfg.firstKey()
                && lastCfgRevision >= lastScaleUpRevision
                && lastCfgRevision >= lastScaleDownRevision
        ) {
            // It means that the zone was created but the data nodes value had not updated yet.
            // So the data nodes value will be equals to the logical topology on the lastCfgRevision.

            Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), zoneLastCfgEntry.getKey());

            Set<NodeWithAttributes> logicalTopology = fromBytes(topologyEntry.value());

            Set<Node> logicalTopologyNodes = logicalTopology.stream().map(n -> n.node()).collect(toSet());

            Set<String> dataNodesNames = filterDataNodes(logicalTopologyNodes, filter, nodesAttributes);

            return dataNodesNames;
        }

        LOG.info("+++++++ dataNodes lastScaleUpRevision " + lastScaleUpRevision);
        LOG.info("+++++++ dataNodes lastScaleDownRevision " + lastScaleDownRevision);

        ZoneState zoneState = zonesState.get(zoneId);

        LOG.info("+++++++ dataNodes zoneState " + zoneState);

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
        long scaleDownDataNodesRevision = searchTriggerKey(lastScaleDownRevision, zoneId, zoneScaleDownChangeTriggerKey(zoneId));

        // Choose the highest revision.
        long dataNodesRevision = max(causalityToken, max(scaleUpDataNodesRevision, scaleDownDataNodesRevision));

        LOG.info("+++++++ dataNodes scaleUpDataNodesRevision " + scaleUpDataNodesRevision);
        LOG.info("+++++++ dataNodes scaleDownDataNodesRevision " + scaleDownDataNodesRevision);

        // Read data nodes value from the meta storage on dataNodesRevision and associated trigger keys.
        Entry dataNodesEntry = msManager.getLocally(zoneDataNodesKey(zoneId), dataNodesRevision);
        Entry scaleUpChangeTriggerKey = msManager.getLocally(zoneScaleUpChangeTriggerKey(zoneId), dataNodesRevision);
        Entry scaleDownChangeTriggerKey = msManager.getLocally(zoneScaleDownChangeTriggerKey(zoneId), dataNodesRevision);

        if (dataNodesEntry.value() == null) {
            LOG.info("+++++++ dataNodes The zone was removed not idempotently");
            // The zone was removed.
            // In this case it is impossible to find out the data nodes value idempotently.
            return emptySet();
        }

        Set<Node> baseDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesEntry.value()));
        long scaleUpTriggerRevision = bytesToLong(scaleUpChangeTriggerKey.value());
        long scaleDownTriggerRevision = bytesToLong(scaleDownChangeTriggerKey.value());

        LOG.info("+++++++ dataNodes scaleUpTriggerRevision " + scaleUpTriggerRevision);
        LOG.info("+++++++ dataNodes scaleDownTriggerRevision " + scaleDownTriggerRevision);

        LOG.info("+++++++ dataNodes baseDataNodes " + baseDataNodes);

        Set<Node> finalDataNodes = new HashSet<>(baseDataNodes);

        LOG.info("+++++++ dataNodes subAugmentationMap " + subAugmentationMap);

        // If the subAugmentationMap is null then it means that the zone was removed. In this case all nodes from topologyAugmentationMap
        // must be already written to the meta storage.
        if (subAugmentationMap != null) {
            // Update the data nodes set with pending data from augmentation map
            subAugmentationMap.forEach((rev, augmentation) -> {
                if (augmentation.addition() && rev > scaleUpTriggerRevision && rev <= lastScaleUpRevision) {
                    for (Node node : augmentation.nodes()) {
                        LOG.info("+++++++ dataNodes finalDataNodes.add " + node);
                        finalDataNodes.add(node);
                    }
                }

                if (!augmentation.addition() && rev > scaleDownTriggerRevision && rev <= lastScaleDownRevision) {
                    for (Node node : augmentation.nodes()) {
                        LOG.info("+++++++ dataNodes finalDataNodes.remove " + node);
                        finalDataNodes.remove(node);
                    }
                }
            });
        }

        // Apply the filter to get the final data nodes set.
        Set<String> result = filterDataNodes(finalDataNodes, filter, nodesAttributes);

        LOG.info("+++++++ dataNodes result " + result);

        return result;
    }

    /**
     * These revisions correspond to the last configuration and topology events after which need to wait for the data nodes recalculation.
     * These events are: a zone creation, changing a scale up timer to immediate, changing a scale down timer to immediate,
     * changing a filter, deleting a zone, topology changes with the adding nodes, topology changes with removing nodes.
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @return Revisions.
     */
    private IgniteBiTuple<Long, Long> getRevisionsOfLastScaleUpAndScaleDownEvents(
            long causalityToken,
            int zoneId) {
        long scaleUpConfigRevision = getLastScaleUpConfigRevision(causalityToken, zoneId);
        long scaleDownConfigRevision = getLastScaleDownConfigRevision(causalityToken, zoneId);

        IgniteBiTuple<Long, Long> scaleUpAndScaleDownTopologyRevisions =
                getLastScaleUpAndScaleDownTopologyRevisions(causalityToken, zoneId);

        long lastScaleUpRevision = max(scaleUpConfigRevision, scaleUpAndScaleDownTopologyRevisions.get1());

        long lastScaleDownRevision = max(scaleDownConfigRevision, scaleUpAndScaleDownTopologyRevisions.get2());

        return new IgniteBiTuple<>(lastScaleUpRevision, lastScaleDownRevision);
    }

    /**
     * Get revision of the latest configuration change event which triggers immediate scale up recalculation of the data nodes value.
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @return Revision.
     */
    private long getLastScaleUpConfigRevision(
            long causalityToken,
            int zoneId
    ) {
        return getLastConfigRevision(causalityToken, zoneId, true);
    }

    /**
     * Get revision of the latest configuration change event which triggers immediate scale down recalculation of the data nodes value.
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @return Revision.
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
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @param isScaleUp isScaleUp.
     * @return Revision.
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
                    if (isScaleUpRevision(olderCfg, newerCfg)) {
                        return entryNewerCfg.getKey();
                    }
                } else {
                    if (isScaleDownRevision(olderCfg, newerCfg)) {
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
     */
    private static boolean isScaleUpRevision(ZoneConfiguration olderCfg, ZoneConfiguration newerCfg) {
        if (olderCfg.getDataNodesAutoAdjustScaleUp() != newerCfg.getDataNodesAutoAdjustScaleUp()
                && newerCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE) {
            return true;
        } else if (!olderCfg.getFilter().equals(newerCfg.getFilter())) {
            return true;
        }

        return false;
    }

    /**
     * Check if newer configuration triggers immediate scale down recalculation of the data nodes value.
     */
    private static boolean isScaleDownRevision(ZoneConfiguration olderCfg, ZoneConfiguration newerCfg) {
        if (olderCfg.getDataNodesAutoAdjustScaleDown() != newerCfg.getDataNodesAutoAdjustScaleDown()
                && newerCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE) {
            return true;
        }

        return false;
    }

    /**
     * Get revisions of the latest topology event with added nodes and with removed nodes when the zone have
     * immediate scale up and scale down timers.
     *
     * @param causalityToken causalityToken.
     * @param zoneId zoneId.
     * @return Revisions.
     */
    private IgniteBiTuple<Long, Long> getLastScaleUpAndScaleDownTopologyRevisions(long causalityToken, int zoneId) {
        Set<NodeWithAttributes> newerLogicalTopology;

        long newerTopologyRevision;

        Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), causalityToken);

        long scaleUpTopologyRevision = 0;
        long scaleDownTopologyRevision = 0;


        if (!topologyEntry.empty()) {
            byte[] newerLogicalTopologyBytes = topologyEntry.value();

            newerLogicalTopology = fromBytes(newerLogicalTopologyBytes);

            newerTopologyRevision = topologyEntry.revision();

            while ((scaleUpTopologyRevision == 0) || (scaleDownTopologyRevision == 0)) {
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

                Set<NodeWithAttributes> finalNewerLogicalTopology = newerLogicalTopology;

                Set<Node> removedNodes =
                        olderLogicalTopology.stream()
                                .filter(node -> !finalNewerLogicalTopology.contains(node))
                                .map(NodeWithAttributes::node)
                                .collect(toSet());

                Set<Node> addedNodes =
                        newerLogicalTopology.stream()
                                .filter(node -> !olderLogicalTopology.contains(node))
                                .map(NodeWithAttributes::node)
                                .collect(toSet());

                Map.Entry<Long, ZoneConfiguration> zoneConfigurationEntry = zonesVersionedCfg.get(zoneId)
                        .floorEntry(newerTopologyRevision);

                if (zoneConfigurationEntry == null) {
                    break;
                }

                ZoneConfiguration zoneCfg = zoneConfigurationEntry.getValue();

                if (scaleUpTopologyRevision == 0
                        && !addedNodes.isEmpty()
                        && zoneCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE) {
                    scaleUpTopologyRevision = newerTopologyRevision;
                }

                if (scaleDownTopologyRevision == 0
                        && !removedNodes.isEmpty()
                        && zoneCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE) {
                    scaleDownTopologyRevision = newerTopologyRevision;
                }

                newerLogicalTopology = olderLogicalTopology;

                newerTopologyRevision = topologyEntry.revision();

                if (topologyEntry.empty()) {
                    break;
                }
            }
        }

        return new IgniteBiTuple<>(scaleUpTopologyRevision, scaleDownTopologyRevision);
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
        System.out.println("searchTriggerKey " + scaleRevision + " " + zoneId);

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
     * causalityOnUpdateScaleUp.
     *
     * @param revision revision.
     * @param zoneId zoneId.
     * @param newScaleUp newScaleUp.
     */
    public void causalityOnUpdateScaleUp(long revision, int zoneId, int newScaleUp) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setDataNodesAutoAdjustScaleUp(newScaleUp);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     * causalityOnUpdateScaleDown.
     *
     * @param revision revision.
     * @param zoneId zoneId.
     * @param newScaleDown newScaleDown.
     */
    public void causalityOnUpdateScaleDown(long revision, int zoneId, int newScaleDown) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setDataNodesAutoAdjustScaleDown(newScaleDown);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     * onUpdateFilter.
     *
     * @param revision revision.
     * @param zoneId zoneId.
     * @param filter filter.
     */
    public void onUpdateFilter(long revision, int zoneId, String filter) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setFilter(filter);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     * onCreateOrRestoreZoneState.
     *
     * @param revision revision.
     * @param zoneCreation zoneCreation.
     * @param zone zone.
     */
    public void onCreateOrRestoreZoneState(long revision, boolean zoneCreation, DistributionZoneView zone) {
        int zoneId = zone.zoneId();

        if (zoneCreation) {
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
            VaultEntry versionedCfgEntry = vaultMgr.get(zoneVersionedConfigurationKey(zoneId)).join();

            if (versionedCfgEntry != null) {
                zonesVersionedCfg.put(zoneId, fromBytes(versionedCfgEntry.value()));
            }
        }
    }

    /**
     * onDelete.
     *
     * @param revision revision.
     * @param zoneId zoneId.
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
    public static class ZoneConfiguration implements Serializable {
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
         * @param isRemoved isRemoved
         * @param dataNodesAutoAdjustScaleUp dataNodesAutoAdjustScaleUp
         * @param dataNodesAutoAdjustScaleDown dataNodesAutoAdjustScaleDown
         * @param filter filter
         */
        public ZoneConfiguration(boolean isRemoved, int dataNodesAutoAdjustScaleUp, int dataNodesAutoAdjustScaleDown, String filter) {
            this.isRemoved = isRemoved;
            this.dataNodesAutoAdjustScaleUp = dataNodesAutoAdjustScaleUp;
            this.dataNodesAutoAdjustScaleDown = dataNodesAutoAdjustScaleDown;
            this.filter = filter;
        }

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
