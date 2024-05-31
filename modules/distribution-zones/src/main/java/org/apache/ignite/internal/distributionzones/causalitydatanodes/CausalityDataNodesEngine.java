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
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.filterDataNodes;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleDownChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneScaleUpChangeTriggerKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.util.ByteUtils.bytesToLongKeepingOrder;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.CompletableFutures.nullCompletedFuture;
import static org.apache.ignite.internal.util.IgniteUtils.inBusyLock;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Consumer;
import java.util.function.LongFunction;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.catalog.descriptors.CatalogZoneDescriptor;
import org.apache.ignite.internal.catalog.events.CatalogEventParameters;
import org.apache.ignite.internal.causality.IncrementalVersionedValue;
import org.apache.ignite.internal.causality.OutdatedTokenException;
import org.apache.ignite.internal.causality.VersionedValue;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;

/**
 * Causality data nodes engine. Contains logic for obtaining zone's data nodes with causality token.
 */
public class CausalityDataNodesEngine {
    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

    /** Meta Storage manager. */
    private final MetaStorageManager msManager;

    /** Distribution zones manager. */
    private final DistributionZoneManager distributionZoneManager;

    /** Catalog manager. */
    private final CatalogManager catalogManager;

    /**
     * Map with states for distribution zones. States are needed to track nodes that we want to add or remove from the data nodes,
     * schedule and stop scale up and scale down processes.
     */
    private final Map<Integer, ZoneState> zonesState;

    /**
     * The map which contains zones' create revision. It is updated only if the current node handled zone's create event.
     * TODO IGNITE-20050 Clean up this map.
     */
    private final ConcurrentHashMap<Integer, Long> zonesCreateRevision = new ConcurrentHashMap<>();

    /** Used to guarantee that the synchronous parts of all metastorage listeners for a specified causality token are handled. */
    private final VersionedValue<Void> zonesVv;

    /**
     * The constructor.
     *
     * @param busyLock Busy lock to stop synchronously.
     * @param registry Registry for versioned values.
     * @param msManager Meta Storage manager.
     * @param zonesState Map with states for distribution zones.
     * @param distributionZoneManager Distribution zones manager.
     * @param catalogManager Catalog manager.
     */
    public CausalityDataNodesEngine(
            IgniteSpinBusyLock busyLock,
            Consumer<LongFunction<CompletableFuture<?>>> registry,
            MetaStorageManager msManager,
            Map<Integer, ZoneState> zonesState,
            DistributionZoneManager distributionZoneManager,
            CatalogManager catalogManager
    ) {
        this.busyLock = busyLock;
        this.msManager = msManager;
        this.zonesState = zonesState;
        this.distributionZoneManager = distributionZoneManager;
        this.catalogManager = catalogManager;

        zonesVv = new IncrementalVersionedValue<>(registry);
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
        if (causalityToken < 1) {
            throw new IllegalArgumentException("causalityToken must be greater then zero [causalityToken=" + causalityToken + '"');
        }

        if (catalogVersion < 0) {
            throw new IllegalArgumentException("catalogVersion must be greater or equal to zero [catalogVersion=" + catalogVersion + '"');
        }

        if (zoneId < 0) {
            throw new IllegalArgumentException("zoneId cannot be a negative number [zoneId=" + zoneId + '"');
        }

        return inBusyLock(busyLock, () -> {
            try {
                return zonesVv.get(causalityToken);
            } catch (OutdatedTokenException e) {
                // This exception means that the DistributionZoneManager has already processed event with the causalityToken.
                return nullCompletedFuture();
            }
        }).thenApply(ignored -> inBusyLock(busyLock, () -> {
            CatalogZoneDescriptor zoneDescriptor = catalogManager.catalog(catalogVersion).zone(zoneId);

            if (zoneDescriptor == null) {
                // It means that the zone does not exist on a given causality token or causality token is lower than metastorage recovery
                // revision.
                throw new DistributionZoneNotFoundException(zoneId);
            }

            Long createRevision = zonesCreateRevision.get(zoneId);

            long descLastUpdateRevision = zoneDescriptor.updateToken();

            // Get revisions of the last scale up and scale down event which triggered immediate data nodes recalculation.
            long lastScaleUpRevision = getRevisionsOfLastScaleUpEvent(causalityToken, catalogVersion, zoneId);
            long lastScaleDownRevision = getRevisionsOfLastScaleDownEvent(causalityToken, catalogVersion, zoneId);

            Entry dataNodes = msManager.getLocally(zoneDataNodesKey(zoneId), causalityToken);

            if (createRevision != null && createRevision.equals(descLastUpdateRevision)
                    && descLastUpdateRevision >= lastScaleUpRevision
                    && descLastUpdateRevision >= lastScaleDownRevision
                    && dataNodes.empty()
            ) {
                // It means that the zone was created but the data nodes value had not been updated yet.
                // So the data nodes value will be equals to the logical topology on the descLastUpdateRevision.
                Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), createRevision);

                if (topologyEntry.empty()) {
                    // A special case for a very first start of a node, when a creation of a zone was before the first topology event,
                    // meaning that zonesLogicalTopologyKey() has not been updated yet, safe to return empty set here.
                    return emptySet();
                }

                Set<NodeWithAttributes> logicalTopology = fromBytes(topologyEntry.value());

                Set<Node> logicalTopologyNodes = logicalTopology.stream().map(n -> n.node()).collect(toSet());

                return filterDataNodes(logicalTopologyNodes, zoneDescriptor, distributionZoneManager.nodesAttributes());
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
            long scaleDownDataNodesRevision = searchTriggerKey(lastScaleDownRevision, zoneId, zoneScaleDownChangeTriggerKey(zoneId));

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
            long scaleUpTriggerRevision = bytesToLongKeepingOrder(scaleUpChangeTriggerKey.value());
            long scaleDownTriggerRevision = bytesToLongKeepingOrder(scaleDownChangeTriggerKey.value());

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
            return filterDataNodes(finalDataNodes, zoneDescriptor, distributionZoneManager.nodesAttributes());
        }));
    }

    /**
     * Return revision of the last event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getRevisionsOfLastScaleUpEvent(
            long causalityToken,
            int catalogVersion,
            int zoneId
    ) {
        return max(
                getLastScaleUpConfigRevision(catalogVersion, zoneId),
                getLastScaleUpTopologyRevisions(causalityToken, catalogVersion, zoneId)
        );
    }

    /**
     * Return revision of the last event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getRevisionsOfLastScaleDownEvent(
            long causalityToken,
            int catalogVersion,
            int zoneId
    ) {
        return max(
                getLastScaleDownConfigRevision(catalogVersion, zoneId),
                getLastScaleDownTopologyRevisions(causalityToken, catalogVersion, zoneId)
        );
    }

    /**
     * Return revision of the last configuration change event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getLastScaleUpConfigRevision(
            int catalogVersion,
            int zoneId
    ) {
        return getLastConfigRevision(catalogVersion, zoneId, true);
    }

    /**
     * Return revision of the last configuration change event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getLastScaleDownConfigRevision(
            int catalogVersion,
            int zoneId
    ) {
        return getLastConfigRevision(catalogVersion, zoneId, false);
    }

    /**
     * Get revision of the latest configuration change event which trigger immediate scale up or scale down recalculation
     * of the data nodes value.
     */
    private long getLastConfigRevision(
            int catalogVersion,
            int zoneId,
            boolean isScaleUp
    ) {
        CatalogZoneDescriptor entryNewerCfg = null;

        // Iterate over zone configurations from newest to oldest.
        for (int i = catalogVersion; i >= catalogManager.earliestCatalogVersion(); i--) {
            CatalogZoneDescriptor entryOlderCfg = catalogManager.zone(zoneId, i);

            if (entryOlderCfg == null) {
                break;
            }

            if (entryNewerCfg != null) {
                if (isScaleUp) {
                    if (isScaleUpConfigRevision(entryOlderCfg, entryNewerCfg)) {
                        return entryNewerCfg.updateToken();
                    }
                } else {
                    if (isScaleDownConfigRevision(entryOlderCfg, entryNewerCfg)) {
                        return entryNewerCfg.updateToken();
                    }
                }
            }

            entryNewerCfg = entryOlderCfg;
        }

        assert entryNewerCfg != null : "At least one zone configuration must be present .";

        // The case when there is only one configuration in the history. This configuration corresponds to the zone creation.
        return entryNewerCfg.updateToken();
    }

    /**
     * Check if newer configuration triggers immediate scale up recalculation of the data nodes value.
     * Return true if an older configuration has not immediate scale up and an newer configuration has immediate scale up
     * or older and newer configuration have different filter.
     */
    private static boolean isScaleUpConfigRevision(CatalogZoneDescriptor olderCfg, CatalogZoneDescriptor newerCfg) {
        return olderCfg.dataNodesAutoAdjustScaleUp() != newerCfg.dataNodesAutoAdjustScaleUp()
                    && newerCfg.dataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE
                || !olderCfg.filter().equals(newerCfg.filter());
    }

    /**
     * Check if newer configuration triggers immediate scale down recalculation of the data nodes value.
     * Return true if an older configuration has not immediate scale down and an newer configuration has immediate scale down.
     */
    private static boolean isScaleDownConfigRevision(CatalogZoneDescriptor olderCfg, CatalogZoneDescriptor newerCfg) {
        return olderCfg.dataNodesAutoAdjustScaleDown() != newerCfg.dataNodesAutoAdjustScaleDown()
                && newerCfg.dataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;
    }

    /**
     * Return revision of the last topology change event which triggers immediate scale up recalculation of the data nodes value.
     */
    private long getLastScaleUpTopologyRevisions(
            long causalityToken,
            int catalogVersion,
            int zoneId
    ) {
        return getLastTopologyRevisions(causalityToken, zoneId, catalogVersion, true);
    }

    /**
     * Return revision of the last topology change event which triggers immediate scale down recalculation of the data nodes value.
     */
    private long getLastScaleDownTopologyRevisions(
            long causalityToken,
            int catalogVersion,
            int zoneId
    ) {
        return getLastTopologyRevisions(causalityToken, zoneId, catalogVersion, false);
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
            int catalogVersion,
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

                CatalogZoneDescriptor zoneDescriptor = catalogManager.catalog(catalogVersion).zone(zoneId);

                if (zoneDescriptor == null) {
                    break;
                }

                if (isScaleUp) {
                    if (isScaleUpTopologyRevision(olderLogicalTopology, newerLogicalTopology, zoneDescriptor)) {
                        return newerTopologyRevision;
                    }
                } else {
                    if (isScaleDownTopologyRevision(olderLogicalTopology, newerLogicalTopology, zoneDescriptor)) {
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
            CatalogZoneDescriptor zoneDescriptor
    ) {
        return newerLogicalTopology.stream().anyMatch(node -> !olderLogicalTopology.contains(node))
                && zoneDescriptor.dataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE;
    }

    /**
     * Check if newer topology triggers immediate scale down recalculation of the data nodes value.
     * Return true if newer logical topology has removed nodes and a zone configuration has immediate scale down.
     */
    private static boolean isScaleDownTopologyRevision(
            Set<NodeWithAttributes> olderLogicalTopology,
            Set<NodeWithAttributes> newerLogicalTopology,
            CatalogZoneDescriptor zoneDescriptor
    ) {
        return olderLogicalTopology.stream().anyMatch(node -> !newerLogicalTopology.contains(node))
                && zoneDescriptor.dataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;
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
                long entryScaleRevision = bytesToLongKeepingOrder(entry.value());

                if (entryScaleRevision >= scaleRevision) {
                    return entry.revision();
                }
            }
        }

        return 0;
    }

    /**
     * Saves revision of the creation a zone to a local state.
     *
     * @param revision Revision.
     * @param zone Zone descriptor.
     */
    public void onCreateZoneState(long revision, CatalogZoneDescriptor zone) {
        int zoneId = zone.id();

        zonesCreateRevision.put(zoneId, revision);
    }

    /**
     * Update zone configuration on a zone dropping.
     *
     * @param revision Revision.
     * @param zoneId Zone id.
     */
    public void onDelete(long revision, int zoneId) {
        // https://issues.apache.org/jira/browse/IGNITE-20050
    }
}
