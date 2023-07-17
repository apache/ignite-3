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
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.Augmentation;
import org.apache.ignite.internal.distributionzones.DistributionZoneManager.ZoneState;
import org.apache.ignite.internal.distributionzones.DistributionZonesUtil;
import org.apache.ignite.internal.distributionzones.Node;
import org.apache.ignite.internal.distributionzones.NodeWithAttributes;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZonesConfiguration;
import org.apache.ignite.internal.distributionzones.rebalance.DistributionZoneRebalanceEngine;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.MetaStorageManager;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
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

    /** Prevents double stopping of the component. */
    private final AtomicBoolean stopGuard;

    /** Busy lock to stop synchronously. */
    private final IgniteSpinBusyLock busyLock;

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
     * zoneId -> (revision -> zoneConfiguration)
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
     * @param stopGuard
     * @param busyLock
     * @param msManager
     * @param vaultMgr
     * @param zonesState
     * @param nodesAttributes
     */
    public CausalityDataNodesEngine(
            AtomicBoolean stopGuard,
            IgniteSpinBusyLock busyLock,
            MetaStorageManager msManager,
            VaultManager vaultMgr,
            Map<Integer, ZoneState> zonesState,
            Map<String, Map<String, String>> nodesAttributes
    ) {
        this.stopGuard = stopGuard;
        this.busyLock = busyLock;
        this.msManager = msManager;
        this.vaultMgr = vaultMgr;
        this.zonesState = zonesState;
        this.nodesAttributes = nodesAttributes;

        zonesVersionedCfg = new ConcurrentHashMap<>();
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
            throw new DistributionZoneNotFoundException(zoneId);
        }

        long lastCfgRevision = zoneLastCfgEntry.getKey();

        ZoneConfiguration zoneLastCfg = zoneLastCfgEntry.getValue();

        String filter = zoneLastCfg.getFilter();

        boolean isZoneRemoved = zoneLastCfg.getIsRemoved();

        boolean isScaleUpImmediate = zoneLastCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE;;
        boolean isScaleDownImmediate = zoneLastCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;

        // Get the last scaleUp and scaleDown revisions.
        if (isZoneRemoved) {
            throw new DistributionZoneNotFoundException(zoneId);
        }

        IgniteBiTuple<Long, Long> revisions = getRevisionsOfLastScaleUpAndScaleDownEvents(causalityToken,
                zoneId, isScaleUpImmediate, isScaleDownImmediate);

        long lastScaleUpRevision = revisions.get1();
        long lastScaleDownRevision = revisions.get2();

        // At the dataNodesRevision the zone was created but the data nodes value had not updated yet.
        // So the data nodes value will be equals to the logical topology on the dataNodesRevision.
        if (lastCfgRevision == versionedCfg.firstKey() && lastCfgRevision >= lastScaleUpRevision && lastCfgRevision >= lastScaleDownRevision) {
            Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), zoneLastCfgEntry.getKey());

            Set<NodeWithAttributes> logicalTopology = fromBytes(topologyEntry.value());

            Set<Node> logicalTopologyNodes = logicalTopology.stream().map(n -> n.node()).collect(toSet());

            Set<String> dataNodesNames = filterDataNodes(logicalTopologyNodes, filter, nodesAttributes);

            return dataNodesNames;
        }

        LOG.info("+++++++ dataNodes lastScaleUpRevision " + lastScaleUpRevision);
        LOG.info("+++++++ dataNodes lastScaleDownRevision " + lastScaleDownRevision);

        ZoneState zoneState = zonesState.get(zoneId);

        TreeMap<Long, Augmentation> subAugmentationMap = null;

        if (zoneState != null) {
            subAugmentationMap = new TreeMap<>(zoneState.topologyAugmentationMap()
                    .subMap(0L, false, causalityToken, true));
        }

        // Wait if needed when the data nodes value will be updated in the meta storage according to calculated lastScaleUpRevision, lastScaleDownRevision and causalityToken.
        long scaleUpDataNodesRevision = waitTriggerKey(lastScaleUpRevision, zoneId, zoneScaleUpChangeTriggerKey(zoneId));

        long scaleDownDataNodesRevision = waitTriggerKey(lastScaleDownRevision, zoneId, zoneScaleDownChangeTriggerKey(zoneId));

        long dataNodesRevision = max(causalityToken, max(scaleUpDataNodesRevision, scaleDownDataNodesRevision));

        LOG.info("+++++++ dataNodes scaleUpDataNodesRevision " + scaleUpDataNodesRevision);
        LOG.info("+++++++ dataNodes scaleDownDataNodesRevision " + scaleDownDataNodesRevision);

        Entry dataNodesEntry = msManager.getLocally(zoneDataNodesKey(zoneId), dataNodesRevision);
        Entry scaleUpChangeTriggerKey = msManager.getLocally(zoneScaleUpChangeTriggerKey(zoneId), dataNodesRevision);
        Entry scaleDownChangeTriggerKey = msManager.getLocally(zoneScaleDownChangeTriggerKey(zoneId), dataNodesRevision);

        Set<Node> baseDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesEntry.value()));
        long scaleUpTriggerRevision = bytesToLong(scaleUpChangeTriggerKey.value());
        long scaleDownTriggerRevision = bytesToLong(scaleDownChangeTriggerKey.value());

        LOG.info("+++++++ dataNodes scaleUpTriggerRevision " + scaleUpTriggerRevision);
        LOG.info("+++++++ dataNodes scaleDownTriggerRevision " + scaleDownTriggerRevision);

        LOG.info("+++++++ dataNodes baseDataNodes " + baseDataNodes);

        Set<Node> finalDataNodes = new HashSet<>(baseDataNodes);

        // If the zoneState is null then it means that the zone was removed. In this case all nodes from topologyAugmentationMap must be
        // already written to the meta storage.
        if (subAugmentationMap != null) {
            LOG.info("+++++++ dataNodes subAugmentationMap " + subAugmentationMap);

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

        Set<String> result = filterDataNodes(finalDataNodes, filter, nodesAttributes);

        if (++x == 2 && result.size() != 1) {
            System.out.println("ASSERT!!!");
        }

        System.out.println("zxc " + x);

        return result;
    }

    volatile int x;

    /**
     * These revisions correspond to the last configuration and topology events after which need to wait for the data nodes recalculation.
     * These events are: a zone creation, changing a scale up timer to immediate, changing a scale down timer to immediate, changing a filter, deleting a zone.
     * If the zone has immediate scale up and scale down timer values, then there are additional topology event:
     * topology changes with the adding nodes
     * topology changes with removing nodes.
     *
     * @param causalityToken
     * @param zoneId
     * @param isScaleUpImmediate
     * @param isScaleDownImmediate
     * @return
     */
    private IgniteBiTuple<Long, Long> getRevisionsOfLastScaleUpAndScaleDownEvents(
            long causalityToken,
            int zoneId,
            boolean isScaleUpImmediate,
            boolean isScaleDownImmediate) {
        IgniteBiTuple<Long, Long> scaleUpAndScaleDownConfigRevisions = getLastScaleUpAndScaleDownConfigRevisions(causalityToken, zoneId,
                isScaleUpImmediate, isScaleDownImmediate);

        long lastScaleUpRevision = scaleUpAndScaleDownConfigRevisions.get1();
        long lastScaleDownRevision = scaleUpAndScaleDownConfigRevisions.get2();

//        if (isScaleUpImmediate || isScaleDownImmediate) {
            IgniteBiTuple<Long, Long> scaleUpAndScaleDownTopologyRevisions = getLastScaleUpAndScaleDownTopologyRevisions(causalityToken,
                    zoneId,
                    isScaleUpImmediate, isScaleDownImmediate);

//            if (isScaleUpImmediate) {
                lastScaleUpRevision = max(lastScaleUpRevision, scaleUpAndScaleDownTopologyRevisions.get1());
//            }

//            if (isScaleDownImmediate) {
                lastScaleDownRevision = max(lastScaleDownRevision, scaleUpAndScaleDownTopologyRevisions.get2());
//            }
//        }

        return new IgniteBiTuple<>(lastScaleUpRevision, lastScaleDownRevision);
    }

    private IgniteBiTuple<Long, Long> getLastScaleUpAndScaleDownConfigRevisions(long causalityToken, int zoneId, boolean isScaleUpImmediate1, boolean isScaleDownImmediate1) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        Iterator<Map.Entry<Long, ZoneConfiguration>> reversedIterator = versionedCfg.headMap(causalityToken, true).descendingMap().entrySet()
                .iterator();

        Map.Entry<Long, ZoneConfiguration> entryNewerCfg = null;

        long scaleUpRevision = 0;
        long scaleDownRevision = 0;

        while (reversedIterator.hasNext()) {
            Map.Entry<Long, ZoneConfiguration> entryOlderCfg = reversedIterator.next();

            ZoneConfiguration olderCfg = entryOlderCfg.getValue();

            if (entryNewerCfg != null) {
                boolean isScaleUpImmediate = entryNewerCfg.getValue().getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE;
                boolean isScaleDownImmediate = entryNewerCfg.getValue().getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE;

                ZoneConfiguration newerCfg = entryNewerCfg.getValue();

                if (scaleUpRevision == 0 && olderCfg.getDataNodesAutoAdjustScaleUp() != newerCfg.getDataNodesAutoAdjustScaleUp()
                        && newerCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE
                        && isScaleUpImmediate) {
                    scaleUpRevision = entryNewerCfg.getKey();
                }

                if (scaleDownRevision == 0 && olderCfg.getDataNodesAutoAdjustScaleDown() != newerCfg.getDataNodesAutoAdjustScaleDown()
                        && newerCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE
                        && isScaleDownImmediate) {
                    scaleDownRevision = entryNewerCfg.getKey();
                }

                if (scaleUpRevision == 0 && !olderCfg.getFilter().equals(newerCfg.getFilter())) {
                    scaleUpRevision = entryNewerCfg.getKey();
                }
            }

            if ((scaleUpRevision > 0) && (scaleDownRevision > 0)) {
                break;
            }

            entryNewerCfg = entryOlderCfg;
        }

        // The case when there is only one configuration in the history.
        if (scaleUpRevision == 0 && entryNewerCfg != null) {
            scaleUpRevision = entryNewerCfg.getKey();
        }

        if (scaleDownRevision == 0 && entryNewerCfg != null) {
            scaleDownRevision = entryNewerCfg.getKey();
        }

        return new IgniteBiTuple<>(scaleUpRevision, scaleDownRevision);
    }

    /**
     * Get revisions of the latest topology event with added nodes if scale up timer value is immediate and with removed nodes
     * if scale down timer value is immediate.
     *
     * @param causalityToken
     * @param zoneId
     * @param isScaleUpImmediate
     * @param isScaleDownImmediate
     * @return
     */
    private IgniteBiTuple<Long, Long> getLastScaleUpAndScaleDownTopologyRevisions(long causalityToken, int zoneId, boolean isScaleUpImmediate1, boolean isScaleDownImmediate1) {
        Set<NodeWithAttributes> newerLogicalTopology = null;

        long newerTopologyRevision = 0;

        Entry topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), causalityToken);

        long scaleUpTopologyRevision = 0;
        long scaleDownTopologyRevision = 0;

//        long topologyRevision = 0;


        if (!topologyEntry.empty()) {
            byte[] newerLogicalTopologyBytes = topologyEntry.value();

            newerLogicalTopology = fromBytes(newerLogicalTopologyBytes);

            newerTopologyRevision = topologyEntry.revision();

            while((scaleUpTopologyRevision == 0) || (scaleDownTopologyRevision == 0)) {
                System.out.println("qwer");
                topologyEntry = msManager.getLocally(zonesLogicalTopologyKey(), newerTopologyRevision - 1);

                if (!topologyEntry.empty() && newerTopologyRevision == topologyEntry.revision()) {
                    break;
                }

                Set<NodeWithAttributes> olderLogicalTopology;

                if (topologyEntry.empty()) {
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

                if (scaleUpTopologyRevision == 0 && !addedNodes.isEmpty() && zoneCfg.getDataNodesAutoAdjustScaleUp() == IMMEDIATE_TIMER_VALUE) {
                    scaleUpTopologyRevision = newerTopologyRevision;
                }

                if (scaleDownTopologyRevision == 0 && !removedNodes.isEmpty() && zoneCfg.getDataNodesAutoAdjustScaleDown() == IMMEDIATE_TIMER_VALUE) {
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
     * Wait when a value of zoneScaleUpChangeTriggerKey/zoneScaleDownChangeTriggerKey will be equals or greater than scaleRevision.
     * First it iterates over the entries in the local metastorage. If there is an entry with a value equal to or greater than
     * the scaleRevision, then it returns the revision of that entry. Шf there is no such entry en it waits this entry by
     * the watch listener.
     *
     * @param scaleRevision Scale revision.
     * @param zoneId Zone id.
     * @param triggerKey Trigger key.
     * @return Future with the revision.
     */
    private long waitTriggerKey(Long scaleRevision, int zoneId, ByteArray triggerKey) {
        System.out.println("waitTriggerKey " + scaleRevision + " " + zoneId);

        System.out.println("waitTriggerKey after registerExactWatch");

        // Must first register the watch listener to listen to new entries. Then read entries from scaleRevision to appliedRevision() + 2.
        // In this case, we are guaranteed to get all entries from the start revision.
        long revAfterRegister = msManager.appliedRevision() + 2;

        System.out.println("waitTriggerKey revAfterRegister " + revAfterRegister);

        long upperRevision = max(revAfterRegister, scaleRevision);

        // Gets old entries from storage to check if the expected value was handled before watch listener was registered.
        List<Entry> entryList = msManager.getLocally(triggerKey.bytes(), scaleRevision, upperRevision);

        long revision = 0;

        for (Entry entry : entryList) {
            System.out.println("waitTriggerKey iteration revision " + entry.value());

            // scaleRevision is null if the zone was removed.
            if (scaleRevision == null) {
                if (entry.value() == null) {
                    revision = entry.revision();

                    break;
                }
            } else {
                long entryValue = bytesToLong(entry.value());

                if (entryValue >= scaleRevision) {
                    revision = entry.revision();

                    break;
                }
            }
        }

        return revision;
    }

    /**
     *
     * @param revision
     * @param zoneId
     * @param newScaleUp
     */
    public void causalityOnUpdateScaleUp(long revision, int zoneId, int newScaleUp) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setDataNodesAutoAdjustScaleUp(newScaleUp);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     *
     * @param revision
     * @param zoneId
     * @param newScaleDown
     */
    public void causalityOnUpdateScaleDown(long revision, int zoneId, int newScaleDown) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setDataNodesAutoAdjustScaleDown(newScaleDown);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     *
     * @param revision
     * @param zoneId
     * @param filter
     */
    public void onUpdateFilter(long revision, int zoneId, String filter) {
        ConcurrentSkipListMap<Long, ZoneConfiguration> versionedCfg = zonesVersionedCfg.get(zoneId);

        ZoneConfiguration previousCfg = versionedCfg.floorEntry(revision).getValue();

        ZoneConfiguration newCfg = new ZoneConfiguration(previousCfg).setFilter(filter);

        versionedCfg.put(revision, newCfg);

        vaultMgr.put(zoneVersionedConfigurationKey(zoneId), toBytes(versionedCfg)).join();
    }

    /**
     *
     * @param revision
     * @param zoneId
     * @param zoneCreation
     * @param zone
     */
    public void onCreateOrRestoreZoneState(long revision, int zoneId, boolean zoneCreation, DistributionZoneView zone) {
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
     *
     * @param revision
     * @param zoneId
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
         * Constructor
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
