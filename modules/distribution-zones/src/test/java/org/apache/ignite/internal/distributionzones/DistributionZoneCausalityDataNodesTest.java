package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.setLogicalTopologyInMetaStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.extractZoneId;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneDataNodesKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesDataNodesPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.apache.ignite.internal.util.IgniteUtils.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.configuration.notifications.ConfigurationListener;
import org.apache.ignite.configuration.notifications.ConfigurationNamedListListener;
import org.apache.ignite.configuration.notifications.ConfigurationNotificationEvent;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.distributionzones.configuration.DistributionZoneView;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.lang.IgniteBiTuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<IgniteBiTuple<Integer, Set<String>>, CompletableFuture<Long>> zoneDataNodesRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<IgniteBiTuple<Integer, Integer>, CompletableFuture<Long>> zoneScaleUpRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> createZoneRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> dropZoneRevisions = new ConcurrentHashMap<>();

    @BeforeEach
    void beforeEach() {
        metaStorageManager.registerPrefixWatch(zoneLogicalTopologyPrefix(), createMetastorageTopologyListener());

        metaStorageManager.registerPrefixWatch(zonesDataNodesPrefix(), createMetastorageDataNodesListener());

        ZonesConfigurationListener zonesConfigurationListener = new ZonesConfigurationListener();

        zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
        zonesConfiguration.distributionZones().listenElements(zonesConfigurationListener);
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());

        zonesConfiguration.defaultDistributionZone().listen(zonesConfigurationListener);
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleUp().listen(onUpdateScaleUp());
        zonesConfiguration.defaultDistributionZone().dataNodesAutoAdjustScaleDown().listen(onUpdateScaleDown());
    }

    @Test
    void testTopologyLeapUpdate() throws Exception {
        // Prerequisite
        startZoneManager();

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        int zoneId1 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_1)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        CompletableFuture<Long> dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId1, twoNodes1);

        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes1, 1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        long dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId1);

        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps

        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId1, twoNodes2);

        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes2, 2);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision2, zoneId1);

        assertThat(dataNodesFut3, willBe(twoNodesNames2));
        assertThat(dataNodesFut4, willBe(twoNodesNames1));

        dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId1);

        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    @Test
    void testDataNodesUpdatedAfterScaleDownChanged() throws Exception {
        // Prerequisite

        startZoneManager();

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps

        Set<LogicalNode> oneNodes = Set.of(NODE_0);
        Set<String> oneNodesNames = Set.of(NODE_0.name());

        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(oneNodes, 2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        assertThat(dataNodesFut2, willBe(twoNodesNames));

        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME_0, IMMEDIATE_TIMER_VALUE);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleDownRevision, zoneId0);

        assertThat(dataNodesFut3, willBe(oneNodesNames));
    }

    private void startZoneManager() throws Exception {
        deployWatchesAndUpdateMetaStorageRevision(metaStorageManager);

        distributionZoneManager.start();

        distributionZoneManager.alterZone(
                        DEFAULT_ZONE_NAME, new DistributionZoneConfigurationParameters.Builder(DEFAULT_ZONE_NAME)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);
    }

    private long setLogicalTopologyInMetaStorageAndGetRevision(Set<LogicalNode> nodes, long topVer) throws Exception {
        Set<String> nodeNames = nodes.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        setLogicalTopologyInMetaStorage(nodes, topVer, metaStorageManager);

        return revisionFut.get(3, SECONDS);
    }

    private long alterZoneScaleDownAndGetRevision(String zoneName, int scaleDown) throws Exception{
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleDownRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleDown(scaleDown).build())
                .get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    private CompletableFuture<Long> getZoneDataNodesRevision(int zoneId, Set<LogicalNode> nodes) {
        Set<String> nodeNames = nodes.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        return zoneDataNodesRevisions.put(new IgniteBiTuple<>(zoneId, nodeNames), revisionFut);
    }

    /**
     * Creates configuration listener for updates of scale up value.
     *
     * @return Configuration listener for updates of scale up value.
     */
    private ConfigurationListener<Integer> onUpdateScaleUp() {
        return ctx -> {
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            int newScaleUp = ctx.newValue().intValue();

            zoneScaleUpRevisions.remove(new IgniteBiTuple<>(zoneId, newScaleUp)).complete(ctx.storageRevision());

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
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            zoneScaleDownRevisions.remove(zoneId).complete(ctx.storageRevision());

            return completedFuture(null);
        };
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            createZoneRevisions.remove(zoneId).complete(ctx.storageRevision());

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            dropZoneRevisions.remove(zoneId).complete(ctx.storageRevision());

            return completedFuture(null);
        }
    }

    private WatchListener createMetastorageTopologyListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                Set<NodeWithAttributes> newLogicalTopology = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (Arrays.equals(e.key(), zonesLogicalTopologyVersionKey().bytes())) {
                        revision = e.revision();
                    } else if (Arrays.equals(e.key(), zonesLogicalTopologyKey().bytes())) {
                        newLogicalTopology = fromBytes(e.value());
                    }
                }

                Set<String> nodeNames = newLogicalTopology.stream().map(node -> node.nodeName()).collect(toSet());

                topologyRevisions.remove(nodeNames).complete(revision);

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        };
    }

    private WatchListener createMetastorageDataNodesListener() {
        return new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent evt) {

                int zoneId = 0;

                Set<Node> newDataNodes = null;

                long revision = 0;

                for (EntryEvent event : evt.entryEvents()) {
                    Entry e = event.newEntry();

                    if (startsWith(e.key(), zoneDataNodesKey().bytes())) {
                        revision = e.revision();

                        zoneId = extractZoneId(e.key());

                        byte[] dataNodesBytes = e.value();

                        if (dataNodesBytes != null) {
                            newDataNodes = DistributionZonesUtil.dataNodes(fromBytes(dataNodesBytes));
                        } else {
                            newDataNodes = emptySet();
                        }
                    }
                }

                Set<String> nodeNames = newDataNodes.stream().map(node -> node.nodeName()).collect(toSet());

                zoneDataNodesRevisions.remove(new IgniteBiTuple<>(zoneId, nodeNames)).complete(revision);

                return completedFuture(null);
            }

            @Override
            public void onError (Throwable e){
            }
        };
    }
}
