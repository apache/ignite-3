package org.apache.ignite.internal.distributionzones;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_NAME;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.deployWatchesAndUpdateMetaStorageRevision;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.setLogicalTopologyInMetaStorage;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zoneLogicalTopologyPrefix;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyKey;
import static org.apache.ignite.internal.distributionzones.DistributionZonesUtil.zonesLogicalTopologyVersionKey;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willSucceedFast;
import static org.apache.ignite.internal.util.ByteUtils.fromBytes;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.internal.distributionzones.DistributionZoneConfigurationParameters.Builder;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.junit.jupiter.api.Test;

public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    @Test
    void testDataNodesUpdatedAfterScaleDownChanged() throws Exception {
        startZoneManager();

        ConcurrentHashMap<Set<NodeWithAttributes>, Long> topologyRevisions = new ConcurrentHashMap<>();

        metaStorageManager.registerPrefixWatch(zoneLogicalTopologyPrefix(), new WatchListener() {
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

                topologyRevisions.put(newLogicalTopology, revision);

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
            }
        });

        AtomicLong scaleDownRevision = new AtomicLong();

        zonesConfiguration.distributionZones().any().dataNodesAutoAdjustScaleDown().listen(ctx -> {
            scaleDownRevision.set(ctx.storageRevision());

            return completedFuture(null);
        });

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);

        setLogicalTopologyInMetaStorage(twoNodes, 1, metaStorageManager);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevisions.get(twoNodes), zoneId0);

        assertThat(dataNodesFut1, willSucceedFast());

        assertEquals(twoNodes, dataNodesFut1.join());

        Set<LogicalNode> oneNodes = Set.of(NODE_0);

        setLogicalTopologyInMetaStorage(oneNodes, 2, metaStorageManager);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevisions.get(oneNodes), zoneId0);

        assertThat(dataNodesFut2, willSucceedFast());

        assertEquals(twoNodes, dataNodesFut2.join());

        distributionZoneManager.alterZone(DEFAULT_ZONE_NAME, new Builder(DEFAULT_ZONE_NAME)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE).build())
                .get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleDownRevision.get(), zoneId0);

        assertThat(dataNodesFut3, willSucceedFast());

        assertEquals(oneNodes, dataNodesFut3.join());

        System.out.println();
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
}
