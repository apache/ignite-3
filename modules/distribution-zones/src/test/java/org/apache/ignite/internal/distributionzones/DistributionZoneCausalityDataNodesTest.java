package org.apache.ignite.internal.distributionzones;

import static java.util.Collections.emptySet;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.DEFAULT_ZONE_ID;
import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
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
import static org.junit.jupiter.api.Assertions.assertThrows;

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
import org.apache.ignite.internal.distributionzones.exception.DistributionZoneNotFoundException;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.NodeStoppingException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DistributionZoneCausalityDataNodesTest extends BaseDistributionZoneManagerTest {
    private ConcurrentHashMap<Set<String>, CompletableFuture<Long>> topologyRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<IgniteBiTuple<Integer, Set<String>>, CompletableFuture<Long>> zoneDataNodesRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleUpRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> zoneScaleDownRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> createZoneRevisions = new ConcurrentHashMap<>();

    private ConcurrentHashMap<Integer, CompletableFuture<Long>> dropZoneRevisions = new ConcurrentHashMap<>();

    @BeforeEach
    void beforeEach() throws NodeStoppingException {
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

        distributionZoneManager.start();

        metaStorageManager.deployWatches();
    }

    /**
     * Test2
     *
     * @throws Exception
     */
    @Test
    void testTopologyLeapUpdate() throws Exception {
        // Prerequisite
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

        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes1 = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames1 = Set.of(NODE_0.name(), NODE_1.name());

        CompletableFuture<Long> dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId1, twoNodes1);

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes1, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes1);

        CompletableFuture<Set<String>> dataNodesFut0 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut0, willBe(twoNodesNames1));

        long dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId1);

        assertThat(dataNodesFut1, willBe(twoNodesNames1));

        // Test steps

        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId1, twoNodes2);

        // TODO: Replace by topology leap update
        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes2, 2);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);
        CompletableFuture<Set<String>> dataNodesFut4 = distributionZoneManager.dataNodes(topologyRevision2, zoneId1);

        assertThat(dataNodesFut3, willBe(twoNodesNames2));
        assertThat(dataNodesFut4, willBe(twoNodesNames1));

        dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut5 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId1);

        assertThat(dataNodesFut5, willBe(twoNodesNames2));
    }

    /**
     * Test3
     *
     * @throws Exception
     */
    @Test
    void testTopologyLeapUpdateScaleDownImmediate() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(1)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps

        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId0, twoNodes2);

        // TODO: Replace by topology leap update
        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes2, 2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        Set<LogicalNode> threeNodes = Set.of(NODE_0, NODE_1, NODE_2);
        Set<String> threeNodesNames = Set.of(NODE_0.name(), NODE_1.name(), NODE_2.name());

        assertThat(dataNodesFut2, willBe(threeNodesNames));

        long dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId0);

        assertThat(dataNodesFut3, willBe(twoNodesNames2));


    }

    /**
     * Test4
     *
     * @throws Exception
     */
    @Test
    void testTopologyLeapUpdateScaleUpImmediate() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(1)
                                .build()
                )
                .get(3, SECONDS);

        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps

        Set<LogicalNode> twoNodes2 = Set.of(NODE_0, NODE_2);
        Set<String> twoNodesNames2 = Set.of(NODE_0.name(), NODE_2.name());

        CompletableFuture<Long> dataNodesRevisionFutZone = getZoneDataNodesRevision(zoneId0, twoNodes2);

        // TODO: Replace by topology leap update
        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes2, 2);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        Set<LogicalNode> oneNodes = Set.of(NODE_0);
        Set<String> oneNodesNames = Set.of(NODE_0.name());

        assertThat(dataNodesFut2, willBe(oneNodesNames));

        long dataNodesRevisionZone = dataNodesRevisionFutZone.get(3, SECONDS);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dataNodesRevisionZone, zoneId0);

        assertThat(dataNodesFut3, willBe(twoNodesNames2));


    }

    /**
     * Test5
     *
     * @throws Exception
     */
    @Test
    void testDataNodesUpdatedAfterScaleUpChanged() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNodes1 = Set.of(NODE_0);
        Set<String> oneNodesNames1 = Set.of(NODE_0.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, oneNodes1);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(oneNodesNames1));

        // Test steps

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(oneNodes, 2);
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        assertThat(dataNodesFut2, willBe(oneNodes1));

        long scaleUpRevision = alterZoneScaleUpAndGetRevision(ZONE_NAME_0, IMMEDIATE_TIMER_VALUE);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleUpRevision, zoneId0);

        assertThat(dataNodesFut3, willBe(twoNodesNames));
    }

    /**
     * Test6
     *
     * @throws Exception
     */
    @Test
    void testDataNodesUpdatedAfterScaleDownChanged() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps

        Set<LogicalNode> oneNodes = Set.of(NODE_0);
        Set<String> oneNodesNames = Set.of(NODE_0.name());

//        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(oneNodes, 2);
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNodes);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        assertThat(dataNodesFut2, willBe(twoNodesNames));

        long scaleDownRevision = alterZoneScaleDownAndGetRevision(ZONE_NAME_0, IMMEDIATE_TIMER_VALUE);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(scaleDownRevision, zoneId0);

        assertThat(dataNodesFut3, willBe(oneNodesNames));
    }

    /**
     * Test7
     *
     * @throws Exception
     */
    @Test
    void testDropZoneScaleDownImmediate() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(10000)
                                .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                                .build()
                )
                .get(3, SECONDS);

        Set<LogicalNode> oneNodes1 = Set.of(NODE_0);
        Set<String> oneNodesNames1 = Set.of(NODE_0.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, oneNodes1);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(oneNodesNames1));

        // Test steps

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(oneNodes, 2);
        long topologyRevision2 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        assertThat(dataNodesFut2, willBe(oneNodes1));

        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME_0);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dropZoneRevision, zoneId0);

        assertThrows(DistributionZoneNotFoundException.class, () -> {
            dataNodesFut3.get(3, SECONDS);
        });
    }

    /**
     * Test8
     *
     * @throws Exception
     */
    @Test
    void testDropZoneScaleUpImmediate() throws Exception {
        // Prerequisite

        int zoneId0 = distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(ZONE_NAME_0)
                                .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                                .dataNodesAutoAdjustScaleDown(10000)
                                .build()
                )
                .get(3, SECONDS);

        topology.putNode(NODE_0);

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

//        long topologyRevision1 = setLogicalTopologyInMetaStorageAndGetRevision(twoNodes, 1);

        long topologyRevision1 = putNodeInLogicalTopologyAndGetRevision(NODE_1, twoNodes);

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(topologyRevision1, zoneId0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        // Test steps

        Set<LogicalNode> oneNodes = Set.of(NODE_0);
        Set<String> oneNodesNames = Set.of(NODE_0.name());

//        long topologyRevision2 = setLogicalTopologyInMetaStorageAndGetRevision(oneNodes, 2);
        long topologyRevision2 = removeNodeInLogicalTopologyAndGetRevision(Set.of(NODE_1), oneNodes);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(topologyRevision2, zoneId0);

        assertThat(dataNodesFut2, willBe(twoNodesNames));

        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME_0);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(dropZoneRevision, zoneId0);

        assertThrows(DistributionZoneNotFoundException.class, () -> {
            dataNodesFut3.get(3, SECONDS);
        });
    }

    /**
     * Test9
     *
     * @throws Exception
     */
    @Test
    void testCreateThenDropZone() throws Exception {
        // Prerequisite

        topology.putNode(NODE_0);
        topology.putNode(NODE_1);

        // Test steps

        Set<LogicalNode> twoNodes = Set.of(NODE_0, NODE_1);
        Set<String> twoNodesNames = Set.of(NODE_0.name(), NODE_1.name());

        long createZoneRevision = createZoneAndGetRevision(ZONE_NAME_0, ZONE_ID_0, IMMEDIATE_TIMER_VALUE, 10000);

        CompletableFuture<Set<String>> dataNodesFut3 = distributionZoneManager.dataNodes(createZoneRevision - 1, ZONE_ID_0);

        assertThrows(DistributionZoneNotFoundException.class, () -> {
            dataNodesFut3.get(3, SECONDS);
        });

        CompletableFuture<Set<String>> dataNodesFut1 = distributionZoneManager.dataNodes(createZoneRevision, ZONE_ID_0);

        assertThat(dataNodesFut1, willBe(twoNodesNames));

        long dropZoneRevision = dropZoneAndGetRevision(ZONE_NAME_0);

        CompletableFuture<Set<String>> dataNodesFut2 = distributionZoneManager.dataNodes(dropZoneRevision, ZONE_ID_0);

        assertThrows(DistributionZoneNotFoundException.class, () -> {
            dataNodesFut2.get(3, SECONDS);
        });
    }

    /**
     * Test10
     *
     * @throws Exception
     */
    @Test
    void testInvalidCausalityToken() {
        assertThrows(AssertionError.class, () -> distributionZoneManager.dataNodes(-1, DEFAULT_ZONE_ID));
        assertThrows(AssertionError.class, () -> distributionZoneManager.dataNodes(0, DEFAULT_ZONE_ID));
    }

    private long setLogicalTopologyInMetaStorageAndGetRevision(Set<LogicalNode> nodes, long topVer) throws Exception {
        Set<String> nodeNames = nodes.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        setLogicalTopologyInMetaStorage(nodes, topVer, metaStorageManager);

        return revisionFut.get(3, SECONDS);
    }

    private long putNodeInLogicalTopologyAndGetRevision(
            LogicalNode nodeToAdd,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.putNode(nodeToAdd);

        return revisionFut.get(3, SECONDS);
    }

    private long removeNodeInLogicalTopologyAndGetRevision(
            Set<LogicalNode> nodesToRemove,
            Set<LogicalNode> expectedTopology
    ) throws Exception {
        Set<String> nodeNames = expectedTopology.stream().map(node -> node.name()).collect(toSet());

        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        topologyRevisions.put(nodeNames, revisionFut);

        topology.removeNodes(nodesToRemove);

        return revisionFut.get(3, SECONDS);
    }

    private long alterZoneScaleUpAndGetRevision(String zoneName, int scaleUp) throws Exception{
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        zoneScaleUpRevisions.put(zoneId, revisionFut);

        distributionZoneManager.alterZone(zoneName, new Builder(zoneName)
                        .dataNodesAutoAdjustScaleUp(scaleUp).build())
                .get(3, SECONDS);

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

    private long dropZoneAndGetRevision(String zoneName) throws Exception{
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        int zoneId = distributionZoneManager.getZoneId(zoneName);

        dropZoneRevisions.put(zoneId, revisionFut);

        distributionZoneManager.dropZone(zoneName).get(3, SECONDS);

        return revisionFut.get(3, SECONDS);
    }

    private long createZoneAndGetRevision(String zoneName, int zoneId, int scaleUp, int scaleDown) throws Exception{
        CompletableFuture<Long> revisionFut = new CompletableFuture<>();

        createZoneRevisions.put(zoneId, revisionFut);

        distributionZoneManager.createZone(
                        new DistributionZoneConfigurationParameters.Builder(zoneName)
                                .dataNodesAutoAdjustScaleUp(scaleUp)
                                .dataNodesAutoAdjustScaleDown(scaleDown)
                                .build()
                )
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

            if (zoneScaleUpRevisions.containsKey(zoneId)) {
                zoneScaleUpRevisions.remove(zoneId).complete(ctx.storageRevision());
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
            int zoneId = ctx.newValue(DistributionZoneView.class).zoneId();

            if (zoneScaleDownRevisions.containsKey(zoneId)) {
                zoneScaleDownRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        };
    }

    private class ZonesConfigurationListener implements ConfigurationNamedListListener<DistributionZoneView> {
        @Override
        public CompletableFuture<?> onCreate(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.newValue().zoneId();

            if (createZoneRevisions.containsKey(zoneId)) {
                createZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

            return completedFuture(null);
        }

        @Override
        public CompletableFuture<?> onDelete(ConfigurationNotificationEvent<DistributionZoneView> ctx) {
            int zoneId = ctx.oldValue().zoneId();

            if (dropZoneRevisions.containsKey(zoneId)) {
                dropZoneRevisions.remove(zoneId).complete(ctx.storageRevision());
            }

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

                if (topologyRevisions.containsKey(nodeNames)) {
                    topologyRevisions.remove(nodeNames).complete(revision);
                }

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

                IgniteBiTuple<Integer, Set<String>> zoneDataNodesKey = new IgniteBiTuple<>(zoneId, nodeNames);

                if (zoneDataNodesRevisions.containsKey(zoneDataNodesKey)) {
                    zoneDataNodesRevisions.remove(new IgniteBiTuple<>(zoneId, nodeNames)).complete(revision);
                }

                return completedFuture(null);
            }

            @Override
            public void onError (Throwable e){
            }
        };
    }
}
