package org.apache.ignite.internal.distributionzones;

import static org.apache.ignite.internal.distributionzones.DistributionZoneManager.IMMEDIATE_TIMER_VALUE;
import static org.apache.ignite.internal.distributionzones.DistributionZonesTestUtil.mockVaultZonesLogicalTopologyKey;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.cluster.management.topology.api.LogicalNode;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.NetworkAddress;
import org.junit.jupiter.api.Test;

public class DistributionZoneManagerFilterTest extends BaseDistributionZoneManagerTest {
    private static final String ZONE_NAME = "zone1";

    private static final LogicalNode A = new LogicalNode(
            new ClusterNode("1", "A", new NetworkAddress("localhost", 123)),
            Map.of("region", "US", "storage", "SSD", "dataRegionSize", "10")
    );

    private static final LogicalNode B = new LogicalNode(
            new ClusterNode("2", "B", new NetworkAddress("localhost", 123)),
            Map.of("region", "EU", "storage", "HHD", "dataRegionSize", "30")
    );

    private static final LogicalNode C = new LogicalNode(
            new ClusterNode("3", "C", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    private static final LogicalNode D = new LogicalNode(
            new ClusterNode("4", "D", new NetworkAddress("localhost", 123)),
            Map.of("region", "CN", "storage", "SSD", "dataRegionSize", "20")
    );

    @Test
    void testFilterOnStart() throws Exception {
        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        Set<LogicalNode> clusterNodes = Set.of(A, B, C);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .filter(filter)
                        .build()
        ).get(10_000, TimeUnit.MILLISECONDS);

        Set<String> nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);
    }

    @Test
    void testFilterOnScaleUp() throws Exception {
        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        Set<LogicalNode> clusterNodes = Set.of(A, B, C);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .filter(filter)
                        .build()
        ).get(10_000, TimeUnit.MILLISECONDS);

        Set<String> nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);

        topology.putNode(D);

        nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, C, D).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);
    }

    @Test
    void testFilterOnScaleDown() throws Exception {
        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        Set<LogicalNode> clusterNodes = Set.of(A, B, C);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .filter(filter)
                        .build()
        ).get(10_000, TimeUnit.MILLISECONDS);

        Set<String> nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);

        topology.removeNodes(Set.of(C));

        nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);
    }

    @Test
    void testFilterOnScaleUpWithNewAttributesAfterRestart() throws Exception {
        String filter = "$[?(@.storage == 'SSD' || @.region == 'US')]";

        topology.putNode(A);
        topology.putNode(B);
        topology.putNode(C);

        Set<LogicalNode> clusterNodes = Set.of(A, B, C);

        mockVaultZonesLogicalTopologyKey(clusterNodes, vaultMgr);

        startDistributionZoneManager();

        distributionZoneManager.createZone(
                new DistributionZoneConfigurationParameters.Builder(ZONE_NAME)
                        .dataNodesAutoAdjustScaleUp(IMMEDIATE_TIMER_VALUE)
                        .dataNodesAutoAdjustScaleDown(IMMEDIATE_TIMER_VALUE)
                        .filter(filter)
                        .build()
        ).get(10_000, TimeUnit.MILLISECONDS);

        Set<String> nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);

        topology.removeNodes(Set.of(B));

        LogicalNode newB = new LogicalNode(
                new ClusterNode("2", "newB", new NetworkAddress("localhost", 123)),
                Map.of("region", "US", "storage", "HHD", "dataRegionSize", "30")
        );

        topology.putNode(newB);

        nodes = distributionZoneManager.topologyVersionedDataNodes(
                1,
                topology.getLogicalTopology().version()
        ).get(10_000, TimeUnit.MILLISECONDS);

        assertEquals(Set.of(A, newB, C).stream().map(ClusterNode::name).collect(Collectors.toSet()), nodes);
    }
}
