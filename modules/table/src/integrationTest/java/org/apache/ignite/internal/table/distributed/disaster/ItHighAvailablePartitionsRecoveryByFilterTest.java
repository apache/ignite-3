package org.apache.ignite.internal.table.distributed.disaster;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ignite.internal.app.IgniteImpl;
import org.junit.jupiter.api.RepeatedTest;

public class ItHighAvailablePartitionsRecoveryByFilterTest extends AbstractHighAvailablePartitionsRecoveryTest {
    public static final String EU_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {region.attribute = EU, zone.attribute = global},\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }, \n"
            + "  rest.port: {}\n"
            + "}";

    public static final String EU_ONLY_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {region.attribute = EU},\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }, \n"
            + "  rest.port: {}\n"
            + "}";

    public static final String GLOBAL_NODES_CONFIG = "ignite {\n"
            + "  nodeAttributes.nodeAttributes: {zone.attribute = global},\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder: {\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    },\n"
            + "    membership: {\n"
            + "      membershipSyncInterval: 1000,\n"
            + "      failurePingInterval: 500,\n"
            + "      scaleCube: {\n"
            + "        membershipSuspicionMultiplier: 1,\n"
            + "        failurePingRequestMembers: 1,\n"
            + "        gossipInterval: 10\n"
            + "      },\n"
            + "    }\n"
            + "  },\n"
            + "  clientConnector: { port:{} }, \n"
            + "  rest.port: {}\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Override
    protected String getNodeBootstrapConfigTemplate() {
        return EU_NODES_CONFIG;
    }

    @RepeatedTest(10)
    void test() throws InterruptedException {
        startNode(1, EU_ONLY_NODES_CONFIG);
        startNode(2, EU_ONLY_NODES_CONFIG);
        startNode(3, GLOBAL_NODES_CONFIG);
        startNode(4, GLOBAL_NODES_CONFIG);

        String euFilter = "$[?(@.region == \"EU\")]";

        Set<String> euNodes = List.of(
                        igniteImpl(0), igniteImpl(1), igniteImpl(2)
                )
                .stream()
                .map(n -> n.name())
                .collect(Collectors.toUnmodifiableSet());

        createHaZoneWithTable(euFilter, euNodes);

        IgniteImpl node = igniteImpl(0);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), euNodes);

        assertRecoveryKeyIsEmpty(node);

        stopNodes(1, 2);

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), Set.of(node.name()));

        String globalFilter = "$[?(@.zone == \"global\")]";

        alterZoneSql(globalFilter, HA_ZONE_NAME);

        System.out.println("------------");
        Set<String> aliveGlobalNodes = Stream.of(
                        igniteImpl(0), igniteImpl(3), igniteImpl(4)
                )
                .map(IgniteImpl::name)
                .collect(Collectors.toUnmodifiableSet());

        waitAndAssertStableAssignmentsOfPartitionEqualTo(node, HA_TABLE_NAME, Set.of(0, 1), aliveGlobalNodes);
    }

    private void alterZoneSql(String filter, String zoneName) {
        executeSql(String.format("ALTER ZONE \"%s\" SET \"DATA_NODES_FILTER\" = '%s'", zoneName, filter));
    }
}
