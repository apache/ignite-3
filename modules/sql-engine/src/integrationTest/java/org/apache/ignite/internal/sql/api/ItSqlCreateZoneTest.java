package org.apache.ignite.internal.sql.api;

import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIMEM_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_AIPERSIST_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_ROCKSDB_PROFILE_NAME;
import static org.apache.ignite.internal.TestDefaultProfilesNames.DEFAULT_TEST_PROFILE_NAME;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.lang.IgniteStringFormatter.format;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.assertThrowsWithCause;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.cluster.management.CmgGroupId;
import org.apache.ignite.internal.metastorage.server.WatchListenerInhibitor;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.sql.SqlException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(60)
public class ItSqlCreateZoneTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_MANE = "test_zone";
    private static final String NOT_EXISTED_PROFILE_NAME = "not-existed-profile";
    private static final String EXTRA_PROFILE_NAME = "extra-profile";
    /** Nodes bootstrap configuration pattern. */
    private static final String NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE = "ignite {\n"
            + "  network: {\n"
            + "    port: {},\n"
            + "    nodeFinder.netClusterNodes: [ {} ]\n"
            + "  },\n"
            + "  storage.profiles: {"
            + "        " + DEFAULT_TEST_PROFILE_NAME + ".engine: test, "
            + "        " + DEFAULT_AIPERSIST_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_AIMEM_PROFILE_NAME + ".engine: aimem, "
            + "        " + EXTRA_PROFILE_NAME + ".engine: aipersist, "
            + "        " + DEFAULT_ROCKSDB_PROFILE_NAME + ".engine: rocksdb"
            + "  },\n"
            + "  clientConnector.port: {},\n"
            + "  rest.port: {},\n"
            + "  failureHandler.dumpThreadsOnFailure: false\n"
            + "}";

    @Override
    protected int initialNodes() {
        return 1;
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnSameNode() {
        assertDoesNotThrow(() -> createZoneQuery(0, "default"));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNode() {
        cluster.startNode(1, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);
        assertDoesNotThrow(() -> createZoneQuery(0, EXTRA_PROFILE_NAME));
    }

    @Test
    void testCreateZoneSucceedWithCorrectStorageProfileOnDifferentNodeWithDistributedLogicalTopologyUpdate() throws InterruptedException {
        // Node 0 is CMG leader and Node 1 is a laggy query executor
        cluster.startNode(1);

        assertTrue(waitForCondition(
                () -> {
                    IgniteImpl node1 = unwrapIgniteImpl(node(1));

                    return node1.logicalTopologyService().localLogicalTopology().nodes().size() == 2;
                },
                10_000
        ));

        cluster.transferLeadershipTo(0, CmgGroupId.INSTANCE);
        cluster.transferLeadershipTo(0, MetastorageGroupId.INSTANCE);

        assertThrowsWithCause(
                () -> createZoneQuery(1, EXTRA_PROFILE_NAME),
                SqlException.class,
                "Storage profile [" + EXTRA_PROFILE_NAME + "] doesn't exist."
        );

        // Node 1 won't see Node 2 joined with extra profile
        WatchListenerInhibitor.metastorageEventsInhibitor(cluster.node(1)).startInhibit();

        cluster.startNode(2, NODE_BOOTSTRAP_CFG_TEMPLATE_WITH_EXTRA_PROFILE);

        assertThrowsWithCause(
                () -> createZoneQuery(1, EXTRA_PROFILE_NAME),
                SqlException.class,
                "Storage profile " + EXTRA_PROFILE_NAME + " doesn't exist in local topology snapshot with profiles"
        );
    }

    @Test
    void testCreateZoneFailedWithoutCorrectStorageProfileInCluster() {
        assertThrowsWithCause(
                () -> createZoneQuery(0, NOT_EXISTED_PROFILE_NAME),
                SqlException.class,
                "Storage profile [" + NOT_EXISTED_PROFILE_NAME + "] doesn't exist."
        );
    }

    private  List<List<Object>> createZoneQuery(int nodeIdx, String storageProfile) {
        return executeSql(nodeIdx, format("CREATE ZONE IF NOT EXISTS {} STORAGE PROFILES ['{}']", ZONE_MANE, storageProfile));
    }
}
