package org.apache.ignite.internal.distributionzones;

import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.Cluster.NodeKnockout.PARTITION_NETWORK;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(WorkDirectoryExtension.class)
@Timeout(90)
public class ItDistributionZonesTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItDistributionZonesTest.class);

    /**
     * Nodes bootstrap configuration pattern.
     *
     * <p>rpcInstallSnapshotTimeout is changed to 10 seconds so that sporadic snapshot installation failures still
     * allow tests pass thanks to retries.
     */
    private static final String NODE_BOOTSTRAP_CFG = "{\n"
            + "  network: {\n"
            + "    port:{},\n"
            + "    nodeFinder:{\n"
            + "      netClusterNodes: [ {} ]\n"
            + "    }\n"
            + "  },\n"
            + "  raft.rpcInstallSnapshotTimeout: 10000"
            + "}";

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir, NODE_BOOTSTRAP_CFG);
    }

    @AfterEach
    @Timeout(60)
    void shutdownCluster() {
        cluster.shutdown();
    }
    @Test
    void assingmentsChangingOnNodeLeaveNodeJoin() throws Exception {
        cluster.startAndInit(3);

        createTestTableWith3Replicas();

        assertTrue(waitForCondition(() -> assingmentsCount(0) == 3, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(1) == 3, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(2) == 3, 5000));

        cluster.knockOutNode(1, PARTITION_NETWORK);

        assertTrue(waitForCondition(() -> assingmentsCount(0) == 2, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(1) == 3, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(2) == 2, 5000));

        cluster.reanimateNode(1, PARTITION_NETWORK);

        assertTrue(waitForCondition(() -> assingmentsCount(0) == 3, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(1) == 3, 5000));
        assertTrue(waitForCondition(() -> assingmentsCount(2) == 3, 5000));
    }

    private int assingmentsCount(int node) {
        ExtendedTableConfiguration table =
                (ExtendedTableConfiguration) cluster.node(node)
                        .clusterConfiguration().getConfiguration(TablesConfiguration.KEY).tables().get("TEST");

        byte[] assignmentsBytes = table.assignments().value();

        if (assignmentsBytes != null) {
            return ((List<Set<Assignment>>) ByteUtils.fromBytes(assignmentsBytes)).get(0).size();
        } else {
            return -1;
        }
    }

    private void createTestTableWith3Replicas() throws InterruptedException {
        String sql1 = "create zone test_zone with "
                + "data_nodes_auto_adjust_scale_up=2, "
                + "data_nodes_auto_adjust_scale_down=3";
        String sql2 = "create table test (key int primary key, value varchar(20))"
                + " with partitions=1, replicas=3, primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
            executeUpdate(sql2, session);
        });

        waitForTableToStart();
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18203 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.runningNodes()
                    .map(ItDistributionZonesTest::tablePartitionIds)
                    .mapToInt(List::size)
                    .sum();
            return numberOfStartedRaftNodes == 3;
        };

        assertTrue(waitForCondition(tableStarted, 10_000), "Did not see all table RAFT nodes started");
    }

    /**
     * Returns the IDs of all table partitions that exist on the given node.
     */
    private static List<TablePartitionId> tablePartitionIds(IgniteImpl node) {
        return node.raftManager().localNodes().stream()
                .map(RaftNodeId::groupId)
                .filter(TablePartitionId.class::isInstance)
                .map(TablePartitionId.class::cast)
                .collect(toList());
    }
}
