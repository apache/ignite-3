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

package org.apache.ignite.internal.distributionzones;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.Cluster.NodeKnockout.PARTITION_NETWORK;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.exception.ReplicaUnavailableException;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.configuration.ExtendedTableConfiguration;
import org.apache.ignite.internal.schema.configuration.TablesConfiguration;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.table.distributed.replicator.TablePartitionId;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.util.ByteUtils;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for rebalance process.
 */
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
    @Disabled
    void assingmentsChangingOnNodeLeaveNodeJoin() throws Exception {
        cluster.startAndInit(4);

        createTestTable();

        assertTrue(waitAssingments(List.of(
                Set.of(0, 1, 2),
                Set.of(0, 1, 2),
                Set.of(0, 1, 2),
                Set.of(0, 1, 2)
        )));

        TableImpl table = (TableImpl) cluster.node(0).tables().table("TEST");

        BinaryRowEx key = new TupleMarshallerImpl(table.schemaView()).marshal(Tuple.create().set("id", 1));

        assertNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(0).node()).get());
        assertNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(1).node()).get());
        assertNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(2).node()).get());

        putData();

        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(0).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(1).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(2).node()).get());

        try {
            table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(3).node()).get();

            fail();
        } catch (Exception e) {
            assertInstanceOf(ExecutionException.class, e);

            assertInstanceOf(ReplicaUnavailableException.class, e.getCause());
        }

        cluster.knockOutNode(2, PARTITION_NETWORK);

        assertTrue(waitAssingments(List.of(
                Set.of(0, 1, 3),
                Set.of(0, 1, 3),
                Set.of(0, 1, 2),
                Set.of(0, 1, 3)
        )));

        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(0).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(1).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(3).node()).get());

        cluster.reanimateNode(2, PARTITION_NETWORK);

        assertTrue(waitAssingments(List.of(
                Set.of(0, 1, 2),
                Set.of(0, 1, 2),
                Set.of(0, 1, 2),
                Set.of(0, 1, 2)
        )));

        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(0).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(1).node()).get());
        assertNotNull(table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(2).node()).get());

        try {
            table.internalTable().get(key, new HybridClockImpl().now(), cluster.node(3).node()).get();

            fail();
        } catch (Exception e) {
            assertInstanceOf(ExecutionException.class, e);

            assertInstanceOf(ReplicaUnavailableException.class, e.getCause());
        }
    }

    private boolean waitAssingments(List<Set<Integer>> nodes) throws InterruptedException {
        return waitForCondition(() -> {
            for (int i = 0; i < nodes.size(); i++) {
                Set<Integer> excpectedAssignments = nodes.get(i);

                ExtendedTableConfiguration table =
                        (ExtendedTableConfiguration) cluster.node(i)
                                .clusterConfiguration().getConfiguration(TablesConfiguration.KEY).tables().get("TEST");

                byte[] assignmentsBytes = table.assignments().value();

                Set<String> assignments;

                if (assignmentsBytes != null) {
                    assignments = ((List<Set<Assignment>>) ByteUtils.fromBytes(assignmentsBytes)).get(0)
                            .stream().map(assignment -> assignment.consistentId()).collect(Collectors.toSet());
                } else {
                    assignments = Collections.emptySet();
                }

                LOG.info("Assignments for node " + i + ": " + assignments);

                if (!(excpectedAssignments.size() == assignments.size())
                        || !excpectedAssignments.stream().allMatch(node -> assignments.contains(cluster.node(node).name()))) {
                    return false;
                }
            }

            return true;
        },
                5000);
    }

    private void createTestTable() throws InterruptedException {
        String sql1 = "create zone test_zone with "
                + "data_nodes_auto_adjust_scale_up=0, "
                + "data_nodes_auto_adjust_scale_down=0";
        String sql2 = "create table test (id int primary key, value varchar(20))"
                    + " with partitions=1, replicas=3, primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
            executeUpdate(sql2, session);
        });

        waitForTableToStart();
    }

    private void putData() {
        String sql3 = "insert into test (id, value) values(1, 'qwer')";

        cluster.doInSession(0, session -> {
            executeUpdate(sql3, session);
        });
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
