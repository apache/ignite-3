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

package org.apache.ignite.internal.rebalance;

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.IgniteIntegrationTest;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.replicator.TablePartitionId;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.table.TableImpl;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

/**
 * Test suite for the rebalance.
 */
public class ItRebalanceTest extends IgniteIntegrationTest {
    private static final IgniteLogger LOG = Loggers.forClass(ItRebalanceTest.class);

    @WorkDirectory
    private Path workDir;

    private Cluster cluster;

    private final HybridClock clock = new HybridClockImpl();

    @BeforeEach
    void createCluster(TestInfo testInfo) {
        cluster = new Cluster(testInfo, workDir);
    }

    @AfterEach
    void shutdownCluster() {
        cluster.shutdown();
    }

    /**
     * The test checks that data is rebalanced after node with replica is left and joined to the cluster.
     *
     * @throws Exception If failed.
     */
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-19712")
    @Test
    void assignmentsChangingOnNodeLeaveNodeJoin() throws Exception {
        cluster.startAndInit(4);

        //Creates table with 1 partition and 3 replicas.
        createTestTable();

        TableImpl table = (TableImpl) cluster.node(0).tables().table("TEST");

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(2)
        ), table.tableId());

        BinaryRowEx row = new TupleMarshallerImpl(table.schemaView()).marshal(Tuple.create().set("id", 1).set("value", "value1"));
        BinaryRowEx key = new TupleMarshallerImpl(table.schemaView()).marshal(Tuple.create().set("id", 1));

        assertThat(table.internalTable().get(key, clock.now(), cluster.node(0).node()), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(1).node()), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(2).node()), willBe(nullValue()));

        table.internalTable().insert(row, null).get();

        assertThat(table.internalTable().get(key, clock.now(), cluster.node(0).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(1).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(2).node()), willBe(notNullValue()));

        assertThat(
                table.internalTable().get(key, clock.now(), cluster.node(3).node()),
                willThrow(ReplicationException.class, 10, TimeUnit.SECONDS)
        );

        cluster.stopNode(2);

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(3)
        ), table.tableId());

        assertThat(table.internalTable().get(key, clock.now(), cluster.node(0).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(1).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(3).node()), willBe(notNullValue()));

        cluster.startNode(2);

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(2)
        ), table.tableId());

        assertThat(table.internalTable().get(key, clock.now(), cluster.node(0).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(1).node()), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(2).node()), willBe(notNullValue()));

        assertThat(
                table.internalTable().get(key, clock.now(), cluster.node(3).node()),
                willThrow(ReplicationException.class, 10, TimeUnit.SECONDS)
        );
    }

    private void waitForStableAssignmentsInMetastore(Set<String> expectedNodes, int table) throws InterruptedException {
        Set<String>[] lastAssignmentsHolderForLog = new Set[1];

        assertTrue(waitForCondition(() -> {
            Set<String> assignments =
                    partitionAssignments(
                            cluster.aliveNode().metaStorageManager(), table, 0
                    ).join().stream()
                            .map(Assignment::consistentId)
                            .collect(Collectors.toSet());

            lastAssignmentsHolderForLog[0] = assignments;

            return assignments.equals(expectedNodes);
        }, 30000), "Expected nodes: " + expectedNodes + ", actual nodes: " + lastAssignmentsHolderForLog[0]);
    }

    private String nodeName(int nodeIndex) {
        return cluster.node(nodeIndex).name();
    }

    private void createTestTable() throws InterruptedException {
        String sql1 = "create zone test_zone with "
                + "partitions=1, replicas=3, "
                + "data_nodes_auto_adjust_scale_up=0, "
                + "data_nodes_auto_adjust_scale_down=0";
        String sql2 = "create table test (id int primary key, value varchar(20))"
                + " with primary_zone='TEST_ZONE'";

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
            executeUpdate(sql2, session);
        });

        waitForTableToStart();
    }

    private void waitForTableToStart() throws InterruptedException {
        // TODO: IGNITE-18733 - remove this wait because when a table creation query is executed, the table must be fully ready.

        BooleanSupplier tableStarted = () -> {
            int numberOfStartedRaftNodes = cluster.runningNodes()
                    .map(ItRebalanceTest::tablePartitionIds)
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
