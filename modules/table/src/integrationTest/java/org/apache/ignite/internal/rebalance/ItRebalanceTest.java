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

import static org.apache.ignite.internal.SessionUtils.executeUpdate;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.RebalanceUtil.partitionAssignments;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.internal.Cluster;
import org.apache.ignite.internal.affinity.Assignment;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.table.Tuple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Test suite for the rebalance.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItRebalanceTest extends BaseIgniteAbstractTest {
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
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-20996")
    @Test
    void assignmentsChangingOnNodeLeaveNodeJoin() throws Exception {
        cluster.startAndInit(4);

        createZone("TEST_ZONE", 1, 3);
        // Creates table with 1 partition and 3 replicas.
        createTestTable("TEST_TABLE", "TEST_ZONE");

        TableViewInternal table = (TableViewInternal) cluster.node(0).tables().table("TEST_TABLE");

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(2)
        ), table.tableId());

        BinaryRowEx row = marshalTuple(table, Tuple.create().set("id", 1).set("val", "value1"));
        BinaryRowEx key = marshalTuple(table, Tuple.create().set("id", 1));

        assertThat(table.internalTable().get(key, clock.now(), cluster.node(0).node()), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(1).node()), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), cluster.node(2).node()), willBe(nullValue()));

        assertThat(table.internalTable().insert(row, null), willCompleteSuccessfully());

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

    private static Row marshalTuple(TableViewInternal table, Tuple tuple) {
        SchemaRegistry schemaReg = table.schemaView();
        var marshaller = new TupleMarshallerImpl(schemaReg.lastKnownSchema());

        return marshaller.marshal(tuple);
    }

    private void waitForStableAssignmentsInMetastore(Set<String> expectedNodes, int table) throws InterruptedException {
        Set<String>[] lastAssignmentsHolderForLog = new Set[1];

        assertTrue(waitForCondition(() -> {
            Set<String> assignments =
                    await(partitionAssignments(cluster.aliveNode().metaStorageManager(), table, 0))
                            .stream()
                            .map(Assignment::consistentId)
                            .collect(Collectors.toSet());

            lastAssignmentsHolderForLog[0] = assignments;

            return assignments.equals(expectedNodes);
        }, 30000), "Expected nodes: " + expectedNodes + ", actual nodes: " + lastAssignmentsHolderForLog[0]);
    }

    private String nodeName(int nodeIndex) {
        return cluster.node(nodeIndex).name();
    }

    private void createZone(String zoneName, int partitions, int replicas) {
        String sql1 = String.format("create zone %s with "
                + "partitions=%d, replicas=%d, "
                + "data_nodes_auto_adjust_scale_up=0, "
                + "data_nodes_auto_adjust_scale_down=0, "
                + "storage_profiles='%s'", zoneName, partitions, replicas, DEFAULT_STORAGE_PROFILE);

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
        });
    }

    private void alterZone(String zoneName, int replicas) {
        String sql1 = String.format("alter zone %s set "
                + "replicas=%d, "
                + "data_nodes_auto_adjust_scale_up=0, "
                + "data_nodes_auto_adjust_scale_down=0", zoneName, replicas);

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
        });
    }

    private int createTestTable(String tableName, String zoneName) {
        String sql2 = "create table " + tableName + " (id int primary key, val varchar(20))"
                + " with primary_zone='" + zoneName + "'";

        cluster.doInSession(0, session -> {
            executeUpdate(sql2, session);
        });

        CatalogManager catalogManager = cluster.aliveNode().catalogManager();

        return catalogManager.catalog(catalogManager.latestCatalogVersion()).tables().stream()
                .filter(t -> t.name().equals(tableName))
                .findFirst().get().id();
    }

    private void dropTestTable(String tableName) {
        String sql2 = "drop table if exists " + tableName;

        cluster.doInSession(1, session -> {
            executeUpdate(sql2, session);
        });
    }
}
