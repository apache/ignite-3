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

import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableViewInternal;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.distributionzones.rebalance.ZoneRebalanceUtil.stablePartitionAssignments;
import static org.apache.ignite.internal.sql.engine.util.SqlTestUtils.executeUpdate;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.await;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.testNodeName;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.InitParametersBuilder;
import org.apache.ignite.internal.ClusterConfiguration.Builder;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.catalog.CatalogManager;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.marshaller.ReflectionMarshallersProvider;
import org.apache.ignite.internal.partitiondistribution.Assignment;
import org.apache.ignite.internal.replicator.exception.ReplicationException;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.marshaller.reflection.KvMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.table.mapper.Mapper;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the rebalance.
 */
public class ItRebalanceTest extends ClusterPerTestIntegrationTest {
    private final HybridClock clock = new HybridClockImpl();

    @Override
    protected int initialNodes() {
        return 4;
    }

    @Override
    protected void customizeInitParameters(InitParametersBuilder builder) {
        builder.clusterConfiguration("ignite.replication.rpcTimeoutMillis: 8000");
    }

    @Override
    protected void customizeConfiguration(Builder clusterConfigurationBuilder) {
        clusterConfigurationBuilder
                .nodeNamingStrategy((conf, index) -> testNodeName(conf.testInfo(), index));
    }

    /**
     * The test checks that data is rebalanced after node with replica is left and joined to the cluster.
     *
     * @throws Exception If failed.
     */
    @Test
    void assignmentsChangingOnNodeLeaveNodeJoin() throws Exception {
        createZone("TEST_ZONE", 1, 3);
        // Creates table with 1 partition and 3 replicas.
        createTestTable("TEST_TABLE", "TEST_ZONE");

        TableViewInternal table = unwrapTableViewInternal(cluster.node(0).tables().table("TEST_TABLE"));

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(2)
        ), table);

        BinaryRowEx row = marshalTuple(table, Tuple.create().set("id", 1).set("val", "value1"));
        BinaryRowEx key = marshalKey(table, 1, Integer.class);

        assertThat(table.internalTable().get(key, clock.now(), clusterNode(0)), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(1)), willBe(nullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(2)), willBe(nullValue()));

        assertThat(table.internalTable().insert(row, null), willCompleteSuccessfully());

        assertThat(table.internalTable().get(key, clock.now(), clusterNode(0)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(1)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(2)), willBe(notNullValue()));

        assertThat(
                table.internalTable().get(key, clock.now(), clusterNode(3)),
                willThrow(ReplicationException.class, 10, TimeUnit.SECONDS)
        );

        cluster.stopNode(2);

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(3)
        ), table);

        assertThat(table.internalTable().get(key, clock.now(), clusterNode(0)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(1)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(3)), willBe(notNullValue()));

        cluster.startNode(2);

        waitForStableAssignmentsInMetastore(Set.of(
                nodeName(0),
                nodeName(1),
                nodeName(2)
        ), table);

        assertThat(table.internalTable().get(key, clock.now(), clusterNode(0)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(1)), willBe(notNullValue()));
        assertThat(table.internalTable().get(key, clock.now(), clusterNode(2)), willBe(notNullValue()));

        assertThat(
                table.internalTable().get(key, clock.now(), clusterNode(3)),
                willThrow(ReplicationException.class, 10, TimeUnit.SECONDS)
        );
    }

    private static Row marshalTuple(TableViewInternal table, Tuple tuple) {
        SchemaRegistry schemaReg = table.schemaView();
        var marshaller = new TupleMarshallerImpl(table::qualifiedName, schemaReg.lastKnownSchema());

        return marshaller.marshal(tuple);
    }

    private static <K> Row marshalKey(TableViewInternal table, K key, Class<K> keyClass) {
        SchemaRegistry schemaReg = table.schemaView();
        var marshaller = new KvMarshallerImpl<>(
                schemaReg.lastKnownSchema(),
                new ReflectionMarshallersProvider(),
                Mapper.of(keyClass),
                Mapper.of(Void.class)
        );

        return marshaller.marshal(key);
    }

    private void waitForStableAssignmentsInMetastore(Set<String> expectedNodes, TableViewInternal table) throws InterruptedException {
        Set<String>[] lastAssignmentsHolderForLog = new Set[1];

        assertTrue(waitForCondition(() -> {
            IgniteImpl ignite = unwrapIgniteImpl(cluster.aliveNode());

            CompletableFuture<Set<Assignment>> assignmentsFuture = stablePartitionAssignments(
                    ignite.metaStorageManager(),
                    table.zoneId(),
                    0
            );

            Set<String> assignments = await(assignmentsFuture)
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
        String sql1 = String.format("create zone %s "
                + "(partitions %d, replicas %d, "
                + "auto scale up 0, "
                + "auto scale down 0) "
                + "storage profiles ['%s']", zoneName, partitions, replicas, DEFAULT_STORAGE_PROFILE);

        cluster.doInSession(0, session -> {
            executeUpdate(sql1, session);
        });
    }

    private int createTestTable(String tableName, String zoneName) {
        String sql2 = "create table " + tableName + " (id int primary key, val varchar(20))"
                + " zone " + zoneName;

        cluster.doInSession(0, session -> {
            executeUpdate(sql2, session);
        });

        CatalogManager catalogManager = unwrapIgniteImpl(cluster.aliveNode()).catalogManager();

        return catalogManager.latestCatalog()
                .tables()
                .stream()
                .filter(t -> t.name().equals(tableName))
                .findFirst().get().id();
    }
}
