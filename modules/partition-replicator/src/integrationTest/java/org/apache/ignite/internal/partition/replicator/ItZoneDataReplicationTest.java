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

package org.apache.ignite.internal.partition.replicator;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.replicator.Member;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.table.KeyValueView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
public class ItZoneDataReplicationTest extends AbstractZoneReplicationTest {
    /**
     * Tests that inserted data is replicated to all replica nodes.
     */
    @ParameterizedTest(name = "useExplicitTx={0}")
    @ValueSource(booleans = {false, true})
    void testReplicationOnAllNodes(boolean useExplicitTx) throws Exception {
        startCluster(3);

        // Create a zone with a single partition on every node.
        createZone(TEST_ZONE_NAME, 1, cluster.size());

        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        // Test single insert.
        if (useExplicitTx) {
            node.transactions().runInTransaction(tx -> {
                kvView1.put(tx, 42, 69);
                kvView2.put(tx, 142, 169);
            });
        } else {
            kvView1.put(null, 42, 69);
            kvView2.put(null, 142, 169);
        }

        for (Node n : cluster) {
            if (useExplicitTx) {
                node.transactions().runInTransaction(tx -> {
                    assertThat(n.name, kvView1.get(tx, 42), is(69));
                    assertThat(n.name, kvView1.get(tx, 142), is(nullValue()));

                    assertThat(n.name, kvView2.get(tx, 42), is(nullValue()));
                    assertThat(n.name, kvView2.get(tx, 142), is(169));
                });
            } else {
                assertThat(n.name, kvView1.get(null, 42), is(69));
                assertThat(n.name, kvView1.get(null, 142), is(nullValue()));

                assertThat(n.name, kvView2.get(null, 42), is(nullValue()));
                assertThat(n.name, kvView2.get(null, 142), is(169));
            }
        }

        // Test batch insert.
        Map<Integer, Integer> data1 = IntStream.range(0, 10).boxed().collect(toMap(Function.identity(), Function.identity()));
        Map<Integer, Integer> data2 = IntStream.range(10, 20).boxed().collect(toMap(Function.identity(), Function.identity()));

        if (useExplicitTx) {
            node.transactions().runInTransaction(tx -> {
                kvView1.putAll(tx, data1);
                kvView2.putAll(tx, data2);
            });
        } else {
            kvView1.putAll(null, data1);
            kvView2.putAll(null, data2);
        }

        for (Node n : cluster) {
            if (useExplicitTx) {
                node.transactions().runInTransaction(tx -> {
                    assertThat(n.name, kvView1.getAll(tx, data1.keySet()), is(data1));
                    assertThat(n.name, kvView1.getAll(tx, data2.keySet()), is(anEmptyMap()));

                    assertThat(n.name, kvView2.getAll(tx, data1.keySet()), is(anEmptyMap()));
                    assertThat(n.name, kvView2.getAll(tx, data2.keySet()), is(data2));
                });
            } else {
                assertThat(n.name, kvView1.getAll(null, data1.keySet()), is(data1));
                assertThat(n.name, kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

                assertThat(n.name, kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
                assertThat(n.name, kvView2.getAll(null, data2.keySet()), is(data2));
            }
        }
    }

    /**
     * Tests that inserted data is replicated to a newly joined replica node.
     */
    @ParameterizedTest(name = "truncateRaftLog={0}")
    @ValueSource(booleans = {false, true})
    void testDataRebalance(boolean truncateRaftLog) throws Exception {
        startCluster(2);

        // Create a zone with a single partition on every node + one extra replica for the upcoming node.
        int zoneId = createZone(TEST_ZONE_NAME, 1, cluster.size() + 1);

        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        Map<Integer, Integer> data1 = IntStream.range(0, 10).boxed().collect(toMap(Function.identity(), Function.identity()));
        Map<Integer, Integer> data2 = IntStream.range(10, 20).boxed().collect(toMap(Function.identity(), Function.identity()));

        KeyValueView<Integer, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);
        KeyValueView<Integer, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Integer.class, Integer.class);

        kvView1.putAll(null, data1);
        kvView2.putAll(null, data2);

        if (truncateRaftLog) {
            truncateLogOnEveryNode(zonePartitionId);
        }

        Node newNode = addNodeToCluster();

        // Wait for the rebalance to kick in.
        assertTrue(waitForCondition(() -> newNode.replicaManager.isReplicaStarted(zonePartitionId), 10_000L));

        assertThat(kvView1.getAll(null, data1.keySet()), is(data1));
        assertThat(kvView1.getAll(null, data2.keySet()), is(anEmptyMap()));

        assertThat(kvView2.getAll(null, data1.keySet()), is(anEmptyMap()));
        assertThat(kvView2.getAll(null, data2.keySet()), is(data2));
    }

    /**
     * Tests the recovery phase, when a node is restarted and we expect the data to be restored by the Raft mechanisms.
     */
    @Test
    void testLocalRaftLogReapplication() throws Exception {
        startCluster(1);

        // Create a zone with the test profile. The storage in it is augmented to lose all data upon restart, but its Raft configuration
        // is persistent, so the data can be restored.
        createZoneWithProfile(TEST_ZONE_NAME, 1, cluster.size(), new String[]{"test"});

        createTable(TEST_ZONE_NAME, TEST_TABLE_NAME1);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Integer, Integer> kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);

        kvView.put(null, 42, 42);

        // Restart the node.
        node.stop();

        cluster.remove(0);

        node = addNodeToCluster();

        node.waitForMetadataCompletenessAtNow();

        kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Integer.class, Integer.class);

        assertThat(kvView.get(null, 42), is(42));
    }

    private void truncateLogOnEveryNode(ReplicationGroupId groupId) {
        CompletableFuture<?>[] truncateFutures = cluster.stream()
                .map(node -> truncateLog(node, groupId))
                .toArray(CompletableFuture[]::new);

        assertThat(allOf(truncateFutures), willCompleteSuccessfully());
    }

    private static CompletableFuture<Void> truncateLog(Node node, ReplicationGroupId groupId) {
        Member member = Member.votingMember(node.name);

        return node.replicaManager.replica(groupId)
                .thenCompose(replica -> replica.createSnapshotOn(member, true));
    }
}
