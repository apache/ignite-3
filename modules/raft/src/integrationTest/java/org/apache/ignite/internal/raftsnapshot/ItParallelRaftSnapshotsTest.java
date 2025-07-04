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

package org.apache.ignite.internal.raftsnapshot;

import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.TestWrappers.unwrapIgniteImpl;
import static org.apache.ignite.internal.TestWrappers.unwrapTableImpl;
import static org.apache.ignite.internal.catalog.CatalogService.DEFAULT_STORAGE_PROFILE;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.Member;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;

class ItParallelRaftSnapshotsTest extends ClusterPerTestIntegrationTest {
    private static final String TEST_ZONE_NAME = "TEST_ZONE";

    private static final String TEST_TABLE_NAME = "TEST_TABLE";

    @Override
    protected int initialNodes() {
        // We need at least 5 nodes, because we are going to stop 2 nodes and do not want to lose majority.
        return 5;
    }

    /**
     * Tests that a Raft snapshot can be simultaneously streamed to multiple nodes.
     */
    @Test
    void testSnapshotStreamingToMultipleNodes() {
        String zoneSql = String.format(
                "CREATE ZONE %s WITH STORAGE_PROFILES='%s', PARTITIONS=1, REPLICAS=5",
                TEST_ZONE_NAME, DEFAULT_STORAGE_PROFILE
        );

        executeSql(zoneSql);

        String tableSql = String.format(
                "CREATE TABLE %s (key INT PRIMARY KEY, val VARCHAR(20)) ZONE %s ",
                TEST_TABLE_NAME, TEST_ZONE_NAME
        );

        executeSql(tableSql);

        ReplicationGroupId groupId = cluster.solePartitionId(TEST_ZONE_NAME, TEST_TABLE_NAME);

        int primaryReplicaIndex = primaryReplicaIndex(groupId);

        // Stop two nodes that are neither primary replicas for the test table nor for the metastorage.
        List<Integer> nodesToKill = cluster.aliveNodesWithIndices().stream()
                .map(IgniteBiTuple::get1)
                .filter(index -> index != primaryReplicaIndex && index != 0)
                .limit(2)
                .collect(toList());

        nodesToKill.parallelStream().forEach(cluster::stopNode);

        // After the nodes have been stopped, insert some data and truncate the Raft log on primary replica.
        executeSql(primaryReplicaIndex, String.format("INSERT INTO %s VALUES (1, 'one')", TEST_TABLE_NAME));

        validateTableData(primaryReplicaIndex);

        truncateLog(primaryReplicaIndex, groupId);

        // Start the nodes in parallel, Raft leader is expected to start streaming snapshots on them.
        nodesToKill.parallelStream().forEach(cluster::startNode);

        nodesToKill.forEach(this::validateTableData);
    }

    private int primaryReplicaIndex(ReplicationGroupId groupId) {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        CompletableFuture<ReplicaMeta> primaryFuture = node.placementDriver()
                .awaitPrimaryReplica(groupId, node.clockService().now(), 30, SECONDS);

        assertThat(primaryFuture, willBe(notNullValue()));

        String primaryName = primaryFuture.join().getLeaseholder();

        assertThat(primaryName, is(notNullValue()));

        return cluster.nodeIndex(primaryName);
    }

    private void truncateLog(int nodeIndex, ReplicationGroupId groupId) {
        IgniteImpl node = unwrapIgniteImpl(cluster.node(nodeIndex));

        Member member = Member.votingMember(node.name());

        CompletableFuture<Void> truncateFuture = node.replicaManager().replica(groupId)
                .thenCompose(replica -> replica.createSnapshotOn(member, true));

        assertThat(truncateFuture, willCompleteSuccessfully());
    }

    private void validateTableData(int nodeIndex) {
        Table table = cluster.node(nodeIndex).tables().table(TEST_TABLE_NAME);

        InternalTable internalTable = unwrapTableImpl(table).internalTable();

        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(0);

        assertThat(mvPartition, is(notNullValue()));

        await().until(mvPartition::estimatedSize, is(1L));
    }
}
