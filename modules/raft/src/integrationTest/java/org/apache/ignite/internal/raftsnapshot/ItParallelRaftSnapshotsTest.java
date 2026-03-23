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
import static org.apache.ignite.internal.event.EventListener.fromConsumer;
import static org.apache.ignite.internal.lowwatermark.event.LowWatermarkEvent.LOW_WATERMARK_CHANGED;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntPredicate;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.ClusterPerTestIntegrationTest;
import org.apache.ignite.internal.TestWrappers;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.placementdriver.ReplicaMeta;
import org.apache.ignite.internal.replicator.Member;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.schema.configuration.GcExtensionConfiguration;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.table.InternalTable;
import org.apache.ignite.internal.util.CompletableFutures;
import org.apache.ignite.table.Table;
import org.junit.jupiter.api.Test;

class ItParallelRaftSnapshotsTest extends ClusterPerTestIntegrationTest {
    private static final String ZONE_NAME = "TEST_ZONE";

    private static final String TABLE_NAME = "TEST_TABLE";

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
        createZoneAndTable(ZONE_NAME, TABLE_NAME, 1, initialNodes());

        ReplicationGroupId groupId = cluster.solePartitionId(ZONE_NAME);

        int primaryReplicaIndex = primaryReplicaIndex(groupId);

        // Stop two nodes that are neither primary replicas for the test table nor for the metastorage.
        int[] nodesToKill = nodeToKillIndexes(nodeIndex -> nodeIndex != primaryReplicaIndex && nodeIndex != 0, 2);

        Arrays.stream(nodesToKill).parallel().forEach(cluster::stopNode);

        int tableSize = 100;

        // After the nodes have been stopped, insert some data and truncate the Raft log on primary replica.
        insertIntoTable(primaryReplicaIndex, TABLE_NAME, tableSize);

        validateTableData(primaryReplicaIndex, tableSize);

        truncateLog(primaryReplicaIndex, groupId);

        // Start the nodes in parallel, Raft leader is expected to start streaming snapshots on them.
        Arrays.stream(nodesToKill).parallel().forEach(cluster::startNode);

        Arrays.stream(nodesToKill).forEach(nodeIndex -> validateTableData(nodeIndex, tableSize));
    }

    @Test
    void testInstallRaftSnapshotAfterUpdateLowWatermark() {
        updateLowWatermarkUpdateInterval(1_000);

        assertThat(awaitUpdateLowWatermarkAsync(cluster.nodes()), willCompleteSuccessfully());

        testSnapshotStreamingToMultipleNodes();
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

    private void validateTableData(int nodeIndex, long expTableSize) {
        Table table = cluster.node(nodeIndex).tables().table(TABLE_NAME);

        InternalTable internalTable = unwrapTableImpl(table).internalTable();

        MvPartitionStorage mvPartition = internalTable.storage().getMvPartition(0);

        assertThat(mvPartition, is(notNullValue()));

        await().until(mvPartition::estimatedSize, is(expTableSize));
    }

    private void createZoneAndTable(String zoneName, String tableName, int partitions, int replicas) {
        executeSql(String.format(
                "CREATE ZONE %s WITH STORAGE_PROFILES='%s', PARTITIONS=%s, REPLICAS=%s",
                zoneName, DEFAULT_STORAGE_PROFILE, partitions, replicas
        ));

        executeSql(String.format(
                "CREATE TABLE %s (key INT PRIMARY KEY, val VARCHAR(256)) ZONE %s ",
                tableName, zoneName
        ));
    }

    private int[] nodeToKillIndexes(IntPredicate indexFilter, int limit) {
        return cluster.aliveNodesWithIndices().stream()
                .mapToInt(IgniteBiTuple::get1)
                .filter(Objects::nonNull)
                .filter(indexFilter)
                .limit(limit)
                .toArray();
    }

    private void insertIntoTable(int nodeIndex, String tableName, int count) {
        String insertDml = String.format("INSERT INTO %s (key, val) VALUES (?, ?)", tableName);

        for (int i = 0; i < count; i++) {
            executeSql(nodeIndex, insertDml, i, "n_" + count);
        }
    }

    private void updateLowWatermarkUpdateInterval(long intervalMillis) {
        IgniteImpl node = unwrapIgniteImpl(cluster.aliveNode());

        CompletableFuture<Void> updateConfigFuture = node.clusterConfiguration().getConfiguration(GcExtensionConfiguration.KEY)
                .gc()
                .lowWatermark()
                .updateIntervalMillis()
                .update(intervalMillis);

        assertThat(updateConfigFuture, willCompleteSuccessfully());
    }

    private static CompletableFuture<Void> awaitUpdateLowWatermarkAsync(List<Ignite> nodes) {
        List<CompletableFuture<Void>> futures = nodes.stream()
                .map(TestWrappers::unwrapIgniteImpl)
                .map(IgniteImpl::lowWatermark)
                .map(lowWatermark -> {
                    var future = new CompletableFuture<Void>();

                    lowWatermark.listen(LOW_WATERMARK_CHANGED, fromConsumer(p -> future.complete(null)));

                    return future;
                })
                .collect(toList());

        return CompletableFutures.allOf(futures);
    }
}
