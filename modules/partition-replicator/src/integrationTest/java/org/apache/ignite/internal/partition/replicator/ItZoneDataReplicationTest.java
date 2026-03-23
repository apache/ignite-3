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
import static org.apache.ignite.internal.sql.SqlCommon.DEFAULT_SCHEMA_NAME;
import static org.apache.ignite.internal.table.TableTestUtils.dropTable;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.apache.ignite.internal.partition.replicator.fixtures.Node;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.replicator.Member;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.ZonePartitionId;
import org.apache.ignite.internal.storage.MvPartitionStorage;
import org.apache.ignite.internal.storage.engine.MvTableStorage;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.ExecutorServiceExtension;
import org.apache.ignite.internal.testframework.InjectExecutorService;
import org.apache.ignite.raft.jraft.rpc.Message;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotRequest;
import org.apache.ignite.raft.jraft.rpc.RpcRequests.InstallSnapshotResponse;
import org.apache.ignite.table.KeyValueView;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Class containing tests related to Raft-based replication for the Colocation feature.
 */
@ExtendWith(ExecutorServiceExtension.class)
public class ItZoneDataReplicationTest extends ItAbstractColocationTest {
    /**
     * Tests that inserted data is replicated to all replica nodes.
     */
    @ParameterizedTest(name = "useExplicitTx={0}")
    @ValueSource(booleans = {false, true})
    void testReplicationOnAllNodes(boolean useExplicitTx) throws Exception {
        startCluster(3);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node.
        createZone(node0, TEST_ZONE_NAME, 1, cluster.size());

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

        // Test single insert.
        if (useExplicitTx) {
            node.transactions().runInTransaction(tx -> {
                kvView1.put(tx, 42L, 69);
                kvView2.put(tx, 142L, 169);
            });
        } else {
            kvView1.put(null, 42L, 69);
            kvView2.put(null, 142L, 169);
        }

        for (Node n : cluster) {
            if (useExplicitTx) {
                node.transactions().runInTransaction(tx -> {
                    assertThat(n.name, kvView1.get(tx, 42L), is(69));
                    assertThat(n.name, kvView1.get(tx, 142L), is(nullValue()));

                    assertThat(n.name, kvView2.get(tx, 42L), is(nullValue()));
                    assertThat(n.name, kvView2.get(tx, 142L), is(169));
                });
            } else {
                assertThat(n.name, kvView1.get(null, 42L), is(69));
                assertThat(n.name, kvView1.get(null, 142L), is(nullValue()));

                assertThat(n.name, kvView2.get(null, 42L), is(nullValue()));
                assertThat(n.name, kvView2.get(null, 142L), is(169));
            }
        }

        // Test batch insert.
        Map<Long, Integer> data1 = longsToIntsMap(0, 10);
        Map<Long, Integer> data2 = longsToIntsMap(10, 20);

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

    private static Map<Long, Integer> longsToIntsMap(int fromInclusive, int toExclusive) {
        return IntStream.range(fromInclusive, toExclusive).boxed().collect(toMap(n -> (long) n, Function.identity()));
    }

    /**
     * Tests that inserted data is replicated to a newly joined replica node.
     */
    @ParameterizedTest(name = "truncateRaftLog={0}")
    @ValueSource(booleans = {false, true})
    void testDataRebalance(boolean truncateRaftLog) throws Exception {
        startCluster(2);

        Node node0 = getNode(0);

        // Create a zone with a single partition on every node + one extra replica for the upcoming node.
        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size() + 1);

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);
        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME2);

        var zonePartitionId = new ZonePartitionId(zoneId, 0);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        Map<Long, Integer> data1 = longsToIntsMap(0, 10);
        Map<Long, Integer> data2 = longsToIntsMap(10, 20);

        KeyValueView<Long, Integer> kvView1 = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);
        KeyValueView<Long, Integer> kvView2 = node.tableManager.table(TEST_TABLE_NAME2).keyValueView(Long.class, Integer.class);

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

        Node node0 = getNode(0);

        // Create a zone with the test profile. The storage in it is augmented to lose all data upon restart, but its Raft configuration
        // is persistent, so the data can be restored.
        createZoneWithStorageProfiles(node0, TEST_ZONE_NAME, 1, cluster.size(), "test");

        createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);

        cluster.forEach(Node::waitForMetadataCompletenessAtNow);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);

        kvView.put(null, 42L, 42);

        // Restart the node.
        node.stop();

        cluster.remove(0);

        node = addNodeToCluster();

        node.waitForMetadataCompletenessAtNow();

        kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);

        assertThat(kvView.get(null, 42L), is(42));
    }

    /**
     * Tests the following scenario.
     *
     * <ol>
     *     <li>Create a table and fill it with data;</li>
     *     <li>Truncate Raft log to force snapshot installation on joining nodes;</li>
     *     <li>Drop the table, but do not update the low watermark;</li>
     *     <li>Add a new node to the cluster;</li>
     *     <li>Existing node will start streaming the snapshot to the joining node. At the same time, update the Low Watermark value
     *     to force deletion of table storages on the existing node;</li>
     *     <li>Validate that concurrent snapshot streaming and storages removals do not interfere with each other.</li>
     * </ol>
     */
    @Test
    void testTableDropInTheMiddleOfRebalanceOnSendingSide(@InjectExecutorService ExecutorService executorService) throws Exception {
        disableLowWatermarkUpdates();

        startCluster(1);

        Node node0 = getNode(0);

        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size() + 1);

        int tableId = createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);

        Node node = cluster.get(0);

        // Add some data to the table.
        KeyValueView<Long, Integer> kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);

        Map<Long, Integer> data = longsToIntsMap(0, 10);

        kvView.putAll(null, data);

        // Truncate the Raft log to force snapshot installation on the joining node.
        var partitionId = new ZonePartitionId(zoneId, 0);

        truncateLogOnEveryNode(partitionId);

        dropTable(node.catalogManager, DEFAULT_SCHEMA_NAME, TEST_TABLE_NAME1);

        // Block snapshot installation before the new node joins.
        CompletableFuture<Void> outgoingSnapshotInstallationStartedFuture = startBlockOutgoingSnapshot(node, partitionId);

        Node receiverNode = addNodeToCluster();

        // After we detected that snapshot is going to be installed, update the low watermark to start table removal and allow the
        // snapshot to proceed.
        CompletableFuture<Void> runRaceFuture = outgoingSnapshotInstallationStartedFuture
                .thenRunAsync(() -> {
                    node.lowWatermark.updateLowWatermark(node.hybridClock.now());

                    // Check that no partitions are destroyed until the rebalance is completed.
                    verify(tableStorage(node, tableId), timeout(1000).times(0)).destroyPartition(anyInt());

                    stopBlockingMessages(node, partitionId);
                }, executorService);

        assertThat(runRaceFuture, willCompleteSuccessfully());

        // Rebalance is complete, the storage must be destroyed.
        assertTrue(waitForCondition(() -> tableStorage(node, tableId) == null, 10_000));

        MvTableStorage receiverTableStorage = tableStorage(receiverNode, tableId);

        assertThat(receiverTableStorage, is(notNullValue()));

        MvPartitionStorage mvPartition = receiverTableStorage.getMvPartition(0);

        assertThat(mvPartition, is(notNullValue()));

        assertTrue(waitForCondition(() -> mvPartition.estimatedSize() == data.size(), 10_000));
    }

    /**
     * Similar to {@link #testTableDropInTheMiddleOfRebalanceOnSendingSide} but tests table storage removal on the snapshot receiving side.
     */
    @Test
    void testTableDropInTheMiddleOfRebalanceOnReceivingSide(@InjectExecutorService ExecutorService executorService) throws Exception {
        disableLowWatermarkUpdates();

        startCluster(1);

        Node node0 = getNode(0);

        int zoneId = createZone(node0, TEST_ZONE_NAME, 1, cluster.size() + 1);

        int tableId = createTable(node0, TEST_ZONE_NAME, TEST_TABLE_NAME1);

        Node node = cluster.get(0);

        KeyValueView<Long, Integer> kvView = node.tableManager.table(TEST_TABLE_NAME1).keyValueView(Long.class, Integer.class);

        Map<Long, Integer> data = longsToIntsMap(0, 10);

        kvView.putAll(null, data);

        var partitionId = new ZonePartitionId(zoneId, 0);

        truncateLogOnEveryNode(partitionId);

        dropTable(node.catalogManager, DEFAULT_SCHEMA_NAME, TEST_TABLE_NAME1);

        // Block outgoing snapshot so that we have time to register mocks on the receiving side.
        startBlockOutgoingSnapshot(node, partitionId);

        Node receiverNode = addNodeToCluster();

        // Wait for the table to start on the receiving side.
        assertTrue(waitForCondition(() -> tableStorage(receiverNode, tableId) != null, 10_000));

        MvTableStorage receivingTableStorage = tableStorage(receiverNode, tableId);

        // Future that gets completed as soon as "receivingTableStorage.startRebalancePartition" has been called.
        var startRebalanceInvokedFuture = new CompletableFuture<Void>();

        // Future that blocks the execution of "receivingTableStorage.startRebalancePartition".
        var startRebalanceBlockingFuture = new CompletableFuture<Void>();

        doAnswer(invocationOnMock -> {
            CompletableFuture<Void> result = startRebalanceBlockingFuture.thenCompose(v -> {
                try {
                    return (CompletableFuture<Void>) invocationOnMock.callRealMethod();
                } catch (Throwable e) {
                    throw new CompletionException(e);
                }
            });

            startRebalanceInvokedFuture.complete(null);

            return result;
        }).when(receivingTableStorage).startRebalancePartition(anyInt());

        // Mocks have been registered, unblock snapshot on the sending side.
        stopBlockingMessages(node, partitionId);

        // This actually blocks confirmation of the snapshot having been installed from the receiving side. This is needed to make it
        // easier to call verification on mocks: we check that partition was only removed after the snapshot installation has been
        // completed.
        startBlockSnapshotResponse(receiverNode, partitionId);

        CompletableFuture<Void> runRaceFuture = startRebalanceInvokedFuture
                .thenRunAsync(() -> {
                    receiverNode.lowWatermark.updateLowWatermark(receiverNode.hybridClock.now());

                    startRebalanceBlockingFuture.complete(null);

                    verify(receivingTableStorage, timeout(1_000).times(0)).destroyPartition(anyInt());

                    stopBlockingMessages(receiverNode, partitionId);
                }, executorService);

        assertThat(runRaceFuture, willCompleteSuccessfully());

        // Rebalance is complete, the storage must be destroyed.
        assertTrue(waitForCondition(() -> tableStorage(receiverNode, tableId) == null, 10_000));
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

    private static @Nullable MvTableStorage tableStorage(Node node, int tableId) {
        TableViewInternal table = node.tableManager.startedTables().get(tableId);

        return table == null ? null : table.internalTable().storage();
    }

    /**
     * Starts to block {@link InstallSnapshotRequest} sent from the given node (should be the group leader) to a follower.
     *
     * @return Future that completes when the node tried to send the first {@link InstallSnapshotRequest} message.
     */
    private static CompletableFuture<Void> startBlockOutgoingSnapshot(Node node, ZonePartitionId partitionId) throws InterruptedException {
        return startBlockingMessages(node, partitionId, InstallSnapshotRequest.class);
    }

    /**
     * Starts to block {@link InstallSnapshotResponse} sent from the given node to the group leader after the snapshot has been installed.
     *
     * @return Future that completes when the node tried to send the first {@link InstallSnapshotResponse} message.
     */
    private static CompletableFuture<Void> startBlockSnapshotResponse(Node node, ZonePartitionId partitionId) throws InterruptedException {
        return startBlockingMessages(node, partitionId, InstallSnapshotResponse.class);
    }

    private static CompletableFuture<Void> startBlockingMessages(
            Node node,
            ZonePartitionId partitionId,
            Class<? extends Message> messageType
    ) throws InterruptedException {
        var raftServer = (JraftServerImpl) node.raftManager.server();

        var raftNodeId = new RaftNodeId(partitionId, new Peer(node.name));

        // Wait for the Raft node to start.
        assertTrue(waitForCondition(() -> raftServer.raftGroupService(raftNodeId) != null, 10_000));

        var conditionReachedFuture = new CompletableFuture<Void>();

        raftServer.blockMessages(raftNodeId, (msg, peerId) -> {
            if (messageType.isInstance(msg)) {
                conditionReachedFuture.complete(null);

                return true;
            }
            return false;
        });

        return conditionReachedFuture;
    }

    private static void stopBlockingMessages(Node node, ZonePartitionId partitionId) {
        var raftNodeId = new RaftNodeId(partitionId, new Peer(node.name));

        ((JraftServerImpl) node.raftManager.server()).stopBlockMessages(raftNodeId);
    }

    private void disableLowWatermarkUpdates() {
        CompletableFuture<Void> cfgChangeFuture = gcConfiguration.lowWatermark()
                .change(lowWatermarkChange -> lowWatermarkChange.changeUpdateIntervalMillis(Long.MAX_VALUE));

        assertThat(cfgChangeFuture, willCompleteSuccessfully());
    }
}
