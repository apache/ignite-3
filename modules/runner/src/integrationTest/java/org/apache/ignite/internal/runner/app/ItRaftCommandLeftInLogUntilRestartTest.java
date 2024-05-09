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

package org.apache.ignite.internal.runner.app;

import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import org.apache.ignite.internal.ClusterPerClassIntegrationTest;
import org.apache.ignite.internal.app.IgniteImpl;
import org.apache.ignite.internal.lang.IgniteBiTuple;
import org.apache.ignite.internal.lang.IgniteStringFormatter;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.schema.BinaryRow;
import org.apache.ignite.internal.schema.BinaryRowEx;
import org.apache.ignite.internal.schema.SchemaRegistry;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerException;
import org.apache.ignite.internal.schema.marshaller.TupleMarshallerImpl;
import org.apache.ignite.internal.schema.row.Row;
import org.apache.ignite.internal.table.TableViewInternal;
import org.apache.ignite.internal.testframework.IgniteTestUtils;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.ApplyTask;
import org.apache.ignite.raft.jraft.core.FSMCallerImpl.TaskType;
import org.apache.ignite.raft.jraft.disruptor.StripedDisruptor;
import org.apache.ignite.raft.jraft.entity.NodeId;
import org.apache.ignite.table.Table;
import org.apache.ignite.table.Tuple;
import org.apache.ignite.tx.Transaction;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * The class has tests of cluster recovery when no all committed RAFT commands applied to the state machine.
 */
@Disabled("https://issues.apache.org/jira/browse/IGNITE-20393")
public class ItRaftCommandLeftInLogUntilRestartTest extends ClusterPerClassIntegrationTest {

    private final Object[][] dataSet = {
            {1, "Igor", 10d},
            {2, null, 15d},
            {3, "Ilya", 15d},
            {4, "Roma", 10d}
    };

    @Override
    protected int initialNodes() {
        return 2;
    }

    /**
     * Tests recovery of transaction commit from RAFT log on restart.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testCommitCommand() throws Exception {
        restartClusterWithNotAppliedCommands(
                tx -> insertDataInTransaction(tx, "person", List.of("ID", "NAME", "SALARY"), dataSet),
                tx -> {
                },
                ignite -> checkData(ignite, dataSet)
        );
    }

    /**
     * Tests recovery of transaction Update All operation from RAFT log on restart.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testUpdateAllCommand() throws Exception {
        restartClusterWithNotAppliedCommands(
                tx -> {
                },
                tx -> insertDataInTransaction(tx, "person", List.of("ID", "NAME", "SALARY"), dataSet),
                ignite -> checkData(ignite, dataSet)
        );
    }

    /**
     * Tests recovery of transaction commit from RAFT log on restart using key value view.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testCommitCommandKeyValueView() throws Exception {
        restartClusterWithNotAppliedCommands(
                tx -> {
                    var kvView = CLUSTER.aliveNode().tables().table(DEFAULT_TABLE_NAME).keyValueView();

                    for (var row : dataSet) {
                        kvView.put(tx, Tuple.create().set("ID", row[0]), Tuple.create().set("NAME", row[1]).set("SALARY", row[2]));
                    }
                },
                tx -> {
                },
                ignite -> checkData(ignite, dataSet)
        );
    }

    /**
     * Tests recovery of transaction Update All operation from RAFT log on restart using key value view.
     *
     * @throws Exception If fail.
     */
    @Test
    public void testUpdateCommandKeyValueView() throws Exception {
        restartClusterWithNotAppliedCommands(
                tx -> {
                },
                tx -> {
                    var kvView = CLUSTER.aliveNode().tables().table(DEFAULT_TABLE_NAME).keyValueView();

                    for (var row : dataSet) {
                        kvView.put(tx, Tuple.create().set("ID", row[0]), Tuple.create().set("NAME", row[1]).set("SALARY", row[2]));
                    }
                },
                ignite -> checkData(ignite, dataSet)
        );
    }

    /**
     * Restarts a test cluster, where part RAFT command will be applied from log on the second start.
     *
     * @param beforeBlock An action which is applied on all nodes in the test cluster.
     * @param afterBlock An action which is applied on leader and written in log on follower.
     * @param checkAction An action to check data after restart.
     * @throws Exception If fail.
     */
    private void restartClusterWithNotAppliedCommands(
            Consumer<Transaction> beforeBlock,
            Consumer<Transaction> afterBlock,
            Consumer<IgniteImpl> checkAction
    ) throws Exception {
        var node0 = CLUSTER.node(0);
        var node1 = CLUSTER.node(1);

        AtomicReference<IgniteBiTuple<ClusterNode, String>> leaderAndGroupRef = new AtomicReference<>();

        var appliedIndexNode0 = partitionUpdateInhibitor(node0, leaderAndGroupRef);
        var appliedIndexNode1 = partitionUpdateInhibitor(node1, leaderAndGroupRef);

        TableViewInternal table = (TableViewInternal) createTable(DEFAULT_TABLE_NAME, 2, 1);

        ClusterNode leader = table.internalTable().tableRaftService().leaderAssignment(0);

        boolean isNode0Leader = node0.id().equals(leader.id());

        BinaryRowEx key = marshalKey(table, Tuple.create().set("id", 42));

        if (isNode0Leader) {
            assertNull(table.internalTable().get(key, node1.clock().now(), node1.node()).get());
        } else {
            assertNull(table.internalTable().get(key, node1.clock().now(), node0.node()).get());
        }

        var tx = node0.transactions().begin();

        try {
            beforeBlock.accept(tx);

            assertTrue(IgniteTestUtils.waitForCondition(() -> appliedIndexNode0.get() == appliedIndexNode1.get(), 10_000));

            RaftGroupService raftGroupService = table.internalTable().tableRaftService().partitionRaftGroupService(0);

            raftGroupService.peers().forEach(peer -> assertThat(raftGroupService.snapshot(peer), willCompleteSuccessfully()));

            leaderAndGroupRef.set(new IgniteBiTuple<>(leader, table.tableId() + "_part_0"));

            afterBlock.accept(tx);

            tx.commit();
        } finally {
            tx.rollback();
        }

        CLUSTER.stopNode(0);
        CLUSTER.stopNode(1);

        log.info("Restart the cluster");

        var node0Started = CLUSTER.startNode(0);
        var node1Started = CLUSTER.startNode(1);

        var ignite = isNode0Leader ? node1Started : node0Started;

        checkAction.accept(ignite);

        clearData(ignite.tables().table(DEFAULT_TABLE_NAME));
    }

    private static Row marshalKey(TableViewInternal table, Tuple tuple) throws TupleMarshallerException {
        SchemaRegistry schemaReg = table.schemaView();
        var marshaller = new TupleMarshallerImpl(schemaReg.lastKnownSchema());

        return marshaller.marshalKey(tuple);
    }

    /**
     * Inhibits updates on follower node after leader and group name are assigned.
     *
     * @param node Node which storage updates will be inhibited.
     * @param leaderAndGroupRef Pair contains of leader and RAFT group name.
     * @return Atomic long that represents an applied index.
     */
    private AtomicLong partitionUpdateInhibitor(
            IgniteImpl node,
            AtomicReference<IgniteBiTuple<ClusterNode, String>> leaderAndGroupRef
    ) {
        AtomicLong appliedIndex = new AtomicLong();

        var nodeOptions = node.raftManager().server().options();

        var notTunedDisruptor = nodeOptions.getfSMCallerExecutorDisruptor();

        nodeOptions.setfSMCallerExecutorDisruptor(new StripedDisruptor<>(
                node.name() + "-test",
                "JRaft-FSMCaller-Disruptor",
                64,
                () -> new ApplyTask(),
                1,
                false,
                false,
                null
        ) {
            @Override
            public RingBuffer<ApplyTask> subscribe(NodeId group, EventHandler<ApplyTask> handler,
                    BiConsumer<ApplyTask, Throwable> exceptionHandler) {
                return super.subscribe(group, (event, sequence, endOfBatch) -> {
                    if (leaderAndGroupRef.get() != null
                            && event.nodeId().getGroupId().equals(leaderAndGroupRef.get().get2())
                            && !node.node().equals(leaderAndGroupRef.get().get1())) {
                        log.info("Event for RAFT [grp={}, type={}, idx={}]", event.nodeId().getGroupId(), event.type, event.committedIndex);

                        if (event.type == TaskType.SHUTDOWN) {
                            event.shutdownLatch.countDown();
                        }

                        return;
                    }

                    long idx = event.committedIndex;

                    handler.onEvent(event, sequence, endOfBatch);

                    appliedIndex.set(idx);
                }, exceptionHandler);
            }

            @Override
            public void shutdown() {
                super.shutdown();

                if (notTunedDisruptor != null) {
                    notTunedDisruptor.shutdown();
                }
            }
        });

        return appliedIndex;
    }

    private void checkData(IgniteImpl ignite, Object[][] dataSet) {
        TableViewInternal table = (TableViewInternal) ignite.tables().table(DEFAULT_TABLE_NAME);

        assertNotNull(table);

        for (Object[] row : dataSet) {
            try {
                Tuple txTuple = table.keyValueView().get(null, Tuple.create().set("ID", row[0]));

                assertNotNull(txTuple);

                assertEquals(row[1], txTuple.value("NAME"));
                assertEquals(row[2], txTuple.value("SALARY"));

                BinaryRowEx testKey = marshalKey(table, Tuple.create().set("ID", row[0]));

                BinaryRow readOnlyBinaryRow = table.internalTable().get(testKey, ignite.clock().now(), ignite.node()).get();

                assertNotNull(readOnlyBinaryRow);

                Row readOnlyRow = Row.wrapBinaryRow(table.schemaView().lastKnownSchema(), readOnlyBinaryRow);

                assertEquals(row[1], readOnlyRow.stringValue(2));
                assertEquals(row[2], readOnlyRow.doubleValue(1));
            } catch (Exception e) {
                new RuntimeException(IgniteStringFormatter.format("Cannot check a row {}", row), e);
            }
        }

        transferLeadershipToLocalNode(ignite);

        for (Object[] row : dataSet) {
            try {
                Tuple txTuple = table.keyValueView().get(null, Tuple.create().set("ID", row[0]));

                assertNotNull(txTuple);

                assertEquals(row[1], txTuple.value("NAME"));
                assertEquals(row[2], txTuple.value("SALARY"));
            } catch (Exception e) {
                new RuntimeException(IgniteStringFormatter.format("Cannot check a row {} when the local node leader", row), e);
            }
        }
    }

    /**
     * Transfers the leader to the local node related to the Ignite instance.
     *
     * @param ignite Ignite instance.
     */
    private void transferLeadershipToLocalNode(IgniteImpl ignite) {
        TableViewInternal table = (TableViewInternal) ignite.tables().table(DEFAULT_TABLE_NAME);

        RaftGroupService raftGroupService = table.internalTable().tableRaftService().partitionRaftGroupService(0);

        List<Peer> peers = raftGroupService.peers();
        assertNotNull(peers);

        Peer leader = raftGroupService.leader();
        assertNotNull(leader);

        Peer localPeer = peers.stream().filter(peer -> peer.consistentId().equals(ignite.name())).findFirst().get();

        log.info("Leader is transferring [from={}, to={}]", leader, localPeer);

        assertThat(raftGroupService.transferLeadership(localPeer), willCompleteSuccessfully());
    }

    /**
     * Clears data with primary keys for 0 to 100.
     *
     * @param table Ignite table.
     */
    private static void clearData(Table table) {
        ArrayList<Tuple> keysToRemove = new ArrayList<>(100);

        IntStream.range(0, 100).forEach(rowId -> keysToRemove.add(Tuple.create().set("ID", rowId)));

        table.keyValueView().removeAll(null, keysToRemove);
    }
}
