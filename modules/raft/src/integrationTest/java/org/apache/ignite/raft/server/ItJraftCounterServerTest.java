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

package org.apache.ignite.raft.server;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.raft.jraft.core.State.STATE_ERROR;
import static org.apache.ignite.raft.jraft.core.State.STATE_LEADER;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.raft.server.counter.GetValueCommand.getValueCommand;
import static org.apache.ignite.raft.server.counter.IncrementAndGetCommand.incrementAndGetCommand;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.messages.TestRaftMessagesFactory;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.ignite.raft.server.counter.GetValueCommand;
import org.apache.ignite.raft.server.counter.IncrementAndGetCommand;
import org.apache.ignite.raft.server.snasphot.SnapshotInMemoryStorageFactory;
import org.apache.ignite.raft.server.snasphot.UpdateCountRaftListener;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;

/**
 * Jraft server.
 */
class ItJraftCounterServerTest extends JraftAbstractTest {
    /**
     * Counter group name 0.
     */
    private static final TestReplicationGroupId COUNTER_GROUP_0 = new TestReplicationGroupId("counter0");

    /**
     * Counter group name 1.
     */
    private static final TestReplicationGroupId COUNTER_GROUP_1 = new TestReplicationGroupId("counter1");

    /**
     * Listener factory.
     */
    private Supplier<CounterListener> listenerFactory = CounterListener::new;

    /**
     * Starts a cluster for the test.
     *
     * @throws Exception If failed.
     */
    private void startCluster() throws Exception {
        for (int i = 0; i < 3; i++) {
            startServer(i, raftServer -> {
                raftServer.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), initialConf, defaults());
                raftServer.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), initialConf, defaults());
            }, opts -> {});
        }

        startClient(COUNTER_GROUP_0);
        startClient(COUNTER_GROUP_1);
    }

    /**
     * Checks that the number of Disruptor threads does not depend on  count started RAFT nodes.
     */
    @Test
    public void testDisruptorThreadsCount() {
        startServer(0, raftServer -> {
            raftServer.startRaftGroup(new TestReplicationGroupId("test_raft_group"), listenerFactory.get(), initialConf, defaults());
        }, opts -> {});

        Set<Thread> threads = getAllDisruptorCurrentThreads();

        int threadsBefore = threads.size();

        Set<String> threadNamesBefore = threads.stream().map(Thread::getName).collect(toSet());

        assertEquals(NodeOptions.DEFAULT_STRIPES * 4/*services*/, threadsBefore, "Started thread names: " + threadNamesBefore);

        servers.forEach(srv -> {
            for (int i = 0; i < 10; i++) {
                srv.startRaftGroup(new TestReplicationGroupId("test_raft_group_" + i), listenerFactory.get(), initialConf, defaults());
            }
        });

        threads = getAllDisruptorCurrentThreads();

        int threadsAfter = threads.size();

        Set<String> threadNamesAfter = threads.stream().map(Thread::getName).collect(toSet());

        threadNamesAfter.removeAll(threadNamesBefore);

        assertEquals(threadsBefore, threadsAfter, "Difference: " + threadNamesAfter);

        servers.forEach(srv -> {
            srv.stopRaftGroup(new TestReplicationGroupId("test_raft_group"));

            for (int i = 0; i < 10; i++) {
                srv.stopRaftGroup(new TestReplicationGroupId("test_raft_group_" + i));
            }
        });
    }

    /**
     * Get a set of Disruptor threads for the well known JRaft services.
     *
     * @return Set of Disruptor threads.
     */
    @NotNull
    private Set<Thread> getAllDisruptorCurrentThreads() {
        return Thread.getAllStackTraces().keySet().stream().filter(t ->
                        t.getName().contains("JRaft-FSMCaller-Disruptor")
                                || t.getName().contains("JRaft-NodeImpl-Disruptor")
                                || t.getName().contains("JRaft-ReadOnlyService-Disruptor")
                                || t.getName().contains("JRaft-LogManager-Disruptor"))
                .collect(toSet());
    }

    @Test
    public void testRefreshLeader() throws Exception {
        startCluster();

        Peer leader = clients.get(0).leader();

        assertNull(leader);

        clients.get(0).refreshLeader().get();

        assertNotNull(clients.get(0).leader());

        leader = clients.get(1).leader();

        assertNull(leader);

        clients.get(1).refreshLeader().get();

        assertNotNull(clients.get(1).leader());
    }

    @Test
    public void testCounterCommandListener() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        assertNotNull(client1.leader());
        assertNotNull(client2.leader());

        assertEquals(2, client1.<Long>run(incrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Long>run(getValueCommand()).get());
        assertEquals(3, client1.<Long>run(incrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Long>run(getValueCommand()).get());

        assertEquals(4, client2.<Long>run(incrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Long>run(getValueCommand()).get());
        assertEquals(7, client2.<Long>run(incrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Long>run(getValueCommand()).get());
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        JraftServerImpl server = servers.get(0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        client1.snapshot(server.localPeer(COUNTER_GROUP_0)).get();

        long val2 = applyIncrements(client2, 1, 20);

        assertEquals(sum(20), val2);

        client2.snapshot(server.localPeer(COUNTER_GROUP_1)).get();

        Path snapshotDir0 = server.getServerDataPath(COUNTER_GROUP_0).resolve("snapshot");
        assertEquals(1L, countFiles(snapshotDir0));

        Path snapshotDir1 = server.getServerDataPath(COUNTER_GROUP_1).resolve("snapshot");
        assertEquals(1L, countFiles(snapshotDir1));
    }

    /**
     * Returns the number of files in the given directory (non-recursive).
     */
    private static long countFiles(Path dir) throws IOException {
        try (Stream<Path> files = Files.list(dir)) {
            return files.count();
        }
    }

    @Test
    public void testCreateSnapshotGracefulFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override
            public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
                doneClo.accept(new IgniteInternalException("Very bad"));
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        Peer peer = server.localPeer(COUNTER_GROUP_0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        try {
            client1.snapshot(peer).get();

            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    @Test
    public void testCreateSnapshotAbnormalFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override
            public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
                doneClo.accept(new IgniteInternalException("Very bad"));
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        Peer peer = servers.get(0).localPeer(COUNTER_GROUP_0);

        try {
            client1.snapshot(peer).get();

            fail();
        } catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    /** Tests if a raft group become unavailable in case of a critical error. */
    @Test
    public void testApplyWithFailure() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override
            public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                Iterator<CommandClosure<WriteCommand>> wrapper = new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }

                    @Override
                    public CommandClosure<WriteCommand> next() {
                        CommandClosure<WriteCommand> cmd = iterator.next();

                        IncrementAndGetCommand command = (IncrementAndGetCommand) cmd.command();

                        if (command.delta() == 10) {
                            throw new IgniteInternalException("Very bad");
                        }

                        return cmd;
                    }
                };

                super.onWrite(wrapper);
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        NodeImpl leader = servers.stream().map(s -> ((NodeImpl) s.raftGroupService(COUNTER_GROUP_0).getRaftNode()))
                .filter(n -> n.getState() == STATE_LEADER).findFirst().orElse(null);

        assertNotNull(leader);

        long val1 = applyIncrements(client1, 1, 5);
        long val2 = applyIncrements(client2, 1, 7);

        assertEquals(sum(5), val1);
        assertEquals(sum(7), val2);

        long val3 = applyIncrements(client1, 6, 9);
        assertEquals(sum(9), val3);

        try {
            client1.<Long>run(incrementAndGetCommand(10)).get();

            fail();
        } catch (Exception e) {
            // Expected.
            Throwable cause = e.getCause();

            assertTrue(cause instanceof RaftException);
        }

        NodeImpl finalLeader = leader;
        waitForCondition(() -> finalLeader.getState() == STATE_ERROR, 5_000);

        // Client can't switch to new leader, because only one peer in the list.
        try {
            client1.<Long>run(incrementAndGetCommand(11)).get();
        } catch (Exception e) {
            boolean isValid = e.getCause() instanceof TimeoutException;

            if (!isValid) {
                logger().error("Got unexpected exception", e);
            }

            assertTrue(isValid, "Expecting the timeout");
        }
    }

    /** Tests that users related exceptions from SM are propagated to the client. */
    @Test
    public void testClientCatchExceptionFromSm() throws Exception {
        listenerFactory = () -> new CounterListener() {
            @Override
            public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
                while (iterator.hasNext()) {
                    CommandClosure<WriteCommand> clo = iterator.next();

                    IncrementAndGetCommand cmd0 = (IncrementAndGetCommand) clo.command();

                    clo.result(new RuntimeException("Expected message"));
                }
            }

            @Override
            public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
                while (iterator.hasNext()) {
                    CommandClosure<ReadCommand> clo = iterator.next();

                    assert clo.command() instanceof GetValueCommand;

                    clo.result(new RuntimeException("Another expected message"));
                }
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        NodeImpl leader = servers.stream().map(s -> ((NodeImpl) s.raftGroupService(COUNTER_GROUP_0).getRaftNode()))
                .filter(n -> n.getState() == STATE_LEADER).findFirst().orElse(null);

        assertNotNull(leader);

        try {
            client1.<Long>run(incrementAndGetCommand(3)).get();

            fail();
        } catch (Exception e) {
            // Expected.
            Throwable cause = e.getCause();

            assertTrue(cause instanceof RuntimeException);

            assertEquals(cause.getMessage(), "Expected message");
        }

        try {
            client1.<Long>run(getValueCommand()).get();

            fail();
        } catch (Exception e) {
            // Expected.
            Throwable cause = e.getCause();

            assertTrue(cause instanceof RuntimeException);

            assertEquals(cause.getMessage(), "Another expected message");
        }
    }

    /** Tests if a follower is catching up the leader after restarting. */
    @Test
    public void testFollowerCatchUpFromLog() throws Exception {
        doTestFollowerCatchUp(false, true);
    }

    @Test
    public void testFollowerCatchUpFromSnapshot() throws Exception {
        doTestFollowerCatchUp(true, true);
    }

    @Test
    public void testFollowerCatchUpFromLog2() throws Exception {
        doTestFollowerCatchUp(false, false);
    }

    @Test
    public void testFollowerCatchUpFromSnapshot2() throws Exception {
        doTestFollowerCatchUp(true, false);
    }

    /** Tests if a starting a new group in shared pools mode doesn't increases timer threads count. */
    @Test
    public void testTimerThreadsCount() {
        JraftServerImpl srv0 = startServer(0, x -> {
        }, opts -> opts.setTimerPoolSize(1));
        JraftServerImpl srv1 = startServer(1, x -> {
        }, opts -> opts.setTimerPoolSize(1));
        JraftServerImpl srv2 = startServer(2, x -> {
        }, opts -> opts.setTimerPoolSize(1));

        waitForTopology(srv0.clusterService(), 3, 5_000);

        ExecutorService svc = Executors.newFixedThreadPool(16);

        final int groupsCnt = 10;

        try {
            List<Future<?>> futs = new ArrayList<>(groupsCnt);

            for (int i = 0; i < groupsCnt; i++) {
                int finalI = i;
                futs.add(svc.submit(new Runnable() {
                    @Override
                    public void run() {
                        TestReplicationGroupId grp = new TestReplicationGroupId("counter" + finalI);

                        srv0.startRaftGroup(grp, listenerFactory.get(), initialConf, defaults());
                        srv1.startRaftGroup(grp, listenerFactory.get(), initialConf, defaults());
                        srv2.startRaftGroup(grp, listenerFactory.get(), initialConf, defaults());
                    }
                }));
            }

            for (Future<?> fut : futs) {
                try {
                    fut.get();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        } finally {
            ExecutorServiceHelper.shutdownAndAwaitTermination(svc);
        }

        for (int i = 0; i < groupsCnt; i++) {
            TestReplicationGroupId grp = new TestReplicationGroupId("counter" + i);

            assertTrue(waitForCondition(() -> hasLeader(grp), 30_000));
        }

        Set<Thread> threads = Thread.getAllStackTraces().keySet();

        logger().info("RAFT threads count {}", threads.stream().filter(t -> t.getName().contains("JRaft")).count());

        List<Thread> timerThreads = threads.stream().filter(this::isTimer).sorted(comparing(Thread::getName)).collect(toList());

        assertTrue(timerThreads.size() <= 15, // This is a maximum possible number of a timer threads for 3 nodes in this test.
                "All timer threads: " + timerThreads.toString());
    }

    /**
     * The test shows that all committed updates are applied after a RAFT group restart automatically.
     * Actual data be available to read from state storage (not a state machine) directly just after the RAFT node started.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testApplyUpdatesOutOfSnapshot() throws Exception {
        HashMap<Integer, AtomicInteger> counters = new HashMap<>(3);
        HashMap<Path, Integer> snapshotDataStorage = new HashMap<>(3);
        HashMap<String, SnapshotMeta> snapshotMetaStorage = new HashMap<>(3);
        TestReplicationGroupId grpId = new TestReplicationGroupId("test_raft_group");

        for (int i = 0; i < 3; i++) {
            AtomicInteger counter;

            counters.put(i, counter = new AtomicInteger());

            startServer(i, raftServer -> {
                raftServer.startRaftGroup(grpId, new UpdateCountRaftListener(counter, snapshotDataStorage), initialConf,
                        defaults().snapshotStorageFactory(new SnapshotInMemoryStorageFactory(snapshotMetaStorage)));
            }, opts -> {});
        }

        var raftClient = startClient(grpId);

        raftClient.refreshMembers(true).get();
        var peers = raftClient.peers();

        var testWriteCommandBuilder = new TestRaftMessagesFactory().testWriteCommand();

        raftClient.run(testWriteCommandBuilder.build());

        assertTrue(TestUtils.waitForCondition(() -> counters.get(0).get() == 1, 10_000));

        raftClient.snapshot(peers.get(0)).get();

        raftClient.run(testWriteCommandBuilder.build());

        assertTrue(TestUtils.waitForCondition(() -> counters.get(1).get() == 2, 10_000));

        raftClient.snapshot(peers.get(1)).get();

        raftClient.run(testWriteCommandBuilder.build());

        for (AtomicInteger counter : counters.values()) {
            assertTrue(TestUtils.waitForCondition(() -> counter.get() == 3, 10_000));
        }

        shutdownCluster();

        Path peer0SnapPath = snapshotPath(peers, 0);
        Path peer1SnapPath = snapshotPath(peers, 1);
        Path peer2SnapPath = snapshotPath(peers, 2);

        assertEquals(1, snapshotDataStorage.get(peer0SnapPath));
        assertEquals(2, snapshotDataStorage.get(peer1SnapPath));
        assertNull(snapshotDataStorage.get(peer2SnapPath));

        assertNotNull(snapshotMetaStorage.get(peer0SnapPath.toString()));
        assertNotNull(snapshotMetaStorage.get(peer1SnapPath.toString()));
        assertNull(snapshotMetaStorage.get(peer2SnapPath.toString()));

        for (int i = 0; i < 3; i++) {
            var counter = counters.get(i);

            startServer(i, raftServer -> {
                counter.set(0);

                raftServer.startRaftGroup(grpId, new UpdateCountRaftListener(counter, snapshotDataStorage), initialConf,
                        defaults().snapshotStorageFactory(new SnapshotInMemoryStorageFactory(snapshotMetaStorage)));
            }, opts -> {});
        }

        for (AtomicInteger counter : counters.values()) {
            assertEquals(3, counter.get());
        }
    }

    /**
     * Builds a snapshot path by the peer address of RAFT node.
     *
     * @param peers Raft node peers.
     * @param index Raft node peer index.
     * @return Path to snapshot.
     */
    private Path snapshotPath(List<Peer> peers, int index) {
        return workDir
                .resolve("node" + index)
                .resolve("test_raft_group" + "_" + peers.get(index).consistentId())
                .resolve("snapshot");
    }

    /**
     * Returns {@code true} if thread is related to timers.
     *
     * @param thread The thread.
     * @return {@code True} if a timer thread.
     */
    private boolean isTimer(Thread thread) {
        String name = thread.getName();

        return name.contains("ElectionTimer") || name.contains("VoteTimer")
                || name.contains("StepDownTimer") || name.contains("SnapshotTimer") || name.contains("Node-Scheduler");
    }

    /**
     * Returns {@code true} if a raft group has elected a leader for a some term.
     *
     * @param grpId Group id.
     * @return {@code True} if a leader is elected.
     */
    private boolean hasLeader(TestReplicationGroupId grpId) {
        return servers.stream().anyMatch(s -> {
            NodeImpl node = (NodeImpl) s.raftGroupService(grpId).getRaftNode();

            StateMachineAdapter fsm = (StateMachineAdapter) node.getOptions().getFsm();

            return node.isLeader() && fsm.getLeaderTerm() == node.getCurrentTerm();
        });
    }

    /**
     * Do test follower catch up.
     *
     * @param snapshot {@code True} to create snapshot on leader and truncate log.
     * @param cleanDir {@code True} to clean persistent state on follower before restart.
     * @throws Exception If failed.
     */
    private void doTestFollowerCatchUp(boolean snapshot, boolean cleanDir) throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        Peer leader1 = client1.leader();
        assertNotNull(leader1);

        Peer leader2 = client2.leader();
        assertNotNull(leader2);

        applyIncrements(client1, 0, 10);
        applyIncrements(client2, 0, 20);

        // First snapshot will not truncate logs.
        client1.snapshot(leader1).get();
        client2.snapshot(leader2).get();

        JraftServerImpl toStop = null;

        // Find the follower for both groups.
        for (JraftServerImpl server : servers) {
            Peer peer = server.localPeer(COUNTER_GROUP_0);

            if (!peer.equals(leader1) && !peer.equals(leader2)) {
                toStop = server;
                break;
            }
        }

        Path serverDataPath0 = toStop.getServerDataPath(COUNTER_GROUP_0);
        Path serverDataPath1 = toStop.getServerDataPath(COUNTER_GROUP_1);

        final int stopIdx = servers.indexOf(toStop);

        toStop.stopRaftGroup(COUNTER_GROUP_0);
        toStop.stopRaftGroup(COUNTER_GROUP_1);

        toStop.beforeNodeStop();

        toStop.stop();

        applyIncrements(client1, 11, 20);
        applyIncrements(client2, 21, 30);

        if (snapshot) {
            client1.snapshot(leader1).get();
            client2.snapshot(leader2).get();
        }

        if (cleanDir) {
            IgniteUtils.deleteIfExists(serverDataPath0);
            IgniteUtils.deleteIfExists(serverDataPath1);
        }

        var svc2 = startServer(stopIdx, r -> {
            r.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), initialConf, defaults());
            r.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), initialConf, defaults());
        }, opts -> {});

        waitForCondition(() -> validateStateMachine(sum(20), svc2, COUNTER_GROUP_0), 5_000);
        waitForCondition(() -> validateStateMachine(sum(30), svc2, COUNTER_GROUP_1), 5_000);

        svc2.stopRaftGroup(COUNTER_GROUP_0);
        svc2.stopRaftGroup(COUNTER_GROUP_1);

        svc2.beforeNodeStop();

        svc2.stop();

        var svc3 = startServer(stopIdx, r -> {
            r.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), initialConf, defaults());
            r.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), initialConf, defaults());
        }, opts -> {});

        waitForCondition(() -> validateStateMachine(sum(20), svc3, COUNTER_GROUP_0), 5_000);
        waitForCondition(() -> validateStateMachine(sum(30), svc3, COUNTER_GROUP_1), 5_000);
    }

    /**
     * Applies increments.
     *
     * @param client The client
     * @param start  Start element.
     * @param stop   Stop element.
     * @return The counter value.
     * @throws Exception If failed.
     */
    private static long applyIncrements(RaftGroupService client, int start, int stop) throws Exception {
        long val = 0;

        for (int i = start; i <= stop; i++) {
            val = client.<Long>run(incrementAndGetCommand(i)).get();

            logger().info("Val={}, i={}", val, i);
        }

        return val;
    }

    /**
     * Calculates a progression sum.
     *
     * @param until Until value.
     * @return The sum.
     */
    private static long sum(long until) {
        return (1 + until) * until / 2;
    }

    /**
     * Validates state machine.
     *
     * @param expected Expected value.
     * @param server   The server.
     * @param groupId  Group id.
     * @return Validation result.
     */
    private static boolean validateStateMachine(long expected, JraftServerImpl server, TestReplicationGroupId groupId) {
        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(groupId);

        JraftServerImpl.DelegatingStateMachine fsm0 =
                (JraftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        return expected == ((CounterListener) fsm0.getListener()).value();
    }
}
