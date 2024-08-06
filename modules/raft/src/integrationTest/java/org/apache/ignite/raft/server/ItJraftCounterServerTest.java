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
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.core.State.STATE_ERROR;
import static org.apache.ignite.raft.jraft.core.State.STATE_LEADER;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForTopology;
import static org.apache.ignite.raft.server.counter.GetValueCommand.getValueCommand;
import static org.apache.ignite.raft.server.counter.IncrementAndGetCommand.incrementAndGetCommand;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.ComponentWorkingDir;
import org.apache.ignite.internal.lang.IgniteInternalException;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.ReadCommand;
import org.apache.ignite.internal.raft.WriteCommand;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.raft.jraft.core.NodeImpl;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.RaftOutter.SnapshotMeta;
import org.apache.ignite.raft.jraft.rpc.impl.RaftException;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.messages.TestRaftMessagesFactory;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.ignite.raft.server.counter.GetValueCommand;
import org.apache.ignite.raft.server.counter.IncrementAndGetCommand;
import org.apache.ignite.raft.server.snasphot.SnapshotInMemoryStorageFactory;
import org.apache.ignite.raft.server.snasphot.UpdateCountRaftListener;
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

    /** Amount of stripes for disruptors that are used by JRAFT. */
    private static final int RAFT_STRIPES = 3;

    /** Amount of stripes for disruptors that are used by the log service for JRAFT. */
    private static final int RAFT_LOG_STRIPES = 1;

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

            int finalI = i;
            startServer(i, raftServer -> {
                String localNodeName = raftServer.clusterService().topologyService().localMember().name();

                Peer serverPeer = initialMembersConf.peer(localNodeName);

                RaftGroupOptions groupOptions = groupOptions(raftServer);

                groupOptions.setLogStorageFactory(logStorageFactories.get(finalI));
                groupOptions.serverDataPath(serverWorkingDirs.get(finalI).metaPath());

                raftServer.startRaftNode(
                        new RaftNodeId(COUNTER_GROUP_0, serverPeer), initialMembersConf, listenerFactory.get(), groupOptions
                );
                raftServer.startRaftNode(
                        new RaftNodeId(COUNTER_GROUP_1, serverPeer), initialMembersConf, listenerFactory.get(), groupOptions
                );
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
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            var nodeId = new RaftNodeId(new TestReplicationGroupId("test_raft_group"), initialMembersConf.peer(localNodeName));

            raftServer.startRaftNode(
                    nodeId,
                    initialMembersConf,
                    listenerFactory.get(),
                    groupOptions(raftServer)
                            .setLogStorageFactory(logStorageFactories.get(0))
                            .serverDataPath(serverWorkingDirs.get(0).metaPath())
            );
        }, opts -> {
            opts.setStripes(RAFT_STRIPES);
            opts.setLogStripesCount(RAFT_LOG_STRIPES);
            opts.setLogYieldStrategy(true);
        });

        Set<Thread> threads = getAllDisruptorCurrentThreads();

        int threadsBefore = threads.size();

        Set<String> threadNamesBefore = threads.stream().map(Thread::getName).collect(toSet());

        assertEquals(RAFT_STRIPES * 3/* services */ + RAFT_LOG_STRIPES, threadsBefore, "Started thread names: " + threadNamesBefore);

        for (int j = 0; j < servers.size(); j++) {
            JraftServerImpl srv = servers.get(j);
            String localNodeName = srv.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            for (int i = 0; i < 10; i++) {
                var nodeId = new RaftNodeId(new TestReplicationGroupId("test_raft_group_" + i), serverPeer);

                srv.startRaftNode(
                        nodeId,
                        initialMembersConf,
                        listenerFactory.get(),
                        groupOptions(srv)
                                .setLogStorageFactory(logStorageFactories.get(j))
                                .serverDataPath(serverWorkingDirs.get(j).metaPath())
                );
            }
        }

        threads = getAllDisruptorCurrentThreads();

        int threadsAfter = threads.size();

        Set<String> threadNamesAfter = threads.stream().map(Thread::getName).collect(toSet());

        threadNamesAfter.removeAll(threadNamesBefore);

        assertEquals(threadsBefore, threadsAfter, "Difference: " + threadNamesAfter);
    }

    /**
     * Get a set of Disruptor threads for the well known JRaft services.
     *
     * @return Set of Disruptor threads.
     */
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
        ComponentWorkingDir serverWorkingDir = serverWorkingDirs.get(0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        Peer localPeer0 = server.localPeers(COUNTER_GROUP_0).get(0);

        client1.snapshot(localPeer0).get();

        long val2 = applyIncrements(client2, 1, 20);

        assertEquals(sum(20), val2);

        Peer localPeer1 = server.localPeers(COUNTER_GROUP_1).get(0);

        client2.snapshot(localPeer1).get();

        Path snapshotDir0 = JraftServerImpl.getServerDataPath(
                serverWorkingDir.metaPath().get(),
                new RaftNodeId(COUNTER_GROUP_0, localPeer0)
        ).resolve("snapshot");

        assertEquals(1L, countFiles(snapshotDir0));

        Path snapshotDir1 = JraftServerImpl.getServerDataPath(
                serverWorkingDir.metaPath().get(),
                new RaftNodeId(COUNTER_GROUP_1, localPeer1)
        ).resolve("snapshot");

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

        Peer peer = server.localPeers(COUNTER_GROUP_0).get(0);

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

        Peer peer = servers.get(0).localPeers(COUNTER_GROUP_0).get(0);

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

        NodeImpl leader = getRaftNodes(COUNTER_GROUP_0)
                .filter(n -> n.getState() == STATE_LEADER)
                .findFirst()
                .orElse(null);

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

        NodeImpl leader = getRaftNodes(COUNTER_GROUP_0)
                .filter(n -> n.getState() == STATE_LEADER)
                .findFirst()
                .orElse(null);

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
                futs.add(svc.submit(() -> {
                    var groupId = new TestReplicationGroupId("counter" + finalI);

                    List<JraftServerImpl> list = Arrays.asList(srv0, srv1, srv2);
                    for (int j = 0; j < list.size(); j++) {
                        RaftServer srv = list.get(j);

                        String localNodeName = srv.clusterService().topologyService().localMember().name();

                        Peer serverPeer = initialMembersConf.peer(localNodeName);

                        RaftGroupOptions groupOptions = groupOptions(srv)
                                .setLogStorageFactory(logStorageFactories.get(j))
                                .serverDataPath(serverWorkingDirs.get(j).metaPath());
                        srv.startRaftNode(new RaftNodeId(groupId, serverPeer), initialMembersConf, listenerFactory.get(), groupOptions);
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
        var counters = new ConcurrentHashMap<Peer, AtomicInteger>();
        var snapshotDataStorage = new ConcurrentHashMap<Path, Integer>();
        var snapshotMetaStorage = new ConcurrentHashMap<String, SnapshotMeta>();
        var grpId = new TestReplicationGroupId("test_raft_group");

        for (int i = 0; i < 3; i++) {
            int finalI = i;
            startServer(i, raftServer -> {
                String localNodeName = raftServer.clusterService().topologyService().localMember().name();

                Peer serverPeer = initialMembersConf.peer(localNodeName);

                var counter = new AtomicInteger();

                counters.put(serverPeer, counter);

                var listener = new UpdateCountRaftListener(counter, snapshotDataStorage);

                RaftGroupOptions opts = groupOptions(raftServer)
                        .snapshotStorageFactory(new SnapshotInMemoryStorageFactory(snapshotMetaStorage))
                        .setLogStorageFactory(logStorageFactories.get(finalI))
                        .serverDataPath(serverWorkingDirs.get(finalI).metaPath());

                raftServer.startRaftNode(new RaftNodeId(grpId, serverPeer), initialMembersConf, listener, opts);
            }, opts -> {});
        }

        var raftClient = startClient(grpId);

        raftClient.refreshMembers(true).get();

        List<Peer> peers = raftClient.peers();

        var testWriteCommandBuilder = new TestRaftMessagesFactory().testWriteCommand();

        raftClient.run(testWriteCommandBuilder.build());

        Peer peer0 = peers.get(0);

        assertTrue(waitForCondition(() -> counters.get(peer0).get() == 1, 10_000));

        raftClient.snapshot(peer0).get();

        raftClient.run(testWriteCommandBuilder.build());

        Peer peer1 = peers.get(1);

        assertTrue(waitForCondition(() -> counters.get(peer1).get() == 2, 10_000));

        raftClient.snapshot(peer1).get();

        raftClient.run(testWriteCommandBuilder.build());

        for (AtomicInteger counter : counters.values()) {
            assertTrue(waitForCondition(() -> counter.get() == 3, 10_000));
        }

        Path peer0SnapPath = snapshotPath(peer0, grpId);
        Path peer1SnapPath = snapshotPath(peer1, grpId);
        Path peer2SnapPath = snapshotPath(peers.get(2), grpId);

        shutdownCluster();

        assertEquals(1, snapshotDataStorage.get(peer0SnapPath));
        assertEquals(2, snapshotDataStorage.get(peer1SnapPath));
        assertNull(snapshotDataStorage.get(peer2SnapPath));

        assertNotNull(snapshotMetaStorage.get(peer0SnapPath.toString()));
        assertNotNull(snapshotMetaStorage.get(peer1SnapPath.toString()));
        assertNull(snapshotMetaStorage.get(peer2SnapPath.toString()));

        for (int i = 0; i < 3; i++) {
            int finalI = i;
            startServer(i, raftServer -> {
                String localNodeName = raftServer.clusterService().topologyService().localMember().name();

                Peer serverPeer = initialMembersConf.peer(localNodeName);

                var counter = counters.get(serverPeer);

                counter.set(0);

                var listener = new UpdateCountRaftListener(counter, snapshotDataStorage);

                RaftGroupOptions opts = groupOptions(raftServer)
                        .snapshotStorageFactory(new SnapshotInMemoryStorageFactory(snapshotMetaStorage))
                        .setLogStorageFactory(logStorageFactories.get(finalI))
                        .serverDataPath(serverWorkingDirs.get(finalI).metaPath());

                raftServer.startRaftNode(new RaftNodeId(grpId, serverPeer), initialMembersConf, listener, opts);
            }, opts -> {});
        }

        for (AtomicInteger counter : counters.values()) {
            assertTrue(waitForCondition(() -> counter.get() == 3, 10_000));
        }
    }

    /**
     * Builds a snapshot path by the peer address of RAFT node.
     */
    private Path snapshotPath(Peer peer, ReplicationGroupId groupId) {
        JraftServerImpl server = servers.stream()
                .filter(s -> s.localPeers(groupId).contains(peer))
                .findAny()
                .orElseThrow();

        int serverIdx = servers.indexOf(server);

        return JraftServerImpl.getServerDataPath(
                serverWorkingDirs.get(serverIdx).metaPath().get(),
                new RaftNodeId(groupId, peer)
        ).resolve("snapshot");
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
        return getRaftNodes(grpId)
                .anyMatch(node -> {
                    var fsm = (StateMachineAdapter) node.getOptions().getFsm();

                    return node.isLeader() && fsm.getLeaderTerm() == node.getCurrentTerm();
                });
    }

    private Stream<NodeImpl> getRaftNodes(TestReplicationGroupId grpId) {
        return servers.stream()
                .flatMap(s -> s.localPeers(grpId).stream()
                        .map(peer -> new RaftNodeId(grpId, peer))
                        .map(s::raftGroupService))
                .map(s -> ((NodeImpl) s.getRaftNode()));
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
            List<Peer> peers = server.localPeers(COUNTER_GROUP_0);

            if (!peers.contains(leader1) && !peers.contains(leader2)) {
                toStop = server;
                break;
            }
        }

        var raftNodeId0 = new RaftNodeId(COUNTER_GROUP_0, toStop.localPeers(COUNTER_GROUP_0).get(0));
        var raftNodeId1 = new RaftNodeId(COUNTER_GROUP_1, toStop.localPeers(COUNTER_GROUP_1).get(0));

        int stopIdx = servers.indexOf(toStop);

        Path basePath = serverWorkingDirs.get(stopIdx).metaPath().get();

        Path serverDataPath0 = JraftServerImpl.getServerDataPath(basePath, raftNodeId0);
        Path serverDataPath1 = JraftServerImpl.getServerDataPath(basePath, raftNodeId1);

        toStop.stopRaftNode(raftNodeId0);
        toStop.stopRaftNode(raftNodeId1);

        toStop.beforeNodeStop();

        ComponentContext componentContext = new ComponentContext();

        assertThat(toStop.stopAsync(componentContext), willCompleteSuccessfully());
        assertThat(serverServices.get(stopIdx).stopAsync(componentContext), willCompleteSuccessfully());
        assertThat(logStorageFactories.get(stopIdx).stopAsync(componentContext), willCompleteSuccessfully());
        servers.remove(stopIdx);
        serverServices.remove(stopIdx);
        logStorageFactories.remove(stopIdx);
        serverWorkingDirs.remove(stopIdx);

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
            String localNodeName = r.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            r.startRaftNode(
                    new RaftNodeId(COUNTER_GROUP_0, serverPeer),
                    initialMembersConf,
                    listenerFactory.get(),
                    groupOptions(r)
                            .setLogStorageFactory(logStorageFactories.get(stopIdx))
                            .serverDataPath(serverWorkingDirs.get(stopIdx).metaPath())
            );
            r.startRaftNode(
                    new RaftNodeId(COUNTER_GROUP_1, serverPeer),
                    initialMembersConf,
                    listenerFactory.get(),
                    groupOptions(r)
                            .setLogStorageFactory(logStorageFactories.get(stopIdx))
                            .serverDataPath(serverWorkingDirs.get(stopIdx).metaPath())
            );
        }, opts -> {});

        waitForCondition(() -> validateStateMachine(sum(20), svc2, COUNTER_GROUP_0), 5_000);
        waitForCondition(() -> validateStateMachine(sum(30), svc2, COUNTER_GROUP_1), 5_000);

        svc2.stopRaftNodes(COUNTER_GROUP_0);
        svc2.stopRaftNodes(COUNTER_GROUP_1);

        svc2.beforeNodeStop();

        int sv2Idx = servers.size() - 1;
        assertThat(svc2.stopAsync(componentContext), willCompleteSuccessfully());
        assertThat(serverServices.get(sv2Idx).stopAsync(componentContext), willCompleteSuccessfully());
        assertThat(logStorageFactories.get(sv2Idx).stopAsync(componentContext), willCompleteSuccessfully());
        servers.remove(sv2Idx);
        serverServices.remove(sv2Idx);
        logStorageFactories.remove(sv2Idx);
        serverWorkingDirs.remove(sv2Idx);

        var svc3 = startServer(stopIdx, r -> {
            String localNodeName = r.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            r.startRaftNode(
                    new RaftNodeId(COUNTER_GROUP_0, serverPeer),
                    initialMembersConf,
                    listenerFactory.get(),
                    groupOptions(r)
                            .setLogStorageFactory(logStorageFactories.get(stopIdx))
                            .serverDataPath(serverWorkingDirs.get(stopIdx).metaPath())
            );
            r.startRaftNode(
                    new RaftNodeId(COUNTER_GROUP_1, serverPeer),
                    initialMembersConf,
                    listenerFactory.get(),
                    groupOptions(r)
                            .setLogStorageFactory(logStorageFactories.get(stopIdx))
                            .serverDataPath(serverWorkingDirs.get(stopIdx).metaPath())
            );
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
    private long applyIncrements(RaftGroupService client, int start, int stop) throws Exception {
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
        Peer serverPeer = server.localPeers(groupId).get(0);

        org.apache.ignite.raft.jraft.RaftGroupService svc = server.raftGroupService(new RaftNodeId(groupId, serverPeer));

        var fsm0 = (JraftServerImpl.DelegatingStateMachine) svc.getRaftNode().getOptions().getFsm();

        return expected == ((CounterListener) fsm0.getListener()).value();
    }

    @Test
    public void testReadIndex() throws Exception {
        startCluster();
        long index = clients.get(0).readIndex().join();
        clients.get(0).<Long>run(incrementAndGetCommand(1)).get();
        assertEquals(index + 1, clients.get(0).readIndex().join());
    }

    private static RaftGroupOptions groupOptions(RaftServer raftServer) {
        return defaults().commandsMarshaller(new ThreadLocalOptimizedMarshaller(raftServer.clusterService().serializationRegistry()));
    }
}
