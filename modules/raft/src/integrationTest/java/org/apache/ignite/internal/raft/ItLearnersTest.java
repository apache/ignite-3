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

package org.apache.ignite.internal.raft;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.lang.NodeStoppingException;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.service.CommandClosure;
import org.apache.ignite.internal.raft.service.RaftGroupListener;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.messages.TestRaftMessagesFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for the Raft Learner functionality.
 */
@ExtendWith(ConfigurationExtension.class)
public class ItLearnersTest extends IgniteAbstractTest {
    private static final ReplicationGroupId RAFT_GROUP_ID = new ReplicationGroupId() {
        @Override
        public String toString() {
            return "test";
        }
    };

    private static final TestRaftMessagesFactory MESSAGES_FACTORY = new TestRaftMessagesFactory();

    private static TestWriteCommand createWriteCommand(String value) {
        return MESSAGES_FACTORY.testWriteCommand().value(value).build();
    }

    private static final List<NetworkAddress> ADDRS = List.of(
            new NetworkAddress("localhost", 5000),
            new NetworkAddress("localhost", 5001),
            new NetworkAddress("localhost", 5002)
    );

    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    private final List<RaftNode> nodes = new ArrayList<>(ADDRS.size());

    /** Mock Raft node. */
    private class RaftNode implements AutoCloseable {
        final ClusterService clusterService;

        final Loza loza;

        RaftNode(ClusterService clusterService) {
            this.clusterService = clusterService;

            Path raftDir = workDir.resolve(clusterService.nodeName());

            loza = new Loza(clusterService, raftConfiguration, raftDir, new HybridClockImpl());
        }

        String consistentId() {
            return clusterService.topologyService().localMember().name();
        }

        Peer asPeer() {
            return new Peer(consistentId());
        }

        void start() {
            clusterService.start();
            loza.start();
        }

        @Override
        public void close() throws Exception {
            closeAll(
                    loza == null ? null : () -> loza.stopRaftNodes(RAFT_GROUP_ID),
                    loza == null ? null : loza::beforeNodeStop,
                    clusterService == null ? null : clusterService::beforeNodeStop,
                    loza == null ? null : loza::stop,
                    clusterService == null ? null : clusterService::stop
            );
        }
    }

    @BeforeEach
    void setUp(TestInfo testInfo) {
        var nodeFinder = new StaticNodeFinder(ADDRS);

        ADDRS.stream()
                .map(addr -> clusterService(testInfo, addr.port(), nodeFinder))
                .map(RaftNode::new)
                .forEach(nodes::add);

        nodes.parallelStream().forEach(RaftNode::start);
    }

    @AfterEach
    void tearDown() throws Exception {
        closeAll(nodes);
    }

    /**
     * Tests that it is possible to replicate and read data from learners.
     */
    @Test
    void testReadWriteLearners() throws Exception {
        List<TestRaftGroupListener> listeners = IntStream.range(0, nodes.size())
                .mapToObj(i -> new TestRaftGroupListener())
                .collect(toList());

        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        PeersAndLearners configuration = createConfiguration(List.of(follower), learners);

        List<Peer> serverPeers = nodesToPeers(configuration, List.of(follower), learners);

        List<CompletableFuture<RaftGroupService>> services = IntStream.range(0, nodes.size())
                .mapToObj(i -> startRaftGroup(nodes.get(i), serverPeers.get(i), configuration, listeners.get(i)))
                .collect(toList());

        // Check that learners and peers have been set correctly.
        services.forEach(service -> {
            CompletableFuture<RaftGroupService> refreshMembers = service
                    .thenCompose(s -> s.refreshMembers(true).thenApply(v -> s));

            assertThat(refreshMembers.thenApply(RaftGroupService::leader), willBe(follower.asPeer()));
            assertThat(refreshMembers.thenApply(RaftGroupService::peers), will(contains(follower.asPeer())));
            assertThat(refreshMembers.thenApply(RaftGroupService::learners), will(containsInAnyOrder(toPeerArray(learners))));
        });

        listeners.forEach(listener -> assertThat(listener.storage, is(empty())));

        // Test writing data.
        CompletableFuture<?> writeFuture = services.get(0)
                .thenCompose(s -> s.run(createWriteCommand("foo")).thenApply(v -> s))
                .thenCompose(s -> s.run(createWriteCommand("bar")));

        assertThat(writeFuture, willCompleteSuccessfully());

        for (TestRaftGroupListener listener : listeners) {
            assertThat(listener.storage.poll(1, TimeUnit.SECONDS), is("foo"));
            assertThat(listener.storage.poll(1, TimeUnit.SECONDS), is("bar"));
        }
    }

    /**
     * Tests {@link RaftGroupService#addLearners} functionality.
     */
    @Test
    void testAddLearners() {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        PeersAndLearners configuration = createConfiguration(List.of(follower), List.of());

        CompletableFuture<RaftGroupService> service1 =
                startRaftGroup(follower, configuration.peer(follower.consistentId()), configuration, new TestRaftGroupListener());

        assertThat(service1.thenApply(RaftGroupService::leader), willBe(follower.asPeer()));
        assertThat(service1.thenApply(RaftGroupService::learners), willBe(empty()));

        CompletableFuture<Void> addLearners = service1
                .thenCompose(s -> s.addLearners(Arrays.asList(toPeerArray(learners))));

        assertThat(addLearners, willCompleteSuccessfully());

        PeersAndLearners newConfiguration = createConfiguration(List.of(follower), learners);

        RaftNode learner1 = nodes.get(1);

        CompletableFuture<RaftGroupService> service2 =
                startRaftGroup(learner1, newConfiguration.learner(learner1.consistentId()), newConfiguration, new TestRaftGroupListener());

        // Check that learners and peers have been set correctly.
        Stream.of(service1, service2).forEach(service -> {
            CompletableFuture<RaftGroupService> refreshMembers = service
                    .thenCompose(s -> s.refreshMembers(true).thenApply(v -> s));

            assertThat(refreshMembers.thenApply(RaftGroupService::leader), willBe(follower.asPeer()));
            assertThat(refreshMembers.thenApply(RaftGroupService::peers), will(contains(follower.asPeer())));
            assertThat(refreshMembers.thenApply(RaftGroupService::learners), will(containsInAnyOrder(toPeerArray(learners))));
        });
    }

    /**
     * Tests that if the only follower is stopped, then the majority is lost.
     */
    @Test
    void testLostLeadership() throws Exception {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        PeersAndLearners configuration = createConfiguration(List.of(follower), learners);

        List<Peer> serverPeers = nodesToPeers(configuration, List.of(follower), learners);

        List<CompletableFuture<RaftGroupService>> services = IntStream.range(0, nodes.size())
                .mapToObj(i -> startRaftGroup(nodes.get(i), serverPeers.get(i), configuration, new TestRaftGroupListener()))
                .collect(toList());

        // Wait for the leader to be elected.
        services.forEach(service -> assertThat(
                service.thenCompose(s -> s.refreshLeader().thenApply(v -> s.leader())),
                willBe(follower.asPeer()))
        );

        nodes.set(0, null).close();

        assertThat(services.get(1).thenCompose(s -> s.run(createWriteCommand("foo"))), willThrow(TimeoutException.class));
    }

    /**
     * Tests that even if all learners are stopped, then the majority is not lost.
     */
    @Test
    void testLostLearners() throws Exception {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        PeersAndLearners configuration = createConfiguration(List.of(follower), learners);

        List<Peer> serverPeers = nodesToPeers(configuration, List.of(follower), learners);

        List<CompletableFuture<RaftGroupService>> services = IntStream.range(0, nodes.size())
                .mapToObj(i -> startRaftGroup(nodes.get(i), serverPeers.get(i), configuration, new TestRaftGroupListener()))
                .collect(toList());

        // Wait for the leader to be elected.
        services.forEach(service -> assertThat(
                service.thenCompose(s -> s.refreshLeader().thenApply(v -> s.leader())),
                willBe(follower.asPeer()))
        );

        nodes.set(1, null).close();
        nodes.set(2, null).close();

        assertThat(services.get(0).thenCompose(RaftGroupService::refreshLeader), willCompleteSuccessfully());
    }

    /**
     * Tests a situation when a peer and a learner are started on the same node.
     */
    @Test
    void testLearnersOnTheSameNodeAsPeers() throws InterruptedException {
        RaftNode node = nodes.get(0);

        PeersAndLearners configuration = createConfiguration(List.of(node), List.of(node));

        var peerListener = new TestRaftGroupListener();
        var learnerListener = new TestRaftGroupListener();

        Peer peer = configuration.peer(node.consistentId());
        Peer learner = configuration.learner(node.consistentId());

        CompletableFuture<RaftGroupService> peerService = startRaftGroup(node, peer, configuration, peerListener);
        CompletableFuture<RaftGroupService> learnerService = startRaftGroup(node, learner, configuration, learnerListener);

        assertThat(peerService.thenApply(RaftGroupService::leader), willBe(peer));
        assertThat(peerService.thenApply(RaftGroupService::leader), willBe(not(learner)));
        assertThat(learnerService.thenApply(RaftGroupService::leader), willBe(peer));
        assertThat(learnerService.thenApply(RaftGroupService::leader), willBe(not(learner)));

        // Test writing data.
        CompletableFuture<?> writeFuture = peerService
                .thenCompose(s -> s.run(createWriteCommand("foo")).thenApply(v -> s))
                .thenCompose(s -> s.run(createWriteCommand("bar")));

        assertThat(writeFuture, willCompleteSuccessfully());

        for (TestRaftGroupListener listener : Arrays.asList(peerListener, learnerListener)) {
            assertThat(listener.storage.poll(1, TimeUnit.SECONDS), is("foo"));
            assertThat(listener.storage.poll(1, TimeUnit.SECONDS), is("bar"));
        }
    }

    /**
     * Tests adding a new learner using {@link RaftGroupService#changePeersAsync} to an Ignite node that is already running a Raft peer.
     */
    @Test
    void testChangePeersToAddLearnerToSameNodeAsPeer() throws InterruptedException {
        List<RaftNode> followers = nodes.subList(0, 2);
        RaftNode learner = nodes.get(0);

        PeersAndLearners configuration = createConfiguration(followers, List.of(learner));

        CompletableFuture<?>[] followerServices = followers.stream()
                .map(node -> startRaftGroup(node, configuration.peer(node.consistentId()), configuration, new TestRaftGroupListener()))
                .toArray(CompletableFuture[]::new);

        assertThat(CompletableFuture.allOf(followerServices), willCompleteSuccessfully());

        var learnerListener = new TestRaftGroupListener();

        CompletableFuture<RaftGroupService> learnerService = startRaftGroup(
                learner, configuration.learner(learner.consistentId()), configuration, learnerListener
        );

        CompletableFuture<?> writeFuture = learnerService
                .thenCompose(s -> s.run(createWriteCommand("foo")).thenApply(v -> s))
                .thenCompose(s -> s.run(createWriteCommand("bar")));

        assertThat(writeFuture, willCompleteSuccessfully());
        assertThat(learnerListener.storage.poll(1, TimeUnit.SECONDS), is("foo"));
        assertThat(learnerListener.storage.poll(1, TimeUnit.SECONDS), is("bar"));

        // Create a new learner on the second node.
        RaftNode newLearner = nodes.get(1);

        PeersAndLearners newConfiguration = createConfiguration(followers, List.of(learner, newLearner));

        CompletableFuture<Void> changePeersFuture = learnerService.thenCompose(s -> s.refreshAndGetLeaderWithTerm()
                .thenCompose(leaderWithTerm -> s.changePeersAsync(newConfiguration, leaderWithTerm.term())
        ));

        assertThat(changePeersFuture, willCompleteSuccessfully());

        var newLearnerListener = new TestRaftGroupListener();

        CompletableFuture<RaftGroupService> newLearnerService = startRaftGroup(
                newLearner, newConfiguration.learner(newLearner.consistentId()), newConfiguration, newLearnerListener
        );

        assertThat(newLearnerService, willCompleteSuccessfully());
        assertThat(newLearnerListener.storage.poll(10, TimeUnit.SECONDS), is("foo"));
        assertThat(newLearnerListener.storage.poll(10, TimeUnit.SECONDS), is("bar"));
    }

    private PeersAndLearners createConfiguration(Collection<RaftNode> peers, Collection<RaftNode> learners) {
        return PeersAndLearners.fromConsistentIds(
                peers.stream().map(RaftNode::consistentId).collect(toSet()),
                learners.stream().map(RaftNode::consistentId).collect(toSet())
        );
    }

    private List<Peer> nodesToPeers(PeersAndLearners memberConfiguration, Collection<RaftNode> peers, Collection<RaftNode> learners) {
        return Stream.concat(
                peers.stream().map(peer -> memberConfiguration.peer(peer.consistentId())),
                learners.stream().map(learner -> memberConfiguration.learner(learner.consistentId()))
        ).collect(toList());
    }

    private CompletableFuture<RaftGroupService> startRaftGroup(
            RaftNode node,
            Peer serverPeer,
            PeersAndLearners memberConfiguration,
            RaftGroupListener listener
    ) {
        try {
            return node.loza.startRaftGroupNodeAndWaitNodeReadyFuture(
                    new RaftNodeId(RAFT_GROUP_ID, serverPeer),
                    memberConfiguration,
                    listener,
                    RaftGroupEventsListener.noopLsnr
            );
        } catch (NodeStoppingException e) {
            throw new RuntimeException(e);
        }
    }

    private static class TestRaftGroupListener implements RaftGroupListener {
        final BlockingQueue<String> storage = new LinkedBlockingQueue<>();

        @Override
        public void onWrite(Iterator<CommandClosure<WriteCommand>> iterator) {
            iterator.forEachRemaining(closure -> {
                assertThat(closure.command(), is(instanceOf(TestWriteCommand.class)));

                TestWriteCommand writeCommand = (TestWriteCommand) closure.command();

                if (!storage.contains(writeCommand.value())) {
                    storage.add(writeCommand.value());
                }

                closure.result(null);
            });
        }

        @Override
        public void onRead(Iterator<CommandClosure<ReadCommand>> iterator) {
        }

        @Override
        public void onSnapshotSave(Path path, Consumer<Throwable> doneClo) {
        }

        @Override
        public boolean onSnapshotLoad(Path path) {
            return true;
        }

        @Override
        public void onShutdown() {
        }
    }

    private static Peer[] toPeerArray(List<RaftNode> nodes) {
        return nodes.stream().map(RaftNode::asPeer).toArray(Peer[]::new);
    }
}
