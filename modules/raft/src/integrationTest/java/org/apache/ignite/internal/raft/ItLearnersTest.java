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
import static org.apache.ignite.internal.raft.server.RaftGroupEventsListener.noopLsnr;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureExceptionMatcher.willThrow;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.will;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.closeAll;
import static org.apache.ignite.utils.ClusterServiceTestUtils.clusterService;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.TestWriteCommand;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.client.service.RaftGroupService;
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

            Path raftDir = workDir.resolve(clusterService.localConfiguration().getName());

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
                    loza == null ? null : () -> loza.stopRaftGroup(RAFT_GROUP_ID),
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
    public void testReadWriteLearners() throws Exception {
        List<TestRaftGroupListener> listeners = IntStream.range(0, nodes.size())
                .mapToObj(i -> new TestRaftGroupListener())
                .collect(toList());

        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        List<CompletableFuture<RaftGroupService>> services = IntStream.range(0, nodes.size())
                .mapToObj(i -> startRaftGroup(nodes.get(i), listeners.get(i), List.of(follower), learners))
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
    public void testAddLearners() {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        CompletableFuture<RaftGroupService> service1 =
                startRaftGroup(follower, new TestRaftGroupListener(), List.of(follower), List.of());

        assertThat(service1.thenApply(RaftGroupService::leader), willBe(follower.asPeer()));
        assertThat(service1.thenApply(RaftGroupService::learners), willBe(empty()));

        CompletableFuture<Void> addLearners = service1
                .thenCompose(s -> s.addLearners(Arrays.asList(toPeerArray(learners))));

        assertThat(addLearners, willCompleteSuccessfully());

        CompletableFuture<RaftGroupService> service2 =
                startRaftGroup(nodes.get(1), new TestRaftGroupListener(), List.of(follower), learners);

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
    public void testLostLeadership() throws Exception {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        List<CompletableFuture<RaftGroupService>> services = nodes.stream()
                .map(node -> startRaftGroup(node, new TestRaftGroupListener(), List.of(follower), learners))
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
    public void testLostLearners() throws Exception {
        RaftNode follower = nodes.get(0);
        List<RaftNode> learners = nodes.subList(1, nodes.size());

        List<CompletableFuture<RaftGroupService>> services = nodes.stream()
                .map(node -> startRaftGroup(node, new TestRaftGroupListener(), List.of(follower), learners))
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

    private CompletableFuture<RaftGroupService> startRaftGroup(
            RaftNode raftNode,
            RaftGroupListener listener,
            List<RaftNode> peers,
            List<RaftNode> learners
    ) {
        try {
            CompletableFuture<RaftGroupService> future = raftNode.loza.prepareRaftGroup(
                    RAFT_GROUP_ID,
                    peers.stream().map(RaftNode::consistentId).collect(toList()),
                    learners.stream().map(RaftNode::consistentId).collect(toList()),
                    () -> listener,
                    () -> noopLsnr,
                    RaftGroupOptions.defaults()
            );

            return future.thenApply(s -> {
                // Decrease the default timeout to make tests faster.
                s.timeout(100);

                return s;
            });
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
