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

package org.apache.ignite.internal.metastorage.impl;

import static java.util.concurrent.CompletableFuture.allOf;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.notExists;
import static org.apache.ignite.internal.metastorage.dsl.Conditions.revision;
import static org.apache.ignite.internal.metastorage.dsl.Operations.noop;
import static org.apache.ignite.internal.metastorage.dsl.Operations.put;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willBe;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ignite.internal.cluster.management.ClusterManagementGroupManager;
import org.apache.ignite.internal.cluster.management.configuration.ClusterManagementConfiguration;
import org.apache.ignite.internal.cluster.management.configuration.NodeAttributesConfiguration;
import org.apache.ignite.internal.cluster.management.raft.ClusterStateStorage;
import org.apache.ignite.internal.cluster.management.raft.TestClusterStateStorage;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyImpl;
import org.apache.ignite.internal.cluster.management.topology.LogicalTopologyServiceImpl;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.configuration.validation.TestConfigurationValidator;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.manager.IgniteComponent;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.EntryEvent;
import org.apache.ignite.internal.metastorage.WatchEvent;
import org.apache.ignite.internal.metastorage.WatchListener;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.raft.MetastorageGroupId;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.raft.Loza;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.vault.VaultManager;
import org.apache.ignite.internal.vault.inmemory.InMemoryVaultService;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for scenarios when Meta Storage nodes join and leave a cluster.
 */
@ExtendWith(ConfigurationExtension.class)
public abstract class ItMetaStorageMultipleNodesAbstractTest extends IgniteAbstractTest {
    @InjectConfiguration
    private static RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private static ClusterManagementConfiguration cmgConfiguration;

    @InjectConfiguration
    private static NodeAttributesConfiguration nodeAttributes;

    public abstract KeyValueStorage createStorage(String nodeName, Path path);

    private class Node {
        private final VaultManager vaultManager;

        private final ClusterService clusterService;

        private final Loza raftManager;

        private final ClusterStateStorage clusterStateStorage = new TestClusterStateStorage();

        private final ClusterManagementGroupManager cmgManager;

        private final MetaStorageManagerImpl metaStorageManager;

        /** The future have to be complete after the node start and all Meta storage watches are deployd. */
        private final CompletableFuture<Void> deployWatchesFut;

        Node(ClusterService clusterService, Path dataPath) {
            this.clusterService = clusterService;

            this.vaultManager = new VaultManager(new InMemoryVaultService());

            Path basePath = dataPath.resolve(name());

            HybridClock clock = new HybridClockImpl();
            this.raftManager = new Loza(
                    clusterService,
                    raftConfiguration,
                    basePath.resolve("raft"),
                    clock
            );

            var logicalTopology = new LogicalTopologyImpl(clusterStateStorage);

            this.cmgManager = new ClusterManagementGroupManager(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    logicalTopology,
                    cmgConfiguration,
                    nodeAttributes,
                    new TestConfigurationValidator());

            this.metaStorageManager = new MetaStorageManagerImpl(
                    vaultManager,
                    clusterService,
                    cmgManager,
                    new LogicalTopologyServiceImpl(logicalTopology, cmgManager),
                    raftManager,
                    createStorage(name(), basePath),
                    clock
            );

            deployWatchesFut = metaStorageManager.deployWatches();
        }

        void start() throws NodeStoppingException {
            List<IgniteComponent> components = List.of(
                    vaultManager,
                    clusterService,
                    raftManager,
                    clusterStateStorage,
                    cmgManager,
                    metaStorageManager
            );

            components.forEach(IgniteComponent::start);
        }

        /**
         * Waits for watches deployed.
         */
        void waitWatches() {
            assertThat("Watches were not deployed", deployWatchesFut, willCompleteSuccessfully());
        }

        String name() {
            return clusterService.nodeName();
        }

        void stop() throws Exception {
            List<IgniteComponent> components = List.of(
                    metaStorageManager,
                    cmgManager,
                    raftManager,
                    clusterStateStorage,
                    clusterService,
                    vaultManager
            );

            Stream<AutoCloseable> beforeNodeStop = components.stream().map(c -> c::beforeNodeStop);

            Stream<AutoCloseable> nodeStop = components.stream().map(c -> c::stop);

            IgniteUtils.closeAll(Stream.concat(beforeNodeStop, nodeStop));
        }

        CompletableFuture<Set<String>> getMetaStorageLearners() {
            return metaStorageManager
                    .metaStorageServiceFuture()
                    .thenApply(MetaStorageServiceImpl::raftGroupService)
                    .thenCompose(service -> service.refreshMembers(false).thenApply(v -> service.learners()))
                    .thenApply(learners -> learners.stream().map(Peer::consistentId).collect(toSet()));
        }
    }

    private final List<Node> nodes = new ArrayList<>();

    private Node startNode(TestInfo testInfo) throws NodeStoppingException {
        var nodeFinder = new StaticNodeFinder(List.of(new NetworkAddress("localhost", 10_000)));

        ClusterService clusterService = ClusterServiceTestUtils.clusterService(testInfo, 10_000 + nodes.size(), nodeFinder);

        var node = new Node(clusterService, workDir);

        node.start();

        nodes.add(node);

        return node;
    }

    @AfterEach
    void tearDown() throws Exception {
        IgniteUtils.closeAll(nodes.stream().map(node -> node::stop));
    }

    /**
     * Tests that an incoming node gets registered as a Learner and receives Meta Storage updates.
     */
    @Test
    void testLearnerJoin(TestInfo testInfo) throws NodeStoppingException {
        Node firstNode = startNode(testInfo);

        firstNode.cmgManager.initCluster(List.of(firstNode.name()), List.of(firstNode.name()), "test");

        firstNode.waitWatches();

        var key = new ByteArray("foo");
        byte[] value = "bar".getBytes(StandardCharsets.UTF_8);

        CompletableFuture<Boolean> invokeFuture = firstNode.metaStorageManager.invoke(notExists(key), put(key, value), noop());

        assertThat(invokeFuture, willBe(true));

        Node secondNode = startNode(testInfo);

        secondNode.waitWatches();

        // Check that reading remote data works correctly.
        assertThat(secondNode.metaStorageManager.get(key).thenApply(Entry::value), willBe(value));

        // Check that the new node will receive events.
        var awaitFuture = new CompletableFuture<EntryEvent>();

        secondNode.metaStorageManager.registerExactWatch(key, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                // Skip the first update event, because it's not guaranteed to arrive here (insert may have happened before the watch was
                // registered).
                if (event.revision() != 1) {
                    awaitFuture.complete(event.entryEvent());
                }

                return completedFuture(null);
            }

            @Override
            public void onError(Throwable e) {
                awaitFuture.completeExceptionally(e);
            }
        });

        byte[] newValue = "baz".getBytes(StandardCharsets.UTF_8);

        invokeFuture = firstNode.metaStorageManager.invoke(revision(key).eq(1), put(key, newValue), noop());

        assertThat(invokeFuture, willBe(true));

        var expectedEntryEvent = new EntryEvent(
                new EntryImpl(key.bytes(), value, 1, 1),
                new EntryImpl(key.bytes(), newValue, 2, 2)
        );

        assertThat(awaitFuture, willBe(expectedEntryEvent));

        // Check that the second node has been registered as a learner.
        assertThat(firstNode.getMetaStorageLearners(), willBe(Set.of(secondNode.name())));
    }

    /**
     * Tests a case when a node leaves the physical topology without entering the logical topology.
     */
    @Test
    void testLearnerLeavePhysicalTopology(TestInfo testInfo) throws Exception {
        Node firstNode = startNode(testInfo);
        Node secondNode = startNode(testInfo);

        firstNode.cmgManager.initCluster(List.of(firstNode.name()), List.of(firstNode.name()), "test");

        firstNode.waitWatches();
        secondNode.waitWatches();

        // Try reading some data to make sure that Raft has been configured correctly.
        assertThat(secondNode.metaStorageManager.get(new ByteArray("test")).thenApply(Entry::value), willBe(nullValue()));

        // Check that the second node has been registered as a learner.
        assertTrue(waitForCondition(() -> firstNode.getMetaStorageLearners().join().equals(Set.of(secondNode.name())), 10_000));

        // Stop the second node.
        secondNode.stop();

        nodes.remove(1);

        assertTrue(waitForCondition(() -> firstNode.getMetaStorageLearners().join().isEmpty(), 10_000));
    }

    /**
     * Tests a case when a node leaves the physical topology without entering the logical topology.
     */
    @Test
    void testLearnerLeaveLogicalTopology(TestInfo testInfo) throws Exception {
        Node firstNode = startNode(testInfo);
        Node secondNode = startNode(testInfo);

        firstNode.cmgManager.initCluster(List.of(firstNode.name()), List.of(firstNode.name()), "test");

        assertThat(allOf(firstNode.cmgManager.onJoinReady(), secondNode.cmgManager.onJoinReady()), willCompleteSuccessfully());

        firstNode.waitWatches();
        secondNode.waitWatches();

        CompletableFuture<Set<String>> logicalTopologyNodes = firstNode.cmgManager
                .logicalTopology()
                .thenApply(logicalTopology -> logicalTopology.nodes().stream().map(ClusterNode::name).collect(toSet()));

        assertThat(logicalTopologyNodes, willBe(Set.of(firstNode.name(), secondNode.name())));

        // Try reading some data to make sure that Raft has been configured correctly.
        assertThat(secondNode.metaStorageManager.get(new ByteArray("test")).thenApply(Entry::value), willBe(nullValue()));

        // Check that the second node has been registered as a learner.
        assertTrue(waitForCondition(() -> firstNode.getMetaStorageLearners().join().equals(Set.of(secondNode.name())), 10_000));

        // Stop the second node.
        secondNode.stop();

        nodes.remove(1);

        assertTrue(waitForCondition(() -> firstNode.getMetaStorageLearners().join().isEmpty(), 10_000));
    }

    /**
     * Tests that safe time is propagated from the leader to the follower/learner.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSafeTimePropagation(boolean useFollower, TestInfo testInfo) throws Exception {
        Node firstNode = startNode(testInfo);
        Node secondNode = startNode(testInfo);

        List<String> followers = new ArrayList<>();
        followers.add(firstNode.name());

        if (useFollower) {
            followers.add(secondNode.name());
        }

        firstNode.cmgManager.initCluster(followers, List.of(firstNode.name()), "test");

        ClusterTimeImpl firstNodeTime = (ClusterTimeImpl) firstNode.metaStorageManager.clusterTime();
        ClusterTimeImpl secondNodeTime = (ClusterTimeImpl) secondNode.metaStorageManager.clusterTime();

        assertThat(allOf(firstNode.cmgManager.onJoinReady(), secondNode.cmgManager.onJoinReady()), willCompleteSuccessfully());

        firstNode.waitWatches();
        secondNode.waitWatches();

        CompletableFuture<Void> watchCompletedFuture = new CompletableFuture<>();
        CountDownLatch watchCalledLatch = new CountDownLatch(1);

        ByteArray testKey = ByteArray.fromString("test-key");

        // Register watch listener, so that we can control safe time propagation.
        // Safe time can only be propagated when all of the listeners completed their futures successfully.
        secondNode.metaStorageManager.registerExactWatch(testKey, new WatchListener() {
            @Override
            public CompletableFuture<Void> onUpdate(WatchEvent event) {
                watchCalledLatch.countDown();

                return watchCompletedFuture;
            }

            @Override
            public void onError(Throwable e) {
                // No-op.
            }
        });

        HybridTimestamp timeBeforeOp = firstNodeTime.currentSafeTime();

        // Try putting data from both nodes, because any of them can be a leader.
        assertThat(
                firstNode.metaStorageManager.put(testKey, new byte[]{0, 1, 2, 3}),
                willCompleteSuccessfully()
        );

        // Ensure watch listener is called.
        assertTrue(watchCalledLatch.await(1, TimeUnit.SECONDS));

        // Wait until leader's safe time is propagated.
        assertTrue(waitForCondition(() -> {
            return firstNodeTime.currentSafeTime().compareTo(timeBeforeOp) > 0;
        }, TimeUnit.SECONDS.toMillis(1)));

        // Safe time must not be propagated to the second node at this moment.
        assertThat(firstNodeTime.currentSafeTime(), greaterThan(secondNodeTime.currentSafeTime()));

        // Finish watch listener notification process.
        watchCompletedFuture.complete(null);

        // After that in the nearest future safe time must be propagated.
        assertTrue(waitForCondition(() -> {
            HybridTimestamp sf1 = firstNodeTime.currentSafeTime();
            HybridTimestamp sf2 = secondNodeTime.currentSafeTime();

            return sf1.equals(sf2);
        }, TimeUnit.SECONDS.toMillis(1)));

        assertThat(
                secondNode.metaStorageManager.put(ByteArray.fromString("test-key-2"), new byte[]{0, 1, 2, 3}),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(() -> {
            HybridTimestamp sf1 = firstNodeTime.currentSafeTime();
            HybridTimestamp sf2 = secondNodeTime.currentSafeTime();

            return sf1.equals(sf2);
        }, TimeUnit.SECONDS.toMillis(1)));
    }

    /**
     * Tests that safe time is propagated after leader was changed.
     */
    @Test
    void testSafeTimePropagationLeaderTransferred(TestInfo testInfo) throws Exception {
        Node firstNode = startNode(testInfo);
        Node secondNode = startNode(testInfo);

        List<String> followers = List.of(firstNode.name(), secondNode.name());

        firstNode.cmgManager.initCluster(followers, List.of(firstNode.name()), "test");

        ClusterTimeImpl firstNodeTime = (ClusterTimeImpl) firstNode.metaStorageManager.clusterTime();
        ClusterTimeImpl secondNodeTime = (ClusterTimeImpl) secondNode.metaStorageManager.clusterTime();

        assertThat(allOf(firstNode.cmgManager.onJoinReady(), secondNode.cmgManager.onJoinReady()), willCompleteSuccessfully());

        firstNode.waitWatches();
        secondNode.waitWatches();

        assertThat(
                firstNode.metaStorageManager.put(ByteArray.fromString("test-key"), new byte[]{0, 1, 2, 3}),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(() -> {
            HybridTimestamp sf1 = firstNodeTime.currentSafeTime();
            HybridTimestamp sf2 = secondNodeTime.currentSafeTime();

            return sf1.equals(sf2);
        }, TimeUnit.SECONDS.toMillis(1)));

        // Change leader and check if propagation still works.
        Node prevLeader = transferLeadership(firstNode, secondNode);

        assertThat(
                prevLeader.metaStorageManager.put(ByteArray.fromString("test-key-2"), new byte[]{0, 1, 2, 3}),
                willCompleteSuccessfully()
        );

        assertTrue(waitForCondition(() -> {
            HybridTimestamp sf1 = firstNodeTime.currentSafeTime();
            HybridTimestamp sf2 = secondNodeTime.currentSafeTime();

            return sf1.equals(sf2);
        }, TimeUnit.SECONDS.toMillis(1)));
    }

    private Node transferLeadership(Node firstNode, Node secondNode) {
        RaftGroupService svc1 = getMetastorageService(firstNode);
        RaftGroupService svc2 = getMetastorageService(secondNode);

        boolean leaderFirst = false;

        RaftGroupService leader;
        RaftGroupService notLeader;

        if (svc1.getRaftNode().isLeader()) {
            leader = svc1;
            notLeader = svc2;

            leaderFirst = true;
        } else {
            leader = svc2;
            notLeader = svc1;
        }

        leader.getRaftNode().transferLeadershipTo(notLeader.getServerId());

        return leaderFirst ? secondNode : firstNode;
    }

    private RaftGroupService getMetastorageService(Node node) {
        JraftServerImpl server1 = (JraftServerImpl) node.raftManager.server();

        RaftNodeId nodeId = server1.localNodes().stream()
                .filter(id -> MetastorageGroupId.INSTANCE.equals(id.groupId())).findFirst().get();

        return server1.raftGroupService(nodeId);
    }
}
