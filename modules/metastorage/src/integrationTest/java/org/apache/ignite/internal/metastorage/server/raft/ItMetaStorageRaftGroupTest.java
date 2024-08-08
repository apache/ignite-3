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

package org.apache.ignite.internal.metastorage.server.raft;

import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.toSet;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.waitForTopology;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.startAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow.Subscriber;
import java.util.concurrent.Flow.Subscription;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.configuration.SystemLocalConfiguration;
import org.apache.ignite.internal.configuration.testframework.ConfigurationExtension;
import org.apache.ignite.internal.configuration.testframework.InjectConfiguration;
import org.apache.ignite.internal.lang.ByteArray;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.MetaStorageService;
import org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.metastorage.server.time.ClusterTime;
import org.apache.ignite.internal.metastorage.server.time.ClusterTimeImpl;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.configuration.RaftConfiguration;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.TestJraftServerFactory;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.testframework.IgniteAbstractTest;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteSpinBusyLock;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.RaftGroupEventsClientListener;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Meta storage client tests.
 */
@ExtendWith(MockitoExtension.class)
@ExtendWith(ConfigurationExtension.class)
public class ItMetaStorageRaftGroupTest extends IgniteAbstractTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItMetaStorageRaftGroupTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 3;

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY1 =
            new EntryImpl(
                    new byte[]{1},
                    new byte[]{2},
                    10,
                    2
            );

    /** Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY2 =
            new EntryImpl(
                    new byte[]{3},
                    new byte[]{4},
                    11,
                    3
            );

    /** Cluster. */
    private final ArrayList<ClusterService> cluster = new ArrayList<>();

    /** First meta storage raft server. */
    private RaftServer metaStorageRaftSrv1;

    /** Second meta storage raft server. */
    private RaftServer metaStorageRaftSrv2;

    /** Third meta storage raft server. */
    private RaftServer metaStorageRaftSrv3;

    /** First meta storage raft group service. */
    private RaftGroupService metaStorageRaftGrpSvc1;

    /** Second meta storage raft group service. */
    private RaftGroupService metaStorageRaftGrpSvc2;

    /** Third meta storage raft group service. */
    private RaftGroupService metaStorageRaftGrpSvc3;

    /** Mock Metastorage storage. */
    @Mock
    private KeyValueStorage mockStorage;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

    @InjectConfiguration
    private RaftConfiguration raftConfiguration;

    @InjectConfiguration
    private SystemLocalConfiguration systemConfiguration;

    /**
     * Run {@code NODES} cluster nodes.
     */
    @BeforeEach
    public void beforeTest(TestInfo testInfo) throws InterruptedException {
        List<NetworkAddress> localAddresses = findLocalAddresses(NODE_PORT_BASE, NODE_PORT_BASE + NODES);

        var nodeFinder = new StaticNodeFinder(localAddresses);

        localAddresses.stream()
                .map(addr -> ClusterServiceTestUtils.clusterService(testInfo, addr.port(), nodeFinder))
                .forEach(clusterService -> {
                    assertThat(clusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());
                    cluster.add(clusterService);
                });

        for (ClusterService node : cluster) {
            assertTrue(waitForTopology(node, NODES, 1000));
        }

        LOG.info("Cluster started.");

        executor = new ScheduledThreadPoolExecutor(20, new NamedThreadFactory("Raft-Group-Client", LOG));
    }

    /**
     * Shutdown raft server and stop all cluster nodes.
     *
     */
    @AfterEach
    public void afterTest() {
        ComponentContext componentContext = new ComponentContext();

        if (metaStorageRaftSrv3 != null) {
            metaStorageRaftSrv3.stopRaftNodes(MetastorageGroupId.INSTANCE);
            assertThat(metaStorageRaftSrv3.stopAsync(componentContext), willCompleteSuccessfully());
            metaStorageRaftGrpSvc3.shutdown();
        }

        if (metaStorageRaftSrv2 != null) {
            metaStorageRaftSrv2.stopRaftNodes(MetastorageGroupId.INSTANCE);
            assertThat(metaStorageRaftSrv2.stopAsync(componentContext), willCompleteSuccessfully());
            metaStorageRaftGrpSvc2.shutdown();
        }

        if (metaStorageRaftSrv1 != null) {
            metaStorageRaftSrv1.stopRaftNodes(MetastorageGroupId.INSTANCE);
            assertThat(metaStorageRaftSrv1.stopAsync(componentContext), willCompleteSuccessfully());
            metaStorageRaftGrpSvc1.shutdown();
        }

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        for (ClusterService node : cluster) {
            assertThat(node.stopAsync(componentContext), willCompleteSuccessfully());
        }
    }


    /**
     * Tests that {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next command works correctly after leader changing.
     *
     * @throws Exception If failed.
     */
    @Test
    @Disabled("https://issues.apache.org/jira/browse/IGNITE-22891")
    public void testRangeNextWorksCorrectlyAfterLeaderChange() throws Exception {
        AtomicInteger replicatorStartedCounter = new AtomicInteger(0);

        AtomicInteger replicatorStoppedCounter = new AtomicInteger(0);

        when(mockStorage.range(EXPECTED_RESULT_ENTRY1.key(), new byte[]{4})).thenAnswer(invocation -> {
            List<Entry> entries = List.of(EXPECTED_RESULT_ENTRY1, EXPECTED_RESULT_ENTRY2);

            return Cursor.fromBareIterator(entries.iterator());
        });

        List<Pair<RaftServer, RaftGroupService>> raftServersRaftGroups = prepareJraftMetaStorages(replicatorStartedCounter,
                replicatorStoppedCounter);

        List<RaftServer> raftServers = raftServersRaftGroups.stream().map(p -> p.key).collect(Collectors.toList());

        String oldLeaderId = raftServersRaftGroups.get(0).value.leader().consistentId();

        RaftServer oldLeaderServer = raftServers.stream()
                .filter(s -> localMemberName(s.clusterService()).equals(oldLeaderId))
                .findFirst()
                .orElseThrow();

        // Server that will be alive after we stop leader.
        RaftServer liveServer = raftServers.stream()
                .filter(s -> !localMemberName(s.clusterService()).equals(oldLeaderId))
                .findFirst()
                .orElseThrow();

        RaftGroupService raftGroupServiceOfLiveServer = raftServersRaftGroups.stream()
                .filter(p -> p.key.equals(liveServer))
                .findFirst()
                .orElseThrow()
                .value;

        MetaStorageService metaStorageSvc = new MetaStorageServiceImpl(
                liveServer.clusterService().nodeName(),
                raftGroupServiceOfLiveServer,
                new IgniteSpinBusyLock(),
                mock(ClusterTime.class),
                () -> liveServer.clusterService().topologyService().localMember().id());

        var resultFuture = new CompletableFuture<Void>();

        metaStorageSvc.range(new ByteArray(EXPECTED_RESULT_ENTRY1.key()), new ByteArray(new byte[]{4}))
                .subscribe(new Subscriber<>() {
                    private Subscription subscription;

                    private int state = 0;

                    @Override
                    public void onSubscribe(Subscription subscription) {
                        this.subscription = subscription;

                        try {
                            assertTrue(
                                    waitForCondition(() -> replicatorStartedCounter.get() == 2, 5_000),
                                    String.valueOf(replicatorStartedCounter.get())
                            );

                            subscription.request(1);
                        } catch (InterruptedException e) {
                            resultFuture.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void onNext(Entry item) {
                        try {
                            if (state == 0) {
                                assertEquals(EXPECTED_RESULT_ENTRY1, item);

                                // Ensure that leader has not been changed.
                                // In a stable topology unexpected leader election shouldn't happen.
                                assertTrue(
                                        waitForCondition(() -> replicatorStartedCounter.get() == 2, 5_000),
                                        String.valueOf(replicatorStartedCounter.get())
                                );

                                // stop leader
                                oldLeaderServer.stopRaftNodes(MetastorageGroupId.INSTANCE);
                                ComponentContext componentContext = new ComponentContext();

                                assertThat(oldLeaderServer.stopAsync(componentContext), willCompleteSuccessfully());
                                CompletableFuture<Void> stopFuture = cluster.stream()
                                        .filter(c -> localMemberName(c).equals(oldLeaderId))
                                        .findFirst()
                                        .orElseThrow()
                                        .stopAsync(componentContext);
                                assertThat(stopFuture, willCompleteSuccessfully());

                                raftGroupServiceOfLiveServer.refreshLeader().get();

                                assertNotSame(oldLeaderId, raftGroupServiceOfLiveServer.leader().consistentId());

                                // ensure that leader has been changed only once
                                assertTrue(
                                        waitForCondition(() -> replicatorStartedCounter.get() == 4, 5_000),
                                        String.valueOf(replicatorStartedCounter.get())
                                );
                                assertTrue(
                                        waitForCondition(() -> replicatorStoppedCounter.get() == 2, 5_000),
                                        String.valueOf(replicatorStoppedCounter.get())
                                );

                            } else if (state == 1) {
                                assertEquals(EXPECTED_RESULT_ENTRY2, item);
                            }

                            state++;

                            subscription.request(1);
                        } catch (Exception e) {
                            resultFuture.completeExceptionally(e);
                        }
                    }

                    @Override
                    public void onError(Throwable throwable) {
                        resultFuture.completeExceptionally(throwable);
                    }

                    @Override
                    public void onComplete() {
                        resultFuture.complete(null);
                    }
                });

        assertThat(resultFuture, willCompleteSuccessfully());
    }

    private List<Pair<RaftServer, RaftGroupService>> prepareJraftMetaStorages(AtomicInteger replicatorStartedCounter,
            AtomicInteger replicatorStoppedCounter) throws InterruptedException {
        PeersAndLearners membersConfiguration = cluster.stream()
                .map(ItMetaStorageRaftGroupTest::localMemberName)
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));

        assertTrue(cluster.size() > 1);

        var commandsMarshaller = new ThreadLocalOptimizedMarshaller(cluster.get(0).serializationRegistry());

        NodeOptions opt1 = new NodeOptions();
        opt1.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));
        opt1.setCommandsMarshaller(commandsMarshaller);

        NodeOptions opt2 = new NodeOptions();
        opt2.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));
        opt2.setCommandsMarshaller(commandsMarshaller);

        NodeOptions opt3 = new NodeOptions();
        opt3.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));
        opt3.setCommandsMarshaller(commandsMarshaller);

        metaStorageRaftSrv1 = TestJraftServerFactory.create(
                cluster.get(0),
                workDir.resolve("node1"),
                systemConfiguration,
                opt1,
                new RaftGroupEventsClientListener()
        );

        metaStorageRaftSrv2 = TestJraftServerFactory.create(
                cluster.get(1),
                workDir.resolve("node2"),
                systemConfiguration,
                opt2,
                new RaftGroupEventsClientListener()
        );

        metaStorageRaftSrv3 = TestJraftServerFactory.create(
                cluster.get(2),
                workDir.resolve("node3"),
                systemConfiguration,
                opt3,
                new RaftGroupEventsClientListener()
        );

        assertThat(
                startAsync(new ComponentContext(), metaStorageRaftSrv1, metaStorageRaftSrv2, metaStorageRaftSrv3),
                willCompleteSuccessfully()
        );

        var raftNodeId1 = new RaftNodeId(MetastorageGroupId.INSTANCE, membersConfiguration.peer(localMemberName(cluster.get(0))));

        metaStorageRaftSrv1.startRaftNode(
                raftNodeId1,
                membersConfiguration,
                new MetaStorageListener(mockStorage, mock(ClusterTimeImpl.class)),
                defaults()
        );

        var raftNodeId2 = new RaftNodeId(MetastorageGroupId.INSTANCE, membersConfiguration.peer(localMemberName(cluster.get(1))));

        metaStorageRaftSrv2.startRaftNode(
                raftNodeId2,
                membersConfiguration,
                new MetaStorageListener(mockStorage, mock(ClusterTimeImpl.class)),
                defaults()
        );

        var raftNodeId3 = new RaftNodeId(MetastorageGroupId.INSTANCE, membersConfiguration.peer(localMemberName(cluster.get(2))));

        metaStorageRaftSrv3.startRaftNode(
                raftNodeId3,
                membersConfiguration,
                new MetaStorageListener(mockStorage, mock(ClusterTimeImpl.class)),
                defaults()
        );

        metaStorageRaftGrpSvc1 = waitForRaftGroupServiceSafely(RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(0),
                FACTORY,
                raftConfiguration,
                membersConfiguration,
                true,
                executor,
                commandsMarshaller
        ));

        metaStorageRaftGrpSvc2 = waitForRaftGroupServiceSafely(RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(1),
                FACTORY,
                raftConfiguration,
                membersConfiguration,
                true,
                executor,
                commandsMarshaller
        ));

        metaStorageRaftGrpSvc3 = waitForRaftGroupServiceSafely(RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(2),
                FACTORY,
                raftConfiguration,
                membersConfiguration,
                true,
                executor,
                commandsMarshaller
        ));

        assertTrue(waitForCondition(
                        () -> sameLeaders(metaStorageRaftGrpSvc1, metaStorageRaftGrpSvc2, metaStorageRaftGrpSvc3), 10_000),
                "Leaders: " + metaStorageRaftGrpSvc1.leader() + " " + metaStorageRaftGrpSvc2.leader() + " " + metaStorageRaftGrpSvc3
                        .leader());

        List<Pair<RaftServer, RaftGroupService>> raftServersRaftGroups = new ArrayList<>();

        raftServersRaftGroups.add(new Pair<>(metaStorageRaftSrv1, metaStorageRaftGrpSvc1));
        raftServersRaftGroups.add(new Pair<>(metaStorageRaftSrv2, metaStorageRaftGrpSvc2));
        raftServersRaftGroups.add(new Pair<>(metaStorageRaftSrv3, metaStorageRaftGrpSvc3));

        return raftServersRaftGroups;
    }

    private static RaftGroupService waitForRaftGroupServiceSafely(CompletableFuture<RaftGroupService> future) {
        assertThat(future, willCompleteSuccessfully());

        return future.join();
    }

    /**
     * Checks if all raft groups have the same leader.
     *
     * @param group1 Raft group 1
     * @param group2 Raft group 2
     * @param group3 Raft group 3
     * @return {@code true} if all raft groups have the same leader.
     */
    private boolean sameLeaders(RaftGroupService group1, RaftGroupService group2, RaftGroupService group3) {
        group1.refreshLeader();
        group2.refreshLeader();
        group3.refreshLeader();

        return Objects.equals(group1.leader(), group2.leader()) && Objects.equals(group2.leader(), group3.leader());
    }

    private static String localMemberName(ClusterService service) {
        return service.topologyService().localMember().name();
    }

    /**
     * User's replicator state listener.
     */
    static class UserReplicatorStateListener implements Replicator.ReplicatorStateListener {
        /** Replicator started counter. */
        private final AtomicInteger replicatorStartedCounter;

        /** Replicator stopped counter. */
        private final AtomicInteger replicatorStoppedCounter;

        /**
         * Constructor.
         *
         * @param replicatorStartedCounter Replicator started counter.
         * @param replicatorStoppedCounter Replicator stopped counter.
         */
        UserReplicatorStateListener(AtomicInteger replicatorStartedCounter, AtomicInteger replicatorStoppedCounter) {
            this.replicatorStartedCounter = replicatorStartedCounter;
            this.replicatorStoppedCounter = replicatorStoppedCounter;
        }

        /** {@inheritDoc} */
        @Override
        public void onCreated(PeerId peer) {
            int val = replicatorStartedCounter.incrementAndGet();

            LOG.info("Replicator has been created {} {}", peer, val);
        }

        /** {@inheritDoc} */
        @Override
        public void onError(PeerId peer, Status status) {
            LOG.info("Replicator has errors {} {}", peer, status);
        }

        /** {@inheritDoc} */
        @Override
        public void onDestroyed(PeerId peer) {
            int val = replicatorStoppedCounter.incrementAndGet();

            LOG.info("Replicator has been destroyed {} {}", peer, val);
        }
    }

    /**
     * Internal pair implementation.
     *
     * @param <K> Key
     * @param <V> Value
     */
    private static class Pair<K, V> {
        private final K key;
        private final V value;

        Pair(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}
