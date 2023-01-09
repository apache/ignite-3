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
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.utils.ClusterServiceTestUtils.findLocalAddresses;
import static org.apache.ignite.utils.ClusterServiceTestUtils.waitForTopology;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.metastorage.Entry;
import org.apache.ignite.internal.metastorage.impl.EntryImpl;
import org.apache.ignite.internal.metastorage.impl.ItMetaStorageServiceTest;
import org.apache.ignite.internal.metastorage.impl.MetaStorageService;
import org.apache.ignite.internal.metastorage.impl.MetaStorageServiceImpl;
import org.apache.ignite.internal.metastorage.server.KeyValueStorage;
import org.apache.ignite.internal.raft.PeersAndLearners;
import org.apache.ignite.internal.raft.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.internal.thread.NamedThreadFactory;
import org.apache.ignite.internal.util.Cursor;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.lang.ByteArray;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.StaticNodeFinder;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Meta storage client tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
@ExtendWith(MockitoExtension.class)
public class ItMetaStorageRaftGroupTest {
    /** The logger. */
    private static final IgniteLogger LOG = Loggers.forClass(ItMetaStorageServiceTest.class);

    /** Base network port. */
    private static final int NODE_PORT_BASE = 20_000;

    /** Nodes. */
    private static final int NODES = 3;

    /** Factory. */
    private static final RaftMessagesFactory FACTORY = new RaftMessagesFactory();

    /** Expected server result entry. */
    private static final org.apache.ignite.internal.metastorage.server.Entry EXPECTED_SRV_RESULT_ENTRY1 =
            new org.apache.ignite.internal.metastorage.server.Entry(
                    new byte[] {1},
                    new byte[] {2},
                    10,
                    2
            );

    /**  Expected server result entry. */
    private static final org.apache.ignite.internal.metastorage.server.Entry EXPECTED_SRV_RESULT_ENTRY2 =
            new org.apache.ignite.internal.metastorage.server.Entry(
                    new byte[] {3},
                    new byte[] {4},
                    11,
                    3
            );

    /**  Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY1 =
            new EntryImpl(
                    new ByteArray(new byte[] {1}),
                    new byte[] {2},
                    10,
                    2
            );

    /**  Expected server result entry. */
    private static final EntryImpl EXPECTED_RESULT_ENTRY2 =
            new EntryImpl(
                    new ByteArray(new byte[] {3}),
                    new byte[] {4},
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

    @WorkDirectory
    private Path dataPath;

    /** Executor for raft group services. */
    private ScheduledExecutorService executor;

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
                    clusterService.start();
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
     * @throws Exception If failed to shutdown raft server,
     */
    @AfterEach
    public void afterTest() throws Exception {
        if (metaStorageRaftSrv3 != null) {
            metaStorageRaftSrv3.stopRaftNodes(MetastorageGroupId.INSTANCE);
            metaStorageRaftSrv3.stop();
            metaStorageRaftGrpSvc3.shutdown();
        }

        if (metaStorageRaftSrv2 != null) {
            metaStorageRaftSrv2.stopRaftNodes(MetastorageGroupId.INSTANCE);
            metaStorageRaftSrv2.stop();
            metaStorageRaftGrpSvc2.shutdown();
        }

        if (metaStorageRaftSrv1 != null) {
            metaStorageRaftSrv1.stopRaftNodes(MetastorageGroupId.INSTANCE);
            metaStorageRaftSrv1.stop();
            metaStorageRaftGrpSvc1.shutdown();
        }

        IgniteUtils.shutdownAndAwaitTermination(executor, 10, TimeUnit.SECONDS);

        for (ClusterService node : cluster) {
            node.stop();
        }
    }


    /**
     * Tests that {@link MetaStorageService#range(ByteArray, ByteArray, long)}} next command works correctly
     * after leader changing.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testRangeNextWorksCorrectlyAfterLeaderChange() throws Exception {
        final AtomicInteger replicatorStartedCounter = new AtomicInteger(0);

        final AtomicInteger replicatorStoppedCounter = new AtomicInteger(0);

        when(mockStorage.range(EXPECTED_RESULT_ENTRY1.key().bytes(), new byte[]{4}, false)).thenAnswer(invocation -> {
            List<org.apache.ignite.internal.metastorage.server.Entry> entries = new ArrayList<>(
                    List.of(EXPECTED_SRV_RESULT_ENTRY1, EXPECTED_SRV_RESULT_ENTRY2));

            return Cursor.fromBareIterator(entries.iterator());
        });

        List<Pair<RaftServer, RaftGroupService>> raftServersRaftGroups = prepareJraftMetaStorages(replicatorStartedCounter,
                replicatorStoppedCounter);

        List<RaftServer> raftServers = raftServersRaftGroups.stream().map(p -> p.key).collect(Collectors.toList());

        String oldLeaderId = raftServersRaftGroups.get(0).value.leader().consistentId();

        Optional<RaftServer> oldLeaderServer = raftServers.stream()
                .filter(s -> localMemberName(s.clusterService()).equals(oldLeaderId)).findFirst();

        //Server that will be alive after we stop leader.
        Optional<RaftServer> liveServer = raftServers.stream()
                .filter(s -> !localMemberName(s.clusterService()).equals(oldLeaderId)).findFirst();

        if (oldLeaderServer.isEmpty() || liveServer.isEmpty()) {
            fail();
        }

        RaftGroupService raftGroupServiceOfLiveServer = raftServersRaftGroups.stream()
                .filter(p -> p.key.equals(liveServer.get()))
                .findFirst().get().value;

        MetaStorageService metaStorageSvc = new MetaStorageServiceImpl(
                raftGroupServiceOfLiveServer, "some_node", "some_node");

        Cursor<Entry> cursor = metaStorageSvc.range(EXPECTED_RESULT_ENTRY1.key(), new ByteArray(new byte[]{4}));

        assertTrue(waitForCondition(
                () -> replicatorStartedCounter.get() == 2, 5_000), replicatorStartedCounter.get() + "");

        assertTrue(cursor.hasNext());

        assertEquals(EXPECTED_RESULT_ENTRY1, (cursor.iterator().next()));

        // Ensure that leader has not been changed.
        // In a stable topology unexpected leader election shouldn't happen.
        assertTrue(waitForCondition(
                () -> replicatorStartedCounter.get() == 2, 5_000), replicatorStartedCounter.get() + "");

        //stop leader
        oldLeaderServer.get().stopRaftNodes(MetastorageGroupId.INSTANCE);
        oldLeaderServer.get().stop();
        cluster.stream().filter(c -> localMemberName(c).equals(oldLeaderId)).findFirst().get().stop();

        raftGroupServiceOfLiveServer.refreshLeader().get();

        assertNotSame(oldLeaderId, raftGroupServiceOfLiveServer.leader().consistentId());

        // ensure that leader has been changed only once
        assertTrue(waitForCondition(
                () -> replicatorStartedCounter.get() == 4, 5_000), replicatorStartedCounter.get() + "");
        assertTrue(waitForCondition(
                () -> replicatorStoppedCounter.get() == 2, 5_000), replicatorStoppedCounter.get() + "");

        assertTrue(cursor.hasNext());
        assertEquals(EXPECTED_RESULT_ENTRY2, (cursor.iterator().next()));
    }

    private List<Pair<RaftServer, RaftGroupService>> prepareJraftMetaStorages(AtomicInteger replicatorStartedCounter,
            AtomicInteger replicatorStoppedCounter) throws InterruptedException, ExecutionException {
        PeersAndLearners configuration = cluster.stream()
                .map(ItMetaStorageRaftGroupTest::localMemberName)
                .collect(collectingAndThen(toSet(), PeersAndLearners::fromConsistentIds));

        assertTrue(cluster.size() > 1);

        NodeOptions opt1 = new NodeOptions();
        opt1.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));

        NodeOptions opt2 = new NodeOptions();
        opt2.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));

        NodeOptions opt3 = new NodeOptions();
        opt3.setReplicationStateListeners(
                List.of(new UserReplicatorStateListener(replicatorStartedCounter, replicatorStoppedCounter)));

        metaStorageRaftSrv1 = new JraftServerImpl(cluster.get(0), dataPath.resolve("node1"), opt1);

        metaStorageRaftSrv2 = new JraftServerImpl(cluster.get(1), dataPath.resolve("node2"), opt2);

        metaStorageRaftSrv3 = new JraftServerImpl(cluster.get(2), dataPath.resolve("node3"), opt3);

        metaStorageRaftSrv1.start();

        metaStorageRaftSrv2.start();

        metaStorageRaftSrv3.start();

        var raftNodeId1 = new RaftNodeId(MetastorageGroupId.INSTANCE, configuration.peer(localMemberName(cluster.get(0))));

        metaStorageRaftSrv1.startRaftNode(raftNodeId1, configuration, new MetaStorageListener(mockStorage), defaults());

        var raftNodeId2 = new RaftNodeId(MetastorageGroupId.INSTANCE, configuration.peer(localMemberName(cluster.get(1))));

        metaStorageRaftSrv2.startRaftNode(raftNodeId2, configuration, new MetaStorageListener(mockStorage), defaults());

        var raftNodeId3 = new RaftNodeId(MetastorageGroupId.INSTANCE, configuration.peer(localMemberName(cluster.get(2))));

        metaStorageRaftSrv3.startRaftNode(raftNodeId3, configuration, new MetaStorageListener(mockStorage), defaults());

        metaStorageRaftGrpSvc1 = RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(0),
                FACTORY,
                10_000,
                10_000,
                configuration,
                true,
                200,
                executor
        ).get();

        metaStorageRaftGrpSvc2 = RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(1),
                FACTORY,
                10_000,
                10_000,
                configuration,
                true,
                200,
                executor
        ).get();

        metaStorageRaftGrpSvc3 = RaftGroupServiceImpl.start(
                MetastorageGroupId.INSTANCE,
                cluster.get(2),
                FACTORY,
                10_000,
                10_000,
                configuration,
                true,
                200,
                executor
        ).get();

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
