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

package org.apache.ignite.raft.jraft.core;

import static java.lang.Thread.sleep;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.raft.jraft.core.ItNodeTest.waitForTopologyOnEveryNode;
import static org.apache.ignite.raft.jraft.core.TestCluster.ELECTION_TIMEOUT_MILLIS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.testframework.BaseIgniteAbstractTest;
import org.apache.ignite.internal.testframework.WorkDirectory;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.CliService;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.entity.Task;
import org.apache.ignite.raft.jraft.option.CliOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.test.TestPeer;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.internal.network.utils.ClusterServiceTestUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Jraft cli tests.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItCliServiceTest extends BaseIgniteAbstractTest {
    /**
     * The logger.
     */
    private static final IgniteLogger LOG = Loggers.forClass(ItCliServiceTest.class);

    private static final int LEARNER_PORT_STEP = 100;

    private TestCluster cluster;

    private ClusterService clientSvc;

    private final String groupId = "CliServiceTest";

    private CliService cliService;

    private Configuration conf;

    private ExecutorService clientExecutor;

    /** Executes before each test. */
    @BeforeEach
    public void setup(TestInfo testInfo, @WorkDirectory Path dataPath) throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Start test method: " + testInfo.getDisplayName());

        List<TestPeer> peers = TestUtils.generatePeers(testInfo, 3);

        LinkedHashSet<TestPeer> learners = new LinkedHashSet<>();

        // 2 learners
        for (int i = 0; i < 2; i++) {
            learners.add(new TestPeer(testInfo, TestUtils.INIT_PORT + LEARNER_PORT_STEP + i));
        }

        cluster = new TestCluster(groupId, dataPath.toString(), peers, learners, ELECTION_TIMEOUT_MILLIS, testInfo);
        for (TestPeer peer : peers) {
            cluster.start(peer);
        }

        for (TestPeer peer : learners) {
            cluster.startLearner(peer);
        }

        cluster.ensureLeader(cluster.waitAndGetLeader());

        cliService = new CliServiceImpl();
        conf = new Configuration(
                peers.stream().map(TestPeer::getPeerId).collect(toList()),
                learners.stream().map(TestPeer::getPeerId).collect(toList())
        );

        CliOptions opts = new CliOptions();
        clientExecutor = JRaftUtils.createClientExecutor(opts, "client");
        opts.setClientExecutor(clientExecutor);

        List<NetworkAddress> addressList = peers.stream()
                .map(p -> new NetworkAddress(TestUtils.getLocalAddress(), p.getPort()))
                .collect(toList());

        clientSvc = ClusterServiceTestUtils.clusterService(
                testInfo,
                TestUtils.INIT_PORT - 1,
                new StaticNodeFinder(addressList)
        );

        assertThat(clientSvc.startAsync(new ComponentContext()), willCompleteSuccessfully());

        IgniteRpcClient rpcClient = new IgniteRpcClient(clientSvc) {
            @Override public void shutdown() {
                super.shutdown();

                assertThat(clientSvc.stopAsync(new ComponentContext()), willCompleteSuccessfully());
            }
        };

        opts.setRpcClient(rpcClient);
        assertTrue(cliService.init(opts));
    }

    @AfterEach
    public void teardown(TestInfo testInfo) throws Exception {
        LOG.info(">>>>>>>>>>>>>>> Teardown started: " + testInfo.getDisplayName());
        cliService.shutdown();
        cluster.stopAll();
        ExecutorServiceHelper.shutdownAndAwaitTermination(clientExecutor);

        TestUtils.assertAllJraftThreadsStopped();

        LOG.info(">>>>>>>>>>>>>>> End test method: " + testInfo.getDisplayName());
    }

    @Test
    public void testTransferLeader() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId().copy();
        assertNotNull(leader);

        Set<PeerId> peers = conf.getPeerSet();
        PeerId targetPeer = null;
        for (PeerId peer : peers) {
            if (!peer.equals(leader)) {
                targetPeer = peer;
                break;
            }
        }
        assertNotNull(targetPeer);
        assertTrue(cliService.transferLeader(groupId, conf, targetPeer).isOk());
        assertEquals(targetPeer, cluster.waitAndGetLeader().getNodeId().getPeerId());
    }

    @SuppressWarnings("SameParameterValue")
    private void sendTestTaskAndWait(Node node, int code) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(10);
        for (int i = 0; i < 10; i++) {
            ByteBuffer data = ByteBuffer.wrap(("hello" + i).getBytes(UTF_8));
            Task task = new Task(data, new ExpectClosure(code, null, latch));
            node.apply(task);
        }
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testLearnerServices(TestInfo testInfo) throws Exception {
        TestPeer learner3 = new TestPeer(testInfo, TestUtils.INIT_PORT + LEARNER_PORT_STEP + 3);
        assertTrue(cluster.startLearner(learner3));
        sendTestTaskAndWait(cluster.getLeader(), 0);
        cluster.ensureSame(id -> id.equals(learner3.getPeerId()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!fsm.getPeerId().equals(learner3.getPeerId())) {
                assertEquals(10, fsm.getLogs().size());
            }
        }

        assertEquals(0, cluster.getFsmByPeer(learner3.getPeerId()).getLogs().size());
        List<PeerId> oldLearners = new ArrayList<PeerId>(conf.getLearners());
        assertEquals(oldLearners, cliService.getLearners(groupId, conf));
        assertEquals(oldLearners, cliService.getAliveLearners(groupId, conf));

        // Add learner3
        cliService.addLearners(groupId, conf, Collections.singletonList(learner3.getPeerId()));

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(learner3.getPeerId()).getLogs().size() == 10, 5_000));

        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame();

        for (MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }

        List<PeerId> newLearners = new ArrayList<>(oldLearners);
        newLearners.add(learner3.getPeerId());
        assertEquals(newLearners, cliService.getLearners(groupId, conf));
        assertEquals(newLearners, cliService.getAliveLearners(groupId, conf));

        // Remove  3
        cliService.removeLearners(groupId, conf, Collections.singletonList(learner3.getPeerId()));
        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame(id -> id.equals(learner3.getPeerId()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!fsm.getPeerId().equals(learner3.getPeerId())) {
                assertEquals(30, fsm.getLogs().size());
            }
        }

        // Latest 10 logs are not replicated to learner3, because it's removed.
        assertEquals(20, cluster.getFsmByPeer(learner3.getPeerId()).getLogs().size());
        assertEquals(oldLearners, cliService.getLearners(groupId, conf));
        assertEquals(oldLearners, cliService.getAliveLearners(groupId, conf));

        // Set learners into [learner3]
        cliService.resetLearners(groupId, conf, Collections.singletonList(learner3.getPeerId()));

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(learner3.getPeerId()).getLogs().size() == 30, 5_000));

        sendTestTaskAndWait(cluster.getLeader(), 0);

        cluster.ensureSame(oldLearners::contains);

        // Latest 10 logs are not replicated to learner1 and learner2, because they were removed by resetting learners set.
        for (MockStateMachine fsm : cluster.getFsms()) {
            if (!oldLearners.contains(fsm.getPeerId())) {
                assertEquals(40, fsm.getLogs().size());
            } else {
                assertEquals(30, fsm.getLogs().size());
            }
        }

        assertEquals(Collections.singletonList(learner3.getPeerId()), cliService.getLearners(groupId, conf));
        assertEquals(Collections.singletonList(learner3.getPeerId()), cliService.getAliveLearners(groupId, conf));

        // Stop learner3
        cluster.stop(learner3.getPeerId());
        sleep(1000);
        assertEquals(Collections.singletonList(learner3.getPeerId()), cliService.getLearners(groupId, conf));
        assertTrue(cliService.getAliveLearners(groupId, conf).isEmpty());

        TestPeer learner4 = new TestPeer(testInfo, TestUtils.INIT_PORT + LEARNER_PORT_STEP + 4);
        assertTrue(cluster.startLearner(learner4));

        cliService.addLearners(groupId, conf, Collections.singletonList(learner4.getPeerId()));
        sleep(1000);
        assertEquals(1, cliService.getAliveLearners(groupId, conf).size());
        assertTrue(cliService.learner2Follower(groupId, conf, learner4.getPeerId()).isOk());

        sleep(1000);
        List<PeerId> currentLearners = cliService.getAliveLearners(groupId, conf);
        assertFalse(currentLearners.contains(learner4.getPeerId()));

        List<PeerId> currentFollowers = cliService.getPeers(groupId, conf);
        assertTrue(currentFollowers.contains(learner4.getPeerId()));

        cluster.stop(learner4.getPeerId());
    }

    @Test
    public void testAddPeerRemovePeer(TestInfo testInfo) throws Exception {
        TestPeer peer3 = new TestPeer(testInfo, TestUtils.INIT_PORT + 3);
        assertTrue(cluster.start(peer3));
        sendTestTaskAndWait(cluster.getLeader(), 0);
        cluster.ensureSame(addr -> addr.equals(peer3.getPeerId()));
        assertEquals(0, cluster.getFsmByPeer(peer3.getPeerId()).getLogs().size());

        assertTrue(cliService.addPeer(groupId, conf, peer3.getPeerId()).isOk());

        assertTrue(waitForCondition(() -> cluster.getFsmByPeer(peer3.getPeerId()).getLogs().size() == 10, 5_000));
        sendTestTaskAndWait(cluster.getLeader(), 0);

        assertEquals(6, cluster.getFsms().size());

        cluster.ensureSame();

        for (MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(20, fsm.getLogs().size());
        }

        //remove peer3
        assertTrue(cliService.removePeer(groupId, conf, peer3.getPeerId()).isOk());
        sleep(200);
        sendTestTaskAndWait(cluster.getLeader(), 0);

        assertEquals(6, cluster.getFsms().size());

        cluster.ensureSame(addr -> addr.equals(peer3.getPeerId()));

        for (MockStateMachine fsm : cluster.getFsms()) {
            if (fsm.getPeerId().equals(peer3.getPeerId())) {
                assertEquals(20, fsm.getLogs().size());
            } else {
                assertEquals(30, fsm.getLogs().size());
            }
        }
    }

    @Test
    public void testChangePeersAndLearners(TestInfo testInfo) throws Exception {
        List<TestPeer> newPeers = TestUtils.generatePeers(testInfo, 6);
        newPeers.removeIf(p -> conf.getPeerSet().contains(p.getPeerId()));

        assertEquals(3, newPeers.size());

        for (TestPeer peer : newPeers) {
            assertTrue(cluster.start(peer));
        }
        Node oldLeaderNode = cluster.waitAndGetLeader();
        assertNotNull(oldLeaderNode);
        PeerId oldLeader = oldLeaderNode.getNodeId().getPeerId();
        assertNotNull(oldLeader);

        Status status = cliService.changePeersAndLearners(
                groupId,
                conf,
                new Configuration(newPeers.stream().map(TestPeer::getPeerId).collect(toList())),
                oldLeaderNode.getCurrentTerm()
        );

        assertTrue(status.isOk(), status.getErrorMsg());
        PeerId newLeader = cluster.waitAndGetLeader().getNodeId().getPeerId();
        assertNotEquals(oldLeader, newLeader);
        assertTrue(newPeers.stream().anyMatch(p -> p.getPeerId().equals(newLeader)));
    }

    @Test
    public void testSnapshot() throws Exception {
        sendTestTaskAndWait(cluster.getLeader(), 0);
        assertEquals(5, cluster.getFsms().size());

        waitForTopologyOnEveryNode(6, cluster);

        assertTrue(waitForTopology(clientSvc, 6,10_000));

        for (MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(0, fsm.getSaveSnapshotTimes());
        }

        for (PeerId peer : conf) {
            assertTrue(cliService.snapshot(groupId, peer).isOk(), "Failed to trigger snapshot from peer = " + peer);
        }

        for (PeerId peer : conf.getLearners()) {
            assertTrue(cliService.snapshot(groupId, peer).isOk(), "Failed to trigger snapshot from learner = " + peer);
        }

        for (MockStateMachine fsm : cluster.getFsms()) {
            assertEquals(1, fsm.getSaveSnapshotTimes());
        }
    }

    private static boolean waitForTopology(ClusterService cluster, int expected, int timeout) {
        return waitForCondition(() -> cluster.topologyService().allMembers().size() >= expected, timeout);
    }

    @Test
    public void testGetPeers() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getPeers(groupId, conf)).toArray());

        // stop one peer
        List<PeerId> peers = conf.getPeers();
        cluster.stop(peers.get(0));

        leader = cluster.waitAndGetLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getPeers(groupId, conf)).toArray());

        cluster.stopAll();

        try {
            cliService.getPeers(groupId, conf);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Fail to get leader of group " + groupId), e.getMessage());
        }
    }

    @Test
    public void testGetAlivePeers() throws Exception {
        PeerId leader = cluster.getLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(conf.getPeerSet().toArray(),
            new HashSet<>(cliService.getAlivePeers(groupId, conf)).toArray());

        // stop one peer
        List<PeerId> peers = conf.getPeers();
        cluster.stop(peers.get(0));
        peers.remove(0);

        sleep(1000);

        leader = cluster.waitAndGetLeader().getNodeId().getPeerId();
        assertNotNull(leader);
        assertArrayEquals(new HashSet<>(peers).toArray(),
            new HashSet<>(cliService.getAlivePeers(groupId, conf)).toArray());

        cluster.stopAll();

        try {
            cliService.getAlivePeers(groupId, conf);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().startsWith("Fail to get leader of group " + groupId), e.getMessage());
        }
    }

    @Test
    public void testRebalance() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        groupIds.add("group_8");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockCliService(rebalancedLeaderIds, new PeerId("host_1", 8080));

        assertTrue(cliService.rebalance(groupIds, conf, rebalancedLeaderIds).isOk());
        assertEquals(groupIds.size(), rebalancedLeaderIds.size());

        Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet()) {
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        }
        int expectedAvgLeaderNum = (int) Math.ceil((double) groupIds.size() / conf.size());
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            System.out.println(entry);
            assertTrue(entry.getValue() <= expectedAvgLeaderNum);
        }
    }

    @Test
    public void testRebalanceOnLeaderFail() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockLeaderFailCliService();

        assertEquals("Fail to get leader", cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
    }

    @Test
    public void testRelalanceOnTransferLeaderFail() {
        Set<String> groupIds = new TreeSet<>();
        groupIds.add("group_1");
        groupIds.add("group_2");
        groupIds.add("group_3");
        groupIds.add("group_4");
        groupIds.add("group_5");
        groupIds.add("group_6");
        groupIds.add("group_7");
        Configuration conf = new Configuration();
        conf.addPeer(new PeerId("host_1", 8080));
        conf.addPeer(new PeerId("host_2", 8080));
        conf.addPeer(new PeerId("host_3", 8080));

        Map<String, PeerId> rebalancedLeaderIds = new HashMap<>();

        CliService cliService = new MockTransferLeaderFailCliService(rebalancedLeaderIds,
                new PeerId("host_1", 8080));

        assertEquals("Fail to transfer leader",
                cliService.rebalance(groupIds, conf, rebalancedLeaderIds).getErrorMsg());
        assertTrue(groupIds.size() >= rebalancedLeaderIds.size());

        Map<PeerId, Integer> ret = new HashMap<>();
        for (Map.Entry<String, PeerId> entry : rebalancedLeaderIds.entrySet()) {
            ret.compute(entry.getValue(), (ignored, num) -> num == null ? 1 : num + 1);
        }
        for (Map.Entry<PeerId, Integer> entry : ret.entrySet()) {
            LOG.info(entry.toString());
            assertEquals(new PeerId("host_1", 8080), entry.getKey());
        }
    }

    static class MockCliService extends CliServiceImpl {
        private final Map<String, PeerId> rebalancedLeaderIds;
        private final PeerId initialLeaderId;

        MockCliService(Map<String, PeerId> rebalancedLeaderIds, PeerId initialLeaderId) {
            this.rebalancedLeaderIds = rebalancedLeaderIds;
            this.initialLeaderId = initialLeaderId;
        }

        /** {@inheritDoc} */
        @Override
        public Status getLeader(String groupId, Configuration conf, PeerId leaderId) {
            PeerId ret = rebalancedLeaderIds.get(groupId);
            if (ret != null) {
                leaderId.parse(ret.toString());
            } else {
                leaderId.parse(initialLeaderId.toString());
            }
            return Status.OK();
        }

        /** {@inheritDoc} */
        @Override
        public List<PeerId> getAlivePeers(String groupId, Configuration conf) {
            return conf.getPeers();
        }

        /** {@inheritDoc} */
        @Override
        public Status transferLeader(String groupId, Configuration conf, PeerId peer) {
            return Status.OK();
        }
    }

    static class MockLeaderFailCliService extends MockCliService {
        MockLeaderFailCliService() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override
        public Status getLeader(String groupId, Configuration conf, PeerId leaderId) {
            return new Status(-1, "Fail to get leader");
        }
    }

    static class MockTransferLeaderFailCliService extends MockCliService {
        MockTransferLeaderFailCliService(Map<String, PeerId> rebalancedLeaderIds, PeerId initialLeaderId) {
            super(rebalancedLeaderIds, initialLeaderId);
        }

        /** {@inheritDoc} */
        @Override
        public Status transferLeader(String groupId, Configuration conf, PeerId peer) {
            return new Status(-1, "Fail to transfer leader");
        }
    }

    /**
     * Waits for the condition.
     *
     * @param cond The condition.
     * @param timeout The timeout.
     * @return {@code True} if condition has happened within the timeout.
     */
    @SuppressWarnings("BusyWait")
    protected static boolean waitForCondition(BooleanSupplier cond, long timeout) {
        long stop = System.currentTimeMillis() + timeout;

        while (System.currentTimeMillis() < stop) {
            if (cond.getAsBoolean()) {
                return true;
            }

            try {
                sleep(50);
            } catch (InterruptedException e) {
                return false;
            }
        }

        return false;
    }
}
