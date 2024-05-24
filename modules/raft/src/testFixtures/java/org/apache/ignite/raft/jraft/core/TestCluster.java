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

import static java.util.stream.Collectors.toList;
import static org.apache.ignite.internal.network.utils.ClusterServiceTestUtils.clusterService;
import static org.apache.ignite.internal.testframework.IgniteTestUtils.waitForCondition;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.logger.IgniteLogger;
import org.apache.ignite.internal.logger.Loggers;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.network.ClusterService;
import org.apache.ignite.internal.network.StaticNodeFinder;
import org.apache.ignite.internal.raft.JraftGroupEventsListener;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.jraft.JRaftServiceFactory;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.Node;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.option.RaftOptions;
import org.apache.ignite.raft.jraft.rpc.TestIgniteRpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.storage.SnapshotThrottle;
import org.apache.ignite.raft.jraft.test.TestPeer;
import org.apache.ignite.raft.jraft.test.TestUtils;
import org.apache.ignite.raft.jraft.util.ExecutorServiceHelper;
import org.apache.ignite.raft.jraft.util.ExponentialBackoffTimeoutStrategy;
import org.apache.ignite.raft.jraft.util.Utils;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.TestInfo;

/**
 * Test cluster for NodeTest
 */
public class TestCluster {
    /**
     * Default election timeout.
     * Important: due to sync disk ops (writing raft meta) during probe request processing this timeout should be high
     * enough to avoid test flakiness. Test environment might give another instability.
     */
    public static final int ELECTION_TIMEOUT_MILLIS = 1000;

    private static final IgniteLogger LOG = Loggers.forClass(TestCluster.class);

    private final String dataPath;
    private final String name;
    private final List<TestPeer> peers;
    private final List<NodeImpl> nodes;
    private final LinkedHashMap<PeerId, MockStateMachine> fsms;
    private final ConcurrentMap<PeerId, RaftGroupService> serverMap = new ConcurrentHashMap<>();
    private final int electionTimeoutMs;
    private final Lock lock = new ReentrantLock();
    private final Consumer<NodeOptions> optsClo;

    /** Test info. */
    private final TestInfo testInfo;

    private JRaftServiceFactory raftServiceFactory = new TestJRaftServiceFactory();

    private LinkedHashSet<PeerId> learners;

    private JraftGroupEventsListener raftGrpEvtsLsnr;

    public JRaftServiceFactory getRaftServiceFactory() {
        return this.raftServiceFactory;
    }

    public void setRaftServiceFactory(JRaftServiceFactory raftServiceFactory) {
        this.raftServiceFactory = raftServiceFactory;
    }

    public LinkedHashSet<PeerId> getLearners() {
        return this.learners;
    }

    public void setLearners(LinkedHashSet<TestPeer> learners) {
        this.learners = learners.stream().map(TestPeer::getPeerId).collect(Collectors.toCollection(LinkedHashSet::new));
    }

    public TestCluster(String name, String dataPath, List<TestPeer> peers, TestInfo testInfo) {
        this(name, dataPath, peers, ELECTION_TIMEOUT_MILLIS, testInfo);
    }

    public TestCluster(
        String name,
        String dataPath,
        List<TestPeer> peers,
        int electionTimeoutMs,
        TestInfo testInfo
    ) {
        this(name, dataPath, peers, new LinkedHashSet<>(), electionTimeoutMs, null, testInfo);
    }

    public TestCluster(
        String name,
        String dataPath,
        List<TestPeer> peers,
        LinkedHashSet<TestPeer> learners,
        int electionTimeoutMs,
        TestInfo testInfo
    ) {
        this(name, dataPath, peers, learners, electionTimeoutMs, null, testInfo);
    }

    public TestCluster(
        String name,
        String dataPath,
        List<TestPeer> peers,
        LinkedHashSet<TestPeer> learners,
        int electionTimeoutMs,
        @Nullable Consumer<NodeOptions> optsClo,
        TestInfo testInfo
    ) {
        this.name = name;
        this.dataPath = dataPath;
        this.peers = peers;
        this.nodes = new ArrayList<>(this.peers.size());
        this.fsms = new LinkedHashMap<>(this.peers.size());
        this.electionTimeoutMs = electionTimeoutMs;
        this.learners = learners.stream().map(TestPeer::getPeerId).collect(Collectors.toCollection(LinkedHashSet::new));
        this.optsClo = optsClo;
        this.testInfo = testInfo;
    }

    public boolean start(TestPeer peer) throws Exception {
        return start(peer, false, 300);
    }

    public boolean startLearner(TestPeer peer) throws Exception {
        this.learners.add(peer.getPeerId());
        return this.start(peer, false, 300);
    }

    public boolean start(TestPeer peer, boolean emptyPeers, int snapshotIntervalSecs)
        throws IOException {
        return this.start(peer, emptyPeers, snapshotIntervalSecs, false);
    }

    public boolean start(TestPeer peer, boolean emptyPeers, int snapshotIntervalSecs,
        boolean enableMetrics) throws IOException {
        return this.start(peer, emptyPeers, snapshotIntervalSecs, enableMetrics, null, null);
    }

    public boolean start(TestPeer peer, boolean emptyPeers, int snapshotIntervalSecs,
        boolean enableMetrics, SnapshotThrottle snapshotThrottle) throws IOException {
        return this.start(peer, emptyPeers, snapshotIntervalSecs, enableMetrics, snapshotThrottle, null);
    }

    public boolean start(TestPeer peer, boolean emptyPeers, int snapshotIntervalSecs,
        boolean enableMetrics, SnapshotThrottle snapshotThrottle,
        RaftOptions raftOptions) throws IOException {

        return this.start(peer, emptyPeers, snapshotIntervalSecs, enableMetrics, snapshotThrottle, raftOptions, null);
    }

    public boolean start(TestPeer peer, boolean emptyPeers, int snapshotIntervalSecs,
        boolean enableMetrics, SnapshotThrottle snapshotThrottle,
        RaftOptions raftOptions, HybridClock clock) throws IOException {

        this.lock.lock();
        try {
            if (this.serverMap.get(peer.getPeerId()) != null) {
                return true;
            }

            // Start node in non shared pools mode. Pools will be managed by node itself.
            NodeOptions nodeOptions = new NodeOptions();

            nodeOptions.setServerName(peer.getPeerId().toString());

            nodeOptions.setElectionTimeoutMs(this.electionTimeoutMs);
            nodeOptions.setEnableMetrics(enableMetrics);
            nodeOptions.setSnapshotThrottle(snapshotThrottle);
            nodeOptions.setSnapshotIntervalSecs(snapshotIntervalSecs);
            nodeOptions.setServiceFactory(this.raftServiceFactory);
            if (clock != null) {
                nodeOptions.setClock(clock);
            }
            if (raftOptions != null) {
                nodeOptions.setRaftOptions(raftOptions);
            }
            String serverDataPath = this.dataPath + File.separator + peer.getPeerId().toString().replace(':', '_');
            new File(serverDataPath).mkdirs();
            nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
            nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
            nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");
            nodeOptions.setElectionPriority(peer.getPeerId().getPriority());

            // Align rpc options with election timeout.
            nodeOptions.setRpcConnectTimeoutMs(this.electionTimeoutMs / 3);
            nodeOptions.setRpcDefaultTimeout(this.electionTimeoutMs / 2);

            // Reduce default threads count per test node.
            nodeOptions.setRaftRpcThreadPoolSize(Utils.cpus());
            nodeOptions.setTimerPoolSize(Utils.cpus() * 2);
            nodeOptions.setRpcProcessorThreadPoolSize(Utils.cpus() * 3);

            nodeOptions.setElectionTimeoutStrategy(new ExponentialBackoffTimeoutStrategy());

            MockStateMachine fsm = new MockStateMachine(peer.getPeerId());
            nodeOptions.setFsm(fsm);

            nodeOptions.setRaftGrpEvtsLsnr(raftGrpEvtsLsnr);

            List<NetworkAddress> addressList = List.of();

            if (!emptyPeers) {
                addressList = peers.stream()
                        .map(p -> new NetworkAddress(TestUtils.getLocalAddress(), p.getPort()))
                        .collect(toList());

                nodeOptions.setInitialConf(new Configuration(
                        peers.stream().map(TestPeer::getPeerId).collect(toList()),
                        learners
                ));
            }

            NodeManager nodeManager = new NodeManager();

            ClusterService clusterService = clusterService(testInfo, peer.getPort(), new StaticNodeFinder(addressList));

            var rpcClient = new IgniteRpcClient(clusterService);

            nodeOptions.setRpcClient(rpcClient);

            nodeOptions.setCommandsMarshaller(commandsMarshaller(clusterService));

            ExecutorService requestExecutor = JRaftUtils.createRequestExecutor(nodeOptions);

            var rpcServer = new TestIgniteRpcServer(clusterService, nodeManager, nodeOptions, requestExecutor);

            assertThat(clusterService.startAsync(new ComponentContext()), willCompleteSuccessfully());

            if (optsClo != null)
                optsClo.accept(nodeOptions);

            RaftGroupService server = new RaftGroupService(this.name, peer.getPeerId(),
                nodeOptions, rpcServer, nodeManager) {
                @Override public synchronized void shutdown() {
                    // This stop order is consistent with JRaftServerImpl
                    rpcServer.shutdown();

                    ExecutorServiceHelper.shutdownAndAwaitTermination(requestExecutor);

                    super.shutdown();

                    // Network service must be stopped after a node because raft initiates timeoutnowrequest on stop for faster
                    // leader election.
                    assertThat(clusterService.stopAsync(new ComponentContext()), willCompleteSuccessfully());
                }
            };

            this.serverMap.put(peer.getPeerId(), server);

            Node node = server.start();

            this.fsms.put(peer.getPeerId(), fsm);
            this.nodes.add((NodeImpl) node);
            return true;
        }
        finally {
            this.lock.unlock();
        }
    }

    public static ThreadLocalOptimizedMarshaller commandsMarshaller(ClusterService clusterService) {
        return new ThreadLocalOptimizedMarshaller(clusterService.serializationRegistry());
    }

    public Node getNode(PeerId peerId) {
        this.lock.lock();
        try {
            for (NodeImpl node : nodes) {
                if (node.getServerId().equals(peerId))
                    return node;
            }
        }
        finally {
            this.lock.unlock();
        }

        return null;
    }

    public RaftGroupService getServer(PeerId peerId) {
        return serverMap.get(peerId);
    }

    public MockStateMachine getFsmByPeer(PeerId peer) {
        this.lock.lock();
        try {
            return this.fsms.get(peer);
        }
        finally {
            this.lock.unlock();
        }
    }

    public List<MockStateMachine> getFsms() {
        this.lock.lock();
        try {
            return new ArrayList<>(this.fsms.values());
        }
        finally {
            this.lock.unlock();
        }
    }

    public boolean stop(PeerId listenAddr) throws InterruptedException {
        removeNode(listenAddr);
        RaftGroupService raftGroupService = this.serverMap.remove(listenAddr);
        raftGroupService.shutdown();
        return true;
    }

    public void stopAll() throws InterruptedException {
        List<PeerId> addrs = getAllNodes();
        for (PeerId addr : addrs)
            stop(addr);
    }

    public void clean(PeerId listenAddr) {
        Path path = Paths.get(this.dataPath, listenAddr.toString().replace(':', '_'));
        LOG.info("Clean dir: {}", path);
        IgniteUtils.deleteIfExists(path);
    }

    public JraftGroupEventsListener getRaftGrpEvtsLsnr() {
        return raftGrpEvtsLsnr;
    }

    public void setRaftGrpEvtsLsnr(JraftGroupEventsListener raftGrpEvtsLsnr) {
        this.raftGrpEvtsLsnr = raftGrpEvtsLsnr;
    }

    public Node getLeader() {
        this.lock.lock();
        try {
            for (int i = 0; i < this.nodes.size(); i++) {
                NodeImpl node = this.nodes.get(i);
                if (node.isLeader() && this.fsms.get(node.getServerId()).getLeaderTerm() == node.getCurrentTerm()) {
                    return node;
                }
            }
            return null;
        }
        finally {
            this.lock.unlock();
        }
    }

    public MockStateMachine getLeaderFsm() {
        Node leader = getLeader();
        if (leader != null) {
            return (MockStateMachine) leader.getOptions().getFsm();
        }
        return null;
    }

    /**
     * Wait until a leader is elected and return it.
     * @throws InterruptedException
     * @return Leader.
     */
    public Node waitAndGetLeader() throws InterruptedException {
        AtomicReference<Node> node = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            node.set(getLeader());

            return node.get() != null;
        }, 10_000L));

        return node.get();
    }

    /**
     * Wait until a leader is elected and a leader is from the expected set of nodes {@code expectedLeaderPeer} and return it.
     *
     * @param expectedLeaderPeer Set of nodes with the expected node;
     * @return Leader.
     * @throws InterruptedException If failed.
     */
    public Node waitAndGetLeader(Set<PeerId> expectedLeaderPeer) throws InterruptedException {
        AtomicReference<Node> node = new AtomicReference<>();

        assertTrue(waitForCondition(() -> {
            node.set(getLeader());

            return node.get() != null && expectedLeaderPeer.contains(node.get().getNodeId().getPeerId());
        }, 10_000L));

        return node.get();
    }

    public List<Node> getFollowers() {
        List<Node> ret = new ArrayList<>();
        this.lock.lock();
        try {
            for (NodeImpl node : this.nodes) {
                if (!node.isLeader() && !this.learners.contains(node.getServerId())) {
                    ret.add(node);
                }
            }
        }
        finally {
            this.lock.unlock();
        }
        return ret;
    }

    /**
     * Ensure all peers follow the leader
     *
     * @param node The leader.
     * @throws InterruptedException if interrupted
     */
    public void ensureLeader(Node node) throws InterruptedException {
        while (true) {
            this.lock.lock();
            try {
                boolean wait = false;

                for (Node node0 : this.nodes) {
                    PeerId leaderId = node0.getLeaderId();

                    if (leaderId == null || !leaderId.equals(node.getNodeId().getPeerId())) {
                        wait = true;

                        break;
                    }
                }

                if (wait) {
                    Thread.sleep(10);

                    continue;
                }
                else
                    return;
            }
            finally {
                this.lock.unlock();
            }
        }
    }

    public List<NodeImpl> getNodes() {
        this.lock.lock();
        try {
            return new ArrayList<>(this.nodes);
        }
        finally {
            this.lock.unlock();
        }
    }

    public List<PeerId> getAllNodes() {
        this.lock.lock();
        try {
            return this.nodes.stream().map(node -> node.getNodeId().getPeerId())
                .collect(toList());
        }
        finally {
            this.lock.unlock();
        }
    }

    public Node removeNode(PeerId addr) {
        Node ret = null;
        this.lock.lock();
        try {
            for (int i = 0; i < this.nodes.size(); i++) {
                if (this.nodes.get(i).getNodeId().getPeerId().equals(addr)) {
                    ret = this.nodes.remove(i);
                    this.fsms.remove(ret.getNodeId().getPeerId());
                    break;
                }
            }
        }
        finally {
            this.lock.unlock();
        }
        return ret;
    }

    public void ensureSame() throws InterruptedException {
        ensureSame(addr -> false);
    }

    /**
     * @param filter The node to exclude filter.
     * @return {@code True} if all FSM state are the same.
     * @throws InterruptedException
     */
    public void ensureSame(Predicate<PeerId> filter) throws InterruptedException {
        this.lock.lock();

        List<MockStateMachine> fsmList = new ArrayList<>(this.fsms.values());

        if (fsmList.size() <= 1) {
            LOG.warn("ensureSame is skipped because only one node in the group");
            this.lock.unlock();
            return;
        }

        Node leader = getLeader();

        assertNotNull(leader);

        MockStateMachine first = fsms.get(leader.getNodeId().getPeerId());

        LOG.info("Start ensureSame, leader={}", leader);

        try {
            assertTrue(TestUtils.waitForCondition(() -> {
                first.lock();

                try {
                    for (int i = 0; i < fsmList.size(); i++) {
                        MockStateMachine fsm = fsmList.get(i);

                        if (fsm == first || filter.test(fsm.getPeerId()))
                            continue;

                        fsm.lock();

                        try {
                            int size0 = first.getLogs().size();
                            int size1 = fsm.getLogs().size();

                            if (size0 == 0 || size0 != size1)
                                return false;

                            for (int j = 0; j < size0; j++) {
                                ByteBuffer data0 = first.getLogs().get(j);
                                ByteBuffer data1 = fsm.getLogs().get(j);

                                if (!data0.equals(data1))
                                    return false;
                            }
                        }
                        finally {
                            fsm.unlock();
                        }
                    }
                }
                finally {
                    first.unlock();
                }

                return true;
            }, 20_000));
        }
        finally {
            this.lock.unlock();

            Node leader1 = getLeader();

            LOG.info("End ensureSame, leader={}", leader1);

            assertSame(leader, leader1, "Leader shouldn't change while comparing fsms");
        }
    }
}
