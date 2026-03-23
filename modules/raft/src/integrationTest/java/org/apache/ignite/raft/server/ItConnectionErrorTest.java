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

import static java.util.stream.IntStream.range;
import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.internal.testframework.matchers.CompletableFutureMatcher.willCompleteSuccessfully;
import static org.apache.ignite.internal.util.IgniteUtils.forEachIndexed;
import static org.apache.ignite.internal.util.IgniteUtils.stopAsync;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.apache.ignite.internal.manager.ComponentContext;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.client.RaftGroupServiceImpl;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.internal.testframework.log4j2.LogInspector;
import org.apache.ignite.raft.jraft.core.Replicator;
import org.apache.ignite.raft.jraft.core.ReplicatorGroupImpl;
import org.apache.ignite.raft.jraft.rpc.impl.AbstractClientService;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.logging.log4j.core.LogEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test checking amount of errors in logs on Raft node stop.
 */
public class ItConnectionErrorTest extends JraftAbstractTest {
    private static final TestReplicationGroupId TEST_GROUP = new TestReplicationGroupId("testGroup");

    private Supplier<CounterListener> listenerFactory = CounterListener::new;

    private List<LogInspector> logInspectors;

    private final Map<Class<?>, Map<String, AtomicInteger>> perClassCounters = new ConcurrentHashMap<>();

    @BeforeEach
    public void setUp() throws Exception {
        perClassCounters.clear();

        startCluster();

        logInspectors = startLogInspectors();
    }

    @AfterEach
    public void tearDown() {
        stopLogInspectors(logInspectors);
    }

    // Otherwise, the leader elected after the start of two nodes may log an exception about the impossibility of reaching the third
    // starting node, which is expected in essence, but is not taken into account in the test verification invariants.
    // The flow is following:
    // 1. NodeA (5003) starts.
    // 2. NodeB (5004) starts.
    // 3. Leader is elected, let's say that NodeA is a leader.
    // 4. Because NodeC(5005) startup hangs a bit, leader ping message to NodeC may fail and in a rare unfortunate event will be accumulated
    //   by log inspector.
    // 5. NodeC starts.
    // 6. NodeB stops.
    // 7. Leader failed to send message to B which is expected.
    // As a result there are two connectivity related records in log from step 4 and step 7. However within test we expect only one from
    //   step 7.
    // In case of two nodes in the group, situation explained in step 4 becomes impossible, thus we use 2 nodes only.
    @Override
    protected int nodesCount() {
        return 2;
    }

    /**
     * Starts a cluster for the test.
     *
     * @throws Exception If failed.
     */
    private void startCluster() throws Exception {
        for (int i = 0; i < nodesCount(); i++) {

            int finalI = i;
            startServer(i, raftServer -> {
                String localNodeName = raftServer.clusterService().topologyService().localMember().name();

                Peer serverPeer = initialMembersConf.peer(localNodeName);

                RaftGroupOptions groupOptions = groupOptions(raftServer);

                groupOptions.setLogStorageManager(logStorageFactories.get(finalI));
                groupOptions.serverDataPath(serverWorkingDirs.get(finalI).metaPath());

                raftServer.startRaftNode(
                        new RaftNodeId(TEST_GROUP, serverPeer), initialMembersConf, listenerFactory.get(), groupOptions
                );
            }, opts -> {});
        }

        startClient(TEST_GROUP);
    }

    @Test
    public void testStopLeader() throws Exception {
        commonTestStopNode(true);
    }

    @Test
    public void testStopFollower() throws Exception {
        commonTestStopNode(false);
    }

    private void commonTestStopNode(boolean whetherStopLeader) throws Exception {
        int leaderIndex = leaderIndex();

        int nodeToStop = whetherStopLeader
                ? leaderIndex
                : range(0, nodesCount()).filter(i -> i != leaderIndex).findFirst().orElseThrow();

        stopServer(nodeToStop);

        // Wait for some time for log spam.
        Thread.sleep(3_000);

        perClassCounters.forEach((cls, instances) -> {
            boolean correct = instances.values().stream()
                    .filter(v -> v.get() > 1)
                    .findAny()
                    .isEmpty();

            assertTrue(correct, cls.getName() + " has been written to the log more than 1 time.");
        });
    }

    private List<LogInspector> startLogInspectors() {
        List<LogInspector> logInspectors = new ArrayList<>();

        logInspectors.add(logInspector(ReplicatorGroupImpl.class, "Fail to check replicator connection"));
        logInspectors.add(logInspector(Replicator.class, "Fail to issue RPC"));
        logInspectors.add(logInspector(RaftGroupServiceImpl.class, "All peers are unavailable"));
        logInspectors.add(logInspector(AbstractClientService.class, "Fail to connect"));

        for (LogInspector logInspector : logInspectors) {
            logInspector.start();
        }

        return logInspectors;
    }

    private LogInspector logInspector(Class<?> cls, String msg) {
        var instanceCounters = new ConcurrentHashMap<String, AtomicInteger>();

        perClassCounters.put(cls, instanceCounters);

        Predicate<LogEvent> pred = event -> {
            if (event.getMessage().getFormattedMessage().contains(msg)) {
                assertTrue(event.getThreadName().startsWith("%"));
                String instanceName = event.getThreadName().split("%")[1];
                instanceCounters.computeIfAbsent(instanceName, k -> new AtomicInteger()).incrementAndGet();
            }

            return false;
        };

        return new LogInspector(cls.getName(), pred);
    }

    private static void stopLogInspectors(List<LogInspector> logInspectors) {
        for (LogInspector logInspector : logInspectors) {
            logInspector.stop();
        }
    }

    private void stopServer(int index) {
        JraftServerImpl server = servers.set(index, null);

        for (RaftNodeId nodeId : server.localNodes()) {
            server.stopRaftNode(nodeId);
        }

        assertThat(
                stopAsync(
                        new ComponentContext(),
                        server,
                        logStorageFactories.set(index, null),
                        serverServices.set(index, null),
                        vaultManagers.set(index, null)
                ),
                willCompleteSuccessfully()
        );
    }

    private int leaderIndex() {
        RaftGroupService client = clients.get(0);

        CompletableFuture<LeaderWithTerm> leaderFut = client.refreshAndGetLeaderWithTerm();

        assertThat(leaderFut, willCompleteSuccessfully());

        LeaderWithTerm leaderWithTerm = leaderFut.join();

        leaderWithTerm.leader().consistentId();

        String leaderName = servers.get(0).clusterService().topologyService().localMember().name();

        AtomicInteger leaderIndex = new AtomicInteger();

        forEachIndexed(servers, (srv, index) -> {
            if (srv.clusterService().topologyService().localMember().name().equals(leaderName)) {
                leaderIndex.set(index);
            }
        });

        return leaderIndex.get();
    }

    private static RaftGroupOptions groupOptions(RaftServer raftServer) {
        return defaults().commandsMarshaller(new ThreadLocalOptimizedMarshaller(raftServer.clusterService().serializationRegistry()));
    }
}
