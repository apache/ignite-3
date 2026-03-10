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

import static org.apache.ignite.internal.raft.server.RaftGroupOptions.defaults;
import static org.apache.ignite.raft.jraft.test.TestUtils.waitForCondition;
import static org.apache.ignite.raft.server.counter.GetValueCommand.getValueCommand;
import static org.apache.ignite.raft.server.counter.IncrementAndGetCommand.incrementAndGetCommand;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridClockImpl;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.internal.raft.server.impl.JraftServerImpl;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.raft.util.ThreadLocalOptimizedMarshaller;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.Test;

/**
 * Integration test for verifying that a new leader's clock doesn't step down after leadership transfer.
 */
class ItNewLeaderClockTest extends JraftAbstractTest {
    private static final TestReplicationGroupId TEST_GROUP = new TestReplicationGroupId("leader_with_advanced_clock_step_down");

    private static final HybridClock slowClock = new HybridClockImpl() {
        @Override
        protected long physicalTime() {
            return 0;
        }
    };
    private static final HybridClock advancedClock = new HybridClockImpl();

    /**
     * Starts a 3-node Raft cluster where two nodes have slow clock.
     */
    private void startClusterWithSlowClock() throws Exception {
        // Start first server with slow clock.
        startServer(0, raftServer -> {
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            RaftGroupOptions groupOptions = groupOptions(raftServer);

            groupOptions.setLogStorageManager(logStorageFactories.get(0));
            groupOptions.serverDataPath(serverWorkingDirs.get(0).metaPath());

            raftServer.startRaftNode(
                    new RaftNodeId(TEST_GROUP, serverPeer),
                    initialMembersConf,
                    new CounterListener(),
                    groupOptions
            );
        }, opts -> {
            opts.setClock(slowClock);
        });
        // Start second clock with advanced clock. This node is the initial leader.
        startServer(1, raftServer -> {
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            RaftGroupOptions groupOptions = groupOptions(raftServer);

            groupOptions.setLogStorageManager(logStorageFactories.get(1));
            groupOptions.serverDataPath(serverWorkingDirs.get(1).metaPath());

            raftServer.startRaftNode(
                    new RaftNodeId(TEST_GROUP, serverPeer),
                    initialMembersConf,
                    new CounterListener(),
                    groupOptions
            );
        }, opts -> {
            opts.setClock(advancedClock);
        });

        // Start third server with slow clock.
        startServer(2, raftServer -> {
            String localNodeName = raftServer.clusterService().topologyService().localMember().name();

            Peer serverPeer = initialMembersConf.peer(localNodeName);

            RaftGroupOptions groupOptions = groupOptions(raftServer);

            groupOptions.setLogStorageManager(logStorageFactories.get(2));
            groupOptions.serverDataPath(serverWorkingDirs.get(2).metaPath());

            raftServer.startRaftNode(
                    new RaftNodeId(TEST_GROUP, serverPeer),
                    initialMembersConf,
                    new CounterListener(),
                    groupOptions
            );
        }, opts -> {
            opts.setClock(slowClock);
        });

        startClient(TEST_GROUP);
    }

    /**
     * Tests that a new leader's clock doesn't step down after leadership transfer.
     * Verifies that:
     * 1. Writes succeed with node 1 as leader (advanced clock).
     * 2. After stopping node 1, a new leader is elected (node 0 or 2 with slow clock).
     * 3. The new leader's clock is not lower than the last applied command time.
     *
     * @throws Exception If test fails.
     */
    @Test
    public void testNewLeaderClockDoesNotStepDown() throws Exception {
        startClusterWithSlowClock();
        RaftGroupService client = clients.get(0);
        client.refreshLeader().get();
        assertNotNull(client.leader(), "Initial leader should be elected");
        // Force leadership to node 1 (server index 1).
        String node1Name = servers.get(1).clusterService().topologyService().localMember().name();
        Peer node1Peer = initialMembersConf.peer(node1Name);
        client.transferLeadership(node1Peer).get();
        assertTrue(
                waitForCondition(() -> {
                    client.refreshLeader().join();
                    return node1Peer.equals(client.leader());
                }, 10_000),
                "Leadership should transfer to node 1 with advanced clock"
        );
        long expectedValue = 0;
        long currentTimestamp = System.currentTimeMillis() * 2;
        for (int i = 0; i < 100; i++) {
            long increment = i + 1;
            expectedValue += increment;
            HybridTimestamp highTime = HybridTimestamp.hybridTimestamp(currentTimestamp--);
            client.<Long>run(incrementAndGetCommand(increment, highTime)).get();
        }
        // Verify the writes succeeded.
        Long valueBeforeUnisolate = client.<Long>run(getValueCommand()).get();
        assertEquals(expectedValue, valueBeforeUnisolate, "All writes should have succeeded");
        HybridTimestamp lastAppliedCmdTsFromAdvancedClockLeader = advancedClock.update(HybridTimestamp.MIN_VALUE);
        RaftNodeId node1RaftNodeId = new RaftNodeId(TEST_GROUP, node1Peer);
        servers.get(1).blockMessages(node1RaftNodeId, (msg, peerId) -> true);
        // Now stop the leader.
        JraftServerImpl jraftServer = servers.get(1);
        jraftServer.stopRaftNodes(TEST_GROUP);
        assertTrue(
                waitForCondition(() -> {
                    client.refreshLeader().join();
                    return !node1Peer.equals(client.leader());
                }, 30_000),
                "Leadership should transfer to a slow node"
        );
        HybridTimestamp slow = slowClock.update(HybridTimestamp.MIN_VALUE);
        log.info("clocks slow: {}, reg: {}", slow, lastAppliedCmdTsFromAdvancedClockLeader);
        // Verify that new leader's clock is not lower than the last applied command time.
        assertTrue(slow.compareTo(lastAppliedCmdTsFromAdvancedClockLeader) >= 0);
    }

    /**
     * Creates Raft group options with the proper marshaller.
     *
     * @param raftServer Raft server instance.
     * @return Configured Raft group options.
     */
    private static RaftGroupOptions groupOptions(RaftServer raftServer) {
        return defaults().commandsMarshaller(
                new ThreadLocalOptimizedMarshaller(raftServer.clusterService().serializationRegistry())
        );
    }
}
