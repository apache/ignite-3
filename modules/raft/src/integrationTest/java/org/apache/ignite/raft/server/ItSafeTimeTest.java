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
import static org.apache.ignite.raft.server.counter.IncrementAndGetCommand.incrementAndGetCommand;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.ReplicationGroupOptions;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.Test;

/**
 * Integration test for checking safe time propagation.
 */
public class ItSafeTimeTest extends JraftAbstractTest {
    /** Raft group id. */
    private static final ReplicationGroupId RAFT_GROUP_ID = new TestReplicationGroupId("testGroup");

    /** Hybrid clocks. */
    private final Map<String, HybridClock> clocks = new HashMap<>();

    /** Safe times clocks. */
    private final Map<String, PendingComparableValuesTracker<HybridTimestamp>> safeTimeContainers = new HashMap<>();

    /**
     * Starts a cluster for the test.
     *
     * @throws Exception If failed.
     */
    private void startCluster() throws Exception {
        for (int i = 0; i < NODES; i++) {
            HybridClock clock = new TestHybridClock(() -> 1L);
            PendingComparableValuesTracker<HybridTimestamp> safeTime = new PendingComparableValuesTracker<>(clock.now());

            startServer(i,
                    raftServer -> {
                        String localMemberName = raftServer.clusterService().topologyService().localMember().name();

                        clocks.put(localMemberName, clock);
                        safeTimeContainers.put(localMemberName, safeTime);

                        RaftGroupOptions groupOptions = defaults()
                                .replicationGroupOptions(new ReplicationGroupOptions().safeTime(safeTime));

                        var nodeId = new RaftNodeId(RAFT_GROUP_ID, initialConf.peer(localMemberName));

                        raftServer.startRaftNode(nodeId, initialConf, new CounterListener(), groupOptions);
                    },
                    opts -> {
                        opts.setClock(clock);
                        opts.setSafeTimeTracker(safeTime);
                    }
            );
        }

        startClient(RAFT_GROUP_ID);
    }

    /**
     * Tests if a raft group become unavailable in case of a critical error.
     */
    @Test
    public void test() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);

        client1.refreshLeader().get();

        String leaderName = client1.leader().consistentId();

        final long leaderPhysicalTime = 100;

        clocks.get(leaderName).update(new HybridTimestamp(leaderPhysicalTime, 0));

        client1.run(incrementAndGetCommand(1)).get();

        assertTrue(waitForCondition(() -> {
            for (Peer peer : initialConf.peers()) {
                String consistentId = peer.consistentId();

                PendingComparableValuesTracker<HybridTimestamp> safeTimeContainer = safeTimeContainers.get(consistentId);

                // As current time provider for safe time clocks always returns 1,
                // the only way for physical component to reach leaderPhysicalTime is safe time propagation mechanism.
                if (!consistentId.equals(leaderName) && safeTimeContainer.current().getPhysical() != leaderPhysicalTime) {
                    return false;
                }
            }

            return true;
        }, 2000));
    }
}
