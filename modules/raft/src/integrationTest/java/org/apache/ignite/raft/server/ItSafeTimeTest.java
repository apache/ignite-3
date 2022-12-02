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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.internal.TestHybridClock;
import org.apache.ignite.internal.hlc.HybridClock;
import org.apache.ignite.internal.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.ReplicationGroupOptions;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.ReplicationGroupId;
import org.apache.ignite.internal.util.PendingComparableValuesTracker;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.ignite.raft.server.counter.IncrementAndGetCommand;
import org.junit.jupiter.api.Test;

/**
 * Integration test for checking safe time propagation.
 */
public class ItSafeTimeTest extends JraftAbstractTest {
    /** Raft group id. */
    private static final ReplicationGroupId RAFT_GROUP_ID = new TestReplicationGroupId("testGroup");

    /** Nodes count. */
    private static final int NODES = 3;

    /** Hybrid clocks. */
    private List<HybridClock> clocks = new ArrayList<>();

    /** Safe tims clocks. */
    private List<PendingComparableValuesTracker<HybridTimestamp>> safeTimeContainers = new ArrayList<>();

    /**
     * Starts a cluster for the test.
     *
     * @throws Exception If failed.
     */
    private void startCluster() throws Exception {
        for (int i = 0; i < NODES; i++) {
            HybridClock clock = new TestHybridClock(() -> 1L);
            PendingComparableValuesTracker<HybridTimestamp> safeTime = new PendingComparableValuesTracker<>(clock.now());

            clocks.add(clock);
            safeTimeContainers.add(safeTime);

            startServer(i,
                    raftServer -> {
                        RaftGroupOptions groupOptions = defaults()
                                .replicationGroupOptions(new ReplicationGroupOptions().safeTime(safeTime));

                        raftServer.startRaftGroup(RAFT_GROUP_ID, new CounterListener(), initialConf, groupOptions);
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
        Peer leader = client1.leader();

        final int leaderIndex = initialConf.indexOf(leader);

        assertTrue(leaderIndex >= 0);

        final long leaderPhysicalTime = 100;

        clocks.get(leaderIndex).update(new HybridTimestamp(leaderPhysicalTime, 0));

        client1.run(new IncrementAndGetCommand(1)).get();

        waitForCondition(() -> {
            for (int i = 0; i < NODES; i++) {
                // As current time provider for safe time clocks always returns 1,
                // the only way for physical component to reach leaderPhysicalTime is safe time propagation mechanism.
                if (i != leaderIndex && safeTimeContainers.get(i).current().getPhysical() != leaderPhysicalTime) {
                    return false;
                }
            }

            return true;
        }, 2000);
    }
}
