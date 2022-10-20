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
import org.apache.ignite.hlc.HybridClock;
import org.apache.ignite.hlc.HybridTimestamp;
import org.apache.ignite.internal.raft.server.RaftGroupOptions;
import org.apache.ignite.internal.raft.server.ReplicationGroupOptions;
import org.apache.ignite.internal.testframework.WorkDirectoryExtension;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.apache.ignite.raft.server.counter.IncrementAndGetCommand;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration test for checking safe time propagation.
 */
@ExtendWith(WorkDirectoryExtension.class)
public class ItSafeTimeTest extends JRaftAbstractTest {
    /** Raft group name. */
    private static final String RAFT_GROUP_NAME = "testGroup";

    /** Nodes count. */
    private static final int NODES = 3;

    /** Hybrid clocks. */
    private List<HybridClock> clocks = new ArrayList<>();

    /** Safe tims clocks. */
    private List<HybridClock> safeTimeClocks = new ArrayList<>();

    /** Before each. */
    @BeforeEach
    @Override
    void before() {
        LOG.info(">>>>>>>>>>>>>>> Start test method: {}", testInfo.getTestMethod().orElseThrow().getName());

        super.before();
    }

    /** After each. */
    @AfterEach
    @Override
    protected void after() throws Exception {
        super.after();

        LOG.info(">>>>>>>>>>>>>>> End test method: {}", testInfo.getTestMethod().orElseThrow().getName());
    }

    /**
     * Starts a cluster for the test.
     *
     * @throws Exception If failed.
     */
    private void startCluster() throws Exception {
        for (int i = 0; i < NODES; i++) {
            HybridClock clock = new HybridClock(() -> 1L);
            HybridClock safeTimeClock = new HybridClock(() -> 1L);

            clocks.add(clock);
            safeTimeClocks.add(safeTimeClock);

            startServer(i,
                    raftServer -> {
                        RaftGroupOptions groupOptions = defaults()
                                .replicationGroupOptions(new ReplicationGroupOptions().safeTimeClock(safeTimeClock));

                        raftServer.startRaftGroup(RAFT_GROUP_NAME, new CounterListener(), INITIAL_CONF, groupOptions);
                    },
                    opts -> {
                        opts.setClock(clock);
                        opts.setSafeTimeClock(safeTimeClock);
                    }
            );
        }

        startClient(RAFT_GROUP_NAME);
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

        final int leaderIndex = INITIAL_CONF.indexOf(leader);

        assertTrue(leaderIndex >= 0);

        clocks.get(leaderIndex).sync(new HybridTimestamp(100, 50));

        client1.run(new IncrementAndGetCommand(1)).get();

        waitForCondition(() -> {
            for (int i = 0; i < NODES; i++) {
                // As current time provider for safe time clocks always return 1,
                // the only way for physical component to reach 100 is safe time propagation mechanism.
                if (i != leaderIndex && safeTimeClocks.get(i).now().getPhysical() != 100) {
                    return false;
                }
            }

            return true;
        }, 2000);
    }
}
