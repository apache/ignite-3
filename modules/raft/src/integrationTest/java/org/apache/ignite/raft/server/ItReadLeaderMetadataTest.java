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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.Supplier;
import org.apache.ignite.internal.raft.Peer;
import org.apache.ignite.internal.raft.RaftNodeId;
import org.apache.ignite.internal.raft.service.LeaderMetadata;
import org.apache.ignite.internal.raft.service.LeaderWithTerm;
import org.apache.ignite.internal.raft.service.RaftGroupService;
import org.apache.ignite.internal.replicator.TestReplicationGroupId;
import org.apache.ignite.raft.server.counter.CounterListener;
import org.junit.jupiter.api.Test;

/**
 * Reade metadata tests.
 */
public class ItReadLeaderMetadataTest extends JraftAbstractTest {
    /**
     * Listener factory.
     */
    private final Supplier<CounterListener> listenerFactory = CounterListener::new;

    /** Raft group id. */
    private static final TestReplicationGroupId GROUP_ID = new TestReplicationGroupId("GRP_1");

    @Test
    public void test() throws Exception {
        log.info("Test here.");

        for (int i = 0; i < 3; i++) {
            startServer(i, raftServer -> {
                String localNodeName = raftServer.clusterService().topologyService().localMember().name();

                Peer serverPeer = initialMembersConf.peer(localNodeName);

                raftServer.startRaftNode(
                        new RaftNodeId(GROUP_ID, serverPeer), initialMembersConf, listenerFactory.get(), defaults()
                );
            }, opts -> {});
        }

        var raftClient1 = startClient(GROUP_ID);
        var raftClient2 = startClient(GROUP_ID);

        raftClient1.refreshMembers(true).get();
        raftClient1.refreshLeader().get();
        raftClient2.refreshMembers(true).get();
        raftClient2.refreshLeader().get();

        assertEquals(raftClient1.leader(), raftClient2.leader());

        checkLeaderConsistency(raftClient1, raftClient2);

        Peer curLeader = raftClient1.leader();

        Peer newLeader = raftClient1.peers().stream().filter(p -> !p.equals(curLeader)).findAny().get();

        assertNotNull(newLeader);

        log.info("Transfer leadership [cur={}, new={}]", curLeader, newLeader);

        raftClient1.transferLeadership(newLeader).get();

        assertNotEquals(raftClient1.leader(), raftClient2.leader());

        log.info("Leader transferred [client1={}, client2={}]", raftClient1.leader(), raftClient2.leader());

        checkLeaderConsistency(raftClient1, raftClient2);

        assertEquals(raftClient1.leader(), raftClient2.leader());
    }

    /**
     * Checks that leader refreshed consistency on both clients.
     *
     * @param raftClient1 Raft client 1.
     * @param raftClient2 Raft client 2
     * @throws Exception If failed.
     */
    private static void checkLeaderConsistency(RaftGroupService raftClient1, RaftGroupService raftClient2) throws Exception {
        LeaderWithTerm leaderWithTerm = raftClient1.refreshAndGetLeaderWithTerm().get();
        LeaderMetadata leaderMetadata = raftClient2.readLeaderMetadata().get();

        long indexOnClient2 = leaderMetadata.getIndex();

        assertTrue(leaderWithTerm.term() <= leaderMetadata.getTerm());

        if (leaderWithTerm.term() == leaderMetadata.getTerm()) {
            assertEquals(leaderWithTerm.leader(), leaderMetadata.getLeader());
        }

        leaderMetadata = raftClient1.readLeaderMetadata().get();
        leaderWithTerm = raftClient2.refreshAndGetLeaderWithTerm().get();

        long indexOnClient1 = leaderMetadata.getIndex();

        assertTrue(leaderWithTerm.term() <= leaderMetadata.getTerm());

        if (leaderWithTerm.term() == leaderMetadata.getTerm()) {
            assertEquals(leaderWithTerm.leader(), leaderMetadata.getLeader());
        }

        assertEquals(indexOnClient2, indexOnClient1);
    }
}
