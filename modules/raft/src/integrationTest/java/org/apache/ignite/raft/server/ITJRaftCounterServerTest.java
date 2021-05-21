/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.File;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.service.RaftGroupService;
import org.apache.ignite.raft.jraft.StateMachine;
import org.apache.ignite.raft.jraft.util.Utils;
import org.apache.ignite.raft.server.impl.JRaftServerImpl;
import org.junit.jupiter.api.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class ITJRaftCounterServerTest extends ITJRaftServerAbstractTest {
    /** */
    private static final String COUNTER_GROUP_0 = "counter0";

    /** */
    private static final String COUNTER_GROUP_1 = "counter1";

    /** */
    private Supplier<CounterCommandListener> listenerFactory = () -> new CounterCommandListener();

    private void startCluster() {
        startServer(0);
        startServer(1);
        startServer(2);

        List<Peer> initialConf = servers.stream().map(s -> new Peer(s.clusterService().topologyService().localMember().
            address())).collect(Collectors.toList());

        servers.forEach(s -> s.startRaftGroup(COUNTER_GROUP_0, listenerFactory.get(), initialConf));
        servers.forEach(s -> s.startRaftGroup(COUNTER_GROUP_1, listenerFactory.get(), initialConf));

        RaftGroupService client0 = startClient(COUNTER_GROUP_0);
        RaftGroupService client1 = startClient(COUNTER_GROUP_1);
    }

    /**
     */
    @Test
    public void testRefreshLeader() throws Exception {
        startCluster();

        Peer leader = clients.get(0).leader();

        assertNull(leader);

        clients.get(0).refreshLeader().get();

        assertNotNull(clients.get(0).leader());

        leader = clients.get(1).leader();

        assertNull(leader);

        clients.get(1).refreshLeader().get();

        assertNotNull(clients.get(1).leader());
    }

    /**
     * @throws Exception
     */
    @Test
    public void testCounterCommandListener() throws Exception {
        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        assertNotNull(client1.leader());
        assertNotNull(client2.leader());

        assertEquals(2, client1.<Long>run(new IncrementAndGetCommand(2)).get());
        assertEquals(2, client1.<Long>run(new GetValueCommand()).get());
        assertEquals(3, client1.<Long>run(new IncrementAndGetCommand(1)).get());
        assertEquals(3, client1.<Long>run(new GetValueCommand()).get());

        assertEquals(4, client2.<Long>run(new IncrementAndGetCommand(4)).get());
        assertEquals(4, client2.<Long>run(new GetValueCommand()).get());
        assertEquals(7, client2.<Long>run(new IncrementAndGetCommand(3)).get());
        assertEquals(7, client2.<Long>run(new GetValueCommand()).get());
    }

    @Test
    public void testCreateSnapshot() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        client1.snapshot(server.localPeer(COUNTER_GROUP_0)).get();

        long val2 = applyIncrements(client2, 1, 20);

        assertEquals(sum(20), val2);

        client2.snapshot(server.localPeer(COUNTER_GROUP_1)).get();

        String snapshotDir0 = server.getServerDataPath(COUNTER_GROUP_0) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir0).list().length);

        String snapshotDir1 = server.getServerDataPath(COUNTER_GROUP_1) + File.separator + "snapshot";
        assertEquals(1, new File(snapshotDir1).list().length);
    }

    @Test
    public void testCreateSnapshotGracefulFailure() throws Exception {
        listenerFactory = () -> new CounterCommandListener() {
            @Override public void onSnapshotSave(String path, Consumer<Boolean> doneClo) {
                doneClo.accept(Boolean.FALSE);
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        Peer peer = server.localPeer(COUNTER_GROUP_0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        try {
            client1.snapshot(peer).get();

            fail();
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    @Test
    public void testCreateSnapshotAbnormalFailure() throws Exception {
        listenerFactory = () -> new CounterCommandListener() {
            @Override public void onSnapshotSave(String path, Consumer<Boolean> doneClo) {
                throw new IgniteInternalException("Very bad");
            }
        };

        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        RaftServer server = servers.get(0);

        Peer peer = server.localPeer(COUNTER_GROUP_0);

        long val = applyIncrements(client1, 1, 10);

        assertEquals(sum(10), val);

        try {
            client1.snapshot(peer).get();

            fail();
        }
        catch (Exception e) {
            assertTrue(e.getCause() instanceof RaftException);
        }
    }

    @Test
    public void testFollowerCatchUp() throws Exception {
        startCluster();

        RaftGroupService client1 = clients.get(0);
        RaftGroupService client2 = clients.get(1);

        client1.refreshLeader().get();
        client2.refreshLeader().get();

        Peer leader1 = client1.leader();
        assertNotNull(leader1);

        Peer leader2 = client2.leader();
        assertNotNull(leader2);

        applyIncrements(client1, 0, 10);
        applyIncrements(client2, 0, 20);

        client1.snapshot(leader1).get();
        client2.snapshot(leader2).get();

        RaftServer toStop = null;

        for (RaftServer server : servers) {
            Peer peer = server.localPeer(COUNTER_GROUP_0);

            if (!peer.equals(leader1) && !peer.equals(leader2)) {
                toStop = server;
                break;
            }
        }

        servers.remove(toStop);

        String serverDataPath0 = toStop.getServerDataPath(COUNTER_GROUP_0);
        String serverDataPath1 = toStop.getServerDataPath(COUNTER_GROUP_1);

        toStop.shutdown();

        applyIncrements(client1, 11, 20);
        applyIncrements(client2, 21, 30);

        Utils.delete(new File(serverDataPath0));
        Utils.delete(new File(serverDataPath1));

        JRaftServerImpl raftServerNew = (JRaftServerImpl) startServer(1);

        Thread.sleep(5000);

        org.apache.ignite.raft.jraft.RaftGroupService s0 = raftServerNew.raftGroupService(COUNTER_GROUP_0);
        org.apache.ignite.raft.jraft.RaftGroupService s1 = raftServerNew.raftGroupService(COUNTER_GROUP_1);

        JRaftServerImpl.DelegatingStateMachine fsm0 = (JRaftServerImpl.DelegatingStateMachine) s0.getRaftNode().getOptions().getFsm();
        JRaftServerImpl.DelegatingStateMachine fsm1 = (JRaftServerImpl.DelegatingStateMachine) s1.getRaftNode().getOptions().getFsm();

        System.out.println();
    }

    @Test
    public void testRestartAndApply() {

    }


    /**
     * @param client The client
     * @param start Start element.
     * @param stop Stop element.
     * @return The counter value.
     * @throws Exception If failed.
     */
    private long applyIncrements(RaftGroupService client, int start, int stop) throws Exception {
        long val = 0;

        for (int i = start; i <= stop; i++) {
            val = client.<Long>run(new IncrementAndGetCommand(i)).get();

            LOG.info("Val=" + val + ", i=" + i);
        }

        return val;
    }

    /**
     * Calculates a progression sum.
     *
     * @param until Until value.
     * @return The sum.
     */
    public long sum(long until) {
        return (1 + until) * until / 2;
    }
}
