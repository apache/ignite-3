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

package org.apache.ignite.raft.client.rpc;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.MockUtils.TestInput1;
import org.apache.ignite.raft.client.MockUtils.TestInput2;
import org.apache.ignite.raft.client.MockUtils.TestOutput1;
import org.apache.ignite.raft.client.MockUtils.TestOutput2;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static java.util.Collections.singleton;
import static org.apache.ignite.raft.client.MockUtils.LEADER;
import static org.apache.ignite.raft.client.MockUtils.mockLeaderRequest;
import static org.apache.ignite.raft.client.MockUtils.mockUserInput1;
import static org.apache.ignite.raft.client.MockUtils.mockUserInput2;
import static org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl.MESSAGE_FACTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(MockitoExtension.class)
public class RaftGroupRpcClientTest {
    @Mock
    private NetworkCluster cluster;

    @Test
    public void testRefreshLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(cluster, MESSAGE_FACTORY, 5_000, singleton(LEADER.getNode()));

        PeerId leaderId = client.refreshLeader(groupId).get();

        assertEquals(LEADER, client.state(groupId).leader());
        assertEquals(LEADER, leaderId);
    }

    @Test
    public void testRefreshLeaderMultithreaded() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(cluster, MESSAGE_FACTORY, 5_000, singleton(LEADER.getNode()));

        int cnt = 20;

        Thread[] runners = new Thread[cnt];

        CountDownLatch l = new CountDownLatch(cnt);

        for (int i = 0; i < runners.length; i++) {
            runners[i] = new Thread(new Runnable() {
                @Override public void run() {
                    l.countDown();
                    try {
                        l.await();
                    }
                    catch (InterruptedException e) {
                        // Ignored.
                    }

                    try {
                        PeerId leaderId = client.refreshLeader(groupId).get();

                        assertEquals(LEADER, client.state(groupId).leader());
                        assertEquals(LEADER, leaderId);
                    }
                    catch (Exception e) {
                        fail(e);
                    }
                }
            });
            runners[i].setName("Executor-" + i);
            runners[i].start();
        }

        for (int i = 0; i < runners.length; i++)
            runners[i].join();
    }

    @Test
    public void testRefreshLeaderTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, true);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(cluster, MESSAGE_FACTORY, 5_000, singleton(LEADER.getNode()));

        try {
            client.refreshLeader(groupId).get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testUserRequest() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput1(cluster);
        mockUserInput2(cluster);

        RaftGroupRpcClient client = new RaftGroupRpcClientImpl(cluster, MESSAGE_FACTORY, 5_000, singleton(LEADER.getNode()));

        UserRequest req1 =
            client.factory().createUserRequest().setGroupId(groupId).setRequest(new TestInput1()).build();

        assertTrue(client.submit(req1).get().response() instanceof TestOutput1);

        UserRequest req2 =
            client.factory().createUserRequest().setGroupId(groupId).setRequest(new TestInput2()).build();

        assertTrue(client.submit(req2).get().response() instanceof TestOutput2);

        // Expecting raft group state to be transparently loaded on first request.
        assertEquals(LEADER, client.state(groupId).leader());
    }
}
