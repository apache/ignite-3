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

package org.apache.ignite.raft.client.service;

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static java.util.List.of;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;

@ExtendWith(MockitoExtension.class)
public class RaftGroupServiceTest {
    /** */
    private static List<Peer> NODES = of(new Peer(new NetworkMember("node1")), new Peer(new NetworkMember("node2")),
        new Peer(new NetworkMember("node3")));

    /** */
    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** */
    private volatile Peer leader = NODES.get(0);

    /** Call timeout. */
    private static final int TIMEOUT = 1000;

    /** Retry delay. */
    private static final int DELAY = 200;

    /** Cluster. */
    @Mock
    private NetworkCluster cluster;

    @BeforeEach
    void before(TestInfo testInfo) {
        System.out.println(">>>> Starting test " + testInfo.getTestMethod().orElseThrow().getName());
    }

    @Test
    public void testRefreshLeaderStable() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(leader, service.leader());
    }

    @Test
    public void testRefreshLeaderNotElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        // Simulate running elections.
        leader = null;

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        assertNull(service.leader());

        try {
            service.refreshLeader().get();

            fail("Should fail");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testRefreshLeaderElectedAfterDelay() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        // Simulate running elections.
        leader = null;

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override public void run() {
                leader = NODES.get(0);
            }
        }, 500);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, timer);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testRefreshLeaderWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        try {
            service.refreshLeader().get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testUserRequestLeaderElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        service.refreshLeader().get();

        CompletableFuture fut = service.run(new TestCommand());

        TestResponse output1 = (TestResponse) fut.get();

        assertNotNull(output1);
    }

    @Test
    public void testUserRequestLazyInitLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        assertNull(service.leader());

        TestResponse resp = (TestResponse) service.run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(leader, service.leader());
    }

    @Test
    public void testUserRequestWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, false, DELAY, new Timer());

        try {
            service.run(new TestCommand()).get();

            fail();
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testUserRequestLeaderNotElected() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, new Timer());

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        try {
            service.run(new TestCommand()).get();

            fail("Expecting timeout");
        }
        catch (ExecutionException e) {
            assertTrue(e.getCause() instanceof TimeoutException);
        }
    }

    @Test
    public void testUserRequestLeaderElectedAfterDelay() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, new Timer());

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        this.leader = null;

        assertEquals(leader, service.leader());

        Timer timer = new Timer();

        timer.schedule(new TimerTask() {
            @Override public void run() {
                RaftGroupServiceTest.this.leader = NODES.get(0);
            }
        }, 500);

        TestResponse resp = (TestResponse) service.run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(NODES.get(0), service.leader());
    }

    @Test
    public void testUserRequestLeaderChanged() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, NODES, true, DELAY, new Timer());

        Peer leader = this.leader;

        assertEquals(leader, service.leader());

        Peer newLeader = NODES.get(1);

        this.leader = newLeader;

        assertEquals(leader, service.leader());
        assertNotEquals(leader, newLeader);

        // Runs the command on an old leader. It should respond with leader changed error, when transparently retry.
        TestResponse resp = (TestResponse) service.run(new TestCommand()).get();

        assertNotNull(resp);

        assertEquals(newLeader, service.leader());
    }

    public void mockUserInput(NetworkCluster cluster, boolean simulateTimeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                NetworkMember target = invocation.getArgument(0);

                if (simulateTimeout)
                    return failedFuture(new TimeoutException());

                Object resp;

                if (leader == null) {
                    resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
                }
                else if (target != leader.getNode()){
                    resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.LEADER_CHANGED).newLeader(leader).build();
                }
                else {
                    resp = FACTORY.actionResponse().result(new TestResponse()).build();
                }

                return completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(any(), argThat(new ArgumentMatcher<ActionRequest>() {
            @Override public boolean matches(ActionRequest arg) {
                return arg.command() instanceof TestCommand;
            }
        }), anyLong());
    }

    /**
     * @param cluster Cluster.
     * @param simulateTimeout {@code True} to simulate request timeout.
     */
    public void mockLeaderRequest(NetworkCluster cluster, boolean simulateTimeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                if (simulateTimeout)
                    return failedFuture(new TimeoutException());

                Object resp;

                Peer leader0 = leader;

                if (leader0 == null) {
                    resp = FACTORY.raftErrorResponse().errorCode(RaftErrorCode.NO_LEADER).build();
                }
                else {
                    resp = FACTORY.getLeaderResponse().leader(leader0).build();
                }

                return completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(any(), any(GetLeaderRequest.class), anyLong());
    }

    public static class TestCommand implements WriteCommand {
    }

    public static class TestResponse {
    }
}
