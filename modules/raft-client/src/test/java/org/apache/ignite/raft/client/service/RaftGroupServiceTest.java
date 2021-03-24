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

import java.util.Arrays;
import java.util.Timer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

@ExtendWith(MockitoExtension.class)
public class RaftGroupServiceTest {
    /** */
    private static Peer[] NODES = new Peer[]{
        new Peer(new NetworkMember("node1")),
        new Peer(new NetworkMember("node2")),
        new Peer(new NetworkMember("node3"))};

    /** */
    private static RaftClientMessageFactory FACTORY = new RaftClientMessageFactoryImpl();

    /** */
    private volatile Peer prevLeader = null;

    /** */
    private volatile Peer curLeader = NODES[0];

    /** Election state: 0 - running, 1 - finished. */
    private volatile int electionState = 1;

    /** */
    private static final int TIMEOUT = 1_500;

    /** */
    private static final int DELAY = 500;

    @Mock
    private NetworkCluster cluster;

    @Test
    public void testRefreshLeaderStable() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(curLeader, service.leader());
    }

    @Test
    public void testRefreshLeaderElectionInProgress() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        electionState = 0;
        curLeader = null;

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        assertNull(service.leader());

        try {
            service.refreshLeader().get();

            fail("Should fail");
        }
        catch (RaftException e) {
            assertEquals(RaftErrorCode.NO_LEADER, e.errorCode());
        }
    }

    @Test
    public void testRefreshLeaderWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        try {
            service.refreshLeader().get();

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
        mockUserInput(cluster);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        service.refreshLeader().get();

        CompletableFuture<TestResponse> fut = service.run(new TestCommand());

        TestResponse output1 = fut.get();

        assertNotNull(output1);
    }

    @Test
    public void testUserRequestWithLazyInitLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        assertNull(service.leader());

        CompletableFuture<TestResponse> fut = service.run(new TestCommand());

        TestResponse output = fut.get();

        assertNotNull(output);

        assertEquals(curLeader, service.leader());
    }

    @Test
    public void testUserRequestWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput(cluster);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, FACTORY, TIMEOUT, Arrays.asList(NODES), false, DELAY, new Timer());

        service.refreshLeader().get();

        CompletableFuture<TestResponse> fut = service.run(new TestCommand());

        TestResponse output = fut.get();

        assertNotNull(output);
    }

    public static class TestCommand implements WriteCommand {
    }

    public static class TestResponse {
    }

    public void mockUserInput(NetworkCluster cluster) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                UserResponse resp = FACTORY.createUserResponse().setResponse(new TestResponse()).build();

                return completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(curLeader.getNode()), argThat(new ArgumentMatcher<UserRequest>() {
            @Override public boolean matches(UserRequest arg) {
                return arg.request() instanceof TestCommand;
            }
        }), anyLong());
    }

    public void mockLeaderRequest(NetworkCluster cluster, boolean timeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                GetLeaderResponse resp = FACTORY.createGetLeaderResponse().setLeaderId(curLeader).build();

                return timeout ? failedFuture(new TimeoutException()) : completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(any(), any(GetLeaderRequest.class), anyLong());
    }
}
