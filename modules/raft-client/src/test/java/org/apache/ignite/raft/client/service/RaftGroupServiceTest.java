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

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.apache.ignite.raft.client.service.impl.RaftGroupServiceImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.stubbing.Answer;

import static java.util.Collections.singleton;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl.MESSAGE_FACTORY;
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
    private static Peer LEADER = new Peer(new NetworkMember("test"));

    /** */
    private static final int TIMEOUT = 5_000;

    @Mock
    private NetworkCluster cluster;

    @Test
    public void testRefreshLeader() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, MESSAGE_FACTORY, TIMEOUT, this::resolve,false);

        assertNull(service.leader());

        service.refreshLeader().get();

        assertEquals(LEADER, service.leader());
    }

    @Test
    public void testRefreshLeaderWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, true);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, MESSAGE_FACTORY, TIMEOUT, this::resolve, false);

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
        mockUserInput1(cluster);
        mockUserInput2(cluster);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, MESSAGE_FACTORY, TIMEOUT, this::resolve, false);

        service.refreshLeader().get();

        CompletableFuture<TestOutput1> fut1 = service.run(new TestInput1());

        TestOutput1 output1 = fut1.get();

        assertNotNull(output1);

        CompletableFuture<TestOutput2> fut2 = service.run(new TestInput2());

        TestOutput2 output2 = fut2.get();

        assertNotNull(output2);
    }

    @Test
    public void testUserRequestWithTimeout() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput1(cluster);
        mockUserInput2(cluster);

        RaftGroupService service =
            new RaftGroupServiceImpl(groupId, cluster, MESSAGE_FACTORY, TIMEOUT, this::resolve, false);

        service.refreshLeader().get();

        CompletableFuture<TestOutput1> fut1 = service.run(new TestInput1());

        TestOutput1 output1 = fut1.get();

        assertNotNull(output1);

        CompletableFuture<TestOutput2> fut2 = service.run(new TestInput2());

        TestOutput2 output2 = fut2.get();

        assertNotNull(output2);
    }

    /**
     * @param grpId Group id.
     * @return Members.
     */
    private Set<NetworkMember> resolve(String grpId) {
        return singleton(LEADER.getNode());
    }

    public static class TestInput1 implements WriteCommand {
    }

    public static class TestOutput1 {
    }

    public static class TestInput2 implements ReadCommand {
    }

    public static class TestOutput2 {
    }

    public static void mockUserInput1(NetworkCluster cluster) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                UserResponse resp = MESSAGE_FACTORY.createUserResponse().setResponse(new TestOutput1()).build();

                return completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), argThat(new ArgumentMatcher<UserRequest>() {
            @Override public boolean matches(UserRequest arg) {
                return arg.request() instanceof TestInput1;
            }
        }), anyLong());
    }

    public static void mockUserInput2(NetworkCluster cluster) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                UserResponse resp = MESSAGE_FACTORY.createUserResponse().setResponse(new TestOutput2()).build();

                return completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), argThat(new ArgumentMatcher<UserRequest>() {
            @Override public boolean matches(UserRequest arg) {
                return arg.request() instanceof TestInput2;
            }
        }), anyLong());
    }

    public static void mockLeaderRequest(NetworkCluster cluster, boolean timeout) {
        Mockito.doAnswer(new Answer() {
            @Override public Object answer(InvocationOnMock invocation) throws Throwable {
                GetLeaderResponse resp = MESSAGE_FACTORY.createGetLeaderResponse().setLeaderId(LEADER).build();

                return timeout ? failedFuture(new TimeoutException()) : completedFuture(resp);
            }
        }).when(cluster).sendWithResponse(eq(LEADER.getNode()), any(GetLeaderRequest.class), anyLong());
    }
}
