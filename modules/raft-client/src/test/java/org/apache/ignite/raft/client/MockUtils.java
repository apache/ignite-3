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

package org.apache.ignite.raft.client;

import java.util.concurrent.TimeoutException;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;
import static org.apache.ignite.raft.client.message.impl.RaftClientMessageFactoryImpl.MESSAGE_FACTORY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

public class MockUtils {
    public static PeerId LEADER = new PeerId(new NetworkMember("test"));

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
