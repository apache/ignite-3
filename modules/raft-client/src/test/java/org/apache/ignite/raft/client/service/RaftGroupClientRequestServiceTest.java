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

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.raft.client.MockUtils.TestInput1;
import org.apache.ignite.raft.client.MockUtils.TestInput2;
import org.apache.ignite.raft.client.MockUtils.TestOutput1;
import org.apache.ignite.raft.client.MockUtils.TestOutput2;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.rpc.impl.RaftGroupRpcClientImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupClientRequestServiceImpl;
import org.apache.ignite.raft.client.service.impl.RaftGroupManagementServiceImpl;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(MockitoExtension.class)
public class RaftGroupClientRequestServiceTest {
    @Mock
    private NetworkCluster cluster;

    @Test
    public void testUserRequest() throws Exception {
        String groupId = "test";

        mockLeaderRequest(cluster, false);
        mockUserInput1(cluster);
        mockUserInput2(cluster);

        RaftGroupRpcClient rpcClient = new RaftGroupRpcClientImpl(cluster, MESSAGE_FACTORY, 5_000, singleton(LEADER.getNode()));

        RaftGroupClientRequestService reqService = new RaftGroupClientRequestServiceImpl(rpcClient, groupId);
        RaftGroupManagmentService mgmtService = new RaftGroupManagementServiceImpl(rpcClient, groupId);

        assertEquals(LEADER, mgmtService.getLeader());

        CompletableFuture<TestOutput1> fut1 = reqService.submit(new TestInput1());

        TestOutput1 output1 = fut1.get();

        assertNotNull(output1);

        CompletableFuture<TestOutput2> fut2 = reqService.submit(new TestInput2());

        TestOutput2 output2 = fut2.get();

        assertNotNull(output2);
    }
}
