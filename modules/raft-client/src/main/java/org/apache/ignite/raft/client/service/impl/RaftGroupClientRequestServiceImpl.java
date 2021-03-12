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

package org.apache.ignite.raft.client.service.impl;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.PeerId;
import org.apache.ignite.raft.client.message.UserRequest;
import org.apache.ignite.raft.client.message.UserResponse;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupClientRequestService;

/**
 * Replication group client service implementation.
 */
public class RaftGroupClientRequestServiceImpl implements RaftGroupClientRequestService {
    /** */
    private final RaftGroupRpcClient rpcClient;

    /** */
    private final String groupId;

    /**
     * @param rpcClient Client.
     * @param groupId Group id.
     */
    public RaftGroupClientRequestServiceImpl(RaftGroupRpcClient rpcClient, String groupId) {
        this.rpcClient = rpcClient;
        this.groupId = groupId;

        try {
            PeerId peerId = rpcClient.refreshLeader(groupId).get();
        }
        catch (Exception e) {
            // TODO log error.
        }
    }

    /** {@inheritDoc} */
    @Override public <R> CompletableFuture<R> submit(Command cmd) {
        UserRequest r = rpcClient.factory().createUserRequest().setRequest(cmd).setGroupId(groupId).build();

        CompletableFuture<UserResponse<R>> fut = rpcClient.submit(r);

        return fut.thenApply(resp -> resp.response());
    }
}
