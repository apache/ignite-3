package org.apache.ignite.raft.client.service.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.client.RaftClientMessages;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupClientRequestService;

public class RaftGroupClientRequestServiceImpl implements RaftGroupClientRequestService {
    private final RaftGroupRpcClient rpcClient;
    private final String groupId;

    public RaftGroupClientRequestServiceImpl(RaftGroupRpcClient rpcClient, String groupId) {
        this.rpcClient = rpcClient;
        this.groupId = groupId;
    }

    @Override public <R> CompletableFuture<R> submit(Object request) {
        RaftClientMessages.UserRequest r =
            rpcClient.factory().createUserRequest().setRequest(request).setGroupId(groupId).build();

        CompletableFuture<RaftClientMessages.UserResponse<R>> completableFuture = rpcClient.sendUserRequest(r);

        return completableFuture.thenApply(resp -> resp.response());
    }
}
