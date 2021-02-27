package org.apache.ignite.raft.client.service.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.raft.client.RaftClientMessages;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupClientRequestService;
import org.apache.ignite.raft.rpc.Message;

public class RaftGroupClientRequestServiceImpl implements RaftGroupClientRequestService {
    private final RaftGroupRpcClient rpcClient;
    private final String groupId;

    public RaftGroupClientRequestServiceImpl(RaftGroupRpcClient rpcClient, String groupId) {
        this.rpcClient = rpcClient;
        this.groupId = groupId;
    }

    @Override public <T, R> CompletableFuture<R> submit(T request) {
        RaftClientMessages.UserRequest r =
            rpcClient.factory().createUserRequest().setRequest(request).setGroupId(groupId).build();

        return rpcClient.sendCustom(r).thenApply(new Function<Message, R>() {
            @Override public R apply(Message message) {
                RaftClientMessages.UserResponse<R> resp = (RaftClientMessages.UserResponse<R>) message;

                return resp.response();
            }
        });
    }
}
