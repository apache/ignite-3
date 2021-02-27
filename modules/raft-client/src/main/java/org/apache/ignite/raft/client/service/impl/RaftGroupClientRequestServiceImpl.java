package org.apache.ignite.raft.client.service.impl;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import org.apache.ignite.raft.client.RaftClientCommonMessages;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupClientRequestService;
import org.apache.ignite.raft.rpc.Message;

public class RaftGroupClientRequestServiceImpl implements RaftGroupClientRequestService {
    private RaftGroupRpcClient rpcClient;

    public RaftGroupClientRequestServiceImpl(RaftGroupRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override public <T, R> CompletableFuture<R> submit(T request) {
        RaftClientCommonMessages.UserRequest r = rpcClient.factory().createUserRequest().setRequest(request).build();

        return rpcClient.sendCustom(r).thenApply(new Function<Message, R>() {
            @Override public R apply(Message message) {
                RaftClientCommonMessages.UserResponse<R> resp = (RaftClientCommonMessages.UserResponse<R>) message;

                return resp.response();
            }
        });
    }
}
