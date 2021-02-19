package org.apache.ignite.raft.client.service.impl;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupClientRequestService;

public class RaftGroupClientRequestServiceImpl implements RaftGroupClientRequestService {
    private RaftGroupRpcClient rpcClient;

    public RaftGroupClientRequestServiceImpl(RaftGroupRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override public <T, R> CompletableFuture<R> submit(T request) {
        return null;
    }
}
