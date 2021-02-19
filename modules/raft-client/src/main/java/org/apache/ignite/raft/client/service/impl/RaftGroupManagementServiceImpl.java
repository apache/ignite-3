package org.apache.ignite.raft.client.service.impl;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.apache.ignite.raft.client.service.RaftGroupManagmentService;
import org.apache.ignite.raft.rpc.Node;
import org.jetbrains.annotations.Nullable;

public class RaftGroupManagementServiceImpl implements RaftGroupManagmentService {
    private RaftGroupRpcClient rpcClient;

    public RaftGroupManagementServiceImpl(RaftGroupRpcClient rpcClient) {
        this.rpcClient = rpcClient;
    }

    @Override public @Nullable PeerId getLeader(String groupId) {
        return rpcClient.state(groupId).getLeader();
    }

    @Override public @Nullable List<PeerId> getPeers(String groupId) {
        return rpcClient.state(groupId).getPeers();
    }

    @Override public @Nullable List<PeerId> getLearners(String groupId) {
        return rpcClient.state(groupId).getLearners();
    }

    @Override public CompletableFuture<PeersChangeState> addPeer(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> removePeer(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<Void> resetPeers(PeerId peerId, List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<Void> snapshot(PeerId peerId) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> changePeers(List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> addLearners(List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> removeLearners(List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<PeersChangeState> resetLearners(List<PeerId> peers) {
        return null;
    }

    @Override public CompletableFuture<Void> transferLeader(PeerId newLeader) {
        return null;
    }
}
