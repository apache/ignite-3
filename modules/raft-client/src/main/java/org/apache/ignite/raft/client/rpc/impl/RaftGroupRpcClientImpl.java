package org.apache.ignite.raft.client.rpc.impl;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.raft.PeerId;
import org.apache.ignite.raft.State;
import org.apache.ignite.raft.client.RaftClientMessages;
import org.apache.ignite.raft.client.RaftClientMessages.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.rpc.RaftGroupRpcClient;
import org.jetbrains.annotations.Nullable;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class RaftGroupRpcClientImpl implements RaftGroupRpcClient {
    private final NetworkCluster cluster;

    /** Where to ask for initial configuration. */
    private final Set<NetworkMember> initialCfgNodes;

    /** */
    private final RaftClientMessageFactory factory;

    /** */
    private final int defaultTimeout;

    private Map<String, StateImpl> states = new ConcurrentHashMap<>();

    /**
     * Accepts dependencies in constructor.
     * @param cluster Cluster.
     * @param defaultTimeout Default request timeout.
     * @param initialCfgNode Initial configuration nodes.
     */
    public RaftGroupRpcClientImpl(NetworkCluster cluster, RaftClientMessageFactory factory, int defaultTimeout, Set<NetworkMember> initialCfgNodes) {
        this.defaultTimeout = defaultTimeout;
        this.cluster = cluster;
        this.factory = factory;
        this.initialCfgNodes = new HashSet<>(initialCfgNodes);
    }

    @Override public State state(String groupId) {
        return getState(groupId);
    }

    private StateImpl getState(String groupId) {
        return states.computeIfAbsent(groupId, k -> new StateImpl());
    }

    @Override public CompletableFuture<PeerId> refreshLeader(String groupId) {
        StateImpl state = getState(groupId);

        RaftClientMessages.GetLeaderRequest req = factory.createGetLeaderRequest().setGroupId(groupId).build();

        CompletableFuture<GetLeaderResponse> fut = cluster.sendWithResponse(initialCfgNodes.iterator().next(), req, defaultTimeout);

        return fut.thenApply(resp -> state.leader = resp.getLeaderId());
    }

    @Override public CompletableFuture<State> refreshMembers(String groupId) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.AddPeerResponse> addPeer(RaftClientMessages.AddPeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.RemovePeerResponse> removePeer(RaftClientMessages.RemovePeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<Void> resetPeers(PeerId peerId, RaftClientMessages.ResetPeerRequest request) {
        return null;
    }

    @Override public CompletableFuture<Void> snapshot(PeerId peerId, RaftClientMessages.SnapshotRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.ChangePeersResponse> changePeers(RaftClientMessages.ChangePeersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.LearnersOpResponse> addLearners(RaftClientMessages.AddLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.LearnersOpResponse> removeLearners(RaftClientMessages.RemoveLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.LearnersOpResponse> resetLearners(RaftClientMessages.ResetLearnersRequest request) {
        return null;
    }

    @Override public CompletableFuture<Void> transferLeader(RaftClientMessages.TransferLeaderRequest request) {
        return null;
    }

    @Override public CompletableFuture<RaftClientMessages.UserResponse> sendUserRequest(RaftClientMessages.UserRequest request) {
        if (request.getGroupId() == null)
            throw new IllegalArgumentException("groupId is required");

        State state = state(request.getGroupId());

        CompletableFuture<PeerId> fut0 = state.leader() == null ?
            refreshLeader(request.getGroupId()) : completedFuture(state.leader());

        return fut0.thenCompose(peerId -> cluster.sendWithResponse(peerId.getNode(), request, defaultTimeout));
    }

    @Override public RaftClientMessageFactory factory() {
        return this.factory;
    }

    private static class StateImpl implements State {
        private volatile PeerId leader;

        private volatile List<PeerId> peers;

        private volatile List<PeerId> learners;

        @Override public @Nullable PeerId leader() {
            return leader;
        }

        @Override public @Nullable List<PeerId> peers() {
            return peers;
        }

        @Override public @Nullable List<PeerId> learners() {
            return learners;
        }
    }
}
